"""取消最终一致: 与成功/异常/超时/完成边界竞态, 重复取消幂等, 返回语义可区分。"""
import asyncio
import logging
import time
from collections import Counter

from jobqueue import EventType, JobState, WorkerPool

logger = logging.getLogger("test.cancel")


def run(coro):
    return asyncio.run(coro)


async def wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.005)
    return False


def test_cancel_while_leased_always_settles_cancelled_on_success(queue):
    """执行中取消: 处理器成功返回, 仍收口为 CANCELLED, 结果不暴露为成功。"""
    async def scenario():
        started = asyncio.Event()

        async def handler(payload):
            started.set()
            await asyncio.sleep(0.15)
            return "success-must-not-win"

        pool = WorkerPool(queue, handler, concurrency=1, poll_interval=0.005)
        await pool.start()
        job = queue.submit({"task": "cancel-vs-success"})
        await asyncio.wait_for(started.wait(), 2)
        ack = queue.cancel(job.id)
        assert ack.state == JobState.LEASED and ack.cancel_requested
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.CANCELLED)
        await pool.stop()
        return job, ok

    job, ok = run(scenario())
    final = queue.get(job.id)
    assert ok
    assert final.state == JobState.CANCELLED
    assert final.result is None
    logger.info("判定依据: job=%s 处理器成功但取消生效 -> %s", job.id, final.state.value)


def test_cancel_while_leased_settles_cancelled_on_handler_error(queue):
    """执行中取消: 处理器抛错, 也不进入重试, 始终收口 CANCELLED。"""
    async def scenario():
        started = asyncio.Event()

        async def handler(payload):
            started.set()
            await asyncio.sleep(0.1)
            raise RuntimeError("boom during cancel window")

        pool = WorkerPool(queue, handler, concurrency=1, poll_interval=0.005)
        await pool.start()
        job = queue.submit({"task": "cancel-vs-error"}, max_attempts=5)
        await asyncio.wait_for(started.wait(), 2)
        queue.cancel(job.id)
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.CANCELLED)
        await pool.stop()
        return job, ok

    job, ok = run(scenario())
    final = queue.get(job.id)
    assert ok and final.state == JobState.CANCELLED
    assert final.attempts == 1, "取消生效后不得重试, 尝试次数不增加"
    logger.info(
        "判定依据: job=%s 处理器异常但取消收口 -> %s attempts=%d",
        job.id, final.state.value, final.attempts,
    )


def test_cancel_while_leased_settles_cancelled_on_timeout(queue):
    """执行中取消: 处理器超时被中断, 同样收口 CANCELLED 而非重试/死信。"""
    async def scenario():
        started = asyncio.Event()

        async def handler(payload):
            started.set()
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                # 模拟处理器在超时时点又被取消: 取消仍必须收口
                raise

        pool = WorkerPool(queue, handler, concurrency=1, execute_timeout=0.1,
                          poll_interval=0.005)
        await pool.start()
        job = queue.submit({"task": "cancel-vs-timeout"}, max_attempts=5)
        await asyncio.wait_for(started.wait(), 2)
        await asyncio.sleep(0.02)
        queue.cancel(job.id)
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.CANCELLED)
        await pool.stop()
        return job, ok

    job, ok = run(scenario())
    final = queue.get(job.id)
    assert ok and final.state == JobState.CANCELLED
    assert final.state != JobState.DEAD
    logger.info("判定依据: job=%s 超时边界取消 -> %s", job.id, final.state.value)


def test_cancel_race_at_completion_boundary(queue):
    """取消卡在完成边界: 终态只能是 SUCCEEDED 或 CANCELLED, 不会回退/重试。"""
    async def scenario():
        async def handler(payload):
            await asyncio.sleep(0.002)
            return "ok"

        pool = WorkerPool(queue, handler, concurrency=8, poll_interval=0.001)
        await pool.start()
        jobs = [queue.submit({"i": i}) for i in range(100)]
        for j in jobs:
            queue.cancel(j.id)  # 与领取/成功落库并发
        ok = await wait_for(
            lambda: all(
                queue.get(j.id).state in (JobState.SUCCEEDED, JobState.CANCELLED)
                for j in jobs
            ),
            timeout=10,
        )
        await pool.stop()
        return jobs, ok

    jobs, ok = run(scenario())
    states = Counter(queue.get(j.id).state for j in jobs)
    assert ok
    assert set(states) <= {JobState.SUCCEEDED, JobState.CANCELLED}
    assert queue.store.count_active() == 0
    # 成功的任务不得带 cancel 痕迹的回退; 取消的任务不得再被执行
    for j in jobs:
        fresh = queue.get(j.id)
        assert fresh.state in (JobState.SUCCEEDED, JobState.CANCELLED)
        if fresh.state == JobState.CANCELLED:
            assert fresh.result is None
        # 不变量: 取消一旦受理(存在 cancel_requested 审计), 终态只能是已取消,
        # 受理之后不允许再出现成功/重试/死信记录。
        events = queue.events(j.id)
        kinds = [e.event for e in events]
        if EventType.CANCEL_REQUESTED in kinds:
            assert fresh.state == JobState.CANCELLED
            assert fresh.result is None
            tail = kinds[kinds.index(EventType.CANCEL_REQUESTED) + 1:]
            assert tail == [EventType.CANCELLED], (j.id, kinds)
    logger.info(
        "判定依据: 100 个完成边界竞态终态分布=%s, 无 active 残留",
        {s.value: n for s, n in states.items()},
    )


def test_concurrent_duplicate_cancel_is_idempotent(queue):
    """并发重复取消执行中任务: 只受理一次, 终态幂等, 审计不重复。"""
    async def scenario():
        started = asyncio.Event()

        async def handler(payload):
            started.set()
            await asyncio.sleep(0.2)
            return "x"

        pool = WorkerPool(queue, handler, concurrency=1, poll_interval=0.005)
        await pool.start()
        job = queue.submit({"task": "dup-cancel"})
        await asyncio.wait_for(started.wait(), 2)
        acks = await asyncio.gather(
            *[asyncio.to_thread(queue.cancel, job.id) for _ in range(8)]
        )
        assert all(a.state == JobState.LEASED for a in acks)
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.CANCELLED)
        await pool.stop()
        return job, ok

    job, ok = run(scenario())
    assert ok
    final = queue.cancel(job.id)  # 终态后再次取消: 不报错不改变
    assert final.state == JobState.CANCELLED
    requested = [e for e in queue.events(job.id)
                 if e.event == EventType.CANCEL_REQUESTED]
    cancelled = [e for e in queue.events(job.id) if e.event == EventType.CANCELLED]
    assert len(requested) == 1, "并发重复受理只记一条 cancel_requested"
    assert len(cancelled) == 1
    logger.info(
        "判定依据: job=%s 8 次并发取消 -> requested=%d cancelled=%d",
        job.id, len(requested), len(cancelled),
    )


def test_cancel_terminal_states_is_noop(queue):
    """取消 SUCCEEDED / CANCELLED / DEAD: 幂等无副作用, 不追加审计。"""
    succeeded = queue.submit({"task": "done"})
    leased = queue.store.claim_next(time.time(), 5.0)
    assert queue.store.complete(succeeded.id, leased.lease_token, "r")

    already_cancelled = queue.submit({"task": "already-cancelled"})
    queue.cancel(already_cancelled.id)

    dead = queue.submit({"task": "will-die"}, max_attempts=1)

    async def die(payload):
        raise RuntimeError("x")

    async def scenario():
        pool = WorkerPool(queue, die, concurrency=1, poll_interval=0.005)
        await pool.start()
        ok = await wait_for(lambda: queue.get(dead.id).state == JobState.DEAD)
        await pool.stop()
        return ok

    assert run(scenario())

    before = {
        succeeded.id: queue.events(succeeded.id),
        already_cancelled.id: queue.events(already_cancelled.id),
        dead.id: queue.events(dead.id),
    }
    for job_id in before:
        ack = queue.cancel(job_id)  # 全部不应报错
        assert ack is not None
    assert queue.get(succeeded.id).state == JobState.SUCCEEDED
    assert queue.get(succeeded.id).result == "r", "成功结果不得被取消改写"
    assert queue.get(already_cancelled.id).state == JobState.CANCELLED
    assert queue.get(dead.id).state == JobState.DEAD
    for job_id, old_events in before.items():
        assert queue.events(job_id) == old_events, f"{job_id} 审计记录被追加/改写"
    logger.info("判定依据: 对三类终态重复取消均无副作用、审计不变")


def test_pending_vs_leased_cancel_return_semantics_distinct(queue):
    """排队中取消返回 CANCELLED(立即生效), 执行中取消返回 LEASED+标记(已受理)。"""
    pending_job = queue.submit({"task": "still-queued"})
    ack_pending = queue.cancel(pending_job.id)
    assert ack_pending.state == JobState.CANCELLED
    assert ack_pending.cancel_requested is False

    leased_job = queue.submit({"task": "running"})
    claimed = queue.store.claim_next(time.time(), 5.0)
    assert claimed is not None
    ack_leased = queue.cancel(leased_job.id)
    assert ack_leased.state == JobState.LEASED
    assert ack_leased.cancel_requested is True
    logger.info(
        "判定依据: 排队取消返回 state=%s; 执行中取消返回 state=%s + 标记",
        ack_pending.state.value, ack_leased.state.value,
    )
