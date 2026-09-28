"""取消最终一致: 受理后无论处理器成功/异常/超时/完成边界, 必收口 CANCELLED。"""
import asyncio
import logging
import time
from collections import Counter

from jobqueue import CancelEffect, EventType, JobState, WorkerPool

logger = logging.getLogger("test.cancel_consistency")


def run(coro):
    return asyncio.run(coro)


async def wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.005)
    return False


async def _run_until(queue, handler, job_pred, **pool_kw):
    pool = WorkerPool(queue, handler, concurrency=1, poll_interval=0.005,
                      enable_reclaimer=False, **pool_kw)
    await pool.start()
    try:
        ok = await wait_for(job_pred)
    finally:
        await pool.stop()
    return ok


def _start_pool_with_signal(queue, handler_body):
    """辅助: 启动池并等待处理器真正开始执行。"""
    started = asyncio.Event()

    async def handler(payload):
        started.set()
        return await handler_body()

    pool = WorkerPool(queue, handler, concurrency=1, poll_interval=0.005,
                      enable_reclaimer=False)
    return pool, started, handler


def test_cancel_then_handler_succeeds_stays_cancelled(queue):
    """执行中受理取消, 处理器紧接着成功 -> 不得改回 SUCCEEDED。"""
    async def scenario():
        pool, started, handler = _start_pool_with_signal(
            queue, lambda: asyncio.sleep(0.15)
        )
        await pool.start()
        job = queue.submit({"task": "succeed"})
        await asyncio.wait_for(started.wait(), 2)
        res = queue.cancel(job.id)
        assert res.effect == CancelEffect.ACCEPTED
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.CANCELLED)
        await pool.stop()
        return job, ok

    job, ok = run(scenario())
    final = queue.get(job.id)
    logger.info("判定依据: job=%s 取消后成功 -> 终态=%s result=%r",
                job.id, final.state.value, final.result)
    assert ok
    assert final.state == JobState.CANCELLED
    assert final.result is None


def test_cancel_then_handler_raises_stays_cancelled(queue):
    """执行中受理取消, 处理器抛错 -> 不得重排重试, 收口 CANCELLED。"""
    async def body():
        await asyncio.sleep(0.1)
        raise RuntimeError("boom after cancel")

    async def scenario():
        pool, started, handler = _start_pool_with_signal(queue, body)
        await pool.start()
        job = queue.submit({"task": "raise"}, max_attempts=5)
        await asyncio.wait_for(started.wait(), 2)
        queue.cancel(job.id)
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.CANCELLED)
        await pool.stop()
        return job, ok

    job, ok = run(scenario())
    final = queue.get(job.id)
    logger.info("判定依据: job=%s 取消后异常 -> 终态=%s attempts=%d history=%d",
                job.id, final.state.value, final.attempts, len(final.failure_history))
    assert ok
    assert final.state == JobState.CANCELLED
    assert final.attempts == 1, "未发生重试"


def test_cancel_then_handler_times_out_stays_cancelled(queue):
    """执行中受理取消, 处理器超时 -> 不得按超时重试, 收口 CANCELLED。"""
    async def body():
        await asyncio.sleep(5)

    async def scenario():
        pool, started, handler = _start_pool_with_signal(queue, body)
        # 处理器会先遇到取消检查还是 wait_for 超时:
        # sleep 0.1 后 cancel, execute_timeout=0.1 已开始计时;
        # 无论哪个先发生, 都必须 CANCELLED, 不能重试。
        pool._execute_timeout = 0.1
        await pool.start()
        job = queue.submit({"task": "timeout"}, max_attempts=5)
        await asyncio.wait_for(started.wait(), 2)
        await asyncio.sleep(0.02)
        queue.cancel(job.id)
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.CANCELLED)
        await pool.stop()
        return job, ok

    job, ok = run(scenario())
    final = queue.get(job.id)
    logger.info("判定依据: job=%s 取消后超时 -> 终态=%s attempts=%d",
                job.id, final.state.value, final.attempts)
    assert ok
    assert final.state == JobState.CANCELLED
    assert final.attempts == 1


def test_cancel_exactly_at_completion_boundary_never_succeeds(queue):
    """取消与完成并发交错大量重复: 一旦受理, 成功写入必被守卫拒绝。"""
    async def scenario():
        async def handler(payload):
            await asyncio.sleep(0.001)
            return "done"

        pool = WorkerPool(queue, handler, concurrency=4, poll_interval=0.001,
                          enable_reclaimer=False)
        await pool.start()
        jobs = [queue.submit({"i": i}) for i in range(60)]

        async def cancel_spam():
            end = time.monotonic() + 1.0
            while time.monotonic() < end:
                for j in jobs:
                    queue.cancel(j.id)
                await asyncio.sleep(0.0005)

        canceller = asyncio.create_task(cancel_spam())
        ok = await wait_for(
            lambda: all(
                queue.get(j.id).state in (JobState.SUCCEEDED, JobState.CANCELLED)
                for j in jobs
            ), timeout=5.0,
        )
        canceller.cancel()
        await pool.stop()
        return jobs, ok

    jobs, ok = run(scenario())
    states = Counter(queue.get(j.id).state for j in jobs)
    bad = [
        j.id for j in jobs
        if queue.get(j.id).state == JobState.SUCCEEDED
        and queue.get(j.id).cancel_requested
    ]
    logger.info("判定依据: 边界竞态终态分布=%s, 取消后仍成功数=%d",
                {s.value: n for s, n in states.items()}, len(bad))
    assert ok
    assert set(states) <= {JobState.SUCCEEDED, JobState.CANCELLED}
    assert bad == []
    assert queue.store.count_active() == 0


def test_concurrent_duplicate_cancel_is_idempotent(queue):
    """并发重复取消同一执行中任务: 只受理一次, 最终唯一。"""
    async def scenario():
        started = asyncio.Event()

        async def handler(payload):
            started.set()
            await asyncio.sleep(0.2)
            return "x"

        pool = WorkerPool(queue, handler, concurrency=1, poll_interval=0.005,
                          enable_reclaimer=False)
        await pool.start()
        job = queue.submit({"task": "dup-cancel"})
        await asyncio.wait_for(started.wait(), 2)
        results = await asyncio.gather(*[
            asyncio.to_thread(queue.cancel, job.id) for _ in range(10)
        ])
        effects = Counter(r.effect for r in results)
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.CANCELLED)
        await pool.stop()
        return job, effects, ok

    job, effects, ok = run(scenario())
    events = queue.get_events(job.id)
    accepted = [e for e in events if e.event == EventType.CANCEL_REQUESTED]
    logger.info("判定依据: job=%s 10 次并发取消 effect 分布=%s, 受理审计条数=%d",
                job.id, {e.value: n for e, n in effects.items()}, len(accepted))
    assert ok
    assert effects[CancelEffect.ACCEPTED] == 1, "只有第一次受理为 accepted"
    assert effects[CancelEffect.CANCELLED] == 9
    assert len(accepted) == 1
    assert queue.get(job.id).state == JobState.CANCELLED


def test_cancel_terminal_states_is_idempotent(queue):
    """取消成功/已取消/死信终态任务: 不报错、不改写结果、不新增审计。"""
    async def ok_handler(payload):
        return "winner"

    async def fail_handler(payload):
        raise RuntimeError("dead")

    succeeded = queue.submit({"task": "done"})
    assert run(_run_until(
        queue, ok_handler,
        lambda: queue.get(succeeded.id).state == JobState.SUCCEEDED))
    r1 = queue.cancel(succeeded.id)
    r2 = queue.cancel(succeeded.id)
    assert r1.job.state == r2.job.state == JobState.SUCCEEDED
    assert r1.job.result == "winner"
    assert r1.effect == CancelEffect.CANCELLED

    cancelled = queue.submit({"task": "cancelled"})
    queue.cancel(cancelled.id)
    assert queue.cancel(cancelled.id).job.state == JobState.CANCELLED

    dead = queue.submit({"task": "dead"}, max_attempts=1)
    run(_run_until(queue, fail_handler,
                   lambda: queue.get(dead.id).state == JobState.DEAD))
    before = len(queue.get_events(dead.id))
    rd = queue.cancel(dead.id)
    assert rd.job.state == JobState.DEAD, "死信走重放流程, 不被取消改动"
    assert len(queue.get_events(dead.id)) == before
    logger.info("判定依据: 成功/已取消/死信重复取消均幂等, 结果不回写")


def test_cancel_effect_distinguishes_queued_vs_executing(queue):
    """排队中取消立即终态且 effect=cancelled; 执行中 effect=accepted。"""
    queued = queue.submit({"task": "waiting"})
    rq = queue.cancel(queued.id)
    assert rq.effect == CancelEffect.CANCELLED
    assert rq.job.state == JobState.CANCELLED

    async def scenario():
        started = asyncio.Event()

        async def handler(payload):
            started.set()
            await asyncio.sleep(0.15)
            return "x"

        pool = WorkerPool(queue, handler, concurrency=1, poll_interval=0.005,
                          enable_reclaimer=False)
        await pool.start()
        leased = queue.submit({"task": "running"})
        await asyncio.wait_for(started.wait(), 2)
        rl = queue.cancel(leased.id)
        ok = await wait_for(lambda: queue.get(leased.id).state == JobState.CANCELLED)
        await pool.stop()
        return rl, ok

    rl, ok = run(scenario())
    logger.info("判定依据: 排队中 effect=%s, 执行中 effect=%s -> 终态一致",
                rq.effect.value, rl.effect.value)
    assert rl.effect == CancelEffect.ACCEPTED
    assert rl.job.state == JobState.LEASED
    assert ok
