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


def test_complete_rejected_after_cancel_accepted_store_level(queue):
    """确定性复现: 取消已受理(标记已落库)后, 迟到的 complete 必须被拒绝。

    时序: 领取 -> 处理器返回 -> 取消受理(cancel_requested=1) -> complete 落库。
    修复前 complete 不带取消前置条件, 会把任务改回 SUCCEEDED 并带上结果。
    """
    job = queue.submit({"task": "cancel-then-complete"})
    claimed = queue.store.claim_next(time.time(), 5.0)
    assert claimed is not None and claimed.id == job.id

    ack = queue.cancel(job.id)  # 取消被受理
    assert ack.state == JobState.LEASED and ack.cancel_requested

    ok = queue.store.complete(job.id, claimed.lease_token, "late-result")
    assert not ok, "取消已受理后 complete 必须被拒绝"
    assert queue.store.complete_cancel(job.id, claimed.lease_token)

    final = queue.get(job.id)
    assert final.state == JobState.CANCELLED
    assert final.result is None, "取消收口后不得保留处理器返回值"
    kinds = [e.event for e in queue.events(job.id)]
    assert EventType.SUCCEEDED not in kinds
    assert kinds.index(EventType.CANCEL_REQUESTED) < kinds.index(EventType.CANCELLED)
    logger.info("判定依据: job=%s 取消受理后 complete 被拒, 终态=%s", job.id, final.state.value)


# ----------------------------------------------------------------------
# 完成边界竞态: 取消恰好卡在"处理器已结束"与"结果落库"之间
# ----------------------------------------------------------------------
from jobqueue import JobQueue, SQLiteStore  # noqa: E402
from jobqueue import BackoffPolicy  # noqa: E402


class CancelInjectingStore(SQLiteStore):
    """在指定的条件写入落库前注入一次取消受理。

    工作协程在处理器结束后先读取消标记、再执行条件写入(complete/fail);
    本类把取消精确注入到这两者之间的窗口, 确定性复现完成边界竞态。
    与 worker 同属一个线程, RLock 可重入, 不会自锁。
    """

    def __init__(self, path: str, inject_on: str) -> None:
        super().__init__(path)
        self._inject_on = inject_on  # "complete" | "fail"
        self._injected = False

    def _maybe_inject(self, job_id: str) -> None:
        if not self._injected:
            self._injected = True
            self.cancel(job_id)  # 取消在结果落库前被受理

    def complete(self, job_id, lease_token, result):  # noqa: ANN001, ANN201
        if self._inject_on == "complete":
            self._maybe_inject(job_id)
        return super().complete(job_id, lease_token, result)

    def fail(self, job):  # noqa: ANN001, ANN201
        if self._inject_on == "fail":
            self._maybe_inject(job.id)
        return super().fail(job)


def _make_injected_queue(db_path, inject_on):
    store = CancelInjectingStore(db_path, inject_on)
    return JobQueue(
        store,
        capacity=100,
        backoff=BackoffPolicy(base_seconds=0.01, factor=2.0, max_seconds=0.1),
        lease_seconds=5.0,
    )


def test_cancel_in_completion_window_handler_success(db_path):
    """处理器成功返回后、结果落库前取消被受理: 终态必须是 CANCELLED, 不留结果。"""
    queue = _make_injected_queue(db_path, "complete")

    async def scenario():
        async def handler(payload):
            return "handler-return-value"

        pool = WorkerPool(queue, handler, concurrency=1, poll_interval=0.005)
        await pool.start()
        job = queue.submit({"task": "window-success"}, max_attempts=3)
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.CANCELLED)
        await pool.stop()
        return job, ok

    job, ok = run(scenario())
    final = queue.get(job.id)
    assert ok, "取消已受理, 必须收口为 CANCELLED 而不是 SUCCEEDED"
    assert final.state == JobState.CANCELLED
    assert final.result is None, "处理器返回值不得随取消收口暴露"
    assert final.attempts == 1, "不得重新排队重试"
    events = [e.event for e in queue.events(job.id)]
    assert EventType.SUCCEEDED not in events
    assert EventType.RETRY_SCHEDULED not in events
    assert events.index(EventType.CANCEL_REQUESTED) < events.index(EventType.CANCELLED)
    logger.info("判定依据: job=%s 成功落库窗口取消 -> %s", job.id, final.state.value)
    queue.store.close()


def test_cancel_in_completion_window_handler_error(db_path):
    """处理器抛错后、失败落库前取消被受理: 收口 CANCELLED, 不重试不进死信。"""
    queue = _make_injected_queue(db_path, "fail")

    async def scenario():
        async def handler(payload):
            raise RuntimeError("boom in window")

        pool = WorkerPool(queue, handler, concurrency=1, poll_interval=0.005)
        await pool.start()
        job = queue.submit({"task": "window-error"}, max_attempts=5)
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.CANCELLED)
        await pool.stop()
        return job, ok

    job, ok = run(scenario())
    final = queue.get(job.id)
    assert ok and final.state == JobState.CANCELLED
    assert final.attempts == 1, "取消受理后不得重试, 尝试次数不增加"
    events = [e.event for e in queue.events(job.id)]
    assert EventType.RETRY_SCHEDULED not in events
    assert EventType.DEAD_LETTERED not in events
    logger.info("判定依据: job=%s 异常落库窗口取消 -> %s", job.id, final.state.value)
    queue.store.close()


def test_cancel_in_completion_window_handler_timeout(db_path):
    """处理器超时被中断后、失败落库前取消被受理: 收口 CANCELLED, 不重试。"""
    queue = _make_injected_queue(db_path, "fail")

    async def scenario():
        async def handler(payload):
            await asyncio.sleep(10)

        pool = WorkerPool(queue, handler, concurrency=1, execute_timeout=0.05,
                          poll_interval=0.005)
        await pool.start()
        job = queue.submit({"task": "window-timeout"}, max_attempts=5)
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.CANCELLED)
        await pool.stop()
        return job, ok

    job, ok = run(scenario())
    final = queue.get(job.id)
    assert ok and final.state == JobState.CANCELLED
    assert final.attempts == 1
    events = [e.event for e in queue.events(job.id)]
    assert EventType.RETRY_SCHEDULED not in events
    assert EventType.DEAD_LETTERED not in events
    logger.info("判定依据: job=%s 超时落库窗口取消 -> %s", job.id, final.state.value)
    queue.store.close()


def test_accepted_cancel_never_succeeds_under_concurrency(queue):
    """真实并发取消: 凡取消被受理(CANCEL_REQUESTED)的任务, 终态只能是 CANCELLED。"""
    async def scenario():
        started = asyncio.Event()
        in_flight = {"n": 0}

        async def handler(payload):
            in_flight["n"] += 1
            started.set()
            await asyncio.sleep(0.02)
            return f"r{payload['i']}"

        pool = WorkerPool(queue, handler, concurrency=4, poll_interval=0.001)
        await pool.start()
        jobs = [queue.submit({"i": i}) for i in range(60)]
        await asyncio.wait_for(started.wait(), 2)
        # 执行期间持续发起取消, 与完成/失败落库真实并发
        for j in jobs:
            queue.cancel(j.id)
            await asyncio.sleep(0)
        ok = await wait_for(
            lambda: all(
                queue.get(j.id).state in (JobState.SUCCEEDED, JobState.CANCELLED)
                for j in jobs
            ),
            timeout=15,
        )
        await pool.stop()
        return jobs, ok

    jobs, ok = run(scenario())
    assert ok
    accepted = 0
    for j in jobs:
        final = queue.get(j.id)
        events = [e.event for e in queue.events(j.id)]
        if EventType.CANCEL_REQUESTED in events:
            accepted += 1
            assert final.state == JobState.CANCELLED, (
                f"job={j.id} 取消已受理却收口为 {final.state.value}"
            )
            assert final.result is None
            assert EventType.SUCCEEDED not in events, (
                f"job={j.id} 取消受理后出现成功记录"
            )
            assert events.index(EventType.CANCEL_REQUESTED) < len(events), (
                "受理后必须有后续收口"
            )
        else:
            # 排队中被取消(立即生效)或取消前已成功: 均为合法终态
            assert final.state in (JobState.SUCCEEDED, JobState.CANCELLED)
    assert accepted > 0, "场景未覆盖到执行中取消, 测试无效"
    logger.info(
        "判定依据: 60 个并发任务中 %d 个执行中取消被受理, 全部收口 CANCELLED",
        accepted,
    )
