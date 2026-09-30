"""取消受理收口保证: 完成/异常/超时边界竞态的确定性复现 + 真实并发取消。

背景缺陷: 执行者成功路径曾是 "先 get_job() 看 cancel_requested 标记,
再调 complete() 落成功", 两步之间存在 TOCTOU 窗口; 而 complete() 的条件
UPDATE 只校验 state/lease_token, 不校验 cancel_requested。取消若恰好在
该窗口被受理, 任务仍会被翻成 SUCCEEDED 并保留处理器结果。

本文件用仅在测试中使用的存储子类在 "终态 SQL 执行前一刻" 这个接缝点上
从其它真实线程并发发起取消, 稳定复现该窗口, 并断言:
取消一旦受理, 无论处理器成功/抛错/超时, 终态只能是 CANCELLED。
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time

from jobqueue import (
    BackoffPolicy,
    EventType,
    JobQueue,
    JobState,
    SQLiteStore,
    WorkerPool,
)
from jobqueue.models import FailureKind, FailureRecord

logger = logging.getLogger("test.cancel_boundary")

_SETTLED = {JobState.SUCCEEDED, JobState.CANCELLED, JobState.DEAD}


class RaceWindowStore(SQLiteStore):
    """带接缝的存储: 在成功/失败落库 SQL 执行前一刻触发回调。"""

    def __init__(self, path: str, *, on_complete=None, on_fail=None) -> None:
        super().__init__(path)
        self._on_complete = on_complete
        self._on_fail = on_fail

    def complete(self, job_id: str, lease_token: str, result: object) -> bool:
        if self._on_complete is not None:
            self._on_complete(job_id)
        return super().complete(job_id, lease_token, result)

    def fail(self, job) -> bool:
        if self._on_fail is not None:
            self._on_fail(job)
        return super().fail(job)


def make_queue(tmp_path, *, on_complete=None, on_fail=None):
    store = RaceWindowStore(
        str(tmp_path / "jobs.db"), on_complete=on_complete, on_fail=on_fail
    )
    queue = JobQueue(
        store,
        capacity=1000,
        backoff=BackoffPolicy(base_seconds=0.01, factor=2.0, max_seconds=0.1),
        lease_seconds=30.0,
    )
    return queue, store


def concurrent_cancel(queue: JobQueue, job_id: str, *, width: int = 4,
                      hold: float = 0.005) -> None:
    """从 width 个真实线程并发取消同一任务(含重复取消), 并让出时间窗。"""
    threads = [
        threading.Thread(target=queue.cancel, args=(job_id,))
        for _ in range(width)
    ]
    for t in threads:
        t.start()
    # 让出执行权, 保证取消线程在终态 SQL 之前真正参与锁竞争。
    time.sleep(hold)
    for t in threads:
        t.join()


async def settle(queue: JobQueue, job_ids, *, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(queue.get(j).state in _SETTLED for j in job_ids):
            return True
        await asyncio.sleep(0.005)
    return False


def _assert_cancelled_only(queue: JobQueue, job_id: str, *, attempts: int = 1) -> None:
    final = queue.get(job_id)
    assert final.state == JobState.CANCELLED, f"终态={final.state.value}"
    assert final.result is None, "取消收口不得保留处理器结果"
    assert final.attempts == attempts, "取消后不得重试, 尝试次数不得增加/回退"
    assert final.lease_token is None and final.lease_expires_at is None
    assert final.last_failure is None and final.failure_history == [], (
        "取消收口不得写入失败历史"
    )
    events = queue.events(job_id)
    kinds = [e.event for e in events]
    assert kinds.count(EventType.CANCEL_REQUESTED) == 1
    assert kinds.count(EventType.CANCELLED) == 1
    # 取消受理之后绝不允许出现成功/重试/死信记录。
    accepted = kinds.index(EventType.CANCEL_REQUESTED)
    tail = kinds[accepted + 1:]
    assert tail == [EventType.CANCELLED], f"取消受理后的审计尾部={tail}"


# ---------------------------------------------------------------------
# 存储层原子性: 取消已受理后, 成功/失败写入必须被条件 SQL 直接拒绝
# ---------------------------------------------------------------------
def test_store_rejects_success_write_after_cancel_accepted(tmp_path):
    queue, store = make_queue(tmp_path)
    job = queue.submit({"task": "success-vs-cancel"})
    claimed = store.claim_next(time.time(), 30.0)
    assert claimed is not None and claimed.id == job.id
    ack = queue.cancel(job.id)
    assert ack.state == JobState.LEASED and ack.cancel_requested

    assert store.complete(job.id, claimed.lease_token, {"must": "not-persist"}) is False

    fresh = store.get_job(job.id)
    assert fresh.state == JobState.LEASED, "被拒绝后状态不得越位"
    assert fresh.cancel_requested and fresh.result is None
    assert store.complete_cancel(job.id, claimed.lease_token) is True
    assert store.get_job(job.id).state == JobState.CANCELLED
    kinds = [e.event for e in store.list_events(job.id)]
    assert EventType.SUCCEEDED not in kinds
    store.close()


def test_store_rejects_failure_write_after_cancel_accepted(tmp_path):
    queue, store = make_queue(tmp_path)
    job = queue.submit({"task": "fail-vs-cancel"}, max_attempts=5)
    claimed = store.claim_next(time.time(), 30.0)
    queue.cancel(job.id)

    claimed.state = JobState.PENDING
    claimed.run_at = time.time() + 99
    claimed.last_failure = FailureRecord(
        attempt=1, kind=FailureKind.HANDLER_ERROR, reason="x", failed_at=time.time()
    )
    claimed.failure_history = [claimed.last_failure]
    claimed.updated_at = time.time()
    assert store.fail(claimed) is False

    fresh = store.get_job(job.id)
    assert fresh.state == JobState.LEASED, "取消优先: 不得重排/进死信"
    assert fresh.last_failure is None and fresh.failure_history == []
    assert store.complete_cancel(job.id, claimed.lease_token) is True
    kinds = [e.event for e in store.list_events(job.id)]
    assert EventType.RETRY_SCHEDULED not in kinds
    assert EventType.DEAD_LETTERED not in kinds
    store.close()


def test_cancel_settle_keeps_prior_failure_history_but_clears_result(tmp_path):
    """第 1 次失败重试后、第 2 次执行中被取消: 收口 CANCELLED,
    result 清空, 而此前重试的历史失败记录(append-only 语义)保留。"""
    queue, store = make_queue(tmp_path)
    job = queue.submit({"task": "retry-then-cancel"}, max_attempts=5)

    # 第一次领取并失败 -> 重排, 留下一条历史失败。
    first = store.claim_next(time.time(), 30.0)
    rec1 = FailureRecord(
        attempt=1, kind=FailureKind.HANDLER_ERROR,
        reason="RuntimeError: first", failed_at=time.time(),
    )
    first.last_failure = rec1
    first.failure_history = [rec1]
    first.state = JobState.PENDING
    first.run_at = time.time()
    first.updated_at = time.time()
    assert store.fail(first) is True

    # 第二次领取后取消被受理, 处理器又返回了结果: 必须收口 CANCELLED。
    second = store.claim_next(time.time(), 30.0)
    assert second.attempts == 2
    ack = queue.cancel(job.id)
    assert ack.cancel_requested
    assert store.complete(job.id, second.lease_token, {"late": "result"}) is False
    assert store.complete_cancel(job.id, second.lease_token) is True

    final = store.get_job(job.id)
    assert final.state == JobState.CANCELLED
    assert final.result is None
    assert final.attempts == 2, "attempts 不增不退"
    assert len(final.failure_history) == 1, "历史失败记录保留"
    assert final.failure_history[0].kind == FailureKind.HANDLER_ERROR
    assert final.last_failure is not None
    kinds = [e.event for e in store.list_events(job.id)]
    assert kinds == [
        EventType.SUBMITTED, EventType.CLAIMED, EventType.RETRY_SCHEDULED,
        EventType.CLAIMED, EventType.CANCEL_REQUESTED, EventType.CANCELLED,
    ]
    store.close()


# ---------------------------------------------------------------------
# 执行者层面: 取消卡在成功/异常/超时三种完成边界上, 都必须收口 CANCELLED
# ---------------------------------------------------------------------
def test_cancel_at_success_boundary_forces_cancelled(tmp_path):
    queue, store = make_queue(
        tmp_path, on_complete=lambda jid: concurrent_cancel(queue, jid, width=1)
    )

    async def handler(payload):
        return "success-must-not-win"

    async def scenario():
        pool = WorkerPool(queue, handler, concurrency=2, poll_interval=0.002)
        await pool.start()
        job = queue.submit({"task": "boundary-success"})
        ok = await settle(queue, [job.id])
        await pool.stop()
        return job, ok

    job, ok = asyncio.run(scenario())
    assert ok
    _assert_cancelled_only(queue, job.id)
    store.close()


def test_cancel_at_handler_error_boundary_forces_cancelled(tmp_path):
    queue, store = make_queue(
        tmp_path, on_fail=lambda job: concurrent_cancel(queue, job.id, width=1)
    )

    async def handler(payload):
        raise RuntimeError("boom at cancel boundary")

    async def scenario():
        pool = WorkerPool(queue, handler, concurrency=2, poll_interval=0.002)
        await pool.start()
        job = queue.submit({"task": "boundary-error"}, max_attempts=5)
        ok = await settle(queue, [job.id])
        await pool.stop()
        return job, ok

    job, ok = asyncio.run(scenario())
    assert ok
    _assert_cancelled_only(queue, job.id)
    store.close()


def test_cancel_at_timeout_boundary_forces_cancelled(tmp_path):
    queue, store = make_queue(
        tmp_path, on_fail=lambda job: concurrent_cancel(queue, job.id, width=1)
    )

    async def handler(payload):
        await asyncio.sleep(10.0)

    async def scenario():
        pool = WorkerPool(
            queue, handler, concurrency=2, poll_interval=0.002, execute_timeout=0.05
        )
        await pool.start()
        job = queue.submit({"task": "boundary-timeout"}, max_attempts=5)
        ok = await settle(queue, [job.id])
        await pool.stop()
        return job, ok

    job, ok = asyncio.run(scenario())
    assert ok
    _assert_cancelled_only(queue, job.id)
    store.close()


# ---------------------------------------------------------------------
# 真实并发: 多个任务、多个工作协程, 成功落库前一刻由多个真实线程并发
# (含重复) 取消, 终态只能全部是 CANCELLED
# ---------------------------------------------------------------------
def test_real_concurrent_cancel_at_completion_boundary(tmp_path):
    n_jobs = 40
    queue, store = make_queue(
        tmp_path, on_complete=lambda jid: concurrent_cancel(queue, jid, width=4)
    )

    async def handler(payload):
        # 不同处理器时长, 让取消与完成在各个时间点上自然交错。
        await asyncio.sleep(0.001 + (payload["i"] % 5) * 0.001)
        return {"echo": payload["i"]}

    async def scenario():
        pool = WorkerPool(queue, handler, concurrency=8, poll_interval=0.001)
        await pool.start()
        jobs = [queue.submit({"i": i}) for i in range(n_jobs)]
        ok = await settle(queue, [j.id for j in jobs])
        await pool.stop()
        return jobs, ok

    jobs, ok = asyncio.run(scenario())
    assert ok
    states = {}
    for j in jobs:
        _assert_cancelled_only(queue, j.id)  # 每个任务都必须收口为已取消
        states[queue.get(j.id).state] = states.get(queue.get(j.id).state, 0) + 1
    assert states == {JobState.CANCELLED: n_jobs}, states
    assert store.count_active() == 0
    logger.info("判定依据: %d 个任务全部在真实并发取消下收口 CANCELLED", n_jobs)
    store.close()
