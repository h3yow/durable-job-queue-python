"""入口背压：积压达上限时按确定策略拒绝/延迟，原因可区分，内存有界。"""

from __future__ import annotations

import asyncio

import pytest

from jobqueue.errors import QueueFullError
from jobqueue.models import TaskStatus
from jobqueue.queue import JobQueue, QueueOptions
from jobqueue.storage import SQLiteStorage


async def _queue(db_path, **opts) -> JobQueue:
    q = JobQueue(SQLiteStorage(db_path), options=QueueOptions(**opts))
    await q.initialize()
    return q


async def test_reject_policy_immediate_distinct_reason(db_path):
    q = await _queue(db_path, capacity=3, overflow_policy="REJECT")
    for i in range(3):
        await q.submit("work", str(i))
    with pytest.raises(QueueFullError) as ei:
        await q.submit("work", "overflow")
    assert ei.value.reason == "QUEUE_FULL_REJECT"
    assert ei.value.backlog == 3 and ei.value.capacity == 3
    # 拒绝后实际任务数仍为 3：积压不随拒绝无界增长
    assert q._storage._count_pending() == 3
    await q.aclose()


async def test_wait_policy_blocks_until_claim_releases_slot(db_path):
    q = await _queue(
        db_path, capacity=1, overflow_policy="WAIT", wait_timeout=2.0
    )
    await q.submit("work", "first")

    done = asyncio.Event()

    async def late_submit():
        rec = await q.submit("work", "second")
        done.set()
        return rec

    task = asyncio.create_task(late_submit())
    await asyncio.sleep(0.05)
    assert not done.is_set(), "容量满时 WAIT 请求必须挂起"

    first = await q.claim("r1")  # 领取释放一个积压名额
    assert first is not None and first.payload == "first"
    second = await asyncio.wait_for(task, timeout=1.0)
    assert done.is_set() and second.payload == "second"
    await q.aclose()


async def test_wait_policy_timeout_distinct_reason(db_path):
    q = await _queue(
        db_path, capacity=1, overflow_policy="WAIT", wait_timeout=0.05
    )
    await q.submit("work", "first")
    with pytest.raises(QueueFullError) as ei:
        await q.submit("work", "second")
    assert ei.value.reason == "QUEUE_FULL_WAIT_TIMEOUT"
    assert ei.value.backlog == 1 and ei.value.capacity == 1
    await q.aclose()


async def test_cancelled_queued_task_releases_capacity(db_path):
    q = await _queue(db_path, capacity=1, overflow_policy="REJECT")
    t = await q.submit("work", "first")
    with pytest.raises(QueueFullError):
        await q.submit("work", "second")
    await q.request_cancel(t.id)  # 排队取消 -> CANCELLED，腾出名额
    t2 = await q.submit("work", "second")
    assert t2.status is TaskStatus.PENDING
    await q.aclose()


async def test_backlog_counts_pending_including_delayed(db_path):
    """延迟重排中的任务仍占用积压名额（保守背压，不超前放量）。"""
    q = await _queue(
        db_path,
        capacity=2,
        overflow_policy="REJECT",
        max_attempts=5,
    )
    # 手工构造一个延迟可见的 PENDING
    import time
    from jobqueue.models import TaskRecord
    rec = TaskRecord(
        id="delayed-1", task_type="work", payload="d",
        status=TaskStatus.PENDING, idempotency_key=None,
        attempts=1, max_attempts=5, visible_at=time.time() + 3600,
        created_at="", updated_at="",
    )
    q._storage._insert_task(rec)
    await q.submit("work", "ready-1")  # 第 2 个名额
    with pytest.raises(QueueFullError) as ei:
        await q.submit("work", "overflow")
    assert ei.value.backlog == 2
    await q.aclose()
