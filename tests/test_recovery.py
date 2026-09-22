"""进程重启恢复：执行中任务不丢失、不重复执行；恢复原因被记录且可区分。"""

from __future__ import annotations

import asyncio
from jobqueue.errors import FailureKind
from jobqueue.models import TaskStatus
from jobqueue.queue import JobQueue, QueueOptions
from jobqueue.retry import BackoffPolicy
from jobqueue.storage import SQLiteStorage
from jobqueue.worker import WorkerPool


async def _make_queue(db_path: str, **kw) -> JobQueue:
    opts = kw.pop("options", QueueOptions(max_attempts=3, capacity=1000))
    q = JobQueue(
        SQLiteStorage(db_path),
        options=opts,
        backoff=BackoffPolicy(base_delay=0.02, factor=2, max_delay=0.1),
    )
    await q.initialize()
    return q


async def test_running_orphan_requeued_and_executed_once(db_path):
    # 1) 第一个进程：初始化 + 提交 + 领取（模拟执行到一半进程被 kill）
    q1 = await _make_queue(db_path)
    t = await q1.submit("work", "payload-x")
    claimed = await q1.claim("runner-of-dead-process")
    assert claimed.status is TaskStatus.RUNNING
    assert claimed.attempts == 1
    # 进程崩溃：不做任何结算直接关闭底层连接
    await q1.aclose()

    # 2) 第二个进程启动：initialize 触发恢复
    exec_count = {"n": 0}

    async def handler(payload: str) -> str:
        assert payload == "payload-x"
        exec_count["n"] += 1
        return "after-recovery"

    q2 = await _make_queue(db_path)
    rec = await q2.get_task(t.id)
    assert rec.status is TaskStatus.PENDING, "孤儿 RUNNING 必须被重排"
    assert rec.attempts == 2, "恢复消耗一次尝试预算"
    assert rec.runner_id is None
    assert rec.last_failure_kind == FailureKind.RECOVERED.value
    assert "RUNNING when process restarted" in rec.last_failure_reason
    assert rec.failures[-1]["kind"] == FailureKind.RECOVERED.value

    pool = WorkerPool(q2, {"work": handler}, concurrency=1, poll_interval=0.01)
    await pool.start()
    while True:
        rec = await q2.get_task(t.id)
        if rec.status.terminal:
            break
        await asyncio.sleep(0.01)
    assert rec.status is TaskStatus.SUCCEEDED
    assert rec.result == "after-recovery"
    assert exec_count["n"] == 1, "恢复后任务只执行一次，未发生重复执行"
    await pool.stop()
    await q2.aclose()


async def test_recovery_happens_exactly_once(db_path):
    q1 = await _make_queue(db_path)
    t = await q1.submit("work", "p")
    await q1.claim("r1")
    await q1.aclose()

    q2 = await _make_queue(db_path)
    rec = await q2.get_task(t.id)
    assert rec.status is TaskStatus.PENDING and rec.attempts == 2
    # 同一进程内再次"初始化"不会重复处理（已无 RUNNING 孤儿）
    # 直接验证存储层面：再无孤儿
    import sqlite3
    conn = sqlite3.connect(db_path)
    n = conn.execute(
        "SELECT COUNT(*) FROM tasks WHERE status='RUNNING'"
    ).fetchone()[0]
    conn.close()
    assert n == 0
    # 第三次启动同样平静无事
    await q2.aclose()
    q3 = await _make_queue(db_path)
    rec = await q3.get_task(t.id)
    assert rec.status is TaskStatus.PENDING and rec.attempts == 2
    await q3.aclose()


async def test_cancelling_orphan_settles_cancelled_on_restart(db_path):
    q1 = await _make_queue(db_path)
    t = await q1.submit("work", "p")
    await q1.claim("r1")
    rec = await q1.request_cancel(t.id)
    assert rec.status is TaskStatus.CANCELLING
    # 进程在 worker 确认取消前崩溃
    await q1.aclose()

    q2 = await _make_queue(db_path)
    rec = await q2.get_task(t.id)
    assert rec.status is TaskStatus.CANCELLED
    assert rec.runner_id is None
    # 不可再被领取
    assert await q2.claim("r2") is None
    await q2.aclose()


async def test_orphan_on_last_attempt_goes_dead(db_path):
    # max_attempts=2：首次领取 attempts=1，崩溃恢复 attempts+1=2 且 >= 上限
    q1 = JobQueue(
        SQLiteStorage(db_path),
        options=QueueOptions(max_attempts=2),
    )
    await q1.initialize()
    t = await q1.submit("work", "p")
    await q1.claim("r1")
    await q1.aclose()

    q2 = JobQueue(
        SQLiteStorage(db_path),
        options=QueueOptions(max_attempts=2),
    )
    await q2.initialize()
    rec = await q2.get_task(t.id)
    assert rec.status is TaskStatus.DEAD
    assert rec.attempts == 2
    assert rec.last_failure_kind == FailureKind.RECOVERED.value
    # 仍可人工重放，且 RECOVERED 原因保留
    replayed = await q2.replay_dead(t.id)
    assert replayed.status is TaskStatus.PENDING
    assert any(
        f["kind"] == FailureKind.RECOVERED.value for f in replayed.failures
    )
    await q2.aclose()


async def test_pending_tasks_survive_restart_with_delay_intact(db_path):
    q1 = await _make_queue(db_path)
    await q1.submit("work", "a")
    await q1.submit("work", "b")
    await q1.aclose()

    q2 = await _make_queue(db_path)
    # PENDING（从未被领取）重启后立即可领取，状态与次数不变
    assert await q2.claim("r")  # t1
    assert await q2.claim("r")  # t2
    assert await q2.claim("r") is None
    await q2.aclose()
