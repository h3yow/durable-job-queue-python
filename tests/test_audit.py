"""生命周期审计: 关键变化落记录、按时间顺序可查、重启不丢、append-only 禁回写。"""
import logging
import sqlite3
import time

import pytest

from jobqueue import BackoffPolicy, EventType, FailureKind, JobQueue, JobState, SQLiteStore

logger = logging.getLogger("test.audit")


def make_queue(db_path, **kw):
    return JobQueue(
        SQLiteStore(db_path),
        capacity=100,
        backoff=BackoffPolicy(base_seconds=0.01, factor=2.0, max_seconds=0.1),
        lease_seconds=kw.get("lease_seconds", 0.2),
    )


def test_full_lifecycle_events_in_order(queue):
    """提交/领取/失败重试排期/死信/重放/成功: 事件按时间顺序、类型与尝试次数正确。"""
    job = queue.submit({"task": "lifecycle"}, max_attempts=2)
    calls = {"n": 0}

    # 不启动 worker, 手工驱动状态机以精确断言
    j1 = queue.store.claim_next(time.time(), 5.0)
    assert j1.attempts == 1
    from jobqueue.models import FailureRecord
    rec1 = FailureRecord(attempt=1, kind=FailureKind.HANDLER_ERROR,
                         reason="RuntimeError: boom-1", failed_at=time.time())
    j1.failure_history.append(rec1)
    j1.last_failure = rec1
    j1.updated_at = time.time()
    j1.state = JobState.PENDING
    j1.run_at = time.time()
    assert queue.store.fail(j1)

    j2 = queue.store.claim_next(time.time() + 10, 5.0)
    assert j2.attempts == 2
    rec2 = FailureRecord(attempt=2, kind=FailureKind.HANDLER_ERROR,
                         reason="RuntimeError: boom-2", failed_at=time.time())
    j2.failure_history.append(rec2)
    j2.last_failure = rec2
    j2.updated_at = time.time()
    j2.state = JobState.DEAD
    j2.run_at = 0.0
    assert queue.store.fail(j2)

    assert queue.replay(job.id).state == JobState.PENDING
    j3 = queue.store.claim_next(time.time(), 5.0)
    assert queue.store.complete(job.id, j3.lease_token, {"ok": True})

    events = queue.events(job.id)
    seq = [(e.event, e.attempt) for e in events]
    logger.info("判定依据: 完整生命周期事件序列=%s",
                [(e.event.value, e.attempt) for e in events])
    assert seq == [
        (EventType.SUBMITTED, 0),
        (EventType.CLAIMED, 1),
        (EventType.RETRY_SCHEDULED, 1),
        (EventType.CLAIMED, 2),
        (EventType.DEAD_LETTERED, 2),
        (EventType.REPLAYED, 0),
        (EventType.CLAIMED, 1),
        (EventType.SUCCEEDED, 1),
    ]
    # id 单调递增 -> 时间顺序
    ids = [e.id for e in events]
    assert ids == sorted(ids) and len(set(ids)) == len(ids)
    # 原因是可公开的归一化字符串
    retry_ev = next(e for e in events if e.event == EventType.RETRY_SCHEDULED)
    assert retry_ev.reason == "handler_error: RuntimeError: boom-1"


def test_events_persist_across_restart(db_path):
    """重启后历史记录不丢。"""
    q1 = make_queue(db_path)
    job = q1.submit({"task": "persist-me"})
    leased = q1.store.claim_next(time.time(), 0.01)
    q1.store.complete(job.id, leased.lease_token, "done")
    q1.store.close()

    q2 = make_queue(db_path)
    events = q2.events(job.id)
    types = [e.event for e in events]
    assert types == [EventType.SUBMITTED, EventType.CLAIMED, EventType.SUCCEEDED]
    logger.info("判定依据: 重启后 job=%s 审计仍为 %s",
                job.id, [t.value for t in types])
    q2.store.close()


def test_history_not_rewritten_by_later_ops(queue):
    """后续操作只能追加: 既有记录的 attempt/reason/created_at 不被改写。"""
    job = queue.submit({"task": "immutable"}, max_attempts=1)
    leased = queue.store.claim_next(time.time(), 5.0)
    from jobqueue.models import FailureRecord
    rec = FailureRecord(attempt=1, kind=FailureKind.HANDLER_TIMEOUT,
                        reason="execution exceeded 0.01s", failed_at=time.time())
    leased.failure_history.append(rec)
    leased.last_failure = rec
    leased.updated_at = time.time()
    leased.state = JobState.DEAD
    leased.run_at = 0.0
    assert queue.store.fail(leased)

    snapshot = [(e.id, e.event.value, e.attempt, e.reason, e.created_at)
                for e in queue.events(job.id)]

    queue.replay(job.id)
    again = queue.store.claim_next(time.time(), 5.0)
    queue.store.complete(job.id, again.lease_token, "r")
    queue.cancel(job.id)  # 终态取消, no-op

    after = [(e.id, e.event.value, e.attempt, e.reason, e.created_at)
             for e in queue.events(job.id)]
    assert after[: len(snapshot)] == snapshot, "历史记录被后续操作改写"
    assert len(after) == len(snapshot) + 3  # 仅追加 replay/claim/success
    logger.info("判定依据: job=%s 旧记录 %d 条原样保留, 仅追加新记录",
                job.id, len(snapshot))


def test_append_only_enforced_at_storage_layer(db_path):
    """数据库触发器禁止 UPDATE/DELETE 审计记录。"""
    q = make_queue(db_path)
    job = q.submit({"task": "no-tamper"})
    conn = sqlite3.connect(db_path)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE job_events SET reason = 'tampered' WHERE job_id = ?",
                     (job.id,))
    conn.rollback()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM job_events WHERE job_id = ?", (job.id,))
    conn.rollback()
    events = q.events(job.id)
    assert len(events) == 1 and events[0].event == EventType.SUBMITTED
    logger.info("判定依据: job=%s 篡改/删除审计均被触发器拒绝", job.id)
    conn.close()
    q.store.close()


def test_events_do_not_leak_payload_or_traceback(queue):
    """审计原因只含归一化类别与异常类型/消息, 不含载荷字段或堆栈结构。"""
    secret = "super-secret-token-xyz"
    job = queue.submit({"secret": secret, "task": "leak-check"}, max_attempts=1)
    leased = queue.store.claim_next(time.time(), 5.0)
    from jobqueue.models import FailureRecord
    try:
        raise ValueError("sensitive failure")
    except ValueError:
        import traceback
        tb = traceback.format_exc()
    # 直接通过状态机记录的 reason 只允许归一化后的 type: message
    rec = FailureRecord(attempt=1, kind=FailureKind.HANDLER_ERROR,
                        reason="ValueError: sensitive failure",
                        failed_at=time.time())
    leased.failure_history.append(rec)
    leased.last_failure = rec
    leased.updated_at = time.time()
    leased.state = JobState.DEAD
    leased.run_at = 0.0
    assert queue.store.fail(leased)
    assert "Traceback" in tb  # 确认捕获到了堆栈, 但堆栈不允许进入审计
    for e in queue.events(job.id):
        blob = f"{e.event.value} {e.reason or ''}"
        assert secret not in blob, "载荷敏感值进入了审计记录"
        assert "Traceback" not in blob and "File \"" not in blob, "堆栈进入了审计记录"
    logger.info("判定依据: job=%s 审计记录不含载荷与堆栈", job.id)


def test_global_events_since_is_ordered_and_bounded(queue):
    """全局增量查询按 id 顺序, limit 生效, next_since_id 可翻页。"""
    jobs = [queue.submit({"i": i}) for i in range(5)]
    page1 = queue.events_since(0, limit=3)
    assert len(page1) == 3
    assert [e.job_id for e in page1] == [j.id for j in jobs[:3]]
    page2 = queue.events_since(page1[-1].id, limit=3)
    assert [e.job_id for e in page2] == [j.id for j in jobs[3:]]
    assert page1[-1].id < page2[0].id
    logger.info("判定依据: 增量审计翻页有序: page1=%d page2=%d",
                len(page1), len(page2))
