"""提交、幂等、背压与取消语义。"""
import logging

import pytest

from jobqueue import (
    IdempotencyConflict,
    InvalidStateTransition,
    JobNotFound,
    JobQueue,
    JobState,
    QueueFull,
    SQLiteStore,
)

logger = logging.getLogger("test.submission")


def test_idempotent_submit_returns_same_job(queue):
    j1 = queue.submit({"n": 1}, idempotency_key="k-1")
    j2 = queue.submit({"n": 1}, idempotency_key="k-1")
    logger.info("判定依据: 同幂等键同载荷 -> 同一任务 id=%s state=%s", j1.id, j2.state.value)
    assert j1.id == j2.id
    assert queue.store.count_active() == 1


def test_idempotency_conflict_on_different_payload(queue):
    queue.submit({"n": 1}, idempotency_key="k-2")
    with pytest.raises(IdempotencyConflict) as ei:
        queue.submit({"n": 2}, idempotency_key="k-2")
    logger.info("判定依据: 同幂等键不同载荷 -> code=%s", ei.value.code)
    assert ei.value.code == "idempotency_conflict"


def test_backpressure_queue_full(db_path):
    q = JobQueue(SQLiteStore(db_path), capacity=2)
    q.submit({"n": 1})
    q.submit({"n": 2})
    with pytest.raises(QueueFull) as ei:
        q.submit({"n": 3})
    logger.info("判定依据: 积压达容量上限 -> code=%s", ei.value.code)
    assert ei.value.code == "queue_full"
    q.store.close()


def test_cancel_pending_job_is_terminal_and_idempotent(queue):
    job = queue.submit({"n": 1})
    r1 = queue.cancel(job.id)
    logger.info("状态迁移: job=%s pending->%s effect=%s",
                job.id, r1.job.state.value, r1.effect.value)
    assert r1.job.state == JobState.CANCELLED
    assert r1.effect.value == "cancelled"
    r2 = queue.cancel(job.id)
    logger.info("判定依据: 重复取消终态任务 -> 仍 %s effect=%s, 不报错",
                r2.job.state.value, r2.effect.value)
    assert r2.job.state == JobState.CANCELLED
    assert r2.effect.value == "cancelled"


def test_cancel_unknown_job_raises(queue):
    with pytest.raises(JobNotFound) as ei:
        queue.cancel("no-such-job")
    assert ei.value.code == "job_not_found"


def test_replay_non_dead_job_rejected(queue):
    job = queue.submit({"n": 1})
    with pytest.raises(InvalidStateTransition) as ei:
        queue.replay(job.id)
    logger.info("判定依据: 非死信任务重放 -> code=%s state=%s", ei.value.code, job.state.value)
    assert ei.value.code == "invalid_state_transition"
