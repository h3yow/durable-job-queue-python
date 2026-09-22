"""状态机基础：合法迁移、终态稳定、非法迁移被存储层拒绝。"""

from __future__ import annotations

import pytest

from jobqueue.models import ALLOWED_TRANSITIONS, TERMINAL_STATUSES, TaskStatus
from jobqueue.storage import StorageError


def test_terminal_set():
    assert TaskStatus.SUCCEEDED.terminal
    assert TaskStatus.DEAD.terminal
    assert TaskStatus.CANCELLED.terminal
    assert not TaskStatus.PENDING.terminal
    assert not TaskStatus.RUNNING.terminal
    assert not TaskStatus.CANCELLING.terminal
    assert TERMINAL_STATUSES == {
        TaskStatus.SUCCEEDED, TaskStatus.DEAD, TaskStatus.CANCELLED
    }


def test_transition_table_shape():
    # SUCCEEDED / CANCELLED 为不可变终态，无任何出边
    assert ALLOWED_TRANSITIONS[TaskStatus.SUCCEEDED] == frozenset()
    assert ALLOWED_TRANSITIONS[TaskStatus.CANCELLED] == frozenset()
    # DEAD 虽为终态，但保留唯一出边：人工重放 DEAD->PENDING
    assert ALLOWED_TRANSITIONS[TaskStatus.DEAD] == frozenset(
        {TaskStatus.PENDING}
    )
    # 关键路径存在
    assert TaskStatus.RUNNING in ALLOWED_TRANSITIONS[TaskStatus.PENDING]
    assert TaskStatus.CANCELLED in ALLOWED_TRANSITIONS[TaskStatus.PENDING]
    assert TaskStatus.PENDING in ALLOWED_TRANSITIONS[TaskStatus.RUNNING]
    assert TaskStatus.DEAD in ALLOWED_TRANSITIONS[TaskStatus.RUNNING]
    assert TaskStatus.CANCELLING in ALLOWED_TRANSITIONS[TaskStatus.RUNNING]
    assert TaskStatus.CANCELLED in ALLOWED_TRANSITIONS[TaskStatus.CANCELLING]
    # 判定依据：不存在 FAILED 中间态、不存在越级迁移
    assert not hasattr(TaskStatus, "FAILED")
    assert TaskStatus.SUCCEEDED not in ALLOWED_TRANSITIONS[TaskStatus.PENDING]


async def test_storage_rejects_illegal_transition(storage):
    from jobqueue.models import TaskRecord
    import time

    rec = TaskRecord(
        id="t1", task_type="x", payload="{}", status=TaskStatus.PENDING,
        idempotency_key=None, attempts=0, max_attempts=3,
        visible_at=time.time(), created_at="", updated_at="",
    )
    storage._insert_task(rec)
    storage._claim_next(time.time(), "r1")  # -> RUNNING
    # RUNNING -> CANCELLED 是越级（必须经 CANCELLING）
    with pytest.raises(StorageError, match="illegal transition"):
        storage._transition(
            "t1", (TaskStatus.RUNNING,), TaskStatus.CANCELLED
        )
