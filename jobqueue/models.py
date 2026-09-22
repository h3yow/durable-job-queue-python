"""任务状态机与任务记录模型。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime, timezone

# 单任务日志器：测试可通过 caplog 抓取，日志中带任务标识/状态迁移/判定依据
transition_logger = "jobqueue.transition"


class TaskStatus(str, enum.Enum):
    """任务状态机：

    PENDING    -> RUNNING -> SUCCEEDED
               |         |-> PENDING（失败后延迟重排，attempt 未到上限；失败信息入失败历史）
               |         |-> DEAD（重试耗尽 / 永久失败）
               |         |-> CANCELLING -> CANCELLED（取消已被领取的任务）
               |-> CANCELLED（取消排队中的任务）
    DEAD 可由人工 REPLAY 重新进入 PENDING（失败历史保留，attempts 归零）。
    终态：SUCCEEDED / DEAD / CANCELLED。
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    CANCELLING = "CANCELLING"
    SUCCEEDED = "SUCCEEDED"
    DEAD = "DEAD"
    CANCELLED = "CANCELLED"

    @property
    def terminal(self) -> bool:
        return self in TERMINAL_STATUSES


TERMINAL_STATUSES = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.DEAD, TaskStatus.CANCELLED}
)

# 合法状态迁移表（存储层流转校验依据）
ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset(
        {TaskStatus.RUNNING, TaskStatus.CANCELLED}
    ),
    TaskStatus.RUNNING: frozenset(
        {
            TaskStatus.SUCCEEDED,
            TaskStatus.PENDING,
            TaskStatus.DEAD,
            TaskStatus.CANCELLING,
        }
    ),
            TaskStatus.CANCELLING: frozenset({TaskStatus.CANCELLED}),
    TaskStatus.DEAD: frozenset({TaskStatus.PENDING}),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class TaskRecord:
    """单条任务的持久化快照。"""

    id: str
    task_type: str
    payload: str
    status: TaskStatus
    idempotency_key: str | None
    attempts: int
    max_attempts: int
    visible_at: float
    created_at: str
    updated_at: str
    last_failure_kind: str | None = None
    last_failure_reason: str | None = None
    result: str | None = None
    runner_id: str | None = None
    failures: tuple[dict, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "task_type": self.task_type,
            "payload": self.payload,
            "status": self.status.value,
            "idempotency_key": self.idempotency_key,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "visible_at": self.visible_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_failure_kind": self.last_failure_kind,
            "last_failure_reason": self.last_failure_reason,
            "result": self.result,
            "runner_id": self.runner_id,
            "failures": list(self.failures),
        }
