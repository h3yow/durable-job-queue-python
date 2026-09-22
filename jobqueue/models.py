"""任务与状态机的核心类型定义。"""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


class JobState(str, enum.Enum):
    """任务状态机。

    合法迁移:
        PENDING   -> LEASED      (工作协程领取)
        PENDING   -> CANCELLED   (取消排队中的任务)
        LEASED    -> SUCCEEDED   (执行成功)
        LEASED    -> PENDING     (失败重试, 延迟重排 / 崩溃恢复)
        LEASED    -> DEAD        (重试耗尽进入死信)
        LEASED    -> CANCELLED   (取消已领取的任务, 执行被中断)
        DEAD      -> PENDING     (人工重放, 保留历史失败原因)
    终态: SUCCEEDED / CANCELLED; DEAD 仅可经人工重放离开。
    """

    PENDING = "pending"
    LEASED = "leased"
    SUCCEEDED = "succeeded"
    CANCELLED = "cancelled"
    DEAD = "dead"


TERMINAL_STATES = frozenset({JobState.SUCCEEDED, JobState.CANCELLED})


class FailureKind(str, enum.Enum):
    """归一化后的失败类型, 对外只暴露这些可区分的类别。"""

    HANDLER_ERROR = "handler_error"      # 执行器业务异常
    HANDLER_TIMEOUT = "handler_timeout"  # 执行超时被中断
    CANCELLED = "cancelled"              # 执行期间被取消
    INTERNAL = "internal"                # 框架内部异常(不向外透出细节)


@dataclass
class FailureRecord:
    """一次失败尝试的记录, 重放死信任务时保留。"""

    attempt: int
    kind: FailureKind
    reason: str
    failed_at: float


@dataclass
class Job:
    id: str
    idempotency_key: str | None
    payload: dict[str, Any]
    state: JobState = JobState.PENDING
    attempts: int = 0
    max_attempts: int = 3
    run_at: float = 0.0
    lease_token: str | None = None
    lease_expires_at: float | None = None
    cancel_requested: bool = False
    last_failure: FailureRecord | None = None
    failure_history: list[FailureRecord] = field(default_factory=list)
    result: Any = None
    created_at: float = 0.0
    updated_at: float = 0.0

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex

    @staticmethod
    def now() -> float:
        return time.time()
