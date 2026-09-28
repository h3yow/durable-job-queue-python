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


class EventType(str, enum.Enum):
    """生命周期审计事件类型, 与关键状态变化一一对应。

    审计只记录"发生了什么变化、第几次尝试、可公开的原因",
    不记录内部异常堆栈与任务载荷。
    """

    SUBMITTED = "submitted"              # 提交受理
    CLAIMED = "claimed"                  # 被执行者领取
    SUCCEEDED = "succeeded"              # 执行成功
    RETRY_SCHEDULED = "retry_scheduled"  # 失败后按退避重排
    DEAD_LETTERED = "dead_lettered"      # 重试耗尽进入死信
    REPLAYED = "replayed"                # 人工重放死信
    CANCEL_REQUESTED = "cancel_requested"  # 取消受理(排队中立即生效)
    CANCELLED = "cancelled"              # 取消最终生效(含执行中/失联收口)
    RECLAIMED = "reclaimed"              # 自愈收回(重启恢复或运行期失联)


@dataclass
class AuditEvent:
    """一条只追加、不可改写的生命周期审计记录。"""

    id: int
    job_id: str
    event: EventType
    attempt: int
    reason: str | None
    created_at: float


class CancelEffect(str, enum.Enum):
    """取消受理后的即时语义, 用于区分排队中取消与执行中取消。

    CANCELLED  : 排队中(或已终态), 任务此刻即为 CANCELLED
    ACCEPTED   : 执行中, 取消已受理, 必定收口为 CANCELLED
    """

    CANCELLED = "cancelled"
    ACCEPTED = "accepted"


@dataclass
class CancelResult:
    """取消请求的受理结果。

    effect 区分排队中取消(立即终态)与执行中取消(已受理待收口);
    重复取消终态任务时 effect=CANCELLED, 幂等不报错也不改写结果。
    """

    job: Job
    effect: CancelEffect


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
