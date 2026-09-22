"""对外异常体系与失败归一化类型。

执行器抛出的任何异常都会被归一化为 FailureKind；内部异常不直接透出到响应，
统一包装为 InternalJobQueueError（对外不携带内部细节）。
"""

from __future__ import annotations

import enum


class FailureKind(str, enum.Enum):
    """可区分的失败类型（写入持久化失败历史，重放不丢失）。"""

    HANDLER_ERROR = "HANDLER_ERROR"          # 处理器普通异常（可重试）
    PERMANENT_ERROR = "PERMANENT_ERROR"      # 处理器主动声明永久失败（不重试）
    TIMEOUT = "TIMEOUT"                      # 执行超时被中断
    CANCELLED = "CANCELLED"                  # 执行中被取消
    RECOVERED = "RECOVERED"                  # 重启恢复：上次执行未确认结果


class JobQueueError(Exception):
    """所有对外可见的任务队列异常基类。reason 为可区分的机器可读原因。"""

    reason: str = "JOB_QUEUE_ERROR"

    def __init__(self, message: str = "", *, task_id: str | None = None):
        super().__init__(message)
        self.task_id = task_id


class TaskNotFoundError(JobQueueError):
    reason = "TASK_NOT_FOUND"


class IdempotencyMismatchError(JobQueueError):
    """同一幂等标识但载荷不同：明确拒绝，原因可区分。"""

    reason = "IDEMPOTENCY_PAYLOAD_MISMATCH"


class QueueFullError(JobQueueError):
    """入口背压：积压超过容量上限。

    REJECT 策略下立即抛出；WAIT 策略等待超时后抛出，二者 reason 不同。
    """

    def __init__(
        self,
        message: str,
        *,
        task_id: str | None = None,
        reason: str = "QUEUE_FULL_REJECT",
        backlog: int = 0,
        capacity: int = 0,
    ):
        super().__init__(message, task_id=task_id)
        self._reason = reason
        self.backlog = backlog
        self.capacity = capacity

    @property
    def reason(self) -> str:  # type: ignore[override]
        return self._reason


class PermanentJobError(JobQueueError):
    """任务处理器可主动抛出：声明该失败为永久失败，不进入重试。"""

    reason = "PERMANENT"


class TaskNotDeadError(JobQueueError):
    """人工重放仅允许针对 DEAD 状态的任务。"""

    reason = "TASK_NOT_DEAD"


class InvalidTaskTypeError(JobQueueError):
    """提交了未注册处理器的任务类型（fail-fast，避免任务静默死信）。"""

    reason = "UNKNOWN_TASK_TYPE"


class InternalJobQueueError(JobQueueError):
    """内部异常的统一出口；不向外部透出内部堆栈/细节。"""

    reason = "INTERNAL_ERROR"
