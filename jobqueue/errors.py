"""对外可区分的错误类型。内部异常不得直接透出, 统一收敛为这些类型。"""
from __future__ import annotations


class JobQueueError(Exception):
    """子系统错误基类。"""

    code = "internal_error"


class QueueFull(JobQueueError):
    """入口背压: 积压超过容量上限, 按策略拒绝。"""

    code = "queue_full"


class IdempotencyConflict(JobQueueError):
    """幂等标识相同但载荷不同。"""

    code = "idempotency_conflict"


class JobNotFound(JobQueueError):
    code = "job_not_found"


class InvalidStateTransition(JobQueueError):
    """例如对非死信任务重放。"""

    code = "invalid_state_transition"
