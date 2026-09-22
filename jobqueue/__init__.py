"""本地持久化异步任务队列子系统。"""

from __future__ import annotations

from jobqueue.errors import (
    FailureKind,
    IdempotencyMismatchError,
    InvalidTaskTypeError,
    InternalJobQueueError,
    JobQueueError,
    PermanentJobError,
    QueueFullError,
    TaskNotFoundError,
    TaskNotDeadError,
)
from jobqueue.models import TaskRecord, TaskStatus
from jobqueue.queue import JobQueue, QueueOptions
from jobqueue.retry import BackoffPolicy
from jobqueue.storage import SQLiteStorage
from jobqueue.worker import WorkerPool

__all__ = [
    "BackoffPolicy",
    "FailureKind",
    "IdempotencyMismatchError",
    "InternalJobQueueError",
    "JobQueue",
    "JobQueueError",
    "PermanentJobError",
    "QueueFullError",
    "QueueOptions",
    "SQLiteStorage",
    "TaskNotFoundError",
    "TaskNotDeadError",
    "InvalidTaskTypeError",
    "TaskRecord",
    "TaskStatus",
    "WorkerPool",
]
