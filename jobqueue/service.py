"""任务队列服务: 提交 / 查询 / 取消 / 重放 / 恢复, 含入口背压。"""
from __future__ import annotations

from typing import Any

from jobqueue.backoff import BackoffPolicy
from jobqueue.models import Job
from jobqueue.store import SQLiteStore


class JobQueue:
    def __init__(
        self,
        store: SQLiteStore,
        *,
        capacity: int = 1000,
        backoff: BackoffPolicy | None = None,
        lease_seconds: float = 30.0,
    ) -> None:
        raise NotImplementedError

    def recover(self) -> list[Job]:
        """进程重启恢复: 回收过期租约, 不丢弃执行中的任务。"""
        raise NotImplementedError

    def submit(
        self,
        payload: dict[str, Any],
        *,
        idempotency_key: str | None = None,
        max_attempts: int = 3,
    ) -> Job:
        raise NotImplementedError

    def get(self, job_id: str) -> Job:
        raise NotImplementedError

    def cancel(self, job_id: str) -> Job:
        raise NotImplementedError

    def replay(self, job_id: str) -> Job:
        raise NotImplementedError

    def list_dead(self) -> list[Job]:
        raise NotImplementedError
