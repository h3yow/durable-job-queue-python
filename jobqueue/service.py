"""任务队列服务: 提交 / 查询 / 取消 / 重放 / 恢复, 含入口背压。"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from jobqueue.backoff import BackoffPolicy
from jobqueue.errors import (
    IdempotencyConflict,
    InvalidStateTransition,
    JobNotFound,
    QueueFull,
)
from jobqueue.models import Job, JobState
from jobqueue.store import SQLiteStore

logger = logging.getLogger("jobqueue.service")


def _payload_hash(payload: dict[str, Any]) -> str:
    """载荷规范化哈希, 用于幂等冲突判定。"""
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class JobQueue:
    def __init__(
        self,
        store: SQLiteStore,
        *,
        capacity: int = 1000,
        backoff: BackoffPolicy | None = None,
        lease_seconds: float = 30.0,
    ) -> None:
        self._store = store
        self._capacity = capacity
        self._backoff = backoff or BackoffPolicy()
        self._lease_seconds = lease_seconds

    @property
    def backoff(self) -> BackoffPolicy:
        return self._backoff

    @property
    def lease_seconds(self) -> float:
        return self._lease_seconds

    @property
    def store(self) -> SQLiteStore:
        return self._store

    # ------------------------------------------------------------------
    # 恢复
    # ------------------------------------------------------------------
    def recover(self) -> list[Job]:
        """进程重启恢复: 回收过期租约, 执行中的任务重新排队而非丢弃。

        租约尚未过期的任务保持 LEASED, 由后续周期性的 recover 在其
        过期后回收; 旧执行者即使存活, 其租约 token 已无法再通过
        条件更新, 因此不会造成重复生效的写。
        """
        requeued = self._store.requeue_expired_leases(Job.now())
        for job in requeued:
            logger.info(
                "recover: job=%s leased->pending attempts=%d (lease expired)",
                job.id, job.attempts,
            )
        return requeued

    # ------------------------------------------------------------------
    # 提交(幂等 + 背压)
    # ------------------------------------------------------------------
    def submit(
        self,
        payload: dict[str, Any],
        *,
        idempotency_key: str | None = None,
        max_attempts: int = 3,
    ) -> Job:
        digest = _payload_hash(payload)
        if idempotency_key is not None:
            existing = self._store.get_job_by_idempotency_key(idempotency_key)
            if existing is not None:
                if self._store.get_payload_hash(existing.id) != digest:
                    logger.info(
                        "submit: job=%s key=%s rejected (idempotency_conflict)",
                        existing.id, idempotency_key,
                    )
                    raise IdempotencyConflict(
                        f"idempotency key {idempotency_key!r} already used "
                        "with a different payload"
                    )
                logger.info(
                    "submit: job=%s key=%s deduplicated -> existing state=%s",
                    existing.id, idempotency_key, existing.state.value,
                )
                return existing

        if self._store.count_active() >= self._capacity:
            logger.info("submit: rejected (queue_full) capacity=%d", self._capacity)
            raise QueueFull(f"queue capacity {self._capacity} reached")

        now = Job.now()
        job = Job(
            id=Job.new_id(),
            idempotency_key=idempotency_key,
            payload=payload,
            state=JobState.PENDING,
            max_attempts=max_attempts,
            run_at=now,
            created_at=now,
            updated_at=now,
        )
        self._store.insert_job(job, digest)
        logger.info("submit: job=%s -> pending attempts<=%d", job.id, max_attempts)
        return job

    # ------------------------------------------------------------------
    # 查询 / 取消 / 重放
    # ------------------------------------------------------------------
    def get(self, job_id: str) -> Job:
        job = self._store.get_job(job_id)
        if job is None:
            raise JobNotFound(job_id)
        return job

    def cancel(self, job_id: str) -> Job:
        """取消, 幂等: 排队中立即取消; 已领取的置取消标记由执行者中断;
        已到达终态的重复取消不报错也不改变结果。"""
        job = self._store.cancel(job_id)
        if job is None:
            raise JobNotFound(job_id)
        logger.info(
            "cancel: job=%s state=%s cancel_requested=%s",
            job.id, job.state.value, job.cancel_requested,
        )
        return job

    def replay(self, job_id: str) -> Job:
        """人工重放死信任务, 原始失败原因保留在 failure_history。"""
        job = self._store.replay(job_id)
        if job is None:
            current = self._store.get_job(job_id)
            if current is None:
                raise JobNotFound(job_id)
            raise InvalidStateTransition(
                f"only dead jobs can be replayed, got {current.state.value}"
            )
        logger.info(
            "replay: job=%s dead->pending history=%d last_reason=%s",
            job.id, len(job.failure_history),
            job.last_failure.reason if job.last_failure else None,
        )
        return job

    def list_dead(self) -> list[Job]:
        return self._store.list_dead()
