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
from jobqueue.models import (
    AuditEvent,
    CancelEffect,
    CancelResult,
    EventType,
    Job,
    JobState,
)
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
        stale_timeout_seconds: float | None = None,
    ) -> None:
        self._store = store
        self._capacity = capacity
        self._backoff = backoff or BackoffPolicy()
        self._lease_seconds = lease_seconds
        # 运行期失联判定阈值, 默认等于一个租约周期; 调小可快速复现收回。
        self._stale_timeout_seconds = (
            stale_timeout_seconds if stale_timeout_seconds is not None
            else lease_seconds
        )

    @property
    def backoff(self) -> BackoffPolicy:
        return self._backoff

    @property
    def lease_seconds(self) -> float:
        return self._lease_seconds

    @property
    def stale_timeout_seconds(self) -> float:
        return self._stale_timeout_seconds

    @property
    def store(self) -> SQLiteStore:
        return self._store

    # ------------------------------------------------------------------
    # 自愈: 启动恢复 + 运行期失联扫描
    # ------------------------------------------------------------------
    def recover(self) -> list[Job]:
        """启动恢复(默认配置随正常启动流程调用)。

        上次进程残留的全部 LEASED 任务立即收回: 未受理取消的回到
        PENDING 立即可再次执行, 已受理取消的直接收口 CANCELLED。
        不要求先等租约过期, 避免任务在默认配置下长期停在执行中。
        attempts 不增加, 旧租约 token 立即失效。
        """
        return self._reclaim(
            only_expired=False, reason="startup recovery: previous lease abandoned"
        )

    def reclaim_stale(self) -> list[Job]:
        """运行期失联扫描: 收回超过 stale_timeout_seconds 无进展的任务。

        判定依据是领取时写入的 lease_expires_at, 阈值可配置;
        重复扫描是幂等的(条件迁移, 已不在 LEASED 的行不会再动)。
        """
        return self._reclaim(
            only_expired=True,
            reason=f"runtime reclaim: no progress for {self._stale_timeout_seconds}s",
        )

    def _reclaim(self, *, only_expired: bool, reason: str) -> list[Job]:
        now = Job.now()
        results = self._store.reclaim_leased_jobs(
            now, only_expired=only_expired, reason=reason
        )
        requeued: list[Job] = []
        for job, event, why in results:
            if event == EventType.CANCELLED:
                logger.info(
                    "reclaim: job=%s leased->cancelled attempts=%d (%s)",
                    job.id, job.attempts, why,
                )
            else:
                logger.info(
                    "reclaim: job=%s leased->pending attempts=%d (%s)",
                    job.id, job.attempts, why,
                )
            requeued.append(job)
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

    def cancel(self, job_id: str) -> CancelResult:
        """取消, 幂等, 返回语义区分排队中与执行中:

        - PENDING -> 立即 CANCELLED, effect=CANCELLED
        - LEASED  -> 置取消标记, effect=ACCEPTED, 由执行者/自愈收口
        - 已在终态 -> 原样返回, effect=CANCELLED, 不报错不改写结果
        并发重复取消执行中任务只有第一次受理落审计, 其余幂等返回。
        """
        job, event = self._store.cancel(job_id)
        if job is None:
            raise JobNotFound(job_id)
        if event == EventType.CANCEL_REQUESTED:
            effect = CancelEffect.ACCEPTED
        else:
            # PENDING 首次取消、终态重复取消都归为"此刻已是取消终态语义"
            effect = CancelEffect.CANCELLED
        logger.info(
            "cancel: job=%s state=%s effect=%s cancel_requested=%s",
            job.id, job.state.value, effect.value, job.cancel_requested,
        )
        return CancelResult(job=job, effect=effect)

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

    # ------------------------------------------------------------------
    # 生命周期审计查询
    # ------------------------------------------------------------------
    def get_events(self, job_id: str) -> list[AuditEvent]:
        """单任务按时间顺序的关键变化记录; 不存在的任务抛 JobNotFound。"""
        if self._store.get_job(job_id) is None:
            raise JobNotFound(job_id)
        return self._store.list_events(job_id)

    def list_events(self, *, after_id: int = 0, limit: int = 500) -> list[AuditEvent]:
        """全局按时间顺序翻页查询审计记录 (id > after_id)。"""
        return self._store.list_events_since(after_id, limit)
