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
from jobqueue.models import Job, JobEvent, JobState
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
        stale_lease_seconds: float | None = None,
        startup_reclaim_all: bool = True,
    ) -> None:
        self._store = store
        self._capacity = capacity
        self._backoff = backoff or BackoffPolicy()
        self._lease_seconds = lease_seconds
        # 运行期失联判定阈值: 租约超过该时长无进展即视为执行者失联。
        # 默认与租约时长一致(即租约到期即失联), 可调小以便更快收回。
        self._stale_lease_seconds = (
            stale_lease_seconds if stale_lease_seconds is not None else lease_seconds
        )
        # 默认配置下启动即回收全部 LEASED(本地单进程服务, 旧进程已消失)。
        self._startup_reclaim_all = startup_reclaim_all

    @property
    def backoff(self) -> BackoffPolicy:
        return self._backoff

    @property
    def lease_seconds(self) -> float:
        return self._lease_seconds

    @property
    def stale_lease_seconds(self) -> float:
        return self._stale_lease_seconds

    @property
    def store(self) -> SQLiteStore:
        return self._store

    # ------------------------------------------------------------------
    # 恢复 / 自愈
    # ------------------------------------------------------------------
    def recover(self) -> list[Job]:
        """正常启动流程的自愈入口。

        默认(startup_reclaim_all=True)回收所有 LEASED 任务: 本地服务重启
        意味着旧执行者已消失, 不必等待租约到期, 任务不会长期停在执行中。
        置了取消标记的任务直接收口为 CANCELLED, 其余回到 PENDING。
        若显式关闭该开关, 则只回收租约已过期的任务(兼容旧行为)。
        """
        if self._startup_reclaim_all:
            reclaimed = self._store.reclaim_leases(
                Job.now(), only_expired=False, reason="startup recovery: reclaim all leased jobs"
            )
        else:
            reclaimed = self._store.reclaim_leases(
                Job.now(), only_expired=True,
                reason="startup recovery: lease expired",
            )
        for job in reclaimed:
            logger.info(
                "recover: job=%s leased->%s attempts=%d (%s)",
                job.id, job.state.value, job.attempts,
                "cancel requested" if job.state == JobState.CANCELLED else "reclaimed",
            )
        return reclaimed

    def reclaim_stale(self, now: float | None = None) -> list[Job]:
        """运行期自愈: 周期发现长时间无进展(租约到期)的执行中任务并收回。

        收回是带 state=LEASED 前置条件的原子迁移: 旧执行者晚到的完成/
        失败写入会因租约 token 不匹配被拒, 不会二次执行或回退状态;
        attempts 不回退也不增加。返回被收回的任务(PENDING/CANCELLED)。
        """
        ts = now if now is not None else Job.now()
        reclaimed = self._store.reclaim_leases(
            ts, only_expired=True,
            reason=f"runtime reclaim: lease stale for >{self._stale_lease_seconds}s",
        )
        for job in reclaimed:
            logger.info(
                "reclaim: job=%s leased->%s attempts=%d (stale lease, threshold=%.1fs)",
                job.id, job.state.value, job.attempts, self._stale_lease_seconds,
            )
        return reclaimed

    # ------------------------------------------------------------------
    # 生命周期审计
    # ------------------------------------------------------------------
    def events(self, job_id: str) -> list[JobEvent]:
        """按时间顺序返回单个任务的审计记录(任务不存在返回空列表)。"""
        return self._store.list_events(job_id)

    def events_since(self, event_id: int = 0, limit: int = 200) -> list[JobEvent]:
        """全局增量审计查询, 用于排障时按时间顺序追踪变化。"""
        return self._store.list_events_since(event_id, limit)

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
