"""工作协程池: 并发领取、执行归一化、超时中断、失败重试与死信。"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

from jobqueue.models import FailureKind, FailureRecord, Job, JobState
from jobqueue.service import JobQueue

logger = logging.getLogger("jobqueue.worker")

Handler = Callable[[dict], Awaitable[Any]]


class WorkerPool:
    """N 个工作协程并发领取任务。

    - 同一任务任意时刻最多被一个协程持有(存储层条件领取保证)。
    - 执行器异常归一化为 FailureKind, 不向外透出内部异常细节。
    - 执行超时通过 asyncio.wait_for 中断并归类为 HANDLER_TIMEOUT。
    - 执行期间被取消 -> CANCELLED 终态; 失败按退避重排, 耗尽进死信。
    """

    def __init__(
        self,
        queue: JobQueue,
        handler: Handler,
        *,
        concurrency: int = 4,
        execute_timeout: float = 10.0,
        poll_interval: float = 0.05,
        reap_interval: float = 1.0,
    ) -> None:
        self._queue = queue
        self._handler = handler
        self._concurrency = concurrency
        self._execute_timeout = execute_timeout
        self._poll_interval = poll_interval
        # 运行期自愈扫描周期: 每隔多久检查一次失联(租约到期)的执行中任务。
        self._reap_interval = reap_interval
        self._tasks: list[asyncio.Task] = []
        self._reaper: asyncio.Task | None = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        self._stopped.clear()
        self._tasks = [
            asyncio.create_task(self._run_loop(i), name=f"worker-{i}")
            for i in range(self._concurrency)
        ]
        self._reaper = asyncio.create_task(self._reap_loop(), name="lease-reaper")

    async def stop(self) -> None:
        self._stopped.set()
        pending = list(self._tasks)
        if self._reaper is not None:
            pending.append(self._reaper)
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        self._tasks = []
        self._reaper = None

    # ------------------------------------------------------------------
    async def _reap_loop(self) -> None:
        """周期性收回失联执行者持有的租约, 服务持续运行时也能自愈。"""
        while not self._stopped.is_set():
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=self._reap_interval)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                pass
            if self._stopped.is_set():
                break
            try:
                reclaimed = await asyncio.to_thread(self._queue.reclaim_stale)
                for job in reclaimed:
                    logger.info(
                        "reaper: job=%s leased->%s attempts=%d (no progress, reclaimed)",
                        job.id, job.state.value, job.attempts,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:  # 扫描异常不拖垮服务, 仅内部记录
                logger.exception("reaper: reclaim scan failed (internal, suppressed)")

    async def _run_loop(self, worker_id: int) -> None:
        while not self._stopped.is_set():
            job = await asyncio.to_thread(
                self._queue.store.claim_next, Job.now(), self._queue.lease_seconds
            )
            if job is None:
                await asyncio.sleep(self._poll_interval)
                continue
            await self._execute(worker_id, job)

    def _settle_cancel(self, job: Job) -> bool:
        """取消收口: 仅当租约仍持有时落 CANCELLED。返回是否由本次调用收口。"""
        if self._queue.store.complete_cancel(job.id, job.lease_token):
            logger.info("execute: job=%s leased->cancelled (cancel requested)", job.id)
            return True
        logger.info(
            "execute: job=%s cancel settle rejected (lease lost), 状态以存储为准",
            job.id,
        )
        return False

    async def _execute(self, worker_id: int, job: Job) -> None:
        assert job.lease_token is not None
        logger.info(
            "execute: job=%s worker=%d pending->leased attempt=%d/%d",
            job.id, worker_id, job.attempts, job.max_attempts,
        )
        try:
            result = await asyncio.wait_for(
                self._handler(job.payload), timeout=self._execute_timeout
            )
        except asyncio.CancelledError:
            raise  # 池停止, 交由恢复流程回收
        except asyncio.TimeoutError:
            self._on_failure(job, FailureKind.HANDLER_TIMEOUT,
                             f"execution exceeded {self._execute_timeout}s")
            return
        except Exception as exc:  # 归一化业务异常, 不透出内部细节
            self._on_failure(job, FailureKind.HANDLER_ERROR,
                             f"{type(exc).__name__}: {exc}")
            return

        # 执行结束后检查取消标记: 无论处理器成功、抛错还是超时, 取消都收口
        # 为 CANCELLED。取消与完成/失败恰好卡在边界时, 由条件 SQL 保证只有
        # 一方生效, 结果不会被改回成功/重试/再次领取。
        current = self._queue.store.get_job(job.id)
        if current is not None and current.cancel_requested:
            self._settle_cancel(job)
            return
        if self._queue.store.complete(job.id, job.lease_token, result):
            logger.info("execute: job=%s leased->succeeded", job.id)
        else:
            # 完成写入被拒(租约已被自愈收回等): 若取消已受理则改走取消收口
            current = self._queue.store.get_job(job.id)
            if current is not None and current.cancel_requested:
                self._settle_cancel(job)
            else:
                logger.info(
                    "execute: job=%s complete rejected (lease lost), 状态以存储为准",
                    job.id,
                )

    def _on_failure(self, job: Job, kind: FailureKind, reason: str) -> None:
        record = FailureRecord(
            attempt=job.attempts, kind=kind, reason=reason, failed_at=Job.now()
        )
        job.failure_history.append(record)
        job.last_failure = record
        job.updated_at = Job.now()
        if job.attempts >= job.max_attempts:
            job.state = JobState.DEAD
            job.run_at = 0.0
        else:
            delay = self._queue.backoff.delay_for(job.attempts)
            job.state = JobState.PENDING
            job.run_at = Job.now() + delay

        ok = self._queue.store.fail(job)
        if not ok:
            # 失败落库被拒: 取消恰好在失败边界到达, 或租约已被自愈收回。
            # 取消已受理时统一收口为 CANCELLED, 绝不改回重试/死信。
            current = self._queue.store.get_job(job.id)
            if current is not None and current.cancel_requested:
                self._settle_cancel(job)
                logger.info(
                    "execute: job=%s failure overridden by cancel -> cancelled "
                    "kind=%s reason=%r", job.id, kind.value, reason,
                )
            else:
                logger.info(
                    "execute: job=%s fail rejected (lease lost), kind=%s 状态以存储为准",
                    job.id, kind.value,
                )
            return

        if job.state == JobState.DEAD:
            logger.info(
                "execute: job=%s leased->dead kind=%s reason=%r attempts=%d persisted=%s",
                job.id, kind.value, reason, job.attempts, ok,
            )
        else:
            logger.info(
                "execute: job=%s leased->pending(retry) kind=%s reason=%r "
                "attempt=%d delay=%.3fs persisted=%s",
                job.id, kind.value, reason, job.attempts,
                self._queue.backoff.delay_for(job.attempts), ok,
            )
