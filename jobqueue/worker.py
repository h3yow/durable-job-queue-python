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
        reclaim_interval: float = 5.0,
        enable_reclaimer: bool = True,
    ) -> None:
        self._queue = queue
        self._handler = handler
        self._concurrency = concurrency
        self._execute_timeout = execute_timeout
        self._poll_interval = poll_interval
        self._reclaim_interval = reclaim_interval
        self._enable_reclaimer = enable_reclaimer
        self._tasks: list[asyncio.Task] = []
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        self._stopped.clear()
        self._tasks = [
            asyncio.create_task(self._run_loop(i), name=f"worker-{i}")
            for i in range(self._concurrency)
        ]
        if self._enable_reclaimer:
            self._tasks.append(
                asyncio.create_task(self._reclaim_loop(), name="lease-reclaimer")
            )

    async def stop(self) -> None:
        self._stopped.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    # ------------------------------------------------------------------
    async def _reclaim_loop(self) -> None:
        """周期性发现长时间无进展的执行中任务并安全收回。

        判定与迁移都在存储层带条件完成, 因此与工作协程、重复扫描
        并发也是安全的: 旧持有者的晚到写入会被租约前置条件拒绝。
        """
        while not self._stopped.is_set():
            try:
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=self._reclaim_interval
                )
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                pass
            if self._stopped.is_set():
                break
            try:
                reclaimed = await asyncio.to_thread(self._queue.reclaim_stale)
                if reclaimed:
                    logger.info(
                        "reclaimer: reclaimed %d stale leased jobs", len(reclaimed)
                    )
            except asyncio.CancelledError:
                raise
            except Exception:  # 扫描异常不拖垮服务, 内部细节不外泄
                logger.exception("reclaimer: scan failed, will retry next interval")

    async def _run_loop(self, worker_id: int) -> None:
        while not self._stopped.is_set():
            job = await asyncio.to_thread(
                self._queue.store.claim_next, Job.now(), self._queue.lease_seconds
            )
            if job is None:
                await asyncio.sleep(self._poll_interval)
                continue
            await self._execute(worker_id, job)

    async def _execute(self, worker_id: int, job: Job) -> None:
        assert job.lease_token is not None
        token = job.lease_token
        logger.info(
            "execute: job=%s worker=%d pending->leased attempt=%d/%d",
            job.id, worker_id, job.attempts, job.max_attempts,
        )
        try:
            result = await asyncio.wait_for(
                self._handler(job.payload), timeout=self._execute_timeout
            )
        except asyncio.CancelledError:
            raise  # 池停止, 交由(启动)恢复/失联扫描回收
        except asyncio.TimeoutError:
            await asyncio.to_thread(
                self._settle_failure, job, FailureKind.HANDLER_TIMEOUT,
                f"execution exceeded {self._execute_timeout}s",
            )
            return
        except Exception as exc:  # 归一化业务异常, 不透出内部细节
            await asyncio.to_thread(
                self._settle_failure, job, FailureKind.HANDLER_ERROR,
                f"{type(exc).__name__}: {exc}",
            )
            return

        # 执行结束(成功边界): 取消一旦被受理就必须收口为 CANCELLED,
        # 存储层 complete 的 cancel_requested=0 守卫保证取消不被成功覆盖。
        current = await asyncio.to_thread(self._queue.store.get_job, job.id)
        if current is not None and current.cancel_requested:
            ok = await asyncio.to_thread(
                self._queue.store.complete_cancel, job.id, token
            )
            logger.info(
                "execute: job=%s leased->cancelled (cancel_requested) persisted=%s",
                job.id, ok,
            )
            return
        if await asyncio.to_thread(self._queue.store.complete, job.id, token, result):
            logger.info("execute: job=%s leased->succeeded", job.id)
        else:
            # 可能在 get 与 complete 之间取消恰好到达, 或租约已被收回:
            # 状态以存储为准, 若取消已受理则补落 CANCELLED, 绝不回退。
            current = await asyncio.to_thread(self._queue.store.get_job, job.id)
            if (current is not None and current.state == JobState.LEASED
                    and current.cancel_requested):
                ok = await asyncio.to_thread(
                    self._queue.store.complete_cancel, job.id, token
                )
                logger.info(
                    "execute: job=%s leased->cancelled (cancel at boundary) persisted=%s",
                    job.id, ok,
                )
            else:
                logger.info(
                    "execute: job=%s complete rejected (lease lost/cancelled), "
                    "状态以存储为准", job.id,
                )

    def _settle_failure(
        self, job: Job, kind: FailureKind, reason: str
    ) -> None:
        """失败/超时收口: 取消已受理时任何结果都不得重排或成功。

        顺序为"先查取消标记, 再条件落库":
        - 标记已存在 -> 落 CANCELLED(成功/异常/超时都不能改写它);
        - fail() 自身仍带 cancel_requested=0 守卫, 挡住查询后才到达的
          取消; fail 被拒时补落 CANCELLED, 保证取消最终一致。
        """
        current = self._queue.store.get_job(job.id)
        if current is not None and current.cancel_requested:
            ok = self._queue.store.complete_cancel(job.id, job.lease_token)
            logger.info(
                "execute: job=%s leased->cancelled kind=%s before-fail persisted=%s",
                job.id, kind.value, ok,
            )
            return

        record = FailureRecord(
            attempt=job.attempts, kind=kind, reason=reason, failed_at=Job.now()
        )
        job.failure_history.append(record)
        job.last_failure = record
        job.updated_at = Job.now()
        if job.attempts >= job.max_attempts:
            job.state = JobState.DEAD
            job.run_at = 0.0
            ok = self._queue.store.fail(job)
            logger.info(
                "execute: job=%s leased->dead kind=%s reason=%r attempts=%d persisted=%s",
                job.id, kind.value, reason, job.attempts, ok,
            )
        else:
            delay = self._queue.backoff.delay_for(job.attempts)
            job.state = JobState.PENDING
            job.run_at = Job.now() + delay
            ok = self._queue.store.fail(job)
            logger.info(
                "execute: job=%s leased->pending(retry) kind=%s reason=%r "
                "attempt=%d delay=%.3fs persisted=%s",
                job.id, kind.value, reason, job.attempts, delay, ok,
            )

        if not ok:
            # fail 被拒: 取消恰好在落库前受理, 或租约已被自愈收回。
            current = self._queue.store.get_job(job.id)
            if (current is not None and current.state == JobState.LEASED
                    and current.cancel_requested):
                cancelled = self._queue.store.complete_cancel(
                    job.id, job.lease_token
                )
                logger.info(
                    "execute: job=%s leased->cancelled kind=%s after-reject persisted=%s",
                    job.id, kind.value, cancelled,
                )
            else:
                logger.info(
                    "execute: job=%s fail rejected (lease reclaimed), 状态以存储为准",
                    job.id,
                )
