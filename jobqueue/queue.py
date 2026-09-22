"""任务队列门面。

职责：
- 提交（幂等去重 + 载荷冲突拒绝 + 入口背压）、查询、取消、死信重放；
- 供 WorkerPool 调用的领取/成功/失败/取消协作接口；
- 启动时一次性恢复（RUNNING 孤儿重排、CANCELLING 落终态）。

并发模型：存储调用通过 asyncio.to_thread 在线程中执行；进程内取消信号
用 asyncio.Event/Future 传递，存储落盘与信号注册的顺序由本类保证不丢信号。
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass

from jobqueue.errors import (
    IdempotencyMismatchError,
    InternalJobQueueError,
    InvalidTaskTypeError,
    QueueFullError,
    TaskNotDeadError,
    TaskNotFoundError,
)
from jobqueue.models import TaskRecord, TaskStatus, utcnow
from jobqueue.retry import BackoffPolicy
from jobqueue.storage import SQLiteStorage, StorageError

log = logging.getLogger("jobqueue.transition")


@dataclass(frozen=True)
class QueueOptions:
    max_attempts: int = 3
    capacity: int = 1000              # 排队积压上限（PENDING 数量）
    overflow_policy: str = "REJECT"   # REJECT：立即拒绝；WAIT：等待空位
    wait_timeout: float = 5.0         # WAIT 策略最长等待秒数

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.capacity < 1:
            raise ValueError("capacity must be >= 1")
        if self.overflow_policy not in ("REJECT", "WAIT"):
            raise ValueError("overflow_policy must be REJECT or WAIT")
        if self.wait_timeout < 0:
            raise ValueError("wait_timeout must be >= 0")


class JobQueue:
    def __init__(
        self,
        storage: SQLiteStorage,
        *,
        options: QueueOptions | None = None,
        backoff: BackoffPolicy | None = None,
        allowed_task_types: frozenset[str] | None = None,
    ):
        self._storage = storage
        self._options = options or QueueOptions()
        self._backoff = backoff or BackoffPolicy()
        self._allowed_task_types = allowed_task_types
        # 串行化提交路径（幂等判别 + 背压判定 + 插入）
        self._submit_lock = asyncio.Lock()
        self._closed = False
        # 执行中任务的取消信号：task_id -> Event（worker 在领取前注册）
        self._cancel_events: dict[str, asyncio.Event] = {}
        # WAIT 策略下的空位等待者
        self._space_waiters: set[asyncio.Event] = set()

    # ---------- 生命周期 ----------
    async def initialize(self) -> None:
        await asyncio.to_thread(self._storage.initialize)
        recovered = await asyncio.to_thread(
            self._storage._recover_orphans, time.time()
        )
        if recovered:
            log.warning("startup recovery requeued %d in-flight task(s)",
                        len(recovered))
            self._notify_space()

    async def aclose(self) -> None:
        self._closed = True
        await asyncio.to_thread(self._storage.close)

    # ---------- 内部工具 ----------
    def _check_open(self) -> None:
        if self._closed:
            raise InternalJobQueueError("queue is closed")

    def _notify_space(self) -> None:
        waiters = list(self._space_waiters)
        self._space_waiters.clear()
        for ev in waiters:
            ev.set()

    @staticmethod
    def _not_found(task_id: str) -> TaskNotFoundError:
        return TaskNotFoundError(f"task not found: {task_id}", task_id=task_id)

    # ---------- 提交 / 查询 ----------
    async def submit(
        self,
        task_type: str,
        payload: str,
        *,
        idempotency_key: str | None = None,
        max_attempts: int | None = None,
    ) -> TaskRecord:
        """提交任务。

        - 相同 idempotency_key 且 (task_type, payload) 一致：返回同一任务，
          并发下也不会产生第二条记录/重复执行；
        - 相同 idempotency_key 但载荷不同：抛 IdempotencyMismatchError；
        - 积压达 capacity：REJECT 立即拒绝（QUEUE_FULL_REJECT），
          WAIT 等待空位，超时拒绝（QUEUE_FULL_WAIT_TIMEOUT）。
        """
        self._check_open()
        if (
            self._allowed_task_types is not None
            and task_type not in self._allowed_task_types
        ):
            raise InvalidTaskTypeError(f"unknown task type: {task_type}")
        attempts_cap = max_attempts or self._options.max_attempts
        if attempts_cap < 1:
            raise ValueError("max_attempts must be >= 1")

        async with self._submit_lock:
            # 1) 幂等判别
            if idempotency_key is not None:
                existing = await asyncio.to_thread(
                    self._storage._get_by_idempotency_key, idempotency_key
                )
                if existing is not None:
                    same = (
                        existing.task_type == task_type
                        and existing.payload == payload
                    )
                    if not same:
                        log.info(
                            "task=%s submit rejected idempotency_key=%s "
                            "reason=IDEMPOTENCY_PAYLOAD_MISMATCH "
                            "existing_type=%s submitted_type=%s",
                            existing.id, idempotency_key,
                            existing.task_type, task_type,
                        )
                        raise IdempotencyMismatchError(
                            "idempotency key reused with different payload",
                            task_id=existing.id,
                        )
                    log.info(
                        "task=%s idempotent hit key=%s status=%s "
                        "reason=deduplicated_return_existing",
                        existing.id, idempotency_key, existing.status.value,
                    )
                    return existing

            # 2) 背压
            await self._acquire_slot()

            # 3) 插入（唯一索引兜底并发竞态；理论上 submit_lock 已串行）
            now = utcnow().isoformat()
            rec = TaskRecord(
                id=uuid.uuid4().hex,
                task_type=task_type,
                payload=payload,
                status=TaskStatus.PENDING,
                idempotency_key=idempotency_key,
                attempts=0,
                max_attempts=attempts_cap,
                visible_at=time.time(),
                created_at=now,
                updated_at=now,
            )
            try:
                await asyncio.to_thread(self._storage._insert_task, rec)
            except StorageError:
                # 唯一索引冲突的极端竞态：重读判定幂等命中还是载荷冲突
                if idempotency_key is not None:
                    existing = await asyncio.to_thread(
                        self._storage._get_by_idempotency_key, idempotency_key
                    )
                    if existing is not None:
                        if (
                            existing.task_type == task_type
                            and existing.payload == payload
                        ):
                            return existing
                        raise IdempotencyMismatchError(
                            "idempotency key reused with different payload",
                            task_id=existing.id,
                        ) from None
                raise InternalJobQueueError("failed to persist task") from None
            log.info(
                "task=%s created type=%s idempotency_key=%s max_attempts=%d "
                "reason=submit_accepted",
                rec.id, task_type, idempotency_key, attempts_cap,
            )
            return rec

    async def _acquire_slot(self) -> None:
        opts = self._options
        backlog = await asyncio.to_thread(self._storage._count_pending)
        if backlog < opts.capacity:
            return
        if opts.overflow_policy == "REJECT":
            log.warning(
                "submit rejected reason=QUEUE_FULL_REJECT backlog=%d capacity=%d",
                backlog, opts.capacity,
            )
            raise QueueFullError(
                f"queue full: backlog={backlog} capacity={opts.capacity}",
                reason="QUEUE_FULL_REJECT",
                backlog=backlog, capacity=opts.capacity,
            )
        # WAIT：单空位信号唤醒；被唤醒后重新计数判定
        deadline = time.monotonic() + opts.wait_timeout
        while True:
            ev = asyncio.Event()
            self._space_waiters.add(ev)
            try:
                # 入队后先复查，避免与"空位产生"发生信号丢失竞态
                backlog = await asyncio.to_thread(self._storage._count_pending)
                if backlog < opts.capacity:
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    log.warning(
                        "submit rejected reason=QUEUE_FULL_WAIT_TIMEOUT "
                        "backlog=%d capacity=%d",
                        backlog, opts.capacity,
                    )
                    raise QueueFullError(
                        "queue full: wait timed out",
                        reason="QUEUE_FULL_WAIT_TIMEOUT",
                        backlog=backlog, capacity=opts.capacity,
                    )
                try:
                    await asyncio.wait_for(ev.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    continue  # 回到循环统一判定并给出超时拒绝
            finally:
                self._space_waiters.discard(ev)

    async def get_task(self, task_id: str) -> TaskRecord:
        rec = await asyncio.to_thread(self._storage._get, task_id)
        if rec is None:
            raise self._not_found(task_id)
        return rec

    async def find_by_idempotency_key(self, key: str) -> TaskRecord | None:
        return await asyncio.to_thread(self._storage._get_by_idempotency_key, key)

    # ---------- 取消 ----------
    async def request_cancel(self, task_id: str) -> TaskRecord:
        """请求取消。两种语义明确区分：

        - 任务排队中（PENDING）：原子落盘 PENDING->CANCELLED，取消立即生效，
          永远不会被执行；
        - 任务已被领取（RUNNING）：落盘 RUNNING->CANCELLING（协作取消信号），
          由持有者在执行点检查并中断，最终流转 CANCELLING->CANCELLED；
        - CANCELLING/CANCELLED 及一切终态：重复取消幂等返回当前记录，
          不报错、不改变已定结果；
        - SUCCEEDED/DEAD：返回当前记录（结果不可变）。
        """
        self._check_open()
        rec = await self.get_task(task_id)
        if rec.status in (
            TaskStatus.CANCELLED,
            TaskStatus.CANCELLING,
            TaskStatus.SUCCEEDED,
            TaskStatus.DEAD,
        ):
            log.info(
                "task=%s cancel no-op status=%s reason=already_settled",
                task_id, rec.status.value,
            )
            return rec

        if rec.status is TaskStatus.PENDING:
            updated = await asyncio.to_thread(
                self._storage._transition,
                task_id, (TaskStatus.PENDING,), TaskStatus.CANCELLED,
            )
            if updated is None:
                # 竞态：恰好在 PENDING->RUNNING 之间被领取，按 RUNNING 路径重来
                return await self._cancel_running(task_id)
            log.info(
                "task=%s transition PENDING->CANCELLED reason=cancel_queued "
                "semantics=never_executed",
                task_id,
            )
            self._notify_space()
            return updated

        if rec.status is TaskStatus.RUNNING:
            return await self._cancel_running(task_id)

        raise InternalJobQueueError(f"unexpected status: {rec.status}")

    async def _cancel_running(self, task_id: str) -> TaskRecord:
        updated = await asyncio.to_thread(
            self._storage._transition,
            task_id, (TaskStatus.RUNNING,), TaskStatus.CANCELLING,
        )
        if updated is None:
            # 已被 worker 流转：返回最新状态（终态则 no-op 语义）
            latest = await self.get_task(task_id)
            log.info(
                "task=%s cancel race resolved status=%s "
                "reason=worker_finished_first",
                task_id, latest.status.value,
            )
            return latest
        # 通知持有者（若信号已注册）；未注册的竞态由 worker 领取后补判
        ev = self._cancel_events.get(task_id)
        if ev is not None:
            ev.set()
        log.info(
            "task=%s transition RUNNING->CANCELLING reason=cancel_claimed "
            "semantics=cooperative_interrupt signal_delivered=%s",
            task_id, ev is not None,
        )
        return updated

    # ---------- 死信 ----------
    async def replay_dead(self, task_id: str) -> TaskRecord:
        """人工重放死信：仅 DEAD 可重放；失败历史原样保留，attempts 归零。"""
        self._check_open()
        rec = await self.get_task(task_id)
        if rec.status is not TaskStatus.DEAD:
            raise TaskNotDeadError(
                f"task is not DEAD: {rec.status.value}", task_id=task_id
            )
        updated = await asyncio.to_thread(
            self._storage._replay_dead, task_id, time.time()
        )
        if updated is None:  # 并发重放竞态：重读给出确定结果
            return await self.get_task(task_id)
        log.info(
            "task=%s replay accepted preserved_failures=%d last_failure=%s:%s",
            task_id, len(updated.failures),
            updated.last_failure_kind, updated.last_failure_reason,
        )
        return updated

    async def list_dead(
        self, limit: int = 100, offset: int = 0
    ) -> list[TaskRecord]:
        return await asyncio.to_thread(self._storage._list_dead, limit, offset)

    # ---------- Worker 接口 ----------
    def register_run(self, task_id: str) -> asyncio.Event:
        """worker 领取前注册取消信号；若已处于 CANCELLING 则信号预置。"""
        ev = asyncio.Event()
        self._cancel_events[task_id] = ev
        return ev

    def unregister_run(self, task_id: str) -> None:
        self._cancel_events.pop(task_id, None)

    async def claim(self, runner_id: str) -> TaskRecord | None:
        self._check_open()
        rec = await asyncio.to_thread(
            self._storage._claim_next, time.time(), runner_id
        )
        if rec is not None:
            self._notify_space()  # PENDING->RUNNING 释放一个积压名额
        return rec

    async def succeed(self, task_id: str, result: str | None) -> TaskRecord:
        updated = await asyncio.to_thread(
            self._storage._transition,
            task_id, (TaskStatus.RUNNING,), TaskStatus.SUCCEEDED,
            result=result, runner_id=None,
        )
        if updated is None:
            # 取消先落盘或已被并发处理：返回当前状态，调用方据此判定
            return await self.get_task(task_id)
        log.info("task=%s transition RUNNING->SUCCEEDED reason=handler_done",
                 task_id)
        return updated

    async def fail(
        self,
        task_id: str,
        *,
        kind: str,
        reason: str,
        permanent: bool = False,
    ) -> TaskRecord:
        """执行失败归一化入口：追加失败历史，按预算重排或进死信。

        CANCELLING 状态优先结算为 CANCELLED（取消语义不被失败覆盖）。
        """
        rec = await self.get_task(task_id)
        if rec.status is TaskStatus.CANCELLING:
            return await self.cancel_ack(task_id)
        if rec.status is not TaskStatus.RUNNING:
            # 终态已定（例如并发结算），不覆盖
            log.info(
                "task=%s fail ignored status=%s reason=already_settled",
                task_id, rec.status.value,
            )
            return rec
        delay = self._backoff.delay_for(rec.attempts)
        failure = {
            "kind": kind,
            "reason": reason,
            "attempt": rec.attempts,
            "at": utcnow().isoformat(),
        }
        updated, dead = await asyncio.to_thread(
            self._storage._requeue_or_dead,
            task_id,
            now=time.time(),
            delay=delay,
            failure=failure,
            force_dead=permanent,
        )
        if not dead:
            self._notify_space()  # 重排任务回到 PENDING 名额视角（计数含延迟任务）
        return updated

    async def cancel_ack(self, task_id: str) -> TaskRecord:
        """持有者确认取消：CANCELLING->CANCELLED。"""
        updated = await asyncio.to_thread(
            self._storage._transition,
            task_id, (TaskStatus.CANCELLING,), TaskStatus.CANCELLED,
            runner_id=None,
        )
        if updated is None:
            return await self.get_task(task_id)
        log.info(
            "task=%s transition CANCELLING->CANCELLED reason=cancel_acknowledged",
            task_id,
        )
        return updated
