"""工作协程池。

每个 worker 循环：注册取消信号 -> 原子领取 -> 检查取消信号（领取竞态兜底）
-> 执行处理器（超时 asyncio 包裹、屏蔽内部异常透出）-> 成功/失败/取消结算。

失败归一化（FailureKind）：
- asyncio.CancelledError / 取消事件触发     -> CANCELLED（结算为取消，不算失败尝试）
- asyncio.TimeoutError（超过任务超时）      -> TIMEOUT
- PermanentJobError                         -> PERMANENT_ERROR（直接死信，不重试）
- 处理器其它 Exception                      -> HANDLER_ERROR（按退避重试）
- 任何 BaseException（如 KeyboardInterrupt）-> 不透出，记 HANDLER_ERROR 后重抛/停机
对外只暴露归一化类型与截断后的原因，内部堆栈不进入响应。
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable

from jobqueue.errors import FailureKind, PermanentJobError
from jobqueue.models import TaskRecord, TaskStatus
from jobqueue.queue import JobQueue

log = logging.getLogger("jobqueue.transition")

TaskHandler = Callable[[str], Awaitable[object] | object]

_MAX_REASON_LEN = 2000


def _normalize_reason(exc: BaseException) -> str:
    """只保留异常类型名与消息文本并截断，绝不透出内部堆栈。"""
    text = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
    if len(text) > _MAX_REASON_LEN:
        text = text[:_MAX_REASON_LEN] + "...(truncated)"
    return text


class WorkerPool:
    def __init__(
        self,
        queue: JobQueue,
        handlers: dict[str, TaskHandler],
        *,
        concurrency: int = 2,
        poll_interval: float = 0.05,
        default_timeout: float = 30.0,
        timeouts: dict[str, float] | None = None,
    ):
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self._queue = queue
        self._handlers = dict(handlers)
        self._concurrency = concurrency
        self._poll_interval = poll_interval
        self._default_timeout = default_timeout
        if default_timeout <= 0:
            raise ValueError("default_timeout must be > 0")
        self._timeouts = dict(timeouts or {})
        self._tasks: list[asyncio.Task] = []
        self._stopping = asyncio.Event()
        self._active = 0

    @property
    def active_count(self) -> int:
        """当前正在执行（未结算）的任务数。"""
        return self._active

    # ---------- 生命周期 ----------
    async def start(self) -> None:
        if self._tasks:
            return
        self._stopping.clear()
        for i in range(self._concurrency):
            name = f"jobqueue-worker-{i}-{id(self) & 0xFFFF:04x}"
            self._tasks.append(asyncio.create_task(self._run(i), name=name))
        log.info("worker pool started concurrency=%d", self._concurrency)

    async def stop(self) -> None:
        """优雅停止：停止领取，等待在执行任务到达下一个取消/超时检查点。"""
        self._stopping.set()
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t
        self._tasks.clear()
        log.info("worker pool stopped")

    async def wait_idle(self, timeout: float = 5.0) -> None:
        """测试辅助：等待无活动 worker 且没有立即可领取的 PENDING 任务。

        注意：处于退避延迟中的任务仍算 PENDING 但不可领取，调用方如需等待
        最终终态，应直接轮询具体任务状态。
        """
        import time as _time

        deadline = _time.monotonic() + timeout
        while _time.monotonic() < deadline:
            if self._active == 0:
                return
            await asyncio.sleep(0.01)
        raise asyncio.TimeoutError("workers did not become idle in time")

    # ---------- worker 主循环 ----------
    async def _run(self, index: int) -> None:
        runner_id = f"w{index}-{id(self) & 0xFFFF:04x}"
        try:
            while not self._stopping.is_set():
                try:
                    rec = await self._poll_one(runner_id)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # 领取阶段任何异常都不应杀死 worker
                    log.exception("worker=%s claim loop error; backing off",
                                  runner_id)
                    await asyncio.sleep(self._poll_interval)
                    continue
                if rec is None:
                    try:
                        await asyncio.wait_for(
                            self._stopping.wait(), timeout=self._poll_interval
                        )
                    except asyncio.TimeoutError:
                        pass
                    continue
                # 执行阶段异常必须完全在本任务内消化
                await self._execute(runner_id, rec)
        except asyncio.CancelledError:
            if not self._stopping.is_set():
                log.warning("worker=%s cancelled externally", runner_id)
            raise

    async def _poll_one(self, runner_id: str) -> TaskRecord | None:
        rec = await self._queue.claim(runner_id)
        return rec

    async def _execute(self, runner_id: str, rec: TaskRecord) -> None:
        task_id = rec.id
        self._active += 1
        cancel_event = self._queue.register_run(task_id)
        try:
            # 领取竞态兜底：取消信号在领取后、执行前到达时，直接结算取消
            fresh = await self._queue.get_task(task_id)
            if fresh.status is TaskStatus.CANCELLING:
                await self._queue.cancel_ack(task_id)
                return
            handler = self._handlers.get(rec.task_type)
            if handler is None:
                # 未注册处理器视为永久失败（可重放后由正确版本处理）
                log.error(
                    "task=%s no handler registered type=%s "
                    "reason=PERMANENT_ERROR",
                    task_id, rec.task_type,
                )
                await self._queue.fail(
                    task_id,
                    kind=FailureKind.PERMANENT_ERROR.value,
                    reason=f"no handler registered for task_type={rec.task_type}",
                    permanent=True,
                )
                return
            timeout = self._timeouts.get(rec.task_type, self._default_timeout)
            result = await self._invoke(handler, rec, timeout, cancel_event)
            settled = await self._queue.succeed(task_id, result)
            if settled.status is not TaskStatus.SUCCEEDED:
                log.info(
                    "task=%s handler finished but settled=%s "
                    "reason=cancel_race_lost",
                    task_id, settled.status.value,
                )
        except asyncio.CancelledError:
            # 停机中断：让崩溃恢复语义在重启时接管（不做不可靠的现场结算）
            log.warning(
                "task=%s execution interrupted by shutdown; "
                "crash recovery will requeue it on next start",
                task_id,
            )
            raise
        except _CancelledByRequest:
            settled = await self._queue.cancel_ack(task_id)
            log.info(
                "task=%s execution interrupted by cancel request; settled=%s "
                "kind=CANCELLED",
                task_id, settled.status.value,
            )
        except _TimedOut:
            await self._queue.fail(
                task_id,
                kind=FailureKind.TIMEOUT.value,
                reason="execution exceeded configured timeout",
            )
        except PermanentJobError as exc:
            await self._queue.fail(
                task_id,
                kind=FailureKind.PERMANENT_ERROR.value,
                reason=_normalize_reason(exc),
                permanent=True,
            )
        except Exception as exc:  # 处理器一切其它异常归一化
            await self._queue.fail(
                task_id,
                kind=FailureKind.HANDLER_ERROR.value,
                reason=_normalize_reason(exc),
            )
        finally:
            self._queue.unregister_run(task_id)
            self._active -= 1

    async def _invoke(
        self,
        handler: TaskHandler,
        rec: TaskRecord,
        timeout: float,
        cancel_event: asyncio.Event,
    ) -> str | None:
        """执行处理器：支持同步/异步；超时中断；取消事件协作中断。

        返回 JSON 字符串结果或 None。超时/取消抛出归一化内部信号异常。
        """
        task_id = rec.id
        handler_task: asyncio.Future | None
        try:
            maybe = handler(rec.payload)
            if asyncio.iscoroutine(maybe):
                handler_task = asyncio.ensure_future(maybe)
            else:
                handler_task = None
                result = maybe
        except PermanentJobError:
            raise
        except Exception:
            raise  # 同步抛出归一化由 _execute 处理

        if handler_task is None:
            log.info(
                "task=%s sync handler completed type=%s attempt=%d",
                task_id, rec.task_type, rec.attempts,
            )
            return _stringify(result)

        cancel_wait = asyncio.ensure_future(cancel_event.wait())
        try:
            log.info(
                "task=%s invoking type=%s attempt=%d timeout=%.2f",
                task_id, rec.task_type, rec.attempts, timeout,
            )
            done, _ = await asyncio.wait(
                {handler_task, cancel_wait},
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if handler_task in done:
                if handler_task.exception() is not None:
                    raise handler_task.exception()
                return _stringify(handler_task.result())
            if cancel_event.is_set():
                handler_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await handler_task
                raise _CancelledByRequest()
            # asyncio.wait 超时（二者均未完成）
            handler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await handler_task
            log.warning(
                "task=%s execution timed out after %.2fs kind=TIMEOUT",
                task_id, timeout,
            )
            raise _TimedOut()
        finally:
            cancel_wait.cancel()


class _CancelledByRequest(Exception):
    """内部信号：任务收到取消请求被中断（不对外透出）。"""


class _TimedOut(Exception):
    """内部信号：执行超时（不对外透出）。"""


def _stringify(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    import json

    return json.dumps(value, ensure_ascii=False, default=str)
