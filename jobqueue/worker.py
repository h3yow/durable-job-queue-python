"""工作协程池: 并发领取、执行归一化、超时中断、失败重试与死信。"""
from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

from jobqueue.service import JobQueue

Handler = Callable[[dict], Awaitable[Any]]


class WorkerPool:
    def __init__(
        self,
        queue: JobQueue,
        handler: Handler,
        *,
        concurrency: int = 4,
        execute_timeout: float = 10.0,
        poll_interval: float = 0.05,
    ) -> None:
        raise NotImplementedError

    async def start(self) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError
