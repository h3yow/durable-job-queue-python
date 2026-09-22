"""共享 fixture：每个测试独立临时 DB + 快速确定性退避的 JobQueue。"""

from __future__ import annotations

import os
import tempfile
from collections.abc import AsyncIterator, Iterator

import pytest

from jobqueue.queue import JobQueue, QueueOptions
from jobqueue.retry import BackoffPolicy
from jobqueue.storage import SQLiteStorage
from jobqueue.worker import WorkerPool


@pytest.fixture
def db_path() -> Iterator[str]:
    import contextlib

    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)  # 交给 SQLiteStorage 创建
    yield path
    for suffix in ("", "-wal", "-shm"):
        p = path + suffix
        if os.path.exists(p):
            with contextlib.suppress(OSError):
                os.unlink(p)


@pytest.fixture
def storage(db_path: str) -> Iterator[SQLiteStorage]:
    s = SQLiteStorage(db_path)
    s.initialize()
    yield s
    s.close()


@pytest.fixture
def fast_backoff() -> BackoffPolicy:
    return BackoffPolicy(base_delay=0.02, factor=2.0, max_delay=0.1, jitter=0.0)


@pytest.fixture
async def queue(
    storage: SQLiteStorage, fast_backoff: BackoffPolicy
) -> AsyncIterator[JobQueue]:
    q = JobQueue(
        storage,
        options=QueueOptions(max_attempts=3, capacity=1000),
        backoff=fast_backoff,
    )
    await q.initialize()
    yield q
    await q.aclose()


@pytest.fixture
async def pool_factory(queue: JobQueue) -> AsyncIterator:
    pools: list[WorkerPool] = []

    def _make(handlers: dict, **kwargs) -> WorkerPool:
        kwargs.setdefault("concurrency", 2)
        kwargs.setdefault("poll_interval", 0.01)
        kwargs.setdefault("default_timeout", 1.0)
        pool = WorkerPool(queue, handlers, **kwargs)
        pools.append(pool)
        return pool

    yield _make

    for pool in pools:
        await pool.stop()


async def wait_status(
    queue: JobQueue, task_id: str, statuses, timeout: float = 5.0, poll: float = 0.01
):
    """轮询任务直到进入指定状态集合；返回最终记录。超时则断言失败。"""
    import asyncio
    import time

    if isinstance(statuses, str):
        statuses = {statuses}
    else:
        statuses = set(statuses)
    deadline = time.monotonic() + timeout
    while True:
        rec = await queue.get_task(task_id)
        if rec.status in statuses:
            return rec
        if time.monotonic() > deadline:
            raise AssertionError(
                f"task {task_id} did not reach {statuses}; "
                f"last status={rec.status.value}"
            )
        await asyncio.sleep(poll)


@pytest.fixture
def wait_terminal():
    from jobqueue.models import TERMINAL_STATUSES

    async def _wait(queue: JobQueue, task_id: str, timeout: float = 5.0):
        return await wait_status(queue, task_id, TERMINAL_STATUSES, timeout=timeout)

    return _wait
