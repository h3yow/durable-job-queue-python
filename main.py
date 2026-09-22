"""本地异步任务队列 HTTP 入口。

状态机/重试/恢复语义见 docs/task-queue.md。所有对外错误均归一化为：
    {"error": {"reason": <机器可读原因>, "message": <说明>, "task_id": ...}}
内部异常只返回 INTERNAL_ERROR，不携带堆栈或内部细节。
"""

from __future__ import annotations

import contextlib
import os
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from jobqueue.errors import (
    InternalJobQueueError,
    JobQueueError,
    QueueFullError,
    TaskNotFoundError,
)
from jobqueue.models import TaskStatus
from jobqueue.queue import JobQueue, QueueOptions
from jobqueue.retry import BackoffPolicy
from jobqueue.storage import SQLiteStorage
from jobqueue.worker import WorkerPool

DB_PATH = os.environ.get("JOBQUEUE_DB", "jobqueue.db")
CAPACITY = int(os.environ.get("JOBQUEUE_CAPACITY", "1000"))
MAX_ATTEMPTS = int(os.environ.get("JOBQUEUE_MAX_ATTEMPTS", "3"))
CONCURRENCY = int(os.environ.get("JOBQUEUE_CONCURRENCY", "2"))

# 任务处理器注册表：业务侧在此登记 task_type -> handler（支持同步/异步）
TASK_HANDLERS: dict[str, Any] = {}

app = FastAPI(title="Durable Local Job Queue", version="0.1.0")

_state: dict[str, Any] = {}


@app.on_event("startup")
async def _startup() -> None:
    storage = SQLiteStorage(DB_PATH)
    # 空注册表时给出空集合（而非 None）：未配置处理器时提交一律 fail-fast，
    # 避免任务被接受后永远 PENDING；需要放开校验可直接构造 JobQueue(allowed_task_types=None)
    allowed = (
        frozenset(TASK_HANDLERS) if TASK_HANDLERS else frozenset()
    )
    queue = JobQueue(
        storage,
        options=QueueOptions(capacity=CAPACITY, max_attempts=MAX_ATTEMPTS),
        backoff=BackoffPolicy(),
        allowed_task_types=allowed,
    )
    await queue.initialize()  # 内部执行一次性崩溃恢复
    pool = WorkerPool(queue, TASK_HANDLERS, concurrency=CONCURRENCY)
    if TASK_HANDLERS:
        await pool.start()
    _state["queue"] = queue
    _state["pool"] = pool
    _state["storage"] = storage


@app.on_event("shutdown")
async def _shutdown() -> None:
    pool = _state.get("pool")
    if pool is not None:
        with contextlib.suppress(Exception):
            await pool.stop()
    queue = _state.get("queue")
    if queue is not None:
        with contextlib.suppress(Exception):
            await queue.aclose()
    _state.clear()


def get_queue() -> JobQueue:
    queue = _state.get("queue")
    if queue is None:  # 测试场景下手动构造 App 尚未 startup
        raise InternalJobQueueError("queue not initialized")
    return queue


# ---------- 错误归一化 ----------
@app.exception_handler(JobQueueError)
async def _jobqueue_error_handler(_: Request, exc: JobQueueError) -> JSONResponse:
    status_map = {
        "TASK_NOT_FOUND": 404,
        "IDEMPOTENCY_PAYLOAD_MISMATCH": 409,
        "QUEUE_FULL_REJECT": 503,
        "QUEUE_FULL_WAIT_TIMEOUT": 503,
        "TASK_NOT_DEAD": 409,
        "UNKNOWN_TASK_TYPE": 400,
        "INTERNAL_ERROR": 500,
        "PERMANENT": 400,
    }
    detail: dict[str, Any] = {
        "reason": getattr(exc, "reason", "JOB_QUEUE_ERROR"),
        "message": str(exc),
    }
    if exc.task_id:
        detail["task_id"] = exc.task_id
    if isinstance(exc, QueueFullError):
        detail["backlog"] = exc.backlog
        detail["capacity"] = exc.capacity
    return JSONResponse(status_code=status_map.get(detail["reason"], 400),
                        content={"error": detail})


@app.exception_handler(Exception)
async def _unexpected_error_handler(_: Request, exc: Exception) -> JSONResponse:
    # 兜底：内部异常不透出，只给稳定的机器可读原因
    return JSONResponse(
        status_code=500,
        content={"error": {
            "reason": "INTERNAL_ERROR",
            "message": "internal server error",
        }},
    )


# ---------- 请求/响应模型（内联，保持单文件入口） ----------
from pydantic import BaseModel, Field  # noqa: E402


class SubmitRequest(BaseModel):
    task_type: str = Field(..., min_length=1)
    payload: str = ""
    idempotency_key: str | None = None
    max_attempts: int | None = Field(default=None, ge=1)


class CancelResponse(BaseModel):
    task_id: str
    status: str
    semantics: str  # cancelled_while_queued / cancel_requested / already_settled


class ReplayResponse(BaseModel):
    task_id: str
    status: str
    preserved_failures: int


@app.post("/tasks")
async def submit_task(req: SubmitRequest) -> JSONResponse:
    q = get_queue()
    rec = await q.submit(
        req.task_type,
        req.payload,
        idempotency_key=req.idempotency_key,
        max_attempts=req.max_attempts,
    )
    return JSONResponse(status_code=201, content=rec.to_dict())


@app.get("/tasks/{task_id}")
async def get_task(task_id: str) -> dict:
    return (await get_queue().get_task(task_id)).to_dict()


@app.get("/tasks/idempotency/{key}")
async def get_by_idempotency(key: str) -> dict:
    rec = await get_queue().find_by_idempotency_key(key)
    if rec is None:
        raise TaskNotFoundError(
            f"no task with idempotency key: {key}"
        )
    return rec.to_dict()


@app.post("/tasks/{task_id}/cancel", response_model=CancelResponse)
async def cancel_task(task_id: str) -> CancelResponse:
    before = await get_queue().get_task(task_id)
    after = await get_queue().request_cancel(task_id)
    if before.status is TaskStatus.PENDING and after.status is TaskStatus.CANCELLED:
        semantics = "cancelled_while_queued"
    elif after.status is TaskStatus.CANCELLING:
        semantics = "cancel_requested"
    else:
        semantics = "already_settled"
    return CancelResponse(
        task_id=task_id, status=after.status.value, semantics=semantics
    )


@app.get("/dead-letter")
async def list_dead(limit: int = 100, offset: int = 0) -> dict:
    rows = await get_queue().list_dead(limit=limit, offset=offset)
    return {"tasks": [r.to_dict() for r in rows], "count": len(rows)}


@app.post("/tasks/{task_id}/replay", response_model=ReplayResponse)
async def replay_task(task_id: str) -> ReplayResponse:
    rec = await get_queue().replay_dead(task_id)
    return ReplayResponse(
        task_id=task_id,
        status=rec.status.value,
        preserved_failures=len(rec.failures),
    )


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}
