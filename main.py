"""HTTP 入口: 提交 / 查询 / 取消 / 死信查询与重放。

对外只暴露归一化错误(code + message), 内部异常不透出。
"""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from jobqueue import (
    BackoffPolicy,
    JobQueue,
    JobQueueError,
    JobState,
    SQLiteStore,
    WorkerPool,
)
from jobqueue.models import Job

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

DB_PATH = os.environ.get("JOBQUEUE_DB", "jobqueue.db")


def job_view(job: Job) -> dict[str, Any]:
    return {
        "id": job.id,
        "state": job.state.value,
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "cancel_requested": job.cancel_requested,
        "last_failure": (
            {
                "attempt": job.last_failure.attempt,
                "kind": job.last_failure.kind.value,
                "reason": job.last_failure.reason,
            }
            if job.last_failure
            else None
        ),
        "result": job.result if job.state == JobState.SUCCEEDED else None,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }


class SubmitRequest(BaseModel):
    payload: dict[str, Any]
    idempotency_key: str | None = None
    max_attempts: int = 3


async def demo_handler(payload: dict[str, Any]) -> Any:
    await asyncio.sleep(0)
    return {"echo": payload}


@asynccontextmanager
async def lifespan(app: FastAPI):
    store = SQLiteStore(DB_PATH)
    queue = JobQueue(store, capacity=int(os.environ.get("JOBQUEUE_CAPACITY", "1000")),
                     backoff=BackoffPolicy())
    recovered = queue.recover()
    if recovered:
        logger.info("recovered %d leased jobs after restart", len(recovered))
    pool = WorkerPool(queue, demo_handler, concurrency=4)
    await pool.start()
    app.state.queue = queue
    app.state.pool = pool
    try:
        yield
    finally:
        await pool.stop()
        store.close()


app = FastAPI(lifespan=lifespan)


@app.exception_handler(JobQueueError)
async def jobqueue_error_handler(_: Request, exc: JobQueueError) -> JSONResponse:
    status = {
        "queue_full": 429,
        "idempotency_conflict": 409,
        "job_not_found": 404,
        "invalid_state_transition": 409,
    }.get(exc.code, 500)
    return JSONResponse(status_code=status,
                        content={"error": {"code": exc.code, "message": str(exc)}})


@app.exception_handler(Exception)
async def unhandled_error_handler(_: Request, exc: Exception) -> JSONResponse:
    logger.exception("unhandled internal error")
    return JSONResponse(
        status_code=500,
        content={"error": {"code": "internal_error", "message": "internal error"}},
    )


@app.post("/jobs", status_code=201)
def submit(req: SubmitRequest, request: Request) -> dict[str, Any]:
    queue: JobQueue = request.app.state.queue
    job = queue.submit(req.payload, idempotency_key=req.idempotency_key,
                       max_attempts=req.max_attempts)
    return job_view(job)


@app.get("/jobs/{job_id}")
def get_job(job_id: str, request: Request) -> dict[str, Any]:
    return job_view(request.app.state.queue.get(job_id))


@app.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str, request: Request) -> dict[str, Any]:
    return job_view(request.app.state.queue.cancel(job_id))


@app.get("/dead")
def list_dead(request: Request) -> list[dict[str, Any]]:
    return [job_view(j) for j in request.app.state.queue.list_dead()]


@app.post("/jobs/{job_id}/replay")
def replay_job(job_id: str, request: Request) -> dict[str, Any]:
    return job_view(request.app.state.queue.replay(job_id))
