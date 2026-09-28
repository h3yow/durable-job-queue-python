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


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


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


def event_view(event) -> dict[str, Any]:
    return {
        "id": event.id,
        "job_id": event.job_id,
        "event": event.event.value,
        "attempt": event.attempt,
        "reason": event.reason,
        "created_at": event.created_at,
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
    # 配置兼容: 未设置环境变量时保持原有默认(租约 30s), 失联阈值默认取租约值;
    # 调小 JOBQUEUE_STALE_TIMEOUT 可快速复现运行期收回。
    lease_seconds = _env_float("JOBQUEUE_LEASE_SECONDS", 30.0)
    stale_timeout = _env_float("JOBQUEUE_STALE_TIMEOUT", lease_seconds)
    reclaim_interval = _env_float("JOBQUEUE_RECLAIM_INTERVAL", 5.0)
    queue = JobQueue(store, capacity=int(os.environ.get("JOBQUEUE_CAPACITY", "1000")),
                     backoff=BackoffPolicy(), lease_seconds=lease_seconds,
                     stale_timeout_seconds=stale_timeout)
    # 正常启动流程默认即收回上次进程残留的执行中任务(无需等租约过期)
    recovered = queue.recover()
    if recovered:
        logger.info("recovered %d leased jobs after restart", len(recovered))
    pool = WorkerPool(queue, demo_handler, concurrency=4,
                      reclaim_interval=reclaim_interval)
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
    result = request.app.state.queue.cancel(job_id)
    body = job_view(result.job)
    # 排队中取消(及终态幂等)返回 cancelled; 执行中取消返回 accepted,
    # 调用方据此区分"已终态"与"已受理、稍后收口"。
    body["cancel_effect"] = result.effect.value
    return body


@app.get("/jobs/{job_id}/events")
def list_job_events(job_id: str, request: Request) -> list[dict[str, Any]]:
    """单任务生命周期审计: 按时间顺序, 仅含公开字段。"""
    events = request.app.state.queue.get_events(job_id)
    return [event_view(e) for e in events]


@app.get("/events")
def list_events(request: Request, after_id: int = 0, limit: int = 500) -> dict[str, Any]:
    """全局审计流: 按时间顺序翻页(after_id 传上一页最后一条 id)。"""
    events = request.app.state.queue.list_events(after_id=after_id, limit=limit)
    return {"events": [event_view(e) for e in events]}


@app.get("/dead")
def list_dead(request: Request) -> list[dict[str, Any]]:
    return [job_view(j) for j in request.app.state.queue.list_dead()]


@app.post("/jobs/{job_id}/replay")
def replay_job(job_id: str, request: Request) -> dict[str, Any]:
    return job_view(request.app.state.queue.replay(job_id))
