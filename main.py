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
from jobqueue.models import Job, JobEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

DB_PATH = os.environ.get("JOBQUEUE_DB", "jobqueue.db")


def event_view(ev: JobEvent) -> dict[str, Any]:
    """审计视图: 只暴露变化类型/尝试次数/归一化原因, 不含载荷与堆栈。"""
    return {
        "id": ev.id,
        "job_id": ev.job_id,
        "event": ev.event.value,
        "attempt": ev.attempt,
        "reason": ev.reason,
        "created_at": ev.created_at,
    }


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
    queue = JobQueue(
        store,
        capacity=int(os.environ.get("JOBQUEUE_CAPACITY", "1000")),
        backoff=BackoffPolicy(),
        lease_seconds=float(os.environ.get("JOBQUEUE_LEASE_SECONDS", "30")),
        stale_lease_seconds=float(
            os.environ.get("JOBQUEUE_STALE_LEASE_SECONDS",
                           os.environ.get("JOBQUEUE_LEASE_SECONDS", "30"))
        ),
        startup_reclaim_all=os.environ.get(
            "JOBQUEUE_STARTUP_RECLAIM_ALL", "1"
        ) not in ("0", "false", "False", ""),
    )
    # 默认配置下正常启动流程即自愈: 回收上次进程留下的全部执行中任务。
    recovered = queue.recover()
    if recovered:
        logger.info("recovered %d leased jobs after restart", len(recovered))
    pool = WorkerPool(
        queue, demo_handler, concurrency=4,
        reap_interval=float(os.environ.get("JOBQUEUE_REAP_INTERVAL", "1")),
    )
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


@app.get("/jobs/{job_id}/events")
def get_job_events(job_id: str, request: Request) -> list[dict[str, Any]]:
    # 任务不存在时返回空列表(审计能力不泄露任务存在性之外的内部信息);
    # 调用方可先经 GET /jobs/{id} 确认任务。
    queue: JobQueue = request.app.state.queue
    queue.get(job_id)  # 不存在则抛 JobNotFound -> 404
    return [event_view(e) for e in queue.events(job_id)]


@app.get("/events")
def list_events(request: Request, since_id: int = 0, limit: int = 200) -> dict[str, Any]:
    """全局增量审计查询: 返回 id > since_id、按 id(时间顺序) 排列的记录。"""
    limit = max(1, min(limit, 1000))
    queue: JobQueue = request.app.state.queue
    events = queue.events_since(since_id, limit)
    return {
        "events": [event_view(e) for e in events],
        "next_since_id": events[-1].id if events else since_id,
    }
