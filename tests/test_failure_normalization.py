"""异常归一化：可区分失败类型；内部异常不进入对外响应。"""

from __future__ import annotations

import asyncio

from jobqueue.errors import FailureKind
from jobqueue.models import TaskStatus
from jobqueue.worker import _normalize_reason


def test_reason_normalization_strips_internals():
    class _Secret(Exception):
        pass

    err = _Secret("db password=hunter2 host=10.0.0.1")
    reason = _normalize_reason(err)
    assert reason.startswith("_Secret: ")
    assert "password=hunter2" in reason  # 消息文本保留为失败原因（内部堆栈不在其中）
    # 截断保护
    big = RuntimeError("x" * 5000)
    out = _normalize_reason(big)
    assert out.endswith("...(truncated)") and len(out) <= 2020


async def test_handler_exception_kinds(queue, pool_factory):
    async def raise_runtime(payload: str) -> None:
        raise RuntimeError("rte")

    async def raise_value(payload: str) -> None:
        raise ValueError("ve")

    async def raise_nested(payload: str) -> None:
        async def inner():
            raise KeyError("nested-key")
        await inner()

    pool = pool_factory(
        {"rte": raise_runtime, "ve": raise_value, "nest": raise_nested},
        concurrency=1,
    )
    t1 = await queue.submit("rte", "{}", max_attempts=1)
    t2 = await queue.submit("ve", "{}", max_attempts=1)
    t3 = await queue.submit("nest", "{}", max_attempts=1)
    await pool.start()

    finals = {}
    for t in (t1, t2, t3):
        while True:
            rec = await queue.get_task(t.id)
            if rec.status.terminal:
                finals[t.task_type] = rec
                break
            await asyncio.sleep(0.005)

    assert finals["rte"].failures[0]["kind"] == FailureKind.HANDLER_ERROR.value
    assert finals["rte"].failures[0]["reason"].startswith("RuntimeError: rte")
    assert finals["ve"].failures[0]["reason"].startswith("ValueError: ve")
    assert finals["nest"].failures[0]["reason"].startswith(
        "KeyError: 'nested-key'"
    )
    for rec in finals.values():
        assert rec.status is TaskStatus.DEAD
        # 无堆栈/traceback 字样进入持久化原因
        assert "Traceback" not in rec.failures[0]["reason"]


async def test_unknown_exception_does_not_kill_worker(queue, pool_factory):
    """处理器抛出任意异常后 worker 仍可持续领取后续任务。"""
    counter = {"n": 0}

    async def sometimes(payload: str) -> str:
        counter["n"] += 1
        if counter["n"] == 1:
            raise RuntimeError("first fails")
        return "ok"

    pool = pool_factory({"s": sometimes}, concurrency=1)
    t1 = await queue.submit("s", "a", max_attempts=1)
    t2 = await queue.submit("s", "b")
    await pool.start()
    results = {}
    for t in (t1, t2):
        while True:
            rec = await queue.get_task(t.id)
            if rec.status.terminal:
                results[t.id] = rec
                break
            await asyncio.sleep(0.005)
    assert results[t1.id].status is TaskStatus.DEAD
    assert results[t2.id].status is TaskStatus.SUCCEEDED
    assert results[t2.id].result == "ok"
