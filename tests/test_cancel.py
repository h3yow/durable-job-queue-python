"""取消语义：排队取消 vs 领取后取消明确区分；重复取消幂等；竞态结果确定。"""

from __future__ import annotations

import asyncio

from jobqueue.models import TaskStatus


async def test_cancel_queued_task_never_runs(queue, pool_factory):
    ran = asyncio.Event()

    async def handler(payload: str) -> None:
        ran.set()

    # 不启动 worker：任务保持 PENDING
    t = await queue.submit("work", "p")
    rec = await queue.request_cancel(t.id)
    assert rec.status is TaskStatus.CANCELLED
    assert rec.runner_id is None and rec.attempts == 0

    pool = pool_factory({"work": handler})
    await pool.start()
    await asyncio.sleep(0.1)
    assert not ran.is_set(), "排队中被取消的任务不得执行"
    rec = await queue.get_task(t.id)
    assert rec.status is TaskStatus.CANCELLED  # 终态稳定


async def test_cancel_claimed_task_interrupts_cooperatively(queue, pool_factory):
    started = asyncio.Event()
    finish = asyncio.Event()
    observed_cancel = asyncio.Event()

    async def long_job(payload: str) -> str:
        started.set()
        # 协作响应：等待取消事件（由 queue 注入的机制需通过 asyncio 任务取消）
        try:
            await finish.wait()
        except asyncio.CancelledError:
            observed_cancel.set()
            raise
        return "should-not-return"

    pool = pool_factory({"work": long_job}, concurrency=1)
    t = await queue.submit("work", "p")
    await pool.start()
    await asyncio.wait_for(started.wait(), timeout=2)

    rec = await queue.request_cancel(t.id)
    # 语义区分：已领取 -> CANCELLING（取消请求，等待持有者确认）
    assert rec.status is TaskStatus.CANCELLING
    assert rec.runner_id is not None

    while True:
        rec = await queue.get_task(t.id)
        if rec.status is TaskStatus.CANCELLED:
            break
        await asyncio.sleep(0.01)
    assert observed_cancel.is_set(), "执行协程应被取消中断"
    finish.set()


async def test_repeated_cancel_is_idempotent_and_keeps_result(queue):
    t = await queue.submit("work", "p")
    r1 = await queue.request_cancel(t.id)
    r2 = await queue.request_cancel(t.id)
    r3 = await queue.request_cancel(t.id)
    assert r1.status is r2.status is r3.status is TaskStatus.CANCELLED
    assert r2.updated_at == r1.updated_at and r3.updated_at == r1.updated_at


async def test_cancel_succeeded_task_is_noop(queue, pool_factory):
    async def quick(payload: str) -> str:
        return "done"

    pool = pool_factory({"work": quick}, concurrency=1)
    t = await queue.submit("work", "p")
    await pool.start()
    while True:
        rec = await queue.get_task(t.id)
        if rec.status.terminal:
            break
        await asyncio.sleep(0.01)
    assert rec.status is TaskStatus.SUCCEEDED

    again = await queue.request_cancel(t.id)
    assert again.status is TaskStatus.SUCCEEDED  # 已完成结果不可变
    assert again.result == "done"


async def test_cancel_dead_task_is_noop(queue, pool_factory):
    async def boom(payload: str) -> None:
        raise RuntimeError("x")

    pool = pool_factory({"work": boom}, concurrency=1)
    t = await queue.submit("work", "p")
    await pool.start()
    while True:
        rec = await queue.get_task(t.id)
        if rec.status is TaskStatus.DEAD:
            break
        await asyncio.sleep(0.01)
    again = await queue.request_cancel(t.id)
    assert again.status is TaskStatus.DEAD  # 死信不受取消影响，仍可重放


async def test_cancel_race_with_handler_completion(queue, pool_factory):
    """取消与处理器完成竞速：两种结局都必须确定，且失败不得覆盖已定终态。

    处理器几乎瞬时完成；随后取消。已 SUCCEEDED 则取消 no-op。
    """
    async def quick(payload: str) -> str:
        return "v"

    pool = pool_factory({"work": quick}, concurrency=1)
    t = await queue.submit("work", "p")
    await pool.start()
    while True:
        rec = await queue.get_task(t.id)
        if rec.status.terminal:
            break
        await asyncio.sleep(0.005)
    rec = await queue.request_cancel(t.id)
    # 完成先落盘：结果保持
    assert rec.status is TaskStatus.SUCCEEDED and rec.result == "v"


async def test_parallel_cancels_single_settlement(queue, pool_factory):
    started = asyncio.Event()
    finish = asyncio.Event()

    async def long_job(payload: str) -> None:
        started.set()
        await finish.wait()

    pool = pool_factory({"work": long_job}, concurrency=1)
    t = await queue.submit("work", "p")
    await pool.start()
    await asyncio.wait_for(started.wait(), timeout=2)

    # 同时发起多个取消请求
    results = await asyncio.gather(*[
        queue.request_cancel(t.id) for _ in range(10)
    ])
    statuses = {r.status for r in results}
    # 首个落 CANCELLING，后续看到 CANCELLING 即 no-op 返回
    assert statuses <= {TaskStatus.CANCELLING, TaskStatus.CANCELLED}

    while True:
        rec = await queue.get_task(t.id)
        if rec.status is TaskStatus.CANCELLED:
            break
        await asyncio.sleep(0.01)
    finish.set()
    # 再取消：稳定终态
    assert (await queue.request_cancel(t.id)).status is TaskStatus.CANCELLED
