"""并发领取：同一任务任意时刻最多一个执行者；每个任务恰好执行一次。"""

from __future__ import annotations

import asyncio

from jobqueue.models import TaskStatus


async def test_concurrent_claim_each_task_once(queue, pool_factory):
    N = 100
    executed: set[str] = set()
    exec_lock = asyncio.Lock()
    duplicate = asyncio.Event()
    started = asyncio.Event()
    counter = {"n": 0}

    async def handler(payload: str) -> str:
        async with exec_lock:
            counter["n"] += 1
            if payload in executed:
                duplicate.set()
            executed.add(payload)
            if counter["n"] == N:
                started.set()
        # 制造交错，放大竞态
        await asyncio.sleep(0)
        return "ok:" + payload

    pool = pool_factory({"work": handler}, concurrency=8, poll_interval=0.005)
    ids = []
    for i in range(N):
        rec = await queue.submit("work", f"job-{i:03d}")
        ids.append(rec.id)

    await pool.start()
    await asyncio.wait_for(started.wait(), timeout=10)
    # 等待全部终态
    for tid in ids:
        rec = await queue.get_task(tid)
        while not rec.status.terminal:
            await asyncio.sleep(0.005)
            rec = await queue.get_task(tid)
    await pool.wait_idle(timeout=5)

    assert not duplicate.is_set(), "同一任务被多个执行者执行"
    assert len(executed) == N
    for tid in ids:
        rec = await queue.get_task(tid)
        assert rec.status is TaskStatus.SUCCEEDED, (tid, rec.status)
        assert rec.attempts == 1, "无失败时每个任务只应被领取一次"


async def test_concurrent_claim_and_cancel_no_duplicate_execution(
    queue, pool_factory
):
    """领取与取消并发：被取消的排队任务绝不执行；被领取的任务最多执行一次。"""
    N = 60
    ran: set[str] = set()
    lock = asyncio.Lock()
    gate = asyncio.Event()

    async def slow_handler(payload: str) -> str:
        async with lock:
            assert payload not in ran, "重复执行"
            ran.add(payload)
        await gate.wait()  # 让任务停留在 RUNNING 制造取消竞态窗口
        return "done"

    pool = pool_factory({"work": slow_handler}, concurrency=4,
                        poll_interval=0.005)
    await pool.start()

    tasks = [await queue.submit("work", f"p-{i}") for i in range(N)]
    # 等一部分进入 RUNNING
    await asyncio.sleep(0.1)
    # 并发取消所有任务
    await asyncio.gather(
        *[queue.request_cancel(t.id) for t in tasks]
    )
    gate.set()

    settled = []
    for t in tasks:
        rec = await queue.get_task(t.id)
        while not rec.status.terminal:
            await asyncio.sleep(0.005)
            rec = await queue.get_task(t.id)
        settled.append(rec)

    # 每个任务：要么被执行（SUCCEEDED 或 CANCELLED 取决于取消是否先落盘），
    # 要么从未执行（排队中被取消）；不变量是 ran 中的任务无重复
    for rec in settled:
        assert rec.status in {
            TaskStatus.SUCCEEDED, TaskStatus.CANCELLED
        }, rec.status
    assert len(ran) == len({r for r in ran})
    # 排队中被取消的任务从未运行：payload p-i 若不在 ran 中则必为 CANCELLED
    for i, rec in enumerate(settled):
        if f"p-{i}" not in ran:
            assert rec.status is TaskStatus.CANCELLED
            assert rec.attempts == 0, "排队取消的任务不应产生领取"
