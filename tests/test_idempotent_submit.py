"""幂等提交：重复请求返回同一任务、不并发执行；载荷冲突被明确拒绝。"""

from __future__ import annotations

import asyncio

import pytest

from jobqueue.errors import IdempotencyMismatchError
from jobqueue.models import TaskStatus


async def test_duplicate_submit_returns_same_task(queue):
    t1 = await queue.submit("email", '{"v":1}', idempotency_key="key-1")
    t2 = await queue.submit("email", '{"v":1}', idempotency_key="key-1")
    t3 = await queue.submit("email", '{"v":1}', idempotency_key="key-1")
    assert t1.id == t2.id == t3.id
    assert t1.idempotency_key == "key-1"
    # 存储中只有一条记录
    found = await queue.find_by_idempotency_key("key-1")
    assert found is not None and found.id == t1.id


async def test_concurrent_duplicate_submit_single_task(queue, pool_factory):
    """并发相同请求必须只产生一个任务，且只执行一次。"""
    N = 20
    exec_count = {"n": 0}
    gate = asyncio.Event()

    async def handler(payload: str) -> str:
        exec_count["n"] += 1
        await gate.wait()
        return "ok"

    pool = pool_factory({"email": handler}, concurrency=1)
    await pool.start()

    records = await asyncio.gather(*[
        queue.submit("email", '{"x":1}', idempotency_key="hot-key")
        for _ in range(N)
    ])
    ids = {r.id for r in records}
    assert len(ids) == 1, f"并发提交产生了 {len(ids)} 条任务"
    gate.set()

    # 唯一执行
    tid = records[0].id
    deadline = asyncio.get_running_loop().time() + 3
    while True:
        rec = await queue.get_task(tid)
        if rec.status.terminal:
            break
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("任务未到终态")
        await asyncio.sleep(0.01)
    assert rec.status is TaskStatus.SUCCEEDED
    assert exec_count["n"] == 1, f"同一幂等任务被执行 {exec_count['n']} 次"
    assert rec.attempts == 1


async def test_same_key_different_payload_rejected_with_distinct_reason(queue):
    t = await queue.submit("email", '{"to":"a@x"}', idempotency_key="dup")
    with pytest.raises(IdempotencyMismatchError) as ei:
        await queue.submit("email", '{"to":"b@x"}', idempotency_key="dup")
    assert ei.value.reason == "IDEMPOTENCY_PAYLOAD_MISMATCH"
    assert ei.value.task_id == t.id  # 可定位到已有任务

    # 不同 task_type 复用同一 key 同样视为载荷不一致
    with pytest.raises(IdempotencyMismatchError):
        await queue.submit("sms", '{"to":"a@x"}', idempotency_key="dup")

    # 被拒绝后，原任务状态与归属不受影响
    fresh = await queue.get_task(t.id)
    assert fresh.task_type == "email"
    assert fresh.payload == '{"to":"a@x"}'
    assert fresh.status is TaskStatus.PENDING


async def test_idempotency_independent_across_distinct_keys(queue):
    t1 = await queue.submit("email", "{}", idempotency_key="k-a")
    t2 = await queue.submit("email", "{}", idempotency_key="k-b")
    t3 = await queue.submit("email", "{}")  # 无幂等键
    assert len({t1.id, t2.id, t3.id}) == 3
