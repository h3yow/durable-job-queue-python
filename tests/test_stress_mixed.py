"""混合压力：大量提交/领取/取消/重试交织，只断言最终不变量与失败归类。"""

from __future__ import annotations

import asyncio
import random

from jobqueue.errors import FailureKind
from jobqueue.models import TERMINAL_STATUSES, TaskStatus


async def test_mixed_workload_invariants(queue, pool_factory):
    N_OK, N_FLAKY, N_CANCELLED = 60, 40, 30
    execution_owners: dict[str, str] = {}
    exec_counts: dict[str, int] = {}
    lock = asyncio.Lock()
    rng = random.Random(42)

    async def succeed(payload: str) -> str:
        async with lock:
            exec_counts[payload] = exec_counts.get(payload, 0) + 1
            assert execution_owners.setdefault(payload, "s") == "s"
        await asyncio.sleep(rng.random() * 0.002)
        return "ok"

    async def flaky(payload: str) -> None:
        async with lock:
            exec_counts[payload] = exec_counts.get(payload, 0) + 1
        await asyncio.sleep(rng.random() * 0.002)
        raise RuntimeError("flaky-failure")

    async def cancellable(payload: str) -> None:
        async with lock:
            exec_counts[payload] = exec_counts.get(payload, 0) + 1
        await asyncio.sleep(0.5)  # 等待被取消

    pool = pool_factory(
        {"s": succeed, "f": flaky, "c": cancellable},
        concurrency=6, poll_interval=0.003,
    )

    ids_s, ids_f, ids_c = [], [], []
    for i in range(N_OK):
        ids_s.append((await queue.submit("s", f"s-{i}")).id)
    for i in range(N_FLAKY):
        ids_f.append((await queue.submit("f", f"f-{i}")).id)
    await pool.start()
    # 可取消任务在 worker 运行后再提交/取消，制造竞态
    for i in range(N_CANCELLED):
        ids_c.append((await queue.submit("c", f"c-{i}")).id)
    await asyncio.sleep(0.05)
    await asyncio.gather(*[queue.request_cancel(t) for t in ids_c])

    all_ids = ids_s + ids_f + ids_c
    deadline = asyncio.get_running_loop().time() + 15
    while True:
        recs = await asyncio.gather(*[queue.get_task(t) for t in all_ids])
        if all(r.status in TERMINAL_STATUSES for r in recs):
            break
        if asyncio.get_running_loop().time() > deadline:
            pending = [r.id for r in recs if r.status not in TERMINAL_STATUSES]
            raise AssertionError(f"{len(pending)} tasks not terminal")
        await asyncio.sleep(0.02)

    by_id = {r.id: r for r in recs}

    # 不变量 1：成功组全部 SUCCEEDED 且恰好执行一次
    for tid in ids_s:
        r = by_id[tid]
        assert r.status is TaskStatus.SUCCEEDED
        assert exec_counts[r.payload] == 1

    # 不变量 2：失败组全部 DEAD，3 次 HANDLER_ERROR，原因可区分
    for tid in ids_f:
        r = by_id[tid]
        assert r.status is TaskStatus.DEAD
        assert r.attempts == 3
        assert [f["kind"] for f in r.failures] == [
            FailureKind.HANDLER_ERROR.value
        ] * 3
        assert all("flaky-failure" in f["reason"] for f in r.failures)

    # 不变量 3：取消组要么 CANCELLED（执行 0 或 1 次，均无重复），
    # 极端竞态下可能 SUCCEEDED（本 handler 不会自行返回，故预期全部 CANCELLED）
    for tid in ids_c:
        r = by_id[tid]
        assert r.status is TaskStatus.CANCELLED, (tid, r.status)
        assert exec_counts.get(r.payload, 0) <= 1  # 最多执行一次，无重复领取

    # 不变量 4：死信列表与 DEAD 集合一致
    dead = await queue.list_dead(limit=1000)
    assert {d.id for d in dead} == set(ids_f)
