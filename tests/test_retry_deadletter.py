"""失败归类、退避延迟重排、重试耗尽进死信、人工重放保留原始原因。"""

from __future__ import annotations

import asyncio
import time

from jobqueue.errors import FailureKind, PermanentJobError, TaskNotDeadError
from jobqueue.models import TaskStatus
from jobqueue.retry import BackoffPolicy


def test_backoff_is_monotonic_and_configurable():
    # 判定依据：factor>=1、封顶 max_delay，序列单调非递减
    p = BackoffPolicy(base_delay=0.01, factor=2.0, max_delay=0.08, jitter=0.0)
    seq = [p.delay_for(n) for n in range(1, 8)]
    assert seq == sorted(seq)
    assert seq == [0.01, 0.02, 0.04, 0.08, 0.08, 0.08, 0.08]
    # 线性退避也单调
    p2 = BackoffPolicy(base_delay=0.03, factor=1.0, max_delay=1.0)
    seq2 = [p2.delay_for(n) for n in range(1, 5)]
    assert seq2 == [0.03, 0.03, 0.03, 0.03]
    # 非单调配置必须在构造期拒绝
    import pytest
    with pytest.raises(ValueError):
        BackoffPolicy(factor=0.5)


async def test_handler_error_retried_until_dead(queue, pool_factory):
    calls = {"n": 0}

    async def flaky(payload: str) -> None:
        calls["n"] += 1
        raise RuntimeError("transient-boom")

    pool = pool_factory({"flaky": flaky})
    t = await queue.submit("flaky", "{}")
    await pool.start()

    # 等至 DEAD
    rec = t
    while rec.status is not TaskStatus.DEAD:
        await asyncio.sleep(0.01)
        rec = await queue.get_task(t.id)

    assert calls["n"] == 3, calls  # max_attempts=3（fixture 默认）
    assert rec.attempts == 3
    kinds = [f["kind"] for f in rec.failures]
    assert kinds == [FailureKind.HANDLER_ERROR.value] * 3
    # 原始原因可区分、可追溯
    assert all("transient-boom" in f["reason"]
               and f["reason"].startswith("RuntimeError")
               for f in rec.failures)
    assert [f["attempt"] for f in rec.failures] == [1, 2, 3]
    # 死信可查询
    dead = await queue.list_dead()
    assert any(d.id == t.id for d in dead)


async def test_retry_respects_backoff_timing(queue, pool_factory):
    """退避窗口内不可领取（延迟重排），到点后才再次执行。"""
    timestamps: list[float] = []

    async def fail3(payload: str) -> None:
        timestamps.append(time.monotonic())
        raise ValueError("v")

    # base 0.02, factor 2 -> 第一次失败后至少等 0.02s
    pool = pool_factory({"fail3": fail3})
    t = await queue.submit("fail3", "{}")
    await pool.start()
    rec = t
    while rec.status is not TaskStatus.DEAD:
        await asyncio.sleep(0.005)
        rec = await queue.get_task(t.id)

    assert len(timestamps) == 3
    assert timestamps[1] - timestamps[0] >= 0.02 - 0.005
    assert timestamps[2] - timestamps[1] >= 0.04 - 0.005
    # 单调递增的实际间隔
    assert timestamps[2] - timestamps[1] > timestamps[1] - timestamps[0]


async def test_permanent_error_goes_dead_without_retry(queue, pool_factory):
    calls = {"n": 0}

    async def doomed(payload: str) -> None:
        calls["n"] += 1
        raise PermanentJobError("business rule violation: x")

    pool = pool_factory({"doomed": doomed})
    t = await queue.submit("doomed", "{}")
    await pool.start()
    while True:
        rec = await queue.get_task(t.id)
        if rec.status.terminal:
            break
        await asyncio.sleep(0.01)

    assert rec.status is TaskStatus.DEAD
    assert calls["n"] == 1  # 永久失败不重试
    assert rec.failures[-1]["kind"] == FailureKind.PERMANENT_ERROR.value
    assert "business rule violation" in rec.failures[-1]["reason"]


async def test_timeout_classified_as_timeout(queue, pool_factory):
    async def slow(payload: str) -> None:
        await asyncio.sleep(5)

    pool = pool_factory({"slow": slow}, default_timeout=0.05, concurrency=1)
    t = await queue.submit("slow", "{}")
    await pool.start()
    while True:
        rec = await queue.get_task(t.id)
        if rec.status.terminal:
            break
        await asyncio.sleep(0.01)

    assert rec.status is TaskStatus.DEAD
    assert all(f["kind"] == FailureKind.TIMEOUT.value for f in rec.failures)
    assert all("timeout" in f["reason"].lower() for f in rec.failures)


async def test_replay_preserves_failure_history_and_reexecutes(
    queue, pool_factory
):
    gate = {"fail": True}
    calls: dict[str, int] = {}

    async def healable(payload: str) -> str:
        calls[payload] = calls.get(payload, 0) + 1
        if gate["fail"]:
            raise RuntimeError("downstream unavailable")
        return "recovered-result"

    pool = pool_factory({"healable": healable})
    t = await queue.submit("healable", "target")
    await pool.start()
    while True:
        rec = await queue.get_task(t.id)
        if rec.status is TaskStatus.DEAD:
            break
        await asyncio.sleep(0.01)
    original_failures = list(rec.failures)
    assert len(original_failures) == 3

    # 非 DEAD 不可重放
    ok_task = await queue.submit("healable", "{}-will-succeed")
    import pytest
    with pytest.raises(TaskNotDeadError):
        await queue.replay_dead(ok_task.id)

    # 修复下游后人工重放：attempts 归零，但失败历史保留
    gate["fail"] = False
    replayed = await queue.replay_dead(t.id)
    assert replayed.status is TaskStatus.PENDING
    assert replayed.attempts == 0
    assert len(replayed.failures) == 3
    assert list(replayed.failures) == original_failures  # 原始失败原因不丢失
    assert replayed.last_failure_reason and "downstream unavailable" in (
        replayed.last_failure_reason
    )

    while True:
        rec = await queue.get_task(t.id)
        if rec.status.terminal:
            break
        await asyncio.sleep(0.01)
    assert rec.status is TaskStatus.SUCCEEDED
    assert rec.result == "recovered-result"
    assert calls.get("target") == 4  # 原 3 次 + 重放后 1 次
    # 成功后历史仍在，可审计
    assert len(rec.failures) == 3
    assert rec.failures[0]["reason"].startswith("RuntimeError")
