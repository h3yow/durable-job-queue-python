"""失败重试、退避单调性、死信与重放、失败类型归一化。"""
import asyncio
import logging
import time

from jobqueue import BackoffPolicy, FailureKind, JobState, WorkerPool

logger = logging.getLogger("test.retry")


def run(coro):
    return asyncio.run(coro)


async def wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return False


def test_backoff_is_monotonic_and_capped():
    p = BackoffPolicy(base_seconds=0.05, factor=3.0, max_seconds=0.5)
    delays = [p.delay_for(i) for i in range(1, 10)]
    logger.info("判定依据: 退避序列 %s 单调递增且封顶 %.2f", delays, p.max_seconds)
    assert all(b >= a for a, b in zip(delays, delays[1:]))
    assert delays[-1] == p.max_seconds


def test_retry_exhaustion_goes_dead_with_classified_reason(queue):
    async def scenario():
        async def handler(payload):
            raise RuntimeError("db connection refused")

        pool = WorkerPool(queue, handler, concurrency=2, poll_interval=0.01)
        await pool.start()
        job = queue.submit({"task": "flaky"}, max_attempts=3)
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.DEAD)
        await pool.stop()
        return job, ok

    job, ok = run(scenario())
    dead = queue.get(job.id)
    logger.info(
        "状态迁移: job=%s leased->dead attempts=%d kind=%s reason=%r",
        dead.id, dead.attempts, dead.last_failure.kind.value,
        dead.last_failure.reason,
    )
    assert ok
    assert dead.attempts == 3
    assert dead.last_failure.kind == FailureKind.HANDLER_ERROR
    assert "db connection refused" in dead.last_failure.reason
    assert len(dead.failure_history) == 3
    # 死信可查询
    assert [j.id for j in queue.list_dead()] == [job.id]


def test_timeout_classified_as_handler_timeout(queue):
    async def scenario():
        async def handler(payload):
            await asyncio.sleep(10)

        pool = WorkerPool(queue, handler, concurrency=1,
                          execute_timeout=0.05, poll_interval=0.01)
        await pool.start()
        job = queue.submit({"task": "slow"}, max_attempts=1)
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.DEAD)
        await pool.stop()
        return job, ok

    job, ok = run(scenario())
    dead = queue.get(job.id)
    logger.info("判定依据: job=%s 超时被中断 -> kind=%s", dead.id, dead.last_failure.kind.value)
    assert ok
    assert dead.last_failure.kind == FailureKind.HANDLER_TIMEOUT


def test_replay_dead_job_preserves_failure_history(queue):
    async def scenario():
        calls = {"n": 0}

        async def handler(payload):
            calls["n"] += 1
            if calls["n"] == 1:
                raise ValueError("still broken")
            return "fixed"

        pool = WorkerPool(queue, handler, concurrency=1, poll_interval=0.01)
        await pool.start()
        job = queue.submit({"task": "replay-me"}, max_attempts=1)
        await wait_for(lambda: queue.get(job.id).state == JobState.DEAD)
        replayed = queue.replay(job.id)
        logger.info(
            "状态迁移: job=%s dead->pending, 保留历史 %d 条, last=%r",
            replayed.id, len(replayed.failure_history),
            replayed.last_failure.reason,
        )
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.SUCCEEDED)
        await pool.stop()
        return job, ok

    job, ok = run(scenario())
    final = queue.get(job.id)
    assert ok
    assert final.state == JobState.SUCCEEDED
    assert final.failure_history[0].reason == "ValueError: still broken"
    assert final.last_failure.kind == FailureKind.HANDLER_ERROR
