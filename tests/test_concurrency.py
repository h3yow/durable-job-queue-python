"""多工作协程并发领取与取消竞态。"""
import asyncio
import logging
import time
from collections import Counter

from jobqueue import JobState, WorkerPool

logger = logging.getLogger("test.concurrency")


def run(coro):
    return asyncio.run(coro)


async def wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return False


def test_concurrent_claim_executes_each_job_exactly_once(queue):
    """N 个协程并发领取: 同一任务任意时刻最多一个持有者, 恰好执行一次。"""
    async def scenario():
        executed: list[str] = []
        lock = asyncio.Lock()

        async def handler(payload):
            async with lock:
                executed.append(payload["job_no"])
            await asyncio.sleep(0.01)
            return "ok"

        pool = WorkerPool(queue, handler, concurrency=8, poll_interval=0.005)
        await pool.start()
        jobs = [queue.submit({"job_no": i}) for i in range(50)]
        ok = await wait_for(
            lambda: all(queue.get(j.id).state == JobState.SUCCEEDED for j in jobs)
        )
        await pool.stop()
        return jobs, executed, ok

    jobs, executed, ok = run(scenario())
    counts = Counter(executed)
    logger.info(
        "判定依据: 50 任务/8 协程, 执行次数分布=%s, 重复=%s",
        sorted(counts.values()), [k for k, v in counts.items() if v > 1],
    )
    assert ok
    assert len(executed) == 50
    assert all(v == 1 for v in counts.values()), "同一任务不得被重复执行"


def test_cancel_leased_job_interrupts_execution(queue):
    """取消已领取任务: 执行结束后落 CANCELLED 而非 SUCCEEDED。"""
    async def scenario():
        started = asyncio.Event()

        async def handler(payload):
            started.set()
            await asyncio.sleep(0.2)  # 执行期间被取消
            return "should-not-win"

        pool = WorkerPool(queue, handler, concurrency=1, poll_interval=0.005)
        await pool.start()
        job = queue.submit({"task": "long"})
        await asyncio.wait_for(started.wait(), 2)
        cancelled = queue.cancel(job.id)
        logger.info(
            "状态迁移: job=%s leased(cancel_requested=%s) effect=%s, 等待执行者落终态",
            cancelled.job.id, cancelled.job.cancel_requested, cancelled.effect.value,
        )
        assert cancelled.job.state == JobState.LEASED
        assert cancelled.job.cancel_requested
        assert cancelled.effect.value == "accepted"
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.CANCELLED)
        await pool.stop()
        return job, ok

    job, ok = run(scenario())
    final = queue.get(job.id)
    logger.info("判定依据: job=%s 终态=%s (取消优先于成功)", final.id, final.state.value)
    assert ok
    assert final.state == JobState.CANCELLED
    # 重复取消不改变终态
    again = queue.cancel(job.id)
    assert again.job.state == JobState.CANCELLED
    assert again.effect.value == "cancelled"


def test_cancel_race_with_completion_is_consistent(queue):
    """取消与完成竞态: 终态必为 SUCCEEDED 或 CANCELLED 之一, 不出现半更新。"""
    async def scenario():
        async def handler(payload):
            await asyncio.sleep(0.02)
            return "done"

        pool = WorkerPool(queue, handler, concurrency=4, poll_interval=0.005)
        await pool.start()
        jobs = [queue.submit({"i": i}) for i in range(30)]
        for j in jobs:
            queue.cancel(j.id)  # 与领取/完成并发竞态
        ok = await wait_for(
            lambda: all(
                queue.get(j.id).state in (JobState.SUCCEEDED, JobState.CANCELLED)
                for j in jobs
            )
        )
        await pool.stop()
        return jobs, ok

    jobs, ok = run(scenario())
    states = Counter(queue.get(j.id).state for j in jobs)
    logger.info("判定依据: 竞态后终态分布=%s", {s.value: n for s, n in states.items()})
    assert ok
    assert set(states) <= {JobState.SUCCEEDED, JobState.CANCELLED}
    # 每个任务处于且仅处于一个终态, 存储中无 leased/pending 残留
    assert queue.store.count_active() == 0
