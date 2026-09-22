"""进程重启恢复: 执行中的任务不丢失、不重复执行。"""
import asyncio
import logging
import time

from jobqueue import BackoffPolicy, JobQueue, JobState, SQLiteStore, WorkerPool

logger = logging.getLogger("test.recovery")


def run(coro):
    return asyncio.run(coro)


async def wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return False


def make_queue(db_path, lease_seconds=0.2):
    return JobQueue(
        SQLiteStore(db_path),
        capacity=100,
        backoff=BackoffPolicy(base_seconds=0.01, factor=2.0, max_seconds=0.1),
        lease_seconds=lease_seconds,
    )


def test_restart_requeues_leased_jobs_without_loss(db_path):
    """崩溃时处于 LEASED 的任务, 重启后回到 PENDING 并被再次执行。"""
    # --- 第一个"进程": 提交并制造一个执行中(租约过期)的任务 ---
    q1 = make_queue(db_path)
    done_job = q1.submit({"task": "finished"})
    leased = q1.store.claim_next(time.time(), 0.01)
    assert leased.id == done_job.id
    assert q1.store.complete(done_job.id, leased.lease_token, "ok")

    crashed_job = q1.submit({"task": "was-running"})
    q1.store.claim_next(time.time(), 0.01)  # 短租约
    time.sleep(0.02)  # 租约过期, 模拟崩溃
    pending_job = q1.submit({"task": "still-queued"})
    logger.info(
        "崩溃前快照: done=%s leased(过期)=%s pending=%s",
        done_job.id, crashed_job.id, pending_job.id,
    )
    q1.store.close()

    # --- 第二个"进程": 同一数据库恢复 ---
    q2 = make_queue(db_path)
    recovered = q2.recover()
    recovered_ids = {j.id for j in recovered}
    logger.info("判定依据: 恢复回收 job=%s, 终态任务不受影响", recovered_ids)
    assert crashed_job.id in recovered_ids
    assert q2.get(crashed_job.id).state == JobState.PENDING
    assert q2.get(done_job.id).state == JobState.SUCCEEDED  # 终态不被回退
    assert q2.get(pending_job.id).state == JobState.PENDING

    async def scenario():
        executed = []

        async def handler(payload):
            executed.append(payload["task"])
            return "ok"

        pool = WorkerPool(q2, handler, concurrency=2, poll_interval=0.005)
        await pool.start()
        ok = await wait_for(
            lambda: q2.get(crashed_job.id).state == JobState.SUCCEEDED
            and q2.get(pending_job.id).state == JobState.SUCCEEDED
        )
        await pool.stop()
        return executed, ok

    executed, ok = run(scenario())
    logger.info("判定依据: 重启后执行序列=%s", executed)
    assert ok
    assert executed.count("was-running") == 1, "恢复的任务恰好执行一次"
    assert "finished" not in executed, "已完成任务不得被重复执行"
    q2.store.close()


def test_stale_lease_holder_cannot_overwrite_after_recovery(db_path):
    """旧执行者持有的租约在恢复后失效, 其写入不得覆盖新状态。"""
    q1 = make_queue(db_path)
    job = q1.submit({"task": "contended"})
    stale = q1.store.claim_next(time.time(), 0.01)
    time.sleep(0.02)  # 租约过期
    q1.store.close()

    q2 = make_queue(db_path)
    q2.recover()
    fresh = q2.store.claim_next(time.time(), 30.0)
    assert fresh is not None and fresh.lease_token != stale.lease_token

    # 旧租约持有者的完成/失败写入都必须被拒绝
    assert not q2.store.complete(job.id, stale.lease_token, "stale-result")
    assert q2.get(job.id).state == JobState.LEASED
    assert q2.store.complete(job.id, fresh.lease_token, "fresh-result")
    logger.info(
        "判定依据: job=%s 旧租约写入被拒, 新租约写入生效 -> %s",
        job.id, q2.get(job.id).state.value,
    )
    assert q2.get(job.id).state == JobState.SUCCEEDED
    q2.store.close()
