"""运行期自愈: 默认配置启动回收、失联周期收回、旧执行者晚到不二次执行。"""
import asyncio
import logging
import time

from jobqueue import (
    BackoffPolicy,
    EventType,
    JobQueue,
    JobState,
    SQLiteStore,
    WorkerPool,
)

logger = logging.getLogger("test.selfhealing")


def run(coro):
    return asyncio.run(coro)


async def wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.005)
    return False


def make_queue(db_path, *, lease_seconds=0.2, stale_lease_seconds=None,
               startup_reclaim_all=True):
    return JobQueue(
        SQLiteStore(db_path),
        capacity=100,
        backoff=BackoffPolicy(base_seconds=0.01, factor=2.0, max_seconds=0.1),
        lease_seconds=lease_seconds,
        stale_lease_seconds=stale_lease_seconds if stale_lease_seconds is not None
        else lease_seconds,
        startup_reclaim_all=startup_reclaim_all,
    )


def test_default_startup_reclaims_leased_without_waiting_for_expiry(db_path):
    """默认配置: 重启即回收全部 LEASED, 即使租约远未到期也不长时间挂起。"""
    q1 = make_queue(db_path, lease_seconds=300.0)
    job = q1.submit({"task": "crashed-mid-flight"})
    leased = q1.store.claim_next(time.time(), 300.0)  # 长租约, 远未过期
    assert leased.state == JobState.LEASED
    logger.info("崩溃快照: job=%s LEASED 租约 300s, 不等待到期", job.id)
    q1.store.close()

    q2 = make_queue(db_path, lease_seconds=300.0)  # 默认 startup_reclaim_all=True
    recovered = q2.recover()
    assert [j.id for j in recovered] == [job.id]
    assert q2.get(job.id).state == JobState.PENDING
    events = [e.event for e in q2.events(job.id)]
    logger.info("判定依据: 启动后事件序列=%s", [e.value for e in events])
    assert EventType.LEASE_RECLAIMED in events

    async def scenario():
        async def handler(payload):
            return "ran-once"

        pool = WorkerPool(q2, handler, concurrency=1, poll_interval=0.005)
        await pool.start()
        ok = await wait_for(lambda: q2.get(job.id).state == JobState.SUCCEEDED)
        await pool.stop()
        return ok

    assert run(scenario())
    q2.store.close()


def test_startup_cancel_requested_leased_job_settles_cancelled(db_path):
    """启动回收时, 已受理取消的执行中任务直接收口为 CANCELLED, 不再执行。"""
    q1 = make_queue(db_path, lease_seconds=300.0)
    job = q1.submit({"task": "cancelled-then-crash"})
    q1.store.claim_next(time.time(), 300.0)
    q1.cancel(job.id)  # 取消受理后进程崩溃, 执行者没来得及落终态
    assert q1.get(job.id).cancel_requested
    q1.store.close()

    q2 = make_queue(db_path)
    q2.recover()
    final = q2.get(job.id)
    assert final.state == JobState.CANCELLED
    assert not final.cancel_requested or final.state == JobState.CANCELLED
    event_types = [e.event for e in q2.events(job.id)]
    logger.info("判定依据: 启动收口事件=%s", [e.value for e in event_types])
    assert EventType.CANCEL_REQUESTED in event_types
    assert event_types[-1] == EventType.CANCELLED

    # 收口后不得再被领取执行
    assert q2.store.claim_next(time.time(), 1.0) is None
    q2.store.close()


def test_runtime_reaper_reclaims_stale_leased_job(db_path):
    """服务持续运行: 调小失联阈值后, reaper 周期收回长时间无进展的任务。"""
    q = make_queue(db_path, lease_seconds=0.1, stale_lease_seconds=0.1)
    job = q.submit({"task": "stuck"})
    stuck = q.store.claim_next(time.time(), 0.1)
    assert stuck is not None
    assert q.get(job.id).attempts == 1

    async def scenario():
        executed = []

        async def handler(payload):
            executed.append(payload["task"])
            return "ok"

        # poll_interval 调大: 工作协程不干扰, 先观察 reaper 独立收回
        pool = WorkerPool(q, handler, concurrency=2, poll_interval=0.005,
                          reap_interval=0.02)
        await pool.start()

        def reclaimed_event():
            return [e for e in q.events(job.id)
                    if e.event == EventType.LEASE_RECLAIMED]

        # PENDING 是瞬态(收回后几毫秒内就可能被重新领取), 以审计记录判定收回
        ok_reclaim = await wait_for(lambda: len(reclaimed_event()) == 1, timeout=2.0)
        assert ok_reclaim
        assert reclaimed_event()[0].attempt == 1, "收回不增加也不回退尝试次数"
        logger.info(
            "判定依据: reaper 收回 job=%s, 记录 attempt=%d, 之后重新可领",
            job.id, reclaimed_event()[0].attempt,
        )
        ok = await wait_for(lambda: q.get(job.id).state == JobState.SUCCEEDED)
        await pool.stop()
        return executed, ok

    executed, ok = run(scenario())
    assert ok
    assert executed == ["stuck"]
    final = q.get(job.id)
    assert final.attempts == 2, "收回后重新领取才算新一次尝试"
    assert final.state == JobState.SUCCEEDED
    q.store.close()


def test_runtime_reaper_respects_non_expired_lease(db_path):
    """边界: 租约未到期的任务不被收回, 阈值可调且行为稳定。"""
    q = make_queue(db_path, lease_seconds=10.0, stale_lease_seconds=10.0)
    job = q.submit({"task": "healthy"})
    q.store.claim_next(time.time(), 10.0)
    assert q.reclaim_stale() == []  # 立即扫描, 租约远未到期
    assert q.get(job.id).state == JobState.LEASED
    # 以一个未来的 now 模拟阈值过后, 才会被收回
    reclaimed = q.reclaim_stale(time.time() + 11.0)
    assert [j.id for j in reclaimed] == [job.id]
    assert q.get(job.id).state == JobState.PENDING
    logger.info("判定依据: job=%s 未到期不收回, 过期后一次收回", job.id)
    q.store.close()


def test_late_write_from_old_holder_after_reclaim_has_no_effect(db_path):
    """旧执行者在收回后晚到: 完成/失败写入全部被拒, 无二次执行与状态回退。"""
    q1 = make_queue(db_path, lease_seconds=0.01)
    job = q1.submit({"task": "zombie"})
    stale = q1.store.claim_next(time.time(), 0.01)
    time.sleep(0.03)
    # 模拟同一服务运行期 reaper 收回
    reclaimed = q1.reclaim_stale()
    assert [j.id for j in reclaimed] == [job.id]
    assert q1.get(job.id).state == JobState.PENDING

    # 新执行者领取并完成
    fresh = q1.store.claim_next(time.time(), 5.0)
    assert fresh is not None and fresh.lease_token != stale.lease_token

    # 旧执行者晚到: 成功写入被拒
    assert not q1.store.complete(job.id, stale.lease_token, "stale-result")
    # 旧执行者晚到: 失败重排写入也被拒(且 token 不匹配)
    stale_job = stale
    stale_job.state = JobState.PENDING
    stale_job.run_at = time.time()
    stale_job.lease_token = stale.lease_token
    assert not q1.store.fail(stale_job)
    assert q1.get(job.id).state == JobState.LEASED

    assert q1.store.complete(job.id, fresh.lease_token, "fresh-result")
    final = q1.get(job.id)
    assert final.state == JobState.SUCCEEDED
    assert final.result == "fresh-result"
    logger.info("判定依据: job=%s 旧执行者晚到写入被拒, 新结果生效", job.id)
    q1.store.close()


def test_repeated_reap_scan_is_idempotent(db_path):
    """重复扫描: 收回只发生一次, 审计只记一条 lease_reclaimed。"""
    q = make_queue(db_path, lease_seconds=0.01)
    job = q.submit({"task": "scan-twice"})
    q.store.claim_next(time.time(), 0.01)
    time.sleep(0.03)
    first = q.reclaim_stale()
    second = q.reclaim_stale()  # 已经是 PENDING, 扫描无效果
    assert len(first) == 1 and second == []
    reclaims = [e for e in q.events(job.id) if e.event == EventType.LEASE_RECLAIMED]
    assert len(reclaims) == 1
    logger.info("判定依据: job=%s 重复扫描仅产生 1 条收回记录", job.id)
    q.store.close()
