"""运行期自愈: 默认配置真实重启收回 + 运行期失联周期性收回。

收回后保证:
- 任务重新可被领取执行, 不会长期停在 LEASED;
- 同一任务仍只被一个执行者真正生效(旧租约晚到写入被拒);
- attempts 不因收回而虚增;
- 已受理取消的任务收回即收口 CANCELLED。
"""
import asyncio
import logging
import time

from jobqueue import BackoffPolicy, EventType, JobQueue, JobState, SQLiteStore, WorkerPool

logger = logging.getLogger("test.self_healing")


def run(coro):
    return asyncio.run(coro)


async def wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.005)
    return False


def make_queue(db_path, *, lease_seconds=0.2, stale_timeout_seconds=None):
    return JobQueue(
        SQLiteStore(db_path),
        capacity=100,
        backoff=BackoffPolicy(base_seconds=0.01, factor=2.0, max_seconds=0.1),
        lease_seconds=lease_seconds,
        stale_timeout_seconds=stale_timeout_seconds,
    )


def test_default_startup_reclaims_unexpired_lease_immediately(db_path):
    """默认启动流程: 租约尚未过期也立即收回, 无需等待失联阈值。"""
    q1 = make_queue(db_path, lease_seconds=30.0)  # 长租约, 模拟进程刚领到就被杀
    job = q1.submit({"task": "aborted"})
    leased = q1.store.claim_next(time.time(), 30.0)
    assert leased is not None and leased.attempts == 1
    logger.info(
        "崩溃前: job=%s state=leased lease_expires_in=30s attempts=1", job.id
    )
    q1.store.close()

    q2 = make_queue(db_path, lease_seconds=30.0)
    recovered = q2.recover()  # 正常启动流程, 默认即全量收回
    assert [j.id for j in recovered] == [job.id]
    after = q2.get(job.id)
    logger.info(
        "判定依据: 重启后 job=%s state=%s attempts=%d (租约远未过期也已收回)",
        job.id, after.state.value, after.attempts,
    )
    assert after.state == JobState.PENDING
    assert after.attempts == 1, "收回本身不增加尝试次数"
    assert after.lease_token is None
    q2.store.close()


def test_restart_self_healed_job_runs_once_to_success(db_path):
    """真实重启端到端: 被收回的任务重新执行, 恰好生效一次。"""
    q1 = make_queue(db_path)
    job = q1.submit({"task": "recover-me"})
    stale_lease = q1.store.claim_next(time.time(), 30.0)
    q1.store.close()  # 进程异常退出, 处理器未完成

    q2 = make_queue(db_path)
    q2.recover()

    async def scenario():
        executed: list[str] = []

        async def handler(payload):
            executed.append(payload["task"])
            return "ok"

        pool = WorkerPool(q2, handler, concurrency=2, poll_interval=0.005,
                          enable_reclaimer=False)
        await pool.start()
        ok = await wait_for(lambda: q2.get(job.id).state == JobState.SUCCEEDED)
        await pool.stop()
        return executed, ok

    executed, ok = run(scenario())
    final = q2.get(job.id)
    logger.info("判定依据: job=%s 重启后执行序列=%s 终态=%s attempts=%d",
                job.id, executed, final.state.value, final.attempts)
    assert ok
    assert executed == ["recover-me"]
    assert final.attempts == 2, "第一次崩溃前领取 + 恢复后重新领取, 计数清晰"
    q2.store.close()


def test_runtime_reclaimer_reaps_stale_leased_job(queue):
    """服务持续运行: 失联阈值(调小后)到期, 周期扫描自动收回并再执行。"""
    async def scenario():
        calls = {"n": 0}

        async def handler(payload):
            calls["n"] += 1
            if calls["n"] == 1:
                await asyncio.sleep(5)  # 第一次执行"卡死", 模拟执行者失联
            return "second-run-ok"

        pool = WorkerPool(queue, handler, concurrency=2, poll_interval=0.005,
                          reclaim_interval=0.03)
        await pool.start()
        job = queue.submit({"task": "stale"})
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.SUCCEEDED,
                            timeout=5.0)
        await pool.stop()
        return job, calls["n"], ok

    job, runs, ok = run(scenario())
    final = queue.get(job.id)
    logger.info(
        "判定依据: job=%s 运行期失联收回后处理器调用=%d 终态=%s attempts=%d",
        job.id, runs, final.state.value, final.attempts,
    )
    assert ok
    assert runs == 2
    assert final.state == JobState.SUCCEEDED


def test_repeated_reclaim_scans_are_idempotent(queue):
    """重复扫描不产生二次收回或状态/尝试次数混乱。"""
    job = queue.submit({"task": "scan"})
    leased = queue.store.claim_next(time.time(), 0.01)
    assert leased is not None
    time.sleep(0.02)

    first = queue.reclaim_stale()
    second = queue.reclaim_stale()
    third = queue.reclaim_stale()
    states = [queue.get(job.id).state for _ in range(1)]
    logger.info("判定依据: 首次收回=%d 后续两次收回=%s 终态=%s attempts=%d",
                len(first), [len(second), len(third)], states[0].value,
                queue.get(job.id).attempts)
    assert len(first) == 1
    assert second == [] and third == []
    assert queue.get(job.id).state == JobState.PENDING
    assert queue.get(job.id).attempts == 1
    # 审计只记录一次收回
    events = queue.get_events(job.id)
    assert [e.event for e in events].count(EventType.RECLAIMED) == 1


def test_stale_holder_write_rejected_after_runtime_reclaim(queue):
    """运行期收回后旧执行者晚到的成功/失败/取消写入均不得生效。"""
    job = queue.submit({"task": "late"})
    stale = queue.store.claim_next(time.time(), 0.01)
    token = stale.lease_token
    time.sleep(0.02)
    reclaimed = queue.reclaim_stale()
    assert len(reclaimed) == 1
    fresh = queue.store.claim_next(time.time(), 30.0)
    assert fresh is not None and fresh.lease_token != token

    assert not queue.store.complete(job.id, token, "late-success")
    assert queue.get(job.id).state == JobState.LEASED
    stale.attempts = fresh.attempts
    stale.state = JobState.PENDING
    stale.run_at = time.time()
    stale.lease_token = token
    assert not queue.store.fail(stale), "旧租约失败重排也必须被拒"
    assert queue.get(job.id).state == JobState.LEASED
    assert queue.store.complete(job.id, fresh.lease_token, "fresh")
    logger.info("判定依据: job=%s 旧执行者晚到写入全部被拒, 新执行者生效", job.id)
    assert queue.get(job.id).state == JobState.SUCCEEDED


def test_reclaim_of_cancel_requested_job_closes_to_cancelled(queue):
    """已受理取消的执行中任务被收回时直接收口 CANCELLED, 不再被领取。"""
    job = queue.submit({"task": "cancel-then-crash"})
    leased = queue.store.claim_next(time.time(), 0.01)
    res = queue.cancel(job.id)
    assert res.effect.value == "accepted"
    time.sleep(0.02)  # 执行者失联, 租约过期

    reclaimed = queue.reclaim_stale()
    final = queue.get(job.id)
    events = [e.event for e in queue.get_events(job.id)]
    logger.info("判定依据: job=%s 收回=%s 终态=%s 审计=%s",
                job.id, [j.id for j in reclaimed], final.state.value,
                [e.value for e in events])
    assert [j.id for j in reclaimed] == [job.id]
    assert final.state == JobState.CANCELLED
    assert EventType.CANCEL_REQUESTED in events
    assert EventType.CANCELLED in events
    # 不会再被领取
    assert queue.store.claim_next(time.time(), 1.0) is None
