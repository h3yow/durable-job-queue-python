"""生命周期审计: 关键变化可按时间顺序查询, 持久化且只追加不可回写。

审计记录只包含事件类型、尝试次数、可公开原因与时间戳;
不包含任务载荷与内部异常堆栈。
"""
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

logger = logging.getLogger("test.audit")


def run(coro):
    return asyncio.run(coro)


async def wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.005)
    return False


def make_queue(db_path, **kw):
    return JobQueue(
        SQLiteStore(db_path),
        capacity=100,
        backoff=BackoffPolicy(base_seconds=0.01, factor=2.0, max_seconds=0.1),
        lease_seconds=kw.pop("lease_seconds", 0.2),
        **kw,
    )


def _kinds(events):
    return [e.event for e in events]


def test_full_lifecycle_events_in_order_with_attempts(queue):
    """提交->领取->失败重排->再领取->成功: 事件按时间顺序且带尝试次数。"""
    async def scenario():
        calls = {"n": 0}

        async def handler(payload):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient glitch")
            return "ok"

        pool = WorkerPool(queue, handler, concurrency=1, poll_interval=0.005,
                          enable_reclaimer=False)
        await pool.start()
        job = queue.submit({"task": "lifecycle"}, max_attempts=3)
        ok = await wait_for(lambda: queue.get(job.id).state == JobState.SUCCEEDED)
        await pool.stop()
        return job, ok

    job, ok = run(scenario())
    events = queue.get_events(job.id)
    kinds = _kinds(events)
    logger.info("判定依据: job=%s 事件序列=%s", job.id, [k.value for k in kinds])
    assert ok
    assert kinds == [
        EventType.SUBMITTED,
        EventType.CLAIMED,
        EventType.RETRY_SCHEDULED,
        EventType.CLAIMED,
        EventType.SUCCEEDED,
    ]
    assert events[0].attempt == 0
    assert events[1].attempt == 1
    assert events[2].attempt == 1
    assert events[3].attempt == 2
    assert events[4].attempt == 2
    # id 严格递增即时间顺序
    assert [e.id for e in events] == sorted(e.id for e in events)
    assert all(e.created_at > 0 for e in events)


def test_dead_letter_and_replay_leave_public_reason(queue):
    """进入死信与人工重放都有记录, 原因可公开; 载荷敏感值不进审计。"""
    secret = "super-secret-token"

    async def handler(payload):
        # 异常消息只含可公开的归一化描述, 不含载荷
        raise RuntimeError("dependency unavailable")

    async def scenario():
        pool = WorkerPool(queue, handler, concurrency=1, poll_interval=0.005,
                          enable_reclaimer=False)
        await pool.start()
        job = queue.submit({"task": "dying", "secret": secret}, max_attempts=2)
        await wait_for(lambda: queue.get(job.id).state == JobState.DEAD)
        await pool.stop()
        return job

    job = run(scenario())
    queue.replay(job.id)
    events = queue.get_events(job.id)
    kinds = _kinds(events)
    logger.info("判定依据: job=%s 事件=%s", job.id,
                [(e.event.value, e.attempt, e.reason) for e in events])
    assert kinds.count(EventType.DEAD_LETTERED) == 1
    assert kinds[-1] == EventType.REPLAYED
    dead_event = next(e for e in events if e.event == EventType.DEAD_LETTERED)
    assert dead_event.attempt == 2
    assert "RuntimeError" in (dead_event.reason or "")
    # 只追加表里不得出现载荷敏感值, 也不得包含内部堆栈
    raw = queue.store.conn_export_events_raw(job.id)
    blob = repr(events) + repr(raw)
    assert secret not in blob
    assert "Traceback" not in blob


def test_cancel_events_recorded_for_queued_and_leased(queue):
    """排队中取消: 直接 CANCELLED 事件; 执行中取消: REQUESTED + CANCELLED。"""
    queued = queue.submit({"task": "q"})
    queue.cancel(queued.id)
    qevents = _kinds(queue.get_events(queued.id))
    assert qevents == [EventType.SUBMITTED, EventType.CANCELLED]

    async def scenario():
        started = asyncio.Event()

        async def handler(payload):
            started.set()
            await asyncio.sleep(0.1)
            return "x"

        pool = WorkerPool(queue, handler, concurrency=1, poll_interval=0.005,
                          enable_reclaimer=False)
        await pool.start()
        leased = queue.submit({"task": "l"})
        await asyncio.wait_for(started.wait(), 2)
        queue.cancel(leased.id)
        ok = await wait_for(lambda: queue.get(leased.id).state == JobState.CANCELLED)
        await pool.stop()
        return leased, ok

    leased, ok = run(scenario())
    levents = _kinds(queue.get_events(leased.id))
    logger.info("判定依据: 排队取消=%s 执行中取消=%s",
                [e.value for e in qevents], [e.value for e in levents])
    assert ok
    assert levents == [
        EventType.SUBMITTED, EventType.CLAIMED,
        EventType.CANCEL_REQUESTED, EventType.CANCELLED,
    ]


def test_events_persist_across_restart_and_are_not_rewritten(db_path):
    """重启后审计记录不丢; 后续操作只追加, 历史行 id/时间/原因不被改写。"""
    q1 = make_queue(db_path)
    job = q1.submit({"task": "persist"})
    leased = q1.store.claim_next(time.time(), 0.01)
    q1.store.complete(leased.id, leased.lease_token, {"v": 1})
    before = q1.store.conn_export_events_raw(job.id)
    assert [r[2] for r in before] == ["submitted", "claimed", "succeeded"]
    q1.store.close()

    # 重启: 恢复 + 后续取消/重放等操作都只能追加, 不能改动历史行
    q2 = make_queue(db_path)
    q2.recover()
    q2.cancel(job.id)  # 已成功, 幂等无事件
    after = q2.store.conn_export_events_raw(job.id)
    logger.info("判定依据: 重启前后审计行对比 before=%d after=%d 内容一致=%s",
                len(before), len(after), before == after)
    assert after == before, "历史记录在重启与后续操作后保持完全一致"

    # 再跑一个新任务验证恢复事件也能持久化, 且跨重启可查
    j2 = q2.submit({"task": "second"})
    leased2 = q2.store.claim_next(time.time(), 0.01)
    time.sleep(0.02)
    q2.store.close()
    q3 = make_queue(db_path)
    q3.recover()
    ev2 = [e.event for e in q3.get_events(j2.id)]
    assert EventType.SUBMITTED in ev2 and EventType.CLAIMED in ev2
    # 注意: j2 是在 q2 里残留 LEASED, q3 启动恢复会产生 RECLAIMED
    assert EventType.RECLAIMED in ev2
    logger.info("判定依据: 跨重启可查事件=%s, 自愈收回事件也持久化",
                [e.value for e in ev2])
    q3.store.close()


def test_global_event_feed_is_chronological_and_paginatable(queue):
    """全局审计流按 id 时间顺序, 可按 after_id 翻页, 跨任务混排不乱序。"""
    jobs = [queue.submit({"n": i}) for i in range(5)]
    for i, j in enumerate(jobs):
        leased = queue.store.claim_next(time.time(), 1.0)
        assert leased is not None and leased.id == j.id
        assert queue.store.complete(j.id, leased.lease_token, i)

    first_page = queue.list_events(after_id=0, limit=6)
    assert len(first_page) == 6
    second_page = queue.list_events(after_id=first_page[-1].id, limit=6)
    assert len(second_page) == 6
    third_page = queue.list_events(after_id=second_page[-1].id, limit=6)
    assert len(third_page) == 3
    ids = [e.id for e in first_page + second_page + third_page]
    logger.info("判定依据: 15 条事件三页拉取 id 严格递增=%s", ids == sorted(ids))
    assert ids == sorted(ids)
    assert len(set(ids)) == 15
    # 全部提交事件先于领取/完成事件(提交在领取之前批量完成)
    assert all(e.event == EventType.SUBMITTED for e in first_page[:5])
    # 单任务视图与全局流一致
    for j in jobs:
        per_job = queue.get_events(j.id)
        assert _kinds(per_job) == [EventType.SUBMITTED, EventType.CLAIMED,
                                   EventType.SUCCEEDED]


def test_events_table_is_append_only_no_update_path(queue):
    """表结构语义层面: 应用代码不存在对 job_events 的 UPDATE/DELETE 路径。"""
    import inspect

    from jobqueue import store as store_mod

    source = inspect.getsource(store_mod.SQLiteStore)
    assert "INSERT INTO job_events" in source
    upper = source.upper()
    assert "UPDATE JOB_EVENTS" not in upper
    assert "DELETE FROM JOB_EVENTS" not in upper
    logger.info("判定依据: 存储层只含 INSERT INTO job_events, 无改写路径")
