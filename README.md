# Durable Job Queue (Python)

本地异步任务处理与调度子系统: FastAPI 入口 + asyncio 工作协程池 +
内嵌 SQLite 持久化, 保证提交、执行、重试、取消与故障恢复在各入口下
表现一致且可预期。

## 结构

```
main.py               FastAPI 入口(提交/查询/取消/审计/死信列表/重放)
jobqueue/
  models.py           Job / JobState / FailureKind / FailureRecord /
                      EventType / AuditEvent / CancelEffect / CancelResult
  errors.py           对外可区分的错误类型(含 code)
  backoff.py          可配置、单调递增、有上限的退避策略
  store.py            SQLiteStore: 条件化状态迁移 + 只追加审计表
  service.py          JobQueue: 幂等提交、背压、取消、重放、启动恢复、失联收回
  worker.py           WorkerPool: 并发领取、超时中断、失败归一化、周期失联扫描
tests/                并发/幂等/重试死信/取消竞态/重启恢复/运行期自愈/取消终态/审计
```

## 任务状态机

```
PENDING --领取--> LEASED --成功--> SUCCEEDED (终态)
PENDING --取消--> CANCELLED (终态)
LEASED  --失败且可重试--> PENDING (run_at = now + backoff)
LEASED  --失败且耗尽--> DEAD
LEASED  --取消--> CANCELLED (执行被中断, 取消优先于成功)
LEASED  --启动恢复/失联收回--> PENDING (attempts 不增, 旧租约作废)
LEASED  --收回时已受理取消--> CANCELLED
DEAD    --人工重放--> PENDING (attempts 归零, 失败历史保留)
```

- 终态为 `SUCCEEDED` / `CANCELLED`; 重复取消终态任务不报错也不改变结果。
- 取消语义区分: 排队中的任务立即置 `CANCELLED`; 已领取的任务先置
  `cancel_requested` 标记, 由持有它的执行者在执行结束后落 `CANCELLED`
  (取消优先于成功)。
- 所有迁移都是带前置条件的单条 SQL(`WHERE state=? AND lease_token=?`),
  不会出现半更新或状态越位。

### 取消最终一致

取消一旦受理, 结果必收口为 `CANCELLED`, 不受处理器后续行为影响:

| 处理器紧接着…          | 结果                                    |
|------------------------|-----------------------------------------|
| 成功返回               | 成功写入带 `cancel_requested=0` 守卫, 被拒, 落 `CANCELLED` |
| 抛业务异常             | 不重排、不重试, 落 `CANCELLED`          |
| 执行超时               | 不按超时重试, 落 `CANCELLED`            |
| 正好卡在完成边界       | 取消与完成均为条件写, 谁先生效谁落定, 取消受理后不可能成功 |

- 排队中取消返回 `cancel_effect="cancelled"`(立即终态);
  执行中取消返回 `"accepted"`(已受理, 稍后收口), 两种语义可区分。
- 并发重复取消执行中任务只有**第一次**返回 `accepted` 并写一条受理审计,
  其余幂等返回 `cancelled`; 取消成功/已取消/死信任务同样幂等不改写。

## 运行期自愈

- **启动恢复(默认开启, 随正常启动流程执行)**:`recover()` 立即收回上一个
  进程残留的**全部** `LEASED` 任务——不需要先等租约过期, 因此默认配置下
  任务不会长期停在执行中。未受理取消的回到 `PENDING` 立即可再次执行;
  已受理取消的直接收口 `CANCELLED`。收回不增加 `attempts`, 新执行者重新
  领取时才计入下一次尝试。
- **运行期失联收回**:`WorkerPool` 内置一个周期扫描协程(默认 5s), 把
  `lease_expires_at` 超过 `stale_timeout_seconds`(默认等于租约时长)仍无
  进展的执行中任务安全收回。重复扫描幂等。
- **不会二次执行/状态回退**: 收回即作废旧 `lease_token`, 旧执行者晚到的
  成功/失败/取消写入都被 `state + lease_token` 前置条件拒绝; 状态始终以
  存储为准。扫描在独立线程执行, 与领取/完成并发安全。

## 幂等提交

- `idempotency_key` 唯一约束; 同键同载荷的重复提交返回同一任务, 不产生并发执行。
- 同键不同载荷抛出 `IdempotencyConflict`(`code=idempotency_conflict`, HTTP 409)。

## 重试与死信

- 退避: `delay = min(base * factor^(attempt-1), max_seconds)`, 单调递增、可配置。
- `attempts >= max_attempts` 的失败任务进入 `DEAD`, 可通过 `GET /dead` 查询。
- 重放(`POST /jobs/{id}/replay`)将任务重置为 `PENDING`、`attempts=0`,
  `failure_history` 与 `last_failure` 完整保留, 原始失败原因不丢失。

## 生命周期审计

所有关键变化都写入只追加表 `job_events`, 与状态迁移在**同一事务**内提交,
因此"状态变了但没记录"或"有记录但状态没变"都不会发生:

| event             | 时机                 | attempt 含义       |
|-------------------|----------------------|--------------------|
| `submitted`       | 提交受理             | 0                  |
| `claimed`         | 被执行者领取         | 第几次尝试         |
| `retry_scheduled` | 失败按退避重排       | 本次失败的尝试次数 |
| `dead_lettered`   | 重试耗尽进入死信     | 最后一次尝试次数   |
| `replayed`        | 人工重放死信         | 0(计数已归零)      |
| `cancel_requested`| 执行中取消被受理     | 当时尝试次数       |
| `cancelled`       | 取消最终生效         | 当时尝试次数       |
| `reclaimed`       | 自愈收回(启动/失联)  | 收回时尝试次数     |

- 查询范围: `GET /jobs/{id}/events` 查单任务; `GET /events?after_id=&limit=`
  按自增 id 时间顺序翻页查全局流。Python 侧对应 `queue.get_events(id)` 与
  `queue.list_events(after_id=, limit=)`。
- 记录只含事件类型、任务标识、尝试次数、可公开原因、时间戳; **不含任务
  载荷**, 也不含内部异常堆栈(失败原因是归一化后的"异常类型: 消息")。
- 记录随 SQLite 持久化, 重启不丢; 存储层只有 `INSERT`, 没有任何
  `UPDATE/DELETE` 路径, 历史行不可被后续操作改写。

## 失败归一化

| FailureKind        | 含义                       |
|--------------------|----------------------------|
| `handler_error`    | 执行器业务异常(类型+消息)  |
| `handler_timeout`  | 执行超时, 被 wait_for 中断 |
| `cancelled`        | 执行期间被取消             |
| `internal`         | 框架内部异常(不向外透出)   |

HTTP 层只返回 `{error: {code, message}}`; 未预期异常统一收敛为
`internal_error`, 内部堆栈不透出。

## 背压

积压(`PENDING + LEASED`)达到 `capacity` 时, 提交按确定策略直接拒绝并抛出
`QueueFull`(`code=queue_full`, HTTP 429), 内存不随积压无界增长。

## 租约与恢复

- 每个被领取的任务持有 `(lease_token, lease_expires_at)`。
- 启动恢复立即收回上次进程残留的全部执行中任务(详见"运行期自愈"),
  终态任务不受影响。
- 旧进程即使存活, 其租约 token 已无法通过条件更新, 写入被拒绝,
  因此恢复不会导致同一任务被重复生效。

## 运行

```bash
uv run uvicorn main:app --port 8000
```

环境变量(全部向后兼容, 不设置时保持原行为):

| 变量                          | 默认值     | 说明                         |
|-------------------------------|------------|------------------------------|
| `JOBQUEUE_DB`                 | jobqueue.db | SQLite 路径                |
| `JOBQUEUE_CAPACITY`           | 1000       | 提交背压容量                 |
| `JOBQUEUE_LEASE_SECONDS`      | 30         | 租约时长                     |
| `JOBQUEUE_STALE_TIMEOUT`      | 同租约时长 | 失联判定阈值, 调小可快速复现收回 |
| `JOBQUEUE_RECLAIM_INTERVAL`   | 5          | 运行期失联扫描周期(秒)       |

代码内对应 `JobQueue(..., lease_seconds=, stale_timeout_seconds=)` 与
`WorkerPool(..., reclaim_interval=, enable_reclaimer=False 可关闭扫描)`。

```bash
curl -X POST localhost:8000/jobs -H 'content-type: application/json' \
     -d '{"payload": {"n": 1}, "idempotency_key": "k1", "max_attempts": 3}'
curl localhost:8000/jobs/<id>
curl -X POST localhost:8000/jobs/<id>/cancel     # 返回多带 cancel_effect
curl localhost:8000/jobs/<id>/events            # 单任务生命周期审计
curl 'localhost:8000/events?after_id=0&limit=500'
curl localhost:8000/dead
curl -X POST localhost:8000/jobs/<id>/replay
```

### 本地复现自愈与失联收回

- **真实重启自愈**: 直接运行 `tests/test_self_healing.py` 中
  `test_default_startup_reclaims_unexpired_lease_immediately` /
  `test_restart_self_healed_job_runs_once_to_success`: 用 30s 长租约领取后
  关闭第一个 `JobQueue`(模拟进程被杀), 新建实例调用 `recover()`, 任务
  立即回到 `PENDING`, 无需等待。
- **运行期失联**: 将 `lease_seconds`/`stale_timeout_seconds` 调到 0.2s、
  `reclaim_interval` 调到 0.03s, 处理器首次调用 sleep 5s 模拟失联,
  约 0.2s 后任务被周期扫描收回并由其他协程再次执行(见
  `test_runtime_reclaimer_reaps_stale_leased_job`)。

## 验证方法

```bash
uv run pytest tests/ -q
```

- `test_submission.py`  幂等提交、幂等冲突、背压拒绝、取消幂等与语义
- `test_retry_deadletter.py`  退避单调性、重试耗尽进死信、超时归类、
  重放保留失败原因
- `test_concurrency.py`  8 协程并发领取 50 任务恰好各执行一次、
  取消已领取任务、取消/完成竞态终态一致
- `test_recovery.py`  重启回收租约、终态不回退、旧租约写入被拒
- `test_self_healing.py`  默认配置真实重启立即自愈、运行期失联收回、
  重复扫描幂等、旧执行者晚到写入被拒、已取消任务收回即收口
- `test_cancel_consistency.py`  取消后成功/异常/超时/完成边界四类竞态、
  并发重复取消幂等、终态取消幂等、排队中与执行中返回语义区分
- `test_audit.py`  全生命周期事件顺序与尝试次数、死信/重放/取消事件、
  重启持久化、历史不被回写、全局时间序翻页、无载荷/堆栈泄露

测试日志会打印任务标识、状态迁移与判定依据(`logging` INFO 级)。
