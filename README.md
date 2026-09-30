# Durable Job Queue (Python)

本地异步任务处理与调度子系统: FastAPI 入口 + asyncio 工作协程池 +
内嵌 SQLite 持久化, 保证提交、执行、重试、取消与故障恢复在各入口下
表现一致且可预期。

## 结构

```
main.py               FastAPI 入口(提交/查询/取消/死信列表/重放)
jobqueue/
  models.py           Job / JobState / FailureKind / FailureRecord
  errors.py           对外可区分的错误类型(含 code)
  backoff.py          可配置、单调递增、有上限的退避策略
  store.py            SQLiteStore: 条件化状态迁移 + append-only 审计, 杜绝半更新/状态越位
  service.py          JobQueue: 幂等提交、背压、取消、重放、启动自愈与运行期收回
  worker.py           WorkerPool: 并发领取、超时中断、失败归一化、周期失联收回
tests/                并发领取/重复提交/重试耗尽/取消竞态/完成边界确定性复现/重启恢复/自愈/取消终态/审计
```

## 任务状态机

```
PENDING --领取--> LEASED --成功--> SUCCEEDED (终态)
PENDING --取消--> CANCELLED (终态)
LEASED  --失败且可重试--> PENDING (run_at = now + backoff)
LEASED  --失败且耗尽--> DEAD
LEASED  --取消--> CANCELLED (执行被中断, 取消优先于成功)
LEASED  --启动自愈/失联收回--> PENDING (attempts 不变, 重新可领)
LEASED  --收回时已受理取消--> CANCELLED (不再执行)
DEAD    --人工重放--> PENDING (attempts 归零, 失败历史保留)
```

- 终态为 `SUCCEEDED` / `CANCELLED`; 重复取消终态任务不报错也不改变结果。
- 取消语义可从返回值区分: 排队中的任务立即置 `CANCELLED`(返回
  `state=cancelled, cancel_requested=false`); 已领取的任务先返回
  `state=leased, cancel_requested=true` 表示"取消已受理", 随后始终收口
  为 `CANCELLED`。
- **取消受理后必定收口为已取消(核心不变量)**: 执行中的任务一旦受理取消,
  无论处理器紧接着正常返回、抛异常还是超时, 终态只能是 `CANCELLED`——
  不会改回 `SUCCEEDED`、不会保留处理器返回值(收口 SQL 显式置
  `result=NULL`; 本次尝试的失败因写入被守卫拒绝而不会落库, 此前各次
  重试留下的历史失败记录保留)、不会重排重试或进入死信, `attempts`
  不增加也不回退。成功落库(`complete`)与失败落库(`fail`)的条件 UPDATE
  都带 `AND cancel_requested=0` 前置条件, 取消与"执行刚好结束"卡在同一
  时间点时由 SQL 原子判定只有一方生效: 被拒的一方检测到取消标记后改走
  `complete_cancel` 收口; 执行者若已失联, 则由自愈扫描带取消标记直接
  收口。取消受理(`cancel_requested`)之后的审计记录只可能是一条
  `cancelled`, 不可能再出现 `succeeded`/`retry_scheduled`/`dead_lettered`。
- 并发重复取消、对终态(`SUCCEEDED`/`CANCELLED`)或 `DEAD` 的取消都是
  幂等的: 不报错、不改变已有结果、不重复追加审计记录。
- 所有迁移都是带前置条件的单条 SQL(`WHERE state=? AND lease_token=?`),
  不会出现半更新或状态越位。

## 幂等提交

- `idempotency_key` 唯一约束; 同键同载荷的重复提交返回同一任务, 不产生并发执行。
- 同键不同载荷抛出 `IdempotencyConflict`(`code=idempotency_conflict`, HTTP 409)。

## 重试与死信

- 退避: `delay = min(base * factor^(attempt-1), max_seconds)`, 单调递增、可配置。
- `attempts >= max_attempts` 的失败任务进入 `DEAD`, 可通过 `GET /dead` 查询。
- 重放(`POST /jobs/{id}/replay`)将任务重置为 `PENDING`、`attempts=0`,
  `failure_history` 与 `last_failure` 完整保留, 原始失败原因不丢失。

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

## 自愈: 启动恢复与运行期失联收回

- 每个被领取的任务持有 `(lease_token, lease_expires_at)`。
- **启动自愈(默认配置)**: `recover()` 在正常启动流程中回收上次进程留下的
  **全部** `LEASED` 任务(本地单进程服务, 旧执行者已消失, 不必等待租约
  到期), 任务不会长期停在执行中。已受理取消的直接收口 `CANCELLED`,
  其余回到 `PENDING` 重新排队。
- **运行期自愈**: `WorkerPool` 内置后台扫描, 每隔 `reap_interval` 调用
  `reclaim_stale()`, 收回租约超过失联阈值(`stale_lease_seconds`, 默认等于
  `lease_seconds`, 即租约到期即失联; 可调小以更快收回)仍无进展的任务。
- 收回只做带 `state=LEASED` 前置条件的原子迁移, 因此重启、重复扫描、旧
  执行者晚到都不会二次执行或回退状态: 旧 token 的 `complete/fail` 写入会
  被拒; `attempts` 不回退也不增加, 重新被领取才算一次新尝试。
- 兼容方式: 构造 `JobQueue(..., startup_reclaim_all=False)` 可退回旧行为
  (启动只回收已到期租约)。

## 生命周期审计

`job_events` 表以 append-only 方式记录关键变化, SQLite 触发器
(`job_events_no_update/no_delete`)禁止更新与删除, 历史不可改写, 重启不丢。

| event             | 时机                         | attempt 含义       |
|-------------------|------------------------------|--------------------|
| `submitted`       | 提交受理                     | 0                  |
| `claimed`         | 被执行者领取                 | 第几次尝试         |
| `retry_scheduled` | 失败后排期重试               | 失败的那次尝试     |
| `dead_lettered`   | 重试耗尽进入死信             | 最后一次尝试       |
| `cancel_requested`| 执行中取消被受理(收口前)     | 当前尝试           |
| `cancelled`       | 取消收口为终态               | 当前尝试           |
| `replayed`        | 死信人工重放                 | 0(计数已归零)      |
| `lease_reclaimed` | 自愈收回失联租约, 重回排队   | 收回时的尝试次数   |

- 查询范围: `GET /jobs/{id}/events` 返回单个任务按时间顺序(`id` 递增)的
  全部记录; `GET /events?since_id=N&limit=M` 提供全局增量翻页查询。
- 每条记录只含 `event/attempt/reason/created_at`; `reason` 是归一化的
  可公开字符串(如 `handler_error: ValueError: x`)。**不记录任务载荷,
  也不记录内部异常堆栈**; 未预期错误在 HTTP 层统一为 `internal_error`。

## 配置项与兼容方式

| 参数 / 环境变量                              | 默认  | 说明                                   |
|----------------------------------------------|-------|----------------------------------------|
| `lease_seconds` / `JOBQUEUE_LEASE_SECONDS`   | 30    | 单次领取的租约时长                     |
| `stale_lease_seconds` / `JOBQUEUE_STALE_LEASE_SECONDS` | 同租约 | 失联判定阈值, 调小可更快收回 |
| `startup_reclaim_all` / `JOBQUEUE_STARTUP_RECLAIM_ALL` | `1`(真) | `0/false` 退回"仅回收到期租约"旧行为 |
| `reap_interval`(WorkerPool) / `JOBQUEUE_REAP_INTERVAL` | 1 | 运行期失联扫描周期(秒)       |
| `capacity` / `JOBQUEUE_CAPACITY`             | 1000  | 积压背压上限(原有)                     |

新增参数全部为可选关键字参数, 缺省值与原有行为兼容; 不设置任何新环境变量
时, 现有部署只多出"启动即回收全部执行中任务"和周期失联收回两项自愈能力。

## 运行

```bash
uv run uvicorn main:app --port 8000
# 环境变量: JOBQUEUE_DB(默认 jobqueue.db), JOBQUEUE_CAPACITY(默认 1000),
# JOBQUEUE_LEASE_SECONDS(30), JOBQUEUE_STALE_LEASE_SECONDS(同租约),
# JOBQUEUE_STARTUP_RECLAIM_ALL(1), JOBQUEUE_REAP_INTERVAL(1)
```

```bash
curl -X POST localhost:8000/jobs -H 'content-type: application/json' \
     -d '{"payload": {"n": 1}, "idempotency_key": "k1", "max_attempts": 3}'
curl localhost:8000/jobs/<id>
curl -X POST localhost:8000/jobs/<id>/cancel
curl localhost:8000/dead
curl -X POST localhost:8000/jobs/<id>/replay
curl localhost:8000/jobs/<id>/events          # 单任务生命周期审计
curl 'localhost:8000/events?since_id=0&limit=200'  # 全局增量审计
```

## 验证方法

```bash
uv run pytest tests/ -q
```

- `test_submission.py`  幂等提交、幂等冲突、背压拒绝、取消幂等
- `test_retry_deadletter.py`  退避单调性、重试耗尽进死信、超时归类、
  重放保留失败原因
- `test_concurrency.py`  8 协程并发领取 50 任务恰好各执行一次、
  取消已领取任务、取消/完成竞态终态一致
- `test_recovery.py`  重启回收过期租约、终态不回退、旧租约写入被拒
- `test_self_healing.py`  默认配置启动即回收(不等租约到期)、
  启动时取消收口、运行期 reaper 失联收回(阈值调小可复现)、
  未到期不收回的边界、旧执行者晚到写入被拒、重复扫描幂等
- `test_cancel_consistency.py`  取消对成功/异常/超时/完成边界竞态始终
  收口 CANCELLED、并发重复取消只受理一次、终态取消无副作用、
  排队/执行中返回语义可区分
- `test_cancel_boundary_race.py`  完成边界竞态的**确定性**复现:
  `RaceWindowStore`(仅测试用的存储子类)在 `complete`/`fail` 落库 SQL
  执行前的接缝点上, 从其它真实线程并发发起取消, 分别覆盖处理器成功、
  抛异常、超时三种情况, 以及 40 任务/8 协程/每任务 4 线程重复取消的
  真实并发压力; 断言"取消一旦受理, 终态只能是 CANCELLED、结果为
  NULL、attempts 不增不退、审计尾部只能是一条 cancelled"。另含两条
  存储层原子性用例, 直接断言取消受理后 `complete`/`fail` 写入被拒。

### 复现"取消受理后被改回成功"的缺陷并验证修复

历史缺陷: 执行者成功路径曾是"先 `get_job()` 检查标记、再 `complete()`
落成功"两步操作, 而 `complete()` 的条件 SQL 只校验 `state/lease_token`,
未校验 `cancel_requested`; 取消若恰好在两步之间被受理, 任务仍会落为
`SUCCEEDED` 并带出处理器结果(审计上表现为 `cancel_requested` 后又跟
一条 `succeeded`)。本地可这样复现修复前后的差异:

```bash
uv run pytest tests/test_cancel_boundary_race.py -q   # 修复后: 6 passed
# 临时去掉 store.complete() SQL 里的 "AND cancel_requested = 0" 后再跑:
# -> 3 failed (存储层成功写入 / 成功边界 / 真实并发), 日志可见
#    cancel_requested=True 之后紧接 leased->succeeded
```
- `test_audit.py`  完整生命周期事件序列、重启不丢、历史不被后续操作
  改写、触发器禁止 UPDATE/DELETE、不泄露载荷与堆栈、全局增量有序翻页

测试日志会打印任务标识、状态迁移与判定依据(`logging` INFO 级);
自愈相关日志以 `recover:`/`reclaim:`/`reaper:` 前缀输出任务标识、
收回后的状态与失联阈值判定依据。
