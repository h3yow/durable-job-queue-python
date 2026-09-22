# 本地持久化异步任务队列

内嵌 SQLite（WAL）持久化的本地异步任务处理与调度子系统：提交、查询、取消、
失败重试、延迟重排、死信、人工重放、进程重启恢复，在 HTTP 入口与直接 API
调用两种入口下语义一致。所有状态流转都由"带前置状态条件的单条原子 UPDATE"
完成，不存在半更新与状态越位。

## 1. 任务状态机

```
                         ┌────────────────────────────────────────┐
                         │                                        │
 PENDING ──claim──▶ RUNNING ──succeed──────────────────────▶ SUCCEEDED（终态）
   │                 │  ▲                                      ▲
   │ cancel(queued)  │  │ replay backoff（attempt<max）         │
   ▼                 │  └─────────────────────────────── PENDING（重排，延迟可见）
 CANCELLED（终态）   │
                     ├─fail（attempt 达到上限 / permanent）──▶ DEAD（终态）
                     │                                        │
                     │ cancel(claimed)                         │ replay（人工）
                     ▼                                         ▼
                 CANCELLING ──worker ack──────────────────▶ CANCELLED（终态）
                                                            （失败历史保留）
```

| 状态 | 含义 | 出边 |
|---|---|---|
| `PENDING` | 等待被领取；`visible_at` 未到表示退避延迟中 | `RUNNING` / `CANCELLED` |
| `RUNNING` | 已被某一个执行者持有（`runner_id` 落盘） | `SUCCEEDED` / `PENDING`(重排) / `DEAD` / `CANCELLING` |
| `CANCELLING` | 执行中收到取消请求，等待持有者中断并确认 | `CANCELLED` |
| `SUCCEEDED` | 成功终态，结果不可变 | 无 |
| `DEAD` | 终态：重试耗尽 / 永久失败；**唯一出边**是人工重放 | `PENDING`（重放） |
| `CANCELLED` | 取消终态，不可变 | 无 |

关键约束（`jobqueue/models.py: ALLOWED_TRANSITIONS`）：

- 存储层对每次流转做前置状态 + 状态机表双重校验，非法迁移直接拒绝；
- 所有流转 SQL 形如 `UPDATE ... WHERE id=? AND status IN (...)`，
  竞态输掉（前置状态已变化）时返回空，上层按幂等处理，绝不覆盖已定结果。

## 2. 两种取消语义（明确区分）

| 场景 | 接口行为 | 结果 |
|---|---|---|
| 任务**排队中**（`PENDING`） | 原子 `PENDING→CANCELLED` | 任务永远不会被执行（`attempts=0`，无 `runner_id`） |
| 任务**已被领取**（`RUNNING`） | 原子 `RUNNING→CANCELLING`，并向持有者发协作中断信号 | 执行者在下一检查点中断执行协程，确认 `CANCELLING→CANCELLED` |
| `CANCELLING/CANCELLED/SUCCEEDED/DEAD` 重复取消 | 幂等 no-op，返回当前记录 | 不报错、不改变终态、不改变结果 |

取消与处理器完成发生竞态时，**以谁先成功落盘为准**，结局确定：

- 完成先落盘 → 任务 `SUCCEEDED`，取消返回 no-op，结果不变；
- 取消先落盘（`CANCELLING`）→ 即使处理器晚一步产出结果也不生效，最终 `CANCELLED`；
  失败结算晚到同样不覆盖取消结果。

HTTP `POST /tasks/{id}/cancel` 在 `semantics` 字段显式返回
`cancelled_while_queued` / `cancel_requested` / `already_settled`。

## 3. 幂等提交

- `POST /tasks` 可带 `idempotency_key`；`tasks.idempotency_key` 上有 **UNIQUE 索引**兜底。
- 相同 key + 相同 (`task_type`, `payload`)：返回**同一任务**（同一 id、同一执行），
  并发重复请求在提交锁 + 唯一索引双保护下也只产生一条记录、一次执行。
- 相同 key + 不同载荷：HTTP `409`，`error.reason=IDEMPOTENCY_PAYLOAD_MISMATCH`，
  并带回已存在任务的 `task_id`，原因与"任务不存在""队列满"可明确区分。

## 4. 失败归一化、重试与退避

执行器抛出的一切异常被 WorkerPool 归一化为可区分的 `FailureKind`：

| FailureKind | 触发条件 | 是否重试 |
|---|---|---|
| `HANDLER_ERROR` | 处理器抛出普通异常（消息为 `类型名: 文本`，截断 2000 字符，不含堆栈） | 是 |
| `PERMANENT_ERROR` | 处理器抛 `PermanentJobError`，或任务类型未注册处理器 | 否，直接 DEAD |
| `TIMEOUT` | 执行超过该类型超时（`asyncio.wait` 包裹并取消执行协程） | 是 |
| `CANCELLED` | 执行中被取消请求中断 | 结算为 CANCELLED，不计失败 |
| `RECOVERED` | 重启时发现上次进程在 `RUNNING` 中消失 | 见第 6 节 |

内部异常（存储错误等）统一包装为 `INTERNAL_ERROR`，不向响应透出堆栈与内部细节。

**退避**（`BackoffPolicy`，可配置）：

```
delay(n) = min(max_delay, base_delay * factor^(n-1))
```

- `factor >= 1`（构造期强制校验），封顶 `max_delay`，故延迟序列**单调非递减**；
- 默认 `jitter=0`，序列完全确定，便于测试复现；开启抖动也用确定性系数且不破坏单调性；
- 重排任务写 `visible_at=now+delay`，在到点之前不会被任何 worker 领取。

**尝试次数**：每次成功领取（`PENDING→RUNNING`）`attempts += 1`。
`attempts >= max_attempts` 的失败进入 `DEAD`。每次失败都原子追加一条
`{kind, reason, attempt, at}` 到 `failures` 历史，`GET /tasks/{id}` 可查。

## 5. 死信与人工重放

- `GET /dead-letter` 分页查询死信；每条死信都带完整失败历史与 `last_failure_*`。
- `POST /tasks/{id}/replay` 仅允许对 `DEAD` 操作，否则 `409 TASK_NOT_DEAD`：
  - 流转 `DEAD→PENDING`，`attempts` 归零、立即重新可领取；
  - **`failures` 历史原样保留**，原始失败原因（含历次 kind）不丢失，审计可追溯；
  - 若重放后再次失败，失败历史继续在原有基础上追加。

## 6. 进程重启恢复（at-least-once 执行）

队列初始化（`JobQueue.initialize()`，早于任何 worker 启动）时在单事务内执行一次：

1. 扫描 `RUNNING` 孤儿（上次进程崩溃 / 被 `kill -9`，执行结果未知）：
   - 追加 `RECOVERED` 失败记录；
   - `attempts += 1`、`runner_id=NULL`、立即可见：
     - 未达上限 → `RUNNING→PENDING`，重新参与领取；
     - 因此达到上限 → 直接 `RUNNING→DEAD`（仍可重放，RECOVERED 原因保留）。
2. 扫描 `CANCELLING`：取消信号已落盘但确认协程随进程消失 → 统一收尾为 `CANCELLED`。
3. `PENDING`（含退避中）任务原样保留，`visible_at` 不变。

不变量：

- 孤儿任务**不会被静默丢弃**；
- 恢复在领取前的单进程串行启动窗口执行，条件 UPDATE 保证**不会重复恢复**；
- 处理器语义为 **at-least-once**：崩溃时正在执行的处理器可能已产生部分副作用，
  恢复后会被再次调用，因此**处理器应自行保证幂等**（配合幂等提交键使用）；
  系统保证的是"不会因调度重复而双跑"——同一行任务在任意时刻最多被一个
  runner 持有，恢复后也只重新进入一次领取队列。

## 7. 并发领取与一致性

- 领取是单条条件 UPDATE 抢占：
  `UPDATE tasks SET status='RUNNING', attempts=attempts+1, runner_id=?
   WHERE id=(最早可见的 PENDING) AND status='PENDING' RETURNING *`，
  在 `BEGIN IMMEDIATE` 事务内完成。多 worker 并发时数据库写锁串行化抢占，
  同一任务任意时刻最多一个持有者。
- SQLite：单连接 + 进程内锁，WAL 模式，`busy_timeout=30s`。
- 进程内取消信号（`asyncio.Event`）由 worker 在领取前注册；即使信号与领取
  竞态，worker 在执行前会重读状态，对已处于 `CANCELLING` 的任务直接确认取消。

## 8. 入口背压与容量上限

`QueueOptions`：

| 参数 | 默认 | 说明 |
|---|---|---|
| `capacity` | 1000 | 积压上限：系统中 `PENDING` 任务数（**含退避延迟中**，保守计数） |
| `overflow_policy` | `REJECT` | `REJECT`：满了立即拒绝；`WAIT`：挂起等待空位 |
| `wait_timeout` | 5.0 | `WAIT` 最长等待秒数 |

- 拒绝原因可区分：`QUEUE_FULL_REJECT`（立即）与 `QUEUE_FULL_WAIT_TIMEOUT`（等待超时），
  HTTP 均为 `503`，响应体带 `backlog` / `capacity`。
- 名额在任务被领取（`PENDING→RUNNING`）或排队取消时释放，并唤醒 WAIT 等待者。
- 除数据库中的任务行外，队列不按积压持有任务对象；挂起的 WAIT 请求数受
  调用方并发与超时约束，不会随积压无界增长内存。

## 9. HTTP 接口

| 方法与路径 | 说明 | 关键状态码 / reason |
|---|---|---|
| `POST /tasks` | 提交（body：`task_type`,`payload`,`idempotency_key?`,`max_attempts?`） | 201；400 `UNKNOWN_TASK_TYPE`；409 `IDEMPOTENCY_PAYLOAD_MISMATCH`；503 背压 |
| `GET /tasks/{id}` | 查询完整记录（含 `failures` 历史） | 404 `TASK_NOT_FOUND` |
| `GET /tasks/idempotency/{key}` | 按幂等键查询 | 404 |
| `POST /tasks/{id}/cancel` | 取消（两种语义，见 `semantics`） | 404；终态 no-op 200 |
| `GET /dead-letter?limit=&offset=` | 死信列表 | 200 |
| `POST /tasks/{id}/replay` | 人工重放死信 | 409 `TASK_NOT_DEAD` |
| `GET /healthz` | 健康检查 | 200 |

错误统一形如：

```json
{"error": {"reason": "MACHINE_READABLE", "message": "...", "task_id": "..."}}
```

配置（环境变量）：`JOBQUEUE_DB`、`JOBQUEUE_CAPACITY`、`JOBQUEUE_MAX_ATTEMPTS`、
`JOBQUEUE_CONCURRENCY`。业务处理器在 `main.py: TASK_HANDLERS` 登记
（`task_type -> 同步或异步函数`，入参为 payload 字符串）。
未注册处理器的 `task_type` 在提交期即以 `400 UNKNOWN_TASK_TYPE` fail-fast；
未配置任何处理器时所有提交都会被拒绝（避免任务被接受后永远 PENDING）。

## 10. 验证方法

```bash
# 全量测试（自动打印任务标识、状态迁移、判定依据的 INFO 日志）
uv run pytest                          # 44 个用例
uv run pytest -k "concurrent"          # 并发领取 + 取消竞态
uv run pytest tests/test_idempotent_submit.py      # 重复提交 / 载荷冲突
uv run pytest tests/test_retry_deadletter.py       # 重试耗尽 / 超时 / 永久失败 / 重放
uv run pytest tests/test_recovery.py               # 同进程重启恢复
uv run pytest tests/test_recovery_subprocess.py    # 真·kill -9 跨进程恢复

# 手工运行服务
JOBQUEUE_DB=demo.db uvicorn main:app --reload
```

测试重点覆盖：并发领取恰好一次、重复/并发幂等提交、重试耗尽进死信、
排队取消与领取后取消的语义区分、取消/完成竞态、退避单调与延迟窗口、
两种背压拒绝原因、超时/普通/永久失败归类、以及崩溃后任务不丢失不重复执行。

单测日志通过 `jobqueue.transition` logger 输出，例如：

```
task=9601... transition RUNNING->DEAD reason=retry_exhausted attempts=3
  max_attempts=3 failure_kind=HANDLER_ERROR preserved_failures=3
task=5f37... transition DEAD->PENDING reason=manual_replay preserved_failures=3
task=11b2... transition RUNNING->PENDING reason=crash_recovery attempts=2
```
