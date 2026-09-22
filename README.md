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
  store.py            SQLiteStore: 条件化状态迁移, 杜绝半更新/状态越位
  service.py          JobQueue: 幂等提交、背压、取消、重放、恢复
  worker.py           WorkerPool: 并发领取、超时中断、失败归一化
tests/                并发领取/重复提交/重试耗尽/取消竞态/重启恢复
```

## 任务状态机

```
PENDING --领取--> LEASED --成功--> SUCCEEDED (终态)
PENDING --取消--> CANCELLED (终态)
LEASED  --失败且可重试--> PENDING (run_at = now + backoff)
LEASED  --失败且耗尽--> DEAD
LEASED  --取消--> CANCELLED (执行被中断, 取消优先于成功)
DEAD    --人工重放--> PENDING (attempts 归零, 失败历史保留)
```

- 终态为 `SUCCEEDED` / `CANCELLED`; 重复取消终态任务不报错也不改变结果。
- 取消语义区分: 排队中的任务立即置 `CANCELLED`; 已领取的任务先置
  `cancel_requested` 标记, 由持有它的执行者在执行结束后落 `CANCELLED`
  (取消优先于成功)。
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

## 恢复语义

- 每个被领取的任务持有 `(lease_token, lease_expires_at)`。
- 进程重启后调用 `recover()`: 租约已过期的 `LEASED` 任务回到 `PENDING`,
  不会被静默丢弃; 终态任务不受影响。
- 旧进程即使存活, 其租约 token 已无法通过条件更新, 写入被拒绝,
  因此恢复不会导致同一任务被重复生效。

## 运行

```bash
uv run uvicorn main:app --port 8000
# 环境变量: JOBQUEUE_DB(默认 jobqueue.db), JOBQUEUE_CAPACITY(默认 1000)
```

```bash
curl -X POST localhost:8000/jobs -H 'content-type: application/json' \
     -d '{"payload": {"n": 1}, "idempotency_key": "k1", "max_attempts": 3}'
curl localhost:8000/jobs/<id>
curl -X POST localhost:8000/jobs/<id>/cancel
curl localhost:8000/dead
curl -X POST localhost:8000/jobs/<id>/replay
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

测试日志会打印任务标识、状态迁移与判定依据(`logging` INFO 级)。
