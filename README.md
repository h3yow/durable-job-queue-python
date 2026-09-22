# Durable Local Job Queue (Python)

内嵌 SQLite 持久化的本地异步任务队列：幂等提交、并发安全领取、退避重试、
死信/人工重放、两种取消语义、入口背压、进程崩溃恢复。详细语义见
[`docs/task-queue.md`](docs/task-queue.md)。

## 快速开始

```bash
uv sync
uv run pytest                      # 44 个测试，打印任务状态迁移日志

JOBQUEUE_DB=demo.db uv run uvicorn main:app --reload
```

提交与查询：

```bash
curl -s -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"task_type":"echo","payload":"hello","idempotency_key":"k1"}'
curl -s localhost:8000/tasks/<task_id>
curl -s -X POST localhost:8000/tasks/<task_id>/cancel
curl -s localhost:8000/dead-letter
curl -s -X POST localhost:8000/tasks/<task_id>/replay
```

业务处理器在 `main.py` 的 `TASK_HANDLERS` 中登记 `task_type -> handler`
（同步/异步均可）。处理器异常会被归一化为 `HANDLER_ERROR / PERMANENT_ERROR /
TIMEOUT` 等可区分类型；崩溃恢复语义为 at-least-once，处理器应保持幂等。

## 代码结构

```
jobqueue/models.py    状态机、TaskRecord、合法迁移表
jobqueue/errors.py    对外异常体系 + FailureKind 失败类型
jobqueue/retry.py     BackoffPolicy（可配置、单调非递减）
jobqueue/storage.py   SQLiteStorage（原子领取/条件流转/恢复/死信）
jobqueue/queue.py     JobQueue 门面（幂等/背压/取消/重放）
jobqueue/worker.py    WorkerPool（并发执行、超时中断、异常归一化）
main.py               FastAPI HTTP 入口（错误归一化）
tests/                并发/幂等/重试/取消竞态/重启恢复测试
docs/task-queue.md    状态机、重试死信、恢复、背压与验证说明
```
