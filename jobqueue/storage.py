"""SQLite 内嵌持久化存储。

设计要点：
- 单连接 + threading.RLock 串行化写入；WAL 模式提升读写并发。
- 领取 / 取消 / 状态流转全部是"带前置状态条件的单条 UPDATE"，
  数据库行锁 + WHERE 条件保证原子，杜绝半更新、状态越位与重复领取。
- 每次流转对照 ALLOWED_TRANSITIONS 校验，非法迁移直接拒绝。
- failures 以 JSON 数组追加保存，重放不丢原始失败原因。
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import uuid
from typing import Any

from jobqueue.models import ALLOWED_TRANSITIONS, TaskRecord, TaskStatus, utcnow

log = logging.getLogger("jobqueue.transition")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    task_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    idempotency_key TEXT UNIQUE,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    visible_at REAL NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_failure_kind TEXT,
    last_failure_reason TEXT,
    result TEXT,
    runner_id TEXT,
    failures TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_claim ON tasks(status, visible_at);
CREATE INDEX IF NOT EXISTS idx_idem ON tasks(idempotency_key);
"""


class StorageError(Exception):
    """存储层内部异常（不直接透出对外接口）。"""


def _iso() -> str:
    return utcnow().isoformat()


class SQLiteStorage:
    def __init__(self, path: str):
        self._path = path
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self._runner_uid = uuid.uuid4().hex

    # ---- 连接管理 ----
    def _connect(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        conn = sqlite3.connect(
            self._path,
            check_same_thread=False,
            isolation_level=None,  # autocommit；事务由显式 BEGIN/COMMIT 控制
            timeout=30.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        self._conn = conn
        return conn

    def initialize(self) -> None:
        with self._lock:
            conn = self._connect()
            conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    # ---- 行映射 ----
    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> TaskRecord:
        try:
            failures = tuple(json.loads(row["failures"] or "[]"))
        except json.JSONDecodeError:
            failures = ()
        return TaskRecord(
            id=row["id"],
            task_type=row["task_type"],
            payload=row["payload"],
            status=TaskStatus(row["status"]),
            idempotency_key=row["idempotency_key"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            visible_at=row["visible_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            last_failure_kind=row["last_failure_kind"],
            last_failure_reason=row["last_failure_reason"],
            result=row["result"],
            runner_id=row["runner_id"],
            failures=failures,
        )

    def _select(self, conn: sqlite3.Connection, task_id: str) -> TaskRecord | None:
        cur = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
        row = cur.fetchone()
        return self._row_to_record(row) if row is not None else None

    # ---- 写入 ----
    def _insert_task(self, rec: TaskRecord) -> str:
        """插入新任务；幂等键冲突时抛出 sqlite3.IntegrityError（由上层判别）。"""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """INSERT INTO tasks (id, task_type, payload, status,
                           idempotency_key, attempts, max_attempts, visible_at,
                           created_at, updated_at, failures)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        rec.id,
                        rec.task_type,
                        rec.payload,
                        rec.status.value,
                        rec.idempotency_key,
                        rec.attempts,
                        rec.max_attempts,
                        rec.visible_at,
                        rec.created_at,
                        rec.updated_at,
                        json.dumps(list(rec.failures), ensure_ascii=False),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise StorageError(f"insert conflict: {exc}") from exc
            return rec.id

    def _get_by_idempotency_key(self, key: str) -> TaskRecord | None:
        with self._lock:
            conn = self._connect()
            cur = conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key = ?", (key,)
            )
            row = cur.fetchone()
            return self._row_to_record(row) if row is not None else None

    def _get(self, task_id: str) -> TaskRecord | None:
        with self._lock:
            return self._select(self._connect(), task_id)

    def _count_pending(self) -> int:
        """积压量：所有可被领取/等待被领取的 PENDING 任务（含未到可见时间）。"""
        with self._lock:
            conn = self._connect()
            cur = conn.execute(
                "SELECT COUNT(*) AS c FROM tasks WHERE status = ?",
                (TaskStatus.PENDING.value,),
            )
            return int(cur.fetchone()["c"])

    def _claim_next(self, now: float, runner_id: str) -> TaskRecord | None:
        """原子领取：单条条件 UPDATE 抢占，任意时刻一个任务最多被一个执行者持有。"""
        with self._lock:
            conn = self._connect()
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    """SELECT id FROM tasks
                       WHERE status = ? AND visible_at <= ?
                       ORDER BY visible_at, rowid
                       LIMIT 1""",
                    (TaskStatus.PENDING.value, now),
                )
                row = cur.fetchone()
                if row is None:
                    conn.execute("COMMIT")
                    return None
                task_id = row["id"]
                cur = conn.execute(
                    """UPDATE tasks
                       SET status = ?, attempts = attempts + 1, runner_id = ?,
                           updated_at = ?
                       WHERE id = ? AND status = ?
                       RETURNING *""",
                    (
                        TaskStatus.RUNNING.value,
                        runner_id,
                        _iso(),
                        task_id,
                        TaskStatus.PENDING.value,
                    ),
                )
                updated = cur.fetchone()
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            if updated is None:  # 竞态下已被他人领取
                return None
            rec = self._row_to_record(updated)
            log.info(
                "task=%s transition PENDING->RUNNING reason=claim runner=%s attempt=%d",
                rec.id, runner_id, rec.attempts,
            )
            return rec

    def _transition(
        self,
        task_id: str,
        expected: tuple[TaskStatus, ...],
        status: TaskStatus,
        **fields: Any,
    ) -> TaskRecord | None:
        """通用条件流转：仅当当前状态属于 expected 时更新。

        - 非法迁移（状态机表中不存在）直接抛 StorageError；
        - 前置状态不满足（竞态输掉/重复请求）返回 None，由上层按幂等语义处理；
        - failures 以 append_* 特殊键追加，避免读改写竞态。
        """
        with self._lock:
            conn = self._connect()
            current = self._select(conn, task_id)
            if current is None:
                return None
            if current.status not in expected:
                return None
            if status not in ALLOWED_TRANSITIONS[current.status]:
                raise StorageError(
                    f"illegal transition task={task_id} "
                    f"{current.status.value}->{status.value}"
                )
            append_failure: dict | None = fields.pop("_append_failure", None)
            log_reason: str = fields.pop("_log_reason", "transition")
            sets: list[str] = ["status = ?", "updated_at = ?"]
            params: list[Any] = [status.value, _iso()]
            for key, value in fields.items():
                sets.append(f"{key} = ?")
                params.append(value)
            if append_failure is not None:
                sets.append(
                    "failures = json_insert(failures, '$[#]', json(?))"
                )
                params.append(
                    json.dumps(append_failure, ensure_ascii=False)
                )
            params.extend([task_id] + [s.value for s in expected])
            sql = (
                f"UPDATE tasks SET {', '.join(sets)} "
                f"WHERE id = ? AND status IN ({','.join('?' for _ in expected)})"
                " RETURNING *"
            )
            cur = conn.execute(sql, params)
            row = cur.fetchone()
            if row is None:
                return None
            rec = self._row_to_record(row)
            log.info(
                "task=%s transition %s->%s reason=%s attempt=%d",
                task_id, current.status.value, status.value, log_reason,
                rec.attempts,
            )
            return rec

    def _requeue_or_dead(
        self,
        task_id: str,
        *,
        now: float,
        delay: float,
        failure: dict[str, Any],
        force_dead: bool = False,
    ) -> tuple[TaskRecord, bool]:
        """执行失败后的判定：attempts < max_attempts 且非永久失败 => 延迟重排；
        否则死信。失败记录（kind/reason/at）原子追加到 failures 历史。

        返回 (最新记录, 是否死信)。任务不存在返回 StorageError。
        """
        with self._lock:
            conn = self._connect()
            conn.execute("BEGIN IMMEDIATE")
            try:
                current = self._select(conn, task_id)
                if current is None:
                    raise StorageError(f"task missing on fail: {task_id}")
                exhausted = force_dead or current.attempts >= current.max_attempts
                if exhausted:
                    new_status = TaskStatus.DEAD
                    extra: dict[str, Any] = {
                        "visible_at": current.visible_at,
                        "runner_id": None,
                    }
                else:
                    new_status = TaskStatus.PENDING
                    extra = {
                        "visible_at": now + delay,
                        "runner_id": None,
                    }
                if new_status not in ALLOWED_TRANSITIONS[current.status]:
                    raise StorageError(
                        f"illegal transition task={task_id} "
                        f"{current.status.value}->{new_status.value}"
                    )
                sets = [
                    "status = ?",
                    "updated_at = ?",
                    "last_failure_kind = ?",
                    "last_failure_reason = ?",
                    "failures = json_insert(failures, '$[#]', json(?))",
                ]
                params: list[Any] = [
                    new_status.value,
                    _iso(),
                    failure.get("kind"),
                    failure.get("reason"),
                    json.dumps(failure, ensure_ascii=False),
                ]
                for key, value in extra.items():
                    sets.append(f"{key} = ?")
                    params.append(value)
                params.append(task_id)
                cur = conn.execute(
                    f"UPDATE tasks SET {', '.join(sets)} WHERE id = ? RETURNING *",
                    params,
                )
                row = cur.fetchone()
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            rec = self._row_to_record(row)
            log.info(
                "task=%s transition %s->%s reason=%s attempts=%d max_attempts=%d "
                "failure_kind=%s preserved_failures=%d",
                task_id, current.status.value, new_status.value,
                ("permanent_dead" if force_dead
                 else "retry_exhausted" if exhausted else "retry_backoff"),
                rec.attempts, rec.max_attempts,
                failure.get("kind"), len(rec.failures),
            )
            return rec, exhausted

    def _replay_dead(self, task_id: str, now: float) -> TaskRecord | None:
        """人工重放死信：DEAD->PENDING，attempts 归零，失败历史保留。"""
        with self._lock:
            conn = self._connect()
            current = self._select(conn, task_id)
            if current is None or current.status is not TaskStatus.DEAD:
                return None
            cur = conn.execute(
                """UPDATE tasks
                   SET status = ?, attempts = 0, visible_at = ?, runner_id = NULL,
                       updated_at = ?
                   WHERE id = ? AND status = ?
                   RETURNING *""",
                (
                    TaskStatus.PENDING.value,
                    now,
                    _iso(),
                    task_id,
                    TaskStatus.DEAD.value,
                ),
            )
            row = cur.fetchone()
            if row is None:
                return None
            rec = self._row_to_record(row)
            log.info(
                "task=%s transition DEAD->PENDING reason=manual_replay "
                "preserved_failures=%d",
                task_id, len(rec.failures),
            )
            return rec

    def _recover_orphans(self, now: float) -> list[TaskRecord]:
        """进程启动恢复（必须在任何 worker 领取前调用一次）：

        - RUNNING（上次进程崩溃/被 kill，执行结果未知）-> PENDING，
          追加 RECOVERED 失败历史，attempts +1（消耗一次尝试预算，
          若因此达到上限则进死信），立即重新可领取；
        - CANCELLING（取消信号已落盘但执行协程随进程消失）-> CANCELLED。
        单进程串行启动 + 条件 UPDATE，保证不会重复执行。
        """
        recovered: list[TaskRecord] = []
        with self._lock:
            conn = self._connect()
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    "SELECT id FROM tasks WHERE status = ?",
                    (TaskStatus.RUNNING.value,),
                )
                running_ids = [r["id"] for r in cur.fetchall()]
                for task_id in running_ids:
                    failure = {
                        "kind": "RECOVERED",
                        "reason": "task was RUNNING when process restarted; "
                                  "result unknown, requeued exactly once",
                        "at": _iso(),
                    }
                    rec = self._row_to_record(
                        conn.execute("SELECT * FROM tasks WHERE id = ?",
                                     (task_id,)).fetchone()
                    )
                    exhausted = (rec.attempts + 1) >= rec.max_attempts
                    new_status = (
                        TaskStatus.DEAD if exhausted else TaskStatus.PENDING
                    )
                    cur2 = conn.execute(
                        """UPDATE tasks
                           SET status = ?, attempts = attempts + 1,
                               runner_id = NULL, visible_at = ?,
                               last_failure_kind = 'RECOVERED',
                               last_failure_reason = ?,
                               failures = json_insert(failures, '$[#]', json(?) ),
                               updated_at = ?
                           WHERE id = ? AND status = ?
                           RETURNING *""",
                        (
                            new_status.value,
                            now,
                            failure["reason"],
                            json.dumps(failure, ensure_ascii=False),
                            _iso(),
                            task_id,
                            TaskStatus.RUNNING.value,
                        ),
                    )
                    row = cur2.fetchone()
                    if row is not None:
                        out = self._row_to_record(row)
                        recovered.append(out)
                        log.warning(
                            "task=%s transition RUNNING->%s reason=crash_recovery "
                            "attempts=%d max_attempts=%d",
                            task_id, new_status.value,
                            out.attempts, out.max_attempts,
                        )
                cur = conn.execute(
                    """UPDATE tasks
                       SET status = ?, updated_at = ?, runner_id = NULL
                       WHERE status = ?
                       RETURNING *""",
                    (
                        TaskStatus.CANCELLED.value,
                        _iso(),
                        TaskStatus.CANCELLING.value,
                    ),
                )
                for row in cur.fetchall():
                    rec = self._row_to_record(row)
                    recovered.append(rec)
                    log.warning(
                        "task=%s transition CANCELLING->CANCELLED "
                        "reason=restart_cancel_ack",
                        rec.id,
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return recovered

    def _list_dead(self, limit: int, offset: int) -> list[TaskRecord]:
        with self._lock:
            conn = self._connect()
            cur = conn.execute(
                """SELECT * FROM tasks WHERE status = ?
                   ORDER BY updated_at DESC LIMIT ? OFFSET ?""",
                (TaskStatus.DEAD.value, int(limit), int(offset)),
            )
            return [self._row_to_record(r) for r in cur.fetchall()]

    # 仅供测试/诊断
    def runner_identity(self) -> str:
        return f"proc-{self._runner_uid}"
