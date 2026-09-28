"""内嵌持久化存储 (SQLite), 承接任务状态与失败历史。

并发与一致性约定:
- 所有写操作在单条 SQL 或显式事务内完成, 状态迁移带前置条件
  (state / lease_token), 杜绝半更新与状态越位。
- 同一任务在任意时刻最多被一个执行者持有: claim 是
  "SELECT 候选 + 条件 UPDATE" 的原子组合(单连接串行化)。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid

from jobqueue.models import (
    AuditEvent,
    EventType,
    FailureKind,
    FailureRecord,
    Job,
    JobState,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id                TEXT PRIMARY KEY,
    idempotency_key   TEXT UNIQUE,
    payload           TEXT NOT NULL,
    payload_hash      TEXT NOT NULL,
    state             TEXT NOT NULL,
    attempts          INTEGER NOT NULL,
    max_attempts      INTEGER NOT NULL,
    run_at            REAL NOT NULL,
    lease_token       TEXT,
    lease_expires_at  REAL,
    cancel_requested  INTEGER NOT NULL DEFAULT 0,
    last_failure      TEXT,
    failure_history   TEXT NOT NULL DEFAULT '[]',
    result            TEXT,
    created_at        REAL NOT NULL,
    updated_at        REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_state_runat ON jobs(state, run_at);
CREATE INDEX IF NOT EXISTS idx_jobs_lease_expiry ON jobs(state, lease_expires_at);
CREATE TABLE IF NOT EXISTS job_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL,
    event       TEXT NOT NULL,
    attempt     INTEGER NOT NULL,
    reason      TEXT,
    created_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_job ON job_events(job_id, id);
"""


class SQLiteStore:
    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._lock = threading.RLock()
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # 序列化
    # ------------------------------------------------------------------
    @staticmethod
    def _failure_to_json(rec: FailureRecord | None) -> str | None:
        if rec is None:
            return None
        return json.dumps(
            {"attempt": rec.attempt, "kind": rec.kind.value,
             "reason": rec.reason, "failed_at": rec.failed_at}
        )

    @staticmethod
    def _failure_from_json(raw: str | None) -> FailureRecord | None:
        if raw is None:
            return None
        d = json.loads(raw)
        return FailureRecord(
            attempt=d["attempt"], kind=FailureKind(d["kind"]),
            reason=d["reason"], failed_at=d["failed_at"],
        )

    def _from_row(self, row: sqlite3.Row) -> Job:
        return Job(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            payload=json.loads(row["payload"]),
            state=JobState(row["state"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            run_at=row["run_at"],
            lease_token=row["lease_token"],
            lease_expires_at=row["lease_expires_at"],
            cancel_requested=bool(row["cancel_requested"]),
            last_failure=self._failure_from_json(row["last_failure"]),
            failure_history=[
                self._failure_from_json(item)  # type: ignore[misc]
                for item in json.loads(row["failure_history"])
            ],
            result=json.loads(row["result"]) if row["result"] is not None else None,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    # ------------------------------------------------------------------
    # 生命周期审计 (只追加)
    # ------------------------------------------------------------------
    def _append_event(
        self,
        conn: sqlite3.Connection,
        job_id: str,
        event: EventType,
        attempt: int,
        reason: str | None,
        now: float,
    ) -> None:
        """在调用方事务内追加一条审计记录。表只追加, 无更新/删除路径。"""
        conn.execute(
            """INSERT INTO job_events (job_id, event, attempt, reason, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (job_id, event.value, attempt, reason, now),
        )

    def _event_from_row(self, row: sqlite3.Row) -> AuditEvent:
        return AuditEvent(
            id=int(row["id"]),
            job_id=row["job_id"],
            event=EventType(row["event"]),
            attempt=int(row["attempt"]),
            reason=row["reason"],
            created_at=row["created_at"],
        )

    def list_events(self, job_id: str) -> list[AuditEvent]:
        """单任务审计: 按时间顺序(自增 id)返回。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM job_events WHERE job_id = ? ORDER BY id", (job_id,)
            ).fetchall()
        return [self._event_from_row(r) for r in rows]

    def list_events_since(self, event_id: int = 0, limit: int = 500) -> list[AuditEvent]:
        """全局审计: 返回 id > event_id 的记录, 按时间顺序翻页。"""
        with self._lock:
            rows = self._conn.execute(
                """SELECT * FROM job_events WHERE id > ? ORDER BY id LIMIT ?""",
                (event_id, limit),
            ).fetchall()
        return [self._event_from_row(r) for r in rows]

    def conn_export_events_raw(self, job_id: str) -> list[tuple]:
        """导出审计表原始行(只读), 供核验"只追加、无改写"。"""
        with self._lock:
            rows = self._conn.execute(
                """SELECT id, job_id, event, attempt, reason, created_at
                   FROM job_events WHERE job_id = ? ORDER BY id""",
                (job_id,),
            ).fetchall()
        return [tuple(r) for r in rows]

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------
    def insert_job(self, job: Job, payload_hash: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """INSERT INTO jobs (
                       id, idempotency_key, payload, payload_hash, state,
                       attempts, max_attempts, run_at, lease_token,
                       lease_expires_at, cancel_requested, last_failure,
                       failure_history, result, created_at, updated_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    job.id, job.idempotency_key, json.dumps(job.payload),
                    payload_hash, job.state.value, job.attempts, job.max_attempts,
                    job.run_at, job.lease_token, job.lease_expires_at,
                    int(job.cancel_requested),
                    self._failure_to_json(job.last_failure),
                    json.dumps([self._failure_to_json(r) for r in job.failure_history]),
                    json.dumps(job.result) if job.result is not None else None,
                    job.created_at, job.updated_at,
                ),
            )
            self._append_event(
                self._conn, job.id, EventType.SUBMITTED, 0, None, job.created_at
            )

    def get_job(self, job_id: str) -> Job | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
        return self._from_row(row) if row else None

    def get_job_by_idempotency_key(self, key: str) -> Job | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE idempotency_key = ?", (key,)
            ).fetchone()
        return self._from_row(row) if row else None

    def get_payload_hash(self, job_id: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT payload_hash FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
        return row["payload_hash"] if row else None

    def count_active(self) -> int:
        """积压量 = 排队中 + 执行中。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM jobs WHERE state IN (?, ?)",
                (JobState.PENDING.value, JobState.LEASED.value),
            ).fetchone()
        return int(row["n"])

    # ------------------------------------------------------------------
    # 状态迁移(全部带前置条件)
    # ------------------------------------------------------------------
    def claim_next(self, now: float, lease_seconds: float) -> Job | None:
        """原子领取一个到期任务; 同一任务最多被一个执行者持有。"""
        with self._lock, self._conn:
            row = self._conn.execute(
                """SELECT id FROM jobs
                   WHERE state = ? AND run_at <= ?
                   ORDER BY run_at, created_at LIMIT 1""",
                (JobState.PENDING.value, now),
            ).fetchone()
            if row is None:
                return None
            token = uuid.uuid4().hex
            cur = self._conn.execute(
                """UPDATE jobs SET state = ?, lease_token = ?,
                       lease_expires_at = ?, attempts = attempts + 1,
                       updated_at = ?
                   WHERE id = ? AND state = ?""",
                (JobState.LEASED.value, token, now + lease_seconds, now,
                 row["id"], JobState.PENDING.value),
            )
            if cur.rowcount != 1:
                return None
            claimed = self.get_job(row["id"])
            if claimed is not None:
                self._append_event(
                    self._conn, claimed.id, EventType.CLAIMED,
                    claimed.attempts, None, now,
                )
            return claimed

    def complete(self, job_id: str, lease_token: str, result: object) -> bool:
        """仅当租约仍由该执行者持有且期间未受理取消时置为成功。

        ``cancel_requested = 0`` 前置条件把"取消恰好在完成边界"的竞态
        收口: 取消一旦在执行期间被受理, 成功写入必被拒绝, 由取消路径
        落 CANCELLED。
        """
        now = Job.now()
        with self._lock, self._conn:
            cur = self._conn.execute(
                """UPDATE jobs SET state = ?, result = ?, lease_token = NULL,
                       lease_expires_at = NULL, updated_at = ?
                   WHERE id = ? AND state = ? AND lease_token = ?
                         AND cancel_requested = 0""",
                (JobState.SUCCEEDED.value, json.dumps(result), now,
                 job_id, JobState.LEASED.value, lease_token),
            )
            if cur.rowcount != 1:
                return False
            row = self._conn.execute(
                "SELECT attempts FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            self._append_event(
                self._conn, job_id, EventType.SUCCEEDED,
                int(row["attempts"]) if row else 0, None, now,
            )
            return True

    def fail(self, job: Job) -> bool:
        """失败落库: 重排(PENDING)或死信(DEAD), 追加失败历史。

        仅当租约匹配且 cancel_requested=0 时生效: 取消已受理时失败
        不得把任务重排重试, 由 complete_cancel 收口为 CANCELLED。
        """
        with self._lock, self._conn:
            cur = self._conn.execute(
                """UPDATE jobs SET state = ?, run_at = ?, lease_token = NULL,
                       lease_expires_at = NULL, last_failure = ?,
                       failure_history = ?, updated_at = ?
                   WHERE id = ? AND state = ? AND lease_token = ?
                         AND cancel_requested = 0""",
                (
                    job.state.value, job.run_at,
                    self._failure_to_json(job.last_failure),
                    json.dumps([self._failure_to_json(r) for r in job.failure_history]),
                    job.updated_at, job.id, JobState.LEASED.value, job.lease_token,
                ),
            )
            if cur.rowcount != 1:
                return False
            reason = job.last_failure.reason if job.last_failure else None
            event = (EventType.DEAD_LETTERED if job.state == JobState.DEAD
                     else EventType.RETRY_SCHEDULED)
            self._append_event(
                self._conn, job.id, event, job.attempts, reason, job.updated_at
            )
            return True

    def complete_cancel(self, job_id: str, lease_token: str) -> bool:
        """已领取任务被取消后, 由执行者落终态。

        前置 cancel_requested = 1: 只在取消确已受理时生效。
        """
        now = Job.now()
        with self._lock, self._conn:
            cur = self._conn.execute(
                """UPDATE jobs SET state = ?, lease_token = NULL,
                       lease_expires_at = NULL, updated_at = ?
                   WHERE id = ? AND state = ? AND lease_token = ?
                         AND cancel_requested = 1""",
                (JobState.CANCELLED.value, now, job_id,
                 JobState.LEASED.value, lease_token),
            )
            if cur.rowcount != 1:
                return False
            row = self._conn.execute(
                "SELECT attempts FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            self._append_event(
                self._conn, job_id, EventType.CANCELLED,
                int(row["attempts"]) if row else 0,
                "cancelled while executing", now,
            )
            return True

    def cancel(self, job_id: str) -> tuple[Job | None, EventType | None]:
        """取消入口, 幂等。返回 (任务, 本次受理产生的事件)。

        - PENDING  -> CANCELLED (立即生效)
        - LEASED   -> 仅置 cancel_requested 标记, 由执行者中断后落终态
        - 其他状态 -> 不变, 重复取消不报错; 终态任务事件为 None
        """
        now = Job.now()
        event: EventType | None = None
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT state, attempts FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if row is None:
                return None, None
            state = JobState(row["state"])
            if state == JobState.PENDING:
                cur = self._conn.execute(
                    """UPDATE jobs SET state = ?, run_at = 0, updated_at = ?
                       WHERE id = ? AND state = ?""",
                    (JobState.CANCELLED.value, now, job_id, JobState.PENDING.value),
                )
                if cur.rowcount == 1:
                    self._append_event(
                        self._conn, job_id, EventType.CANCELLED,
                        int(row["attempts"]), "cancelled while queued", now,
                    )
                    event = EventType.CANCELLED
            elif state == JobState.LEASED:
                # 仅在首次受理时置标记并写审计; 并发重复取消不重复记录
                cur = self._conn.execute(
                    """UPDATE jobs SET cancel_requested = 1, updated_at = ?
                       WHERE id = ? AND state = ? AND cancel_requested = 0""",
                    (now, job_id, JobState.LEASED.value),
                )
                if cur.rowcount == 1:
                    self._append_event(
                        self._conn, job_id, EventType.CANCEL_REQUESTED,
                        int(row["attempts"]), "cancel accepted while executing", now,
                    )
                    event = EventType.CANCEL_REQUESTED
            return self.get_job(job_id), event

    def replay(self, job_id: str) -> Job | None:
        """死信人工重放: 重置尝试计数, 保留全部失败历史与最后失败原因。"""
        now = Job.now()
        with self._lock, self._conn:
            cur = self._conn.execute(
                """UPDATE jobs SET state = ?, attempts = 0, run_at = ?,
                       lease_token = NULL, lease_expires_at = NULL,
                       cancel_requested = 0, updated_at = ?
                   WHERE id = ? AND state = ?""",
                (JobState.PENDING.value, now, now, job_id, JobState.DEAD.value),
            )
            if cur.rowcount != 1:
                return None
            self._append_event(
                self._conn, job_id, EventType.REPLAYED, 0,
                "manual replay from dead letter", now,
            )
            return self.get_job(job_id)

    def reclaim_leased_jobs(
        self, now: float, *, only_expired: bool, reason: str
    ) -> list[tuple[Job, EventType, str | None]]:
        """自愈收回执行中任务, 单连接事务内逐条条件迁移。

        - only_expired=False (启动恢复): 所有上次进程残留的 LEASED 任务
          立即收回, 默认配置下重启即恢复可执行, 不依赖等待租约过期。
        - only_expired=True  (运行期失联扫描): 只收回租约已过期的任务。
        - 已受理取消的任务直接收口 CANCELLED, 不再被重新领取执行;
          其余回到 PENDING (run_at=now), attempts 保持不变, 下次领取
          才计为新的一次尝试, 因此收回本身不会造成尝试次数混乱。
        - 旧执行者晚到的写入仍受 state + lease_token 前置条件拒绝。
        """
        results: list[tuple[Job, EventType, str | None]] = []
        with self._lock, self._conn:
            if only_expired:
                rows = self._conn.execute(
                    """SELECT id FROM jobs
                       WHERE state = ? AND lease_expires_at IS NOT NULL
                             AND lease_expires_at <= ?""",
                    (JobState.LEASED.value, now),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT id FROM jobs WHERE state = ?",
                    (JobState.LEASED.value,),
                ).fetchall()
            for row in rows:
                detail = self._conn.execute(
                    "SELECT attempts, cancel_requested FROM jobs WHERE id = ?",
                    (row["id"],),
                ).fetchone()
                if detail is None:
                    continue
                attempts, cancel_requested = (
                    int(detail["attempts"]), bool(detail["cancel_requested"])
                )
                if cancel_requested:
                    cur = self._conn.execute(
                        """UPDATE jobs SET state = ?, lease_token = NULL,
                               lease_expires_at = NULL, updated_at = ?
                           WHERE id = ? AND state = ?""",
                        (JobState.CANCELLED.value, now,
                         row["id"], JobState.LEASED.value),
                    )
                    event = EventType.CANCELLED
                    why: str | None = f"{reason}; cancel already requested"
                else:
                    cur = self._conn.execute(
                        """UPDATE jobs SET state = ?, run_at = ?,
                               lease_token = NULL, lease_expires_at = NULL,
                               updated_at = ?
                           WHERE id = ? AND state = ?""",
                        (JobState.PENDING.value, now, now,
                         row["id"], JobState.LEASED.value),
                    )
                    event = EventType.RECLAIMED
                    why = reason
                if cur.rowcount == 1:
                    self._append_event(
                        self._conn, row["id"], event, attempts, why, now
                    )
                    job = self.get_job(row["id"])
                    if job is not None:
                        results.append((job, event, why))
            return results

    def list_dead(self) -> list[Job]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE state = ? ORDER BY updated_at",
                (JobState.DEAD.value,),
            ).fetchall()
        return [self._from_row(r) for r in rows]
