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

from jobqueue.models import EventType, FailureKind, FailureRecord, Job, JobEvent, JobState

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

-- 生命周期审计: append-only, 只存变化类型/尝试次数/归一化原因,
-- 不存载荷与内部异常堆栈; 触发器禁止更新与删除, 历史不可改写。
CREATE TABLE IF NOT EXISTS job_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL,
    event       TEXT NOT NULL,
    attempt     INTEGER NOT NULL,
    reason      TEXT,
    created_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_job_events_job ON job_events(job_id, id);
CREATE TRIGGER IF NOT EXISTS job_events_no_update
BEFORE UPDATE ON job_events
BEGIN
    SELECT RAISE(ABORT, 'job_events is append-only');
END;
CREATE TRIGGER IF NOT EXISTS job_events_no_delete
BEFORE DELETE ON job_events
BEGIN
    SELECT RAISE(ABORT, 'job_events is append-only');
END;
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
    # 生命周期审计 (append-only)
    # ------------------------------------------------------------------
    def _append_event(
        self,
        conn: sqlite3.Connection,
        job_id: str,
        event: EventType,
        attempt: int,
        reason: str | None = None,
        now: float | None = None,
    ) -> None:
        conn.execute(
            """INSERT INTO job_events (job_id, event, attempt, reason, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (job_id, event.value, attempt, reason, now if now is not None else Job.now()),
        )

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> JobEvent:
        return JobEvent(
            id=row["id"], job_id=row["job_id"], event=EventType(row["event"]),
            attempt=row["attempt"], reason=row["reason"], created_at=row["created_at"],
        )

    def list_events(self, job_id: str) -> list[JobEvent]:
        """按时间顺序返回单个任务的审计记录; 不存在的任务返回空列表。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM job_events WHERE job_id = ? ORDER BY id", (job_id,)
            ).fetchall()
        return [self._event_from_row(r) for r in rows]

    def list_events_since(self, event_id: int, limit: int = 200) -> list[JobEvent]:
        """全局增量查询: 返回 id > event_id 的审计记录(按时间顺序), 用于排障。"""
        with self._lock:
            rows = self._conn.execute(
                """SELECT * FROM job_events WHERE id > ? ORDER BY id LIMIT ?""",
                (event_id, limit),
            ).fetchall()
        return [self._event_from_row(r) for r in rows]

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
            self._append_event(
                self._conn, row["id"], EventType.CLAIMED, claimed.attempts, None, now
            )
            return claimed

    def complete(self, job_id: str, lease_token: str, result: object) -> bool:
        """仅当租约仍由该执行者持有时置为成功。"""
        now = Job.now()
        with self._lock, self._conn:
            cur = self._conn.execute(
                """UPDATE jobs SET state = ?, result = ?, lease_token = NULL,
                       lease_expires_at = NULL, updated_at = ?
                   WHERE id = ? AND state = ? AND lease_token = ?""",
                (JobState.SUCCEEDED.value, json.dumps(result), now,
                 job_id, JobState.LEASED.value, lease_token),
            )
            if cur.rowcount == 1:
                self._append_event(
                    self._conn, job_id, EventType.SUCCEEDED,
                    self._attempts_of(self._conn, job_id), None, now,
                )
            return cur.rowcount == 1

    @staticmethod
    def _attempts_of(conn: sqlite3.Connection, job_id: str) -> int:
        row = conn.execute(
            "SELECT attempts FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        return int(row["attempts"]) if row else 0

    def fail(self, job: Job) -> bool:
        """失败落库: 重排(PENDING)或死信(DEAD), 追加失败历史。

        仅当租约匹配且取消标记未置位时生效: 若取消恰好在失败落库前到达,
        返回 False, 由调用方改走取消收口, 防止结果被改回重试/死信。
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
            if cur.rowcount == 1 and job.last_failure is not None:
                event = (
                    EventType.DEAD_LETTERED
                    if job.state == JobState.DEAD
                    else EventType.RETRY_SCHEDULED
                )
                reason = f"{job.last_failure.kind.value}: {job.last_failure.reason}"
                self._append_event(
                    self._conn, job.id, event, job.attempts, reason, job.updated_at
                )
            return cur.rowcount == 1

    def complete_cancel(
        self, job_id: str, lease_token: str, reason: str = "cancel accepted during execution"
    ) -> bool:
        """已领取任务被取消后, 由执行者或自愈流程落终态。"""
        now = Job.now()
        with self._lock, self._conn:
            cur = self._conn.execute(
                """UPDATE jobs SET state = ?, lease_token = NULL,
                       lease_expires_at = NULL, updated_at = ?
                   WHERE id = ? AND state = ? AND lease_token = ?""",
                (JobState.CANCELLED.value, now, job_id,
                 JobState.LEASED.value, lease_token),
            )
            if cur.rowcount == 1:
                self._append_event(
                    self._conn, job_id, EventType.CANCELLED,
                    self._attempts_of(self._conn, job_id), reason, now,
                )
            return cur.rowcount == 1

    def cancel(self, job_id: str) -> Job | None:
        """取消入口, 幂等。

        - PENDING  -> CANCELLED (立即生效)
        - LEASED   -> 仅置 cancel_requested 标记, 由执行者中断后落终态;
                      标记已是 1 时重复受理不再追加审计记录
        - 终态/DEAD-> 不变, 重复取消不报错、不追加记录
        """
        now = Job.now()
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT state, attempts, cancel_requested FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                return None
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
                        row["attempts"], "cancelled while queued", now,
                    )
            elif state == JobState.LEASED and not row["cancel_requested"]:
                cur = self._conn.execute(
                    """UPDATE jobs SET cancel_requested = 1, updated_at = ?
                       WHERE id = ? AND state = ? AND cancel_requested = 0""",
                    (now, job_id, JobState.LEASED.value),
                )
                if cur.rowcount == 1:
                    self._append_event(
                        self._conn, job_id, EventType.CANCEL_REQUESTED,
                        row["attempts"], "cancel accepted while leased", now,
                    )
            return self.get_job(job_id)

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
            replayed = self.get_job(job_id)
            self._append_event(
                self._conn, job_id, EventType.REPLAYED, replayed.attempts,
                "manual replay from dead letter", now,
            )
            return replayed

    def reclaim_leases(self, now: float, *, only_expired: bool, reason: str) -> list[Job]:
        """自愈收回执行中任务。

        only_expired=True  (运行期失联扫描): 仅收回 lease_expires_at <= now
                           的租约, 且不再重置 run_at(保留原排期, 立刻可领)。
        only_expired=False (启动恢复, 默认配置): 回收全部 LEASED 租约——
                           旧进程已经消失, 不必等租约到期, 任务不会长期
                           停在执行中。

        已置 cancel_requested 的任务直接收口为 CANCELLED(不再执行);
        其余回到 PENDING。迁移带 state=LEASED 前置条件, 重复扫描或旧执行
        者晚到都不会造成二次执行; attempts 不回退也不增加。
        """
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
            reclaimed: list[Job] = []
            for row in rows:
                meta = self._conn.execute(
                    """SELECT attempts, cancel_requested, lease_token FROM jobs
                       WHERE id = ? AND state = ?""",
                    (row["id"], JobState.LEASED.value),
                ).fetchone()
                if meta is None:
                    continue
                if meta["cancel_requested"]:
                    cur = self._conn.execute(
                        """UPDATE jobs SET state = ?, lease_token = NULL,
                               lease_expires_at = NULL, updated_at = ?
                           WHERE id = ? AND state = ?""",
                        (JobState.CANCELLED.value, now, row["id"],
                         JobState.LEASED.value),
                    )
                    if cur.rowcount == 1:
                        self._append_event(
                            self._conn, row["id"], EventType.CANCELLED,
                            meta["attempts"],
                            f"{reason}; cancel requested, settled without re-run", now,
                        )
                else:
                    cur = self._conn.execute(
                        """UPDATE jobs SET state = ?, lease_token = NULL,
                               lease_expires_at = NULL, updated_at = ?
                           WHERE id = ? AND state = ?""",
                        (JobState.PENDING.value, now, row["id"],
                         JobState.LEASED.value),
                    )
                    if cur.rowcount == 1:
                        self._append_event(
                            self._conn, row["id"], EventType.LEASE_RECLAIMED,
                            meta["attempts"], reason, now,
                        )
                job = self.get_job(row["id"])
                if job is not None and job.state in (
                    JobState.PENDING, JobState.CANCELLED
                ):
                    reclaimed.append(job)
            return reclaimed

    def list_dead(self) -> list[Job]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE state = ? ORDER BY updated_at",
                (JobState.DEAD.value,),
            ).fetchall()
        return [self._from_row(r) for r in rows]
