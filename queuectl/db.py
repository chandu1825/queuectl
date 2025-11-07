# queuectl/db.py
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
import os
from typing import Optional, Dict, Any, List

DB_PATH = os.environ.get("QUEUECTL_DB", os.path.join(os.getcwd(), "jobs.db"))

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def init_db():
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 3,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        next_run_at TEXT,
        last_error TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    """)
    conn.commit()
    conn.close()

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def set_config(key: str, value: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO config(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value;", (key, value))

def get_config(key: str, default: Optional[str]=None) -> Optional[str]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT value FROM config WHERE key=?;", (key,))
        row = cur.fetchone()
        return row["value"] if row else default

def add_job(job: Dict[str, Any]) -> None:
    with get_conn() as conn:
        cur = conn.cursor()
        now = now_iso()
        cur.execute("SELECT 1 FROM jobs WHERE id=?;", (job["id"],))
        if cur.fetchone():
            raise ValueError(f"Job with id {job['id']} already exists")
        cur.execute("""
            INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, next_run_at, last_error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            job["id"],
            job["command"],
            job.get("state", "pending"),
            job.get("attempts", 0),
            job.get("max_retries", 3),
            job.get("created_at", now),
            job.get("updated_at", now),
            job.get("next_run_at"),
            job.get("last_error")
        ))
        conn.commit()

def list_jobs(state: Optional[str]=None) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cur = conn.cursor()
        if state:
            cur.execute("SELECT * FROM jobs WHERE state=? ORDER BY created_at ASC;", (state,))
        else:
            cur.execute("SELECT * FROM jobs ORDER BY created_at ASC;")
        rows = cur.fetchall()
        return [dict(r) for r in rows]

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id=?;", (job_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def update_job_state(job_id: str, *, state: str, attempts: Optional[int]=None, next_run_at: Optional[str]=None, last_error: Optional[str]=None):
    with get_conn() as conn:
        cur = conn.cursor()
        fields = ["state = ?", "updated_at = ?"]
        params = [state, now_iso()]
        if attempts is not None:
            fields.append("attempts = ?")
            params.append(attempts)
        if next_run_at is not None:
            fields.append("next_run_at = ?")
            params.append(next_run_at)
        if last_error is not None:
            fields.append("last_error = ?")
            params.append(last_error)
        params.append(job_id)
        sql = f"UPDATE jobs SET {', '.join(fields)} WHERE id = ?;"
        cur.execute(sql, tuple(params))
        conn.commit()

def move_to_dead(job_id: str, last_error: Optional[str]=None):
    update_job_state(job_id, state="dead", last_error=last_error)

def claim_job(worker_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE;")
        cur.execute("""
            SELECT id FROM jobs
            WHERE state = 'pending'
              AND (next_run_at IS NULL OR next_run_at <= ?)
            ORDER BY created_at ASC
            LIMIT 1;
        """, (now_iso(),))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None
        job_id = row["id"]
        cur.execute("UPDATE jobs SET state = 'processing', updated_at = ? WHERE id = ?;", (now_iso(), job_id))
        conn.commit()
        cur.execute("SELECT * FROM jobs WHERE id = ?;", (job_id,))
        job = cur.fetchone()
        return dict(job) if job else None
