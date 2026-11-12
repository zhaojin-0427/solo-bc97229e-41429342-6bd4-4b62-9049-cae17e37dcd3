"""SQLite persistence layer.

Concurrency model
-----------------
* WAL journal + a 30 s busy timeout allow many concurrent readers alongside a
  single writer. Each HTTP request opens its own connection (sqlite3 objects
  must not be shared across threads) and every batch is committed in one
  ``BEGIN IMMEDIATE`` transaction, so two concurrent batches serialize cleanly
  and either the whole batch lands or nothing does.
* All identity/mapping rows carry a UNIQUE constraint on
  ``(center_id, original_uid)`` and ``(center_id, original_sop_uid, sha256)``;
  a conflicting insert is detected and turned into a batch-level rejection.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

from .config import BUSY_TIMEOUT_MS, DB_PATH, ensure_dirs

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS centers (
    center_id   TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- One row per (center, original UID) ever observed. The mapping value itself
-- is derivable with app.uid.remap_uid; the table exists so reference-first
-- uploads can reserve rows before the referenced image arrives.
CREATE TABLE IF NOT EXISTS uid_mappings (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    center_id         TEXT NOT NULL,
    original_uid      TEXT NOT NULL,
    new_uid           TEXT NOT NULL,
    uid_kind          TEXT NOT NULL,                 -- study | series | sop | other_ui
    present           INTEGER NOT NULL DEFAULT 0,    -- 1 once an object carrying this SOP exists
    first_seen_batch  TEXT,
    created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (center_id, original_uid)
);
CREATE INDEX IF NOT EXISTS idx_uid_mappings_new
    ON uid_mappings (center_id, new_uid);

-- Ingested DICOM objects (one physical rewritten file per row).
CREATE TABLE IF NOT EXISTS instances (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    center_id              TEXT NOT NULL,
    batch_id               TEXT NOT NULL,
    original_sop_uid       TEXT NOT NULL,
    new_sop_uid            TEXT NOT NULL,
    original_study_uid     TEXT NOT NULL,
    new_study_uid          TEXT NOT NULL,
    original_series_uid    TEXT NOT NULL,
    new_series_uid         TEXT NOT NULL,
    modality               TEXT,
    transfer_syntax_uid    TEXT NOT NULL,
    sha256                 TEXT NOT NULL,
    byte_size              INTEGER NOT NULL,
    stored_path            TEXT NOT NULL,
    created_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (center_id, original_sop_uid, sha256),
    UNIQUE (new_sop_uid),
    FOREIGN KEY (center_id) REFERENCES centers (center_id)
);
CREATE INDEX IF NOT EXISTS idx_instances_batch ON instances (batch_id);
CREATE INDEX IF NOT EXISTS idx_instances_lookup
    ON instances (center_id, original_sop_uid);

-- Every reference encountered while rewriting, including not-yet-resolved ones.
CREATE TABLE IF NOT EXISTS instance_references (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    center_id                TEXT NOT NULL,
    source_instance_id       INTEGER NOT NULL,
    original_referenced_uid  TEXT NOT NULL,
    new_referenced_uid       TEXT NOT NULL,
    resolved                 INTEGER NOT NULL DEFAULT 0,
    created_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (source_instance_id, original_referenced_uid),
    FOREIGN KEY (source_instance_id) REFERENCES instances (id)
);
CREATE INDEX IF NOT EXISTS idx_references_pending
    ON instance_references (center_id, resolved);

CREATE TABLE IF NOT EXISTS batches (
    batch_id     TEXT PRIMARY KEY,
    center_id    TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('committed', 'rejected')),
    file_count   INTEGER NOT NULL,
    reason       TEXT,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    FOREIGN KEY (center_id) REFERENCES centers (center_id)
);
CREATE INDEX IF NOT EXISTS idx_batches_center ON batches (center_id);

-- Background backfill jobs. The cursor plus the per-page batch rows are the
-- whole durable state: on restart the worker resumes at ``cursor`` and every
-- already-committed page is visible as a normal batch.
CREATE TABLE IF NOT EXISTS backfill_jobs (
    job_id               TEXT PRIMARY KEY,
    center_id            TEXT NOT NULL,
    original_study_uid   TEXT NOT NULL,
    source_base_url      TEXT NOT NULL,
    page_size            INTEGER NOT NULL DEFAULT 5,
    status               TEXT NOT NULL CHECK (
        status IN ('queued', 'running', 'paused', 'completed', 'failed')
    ),
    cursor               TEXT NOT NULL DEFAULT '',
    pages_committed      INTEGER NOT NULL DEFAULT 0,
    instances_committed  INTEGER NOT NULL DEFAULT 0,
    instances_reused     INTEGER NOT NULL DEFAULT 0,
    last_batch_id        TEXT,
    last_error_code      TEXT,
    last_error_message   TEXT,
    last_error_at        TEXT,
    runner_token         TEXT,
    runner_started_at    TEXT,
    created_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (center_id, original_study_uid),
    FOREIGN KEY (center_id) REFERENCES centers (center_id)
);
CREATE INDEX IF NOT EXISTS idx_backfill_status ON backfill_jobs (status);

-- One row per page attempt, including failed ones, so operators can inspect
-- the error history without polluting the jobs table.
CREATE TABLE IF NOT EXISTS backfill_job_attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          TEXT NOT NULL,
    page_no         INTEGER NOT NULL,
    cursor          TEXT NOT NULL,
    outcome         TEXT NOT NULL CHECK (
        outcome IN ('committed', 'paused', 'failed')
    ),
    error_code      TEXT,
    error_message   TEXT,
    batch_id        TEXT,
    item_count      INTEGER,
    reused_count    INTEGER,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    FOREIGN KEY (job_id) REFERENCES backfill_jobs (job_id)
);
CREATE INDEX IF NOT EXISTS idx_backfill_attempts_job
    ON backfill_job_attempts (job_id, id);
"""

# Columns added after the first release; applied idempotently so databases
# created by an older image keep working after an upgrade.
_ADDED_COLUMNS: dict[str, list[tuple[str, str]]] = {
    "batches": [
        ("origin_kind", "TEXT NOT NULL DEFAULT 'upload'"),
        ("origin_job_id", "TEXT"),
        ("page_no", "INTEGER"),
        ("cursor", "TEXT"),
    ],
    "backfill_jobs": [
        ("page_size", "INTEGER NOT NULL DEFAULT 5"),
    ],
}


def _apply_column_migrations(conn: sqlite3.Connection) -> None:
    for table, columns in _ADDED_COLUMNS.items():
        existing = {
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        for name, declaration in columns:
            if name not in existing:
                conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN {name} {declaration}"
                )


def get_connection() -> sqlite3.Connection:
    """Open a SQLite connection with safe pragmas and row access."""
    ensure_dirs()
    conn = sqlite3.connect(
        DB_PATH,
        timeout=BUSY_TIMEOUT_MS / 1000.0,
        isolation_level=None,  # transactions managed explicitly below
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def init_db() -> None:
    conn = get_connection()
    try:
        conn.executescript(SCHEMA)
        _apply_column_migrations(conn)
    finally:
        conn.close()


@contextmanager
def immediate_transaction() -> Iterator[sqlite3.Connection]:
    """Acquire a writer lock up front; commit on success, roll back on error."""
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
