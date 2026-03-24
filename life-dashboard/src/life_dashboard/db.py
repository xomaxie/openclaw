"""SQLite database layer for Life Dashboard."""

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

import structlog

log = structlog.get_logger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS reports (
    id           TEXT PRIMARY KEY,
    type         TEXT NOT NULL,          -- 'meteorology' | 'daily' | 'site'
    title        TEXT NOT NULL,
    summary      TEXT,
    content      TEXT NOT NULL,           -- JSON
    generated_at TIMESTAMPTZ NOT NULL,
    source_tag   TEXT,
    raw_inputs   TEXT,                    -- JSON array of source refs
    created_at   TIMESTAMPTZ DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

CREATE TABLE IF NOT EXISTS dashboard_state (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,             -- JSON
    updated_at TIMESTAMPTZ DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    id          TEXT PRIMARY KEY,
    pipeline    TEXT NOT NULL,   -- 'meteorology' | 'daily' | 'site'
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status      TEXT NOT NULL,  -- 'running' | 'success' | 'error'
    error_msg   TEXT,
    report_id   TEXT REFERENCES reports(id)
);

CREATE INDEX IF NOT EXISTS idx_reports_type         ON reports(type);
CREATE INDEX IF NOT EXISTS idx_reports_generated_at  ON reports(generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_pipeline         ON ingestion_runs(pipeline);
CREATE INDEX IF NOT EXISTS idx_runs_started_at       ON ingestion_runs(started_at DESC);
"""


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA foreign_keys = ON")
        return self._conn

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Cursor, None, None]:
        conn = self.connect()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def init_schema(self) -> None:
        with self.transaction() as cur:
            cur.executescript(SCHEMA)
        log.info("db.schema_init", path=str(self.path))

    # ── Reports ────────────────────────────────────────────────────────────────

    def insert_report(
        self,
        rtype: str,
        title: str,
        content: dict[str, Any],
        generated_at: datetime,
        summary: str | None = None,
        source_tag: str | None = None,
        raw_inputs: list[str] | None = None,
    ) -> str:
        report_id = str(uuid.uuid4())
        with self.transaction() as cur:
            cur.execute(
                """INSERT INTO reports
                   (id, type, title, summary, content, generated_at, source_tag, raw_inputs)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    report_id,
                    rtype,
                    title,
                    summary,
                    json.dumps(content, default=str),
                    generated_at.isoformat(),
                    source_tag,
                    json.dumps(raw_inputs, default=str) if raw_inputs else None,
                ),
            )
        log.info("db.report_inserted", report_id=report_id, type=rtype)
        return report_id

    def get_reports(
        self,
        rtype: str | None = None,
        limit: int = 10,
        since: datetime | None = None,
    ) -> list[dict[str, Any]]:
        conn = self.connect()
        query = "SELECT * FROM reports WHERE 1=1"
        params: list[Any] = []
        if rtype:
            query += " AND type = ?"
            params.append(rtype)
        if since:
            query += " AND generated_at >= ?"
            params.append(since.isoformat())
        query += " ORDER BY generated_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        return [self._row_to_report(dict(r)) for r in rows]

    def get_report(self, report_id: str) -> dict[str, Any] | None:
        conn = self.connect()
        row = conn.execute(
            "SELECT * FROM reports WHERE id = ?", (report_id,)
        ).fetchone()
        return self._row_to_report(dict(row)) if row else None

    def get_latest_report(self, rtype: str) -> dict[str, Any] | None:
        conn = self.connect()
        row = conn.execute(
            "SELECT * FROM reports WHERE type = ? ORDER BY generated_at DESC LIMIT 1",
            (rtype,),
        ).fetchone()
        return self._row_to_report(dict(row)) if row else None

    @staticmethod
    def _row_to_report(row: dict[str, Any]) -> dict[str, Any]:
        row["content"] = json.loads(row["content"])
        if row.get("raw_inputs"):
            row["raw_inputs"] = json.loads(row["raw_inputs"])
        return row

    # ── Dashboard State ────────────────────────────────────────────────────────

    def set_state(self, key: str, value: Any) -> None:
        with self.transaction() as cur:
            cur.execute(
                """INSERT INTO dashboard_state (key, value, updated_at)
                   VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at""",
                (key, json.dumps(value, default=str)),
            )
        log.info("db.state_updated", key=key)

    def get_state(self, key: str) -> Any | None:
        conn = self.connect()
        row = conn.execute(
            "SELECT value FROM dashboard_state WHERE key = ?", (key,)
        ).fetchone()
        return json.loads(row["value"]) if row else None

    def get_all_state(self) -> dict[str, Any]:
        conn = self.connect()
        rows = conn.execute("SELECT key, value FROM dashboard_state").fetchall()
        return {r["key"]: json.loads(r["value"]) for r in rows}

    # ── Ingestion Runs ─────────────────────────────────────────────────────────

    def start_run(self, pipeline: str) -> str:
        run_id = str(uuid.uuid4())
        with self.transaction() as cur:
            cur.execute(
                """INSERT INTO ingestion_runs (id, pipeline, started_at, status)
                   VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%SZ','now'), 'running')""",
                (run_id, pipeline),
            )
        log.info("db.run_started", run_id=run_id, pipeline=pipeline)
        return run_id

    def finish_run(self, run_id: str, status: str, report_id: str | None = None, error: str | None = None) -> None:
        with self.transaction() as cur:
            cur.execute(
                """UPDATE ingestion_runs
                   SET finished_at = strftime('%Y-%m-%dT%H:%M:%SZ','now'),
                       status = ?, report_id = ?, error_msg = ?
                   WHERE id = ?""",
                (status, report_id, error, run_id),
            )
        log.info("db.run_finished", run_id=run_id, status=status)

    def get_recent_runs(self, pipeline: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
        conn = self.connect()
        query = "SELECT * FROM ingestion_runs"
        params: list[Any] = []
        if pipeline:
            query += " WHERE pipeline = ?"
            params.append(pipeline)
        query += " ORDER BY started_at DESC LIMIT ?"
        params.append(limit)
        return [dict(r) for r in conn.execute(query, params).fetchall()]
