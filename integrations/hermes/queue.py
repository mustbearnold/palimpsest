"""Durable write-behind queue for Hermes turn episodes.

``sync_turn`` must never block the agent loop, and a committed turn must
survive a Hermes crash. Every turn is written to a SQLite journal in
``$HERMES_HOME/palimpsest/`` and flushed to Palimpsest by a daemon thread.
Idempotency keys are derived from a per-session turn counter that also lives
in SQLite, so a crash between commit and flush can never duplicate an
episode: replaying a row sends the same ``Idempotency-Key`` and the server
deduplicates.

Stdlib only (``sqlite3``, ``threading``, ``queue``).
"""

from __future__ import annotations

import json
import logging
import queue
import sqlite3
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_ASYNC_SHUTDOWN = object()


class PalimpsestWriteQueue:
    """Crash-safe FIFO of pending episode writes flushed by one daemon thread."""

    def __init__(
        self,
        db_path: Path,
        flush: Callable[[dict], None],
        *,
        retry_delay_seconds: float = 2.0,
    ) -> None:
        self._db_path = db_path
        self._flush = flush
        self._retry_delay_seconds = retry_delay_seconds
        self._q: queue.Queue[Any] = queue.Queue()
        self._local = threading.local()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._thread = threading.Thread(
            target=self._loop, name="palimpsest-writer", daemon=True
        )
        self._thread.start()
        # Replay rows left by a previous crash before new writes.
        for row_id, payload in self._pending_rows():
            self._q.put((row_id, payload))

    # -- sqlite helpers -------------------------------------------------------

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(str(self._db_path), timeout=30)
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return conn

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS pending (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    last_error TEXT
                )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS counters (
                    session_id TEXT PRIMARY KEY,
                    turn INTEGER NOT NULL
                )"""
            )

    def _pending_rows(self) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id, payload FROM pending ORDER BY id ASC LIMIT 500"
            ).fetchall()
        return [(int(row["id"]), json.loads(row["payload"])) for row in rows]

    # -- public API ------------------------------------------------------------

    def enqueue_episode(self, payload: dict) -> None:
        """Persist an episode write, then hand it to the flush thread."""
        now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO pending (payload, created_at) VALUES (?, ?)",
                (json.dumps(payload, ensure_ascii=False), now),
            )
            lastrowid = cur.lastrowid
            if lastrowid is None:
                raise RuntimeError("palimpsest queue: INSERT returned no row id")
            row_id = int(lastrowid)
        self._q.put((row_id, payload))

    def next_turn_number(self, session_id: str) -> int:
        """Atomically allocate the next turn counter for a session."""
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO counters (session_id, turn) VALUES (?, 1) "
                "ON CONFLICT(session_id) DO UPDATE SET turn = counters.turn + 1 "
                "RETURNING turn",
                (session_id,),
            )
            row = cur.fetchone()
        return int(row["turn"]) if row is not None else 1

    def pending_count(self) -> int:
        with self._conn() as conn:
            row = conn.execute("SELECT COUNT(*) AS n FROM pending").fetchone()
        return int(row["n"]) if row is not None else 0

    def shutdown(self, timeout: float = 10.0) -> None:
        """Stop the flush thread and drain remaining rows best-effort."""
        self._q.put(_ASYNC_SHUTDOWN)
        self._thread.join(timeout=timeout)

    # -- flush loop ------------------------------------------------------------

    def _flush_row(self, row_id: int, payload: dict) -> None:
        try:
            self._flush(payload)
        except Exception as exc:  # noqa: BLE001 - writer thread must never die
            logger.warning("Palimpsest flush failed (will retry): %s", exc)
            with self._conn() as conn:
                conn.execute(
                    "UPDATE pending SET last_error = ? WHERE id = ?",
                    (str(exc)[:500], row_id),
                )
            time.sleep(self._retry_delay_seconds)
            self._q.put((row_id, payload))  # re-queue for the next loop iteration
            return
        with self._conn() as conn:
            conn.execute("DELETE FROM pending WHERE id = ?", (row_id,))

    def _loop(self) -> None:
        while True:
            try:
                item = self._q.get(timeout=5)
            except queue.Empty:
                continue
            if item is _ASYNC_SHUTDOWN:
                break
            try:
                self._flush_row(*item)
            except Exception:
                logger.exception("Palimpsest writer error")
