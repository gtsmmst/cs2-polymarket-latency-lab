"""Shared SQLite "events bus" used by every watcher, the dashboards and the analysis scripts.

All feeds write into a single ``events`` table. The one thing that matters for latency
measurement is ``received_ts_ms``: it is stamped from the *local* clock at the moment a
message reaches this machine, for every source alike, so deltas between sources can be
computed without any clock synchronisation against the upstream servers.

The writer is thread-safe: ``gotv_ingest`` serves HTTP POSTs from several threads.
"""
import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent

# Override with the LATENCY_LAB_DB environment variable (absolute or relative path).
DB_PATH = Path(os.environ.get("LATENCY_LAB_DB") or ROOT / "data" / "events.sqlite")

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    received_ts_ms  INTEGER NOT NULL,    -- local wall clock, shared by all sources
    source_ts_ms    INTEGER,             -- upstream timestamp, if the feed provides one
    source          TEXT NOT NULL,       -- 'gotv' | 'poly' | 'panda' | 'bo3' | 'manual'
    kind            TEXT NOT NULL,       -- 'kill' | 'round_end' | 'price_change' | 'best_bid_ask' | ...
    match_id        TEXT,                -- match identifier (gotv/panda/bo3)
    asset_id        TEXT,                -- Polymarket asset id (poly)
    market_id       TEXT,                -- Polymarket condition id (poly)
    round_num       INTEGER,             -- when applicable
    payload         TEXT NOT NULL        -- raw JSON
);

CREATE INDEX IF NOT EXISTS ix_events_ts    ON events(received_ts_ms);
CREATE INDEX IF NOT EXISTS ix_events_src   ON events(source, kind);
CREATE INDEX IF NOT EXISTS ix_events_asset ON events(asset_id);
"""

# Polymarket event kinds that count as "the market moved".
POLY_PRICE_KINDS = ("price_change", "last_trade_price", "best_bid_ask")


def init_schema(con: sqlite3.Connection) -> None:
    """Create the events table and indexes if they do not exist yet (idempotent)."""
    con.executescript(SCHEMA)
    con.commit()


def connect(db_path: Path = DB_PATH, *, row_factory: bool = True) -> sqlite3.Connection:
    """Open a read/write connection. Creates the parent directory and schema on first use."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    if row_factory:
        con.row_factory = sqlite3.Row
    init_schema(con)
    return con


class EventLog:
    """Append-only writer for the events bus."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False lets several threads share the connection;
        # the lock serialises INSERTs explicitly (SQLite would serialise anyway,
        # the lock just makes the guarantee visible).
        self.con = sqlite3.connect(str(self.db_path), isolation_level=None, check_same_thread=False)
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.execute("PRAGMA synchronous=NORMAL")
        init_schema(self.con)
        self._lock = threading.Lock()

    def write(
        self,
        source: str,
        kind: str,
        payload: dict,
        *,
        source_ts_ms: Optional[int] = None,
        match_id: Optional[str] = None,
        asset_id: Optional[str] = None,
        market_id: Optional[str] = None,
        round_num: Optional[int] = None,
        received_ts_ms: Optional[int] = None,
    ) -> int:
        """Insert one event. Returns the ``received_ts_ms`` that was stored.

        ``received_ts_ms`` defaults to "now" and should only be passed explicitly when
        replaying recorded data or building fixtures.
        """
        now_ms = int(time.time() * 1000) if received_ts_ms is None else int(received_ts_ms)
        with self._lock:
            self.con.execute(
                """INSERT INTO events
                   (received_ts_ms, source_ts_ms, source, kind, match_id,
                    asset_id, market_id, round_num, payload)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    now_ms,
                    source_ts_ms,
                    source,
                    kind,
                    match_id,
                    asset_id,
                    market_id,
                    round_num,
                    json.dumps(payload, default=str),
                ),
            )
        return now_ms

    def close(self) -> None:
        try:
            self.con.close()
        except Exception:
            pass
