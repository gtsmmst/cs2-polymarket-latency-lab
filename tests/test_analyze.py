"""Tests for scripts/analyze.py against the real schema and writer, with synthetic events.

No network. A temporary SQLite file is created through ``watchers._db.EventLog`` (the
same writer every watcher uses), filled with game events and Polymarket ticks at known
offsets, and ``compute_gaps`` / ``summarize`` are checked against the expected numbers.
"""
import sqlite3

import pytest

from scripts.analyze import compute_gaps, parse_since, percentile, summarize
from watchers._db import EventLog, connect

T0 = 1_700_000_000_000  # arbitrary epoch ms base


def _write(log: EventLog, source: str, kind: str, offset_ms: int, **kw):
    return log.write(source, kind, kw.pop("payload", {}), received_ts_ms=T0 + offset_ms, **kw)


@pytest.fixture
def db(tmp_path):
    """Temp DB with:

    poly ticks   at   900, 1250, 1300, 2600, 3100, 50_000
    gotv round_end at 1000, 2000, 3000      -> first following tick gaps: 250, 600, 100
    gotv kill      at 4000                  -> next tick at 50_000 (46 s) -> outside 30 s window
    gotv tick      at 1000 (heartbeat)      -> must be ignored
    manual round_end at 2100                -> gap 500 (only when 'manual' is requested)
    poly 'book' at 1100                     -> not a price kind, must be ignored
    """
    path = tmp_path / "events.sqlite"
    log = EventLog(path)
    for off in (900, 1250, 1300, 2600, 3100, 50_000):
        _write(log, "poly", "price_change", off, asset_id="asset-A", payload={"price": "0.5"})
    _write(log, "poly", "book", 1100, asset_id="asset-A")

    for off in (1000, 2000, 3000):
        _write(log, "gotv", "round_end", off, match_id="m1", payload={"winner": "T"})
    _write(log, "gotv", "kill", 4000, match_id="m1")
    _write(log, "gotv", "tick", 1000, match_id="m1")
    _write(log, "manual", "manual:round_end", 2100)
    log.close()

    con = connect(path)
    yield con
    con.close()


def test_schema_matches_writer(db):
    cols = {row[1] for row in db.execute("PRAGMA table_info(events)")}
    assert {"received_ts_ms", "source_ts_ms", "source", "kind", "match_id",
            "asset_id", "market_id", "round_num", "payload"} <= cols
    assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 13


def test_first_following_tick_gaps(db):
    rows = compute_gaps(db, ("gotv",), window_ms=30_000)
    by_kind = {}
    for r in rows:
        by_kind.setdefault(r["kind"], []).append(r["gap_ms"])

    # The tick at 900 (before the first event) and the second tick at 1300 must not
    # change the result: only the FIRST tick at or after each event counts.
    assert by_kind["round_end"] == [250, 600, 100]
    # kill at 4000 has no tick within 30 s -> dropped entirely
    assert "kill" not in by_kind
    # heartbeat ticks are noise
    assert "tick" not in by_kind
    # every pair records which Polymarket event closed it
    assert all(r["poly_kind"] == "price_change" and r["asset_id"] == "asset-A" for r in rows)


def test_summary_medians(db):
    stats = summarize(compute_gaps(db, ("gotv",), window_ms=30_000))
    s = stats[("gotv", "round_end")]
    assert s["n"] == 3
    assert s["median_ms"] == 250
    assert s["min_ms"] == 100
    assert s["max_ms"] == 600
    assert s["p95_ms"] == 600  # nearest-rank p95 of three values is the max


def test_multiple_sources_and_window(db):
    stats = summarize(compute_gaps(db, ("gotv", "manual"), window_ms=30_000))
    assert stats[("manual", "manual:round_end")]["median_ms"] == 500
    assert stats[("gotv", "round_end")]["n"] == 3

    # A very wide window lets the kill at 4000 pair with the tick at 50 000.
    wide = summarize(compute_gaps(db, ("gotv",), window_ms=60_000))
    assert wide[("gotv", "kill")]["median_ms"] == 46_000


def test_since_filter(db):
    stats = summarize(compute_gaps(db, ("gotv",), since_ms=T0 + 1500, window_ms=30_000))
    s = stats[("gotv", "round_end")]
    assert s["n"] == 2
    assert s["median_ms"] == 350  # median of [600, 100]


def test_empty_inputs(tmp_path):
    con = connect(tmp_path / "empty.sqlite")
    assert compute_gaps(con, ("gotv",)) == []
    assert compute_gaps(con, ()) == []
    assert summarize([]) == {}
    con.close()


def test_in_memory_connection_works_too():
    con = sqlite3.connect(":memory:")
    from watchers._db import init_schema
    init_schema(con)
    con.execute("INSERT INTO events(received_ts_ms, source, kind, payload) VALUES (?,?,?,?)",
                (T0 + 10, "gotv", "round_end", "{}"))
    con.execute("INSERT INTO events(received_ts_ms, source, kind, payload) VALUES (?,?,?,?)",
                (T0 + 42, "poly", "last_trade_price", "{}"))
    rows = compute_gaps(con, ("gotv",))
    assert [r["gap_ms"] for r in rows] == [32]


def test_percentile_nearest_rank():
    vals = list(range(1, 101))  # 1..100
    assert percentile(vals, 0.95) == 95
    assert percentile(vals, 0.5) == 50
    assert percentile([7], 0.95) == 7
    with pytest.raises(ValueError):
        percentile([], 0.5)


def test_parse_since():
    assert parse_since(None) == 0
    assert parse_since("") == 0
    a = parse_since("2026-06-09 12:00")
    b = parse_since("2026-06-09T12:00")
    assert a == b and a > 0
