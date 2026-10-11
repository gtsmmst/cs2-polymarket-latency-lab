"""Offline tests for the live pairing pieces in scripts/latency_lab.py (no network).

``SqliteBusFeed`` is driven by rows written to a temp DB; ``PairTracker`` is fed
synthetic Polymarket price_change events.
"""
import asyncio
import json

import pytest

from scripts import latency_lab as lab
from watchers._db import EventLog


def test_norm_mid():
    assert lab._norm_mid(None) == 0
    assert lab._norm_mid("", default="x") == "x"
    assert lab._norm_mid("1513130") == 1513130
    assert lab._norm_mid("m1") == "m1"


def test_pair_tracker_pairs_oldest_event_with_next_price_change(tmp_path, monkeypatch):
    # Redirect CSV output into the temp dir.
    monkeypatch.setattr(lab, "OUT_DIR", tmp_path)
    monkeypatch.setattr(lab, "LOG_CSV", tmp_path / "latency_log.csv")
    monkeypatch.setattr(lab, "SUMMARY_TXT", tmp_path / "latency_summary.txt")

    tr = lab.PairTracker()
    tr.register_match_assets(7, {"A": "tok-a", "B": "tok-b"})

    tr.on_game_event({"type": "round_end", "_recv_ts": 100.0, "_match_id": 7, "source": "gotv", "winner": "A"})
    tr.on_game_event({"type": "tick", "_recv_ts": 100.1, "_match_id": 7, "source": "gotv"})  # ignored
    tr.on_game_event({"type": "kill", "_recv_ts": 101.0, "_match_id": 7, "source": "gotv"})

    # Unknown asset -> ignored; non-price_change -> ignored.
    tr.on_poly_event({"event_type": "price_change", "asset_id": "other", "_recv_ts": 100.2})
    tr.on_poly_event({"event_type": "book", "asset_id": "tok-a", "_recv_ts": 100.2})
    assert tr.rows_written == 0

    tr.on_poly_event({"event_type": "price_change", "asset_id": "tok-a", "_recv_ts": 100.45,
                      "price_changes": [{"price": "0.6"}]})
    assert tr.rows_written == 1
    assert tr.deltas_ms == pytest.approx([450.0])

    # Event older than the window is dropped instead of paired.
    tr.on_poly_event({"event_type": "price_change", "asset_id": "tok-b", "_recv_ts": 101.0 + lab.PAIR_WINDOW_S + 1})
    assert tr.rows_written == 1
    assert not tr.pending[7]

    lab.write_summary(tr)
    assert "pairs collected: 1" in (tmp_path / "latency_summary.txt").read_text()
    csv_text = (tmp_path / "latency_log.csv").read_text().splitlines()
    assert csv_text[0].startswith("ts_iso,match_id,game_source,game_event_type")
    assert ",gotv,round_end,A,A,tok-a," in csv_text[1]


def test_sqlite_bus_feed_tails_new_rows_and_strips_prefixes(tmp_path):
    db = tmp_path / "events.sqlite"
    log = EventLog(db)
    # Rows written BEFORE the feed starts are not replayed (except match_meta).
    log.write("gotv", "round_end", {"winner": "T"}, match_id="5", received_ts_ms=1_000)
    log.write("panda", "match_meta", {"name": "A vs B", "team_a": "A", "team_b": "B"}, match_id="5")

    got = []
    feed = lab.SqliteBusFeed(got.append, db_path=db, sources=("gotv", "panda", "manual"), poll_s=0.05)

    async def run():
        await feed.start()
        await asyncio.sleep(0.15)
        log.write("gotv", "kill", {"killer": "x"}, match_id="5", received_ts_ms=2_000)
        log.write("gotv", "tick", {}, match_id="5", received_ts_ms=2_001)
        log.write("panda", "ws:round_end", {"winner": "B"}, match_id="5", received_ts_ms=2_500)
        log.write("manual", "manual:bomb_planted", {}, received_ts_ms=3_000)
        log.write("poly", "price_change", {}, asset_id="tok", received_ts_ms=3_100)  # not a game source
        await asyncio.sleep(0.3)
        await feed.stop()

    asyncio.run(run())
    log.close()

    kinds = [(e["source"], e["type"], e["_recv_ts"], e["_match_id"]) for e in got]
    assert kinds == [
        ("gotv", "kill", 2.0, 5),
        ("panda", "round_end", 2.5, 5),
        ("manual", "bomb_planted", 3.0, 0),
    ]
    assert got[1]["winner"] == "B"
    assert feed.get_matches() == [{"id": 5, "team1": {"name": "A"}, "team2": {"name": "B"}, "name": "A vs B"}]


def test_place_bet_is_not_implemented():
    with pytest.raises(NotImplementedError):
        lab.place_bet()
