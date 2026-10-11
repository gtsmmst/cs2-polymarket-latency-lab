"""
Offline gap analysis over the SQLite events bus.

For every game event from the chosen source(s) (gotv / panda / manual / bo3) find the
FIRST subsequent Polymarket event (price_change / last_trade_price / best_bid_ask) within
a window of +N ms and record the delta. Group by (source, kind) and print N, median,
p95, min, max.

A positive gap means the market moved *after* the game event reached this machine; a
small or zero gap means the market had already moved by the time our feed saw the event
(i.e. someone had a faster source). Gaps larger than the window are treated as
"no reaction" and dropped.

Usage:
    python -m scripts.analyze
    python -m scripts.analyze --source gotv --source manual
    python -m scripts.analyze --since "2026-06-09 12:00"
    python -m scripts.analyze --window-ms 30000 --csv gaps.csv
"""
import argparse
import csv
import math
import sqlite3
import statistics
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from watchers._db import DB_PATH, POLY_PRICE_KINDS, connect  # noqa: E402

DEFAULT_SOURCES = ("gotv",)
DEFAULT_WINDOW_MS = 30_000

# Event kinds that are heartbeats / metadata rather than game events.
NOISE_KINDS = ("tick", "match_meta")


def parse_since(s: Optional[str]) -> int:
    """ISO datetime ('2026-06-09 12:00' or '2026-06-09T12:00') -> epoch milliseconds."""
    if not s:
        return 0
    dt = datetime.fromisoformat(s.replace(" ", "T"))
    return int(dt.timestamp() * 1000)


def percentile(values: list[int], q: float) -> int:
    """Nearest-rank percentile (q in 0..1). Well-defined for any non-empty list."""
    if not values:
        raise ValueError("percentile of empty list")
    ordered = sorted(values)
    rank = max(1, math.ceil(q * len(ordered)))
    return ordered[rank - 1]


def compute_gaps(
    con: sqlite3.Connection,
    sources: Iterable[str] = DEFAULT_SOURCES,
    *,
    since_ms: int = 0,
    window_ms: int = DEFAULT_WINDOW_MS,
) -> list[dict]:
    """Pair every game event with the first following Polymarket price event.

    Returns one row per paired event:
        {source, kind, event_ts, poly_kind, poly_ts, gap_ms, asset_id}

    Events with no Polymarket activity within ``window_ms`` are skipped. Several game
    events may legitimately pair with the same Polymarket tick (e.g. a burst of kills
    followed by one price move).
    """
    sources = tuple(sources)
    if not sources:
        return []
    src_placeholders = ",".join("?" * len(sources))
    noise_placeholders = ",".join("?" * len(NOISE_KINDS))
    poly_placeholders = ",".join("?" * len(POLY_PRICE_KINDS))

    game_rows = con.execute(
        f"SELECT received_ts_ms, source, kind FROM events "
        f"WHERE source IN ({src_placeholders}) AND kind NOT IN ({noise_placeholders}) "
        f"AND received_ts_ms >= ? ORDER BY received_ts_ms",
        (*sources, *NOISE_KINDS, since_ms),
    ).fetchall()

    poly_rows = con.execute(
        f"SELECT received_ts_ms, kind, asset_id FROM events "
        f"WHERE source='poly' AND kind IN ({poly_placeholders}) "
        f"AND received_ts_ms >= ? ORDER BY received_ts_ms",
        (*POLY_PRICE_KINDS, since_ms),
    ).fetchall()

    if not game_rows or not poly_rows:
        return []

    # Both lists are sorted by time, so a single forward pointer into the Polymarket
    # list suffices: advance it to the first tick at or after each game event.
    poly_ts = [r[0] for r in poly_rows]
    rows_out: list[dict] = []
    p_idx = 0
    for g in game_rows:
        g_ts, source, kind = g[0], g[1], g[2]
        while p_idx < len(poly_ts) and poly_ts[p_idx] < g_ts:
            p_idx += 1
        if p_idx >= len(poly_ts):
            break
        delta = poly_ts[p_idx] - g_ts
        if delta > window_ms:
            continue
        rows_out.append(
            {
                "source": source,
                "kind": kind,
                "event_ts": g_ts,
                "poly_kind": poly_rows[p_idx][1],
                "poly_ts": poly_ts[p_idx],
                "gap_ms": delta,
                "asset_id": poly_rows[p_idx][2],
            }
        )
    return rows_out


def summarize(rows: list[dict]) -> dict[tuple[str, str], dict]:
    """Aggregate paired rows into per-(source, kind) statistics."""
    grouped: dict[tuple[str, str], list[int]] = {}
    for r in rows:
        grouped.setdefault((r["source"], r["kind"]), []).append(r["gap_ms"])
    out: dict[tuple[str, str], dict] = {}
    for key, vals in grouped.items():
        out[key] = {
            "n": len(vals),
            "median_ms": int(statistics.median(vals)),
            "p95_ms": percentile(vals, 0.95),
            "min_ms": min(vals),
            "max_ms": max(vals),
        }
    return out


def print_summary(stats: dict[tuple[str, str], dict], window_ms: int) -> None:
    print(f"\nGap analysis: game event -> first Polymarket tick (window {window_ms} ms)\n")
    print(f"{'source':8} {'kind':28} {'N':>5} {'median ms':>10} {'p95 ms':>10} {'min ms':>8} {'max ms':>8}")
    print("-" * 84)
    for (source, kind), s in sorted(stats.items(), key=lambda kv: (kv[0][0], -kv[1]["n"])):
        print(
            f"{source:8} {kind:28} {s['n']:>5} {s['median_ms']:>10} {s['p95_ms']:>10} "
            f"{s['min_ms']:>8} {s['max_ms']:>8}"
        )


def main(argv: Optional[list[str]] = None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(DB_PATH), help="path to events.sqlite (default: $LATENCY_LAB_DB or data/events.sqlite)")
    ap.add_argument("--source", action="append", dest="sources",
                    help="game-event source to analyse (repeatable). Default: gotv")
    ap.add_argument("--since", help="ISO datetime, e.g. '2026-06-09 12:00'")
    ap.add_argument("--window-ms", type=int, default=DEFAULT_WINDOW_MS)
    ap.add_argument("--csv", help="optional path to export the per-event gap table")
    args = ap.parse_args(argv)

    sources = tuple(args.sources) if args.sources else DEFAULT_SOURCES
    con = connect(Path(args.db))
    rows = compute_gaps(con, sources, since_ms=parse_since(args.since), window_ms=args.window_ms)
    con.close()

    if not rows:
        print(f"No pairs found for sources={sources}. Is there data from both the game feed and 'poly'?")
        return

    print_summary(summarize(rows), args.window_ms)

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"\nCSV written: {args.csv} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
