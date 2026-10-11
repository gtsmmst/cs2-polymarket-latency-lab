"""
Live pairing of game events with Polymarket price moves (CSV output).

PURPOSE
=======
``scripts/analyze.py`` works offline on the SQLite bus. This script does the same
pairing *live*: it tails game events as watchers write them into the bus, listens to
the Polymarket CLOB WebSocket in-process, pairs each game event with the next
``price_change`` on that match's tokens, and appends ``delta_ms = t_poly - t_game`` to
a CSV as it happens. A one-page summary is rewritten every minute.

If the median delta is clearly positive, the game feed reached this machine before the
market moved. If it is near zero or negative, the market already had a faster source.
Nothing here places orders; see ``place_bet`` at the bottom.

HOW IT WORKS
============
1. ``SqliteBusFeed`` polls the events table for new rows from the chosen sources
   (default: gotv, panda, manual) and forwards kill / round_end / bomb events.
2. ``PolymarketWSFeed`` streams price events for the subscribed asset ids.
3. ``PairTracker`` pairs the oldest unpaired game event of a match with the next
   price_change on one of that match's assets, within ``PAIR_WINDOW_S``.

Asset ids are either given explicitly (``--asset-ids``) or discovered from the
``match_meta`` events written by the PandaScore watcher (team names -> Gamma API slug).

USAGE
=====
    # run the watchers first (gotv_ingest + Go parser, and/or panda_cs2_watcher), then:
    python -m scripts.latency_lab --asset-ids <token_a>,<token_b> --team-a NAVI --team-b G2
    python -m scripts.latency_lab --match-id 1234567        # auto-discover assets via match_meta
    python -m scripts.latency_lab --source gotv --source manual

OUTPUT
======
    latency_data/latency_log.csv      - one row per paired event
    latency_data/latency_summary.txt  - distribution stats, rewritten every minute
"""

import argparse
import asyncio
import csv
import json
import logging
import statistics
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from watchers._db import DB_PATH, connect  # noqa: E402
from watchers.polymarket_feed import PolymarketWSFeed, find_polymarket_cs2_market  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lab")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OUT_DIR = Path("./latency_data")
LOG_CSV = OUT_DIR / "latency_log.csv"
SUMMARY_TXT = OUT_DIR / "latency_summary.txt"

PAIR_WINDOW_S = 30.0  # max seconds to wait for a Polymarket reaction after a game event
PAIRABLE_KINDS = ("kill", "round_end", "bomb_planted", "bomb_defused", "bomb_exploded", "win")
DEFAULT_SOURCES = ("gotv", "panda", "manual")

MatchId = Union[int, str]


def _norm_mid(raw: Any, default: MatchId = 0) -> MatchId:
    """Normalise a match id coming from the bus (TEXT column) so keys compare equal."""
    if raw is None or raw == "":
        return default
    s = str(raw)
    return int(s) if s.isdigit() else s


# ---------------------------------------------------------------------------
# Game feed: tail the SQLite bus
# ---------------------------------------------------------------------------
class SqliteBusFeed:
    """Forward new game events from the events bus to a callback.

    Events are delivered as dicts with ``type``, ``_recv_ts`` (seconds, local clock),
    ``_match_id``, ``source`` and the original payload fields. ``match_meta`` rows from
    the PandaScore watcher are kept so ``get_matches()`` can drive asset discovery.
    """

    def __init__(
        self,
        on_event: Callable[[Dict[str, Any]], None],
        *,
        db_path: Path = DB_PATH,
        sources: tuple = DEFAULT_SOURCES,
        default_match_id: MatchId = 0,
        poll_s: float = 0.25,
        meta_lookback_s: int = 3 * 3600,
    ):
        self.on_event = on_event
        self.db_path = db_path
        self.sources = tuple(sources)
        self.default_match_id = default_match_id
        self.poll_s = poll_s
        self.meta_lookback_s = meta_lookback_s
        self._matches: Dict[MatchId, Dict[str, Any]] = {}
        self._task: Optional[asyncio.Task] = None
        self._stop = False

    async def start(self):
        self._stop = False
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        self._stop = True
        if self._task:
            self._task.cancel()

    def get_matches(self) -> List[Dict[str, Any]]:
        return list(self._matches.values())

    def _remember_match(self, match_id: MatchId, payload: Dict[str, Any]):
        if payload.get("team_a") and payload.get("team_b"):
            self._matches[match_id] = {
                "id": match_id,
                "team1": {"name": payload["team_a"]},
                "team2": {"name": payload["team_b"]},
                "name": payload.get("name"),
            }

    async def _loop(self):
        con = connect(self.db_path)
        placeholders = ",".join("?" * len(self.sources))
        # Pick up recent match metadata, then only follow rows written from now on.
        since_ms = int(time.time() * 1000) - self.meta_lookback_s * 1000
        for r in con.execute(
            "SELECT match_id, payload FROM events WHERE source='panda' AND kind='match_meta' "
            "AND received_ts_ms >= ? ORDER BY id",
            (since_ms,),
        ):
            try:
                self._remember_match(_norm_mid(r["match_id"], self.default_match_id), json.loads(r["payload"]))
            except Exception:
                pass
        last_id = con.execute("SELECT COALESCE(MAX(id), 0) FROM events").fetchone()[0]
        log.info(f"bus feed: tailing {self.db_path} from id {last_id}, sources={self.sources}")

        try:
            while not self._stop:
                rows = con.execute(
                    f"SELECT id, received_ts_ms, source, kind, match_id, payload FROM events "
                    f"WHERE id > ? AND source IN ({placeholders}) ORDER BY id",
                    (last_id, *self.sources),
                ).fetchall()
                for r in rows:
                    last_id = r["id"]
                    kind = r["kind"]
                    for prefix in ("ws:", "manual:"):
                        if kind.startswith(prefix):
                            kind = kind[len(prefix):]
                    try:
                        payload = json.loads(r["payload"]) if r["payload"] else {}
                    except Exception:
                        payload = {}
                    mid = _norm_mid(r["match_id"], self.default_match_id)
                    if kind == "match_meta":
                        self._remember_match(mid, payload)
                        continue
                    if kind == "tick":
                        continue
                    ev = {**payload, "type": kind, "source": r["source"],
                          "_recv_ts": r["received_ts_ms"] / 1000.0, "_match_id": mid}
                    self.on_event(ev)
                await asyncio.sleep(self.poll_s)
        finally:
            con.close()


# ---------------------------------------------------------------------------
# Pairing state
# ---------------------------------------------------------------------------
class PairTracker:
    """Tracks unpaired game events and matches them to the next Polymarket price_change."""

    def __init__(self):
        # match_id -> deque of unmatched game events
        self.pending: Dict[MatchId, deque] = defaultdict(lambda: deque(maxlen=50))
        # match_id -> {team_name: asset_id}
        self.match_assets: Dict[MatchId, Dict[str, str]] = {}
        # match_id -> {asset_id: team_name}
        self.asset_to_team: Dict[MatchId, Dict[str, str]] = {}

        self.rows_written = 0
        self.deltas_ms: List[float] = []

    def on_game_event(self, ev: Dict[str, Any]):
        mid = ev.get("_match_id")
        if mid is None:
            return
        etype = ev.get("type") or ev.get("event_type")
        if etype not in PAIRABLE_KINDS:
            return
        self.pending[mid].append({
            "t_game": ev["_recv_ts"],
            "type": etype,
            "source": ev.get("source", ""),
            "winner": ev.get("winner") or ev.get("winning_team_name"),
            "raw": ev,
        })

    def register_match_assets(self, match_id: MatchId, mapping: Dict[str, str]):
        """mapping: {team_name: asset_id}"""
        self.match_assets[match_id] = mapping
        self.asset_to_team[match_id] = {v: k for k, v in mapping.items()}

    def on_poly_event(self, ev: Dict[str, Any]):
        if (ev.get("event_type") or "") != "price_change":
            return
        asset = ev.get("asset_id")
        t_poly = ev["_recv_ts"]
        for mid, mapping in self.asset_to_team.items():
            if asset in mapping:
                self._try_pair(mid, asset, mapping[asset], t_poly, ev)
                return

    def _try_pair(self, match_id: MatchId, asset_id: str, team: str,
                  t_poly: float, poly_ev: Dict[str, Any]):
        if not self.pending[match_id]:
            return
        game_ev = self.pending[match_id][0]
        delta_s = t_poly - game_ev["t_game"]
        if delta_s < 0 or delta_s > PAIR_WINDOW_S:
            if delta_s > PAIR_WINDOW_S:
                self.pending[match_id].popleft()  # too old, drop
            return

        delta_ms = delta_s * 1000.0
        row = {
            "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "match_id": match_id,
            "game_source": game_ev["source"],
            "game_event_type": game_ev["type"],
            "game_winner": game_ev.get("winner") or "",
            "poly_asset_team": team,
            "poly_asset_id": asset_id,
            "t_game": f"{game_ev['t_game']:.3f}",
            "t_poly": f"{t_poly:.3f}",
            "delta_ms": f"{delta_ms:.1f}",
            "poly_price_changes": json.dumps(poly_ev.get("price_changes") or []),
        }
        _append_csv(row)
        self.deltas_ms.append(delta_ms)
        self.rows_written += 1
        log.info(f"PAIR #{self.rows_written}: match={match_id} "
                 f"{game_ev['source']}:{game_ev['type']} -> poly delta={delta_ms:.0f}ms (team={team})")

        self.pending[match_id].popleft()  # do not double-pair


def _append_csv(row: Dict[str, Any]):
    OUT_DIR.mkdir(exist_ok=True)
    fields = list(row.keys())
    new = not LOG_CSV.exists()
    with open(LOG_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if new:
            w.writeheader()
        w.writerow(row)


def write_summary(tracker: PairTracker):
    OUT_DIR.mkdir(exist_ok=True)
    deltas = tracker.deltas_ms
    if not deltas:
        SUMMARY_TXT.write_text(f"No pairs yet. Pending events: "
                               f"{sum(len(q) for q in tracker.pending.values())}\n")
        return
    s = (
        f"=== Latency Lab — game feed vs Polymarket ===\n"
        f"  pairs collected: {len(deltas)}\n"
        f"  delta_ms (Poly - game event):\n"
        f"    min     : {min(deltas):.0f}\n"
        f"    median  : {statistics.median(deltas):.0f}\n"
        f"    mean    : {statistics.mean(deltas):.0f}\n"
        f"    p90     : {sorted(deltas)[int(0.9 * len(deltas))]:.0f}\n"
        f"    max     : {max(deltas):.0f}\n"
        f"  matches tracked: {len(tracker.match_assets)}\n"
        f"  last updated: {time.strftime('%H:%M:%S')}\n"
    )
    SUMMARY_TXT.write_text(s)
    log.info("Summary updated.")
    print("\n" + s)


# ---------------------------------------------------------------------------
# Asset discovery: for every match known from match_meta that has no assets yet,
# look up the Polymarket market via the Gamma API and (re)subscribe the WS feed.
# ---------------------------------------------------------------------------
async def pairing_loop(game: SqliteBusFeed, poly: PolymarketWSFeed, tracker: PairTracker,
                       only_match_id: Optional[MatchId] = None):
    import aiohttp
    while True:
        try:
            matches = game.get_matches()
            if only_match_id is not None:
                matches = [m for m in matches if m["id"] == only_match_id]
            if matches:
                async with aiohttp.ClientSession() as sess:
                    asset_ids: List[str] = []
                    changed = False
                    for m in matches:
                        mid = m["id"]
                        if mid in tracker.match_assets:
                            asset_ids.extend(tracker.match_assets[mid].values())
                            continue
                        t1, t2 = m["team1"]["name"], m["team2"]["name"]
                        date_iso = time.strftime("%Y-%m-%d")
                        result = await find_polymarket_cs2_market(sess, t1, t2, date_iso)
                        if result and result.get("token_ids"):
                            tokens = result["token_ids"]
                            # First token in the Polymarket array is the first outcome
                            mapping = {t1: tokens[0], t2: tokens[1]} \
                                if result["found_team_order"] == "ab" \
                                else {t2: tokens[0], t1: tokens[1]}
                            tracker.register_match_assets(mid, mapping)
                            asset_ids.extend(mapping.values())
                            changed = True
                            log.info(f"PAIRED: {t1} vs {t2} -> {result['slug']} ({result['prices']})")
                        else:
                            log.debug(f"No Polymarket market for {t1} vs {t2}")
                    if asset_ids and changed:
                        poly.update_subscription(asset_ids)
        except Exception as e:
            log.warning(f"pairing_loop error: {e}")
        await asyncio.sleep(60)


async def summary_loop(tracker: PairTracker):
    while True:
        await asyncio.sleep(60)
        write_summary(tracker)


def place_bet(*args, **kwargs):
    """Intentionally not implemented. This project measures latency; it does not trade."""
    raise NotImplementedError("latency lab is measurement-only; no order placement exists")


# ---------------------------------------------------------------------------
async def main():
    ap = argparse.ArgumentParser(
        description="Latency lab: live pairing of game events with Polymarket price moves.")
    ap.add_argument("--db", default=str(DB_PATH), help="events.sqlite to tail")
    ap.add_argument("--source", action="append", dest="sources",
                    help=f"bus source to tail (repeatable). Default: {','.join(DEFAULT_SOURCES)}")
    ap.add_argument("--match-id", type=str, default=None,
                    help="Track only this match id (as written by the watcher)")
    ap.add_argument("--asset-ids", type=str, default=None,
                    help="Comma-separated Polymarket clobTokenIds to subscribe to directly "
                         "(skips auto-discovery). Two ids: team A then team B.")
    ap.add_argument("--team-a", type=str, default="team_A",
                    help="Name of team A (matches first asset-id)")
    ap.add_argument("--team-b", type=str, default="team_B",
                    help="Name of team B (matches second asset-id)")
    args = ap.parse_args()

    sources = tuple(args.sources) if args.sources else DEFAULT_SOURCES
    target_mid: MatchId = _norm_mid(args.match_id, 0)

    tracker = PairTracker()
    game = SqliteBusFeed(tracker.on_game_event, db_path=Path(args.db), sources=sources,
                         default_match_id=target_mid)
    poly = PolymarketWSFeed(on_update=tracker.on_poly_event)

    await game.start()

    manual_assets: List[str] = []
    if args.asset_ids:
        manual_assets = [a.strip() for a in args.asset_ids.split(",") if a.strip()]
        if len(manual_assets) >= 2:
            mapping = {args.team_a: manual_assets[0], args.team_b: manual_assets[1]}
            tracker.register_match_assets(target_mid, mapping)
            log.info(f"MANUAL pair: {args.team_a}={manual_assets[0][:8]}... "
                     f"{args.team_b}={manual_assets[1][:8]}... (match_id={target_mid})")
        else:
            log.error("--asset-ids needs two comma-separated token ids")
            return

    await poly.start(manual_assets)

    tasks = [asyncio.create_task(summary_loop(tracker))]
    if not manual_assets:
        tasks.append(asyncio.create_task(
            pairing_loop(game, poly, tracker, only_match_id=target_mid if args.match_id else None)))

    OUT_DIR.mkdir(exist_ok=True)
    log.info(f"Lab running. Output: {OUT_DIR.absolute()}")
    log.info("   Press Ctrl-C to stop. Summary updated every 60s.")
    log.info(f"   CSV: {LOG_CSV.absolute()}")
    log.info(f"   Summary: {SUMMARY_TXT.absolute()}")

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Stopping...")
    finally:
        write_summary(tracker)
        await game.stop()
        await poly.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
