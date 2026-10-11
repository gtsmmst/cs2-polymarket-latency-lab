"""
Polymarket CLOB WebSocket client + match lookup (in-process, callback based).

This is the library counterpart of ``poly_ws_watcher`` (which writes to SQLite).
It is used by ``scripts/latency_lab.py`` for live, in-memory pairing.

Two pieces:

1. ``find_polymarket_cs2_market(sess, team_a, team_b, date)`` — query the Gamma API to
   find the conditionId + clobTokenIds (asset IDs) for a specific CS2 match.

2. ``PolymarketWSFeed`` — connects to wss://ws-subscriptions-clob.polymarket.com/ws/market
   and streams book + price_change events for a set of asset IDs. Every message gets a
   ``_recv_ts`` (local time.time()) for latency analysis.

Public URL pattern observed (June 2026):
    https://polymarket.com/esports/cs2/{tournament-slug}/cs2-{teamA-abbr}-{teamB-abbr}-{YYYY-MM-DD}

Gamma API lookup:
    GET https://gamma-api.polymarket.com/events?slug={match-slug}
    -> event with markets[].conditionId and markets[].clobTokenIds ([team_a_token, team_b_token])
"""

import asyncio
import json
import logging
import re
import time
from collections import defaultdict, deque
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger("polymarket")

GAMMA = "https://gamma-api.polymarket.com"
WSS_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


# ---------------------------------------------------------------------------
# Match lookup
# ---------------------------------------------------------------------------
def _slugify(s: str) -> str:
    """Lowercase, replace spaces with hyphens, strip non-alphanumeric."""
    return re.sub(r"[^a-z0-9-]", "", re.sub(r"\s+", "-", s.lower().strip()))


# Team abbreviations as they appear in Polymarket CS2 slugs. Extend as needed.
TEAM_ABBR = {
    "natus vincere": "navi", "navi": "navi",
    "g2 esports": "g2", "g2": "g2",
    "faze clan": "faze", "faze": "faze",
    "team vitality": "vit", "vitality": "vit",
    "team spirit": "spirit", "spirit": "spirit",
    "team liquid": "liq", "liquid": "liq",
    "mibr": "mibr",
    "9z team": "9z", "9z": "9z",
    "themongolz": "mngz", "the mongolz": "mngz", "mongolz": "mngz",
    "fut esports": "fut", "fut": "fut",
    "betboom team": "bbt", "betboom": "bbt",
    "complexity gaming": "col", "complexity": "col",
    "heroic": "hero",
    "astralis": "ast",
    "ence": "ence",
    "gamerlegion": "gl", "gamer legion": "gl",
    "movistar koi": "koi", "koi": "koi",
    "fnatic": "fnc",
    "cloud9": "c9",
    "bebop": "bbp",
    "rustec": "rust",
    "sparta": "spar",
    "g2 ares": "ares",
    "100 thieves": "100t",
    "betclic apogee esports": "apo",
    "tdk": "tdk",
    "young ninjas": "yn",
    "walczaki": "wal",
    "ground zero": "gz",
    "markandlarry": "mal",
    "rooster": "roo",
    "dxa esports": "dxa", "dxa": "dxa",
    "team nemesis": "nem", "nemesis": "nem",
    "cybershoke esports": "cs", "cybershoke": "cs",
    "marsborne": "mars", "iowa stormboar": "stb",
    "hyperspirit": "hyp",
    "sashi esport": "sashi", "sashi": "sashi",
    "infinite": "inf",
    "ex-ruby": "ruby",
}


def team_abbr(name: str) -> str:
    """Return Polymarket-style 2-5 char team abbreviation, falling back to first 3 letters."""
    key = name.lower().strip()
    if key in TEAM_ABBR:
        return TEAM_ABBR[key]
    # Strip common suffixes
    for suf in [" esports", " gaming", " team", " clan", " club"]:
        if key.endswith(suf):
            key = key[: -len(suf)]
            if key in TEAM_ABBR:
                return TEAM_ABBR[key]
    # Use first 3-4 letters, alphanumeric only
    clean = re.sub(r"[^a-z0-9]", "", key)
    return clean[:4]


def candidate_slugs(team_a: str, team_b: str, date_iso: str) -> List[str]:
    """Generate likely Polymarket slug candidates for a match."""
    a, b = team_abbr(team_a), team_abbr(team_b)
    d = date_iso[:10]  # YYYY-MM-DD
    return [
        f"cs2-{a}-{b}-{d}",
        f"cs2-{b}-{a}-{d}",
    ]


async def find_polymarket_cs2_market(
    sess, team_a: str, team_b: str, date_iso: str
) -> Optional[Dict[str, Any]]:
    """Try to find a Polymarket market for this match.

    Returns a dict with conditionId, clobTokenIds, outcomes, prices, slug, url.
    Returns None if not found.
    """
    for slug in candidate_slugs(team_a, team_b, date_iso):
        try:
            async with sess.get(f"{GAMMA}/events?slug={slug}", timeout=6) as r:
                if r.status != 200:
                    continue
                data = await r.json()
                if not data:
                    continue
                event = data[0]
                markets = event.get("markets", [])
                if not markets:
                    continue
                market = markets[0]  # Match-winner market is usually first
                token_ids = market.get("clobTokenIds")
                if isinstance(token_ids, str):
                    try:
                        token_ids = json.loads(token_ids)
                    except Exception:
                        token_ids = []
                outcomes = market.get("outcomes")
                if isinstance(outcomes, str):
                    try:
                        outcomes = json.loads(outcomes)
                    except Exception:
                        outcomes = []
                prices = market.get("outcomePrices")
                if isinstance(prices, str):
                    try:
                        prices = json.loads(prices)
                    except Exception:
                        prices = []

                return {
                    "slug": slug,
                    "url": f"https://polymarket.com/event/{event.get('slug')}",
                    "event_slug": event.get("slug"),
                    "event_id": event.get("id"),
                    "condition_id": market.get("conditionId"),
                    "question": market.get("question"),
                    "token_ids": token_ids,
                    "outcomes": outcomes,
                    "prices": prices,
                    "found_team_order": "ab" if slug.startswith(f"cs2-{team_abbr(team_a)}-") else "ba",
                }
        except Exception as e:
            log.debug(f"lookup {slug} err: {e}")
    return None


# ---------------------------------------------------------------------------
# WebSocket client
# ---------------------------------------------------------------------------
class PolymarketWSFeed:
    """Subscribe to Polymarket CLOB book + price_change updates for asset IDs."""

    def __init__(self, on_update: Optional[Callable[[Dict[str, Any]], None]] = None):
        self.on_update = on_update or (lambda x: None)
        self._asset_ids: List[str] = []
        self._stop = False
        self._task: Optional[asyncio.Task] = None
        self._latest_book: Dict[str, Dict[str, Any]] = {}    # asset_id -> last book
        self._latest_price: Dict[str, Dict[str, Any]] = {}   # asset_id -> last price_change
        self._history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=500))

    async def start(self, asset_ids: List[str]):
        self._asset_ids = list(asset_ids)
        self._stop = False
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        self._stop = True
        if self._task:
            self._task.cancel()

    def update_subscription(self, new_asset_ids: List[str]):
        """Restart with a different asset list (forces reconnect)."""
        self._asset_ids = list(new_asset_ids)
        if self._task:
            self._task.cancel()
        self._task = asyncio.create_task(self._loop())

    def get_latest(self, asset_id: str) -> Dict[str, Any]:
        return {
            "book": self._latest_book.get(asset_id),
            "price": self._latest_price.get(asset_id),
            "history_len": len(self._history.get(asset_id, [])),
        }

    def get_history(self, asset_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        return list(self._history.get(asset_id, []))[:limit]

    async def _loop(self):
        import websockets
        backoff = 1.0
        while not self._stop:
            if not self._asset_ids:
                await asyncio.sleep(1.0)
                continue
            try:
                async with websockets.connect(WSS_MARKET, ping_interval=20) as ws:
                    sub = {"assets_ids": self._asset_ids, "type": "market"}
                    await ws.send(json.dumps(sub))
                    log.info(f"Polymarket WSS subscribed to {len(self._asset_ids)} assets")
                    backoff = 1.0
                    async for raw in ws:
                        recv_ts = time.time()
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        # Server batches multiple events as a JSON array
                        events = data if isinstance(data, list) else [data]
                        for ev in events:
                            ev["_recv_ts"] = recv_ts
                            asset = ev.get("asset_id") or ev.get("market")
                            etype = ev.get("event_type") or ev.get("type")
                            if etype == "book":
                                self._latest_book[asset] = ev
                            elif etype == "price_change":
                                self._latest_price[asset] = ev
                            if asset:
                                self._history[asset].appendleft(ev)
                            self.on_update(ev)
            except Exception as e:
                log.info(f"Polymarket WSS disconnect: {e}, backoff={backoff}s")
                await asyncio.sleep(min(backoff, 30))
                backoff *= 2
