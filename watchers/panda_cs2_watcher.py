"""
PandaScore CS2 live watcher.

Observed behaviour on the free API tier (checked June 2026):
  works   GET /csgo/matches/running   - list of live matches, MAP-level score (1:1, 0:1)
  works   GET /csgo/matches/upcoming  - 14-day schedule
  works   GET /lives                  - live matches + their WebSocket endpoint URLs
                                        (connecting to those sockets returns a permission error)
  denied  GET /csgo/matches/{id}      - round-by-round detail
  denied  GET /csgo/games/{id}/frames - economy / positions / HP
  denied  wss://live.pandascore.co/matches/{id}/events - real-time kill / round_end

What this watcher does:
  Poll mode (default): polls /lives + /csgo/matches/running every N seconds and records
  MAP-score changes into the events bus. No round-level events, but it gives a coarse
  reference: "map ended" with roughly the polling interval of delay.

  WS mode (--ws MATCH_ID): connects to the live events socket from /lives and writes
  every message as kind='ws:<type>'. Requires a paid plan; this was never exercised with
  a working subscription, so treat it as untested plumbing.

Configuration:
  PANDASCORE_TOKEN environment variable (required). Nothing else.

Run:
    python -m watchers.panda_cs2_watcher                       # poll mode
    python -m watchers.panda_cs2_watcher --interval 5
    python -m watchers.panda_cs2_watcher --leagues "IEM,BLAST,ESL Pro,Major"
    python -m watchers.panda_cs2_watcher --ws 1234567          # WS mode (paid plan)
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

import requests

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from watchers._db import EventLog  # noqa: E402

PANDA_BASE = "https://api.pandascore.co"
PANDA_WS_BASE = "wss://live.pandascore.co"
TOKEN_ENV = "PANDASCORE_TOKEN"


def token() -> str:
    """Read the API token from the environment. Exits with a clear message if unset."""
    tok = os.environ.get(TOKEN_ENV, "").strip()
    if not tok:
        raise SystemExit(
            f"{TOKEN_ENV} is not set. Export your PandaScore API token as an environment "
            f"variable (see .env.example); it is never read from a file or hardcoded."
        )
    return tok


def http_get(path: str, params: Optional[dict] = None) -> Any:
    """REST helper with bearer authorization."""
    headers = {"Authorization": f"Bearer {token()}"}
    r = requests.get(f"{PANDA_BASE}{path}", headers=headers, params=params or {}, timeout=15)
    if r.status_code == 403:
        return {"_error": "forbidden", "_path": path}
    if r.status_code == 404:
        return {"_error": "not_found", "_path": path}
    r.raise_for_status()
    return r.json()


def list_running(leagues_filter: Optional[list[str]] = None) -> list[dict]:
    """Live CS2 matches, optionally filtered by substring of league/tournament name."""
    data = http_get("/csgo/matches/running")
    if isinstance(data, dict) and "_error" in data:
        return []
    if not leagues_filter:
        return data
    return [
        m for m in data
        if any(
            f.lower() in (m.get("league", {}).get("name", "") + " " + m.get("tournament", {}).get("name", "")).lower()
            for f in leagues_filter
        )
    ]


def list_lives() -> list[dict]:
    """/lives endpoint: live matches plus their WebSocket endpoint URLs. CS only."""
    data = http_get("/lives")
    if isinstance(data, dict) and "_error" in data:
        return []
    return [
        item for item in data
        if item.get("match", {}).get("videogame", {}).get("slug") in ("cs-go",)
    ]


def write_match_meta(log: EventLog, m: dict, now_ms: int):
    """Record match metadata (on first sight)."""
    opps = m.get("opponents") or []
    team_a = opps[0]["opponent"]["name"] if len(opps) > 0 else None
    team_b = opps[1]["opponent"]["name"] if len(opps) > 1 else None
    log.write(
        source="panda",
        kind="match_meta",
        payload={
            "match_id": m.get("id"),
            "name": m.get("name"),
            "team_a": team_a,
            "team_b": team_b,
            "league": m.get("league", {}).get("name"),
            "tournament": m.get("tournament", {}).get("name"),
            "tier": m.get("tournament", {}).get("tier"),
            "bo": m.get("number_of_games"),
            "begin_at": m.get("begin_at"),
            "low_latency_feed": m.get("low_latency_feed"),
            "live_supported": m.get("tournament", {}).get("live_supported"),
        },
        source_ts_ms=now_ms,
        match_id=str(m.get("id")),
    )


def write_score_tick(log: EventLog, m: dict, now_ms: int):
    """Record the current map-level score of a match."""
    results = m.get("results") or []
    opps = m.get("opponents") or []
    team_a_name = opps[0]["opponent"]["name"] if len(opps) > 0 else None
    team_b_name = opps[1]["opponent"]["name"] if len(opps) > 1 else None
    score_a = results[0]["score"] if len(results) > 0 else None
    score_b = results[1]["score"] if len(results) > 1 else None
    log.write(
        source="panda",
        kind="map_score",
        payload={
            "name": m.get("name"),
            "team_a": team_a_name,
            "team_b": team_b_name,
            "maps_a": score_a,
            "maps_b": score_b,
            "status": m.get("status"),
        },
        source_ts_ms=now_ms,
        match_id=str(m.get("id")),
    )


def poll_loop(interval: int, leagues_filter: Optional[list[str]]):
    """Main loop for the REST polling mode."""
    token()  # fail fast if the token is missing
    log = EventLog()
    print(f"[panda] poll loop, interval={interval}s leagues={leagues_filter}")
    seen_matches = set()
    last_score: dict[str, tuple] = {}  # match_id -> (score_a, score_b)

    while True:
        try:
            running = list_running(leagues_filter)
            lives = list_lives()
            now_ms = int(time.time() * 1000)

            # Merge by match id; /lives carries the WS endpoint metadata when present.
            by_id = {}
            for m in running:
                by_id[m["id"]] = m
            for live_item in lives:
                m = live_item["match"]
                if m["id"] in by_id:
                    by_id[m["id"]].update({
                        "low_latency_feed": m.get("low_latency_feed"),
                        "_live_endpoints": live_item.get("endpoints"),
                    })

            matches = list(by_id.values())

            for m in matches:
                mid = str(m["id"])

                if mid not in seen_matches:
                    seen_matches.add(mid)
                    write_match_meta(log, m, now_ms)

                results = m.get("results") or []
                if len(results) >= 2:
                    cur = (results[0]["score"], results[1]["score"])
                    prev = last_score.get(mid)
                    if cur != prev:
                        last_score[mid] = cur
                        write_score_tick(log, m, now_ms)
                        print(
                            f"[panda] {time.strftime('%H:%M:%S')} {m['name']}: "
                            f"score {cur[0]}:{cur[1]} (was {prev})"
                        )

            print(
                f"[panda] {time.strftime('%H:%M:%S')} active CS2 matches: "
                f"{len(matches)} (running: {len(running)}, lives: {len(lives)})"
            )

        except KeyboardInterrupt:
            print("\n[panda] interrupted")
            return
        except Exception as e:
            print(f"[panda] ERROR: {e}")
        time.sleep(interval)


def ws_loop(match_id: int):
    """Live events socket. Only works on a paid plan; the free tier closes the socket
    with a permission error. The token goes into the query string because that is how
    the PandaScore live socket authenticates; it is still only ever read from the
    environment and is never printed."""
    try:
        import websocket  # type: ignore
    except ImportError:
        raise SystemExit("pip install websocket-client")

    log = EventLog()
    url_events = f"{PANDA_WS_BASE}/matches/{match_id}/events?token={token()}"
    print(f"[panda-ws] connecting events: {url_events.split('?')[0]}")

    def on_message(ws, msg):
        try:
            payload = json.loads(msg)
        except Exception:
            payload = {"raw": msg[:500]}
        now_ms = int(time.time() * 1000)
        kind = payload.get("type") or payload.get("event") or "ws_msg"
        log.write(
            source="panda",
            kind=f"ws:{kind}",
            payload=payload,
            source_ts_ms=now_ms,
            match_id=str(match_id),
        )
        print(f"[panda-ws] {time.strftime('%H:%M:%S')} {kind}: {str(payload)[:120]}")

    def on_error(ws, err):
        print(f"[panda-ws] ERR: {err}")

    def on_close(ws, code, reason):
        print(f"[panda-ws] closed code={code} reason={reason}")
        if "permission" in str(reason).lower():
            print("[panda-ws] the live socket needs a paid plan; fall back to poll mode")

    ws = websocket.WebSocketApp(
        url_events,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(ping_interval=15, ping_timeout=5)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--interval", type=int, default=5,
                    help="seconds between REST polls (poll mode)")
    ap.add_argument("--leagues", type=str, default=None,
                    help="comma-separated substring filter: 'IEM,BLAST,Major,ESL Pro'")
    ap.add_argument("--ws", type=int, default=None,
                    help="match_id for WS mode (needs a paid plan)")
    args = ap.parse_args()

    leagues = [s.strip() for s in args.leagues.split(",")] if args.leagues else None

    if args.ws:
        ws_loop(args.ws)
    else:
        poll_loop(args.interval, leagues)


if __name__ == "__main__":
    main()
