"""
Polymarket CLOB market-channel watcher.

Connects to wss://ws-subscriptions-clob.polymarket.com/ws/market, subscribes to the
given asset ids (CLOB token ids) and writes every message into the SQLite events bus
with source='poly'.

Run:
    python -m watchers.poly_ws_watcher --assets 7512...,8847...

Use ``scripts/find_market.py`` to look up the asset ids for a match.
"""
import argparse
import asyncio
import json
import logging
import sys

import websockets

from ._db import EventLog

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL_S = 10  # the server drops idle connections after ~10 s, so send our own ping
LOG = logging.getLogger("poly_ws")


async def _subscribe(ws, assets):
    msg = {
        "assets_ids": assets,
        "type": "market",
        "custom_feature_enabled": True,
    }
    await ws.send(json.dumps(msg))
    LOG.info("subscribed to %d assets", len(assets))


async def _keepalive(ws):
    while True:
        await asyncio.sleep(PING_INTERVAL_S)
        try:
            # Polymarket accepts a plain-text "PING"
            await ws.send("PING")
        except Exception:
            return


async def _consume(ws, log: EventLog):
    async for raw in ws:
        if raw == "PONG":
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            LOG.warning("non-json frame: %s", raw[:120])
            continue

        # The server sometimes batches several events into one JSON array
        events = data if isinstance(data, list) else [data]
        for ev in events:
            kind = ev.get("event_type", "unknown")
            try:
                src_ts = int(ev.get("timestamp")) if ev.get("timestamp") else None
            except (TypeError, ValueError):
                src_ts = None
            log.write(
                source="poly",
                kind=kind,
                payload=ev,
                source_ts_ms=src_ts,
                asset_id=ev.get("asset_id"),
                market_id=ev.get("market"),
            )
            if kind in ("best_bid_ask", "last_trade_price"):
                LOG.info(
                    "[%s] asset=%s... bid=%s ask=%s",
                    kind,
                    str(ev.get("asset_id", ""))[:10],
                    ev.get("best_bid", ev.get("price")),
                    ev.get("best_ask", ""),
                )


async def run(assets):
    log = EventLog()
    backoff = 1.0
    while True:
        try:
            LOG.info("connecting to %s", WS_URL)
            async with websockets.connect(WS_URL, ping_interval=None) as ws:
                backoff = 1.0
                await _subscribe(ws, assets)
                ka = asyncio.create_task(_keepalive(ws))
                try:
                    await _consume(ws, log)
                finally:
                    ka.cancel()
        except (websockets.ConnectionClosed, OSError) as e:
            LOG.warning("disconnect: %s; reconnecting in %.1fs", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
        except Exception as e:
            LOG.exception("unexpected: %s", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--assets",
        required=True,
        help="comma-separated Polymarket asset_id (CLOB token IDs)",
    )
    args = ap.parse_args()
    assets = [a.strip() for a in args.assets.split(",") if a.strip()]
    if not assets:
        sys.exit("no asset_ids supplied")
    try:
        asyncio.run(run(assets))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
