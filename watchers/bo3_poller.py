"""
Fallback live source: BO3.gg public match page.

Polls the match page every 2 s, extracts the embedded ``__NEXT_DATA__`` JSON and writes a
``snapshot`` event whenever the match state changes. Coarse (polling, page-level state,
no round-level events) but independent of every other feed, which is what makes it
useful as a sanity reference.

Run:
    python -m watchers.bo3_poller --match-slug astralis-vs-vitality-iem-cologne
"""
import argparse
import asyncio
import json
import logging
import re

import httpx

from ._db import EventLog

LOG = logging.getLogger("bo3")
POLL_INTERVAL = 2.0


async def run(slug: str):
    log = EventLog()
    url = f"https://bo3.gg/matches/{slug}"
    prev_hash = None
    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
        while True:
            try:
                r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                # The BO3 page embeds a __NEXT_DATA__ JSON blob with the match state.
                m = re.search(
                    r'<script id="__NEXT_DATA__"[^>]*>(.+?)</script>',
                    r.text,
                    re.S,
                )
                if not m:
                    LOG.warning("no __NEXT_DATA__ in page")
                else:
                    data = json.loads(m.group(1))
                    h = hash(json.dumps(data, sort_keys=True))
                    if h != prev_hash:
                        prev_hash = h
                        log.write(
                            source="bo3",
                            kind="snapshot",
                            payload=data.get("props", {}).get("pageProps", {}),
                            match_id=slug,
                        )
                        LOG.info("snapshot changed")
            except Exception as e:
                LOG.warning("poll error: %s", e)
            await asyncio.sleep(POLL_INTERVAL)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s :: %(message)s")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--match-slug", required=True, help="bo3.gg match url slug")
    args = ap.parse_args()
    try:
        asyncio.run(run(args.match_slug))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
