"""
Print the Polymarket markets (and their asset ids) for a specific CS2 match.

The Gamma API resolves exact slugs reliably, while full-text search over the whole market
list gives few hits. The most dependable path is therefore: open the match page in a
browser (https://polymarket.com/esports/cs2/games), copy the URL and pass it here.

Usage:

  # 1) pass the event URL (recommended):
  python -m scripts.find_market "https://polymarket.com/esports/cs2/<tournament>/cs2-<a>-<b>-2026-06-10"

  # 2) pass just the slug (last URL segment):
  python -m scripts.find_market cs2-<a>-<b>-2026-06-10

  # 3) text search across recent esports markets:
  python -m scripts.find_market --search "Team Name"

The printed ``asset_id`` values are what ``watchers.poly_ws_watcher --assets`` expects.
"""
import argparse
import json
import re
import sys

import httpx

GAMMA = "https://gamma-api.polymarket.com"


def _from_url_or_slug(s: str) -> str:
    s = s.strip().strip('"').strip("'")
    m = re.search(r"polymarket\.com/[^?\s]+/([a-z0-9\-]+)/?(?:\?|$)", s, re.I)
    if m:
        return m.group(1)
    if re.fullmatch(r"[a-z0-9\-]+", s, re.I):
        return s
    sys.exit(
        "Could not parse a URL or slug. Expected something like:\n"
        "  https://polymarket.com/esports/cs2/<tournament>/cs2-<a>-<b>-2026-06-10\n"
        "  or just:  cs2-<a>-<b>-2026-06-10"
    )


def _parse_list_field(v):
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except json.JSONDecodeError:
            return []
    return []


def _print_market(m, indent=""):
    outcomes = _parse_list_field(m.get("outcomes"))
    prices = _parse_list_field(m.get("outcomePrices"))
    token_ids = _parse_list_field(m.get("clobTokenIds"))
    title = m.get("groupItemTitle") or m.get("question") or "?"
    print(f"{indent}-- {title}")
    print(f"{indent}   question  : {m.get('question')}")
    print(f"{indent}   condition : {m.get('conditionId')}")
    print(f"{indent}   liquidity : {m.get('liquidityNum') or m.get('liquidity')}")
    for i, tid in enumerate(token_ids):
        label = outcomes[i] if i < len(outcomes) else f"#{i}"
        price = prices[i] if i < len(prices) else "?"
        print(f"{indent}   asset_id [{label} @ {price}]: {tid}")


def by_slug(slug: str):
    # Try events first: they contain the nested markets (Match Winner + per-map).
    r = httpx.get(f"{GAMMA}/events", params={"slug": slug}, timeout=30)
    r.raise_for_status()
    events = r.json()
    if events:
        ev = events[0]
        print()
        print("=" * 70)
        print(f"  EVENT: {ev.get('title')}")
        print(f"  slug : {ev.get('slug')}")
        print(f"  end  : {ev.get('endDate')}")
        print(f"  vol  : {ev.get('volume')} | liquidity: {ev.get('liquidity')}")
        print("=" * 70)
        markets = ev.get("markets") or []
        if not markets:
            print("The event has no nested markets.")
            return

        # Match Winner first, then per-map markets.
        def _rank(m):
            t = (m.get("groupItemTitle") or "").lower()
            if "match" in t:
                return 0
            if "map 1" in t or "game1" in (m.get("slug") or ""):
                return 1
            if "map 2" in t or "game2" in (m.get("slug") or ""):
                return 2
            if "map 3" in t or "game3" in (m.get("slug") or ""):
                return 3
            return 9

        for m in sorted(markets, key=_rank):
            print()
            _print_market(m)
        print()
        print("-" * 70)
        print("For the Polymarket watcher, take the asset_id(s) from 'Match Winner'.")
        print("To measure per-map reactions, use the Map 1 / Map 2 asset ids instead.")
        return

    # Fallback: a single market with this slug.
    r = httpx.get(f"{GAMMA}/markets", params={"slug": slug}, timeout=30)
    r.raise_for_status()
    markets = r.json()
    if not markets:
        sys.exit(f"No event or market found with slug='{slug}'.")
    for m in markets:
        _print_market(m)


def by_search(query: str):
    # Page through recent active markets and filter by text.
    print(f"Searching '{query}' in active markets...")
    hits = []
    offset = 0
    page_size = 100
    while offset < 1500:  # up to 15 pages
        r = httpx.get(
            f"{GAMMA}/markets",
            params={
                "active": "true",
                "closed": "false",
                "limit": page_size,
                "offset": offset,
                "order": "startDate",
                "ascending": "false",
            },
            timeout=30,
        )
        r.raise_for_status()
        page = r.json()
        if not page:
            break
        for m in page:
            q = (m.get("question") or "") + " " + (m.get("slug") or "")
            if query.lower() in q.lower():
                hits.append(m)
        offset += page_size
        if len(page) < page_size:
            break
    if not hits:
        sys.exit(
            f"Nothing found for '{query}'.\n"
            "Most reliable: open https://polymarket.com/esports/cs2/games,\n"
            "click the match, copy the URL and pass it to this script."
        )
    print(f"found: {len(hits)}\n")
    for m in hits[:10]:
        _print_market(m)
        print()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "target",
        nargs="?",
        help="Polymarket event URL or slug; prompts interactively if omitted",
    )
    ap.add_argument("--search", help="text search across active markets")
    args = ap.parse_args()

    if args.search:
        by_search(args.search)
        return

    if not args.target:
        print("Open https://polymarket.com/esports/cs2/games")
        print("Click the match and copy the URL from the address bar.\n")
        target = input("Paste the Polymarket URL: ").strip()
        if not target:
            sys.exit("Empty input.")
    else:
        target = args.target

    slug = _from_url_or_slug(target)
    by_slug(slug)


if __name__ == "__main__":
    main()
