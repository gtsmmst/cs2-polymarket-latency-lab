"""
Turn a match URL (HLTV or bo3.gg) into the identifiers the watchers use.

- HLTV URLs yield a numeric match id. There is no HLTV watcher in this repository
  (the scorebot endpoint is behind Cloudflare and was dropped); the id is only useful
  as a label, e.g. for the Go parser's ``-match`` flag so GOTV events carry a match_id.
- bo3.gg URLs yield the slug that ``watchers.bo3_poller --match-slug`` expects.

Usage:

    python -m scripts.find_match "https://www.hltv.org/matches/2348837/team-a-vs-team-b-event"
    python -m scripts.find_match "https://bo3.gg/matches/team-a-vs-team-b-event"
    python -m scripts.find_match            # prompts for the URL
"""
import re
import sys

HLTV_RX = re.compile(r"hltv\.org/matches/(\d+)/([a-z0-9\-]+)", re.I)
BO3_RX = re.compile(r"bo3\.gg/matches/([a-z0-9\-]+)", re.I)


def parse(url: str):
    url = url.strip().strip('"').strip("'")
    m = HLTV_RX.search(url)
    if m:
        return ("hltv", m.group(1), m.group(2))
    m = BO3_RX.search(url)
    if m:
        return ("bo3", None, m.group(1))
    return None


def main():
    if len(sys.argv) >= 2:
        url = " ".join(sys.argv[1:])
    else:
        print("Open one of these in a browser:")
        print("  https://www.hltv.org/matches      (live + upcoming)")
        print("  https://bo3.gg/matches/current    (live)")
        print("Click the match and copy the URL from the address bar.\n")
        url = input("Paste the URL and press Enter: ").strip()

    if not url:
        sys.exit("Empty input.")

    parsed = parse(url)
    if not parsed:
        sys.exit(
            "Does not look like an HLTV or bo3.gg match link. Expected something like:\n"
            "  https://www.hltv.org/matches/2348837/team-a-vs-team-b-event\n"
            "  https://bo3.gg/matches/team-a-vs-team-b-event"
        )

    source, match_id, slug = parsed
    print()
    print("=" * 60)
    if source == "hltv":
        print(f"  matchId : {match_id}")
        print(f"  slug    : {slug}")
        print()
        print("Use it as a label for GOTV events:")
        print(f"  go run gotv_watcher.go -url <broadcast-url> -match {match_id}")
    else:
        print(f"  bo3 slug: {slug}")
        print()
        print("Run the bo3.gg poller:")
        print(f"  python -m watchers.bo3_poller --match-slug {slug}")
    print("=" * 60)


if __name__ == "__main__":
    main()
