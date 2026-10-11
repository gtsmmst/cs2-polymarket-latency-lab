"""HTTP sidecar that receives events from the Go GOTV/CSTV+ parser (``watchers/gotv``).

Listens on 127.0.0.1:8766, accepts POST /api/ingest_gotv from the Go process and writes
each event into the SQLite bus through the shared EventLog with source='gotv'.

Run:
    python -m watchers.gotv_ingest

or from the project root:
    python watchers/gotv_ingest.py

Endpoint:
    POST /api/ingest_gotv
    JSON: { "kind": "kill", "match_id": "...", "tick": 12345, "payload": {...} }
"""
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional

# Support running as `python watchers/gotv_ingest.py` (without -m):
# put the project root on sys.path.
if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from watchers._db import EventLog  # noqa: E402

INGEST_HOST = "127.0.0.1"
INGEST_PORT = 8766


class IngestHandler(BaseHTTPRequestHandler):
    log: Optional[EventLog] = None  # set in main()

    def do_POST(self):
        if self.path != "/api/ingest_gotv":
            self.send_response(404)
            self.end_headers()
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length)
            data = json.loads(raw)
        except Exception as e:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(f"bad json: {e}".encode())
            return

        kind = data.get("kind", "unknown")
        match_id = data.get("match_id") or None
        payload = data.get("payload") or {}
        tick = data.get("tick")
        round_num = payload.get("round_num")

        # received_ts_ms is stamped by EventLog from the local clock —
        # that is the reference point for comparison against Polymarket ticks.
        self.log.write(
            source="gotv",
            kind=kind,
            payload={**payload, "ingame_tick": tick},
            match_id=match_id,
            round_num=round_num,
        )

        # Echo to the console, except for heartbeat ticks (there are many).
        if kind != "tick":
            print(f"[gotv] {kind} match={match_id} payload={payload}")

        self.send_response(204)
        self.end_headers()

    def log_message(self, format, *args):
        # Silence the default noisy http.server access log.
        pass


def main():
    log = EventLog()
    IngestHandler.log = log
    server = ThreadingHTTPServer((INGEST_HOST, INGEST_PORT), IngestHandler)
    print(f"GOTV ingest sidecar listening on http://{INGEST_HOST}:{INGEST_PORT}/api/ingest_gotv")
    print("Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping...")
    finally:
        log.close()


if __name__ == "__main__":
    main()
