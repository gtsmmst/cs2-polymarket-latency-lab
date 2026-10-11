"""
Latency dashboard + manual event marker.

Panels:
 - EVENT MARKERS  (press a key when you see an event on the stream -> timestamp is stored)
 - LIVE TAIL      (last events from every source)
 - GAP TABLE      (event -> nearest Polymarket tick, per source/kind)
 - TICK RATE      (how often the Polymarket book moves, per minute)

Hotkeys:
  K - first kill of the round
  B - bomb planted
  D - defuse / defuse attempt
  R - round end
  P - pistol round end
  W - round won by the favourite
  L - round won by the underdog
  E / space - generic event

Run:
    python -m ui.dashboard
    http://localhost:8765
"""
import bisect
import json
import statistics
import sys
import time
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
import uvicorn

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from watchers._db import DB_PATH, POLY_PRICE_KINDS, connect  # noqa: E402

app = FastAPI(title="CS2 Latency Lab")

GAME_SOURCES = ("manual", "gotv", "panda")
NOISE_KINDS = ("tick", "match_meta")


def _conn():
    return connect(DB_PATH)


HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>CS2 Latency Lab</title>
<style>
 body{background:#0d1117;color:#e6edf3;font-family:ui-monospace,Menlo,Consolas,monospace;margin:0;padding:14px}
 h1{font-size:15px;margin:0 0 6px;color:#7ee787}
 h3{margin:0 0 6px;color:#e6edf3;font-size:13px}
 .row{display:grid;grid-template-columns:1.3fr 1fr;gap:14px;margin-top:10px}
 .card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:10px;max-height:78vh;overflow:auto}
 table{width:100%;border-collapse:collapse;font-size:11.5px}
 th,td{padding:3px 6px;border-bottom:1px solid #21262d;text-align:left;white-space:nowrap}
 th{color:#8b949e;font-weight:600;position:sticky;top:0;background:#161b22}
 .panda{color:#79c0ff}.poly{color:#f0883e}.bo3{color:#a371f7}.manual{color:#7ee787;font-weight:600}.gotv{color:#d2a8ff;font-weight:600}
 .small{color:#8b949e;font-size:11px}
 .meta{color:#8b949e;font-size:12px;margin-bottom:6px}
 .keys{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-top:8px}
 .key{background:#21262d;border:1px solid #30363d;border-radius:6px;padding:8px;text-align:center;cursor:pointer;user-select:none;transition:0.1s}
 .key:hover{background:#30363d}
 .key:active,.key.flash{background:#1f6feb;border-color:#58a6ff;transform:scale(0.97)}
 .key kbd{display:block;font-size:18px;font-weight:700;color:#7ee787;margin-bottom:2px}
 .key span{font-size:11px;color:#8b949e}
 .toast{position:fixed;right:18px;bottom:18px;background:#1f6feb;color:#fff;padding:8px 12px;border-radius:6px;
        font-size:13px;opacity:0;transition:opacity 0.3s;pointer-events:none}
 .toast.show{opacity:1}
 .ticker{font-family:ui-monospace,monospace;font-size:11px;line-height:1.4;color:#e6edf3;height:130px;overflow:hidden;
         border:1px dashed #30363d;padding:6px;border-radius:6px;background:#0d1117}
 a{color:#7ee787}
</style></head><body>

<h1>CS2 latency lab</h1>
<div class="meta" id="meta">...</div>

<div class="card" style="margin-top:8px">
 <h3>Event markers (press a key or click)</h3>
 <div class="small">When you see an event on the stream, press the key. The timestamp is stored in the bus and the gap table below shows how long until Polymarket prices moved.</div>
 <div class="keys" id="keys"></div>
 <h3 style="margin-top:10px">Latest Polymarket ticks (live)</h3>
 <div class="ticker" id="ticker"></div>
</div>

<div class="row">
  <div class="card">
    <h3>Live tail — last 150 events</h3>
    <table id="tail">
      <thead><tr><th>t (local)</th><th>src</th><th>kind</th><th>match/asset</th><th>payload</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>
  <div class="card">
    <h3>Gap: event → nearest Polymarket tick</h3>
    <div class="small" style="margin-bottom:4px">sources: <span class="gotv">gotv</span> = CSTV+ parser, <span class="panda">panda</span> = PandaScore watcher, <span class="manual">manual</span> = your hotkey press while watching the stream</div>
    <table id="gaps">
      <thead><tr><th>src</th><th>kind</th><th>N</th><th>median ms</th><th>p95 ms</th><th>min ms</th></tr></thead>
      <tbody></tbody>
    </table>
    <div class="small" style="margin-top:8px">
     <b>How to read it (automated sources: gotv / panda):</b><br>
     &bull; median &lt; 0 → the market moved <i>before</i> the feed delivered the event → the feed is slower than whatever moves the market.<br>
     &bull; median around 0 → parity.<br>
     &bull; median clearly &gt; 0 → the feed delivered the event before the market moved.<br>
     <br><b>manual → poly</b> only counts forward gaps and shows how far behind the stream is (always positive, usually tens of seconds).<br>
     <a href="/api/export.csv">download all events as CSV</a>
    </div>
    <h3 style="margin-top:14px">Polymarket activity (ticks per minute)</h3>
    <table id="rate">
      <thead><tr><th>minute</th><th>price_change</th><th>best_bid_ask</th><th>last_trade</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>
</div>

<div class="toast" id="toast">recorded</div>

<script>
const KEYS = [
  ['k', 'first_kill',     'First kill of the round'],
  ['b', 'bomb_planted',   'Bomb planted'],
  ['d', 'defuse',         'Defuse / attempt'],
  ['r', 'round_end',      'Round end'],
  ['p', 'pistol_end',     'Pistol round end'],
  ['w', 'round_won_fav',  'Round won by favourite'],
  ['l', 'round_won_und',  'Round won by underdog'],
  ['e', 'event',          'Any event'],
];

const keysEl = document.getElementById('keys');
KEYS.forEach(([k, kind, label]) => {
  const d = document.createElement('div');
  d.className = 'key';
  d.dataset.kind = kind;
  d.dataset.key = k;
  d.innerHTML = `<kbd>${k.toUpperCase()}</kbd><span>${label}</span>`;
  d.onclick = () => mark(kind, k);
  keysEl.appendChild(d);
});

function flash(k){
  const el = document.querySelector(`.key[data-key="${k}"]`);
  if (el){ el.classList.add('flash'); setTimeout(() => el.classList.remove('flash'), 250); }
}

let toastTimer;
function toast(msg){
  const t = document.getElementById('toast');
  t.textContent = msg; t.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove('show'), 1200);
}

async function mark(kind, k){
  flash(k);
  const r = await fetch('/api/mark', {method:'POST', headers:{'Content-Type':'application/json'},
                                       body: JSON.stringify({kind})});
  const j = await r.json();
  toast(`${kind} @ ${new Date(j.ts).toISOString().substr(11,12)}`);
}

document.addEventListener('keydown', (e) => {
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
  const k = e.key.toLowerCase();
  const found = KEYS.find(x => x[0] === k);
  if (found){ e.preventDefault(); mark(found[1], k); return; }
  if (e.key === ' '){ e.preventDefault(); mark('event','e'); }
});

function esc(s){
  return String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

async function refresh(){
  const [tail, gaps, rate] = await Promise.all([
    fetch('/api/tail').then(r=>r.json()),
    fetch('/api/gaps').then(r=>r.json()),
    fetch('/api/rate').then(r=>r.json()),
  ]);

  document.getElementById('meta').textContent =
    `events: ${tail.total} | gotv: ${tail.n_gotv} | panda: ${tail.n_panda} | poly: ${tail.n_poly} | manual: ${tail.n_manual} | bo3: ${tail.n_bo3}`;

  const tb = document.querySelector('#tail tbody'); tb.innerHTML='';
  for (const ev of tail.rows){
    const tr = document.createElement('tr');
    const ts = new Date(ev.received_ts_ms).toISOString().substr(11,12);
    tr.innerHTML = `<td>${ts}</td>
      <td class="${esc(ev.source)}">${esc(ev.source)}</td>
      <td>${esc(ev.kind)}</td>
      <td class="small">${esc(ev.match_id||'')} ${ev.asset_id? esc(ev.asset_id.slice(0,8))+'…':''}</td>
      <td class="small">${esc((ev.payload||'').slice(0,140))}</td>`;
    tb.appendChild(tr);
  }

  const gb = document.querySelector('#gaps tbody'); gb.innerHTML='';
  for (const g of gaps){
    const tr = document.createElement('tr');
    tr.innerHTML = `<td class="${esc(g.source)}">${esc(g.source)}</td><td>${esc(g.kind)}</td><td>${g.n}</td><td>${g.median_ms}</td><td>${g.p95_ms}</td><td>${g.min_ms}</td>`;
    gb.appendChild(tr);
  }

  const rb = document.querySelector('#rate tbody'); rb.innerHTML='';
  for (const r of rate){
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${esc(r.minute)}</td><td>${r.price_change}</td><td>${r.best_bid_ask}</td><td>${r.last_trade}</td>`;
    rb.appendChild(tr);
  }

  // ticker: the most recent Polymarket ticks, one per line
  const tk = document.getElementById('ticker');
  const polyTicks = tail.rows.filter(r => r.source==='poly').slice(0,12);
  tk.innerHTML = polyTicks.map(r => {
    const ts = new Date(r.received_ts_ms).toISOString().substr(11,12);
    let p; try { p = JSON.parse(r.payload); } catch(e){ p={}; }
    const side = r.asset_id ? r.asset_id.slice(0,6) : '';
    const price = p.best_bid || p.price || p.best_ask || p.bestBid || '';
    return `<span style="color:#8b949e">${ts}</span> <span class="poly">${esc(r.kind)}</span> ${esc(side)}… ${esc(price)}`;
  }).join('<br>');
}
refresh(); setInterval(refresh, 1000);
</script>
</body></html>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    return HTML


@app.post("/api/mark")
async def mark(req: Request):
    body = await req.json()
    kind = (body.get("kind") or "event").lower()
    now_ms = int(time.time() * 1000)
    con = _conn()
    con.execute(
        "INSERT INTO events(received_ts_ms, source, kind, payload) VALUES (?,?,?,?)",
        (now_ms, "manual", f"manual:{kind}", json.dumps({"kind": kind})),
    )
    con.commit()
    con.close()
    return {"ts": now_ms, "kind": kind}


@app.get("/api/tail")
def tail(limit: int = 150):
    con = _conn()
    rows = con.execute(
        "SELECT received_ts_ms, source, kind, match_id, asset_id, payload "
        "FROM events ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    counts = dict(
        con.execute("SELECT source, COUNT(*) c FROM events GROUP BY source").fetchall()
    )
    total = sum(counts.values())
    con.close()
    return JSONResponse({
        "rows": [dict(r) for r in rows],
        "total": total,
        "n_poly": counts.get("poly", 0),
        "n_bo3": counts.get("bo3", 0),
        "n_manual": counts.get("manual", 0),
        "n_gotv": counts.get("gotv", 0),
        "n_panda": counts.get("panda", 0),
    })


@app.get("/api/gaps")
def gaps(window_ms: int = 120_000):
    """Gap from a game event (manual / gotv / panda) to the NEAREST Polymarket price tick.

    For automated sources (gotv, panda) negative deltas are allowed: a negative median
    means the market moved before the feed delivered the event. Manual hotkey presses
    only count forward gaps (a human reacting to a delayed stream is always late).
    """
    con = _conn()
    src_events = con.execute(
        f"SELECT received_ts_ms, source, kind FROM events "
        f"WHERE source IN ({','.join('?' * len(GAME_SOURCES))}) "
        f"  AND kind NOT IN ({','.join('?' * len(NOISE_KINDS))}) "
        f"ORDER BY received_ts_ms",
        (*GAME_SOURCES, *NOISE_KINDS),
    ).fetchall()
    poly = con.execute(
        f"SELECT received_ts_ms FROM events WHERE source='poly' "
        f"AND kind IN ({','.join('?' * len(POLY_PRICE_KINDS))}) "
        f"ORDER BY received_ts_ms",
        POLY_PRICE_KINDS,
    ).fetchall()
    con.close()

    poly_ts = [r["received_ts_ms"] for r in poly]
    grouped = {}  # (source, kind) -> list[delta_ms]

    for e in src_events:
        m_ts = e["received_ts_ms"]
        src = e["source"]
        kind = e["kind"]

        # Nearest tick may be before or after the event.
        i = bisect.bisect_left(poly_ts, m_ts)
        candidates = []
        if i < len(poly_ts):
            candidates.append(poly_ts[i] - m_ts)  # next (>= 0)
        if i > 0:
            candidates.append(poly_ts[i - 1] - m_ts)  # previous (<= 0)
        if not candidates:
            continue
        delta = min(candidates, key=abs)
        if abs(delta) > window_ms:
            continue
        if src == "manual" and delta < 0:
            continue
        grouped.setdefault((src, kind), []).append(delta)

    out = []
    for (src, kind), vals in sorted(grouped.items(), key=lambda kv: (kv[0][0], -len(kv[1]))):
        if not vals:
            continue
        med = int(statistics.median(vals))
        p95 = int(sorted(vals)[max(0, int(len(vals) * 0.95) - 1)])
        out.append({
            "source": src,
            "kind": kind,
            "n": len(vals),
            "median_ms": med,
            "p95_ms": p95,
            "min_ms": min(vals),
        })
    return JSONResponse(out)


@app.get("/api/rate")
def rate(minutes: int = 10):
    """Polymarket ticks per minute by kind over the last N minutes."""
    con = _conn()
    now = int(time.time() * 1000)
    since = now - minutes * 60_000
    rows = con.execute(
        "SELECT received_ts_ms, kind FROM events "
        "WHERE source='poly' AND received_ts_ms >= ? "
        "ORDER BY received_ts_ms DESC",
        (since,),
    ).fetchall()
    con.close()
    buckets = {}
    for r in rows:
        m = (now - r["received_ts_ms"]) // 60_000
        b = buckets.setdefault(m, {"price_change": 0, "best_bid_ask": 0, "last_trade": 0})
        if r["kind"] == "price_change":
            b["price_change"] += 1
        elif r["kind"] == "best_bid_ask":
            b["best_bid_ask"] += 1
        elif r["kind"] == "last_trade_price":
            b["last_trade"] += 1
    out = []
    for m in sorted(buckets.keys()):
        b = buckets[m]
        out.append({"minute": f"-{m} min", **b})
    return JSONResponse(out)


@app.get("/api/export.csv")
def export_csv():
    con = _conn()
    rows = con.execute(
        "SELECT received_ts_ms, source_ts_ms, source, kind, match_id, "
        "asset_id, market_id, round_num, payload FROM events ORDER BY id"
    ).fetchall()
    con.close()
    header = "received_ts_ms,source_ts_ms,source,kind,match_id,asset_id,market_id,round_num,payload\n"
    body_lines = []
    for r in rows:
        payload = (r["payload"] or "").replace("\n", " ").replace('"', '""')
        body_lines.append(
            f'{r["received_ts_ms"]},{r["source_ts_ms"] or ""},{r["source"]},'
            f'{r["kind"]},{r["match_id"] or ""},{r["asset_id"] or ""},'
            f'{r["market_id"] or ""},{r["round_num"] or ""},"{payload}"'
        )
    return Response(
        content=header + "\n".join(body_lines),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=events.csv"},
    )


def main():
    print("\nOpen in a browser: http://localhost:8765\n")
    uvicorn.run(app, host="127.0.0.1", port=8765, log_level="info")


if __name__ == "__main__":
    main()
