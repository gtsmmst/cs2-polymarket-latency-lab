"""
Live events feed — a separate page to watch the stream of CS2 match events
(kill / bomb_planted / defuse / round_end) as they land in the bus.

Shows:
  - Right: a scrolling feed with colour-coded event types, fresh events highlighted
  - Left: active matches (from PandaScore match_meta / map_score), source filters,
    and a compact "gap to Polymarket" table

Data sources:
  - source='panda', kind='ws:*'      (PandaScore live socket, paid plan only)
  - source='panda', kind='map_score' (PandaScore polling, map-level)
  - source='gotv',  kind='*'         (CSTV+ parser, round-level)
  - source='manual'                  (dashboard hotkeys)

Run:
    python -m ui.live_events
    http://localhost:8767
"""
import bisect
import json
import statistics
import sys
import time
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from watchers._db import DB_PATH, POLY_PRICE_KINDS, connect  # noqa: E402

app = FastAPI(title="CS2 Live Events Stream")


def _conn():
    return connect(DB_PATH)


# ---- Event kind -> colour, label, importance ----
EVENT_STYLE = {
    # PandaScore live socket (paid plan)
    "ws:kill":           {"emoji": "K",  "color": "#f85149", "label": "KILL",          "weight": 1},
    "ws:first_kill":     {"emoji": "K1", "color": "#ff6b6b", "label": "FIRST KILL",    "weight": 3},
    "ws:bomb_planted":   {"emoji": "B",  "color": "#ffa657", "label": "BOMB PLANTED",  "weight": 5},
    "ws:bomb_defused":   {"emoji": "D",  "color": "#56d364", "label": "BOMB DEFUSED",  "weight": 4},
    "ws:bomb_exploded":  {"emoji": "X",  "color": "#ff7b72", "label": "BOMB EXPLODED", "weight": 4},
    "ws:round_start":    {"emoji": ">",  "color": "#8b949e", "label": "ROUND START",   "weight": 1},
    "ws:round_end":      {"emoji": "R",  "color": "#79c0ff", "label": "ROUND END",     "weight": 3},
    "ws:freezetime_end": {"emoji": "F",  "color": "#a5d6ff", "label": "FREEZE END",    "weight": 2},
    "ws:match_start":    {"emoji": "M",  "color": "#d2a8ff", "label": "MATCH START",   "weight": 5},
    "ws:match_end":      {"emoji": "M",  "color": "#d2a8ff", "label": "MATCH END",     "weight": 5},
    "ws:map_start":      {"emoji": "MP", "color": "#d2a8ff", "label": "MAP START",     "weight": 4},
    "ws:map_end":        {"emoji": "MP", "color": "#a371f7", "label": "MAP END",       "weight": 5},
    "ws:pause":          {"emoji": "||", "color": "#8b949e", "label": "PAUSE",         "weight": 2},
    "ws:resume":         {"emoji": ">",  "color": "#8b949e", "label": "RESUME",        "weight": 2},
    # PandaScore polling (map-level)
    "map_score":         {"emoji": "S",  "color": "#7ee787", "label": "MAP SCORE",     "weight": 4},
    "match_meta":        {"emoji": "i",  "color": "#8b949e", "label": "MATCH META",    "weight": 1},
    # GOTV (Go parser)
    "kill":              {"emoji": "K",  "color": "#f85149", "label": "KILL (GOTV)",   "weight": 1},
    "bomb_planted":      {"emoji": "B",  "color": "#ffa657", "label": "BOMB (GOTV)",   "weight": 5},
    "bomb_defused":      {"emoji": "D",  "color": "#56d364", "label": "DEFUSED (GOTV)", "weight": 4},
    "bomb_exploded":     {"emoji": "X",  "color": "#ff7b72", "label": "EXPLODED (GOTV)", "weight": 4},
    "round_end":         {"emoji": "R",  "color": "#79c0ff", "label": "ROUND (GOTV)",  "weight": 3},
    "match_start":       {"emoji": "M",  "color": "#d2a8ff", "label": "MATCH START (GOTV)", "weight": 5},
    # Manual hotkeys
    "manual:first_kill":  {"emoji": "K1", "color": "#ff6b6b", "label": "1st KILL (manual)", "weight": 3},
    "manual:bomb_planted": {"emoji": "B", "color": "#ffa657", "label": "BOMB (manual)",     "weight": 5},
    "manual:round_end":   {"emoji": "R",  "color": "#79c0ff", "label": "ROUND (manual)",    "weight": 3},
    # Polymarket reactions
    "price_change":      {"emoji": "$",  "color": "#f0883e", "label": "POLY PRICE",    "weight": 2},
    "best_bid_ask":      {"emoji": "$",  "color": "#f0883e", "label": "POLY BBO",      "weight": 1},
    "last_trade_price":  {"emoji": "$",  "color": "#f0883e", "label": "POLY TRADE",    "weight": 3},
}

SOURCE_FALLBACK = {
    "panda":  {"emoji": "?", "color": "#ffd166", "weight": 2},
    "poly":   {"emoji": "?", "color": "#f0883e", "weight": 1},
    "gotv":   {"emoji": "?", "color": "#d2a8ff", "weight": 2},
    "bo3":    {"emoji": "?", "color": "#a371f7", "weight": 1},
    "manual": {"emoji": "?", "color": "#7ee787", "weight": 2},
}


def style_for(source: str, kind: str) -> dict:
    """Resolve the visual style for an event."""
    if kind in EVENT_STYLE:
        return EVENT_STYLE[kind]
    base = SOURCE_FALLBACK.get(source, {"emoji": "?", "color": "#8b949e", "weight": 1})
    return {**base, "label": kind.upper()}


HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>CS2 Live Events Stream</title>
<style>
 * { box-sizing: border-box; }
 body{background:#0d1117;color:#e6edf3;font-family:ui-monospace,Menlo,Consolas,monospace;margin:0;padding:14px;height:100vh;overflow:hidden}
 h1{font-size:16px;margin:0 0 10px;color:#7ee787;display:flex;align-items:center;gap:10px}
 h1 .pulse{width:12px;height:12px;border-radius:50%;background:#7ee787;animation:pulse 1.5s infinite}
 @keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
 .grid{display:grid;grid-template-columns:320px 1fr;gap:14px;height:calc(100vh - 60px)}
 .panel{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px;overflow:auto}
 .panel h3{margin:0 0 10px;color:#8b949e;font-size:12px;text-transform:uppercase;letter-spacing:1px}

 .match{margin-bottom:14px;padding:10px;background:#0d1117;border:1px solid #21262d;border-radius:6px}
 .match-name{font-size:13px;color:#e6edf3;font-weight:600;margin-bottom:6px}
 .match-score{font-size:24px;font-weight:700;text-align:center;margin:6px 0}
 .match-meta{font-size:11px;color:#8b949e;margin-top:4px}
 .badge{display:inline-block;padding:2px 6px;background:#21262d;border-radius:4px;font-size:10px;margin-right:4px}
 .badge.live{background:#da3633;color:#fff;font-weight:600;animation:pulse 1s infinite}
 .badge.tier-s{background:#a371f7;color:#fff}

 .feed{display:flex;flex-direction:column;gap:4px;padding:4px;height:100%;overflow-y:auto}
 .ev{display:grid;grid-template-columns:90px 38px 1fr 100px;gap:8px;padding:6px 8px;border-radius:4px;background:#0d1117;border-left:3px solid #30363d;font-size:12px;align-items:center;animation:slideIn 0.3s ease-out}
 @keyframes slideIn{from{opacity:0;transform:translateX(-6px)}to{opacity:1;transform:translateX(0)}}
 .ev.fresh{box-shadow:0 0 0 1px #7ee787;background:#1f6feb22}
 .ev.weight-3{background:#161b22}
 .ev.weight-4{background:#1f6feb22}
 .ev.weight-5{background:#da363322;font-weight:600}
 .ev .ts{color:#8b949e;font-size:11px}
 .ev .em{font-size:13px;text-align:center;font-weight:700}
 .ev .label{font-weight:600}
 .ev .meta{color:#8b949e;font-size:11px;text-align:right}

 .filters{display:flex;gap:6px;margin-bottom:8px;flex-wrap:wrap}
 .chip{padding:4px 10px;background:#21262d;border:1px solid #30363d;border-radius:14px;font-size:11px;cursor:pointer;user-select:none}
 .chip.on{background:#1f6feb;border-color:#58a6ff;color:#fff}

 a{color:#7ee787}
</style></head><body>

<h1><span class="pulse"></span>CS2 Live Events Stream <span style="color:#8b949e;font-size:12px;margin-left:auto;font-weight:400" id="lastUpdate">...</span></h1>

<div class="grid">
  <div class="panel">
    <h3>Active matches</h3>
    <div id="matches">loading...</div>

    <h3 style="margin-top:18px">Event filters</h3>
    <div class="filters" id="filters">
      <div class="chip on" data-src="panda">panda</div>
      <div class="chip on" data-src="gotv">gotv</div>
      <div class="chip on" data-src="poly">poly</div>
      <div class="chip on" data-src="manual">manual</div>
      <div class="chip on" data-src="bo3">bo3</div>
    </div>

    <h3 style="margin-top:18px">Latency (event → nearest poly tick)</h3>
    <div id="gaps" class="small" style="font-size:11px;color:#8b949e">loading...</div>

    <h3 style="margin-top:18px">What this is</h3>
    <div style="font-size:11px;color:#8b949e;line-height:1.5">
      Live feed of CS2 match events from the SQLite bus.<br>
      &bull; <span style="color:#79c0ff">panda</span> = PandaScore (polling, or live socket on a paid plan)<br>
      &bull; <span style="color:#d2a8ff">gotv</span> = CSTV+ parser<br>
      &bull; <span style="color:#f0883e">poly</span> = Polymarket reaction<br>
      &bull; <span style="color:#7ee787">manual</span> = your hotkey press<br>
    </div>
  </div>

  <div class="panel" style="padding:6px">
    <div class="feed" id="feed"></div>
  </div>
</div>

<script>
let activeFilters = new Set(['panda','gotv','poly','manual','bo3']);
document.querySelectorAll('.chip').forEach(c => {
  c.onclick = () => {
    c.classList.toggle('on');
    const src = c.dataset.src;
    if (activeFilters.has(src)) activeFilters.delete(src); else activeFilters.add(src);
    render();
  };
});

let cached = {events: [], matches: [], gaps: []};

function fmtTs(ms) {
  const d = new Date(ms);
  return d.toISOString().substr(11, 12);
}
function escapeHtml(s){
  return String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

async function tick(){
  try{
    const [events, matches, gaps] = await Promise.all([
      fetch('/api/events?limit=200').then(r=>r.json()),
      fetch('/api/matches').then(r=>r.json()),
      fetch('/api/gaps').then(r=>r.json()).catch(()=>[]),
    ]);
    cached = {events, matches, gaps};
    document.getElementById('lastUpdate').textContent = 'updated ' + new Date().toISOString().substr(11,8);
    render();
  } catch(e){
    document.getElementById('lastUpdate').textContent = 'error: ' + e.message;
  }
}

function render(){
  const mEl = document.getElementById('matches');
  if (!cached.matches.length) {
    mEl.innerHTML = '<div style="color:#8b949e;font-size:11px">no active matches. Start <code>watchers.panda_cs2_watcher</code>.</div>';
  } else {
    mEl.innerHTML = cached.matches.map(m => `
      <div class="match">
        <div class="match-name">${escapeHtml(m.name || '?')}</div>
        <div class="match-score">${escapeHtml(m.team_a||'?')} <span style="color:#8b949e">${m.maps_a ?? '-'}</span> : <span style="color:#8b949e">${m.maps_b ?? '-'}</span> ${escapeHtml(m.team_b||'?')}</div>
        <div class="match-meta">
          <span class="badge live">LIVE</span>
          ${m.tier === 's' ? '<span class="badge tier-s">TIER S</span>' : ''}
          ${m.low_latency ? '<span class="badge" style="background:#7ee787;color:#000">LL feed</span>' : ''}
          <span class="badge">${escapeHtml(m.tournament || m.league || '')}</span>
        </div>
        <div class="match-meta" style="margin-top:6px;color:#8b949e">
          id ${escapeHtml(m.match_id)} · updated ${fmtTs(m.updated_ts_ms)}
        </div>
      </div>
    `).join('');
  }

  const feedEl = document.getElementById('feed');
  const nowMs = Date.now();
  const visible = cached.events.filter(e => activeFilters.has(e.source));
  feedEl.innerHTML = visible.slice(0, 150).map(e => {
    const s = e.style;
    const isFresh = (nowMs - e.received_ts_ms) < 8000;
    const weightCls = s.weight >= 3 ? `weight-${s.weight}` : '';
    let summary = '';
    try {
      const p = e.payload ? JSON.parse(e.payload) : {};
      if (e.kind === 'map_score') summary = `${p.team_a||''} ${p.maps_a}:${p.maps_b} ${p.team_b||''}`;
      else if (e.kind === 'match_meta') summary = (p.name || '') + ' · BO' + (p.bo || '?');
      else if (e.kind.startsWith('ws:')) summary = p.killer_name || p.player_name || p.team || p.message || JSON.stringify(p).slice(0,50);
      else if (e.source === 'poly') summary = (p.price !== undefined ? `price ${p.price}` : (p.best_bid ? `bid ${p.best_bid} ask ${p.best_ask||'?'}` : ''));
      else summary = JSON.stringify(p).slice(0, 70);
    } catch(_){ summary = String(e.payload||'').slice(0,60); }

    return `<div class="ev ${weightCls} ${isFresh?'fresh':''}" style="border-left-color:${s.color}">
      <div class="ts">${fmtTs(e.received_ts_ms)}</div>
      <div class="em">${escapeHtml(s.emoji)}</div>
      <div><span style="color:${s.color};font-weight:600">${escapeHtml(s.label)}</span> <span style="color:#8b949e;margin-left:6px">${escapeHtml(summary)}</span></div>
      <div class="meta">${escapeHtml(e.match_id || '')}</div>
    </div>`;
  }).join('');

  const gEl = document.getElementById('gaps');
  if (!cached.gaps.length) {
    gEl.innerHTML = '<div>nothing yet</div>';
  } else {
    gEl.innerHTML = cached.gaps.slice(0,6).map(g => `
      <div style="display:flex;justify-content:space-between;padding:3px 0;border-bottom:1px solid #21262d">
        <span><span style="color:${g.source==='gotv'?'#d2a8ff':'#79c0ff'}">${escapeHtml(g.source)}</span> ${escapeHtml(g.kind)}</span>
        <span style="color:${g.median_ms < 0 ? '#f85149' : '#7ee787'}">${g.median_ms} ms</span>
      </div>
    `).join('');
  }
}

tick();
setInterval(tick, 1000);
</script>
</body></html>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    return HTML


@app.get("/api/events")
def events(limit: int = 200, since_sec: int = 600):
    """Events from the last N seconds, newest first."""
    since_ms = int(time.time() * 1000) - since_sec * 1000
    con = _conn()
    rows = con.execute(
        """SELECT received_ts_ms, source, kind, match_id, payload
           FROM events WHERE received_ts_ms >= ?
           ORDER BY id DESC LIMIT ?""",
        (since_ms, limit),
    ).fetchall()
    con.close()
    out = []
    for r in rows:
        s = style_for(r["source"], r["kind"])
        out.append({
            "received_ts_ms": r["received_ts_ms"],
            "source": r["source"],
            "kind": r["kind"],
            "match_id": r["match_id"],
            "payload": r["payload"],
            "style": s,
        })
    return JSONResponse(out)


@app.get("/api/matches")
def matches():
    """Active matches: latest match_meta + latest map_score per match_id (last 30 min)."""
    con = _conn()
    since_ms = int(time.time() * 1000) - 30 * 60 * 1000
    meta_rows = con.execute(
        """SELECT match_id, MAX(received_ts_ms) AS ts, payload
           FROM events
           WHERE source='panda' AND kind='match_meta' AND received_ts_ms >= ?
           GROUP BY match_id""",
        (since_ms,),
    ).fetchall()
    score_rows = con.execute(
        """SELECT e.match_id, e.received_ts_ms AS ts, e.payload
           FROM events e
           JOIN (SELECT match_id, MAX(id) AS max_id
                 FROM events WHERE source='panda' AND kind='map_score'
                   AND received_ts_ms >= ? GROUP BY match_id) m
             ON e.id = m.max_id""",
        (since_ms,),
    ).fetchall()
    con.close()

    by_id = {}
    for r in meta_rows:
        try:
            p = json.loads(r["payload"])
        except Exception:
            p = {}
        by_id[r["match_id"]] = {
            "match_id": r["match_id"],
            "name": p.get("name"),
            "team_a": p.get("team_a"),
            "team_b": p.get("team_b"),
            "league": p.get("league"),
            "tournament": p.get("tournament"),
            "tier": p.get("tier"),
            "low_latency": p.get("low_latency_feed"),
            "updated_ts_ms": r["ts"],
            "maps_a": None,
            "maps_b": None,
        }
    for r in score_rows:
        try:
            p = json.loads(r["payload"])
        except Exception:
            p = {}
        d = by_id.setdefault(r["match_id"], {
            "match_id": r["match_id"], "name": None, "team_a": None, "team_b": None,
            "league": None, "tournament": None, "tier": None, "low_latency": None,
        })
        d["maps_a"] = p.get("maps_a")
        d["maps_b"] = p.get("maps_b")
        d["updated_ts_ms"] = max(d.get("updated_ts_ms", 0) or 0, r["ts"])
        if not d.get("team_a"):
            d["team_a"] = p.get("team_a")
        if not d.get("team_b"):
            d["team_b"] = p.get("team_b")
    out = sorted(by_id.values(), key=lambda x: -(x.get("updated_ts_ms") or 0))
    return JSONResponse(out)


@app.get("/api/gaps")
def gaps():
    """Compact gap stats (same nearest-tick logic as ui.dashboard)."""
    window_ms = 120_000
    con = _conn()
    src_events = con.execute(
        "SELECT received_ts_ms, source, kind FROM events "
        "WHERE source IN ('manual','panda','gotv') AND kind NOT IN ('match_meta','tick') "
        "ORDER BY received_ts_ms"
    ).fetchall()
    poly = con.execute(
        f"SELECT received_ts_ms FROM events WHERE source='poly' "
        f"AND kind IN ({','.join('?' * len(POLY_PRICE_KINDS))}) "
        f"ORDER BY received_ts_ms",
        POLY_PRICE_KINDS,
    ).fetchall()
    con.close()
    poly_ts = [r["received_ts_ms"] for r in poly]
    grouped = {}
    for e in src_events:
        m_ts = e["received_ts_ms"]
        i = bisect.bisect_left(poly_ts, m_ts)
        candidates = []
        if i < len(poly_ts):
            candidates.append(poly_ts[i] - m_ts)
        if i > 0:
            candidates.append(poly_ts[i - 1] - m_ts)
        if not candidates:
            continue
        delta = min(candidates, key=abs)
        if abs(delta) > window_ms:
            continue
        if e["source"] == "manual" and delta < 0:
            continue  # a hotkey press reacts to the stream; only forward gaps are meaningful
        grouped.setdefault((e["source"], e["kind"]), []).append(delta)
    out = []
    for (src, kind), vals in grouped.items():
        if not vals:
            continue
        med = int(statistics.median(vals))
        out.append({"source": src, "kind": kind, "n": len(vals), "median_ms": med})
    out.sort(key=lambda x: (-x["n"]))
    return JSONResponse(out)


def main():
    print("\nCS2 Live Events Stream — http://localhost:8767\n")
    uvicorn.run(app, host="127.0.0.1", port=8767, log_level="warning")


if __name__ == "__main__":
    main()
