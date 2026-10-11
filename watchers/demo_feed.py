"""
Synthetic CS2 match-state generator for UI experiments (no network, stdlib only).

When no live matches are on (between tournaments, at night), this generator produces
two fake matches that evolve plausibly: rounds tick by, sides swap at round 12, bombs
get planted, odds drift with the score.

The output shape mirrors the bo3.gg live-match JSON (the structure ``bo3_poller``
snapshots), so a UI built against the real feed renders the fake one identically.

Status: this module is a standalone fixture generator. Nothing in this repository
imports it; the match-list UI it originally fed was not carried over. Call
``get_live_matches_fake()`` repeatedly to advance the simulation.
"""

import random
import time
from typing import Any, Dict

# ---------------------------------------------------------------------------
# Two fake matches to make the dashboard look populated
# ---------------------------------------------------------------------------
MAPS = ["de_inferno", "de_mirage", "de_ancient", "de_anubis", "de_nuke", "de_dust2"]
PHASES = ["BUY_TIME", "IN_PROGRESS", "IN_PROGRESS", "IN_PROGRESS", "FINISHED"]

_FAKE_STATE = {
    "matches": [
        {
            "id": 999001,
            "slug": "demo-navi-vs-faze",
            "t1_name": "Natus Vincere", "t1_img": "https://cdn.bo3.gg/uploads/team/144/image_navi.png",
            "t2_name": "FaZe Clan",     "t2_img": "https://cdn.bo3.gg/uploads/team/2/image_faze.png",
            "t1_id": 144, "t2_id": 2,
            "tournament": "IEM Cologne Major 2026 [DEMO]",
            "tier": 1, "prize": "$1,250,000",
            "streams": ["ESL_CSGO", "ESL_CSGO_b"],
            "bo_type": 3,
            "map_score": [1, 1],
            "map_idx": 0,
            "round": 1,
            "phase_idx": 0,
            "t1_score": 0, "t2_score": 0,
            "t1_side": "CT", "t2_side": "TERRORIST",
            "t1_eq": 800, "t2_eq": 800,
            "odds_t1": 1.65, "odds_t2": 2.25,
            "bomb_planted": False,
            "last_tick": time.time(),
        },
        {
            "id": 999002,
            "slug": "demo-vitality-vs-spirit",
            "t1_name": "Team Vitality", "t1_img": "https://cdn.bo3.gg/uploads/team/9/image_vit.png",
            "t2_name": "Team Spirit",   "t2_img": "https://cdn.bo3.gg/uploads/team/57/image_spirit.png",
            "t1_id": 9, "t2_id": 57,
            "tournament": "BLAST Premier Spring [DEMO]",
            "tier": 1, "prize": "$425,000",
            "streams": ["BLASTPremier"],
            "bo_type": 3,
            "map_score": [0, 0],
            "map_idx": 1,
            "round": 12,
            "phase_idx": 1,
            "t1_score": 8, "t2_score": 4,
            "t1_side": "TERRORIST", "t2_side": "CT",
            "t1_eq": 24500, "t2_eq": 5200,
            "odds_t1": 1.42, "odds_t2": 2.85,
            "bomb_planted": True,
            "last_tick": time.time(),
        },
    ],
    "tick_count": 0,
}


def _advance_match(m: Dict[str, Any], dt: float) -> None:
    """Move one fake match forward by `dt` seconds of game-time."""
    # Phase progresses every ~4 ticks
    m["phase_idx"] = (m["phase_idx"] + (1 if random.random() < 0.25 else 0)) % len(PHASES)

    # Bomb plant flip-flops in IN_PROGRESS
    if PHASES[m["phase_idx"]] == "IN_PROGRESS" and random.random() < 0.08:
        m["bomb_planted"] = not m["bomb_planted"]

    # Equipment value drifts
    m["t1_eq"] = max(800, min(28000, m["t1_eq"] + random.randint(-2000, 3000)))
    m["t2_eq"] = max(800, min(28000, m["t2_eq"] + random.randint(-2000, 3000)))

    # Odds drift slightly with score
    diff = m["t1_score"] - m["t2_score"]
    m["odds_t1"] = round(max(1.05, 1.85 - 0.07 * diff + random.uniform(-0.05, 0.05)), 2)
    m["odds_t2"] = round(max(1.05, 1.85 + 0.07 * diff + random.uniform(-0.05, 0.05)), 2)

    # End of round → 30% chance per tick to award a round
    if PHASES[m["phase_idx"]] == "FINISHED" and random.random() < 0.6:
        winner = 1 if random.random() < (0.5 + 0.02 * diff) else 2
        if winner == 1:
            m["t1_score"] += 1
        else:
            m["t2_score"] += 1
        m["round"] += 1
        m["bomb_planted"] = False

        # Side switch at round 12
        if m["round"] == 13:
            m["t1_side"], m["t2_side"] = m["t2_side"], m["t1_side"]

        # Map ended (someone reached 13)
        if m["t1_score"] >= 13 or m["t2_score"] >= 13:
            if m["t1_score"] >= 13:
                m["map_score"][0] += 1
            else:
                m["map_score"][1] += 1
            m["map_idx"] = (m["map_idx"] + 1) % len(MAPS)
            m["round"] = 1
            m["t1_score"] = 0
            m["t2_score"] = 0
            m["t1_side"], m["t2_side"] = "CT", "TERRORIST"


def _build_live_updates(m: Dict[str, Any]) -> Dict[str, Any]:
    """Build the live_updates dict in BO3.gg's exact format."""
    return {
        "map_name": MAPS[m["map_idx"]],
        "round_number": m["round"],
        "round_phase": PHASES[m["phase_idx"]],
        "game_number": 1,
        "game_ended": False,
        "team_1": {
            "side": m["t1_side"],
            "game_score": m["t1_score"],
            "equipment_value": m["t1_eq"],
            "economy_level": 5,
        },
        "team_2": {
            "side": m["t2_side"],
            "game_score": m["t2_score"],
            "equipment_value": m["t2_eq"],
            "economy_level": 5,
        },
    }


def _build_bet_updates(m: Dict[str, Any]) -> Dict[str, Any]:
    """Build bet_updates with a few realistic markets."""
    return {
        "team_1": {"coeff": m["odds_t1"]},
        "team_2": {"coeff": m["odds_t2"]},
        "markets_count": 42,
        "additional_markets": [
            {"bet_type": "team_1_handicap_1.5", "team_id": m["t1_id"], "coeff": round(m["odds_t1"] * 1.6, 2), "aggrement_score": "+1.5"},
            {"bet_type": "team_2_handicap_1.5", "team_id": m["t2_id"], "coeff": round(m["odds_t2"] * 1.6, 2), "aggrement_score": "+1.5"},
            {"bet_type": "team_1_handicap_-1.5", "team_id": m["t1_id"], "coeff": round(m["odds_t1"] * 1.9, 2), "aggrement_score": "-1.5"},
            {"bet_type": "team_2_handicap_-1.5", "team_id": m["t2_id"], "coeff": round(m["odds_t2"] * 1.9, 2), "aggrement_score": "-1.5"},
            {"bet_type": "total_maps_2.5", "team_id": None, "coeff": 1.55, "aggrement_score": "over"},
            {"bet_type": "total_maps_2.5_under", "team_id": None, "coeff": 2.40, "aggrement_score": "under"},
        ],
    }


def get_live_matches_fake() -> Dict[str, Any]:
    """Return a bo3.gg-shaped ``{"results": [...]}`` dict and advance the fake matches one step."""
    now = time.time()
    _FAKE_STATE["tick_count"] += 1

    results = []
    for m in _FAKE_STATE["matches"]:
        dt = now - m["last_tick"]
        _advance_match(m, dt)
        m["last_tick"] = now

        results.append({
            "id": m["id"],
            "slug": m["slug"],
            "status": "in_progress",
            "parsed_status": "live",
            "bo_type": m["bo_type"],
            "team1_score": m["map_score"][0],
            "team2_score": m["map_score"][1],
            "stars": 5,
            "live_coverage": True,
            "team1": {"id": m["t1_id"], "name": m["t1_name"], "image_url": m["t1_img"]},
            "team2": {"id": m["t2_id"], "name": m["t2_name"], "image_url": m["t2_img"]},
            "tournament": {"name": m["tournament"], "tier_rank": m["tier"], "prize": m["prize"]},
            "streams": [{"name": s} for s in m["streams"]],
            "live_updates": _build_live_updates(m),
            "bet_updates": _build_bet_updates(m),
        })

    return {"results": results, "_demo": True, "_tick": _FAKE_STATE["tick_count"]}
