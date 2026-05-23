#!/usr/bin/env python3
"""cocapn-plato/scripts/breeder_snapshot.py — Extend fleet snapshot with breeding metrics.

This is the 'how people feel' layer for PLATO. When Casey opens the
dashboard, he sees not just service health but the genetic health of
the fleet — diversity, thermal pressure, lifecycle stage.

Usage:
    python3 breeder_snapshot.py --ship sunset-ecosystem
    python3 breeder_snapshot.py --all-ships
    python3 breeder_snapshot.py --watch  # poll every 60s

Outputs: JSON tile ready for PLATO tile store.
"""

import argparse
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── Paths ────────────────────────────────────────────────────────
PLATO_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PLATO_DIR))

# ── Optional sunset imports ────────────────────────────────────
try:
    sys.path.insert(0, str(PLATO_DIR.parent / "sunset-ecosystem"))
    from swarm.breeder import BreederDaemonV2
    _HAS_BREEDER = True
except Exception:
    _HAS_BREEDER = False

# ── PLATO API ──────────────────────────────────────────────────
PLATO_URL = "http://147.224.38.131:8847"


def plato_submit_tile(tile: Dict[str, Any]) -> bool:
    """Submit a tile to the PLATO store."""
    try:
        data = json.dumps(tile).encode()
        req = urllib.request.Request(
            f"{PLATO_URL}/tiles",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"PLATO submit failed: {e}")
        return False


# ── Snapshot builders ──────────────────────────────────────────
def build_diversity_tile(diversity: float, threshold: float = 0.35) -> Dict[str, Any]:
    """Build a PLATO tile for diversity status."""
    level = "healthy"
    if diversity < 0.20:
        level = "critical"
    elif diversity < threshold:
        level = "warning"

    return {
        "tile_type": "breeder_diversity",
        "level": level,
        "diversity": round(diversity, 3),
        "threshold": threshold,
        "timestamp": time.time_ns(),
        "summary": f"Fleet genetic diversity is {level} ({diversity:.2f})",
    }


def build_thermal_tile(pressure: float) -> Dict[str, Any]:
    """Build a PLATO tile for thermal pressure."""
    level = "normal"
    if pressure >= 0.9:
        level = "critical"
    elif pressure >= 0.7:
        level = "elevated"

    return {
        "tile_type": "breeder_thermal",
        "level": level,
        "pressure": round(pressure, 3),
        "timestamp": time.time_ns(),
        "summary": f"Thermal pressure {level} ({pressure:.2f})",
    }


def build_lifecycle_tile(state: str, active: int) -> Dict[str, Any]:
    """Build a PLATO tile for lifecycle state."""
    return {
        "tile_type": "breeder_lifecycle",
        "state": state,
        "active_agents": active,
        "timestamp": time.time_ns(),
        "summary": f"Breeder in {state} with {active} active agents",
    }


def build_alert_tile(alert: Dict[str, Any]) -> Dict[str, Any]:
    """Build a PLATO tile from a diversity or thermal alert."""
    return {
        "tile_type": "breeder_alert",
        "level": alert.get("level", "warning"),
        "dimension": alert.get("dimension", "unknown"),
        "recommended_action": alert.get("recommended_action", ""),
        "timestamp": time.time_ns(),
        "summary": alert.get("recommended_action", "Alert triggered"),
    }


# ── Main snapshot ────────────────────────────────────────────────
def build_full_snapshot(
    diversity: float = 0.85,
    thermal: float = 0.3,
    state: str = "COMPETE",
    active: int = 12,
    alerts: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    """Build a complete breeder snapshot with all tiles."""
    tiles = [
        build_diversity_tile(diversity),
        build_thermal_tile(thermal),
        build_lifecycle_tile(state, active),
    ]

    for alert in (alerts or []):
        tiles.append(build_alert_tile(alert))

    return {
        "snapshot_type": "breeder",
        "ship": "sunset-ecosystem",
        "timestamp": time.time_ns(),
        "tiles": tiles,
        "overall_health": _overall_health(diversity, thermal, state),
    }


def _overall_health(diversity: float, thermal: float, state: str) -> str:
    if state in ("STALLED", "DEAD"):
        return "critical"
    if thermal >= 0.9 or diversity < 0.20:
        return "critical"
    if thermal >= 0.7 or diversity < 0.35:
        return "warning"
    return "healthy"


# ── CLI ─────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Breeder snapshot for PLATO")
    parser.add_argument("--diversity", type=float, default=0.85)
    parser.add_argument("--thermal", type=float, default=0.3)
    parser.add_argument("--state", default="COMPETE")
    parser.add_argument("--active", type=int, default=12)
    parser.add_argument("--submit", action="store_true", help="Submit to PLATO")
    parser.add_argument("--watch", action="store_true", help="Poll every 60s")
    args = parser.parse_args()

    snapshot = build_full_snapshot(
        diversity=args.diversity,
        thermal=args.thermal,
        state=args.state,
        active=args.active,
    )

    print(json.dumps(snapshot, indent=2))

    if args.submit:
        for tile in snapshot["tiles"]:
            ok = plato_submit_tile(tile)
            print(f"  submitted {tile['tile_type']}: {'OK' if ok else 'FAIL'}")

    if args.watch:
        print("\nWatching (Ctrl+C to stop)...")
        while True:
            time.sleep(60)
            snapshot = build_full_snapshot()
            print(f"[{datetime.now(timezone.utc).isoformat()}] health={snapshot['overall_health']}")
            if args.submit:
                for tile in snapshot["tiles"]:
                    plato_submit_tile(tile)


if __name__ == "__main__":
    main()
