#!/usr/bin/env python3
"""Migrate tiles from old PLATO (v2) to new cocapn-plato query format.

Pulls all tiles from old /export endpoint, normalizes them, writes to JSONL.
"""
import json
import urllib.request
from pathlib import Path
from datetime import datetime

OLD_PLATO = "http://147.224.38.131:8847"
DATA_DIR = Path("./fleet_data")


def fetch_tiles():
    """Fetch tiles from old PLATO export endpoint."""
    print(f"Fetching from {OLD_PLATO}/export/plato-tile-spec ...")
    req = urllib.request.Request(f"{OLD_PLATO}/export/plato-tile-spec", headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())
    
    # Extract tiles from various possible formats
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ["tiles", "data", "records", "items", "results"]:
            if key in data and isinstance(data[key], list):
                return data[key]
    return []


def normalize_tile(tile):
    """Normalize a tile from old format to new standard format."""
    # Old PLATO tiles may have different field names
    normalized = {
        "agent": tile.get("agent") or tile.get("creator") or tile.get("source", "unknown"),
        "domain": tile.get("domain") or tile.get("room") or tile.get("category", "general"),
        "question": tile.get("question") or tile.get("title") or tile.get("prompt", ""),
        "answer": tile.get("answer") or tile.get("body") or tile.get("content", ""),
        "timestamp": tile.get("timestamp") or tile.get("created_at") or datetime.now().timestamp(),
        "confidence": tile.get("confidence", 0.5),
        "provenance": tile.get("provenance", {}),
    }
    return normalized


def migrate():
    tiles = fetch_tiles()
    print(f"Fetched {len(tiles)} tiles")
    
    if not tiles:
        print("No tiles to migrate.")
        return
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / "tiles.jsonl"
    
    # Append normalized tiles
    with open(path, "a") as f:
        for tile in tiles:
            normalized = normalize_tile(tile)
            f.write(json.dumps(normalized) + "\n")
    
    print(f"Migrated {len(tiles)} tiles to {path}")
    
    # Quick stats
    domains = {}
    agents = {}
    for tile in tiles:
        d = tile.get("domain") or tile.get("room", "general")
        a = tile.get("agent") or tile.get("creator", "unknown")
        domains[d] = domains.get(d, 0) + 1
        agents[a] = agents.get(a, 0) + 1
    
    print(f"\nTop domains:")
    for d, c in sorted(domains.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {d}: {c}")
    
    print(f"\nTop agents:")
    for a, c in sorted(agents.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {a}: {c}")
    
    print(f"\nNext: start the server to query migrated tiles")
    print(f"  python -m cocapn_plato.server")


if __name__ == "__main__":
    migrate()
