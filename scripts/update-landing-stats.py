#!/usr/bin/env python3
"""Landing page stat updater — replace stale hardcoded numbers with live PLATO data.

Usage:
    python update-landing-stats.py /path/to/index.html --output updated.html
    python update-landing-stats.py /path/to/oracle1-workspace/data/ --batch

Replaces patterns like:
    "18 services, 3,000+ knowledge tiles, 57 rooms"
    "24 services UP"
    "7,970+ knowledge tiles"

With live data from PLATO.
"""
import argparse
import json
import re
import urllib.request
from pathlib import Path
from typing import Dict, Any

PLATO_URL = "http://147.224.38.131:8847"


def fetch_stats() -> Dict[str, Any]:
    """Fetch live fleet stats from PLATO and other services."""
    stats = {"services_up": 0, "services_total": 0, "tiles": 0, "rooms": 0, "mud_rooms": 0}
    
    # PLATO tiles
    try:
        req = urllib.request.Request(f"{PLATO_URL}/export/plato-tile-spec", headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        if isinstance(data, list):
            stats["tiles"] = len(data)
        elif isinstance(data, dict):
            for key in ["tiles", "data", "records", "total"]:
                if key in data:
                    stats["tiles"] = len(data[key]) if isinstance(data[key], list) else data[key]
                    break
    except Exception as e:
        pass
    
    # Fallback: query /rooms for count
    try:
        req = urllib.request.Request(f"{PLATO_URL}/rooms", headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        if isinstance(data, list):
            stats["rooms"] = len(data)
        elif isinstance(data, dict):
            stats["rooms"] = data.get("rooms", data.get("total", 0))
    except Exception:
        pass
    
    # Service health checks
    services = [
        ("MUD", 4042, "/status"),
        ("Lock", 4043, "/"),
        ("Arena", 4044, "/stats"),
        ("Grammar", 4045, "/grammar"),
        ("Dashboard", 4046, "/"),
        ("Nexus", 4047, "/"),
        ("Compactor", 4055, "/status"),
        ("RateAttn", 4056, "/streams"),
        ("SkillForge", 4057, "/status"),
        ("Harbor", 4050, "/"),
        ("Terminal", 4060, "/"),
        ("PLATO", 8847, "/rooms"),
        ("Shell", 8848, "/"),
        ("Matrix", 6168, "/status"),
        ("Conduit", 6167, "/"),
        ("Guard", 8899, "/"),
        ("Queue", 8900, "/"),
        ("Steward", 8901, "/"),
    ]
    
    up = 0
    for name, port, path in services:
        try:
            url = f"http://147.224.38.131:{port}{path}"
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=3) as resp:
                resp.read(1)
                up += 1
        except Exception:
            pass
    
    stats["services_up"] = up
    stats["services_total"] = len(services)
    
    # MUD rooms
    try:
        req = urllib.request.Request("http://147.224.38.131:4042/status", headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        stats["mud_rooms"] = data.get("rooms", 0)
    except Exception:
        pass
    
    return stats


def format_number(n: int) -> str:
    """Format a number with commas."""
    return f"{n:,}"


def update_html(html: str, stats: Dict[str, Any]) -> str:
    """Replace stale stats in HTML with live data."""
    tiles = stats["tiles"]
    rooms = stats["rooms"]
    mud = stats["mud_rooms"]
    up = stats["services_up"]
    total = stats["services_total"]
    total_rooms = rooms + mud
    
    # Pattern replacements
    replacements = [
        # Services
        (r'\d+\s*services', f"{up} services"),
        (r'\d+/\d+\s*services', f"{up}/{total} services"),
        (r'\d+\s*service', f"{up} service"),  # singular fallback
        
        # Tiles with various formats
        (r'[\d,]+\+?\s*knowledge\s*tiles', f"{format_number(tiles)} knowledge tiles"),
        (r'[\d,]+\+?\s*tiles', f"{format_number(tiles)} tiles"),
        (r'[\d,]+\+?\s*knowledge\s*fragments', f"{format_number(tiles)} knowledge fragments"),
        
        # Rooms
        (r'[\d,]+\+?\s*MUD\s*rooms', f"{format_number(mud)} MUD rooms"),
        (r'[\d,]+\+?\s*rooms', f"{format_number(total_rooms)} rooms"),
        (r'[\d,]+\+?\s+rooms', f"{format_number(total_rooms)} rooms"),
        
        # Combined patterns
        (rf'{up}\s*services,\s*[\d,]+\+?\s*knowledge\s*tiles,\s*[\d,]+\+?\s*rooms',
         f"{up} services, {format_number(tiles)} knowledge tiles, {format_number(total_rooms)} rooms"),
    ]
    
    updated = html
    for pattern, replacement in replacements:
        updated = re.sub(pattern, replacement, updated, flags=re.IGNORECASE)
    
    return updated


def update_meta_tags(html: str, stats: Dict[str, Any]) -> str:
    """Update meta description tags with fresh stats."""
    tiles = stats["tiles"]
    rooms = stats["rooms"]
    mud = stats["mud_rooms"]
    up = stats["services_up"]
    total = stats["services_total"]
    total_rooms = rooms + mud
    
    # Common meta description pattern
    desc = f"The Cocapn Fleet is live. {up}/{total} services, {format_number(tiles)} knowledge tiles, {format_number(total_rooms)} rooms."
    
    # Replace content="..." in meta description and og:description
    html = re.sub(
        r'(<meta\s+name=["\']description["\']\s+content=["\'])(.*?)(["\'])',
        lambda m: f'{m.group(1)}{desc}{m.group(3)}',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    html = re.sub(
        r'(<meta\s+property=["\']og:description["\']\s+content=["\'])(.*?)(["\'])',
        lambda m: f'{m.group(1)}{desc}{m.group(3)}',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    
    return html


def process_file(input_path: str, output_path: str = None):
    """Process a single HTML file."""
    stats = fetch_stats()
    print(f"Live stats: {stats}")
    
    with open(input_path) as f:
        html = f.read()
    
    updated = update_html(html, stats)
    updated = update_meta_tags(updated, stats)
    
    out = output_path or input_path
    with open(out, "w") as f:
        f.write(updated)
    
    print(f"Updated {out}")
    return stats


def batch_process(directory: str):
    """Process all HTML files in a directory."""
    stats = fetch_stats()
    print(f"Live stats: {stats}\n")
    
    path = Path(directory)
    files = list(path.glob("*.html"))
    
    if not files:
        print(f"No HTML files found in {directory}")
        return
    
    for f in files:
        with open(f) as fh:
            html = fh.read()
        updated = update_html(html, stats)
        updated = update_meta_tags(updated, stats)
        with open(f, "w") as fh:
            fh.write(updated)
        print(f"  Updated {f.name}")
    
    print(f"\nProcessed {len(files)} files")


def main():
    parser = argparse.ArgumentParser(prog="update-landing-stats", description="Update landing page stats from live PLATO data")
    parser.add_argument("input", help="Input HTML file or directory")
    parser.add_argument("--output", help="Output file (default: overwrite input)")
    parser.add_argument("--batch", action="store_true", help="Process all HTML files in directory")
    args = parser.parse_args()
    
    if args.batch:
        batch_process(args.input)
    else:
        process_file(args.input, args.output)


if __name__ == "__main__":
    main()
