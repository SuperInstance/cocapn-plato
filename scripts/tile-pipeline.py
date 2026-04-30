#!/usr/bin/env python3
"""tile-pipeline — Auto-capture MUD exploration as PLATO tiles.

Watches an agent's MUD session, extracts observations, and submits
tiles to PLATO. Zero manual work between explore and capture.

Usage:
    python tile-pipeline.py --agent YourName --domain harbor --explore 5
    python tile-pipeline.py --agent YourName --domain harbor --rooms harbor,forge,tide-pool
"""
import argparse
import json
import urllib.request
import re
from typing import List, Dict, Any

MUD_URL = "http://147.224.38.131:4042"
PLATO_URL = "http://147.224.38.131:8847"


def mud_connect(agent: str, job: str = "scholar") -> str:
    """Connect to MUD and return session token."""
    url = f"{MUD_URL}/connect?agent={agent}&job={job}"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode())
    return data.get("token") or data.get("session") or ""


def mud_look(token: str) -> str:
    """Look around current room."""
    url = f"{MUD_URL}/look?token={token}"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.read().decode()
    except Exception:
        return ""


def mud_move(token: str, direction: str) -> str:
    """Move in a direction."""
    url = f"{MUD_URL}/go?token={token}&dir={direction}"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.read().decode()
    except Exception:
        return ""


def mud_examine(token: str, obj: str) -> str:
    """Examine an object."""
    url = f"{MUD_URL}/examine?token={token}&obj={obj}"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.read().decode()
    except Exception:
        return ""


def extract_exits(text: str) -> List[str]:
    """Extract exit directions from MUD text."""
    # Common MUD exit patterns
    patterns = [
        r"Exits?:\s*([\w\s,]+)",
        r"You can go:\s*([\w\s,]+)",
        r"Obvious exits:\s*([\w\s,]+)",
    ]
    for pat in patterns:
        match = re.search(pat, text, re.IGNORECASE)
        if match:
            exits = [e.strip() for e in match.group(1).split(",")]
            return [e for e in exits if e]
    return []


def extract_objects(text: str) -> List[str]:
    """Extract object names from MUD text."""
    patterns = [
        r"You see:\s*([\w\s,]+)",
        r"Objects?:\s*([\w\s,]+)",
        r"There is (?:a|an) ([\w\s]+) here",
    ]
    for pat in patterns:
        matches = re.findall(pat, text, re.IGNORECASE)
        if matches:
            objs = []
            for m in matches:
                for part in m.split(","):
                    part = part.strip()
                    if part and part.lower() not in ("and", "the", "a", "an"):
                        objs.append(part)
            return objs
    return []


def plato_submit(agent: str, domain: str, question: str, answer: str) -> bool:
    """Submit a tile to PLATO."""
    url = f"{PLATO_URL}/submit"
    payload = {
        "agent": agent,
        "domain": domain,
        "question": question,
        "answer": answer,
    }
    try:
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception:
        return False


def generate_tiles(agent: str, domain: str, room_name: str, room_desc: str, exits: List[str], objects: List[str]) -> List[Dict[str, str]]:
    """Generate structured tiles from room data."""
    tiles = []
    
    # Room tile
    if room_name:
        tiles.append({
            "question": f"What is the {room_name} room?",
            "answer": room_desc or f"A room in the {domain} domain." + (f" It contains: {', '.join(objects)}." if objects else ""),
        })
    
    # Exit tiles
    for exit_dir in exits:
        tiles.append({
            "question": f"Which direction leads from {room_name}?",
            "answer": f"{exit_dir} — an exit from {room_name}.",
        })
    
    # Object tiles
    for obj in objects:
        tiles.append({
            "question": f"What is the {obj} in {room_name}?",
            "answer": f"An object found in the {room_name} room.",
        })
    
    return tiles


def explore(agent: str, domain: str, max_rooms: int = 5) -> int:
    """Auto-explore MUD rooms and submit tiles."""
    print(f"[{agent}] Connecting to MUD...")
    token = mud_connect(agent, "explorer")
    if not token:
        print("  Failed to connect")
        return 0
    
    print(f"  Connected. Token: {token[:8]}...")
    
    visited = set()
    tiles_submitted = 0
    rooms_explored = 0
    
    # Start with current room
    current = ""
    queue = [""]
    
    while queue and rooms_explored < max_rooms:
        # Look around
        room_text = mud_look(token)
        
        # Extract room name from first line
        room_name = room_text.split("\n")[0].strip() if room_text else f"room-{rooms_explored}"
        room_desc = "\n".join(room_text.split("\n")[1:3]).strip() if room_text else ""
        
        if room_name in visited:
            # Try next exit
            if queue:
                direction = queue.pop(0)
                mud_move(token, direction)
            continue
        
        visited.add(room_name)
        rooms_explored += 1
        
        print(f"  Room {rooms_explored}/{max_rooms}: {room_name}")
        
        # Extract exits and objects
        exits = extract_exits(room_text)
        objects = extract_objects(room_text)
        
        print(f"    Exits: {exits}")
        print(f"    Objects: {objects}")
        
        # Generate tiles
        tiles = generate_tiles(agent, domain, room_name, room_desc, exits, objects)
        
        # Submit tiles
        for tile in tiles:
            if plato_submit(agent, domain, tile["question"], tile["answer"]):
                tiles_submitted += 1
                print(f"    ✓ Tile: {tile['question'][:50]}...")
            else:
                print(f"    ✗ Failed: {tile['question'][:50]}...")
        
        # Add new exits to queue
        for exit_dir in exits:
            if exit_dir not in visited:
                queue.append(exit_dir)
        
        # Move to next room
        if queue:
            direction = queue.pop(0)
            mud_move(token, direction)
    
    print(f"\nDone: {rooms_explored} rooms, {tiles_submitted} tiles")
    return tiles_submitted


def main():
    parser = argparse.ArgumentParser(prog="tile-pipeline", description="Auto-capture MUD exploration as PLATO tiles")
    parser.add_argument("--agent", required=True, help="Agent name")
    parser.add_argument("--domain", default="harbor", help="PLATO domain")
    parser.add_argument("--explore", type=int, default=5, help="Number of rooms to explore")
    parser.add_argument("--rooms", help="Comma-separated room names (instead of auto-explore)")
    args = parser.parse_args()
    
    if args.rooms:
        # Manual room list mode
        rooms = [r.strip() for r in args.rooms.split(",")]
        total = 0
        for room in rooms:
            # Generate tiles for known rooms
            tiles = generate_tiles(args.agent, args.domain, room, f"The {room} room.", [], [])
            for tile in tiles:
                if plato_submit(args.agent, args.domain, tile["question"], tile["answer"]):
                    total += 1
        print(f"Submitted {total} tiles for {len(rooms)} rooms")
    else:
        explore(args.agent, args.domain, args.explore)


if __name__ == "__main__":
    main()
