"""Tile migration pipeline: normalize, deduplicate, score, reindex.

Handles the full journey from raw PLATO v2 tiles to query-ready records.
Maximum capability in minimum lines.
"""
import json
import re
from typing import List, Dict, Any, Optional, Set
from collections import Counter
from difflib import SequenceMatcher


def normalize(tile: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize a tile from any old format to standard format.
    
    Returns None if tile is completely unrecoverable.
    """
    # Extract fields with fallbacks for every known variant
    agent = (
        tile.get("agent")
        or tile.get("creator")
        or tile.get("source")
        or tile.get("author")
        or tile.get("user")
        or "unknown"
    )
    
    domain = (
        tile.get("domain")
        or tile.get("room")
        or tile.get("category")
        or tile.get("tag")
        or tile.get("channel")
        or "general"
    )
    
    question = (
        tile.get("question")
        or tile.get("title")
        or tile.get("prompt")
        or tile.get("query")
        or tile.get("input")
        or ""
    )
    
    answer = (
        tile.get("answer")
        or tile.get("body")
        or tile.get("content")
        or tile.get("response")
        or tile.get("output")
        or tile.get("text")
        or ""
    )
    
    # If no question or answer, try to construct from raw fields
    if not question and not answer:
        # Maybe it's a plain text tile
        for key in ["message", "data", "value", "string", "raw"]:
            if key in tile and isinstance(tile[key], str):
                answer = tile[key]
                break
    
    # Still nothing? Try dumping non-metadata keys (need at least 2 to avoid single metadata fields)
    if not question and not answer:
        meta_keys = {"id", "_id", "timestamp", "created_at", "updated_at", 
                     "agent", "creator", "source", "domain", "room", "category",
                     "type", "format", "version", "meta", "schema"}
        remaining = {k: v for k, v in tile.items() if k not in meta_keys and isinstance(v, (str, int, float, bool, list, dict))}
        if remaining and len(remaining) >= 2:
            answer = json.dumps(remaining, ensure_ascii=False)
    
    if not question and not answer:
        return None  # Unrecoverable
    
    timestamp = (
        tile.get("timestamp")
        or tile.get("created_at")
        or tile.get("ts")
        or tile.get("time")
    )
    
    # Normalize timestamp to float epoch
    if isinstance(timestamp, str):
        try:
            from datetime import datetime
            timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
        except:
            try:
                timestamp = float(timestamp)
            except:
                timestamp = None
    elif isinstance(timestamp, int):
        # Could be milliseconds or seconds
        timestamp = timestamp / 1000.0 if timestamp > 1e10 else float(timestamp)
    
    confidence = tile.get("confidence", 0.5)
    if isinstance(confidence, str):
        try:
            confidence = float(confidence)
        except:
            confidence = 0.5
    
    provenance = tile.get("provenance", {})
    if isinstance(provenance, str):
        try:
            provenance = json.loads(provenance)
        except:
            provenance = {}
    
    return {
        "agent": str(agent).strip() or "unknown",
        "domain": str(domain).strip().lower() or "general",
        "question": str(question).strip(),
        "answer": str(answer).strip(),
        "timestamp": timestamp,
        "confidence": max(0.0, min(1.0, confidence)),
        "provenance": provenance if isinstance(provenance, dict) else {},
    }


def normalize_all(tiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize a batch of tiles, skipping unrecoverable ones."""
    results = []
    for tile in tiles:
        normalized = normalize(tile)
        if normalized:
            results.append(normalized)
    return results


def _tile_signature(tile: Dict[str, Any]) -> str:
    """Create a deduplication signature from a tile."""
    q = re.sub(r"\s+", " ", (tile.get("question", "") or "").lower().strip())[:80]
    a = re.sub(r"\s+", " ", (tile.get("answer", "") or "").lower().strip())[:120]
    return f"{q}::{a}"


def dedup_exact(tiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove exact duplicates by signature."""
    seen: Set[str] = set()
    unique = []
    for tile in tiles:
        sig = _tile_signature(tile)
        if sig not in seen:
            seen.add(sig)
            unique.append(tile)
    return unique


def dedup_fuzzy(tiles: List[Dict[str, Any]], threshold: float = 0.92) -> List[Dict[str, Any]]:
    """Remove near-duplicate tiles using fuzzy string matching.
    
    O(n²) — use only on small batches or sampled data.
    """
    if len(tiles) > 5000:
        # For large datasets, use exact dedup only
        return dedup_exact(tiles)
    
    unique = []
    for tile in tiles:
        sig = _tile_signature(tile)
        is_dup = False
        for existing in unique:
            existing_sig = _tile_signature(existing)
            ratio = SequenceMatcher(None, sig, existing_sig).ratio()
            if ratio >= threshold:
                is_dup = True
                # Keep the one with higher confidence / more complete
                if _score_completeness(tile) > _score_completeness(existing):
                    unique[unique.index(existing)] = tile
                break
        if not is_dup:
            unique.append(tile)
    return unique


def _score_completeness(tile: Dict[str, Any]) -> int:
    """Score how complete a tile is (0-4)."""
    score = 0
    if tile.get("question"): score += 1
    if tile.get("answer"): score += 1
    if tile.get("domain") and tile["domain"] != "general": score += 1
    if tile.get("agent") and tile["agent"] != "unknown": score += 1
    return score


def score_tile(tile: Dict[str, Any]) -> Dict[str, Any]:
    """Score a single tile for quality.
    
    Returns tile with added `_quality` field.
    """
    q = tile.get("question", "")
    a = tile.get("answer", "")
    
    completeness = _score_completeness(tile)
    
    # Length score: ideal answer is 50-500 chars
    a_len = len(a)
    if a_len == 0:
        length_score = 0
    elif a_len < 20:
        length_score = 0.3
    elif a_len < 50:
        length_score = 0.6
    elif a_len <= 500:
        length_score = 1.0
    elif a_len <= 1000:
        length_score = 0.8
    else:
        length_score = 0.5
    
    # Specificity: does it contain concrete terms?
    specificity = 0.5
    if any(c.isdigit() for c in a):
        specificity += 0.15
    if len(set(a.split())) > 10:
        specificity += 0.15
    if "http" in a or "." in a:
        specificity += 0.1
    if "{" in a or "[" in a:
        specificity += 0.1
    
    # Confidence integration
    confidence = tile.get("confidence", 0.5)
    
    # Overall quality (0-1)
    quality = (
        completeness / 4 * 0.35 +
        length_score * 0.30 +
        min(specificity, 1.0) * 0.20 +
        confidence * 0.15
    )
    
    tile["_quality"] = round(quality, 3)
    tile["_completeness"] = completeness
    return tile


def score_all(tiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Score all tiles."""
    return [score_tile(tile.copy()) for tile in tiles]


def pipeline(raw_tiles: List[Dict[str, Any]], fuzzy: bool = False) -> Dict[str, Any]:
    """Run the full migration pipeline.
    
    Returns dict with stats and processed tiles.
    """
    step1 = normalize_all(raw_tiles)
    step2 = dedup_fuzzy(step1) if fuzzy else dedup_exact(step1)
    step3 = score_all(step2)
    
    # Sort by quality descending
    step3.sort(key=lambda t: t.get("_quality", 0), reverse=True)
    
    # Stats
    domains = Counter(t["domain"] for t in step3)
    agents = Counter(t["agent"] for t in step3)
    quality_dist = Counter(round(t.get("_quality", 0) * 10) / 10 for t in step3)
    
    return {
        "stats": {
            "raw_count": len(raw_tiles),
            "normalized_count": len(step1),
            "unique_count": len(step2),
            "dups_removed": len(step1) - len(step2),
            "unrecoverable": len(raw_tiles) - len(step1),
            "top_domains": domains.most_common(10),
            "top_agents": agents.most_common(10),
            "quality_distribution": dict(sorted(quality_dist.items())),
            "avg_quality": round(sum(t.get("_quality", 0) for t in step3) / len(step3), 3) if step3 else 0,
        },
        "tiles": step3,
    }
