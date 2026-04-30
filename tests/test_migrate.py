"""Tests for the tile migration pipeline."""
import pytest
from cocapn_plato.engine.migrate import (
    normalize, normalize_all, dedup_exact, dedup_fuzzy,
    score_tile, pipeline, _tile_signature
)


def test_normalize_basic():
    raw = {
        "question": "What is PLATO?",
        "answer": "A knowledge system",
        "domain": "fleet",
        "agent": "ccc",
        "timestamp": 1714406400,
        "confidence": 0.9,
    }
    t = normalize(raw)
    assert t["question"] == "What is PLATO?"
    assert t["domain"] == "fleet"
    assert t["agent"] == "ccc"
    assert t["timestamp"] == 1714406400
    assert t["confidence"] == 0.9


def test_normalize_variant_fields():
    raw = {
        "title": "How to MUD",
        "body": "Use curl",
        "room": "harbor",
        "creator": "oracle1",
        "created_at": "2024-04-30T00:00:00Z",
    }
    t = normalize(raw)
    assert t["question"] == "How to MUD"
    assert t["answer"] == "Use curl"
    assert t["domain"] == "harbor"
    assert t["agent"] == "oracle1"
    assert isinstance(t["timestamp"], float)


def test_normalize_unrecoverable():
    raw = {"id": 123, "meta": "only"}
    assert normalize(raw) is None


def test_normalize_millis_timestamp():
    raw = {"question": "Q", "answer": "A", "ts": 1714406400000}
    t = normalize(raw)
    assert t["timestamp"] == 1714406400.0


def test_dedup_exact():
    tiles = [
        {"question": "Same", "answer": "Same answer", "domain": "test"},
        {"question": "Same", "answer": "Same answer", "domain": "test"},
        {"question": "Different", "answer": "Other", "domain": "test"},
    ]
    unique = dedup_exact(tiles)
    assert len(unique) == 2


def test_dedup_fuzzy():
    tiles = [
        {"question": "How to deploy?", "answer": "Use docker", "domain": "test"},
        {"question": "How to deploy", "answer": "Use docker", "domain": "test"},
        {"question": "Totally different", "answer": "Another thing", "domain": "test"},
    ]
    unique = dedup_fuzzy(tiles, threshold=0.85)
    assert len(unique) == 2


def test_score_tile():
    tile = {
        "question": "What is the best way?",
        "answer": "Use Python 3.12 with asyncio. The key is to set up a TaskGroup.",
        "domain": "engine",
        "agent": "ccc",
        "confidence": 0.95,
    }
    scored = score_tile(tile)
    assert "_quality" in scored
    assert 0 <= scored["_quality"] <= 1
    assert scored["_completeness"] == 4
    assert scored["_quality"] > 0.6  # Should be high quality


def test_score_tile_poor_quality():
    tile = {
        "question": "",
        "answer": "ok",
        "domain": "general",
        "agent": "unknown",
        "confidence": 0.1,
    }
    scored = score_tile(tile)
    assert scored["_quality"] < 0.4  # Should be low quality


def test_pipeline():
    raw = [
        {"question": "Q1", "answer": "A1", "domain": "fleet", "agent": "ccc"},
        {"question": "Q1", "answer": "A1", "domain": "fleet", "agent": "ccc"},  # dup
        {"title": "Q2", "body": "A2", "room": "harbor", "creator": "oracle1"},
        {"id": 99, "meta": "bad"},  # unrecoverable
    ]
    result = pipeline(raw, fuzzy=False)
    stats = result["stats"]
    
    assert stats["raw_count"] == 4
    assert stats["normalized_count"] == 3
    assert stats["unique_count"] == 2
    assert stats["dups_removed"] == 1
    assert stats["unrecoverable"] == 1
    assert stats["avg_quality"] > 0
    
    # Should be sorted by quality
    tiles = result["tiles"]
    if len(tiles) >= 2:
        assert tiles[0]["_quality"] >= tiles[-1]["_quality"]
