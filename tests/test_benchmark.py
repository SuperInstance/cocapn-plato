"""Benchmark QueryEngine performance with large datasets."""
import json
import tempfile
import time
from pathlib import Path
import pytest

from cocapn_plato.engine.query import QueryEngine


@pytest.fixture
def big_engine():
    """Seed 10K tiles for stress testing."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = QueryEngine(tmp)
        path = Path(tmp) / "tiles.jsonl"
        
        domains = ["harbor", "forge", "archives", "tide-pool", "engine-room", "barracks", "ouroboros"]
        agents = ["ccc", "oracle1", "fm", "jc1", "fleet-bot", "scout-1", "scout-2"]
        
        with open(path, "w") as f:
            for i in range(10000):
                tile = {
                    "agent": agents[i % len(agents)],
                    "domain": domains[i % len(domains)],
                    "question": f"Question {i}: What is {domains[i % len(domains)]}?",
                    "answer": f"Answer {i}: It is a place for {agents[i % len(agents)]}.",
                    "timestamp": 1000 + i,
                    "confidence": 0.5 + (i % 10) / 20.0,
                }
                f.write(json.dumps(tile) + "\n")
        
        yield engine


def test_benchmark_equality_filter(big_engine):
    """Benchmark: equality filter on 10K tiles."""
    start = time.time()
    result = big_engine.query("tiles", where={"domain": "harbor"})
    elapsed = time.time() - start
    
    assert result["total"] == 1429  # 10000 / 7 ≈ 1429
    assert elapsed < 1.0
    print(f"\n  equality filter: {elapsed*1000:.1f}ms")


def test_benchmark_full_text(big_engine):
    """Benchmark: full-text search on 10K tiles."""
    start = time.time()
    result = big_engine.query("tiles", q="forge")
    elapsed = time.time() - start
    
    assert result["total"] > 0
    assert elapsed < 1.0
    print(f"\n  full-text search: {elapsed*1000:.1f}ms")


def test_benchmark_sort(big_engine):
    """Benchmark: sort 10K tiles."""
    start = time.time()
    result = big_engine.query("tiles", sort=[("timestamp", "desc")], limit=20)
    elapsed = time.time() - start
    
    assert len(result["results"]) == 20
    assert result["results"][0]["timestamp"] == 10999
    assert elapsed < 1.0
    print(f"\n  sort: {elapsed*1000:.1f}ms")


def test_benchmark_aggregate(big_engine):
    """Benchmark: aggregate 10K tiles."""
    start = time.time()
    result = big_engine.aggregate("tiles", group_by="domain", metrics=["count", "avg:timestamp"])
    elapsed = time.time() - start
    
    assert len(result) == 7
    assert elapsed < 1.0
    print(f"\n  aggregate: {elapsed*1000:.1f}ms")


def test_benchmark_complex_query(big_engine):
    """Benchmark: complex query with filter + sort + pagination."""
    start = time.time()
    result = big_engine.query(
        "tiles",
        where={"or": [{"domain": "harbor"}, {"domain": "forge"}]},
        sort=[("timestamp", "desc")],
        limit=50,
        offset=100,
    )
    elapsed = time.time() - start
    
    assert len(result["results"]) == 50
    assert 2850 <= result["total"] <= 2860  # ~2/7 of 10K
    assert elapsed < 1.0
    print(f"\n  complex query: {elapsed*1000:.1f}ms")


def test_query_latency_threshold(big_engine):
    """Ensure simple queries complete within 500ms on 10K tiles."""
    start = time.time()
    result = big_engine.query("tiles", where={"agent": "ccc"})
    elapsed = time.time() - start
    
    assert result["total"] == 1429
    assert len(result["results"]) == 50  # default limit
    assert elapsed < 0.5
    print(f"\n  latency threshold: {elapsed*1000:.1f}ms")


def test_pagination_latency(big_engine):
    """Measure offset performance (expected linear)."""
    for offset in [0, 1000, 5000, 9000]:
        start = time.time()
        result = big_engine.query("tiles", limit=10, offset=offset)
        elapsed = time.time() - start
        print(f"\n  offset={offset}: {elapsed*1000:.1f}ms")
        assert len(result["results"]) <= 10
        assert elapsed < 1.0
