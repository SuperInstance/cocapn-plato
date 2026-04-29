"""Tests for QueryEngine — maximum capability in minimum lines."""
import json
import tempfile
from pathlib import Path
import pytest

from cocapn_plato.engine.query import QueryEngine


@pytest.fixture
def engine():
    with tempfile.TemporaryDirectory() as tmp:
        yield QueryEngine(tmp)


@pytest.fixture
def sample_tiles(engine):
    """Seed test data."""
    tiles = [
        {"agent": "ccc", "domain": "harbor", "question": "What is the harbor?", "answer": "A coordination hub.", "timestamp": 1000},
        {"agent": "ccc", "domain": "forge", "question": "How to build?", "answer": "Use the anvil.", "timestamp": 2000},
        {"agent": "oracle1", "domain": "harbor", "question": "Fleet status?", "answer": "All green.", "timestamp": 1500},
        {"agent": "fm", "domain": "forge", "question": "CSS help?", "answer": "Flexbox.", "timestamp": 3000},
        {"agent": "ccc", "domain": "archives", "question": "Old logs", "answer": "From day one.", "timestamp": 500},
    ]
    path = Path(engine.dir) / "tiles.jsonl"
    with open(path, "w") as f:
        for t in tiles:
            f.write(json.dumps(t) + "\n")
    return engine


def test_equality_filter(sample_tiles):
    r = sample_tiles.query("tiles", where={"domain": "harbor"})
    assert len(r["results"]) == 2
    assert all(t["domain"] == "harbor" for t in r["results"])


def test_regex_filter(sample_tiles):
    r = sample_tiles.query("tiles", where={"question": {"op": "regex", "val": "^What"}})
    assert len(r["results"]) == 2


def test_contains_filter(sample_tiles):
    r = sample_tiles.query("tiles", where={"answer": {"op": "contains", "val": "hub"}})
    assert len(r["results"]) == 1
    assert r["results"][0]["domain"] == "harbor"


def test_or_filter(sample_tiles):
    r = sample_tiles.query("tiles", where={"or": [{"domain": "harbor"}, {"domain": "archives"}]})
    assert len(r["results"]) == 3


def test_sort_desc(sample_tiles):
    r = sample_tiles.query("tiles", sort=[("timestamp", "desc")])
    timestamps = [t["timestamp"] for t in r["results"]]
    assert timestamps == [3000, 2000, 1500, 1000, 500]


def test_pagination(sample_tiles):
    r = sample_tiles.query("tiles", limit=2, offset=1, sort=[("timestamp", "desc")])
    assert len(r["results"]) == 2
    assert r["results"][0]["timestamp"] == 2000
    assert r["total"] == 5


def test_full_text_search(sample_tiles):
    r = sample_tiles.query("tiles", q="coordination")
    assert len(r["results"]) == 1
    assert r["results"][0]["domain"] == "harbor"


def test_aggregate(sample_tiles):
    r = sample_tiles.aggregate("tiles", group_by="domain")
    assert len(r) == 3
    counts = {row["_key"]: row["count"] for row in r}
    assert counts["harbor"] == 2
    assert counts["forge"] == 2
    assert counts["archives"] == 1


def test_aggregate_with_metrics(sample_tiles):
    r = sample_tiles.aggregate("tiles", group_by="domain", metrics=["avg:timestamp", "sum:timestamp"])
    harbor = [row for row in r if row["_key"] == "harbor"][0]
    assert harbor["count"] == 2
    assert harbor["avg:timestamp"] == 1250.0
    assert harbor["sum:timestamp"] == 2500.0


def test_empty_table(engine):
    r = engine.query("nonexistent", where={"x": "y"})
    assert r["results"] == []
    assert r["total"] == 0
