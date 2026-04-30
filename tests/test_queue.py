"""Tests for the task queue."""
import pytest
import tempfile
import os
from cocapn_plato.engine.queue import TaskQueue, Task


def test_submit_and_claim():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name
    try:
        q = TaskQueue(path)
        t = q.submit({"action": "scrape", "url": "https://example.com"}, priority=5, tags=["scraper"])
        assert t.status == "pending"
        assert t.priority == 5
        assert t.tags == ["scraper"]

        claimed = q.claim(worker="bot-1", tags=["scraper"])
        assert claimed is not None
        assert claimed.id == t.id
        assert claimed.status == "running"
        assert claimed.worker == "bot-1"
        assert claimed.attempts == 1

        # No more matching tasks
        assert q.claim(worker="bot-2", tags=["other"]) is None
    finally:
        os.unlink(path)


def test_complete_and_fail():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name
    try:
        q = TaskQueue(path)
        t = q.submit({"action": "process"})
        q.claim(worker="bot-1")
        
        q.complete(t.id, {"result": "ok"})
        updated = q.tasks[t.id]
        assert updated.status == "done"
        assert updated.result["result"] == "ok"
        assert updated.completed_at is not None
        
        # New task, fail once
        t2 = q.submit({"action": "risky"}, max_attempts=2)
        q.claim(worker="bot-1")
        q.fail(t2.id, "timeout")
        updated2 = q.tasks[t2.id]
        assert updated2.status == "pending"  # Retry
        assert updated2.attempts == 1
        
        # Fail again
        q.claim(worker="bot-1")
        q.fail(t2.id, "timeout again")
        updated3 = q.tasks[t2.id]
        assert updated3.status == "failed"
        assert updated3.attempts == 2
    finally:
        os.unlink(path)


def test_priority_ordering():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name
    try:
        q = TaskQueue(path)
        low = q.submit({"priority": "low"}, priority=1)
        high = q.submit({"priority": "high"}, priority=10)
        mid = q.submit({"priority": "mid"}, priority=5)
        
        claimed = q.claim()
        assert claimed.id == high.id
        
        claimed2 = q.claim()
        assert claimed2.id == mid.id
        
        claimed3 = q.claim()
        assert claimed3.id == low.id
    finally:
        os.unlink(path)


def test_persistence():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name
    try:
        q1 = TaskQueue(path)
        t = q1.submit({"action": "persist"})
        q1.claim(worker="bot-1")
        
        # New instance reads same file
        q2 = TaskQueue(path)
        assert t.id in q2.tasks
        assert q2.tasks[t.id].status == "running"
        assert q2.tasks[t.id].worker == "bot-1"
    finally:
        os.unlink(path)


def test_stats():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name
    try:
        q = TaskQueue(path)
        q.submit({"a": 1})
        q.submit({"a": 2})
        q.claim(worker="bot-1")
        q.submit({"a": 3})
        
        stats = q.stats()
        assert stats["total"] == 3
        assert stats["counts"]["pending"] == 2
        assert stats["counts"]["running"] == 1
        assert "bot-1" in stats["workers"]
    finally:
        os.unlink(path)


def test_list_filter():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name
    try:
        q = TaskQueue(path)
        t1 = q.submit({"a": 1})
        t2 = q.submit({"a": 2})
        claimed = q.claim(worker="bot-1")  # FIFO: claims t1 (oldest)
        
        pending = q.list(status="pending")
        assert len(pending) == 1
        assert pending[0].status == "pending"
        assert pending[0].id == t2.id
        
        running = q.list(status="running")
        assert len(running) == 1
        assert running[0].id == t1.id
    finally:
        os.unlink(path)
