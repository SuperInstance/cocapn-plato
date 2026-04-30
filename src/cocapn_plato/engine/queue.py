"""Simple task queue for fleet work distribution.

Maximum capability in minimum lines. In-memory + JSONL persistence.
"""
import json
import time
import uuid
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field, asdict
from datetime import datetime


@dataclass
class Task:
    id: str
    status: str  # pending, running, done, failed
    payload: Dict[str, Any]
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    attempts: int = 0
    max_attempts: int = 3
    created_at: float = field(default_factory=lambda: datetime.now().timestamp())
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    worker: Optional[str] = None
    priority: int = 0  # Higher = more urgent
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Task":
        return cls(**{k: v for k, v in d.items() if k in {f.name for f in cls.__dataclass_fields__.values()}})


class TaskQueue:
    """In-memory task queue with JSONL persistence."""

    def __init__(self, path: str = "./fleet_data/tasks.jsonl"):
        self.path = path
        self.tasks: Dict[str, Task] = {}
        self._load()

    def _load(self):
        try:
            with open(self.path) as f:
                for line in f:
                    if line.strip():
                        self.tasks[json.loads(line)["id"]] = Task.from_dict(json.loads(line))
        except FileNotFoundError:
            pass

    def _save(self):
        with open(self.path, "w") as f:
            for task in self.tasks.values():
                f.write(json.dumps(task.to_dict(), default=str) + "\n")

    def submit(self, payload: Dict[str, Any], priority: int = 0, tags: List[str] = None, max_attempts: int = 3) -> Task:
        task = Task(
            id=str(uuid.uuid4())[:8],
            status="pending",
            payload=payload,
            priority=priority,
            tags=tags or [],
            max_attempts=max_attempts,
        )
        self.tasks[task.id] = task
        self._save()
        return task

    def claim(self, worker: str = "anonymous", tags: List[str] = None) -> Optional[Task]:
        """Claim the highest-priority pending task."""
        candidates = [
            t for t in self.tasks.values()
            if t.status == "pending" and t.attempts < t.max_attempts
            and (not tags or any(tag in t.tags for tag in tags))
        ]
        if not candidates:
            return None
        task = max(candidates, key=lambda t: (t.priority, -t.created_at))
        task.status = "running"
        task.started_at = datetime.now().timestamp()
        task.worker = worker
        task.attempts += 1
        self._save()
        return task

    def complete(self, task_id: str, result: Dict[str, Any] = None) -> Optional[Task]:
        task = self.tasks.get(task_id)
        if not task:
            return None
        task.status = "done"
        task.result = result
        task.completed_at = datetime.now().timestamp()
        self._save()
        return task

    def fail(self, task_id: str, error: str = "") -> Optional[Task]:
        task = self.tasks.get(task_id)
        if not task:
            return None
        if task.attempts >= task.max_attempts:
            task.status = "failed"
        else:
            task.status = "pending"  # Retry
        task.error = error
        task.completed_at = datetime.now().timestamp()
        self._save()
        return task

    def list(self, status: str = None, limit: int = 50) -> List[Task]:
        tasks = list(self.tasks.values())
        if status:
            tasks = [t for t in tasks if t.status == status]
        tasks.sort(key=lambda t: t.created_at, reverse=True)
        return tasks[:limit]

    def stats(self) -> Dict[str, Any]:
        counts = {}
        for t in self.tasks.values():
            counts[t.status] = counts.get(t.status, 0) + 1
        pending = [t for t in self.tasks.values() if t.status == "pending"]
        running = [t for t in self.tasks.values() if t.status == "running"]
        return {
            "total": len(self.tasks),
            "counts": counts,
            "pending_oldest": min(t.created_at for t in pending) if pending else None,
            "running_longest": min(t.started_at for t in running) if running else None,
            "workers": list(set(t.worker for t in self.tasks.values() if t.worker)),
        }
