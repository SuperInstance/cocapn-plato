"""Simple task queue for fleet work distribution.

Maximum capability in minimum lines. In-memory + JSONL persistence.
"""

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class Task:
    id: str
    status: str  # pending, running, done, failed
    payload: dict[str, Any]
    result: dict[str, Any] | None = None
    error: str | None = None
    attempts: int = 0
    max_attempts: int = 3
    created_at: float = field(default_factory=lambda: datetime.now().timestamp())
    started_at: float | None = None
    completed_at: float | None = None
    worker: str | None = None
    priority: int = 0  # Higher = more urgent
    tags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Task":
        return cls(
            **{
                k: v
                for k, v in d.items()
                if k in {f.name for f in cls.__dataclass_fields__.values()}
            }
        )


class TaskQueue:
    """In-memory task queue with JSONL persistence."""

    def __init__(self, path: str = "./fleet_data/tasks.jsonl"):
        self.path = path
        self.tasks: dict[str, Task] = {}
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

    def submit(
        self,
        payload: dict[str, Any],
        priority: int = 0,
        tags: list[str] = None,
        max_attempts: int = 3,
    ) -> Task:
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

    def claim(self, worker: str = "anonymous", tags: list[str] = None) -> Task | None:
        """Claim the highest-priority pending task."""
        candidates = [
            t
            for t in self.tasks.values()
            if t.status == "pending"
            and t.attempts < t.max_attempts
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

    def complete(self, task_id: str, result: dict[str, Any] = None) -> Task | None:
        task = self.tasks.get(task_id)
        if not task:
            return None
        task.status = "done"
        task.result = result
        task.completed_at = datetime.now().timestamp()
        self._save()
        return task

    def fail(self, task_id: str, error: str = "") -> Task | None:
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

    def list(self, status: str = None, limit: int = 50) -> list[Task]:
        tasks = list(self.tasks.values())
        if status:
            tasks = [t for t in tasks if t.status == status]
        tasks.sort(key=lambda t: t.created_at, reverse=True)
        return tasks[:limit]

    def stats(self) -> dict[str, Any]:
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
