"""RoomHistory — full event log with replay capability."""

from __future__ import annotations

import json
import time as _time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class EventType(Enum):
    JOIN = "join"
    LEAVE = "leave"
    MESSAGE = "message"
    FLOOR_GRANT = "floor_grant"
    FLOOR_RELEASE = "floor_release"
    TOPIC_START = "topic_start"
    TOPIC_END = "topic_end"
    VOTE = "vote"
    STATE_CHANGE = "state_change"
    CUSTOM = "custom"


@dataclass
class Event:
    type: EventType
    actor: str
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=_time.time)

    def to_dict(self) -> dict:
        return {
            "type": self.type.value,
            "actor": self.actor,
            "payload": self.payload,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Event:
        return cls(
            type=EventType(data["type"]),
            actor=data["actor"],
            payload=data.get("payload", {}),
            timestamp=data.get("timestamp", _time.time()),
        )


@dataclass
class RoomHistory:
    """Append-only event log for a room with query and replay."""

    room_id: str
    events: list[Event] = field(default_factory=list)
    _persist_path: str | None = field(default=None, repr=False)

    # -- recording --------------------------------------------------------

    def record(self, event_type: EventType, actor: str, **payload) -> Event:
        event = Event(type=event_type, actor=actor, payload=payload)
        self.events.append(event)
        if self._persist_path:
            self._append_to_file(event)
        return event

    def record_message(self, actor: str, content: str) -> Event:
        return self.record(EventType.MESSAGE, actor, content=content)

    def record_join(self, actor: str) -> Event:
        return self.record(EventType.JOIN, actor)

    def record_leave(self, actor: str) -> Event:
        return self.record(EventType.LEAVE, actor)

    # -- querying ---------------------------------------------------------

    def events_by_type(self, event_type: EventType) -> list[Event]:
        return [e for e in self.events if e.type == event_type]

    def events_by_actor(self, actor: str) -> list[Event]:
        return [e for e in self.events if e.actor == actor]

    def events_since(self, timestamp: float) -> list[Event]:
        return [e for e in self.events if e.timestamp >= timestamp]

    def messages(self) -> list[Event]:
        return self.events_by_type(EventType.MESSAGE)

    @property
    def event_count(self) -> int:
        return len(self.events)

    @property
    def participant_joins(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for e in self.events:
            if e.type == EventType.JOIN:
                counts[e.actor] = counts.get(e.actor, 0) + 1
        return counts

    # -- replay -----------------------------------------------------------

    def replay(self, callback, event_type: EventType | None = None) -> None:
        """Replay events through callback(event). Optionally filter by type."""
        for event in self.events:
            if event_type is None or event.type == event_type:
                callback(event)

    def summary(self) -> dict[str, Any]:
        type_counts: dict[str, int] = {}
        for e in self.events:
            key = e.type.value
            type_counts[key] = type_counts.get(key, 0) + 1
        return {
            "room_id": self.room_id,
            "total_events": len(self.events),
            "event_types": type_counts,
            "first_event": self.events[0].timestamp if self.events else None,
            "last_event": self.events[-1].timestamp if self.events else None,
        }

    # -- persistence ------------------------------------------------------

    def set_persist_path(self, path: str) -> None:
        self._persist_path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    def save(self, path: str | None = None) -> str:
        p = path or self._persist_path or f"history_{self.room_id}.jsonl"
        Path(p).parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w") as f:
            for event in self.events:
                f.write(json.dumps(event.to_dict()) + "\n")
        return p

    def load(self, path: str | None = None) -> int:
        p = path or self._persist_path or f"history_{self.room_id}.jsonl"
        if not Path(p).exists():
            return 0
        count = 0
        with open(p) as f:
            for line in f:
                if line.strip():
                    self.events.append(Event.from_dict(json.loads(line)))
                    count += 1
        return count

    def _append_to_file(self, event: Event) -> None:
        if not self._persist_path:
            return
        Path(self._persist_path).parent.mkdir(parents=True, exist_ok=True)
        with open(self._persist_path, "a") as f:
            f.write(json.dumps(event.to_dict()) + "\n")
