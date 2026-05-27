"""Collaborative room with capacity, participants, and state persistence."""

from __future__ import annotations

import json
import time as _time
from dataclasses import dataclass, field
from pathlib import Path

from .participant import Participant


@dataclass
class Room:
    """A persistent collaboration room."""

    id: str
    name: str
    capacity: int = 50
    description: str = ""
    participants: dict[str, Participant] = field(default_factory=dict)
    created_at: float = field(default_factory=_time.time)
    state: dict = field(default_factory=dict)
    _persist_dir: str | None = field(default=None, repr=False)

    # -- participant management -------------------------------------------

    def join(self, participant: Participant) -> bool:
        if len(self.participants) >= self.capacity:
            return False
        if participant.name in self.participants:
            return False
        self.participants[participant.name] = participant
        return True

    def leave(self, name: str) -> Participant | None:
        return self.participants.pop(name, None)

    def get_participant(self, name: str) -> Participant | None:
        return self.participants.get(name)

    @property
    def participant_count(self) -> int:
        return len(self.participants)

    @property
    def is_full(self) -> bool:
        return len(self.participants) >= self.capacity

    @property
    def active_participants(self) -> list[Participant]:
        cutoff = _time.time() - 300  # 5 min
        return [p for p in self.participants.values() if p.last_active >= cutoff]

    # -- state persistence ------------------------------------------------

    def set_state(self, key: str, value) -> None:
        self.state[key] = value

    def get_state(self, key: str, default=None):
        return self.state.get(key, default)

    def save(self, directory: str | None = None) -> str:
        d = directory or self._persist_dir or "."
        Path(d).mkdir(parents=True, exist_ok=True)
        path = Path(d) / f"room_{self.id}.json"
        data = self.to_dict()
        path.write_text(json.dumps(data, indent=2, default=str))
        return str(path)

    @classmethod
    def load(cls, path: str) -> Room:
        data = json.loads(Path(path).read_text())
        return cls.from_dict(data)

    # -- serialization ----------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "capacity": self.capacity,
            "description": self.description,
            "participants": {n: p.to_dict() for n, p in self.participants.items()},
            "created_at": self.created_at,
            "state": self.state,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Room:
        participants = {}
        for name, pdata in data.get("participants", {}).items():
            participants[name] = Participant.from_dict(pdata)
        return cls(
            id=data["id"],
            name=data["name"],
            capacity=data.get("capacity", 50),
            description=data.get("description", ""),
            participants=participants,
            created_at=data.get("created_at", _time.time()),
            state=data.get("state", {}),
        )
