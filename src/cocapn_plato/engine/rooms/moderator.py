"""Moderator managing turn-taking and floor control."""

from __future__ import annotations

import time as _time
from collections import deque
from dataclasses import dataclass, field

from .participant import Participant, Permission


@dataclass
class FloorState:
    """Tracks who has the floor and turn order."""
    holder: str | None = None
    queue: deque[str] = field(default_factory=deque)
    granted_at: float = 0.0
    time_limit: float = 60.0  # seconds


@dataclass
class Moderator:
    """Manages turn-taking, floor control, and speaking order."""

    room_id: str
    floor: FloorState = field(default_factory=FloorState)
    speaking_order: list[str] = field(default_factory=list)
    round_robin: bool = True
    _spoken_this_round: set[str] = field(default_factory=set)
    _turn_number: int = 0

    # -- floor control ----------------------------------------------------

    def request_floor(self, participant: Participant) -> bool:
        """Request the floor. Returns True if granted immediately."""
        if not participant.has_permission(Permission.SPEAK):
            return False

        name = participant.name
        if self.floor.holder == name:
            return True  # already has it

        if self.floor.holder is None:
            return self._grant(name)
        elif name not in self.floor.queue:
            self.floor.queue.append(name)
        return False

    def release_floor(self, name: str) -> str | None:
        """Release the floor. Returns next holder or None."""
        if self.floor.holder != name:
            return None
        self._spoken_this_round.add(name)
        self._turn_number += 1
        self.floor.holder = None

        # Pick next from queue or round-robin
        next_speaker = self._next_speaker()
        if next_speaker:
            self._grant(next_speaker)
        return next_speaker

    def force_release(self) -> str | None:
        """Force-release floor (e.g., time limit exceeded)."""
        if self.floor.holder is None:
            return None
        return self.release_floor(self.floor.holder)

    def floor_status(self) -> dict:
        return {
            "holder": self.floor.holder,
            "queue": list(self.floor.queue),
            "granted_at": self.floor.granted_at,
            "time_limit": self.floor.time_limit,
            "turn_number": self._turn_number,
        }

    # -- turn-taking ------------------------------------------------------

    def set_speaking_order(self, names: list[str]) -> None:
        self.speaking_order = list(names)

    def new_round(self) -> None:
        self._spoken_this_round.clear()
        self._turn_number = 0
        if self.floor.holder:
            self.floor.holder = None

    def is_turn_expired(self) -> bool:
        if self.floor.holder is None:
            return False
        elapsed = _time.time() - self.floor.granted_at
        return elapsed > self.floor.time_limit

    # -- internals --------------------------------------------------------

    def _grant(self, name: str) -> bool:
        self.floor.holder = name
        self.floor.granted_at = _time.time()
        # Remove from queue if present
        try:
            self.floor.queue.remove(name)
        except ValueError:
            pass
        return True

    def _next_speaker(self) -> str | None:
        # First: try queue
        if self.floor.queue:
            return self.floor.queue.popleft()

        if not self.round_robin or not self.speaking_order:
            return None

        # Round-robin: find next who hasn't spoken this round
        for name in self.speaking_order:
            if name not in self._spoken_this_round:
                return name

        # All spoke; new round
        self.new_round()
        return self.speaking_order[0] if self.speaking_order else None
