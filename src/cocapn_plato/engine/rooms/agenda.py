"""Agenda with topics, time allocation, and voting."""

from __future__ import annotations

import time as _time
from dataclasses import dataclass, field
from enum import Enum


class TopicStatus(Enum):
    PENDING = "pending"
    ACTIVE = "active"
    COMPLETED = "completed"
    DEFERRED = "deferred"


@dataclass
class Vote:
    voter: str
    value: str  # "for", "against", "abstain"
    timestamp: float = field(default_factory=_time.time)


@dataclass
class Topic:
    title: str
    description: str = ""
    owner: str = ""
    status: TopicStatus = TopicStatus.PENDING
    time_allocated: float = 300.0  # seconds
    time_started: float = 0.0
    votes: list[Vote] = field(default_factory=list)
    priority: int = 1  # 1=low, 2=normal, 3=high
    created_at: float = field(default_factory=_time.time)

    @property
    def time_remaining(self) -> float:
        if self.status != TopicStatus.ACTIVE or not self.time_started:
            return self.time_allocated
        elapsed = _time.time() - self.time_started
        return max(0.0, self.time_allocated - elapsed)

    @property
    def vote_tally(self) -> dict[str, int]:
        tally = {"for": 0, "against": 0, "abstain": 0}
        for v in self.votes:
            if v.value in tally:
                tally[v.value] += 1
        return tally

    def cast_vote(self, voter: str, value: str) -> None:
        # Remove previous vote by same voter
        self.votes = [v for v in self.votes if v.voter != voter]
        self.votes.append(Vote(voter=voter, value=value))

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "description": self.description,
            "owner": self.owner,
            "status": self.status.value,
            "time_allocated": self.time_allocated,
            "priority": self.priority,
            "votes": [{"voter": v.voter, "value": v.value, "ts": v.timestamp} for v in self.votes],
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Topic:
        topic = cls(
            title=data["title"],
            description=data.get("description", ""),
            owner=data.get("owner", ""),
            status=TopicStatus(data.get("status", "pending")),
            time_allocated=data.get("time_allocated", 300.0),
            priority=data.get("priority", 1),
            created_at=data.get("created_at", _time.time()),
        )
        for vdata in data.get("votes", []):
            topic.votes.append(Vote(voter=vdata["voter"], value=vdata["value"], timestamp=vdata.get("ts", _time.time())))
        return topic


@dataclass
class Agenda:
    """Ordered agenda of topics with time allocation and voting."""

    room_id: str
    topics: list[Topic] = field(default_factory=list)
    current_index: int = -1
    created_at: float = field(default_factory=_time.time)

    # -- topic management -------------------------------------------------

    def add_topic(self, topic: Topic) -> int:
        self.topics.append(topic)
        return len(self.topics) - 1

    def remove_topic(self, index: int) -> Topic | None:
        if 0 <= index < len(self.topics):
            return self.topics.pop(index)
        return None

    @property
    def current_topic(self) -> Topic | None:
        if 0 <= self.current_index < len(self.topics):
            return self.topics[self.current_index]
        return None

    def advance(self) -> Topic | None:
        """Move to next topic. Returns it or None."""
        if self.current_topic:
            self.current_topic.status = TopicStatus.COMPLETED
        self.current_index += 1
        if 0 <= self.current_index < len(self.topics):
            self.current_topic.status = TopicStatus.ACTIVE
            self.current_topic.time_started = _time.time()
            return self.current_topic
        return None

    def defer_current(self) -> bool:
        if self.current_topic and self.current_topic.status == TopicStatus.ACTIVE:
            self.current_topic.status = TopicStatus.DEFERRED
            return True
        return False

    # -- ordering ---------------------------------------------------------

    def sort_by_priority(self) -> None:
        self.topics.sort(key=lambda t: -t.priority)

    @property
    def total_time_allocated(self) -> float:
        return sum(t.time_allocated for t in self.topics)

    @property
    def pending_topics(self) -> list[Topic]:
        return [t for t in self.topics if t.status == TopicStatus.PENDING]

    @property
    def completed_topics(self) -> list[Topic]:
        return [t for t in self.topics if t.status == TopicStatus.COMPLETED]

    def to_dict(self) -> dict:
        return {
            "room_id": self.room_id,
            "topics": [t.to_dict() for t in self.topics],
            "current_index": self.current_index,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Agenda:
        agenda = cls(
            room_id=data["room_id"],
            current_index=data.get("current_index", -1),
            created_at=data.get("created_at", _time.time()),
        )
        for tdata in data.get("topics", []):
            agenda.topics.append(Topic.from_dict(tdata))
        return agenda
