"""Persistent rooms for agent collaboration.

Provides Room, Participant, Moderator, Agenda, and RoomHistory for
structured multi-agent collaboration with turn-taking, floor control,
agenda management, and full event replay.
"""

from .participant import Participant, Role, Permission
from .room import Room
from .moderator import Moderator
from .agenda import Agenda, Topic, TopicStatus, Vote
from .history import RoomHistory, Event, EventType

__all__ = [
    "Participant", "Role", "Permission",
    "Room",
    "Moderator",
    "Agenda", "Topic", "TopicStatus", "Vote",
    "RoomHistory", "Event", "EventType",
]
