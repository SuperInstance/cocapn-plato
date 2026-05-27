"""Persistent rooms for agent collaboration.

Provides Room, Participant, Moderator, Agenda, and RoomHistory for
structured multi-agent collaboration with turn-taking, floor control,
agenda management, and full event replay.
"""

from .agenda import Agenda, Topic, TopicStatus, Vote
from .history import Event, EventType, RoomHistory
from .moderator import Moderator
from .participant import Participant, Permission, Role
from .room import Room

__all__ = [
    "Participant", "Role", "Permission",
    "Room",
    "Moderator",
    "Agenda", "Topic", "TopicStatus", "Vote",
    "RoomHistory", "Event", "EventType",
]
