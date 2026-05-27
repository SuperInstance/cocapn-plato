"""Participant in a collaborative room."""

from __future__ import annotations

import time as _time
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class Role(Enum):
    OBSERVER = "observer"
    MEMBER = "member"
    MODERATOR = "moderator"
    ADMIN = "admin"


class Permission(Enum):
    SPEAK = "speak"
    VOTE = "vote"
    PROPOSE = "propose"
    MODERATE = "moderate"
    ADMIN = "admin"


# Role → default permissions mapping
_ROLE_PERMISSIONS: dict[Role, list[Permission]] = {
    Role.OBSERVER: [Permission.SPEAK],
    Role.MEMBER: [Permission.SPEAK, Permission.VOTE, Permission.PROPOSE],
    Role.MODERATOR: [Permission.SPEAK, Permission.VOTE, Permission.PROPOSE, Permission.MODERATE],
    Role.ADMIN: list(Permission),
}


@dataclass
class Participant:
    """An agent participating in a room."""

    name: str
    role: Role = Role.MEMBER
    joined_at: float = field(default_factory=_time.time)
    last_active: float = field(default_factory=_time.time)
    message_count: int = 0
    history: list[dict] = field(default_factory=list)

    # -- derived ----------------------------------------------------------

    @property
    def permissions(self) -> list[Permission]:
        return list(_ROLE_PERMISSIONS.get(self.role, []))

    def has_permission(self, perm: Permission) -> bool:
        return perm in self.permissions

    def promote(self, new_role: Role) -> None:
        self.role = new_role
        self.history.append({"action": "promoted", "role": new_role.value, "ts": _time.time()})

    def touch(self) -> None:
        self.last_active = _time.time()

    def record_message(self, content: str) -> None:
        self.message_count += 1
        self.touch()
        self.history.append({"action": "message", "content": content, "ts": _time.time()})

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "role": self.role.value,
            "joined_at": self.joined_at,
            "last_active": self.last_active,
            "message_count": self.message_count,
            "history": self.history,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Participant:
        p = cls(
            name=data["name"],
            role=Role(data["role"]),
            joined_at=data.get("joined_at", _time.time()),
            last_active=data.get("last_active", _time.time()),
            message_count=data.get("message_count", 0),
            history=data.get("history", []),
        )
        return p
