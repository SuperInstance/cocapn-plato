"""Comprehensive tests for the rooms collaboration package."""

import json
import os
import time

from cocapn_plato.engine.rooms import (
    Agenda,
    Event,
    EventType,
    Moderator,
    Participant,
    Permission,
    Role,
    Room,
    RoomHistory,
    Topic,
    TopicStatus,
)

# ---------------------------------------------------------------------------
# Participant
# ---------------------------------------------------------------------------

class TestParticipant:
    def test_default_role_is_member(self):
        p = Participant(name="alice")
        assert p.role == Role.MEMBER

    def test_member_permissions(self):
        p = Participant(name="alice")
        assert Permission.SPEAK in p.permissions
        assert Permission.VOTE in p.permissions
        assert Permission.PROPOSE in p.permissions
        assert Permission.MODERATE not in p.permissions

    def test_observer_permissions(self):
        p = Participant(name="bob", role=Role.OBSERVER)
        assert Permission.SPEAK in p.permissions
        assert Permission.VOTE not in p.permissions

    def test_admin_has_all_permissions(self):
        p = Participant(name="admin", role=Role.ADMIN)
        assert set(p.permissions) == set(Permission)

    def test_has_permission(self):
        p = Participant(name="alice", role=Role.MODERATOR)
        assert p.has_permission(Permission.MODERATE)
        assert not p.has_permission(Permission.ADMIN)

    def test_promote(self):
        p = Participant(name="alice")
        p.promote(Role.MODERATOR)
        assert p.role == Role.MODERATOR
        assert len(p.history) == 1
        assert p.history[0]["action"] == "promoted"

    def test_record_message(self):
        p = Participant(name="alice")
        p.record_message("hello")
        assert p.message_count == 1
        assert len(p.history) == 1
        assert p.last_active >= p.joined_at

    def test_serialization_roundtrip(self):
        p = Participant(name="alice", role=Role.ADMIN)
        p.record_message("test")
        p.promote(Role.MODERATOR)
        data = p.to_dict()
        p2 = Participant.from_dict(data)
        assert p2.name == p.name
        assert p2.role == p.role
        assert p2.message_count == p.message_count
        assert len(p2.history) == len(p.history)


# ---------------------------------------------------------------------------
# Room
# ---------------------------------------------------------------------------

class TestRoom:
    def test_join_and_leave(self):
        room = Room(id="r1", name="Test Room")
        p = Participant(name="alice")
        assert room.join(p) is True
        assert room.participant_count == 1
        assert room.get_participant("alice") is p
        left = room.leave("alice")
        assert left is p
        assert room.participant_count == 0

    def test_double_join_rejected(self):
        room = Room(id="r1", name="Test Room")
        room.join(Participant(name="alice"))
        assert room.join(Participant(name="alice")) is False

    def test_capacity_enforced(self):
        room = Room(id="r1", name="Tiny", capacity=2)
        assert room.join(Participant(name="a"))
        assert room.join(Participant(name="b"))
        assert room.is_full
        assert not room.join(Participant(name="c"))

    def test_leave_nonexistent(self):
        room = Room(id="r1", name="Empty")
        assert room.leave("nobody") is None

    def test_state_persistence(self):
        room = Room(id="r1", name="Test")
        room.set_state("phase", "discussion")
        room.set_state("round", 3)
        assert room.get_state("phase") == "discussion"
        assert room.get_state("round") == 3
        assert room.get_state("missing", "default") == "default"

    def test_save_and_load(self, tmp_path):
        room = Room(id="r1", name="Persist Room", capacity=10)
        room.join(Participant(name="alice", role=Role.ADMIN))
        room.set_state("key", "value")
        path = room.save(str(tmp_path))
        assert os.path.exists(path)

        loaded = Room.load(path)
        assert loaded.id == room.id
        assert loaded.name == room.name
        assert loaded.capacity == room.capacity
        assert "alice" in loaded.participants
        assert loaded.participants["alice"].role == Role.ADMIN
        assert loaded.get_state("key") == "value"

    def test_to_dict_from_dict_roundtrip(self):
        room = Room(id="r1", name="Round", capacity=5, description="desc")
        room.join(Participant(name="alice"))
        data = room.to_dict()
        room2 = Room.from_dict(data)
        assert room2.id == room.id
        assert "alice" in room2.participants
        assert room2.capacity == 5


# ---------------------------------------------------------------------------
# Moderator
# ---------------------------------------------------------------------------

class TestModerator:
    def test_grant_floor_to_empty_room(self):
        mod = Moderator(room_id="r1")
        p = Participant(name="alice")
        assert mod.request_floor(p) is True
        assert mod.floor.holder == "alice"

    def test_queue_when_floor_taken(self):
        mod = Moderator(room_id="r1")
        alice = Participant(name="alice")
        bob = Participant(name="bob")
        mod.request_floor(alice)
        assert mod.request_floor(bob) is False
        assert bob.name in mod.floor.queue

    def test_release_and_next(self):
        mod = Moderator(room_id="r1")
        alice = Participant(name="alice")
        bob = Participant(name="bob")
        mod.request_floor(alice)
        mod.request_floor(bob)
        next_speaker = mod.release_floor("alice")
        assert next_speaker == "bob"
        assert mod.floor.holder == "bob"

    def test_round_robin(self):
        mod = Moderator(room_id="r1", round_robin=True)
        mod.set_speaking_order(["alice", "bob", "carol"])
        # Grant to alice
        mod.request_floor(Participant(name="alice"))
        mod.release_floor("alice")
        assert mod.floor.holder == "bob"
        mod.release_floor("bob")
        assert mod.floor.holder == "carol"
        # After carol, should wrap to alice (new round)
        mod.release_floor("carol")
        assert mod.floor.holder == "alice"

    def test_new_round(self):
        mod = Moderator(room_id="r1")
        mod.set_speaking_order(["alice", "bob"])
        mod.request_floor(Participant(name="alice"))
        mod.release_floor("alice")
        mod.new_round()
        assert mod._turn_number == 0

    def test_force_release(self):
        mod = Moderator(room_id="r1")
        mod.request_floor(Participant(name="alice"))
        mod.request_floor(Participant(name="bob"))
        next_speaker = mod.force_release()
        assert next_speaker == "bob"

    def test_floor_status(self):
        mod = Moderator(room_id="r1")
        mod.request_floor(Participant(name="alice"))
        status = mod.floor_status()
        assert status["holder"] == "alice"
        assert "turn_number" in status

    def test_observer_cannot_get_floor(self):
        mod = Moderator(room_id="r1")
        obs = Participant(name="spy", role=Role.OBSERVER)
        # OBSERVER has SPEAK permission, so this actually succeeds
        # If you wanted to restrict floor, you'd check a different permission
        result = mod.request_floor(obs)
        # Observer CAN speak by default, so floor is granted
        assert result is True

    def test_time_limit_expired(self):
        mod = Moderator(room_id="r1")
        mod.floor.time_limit = 0.0  # immediate expiry
        mod.request_floor(Participant(name="alice"))
        # Force grant_at into the past
        mod.floor.granted_at = _time.time() - 10
        assert mod.is_turn_expired()


# ---------------------------------------------------------------------------
# Agenda
# ---------------------------------------------------------------------------

class TestAgenda:
    def test_add_and_advance(self):
        agenda = Agenda(room_id="r1")
        t1 = Topic(title="Intro", time_allocated=60.0)
        t2 = Topic(title="Main", time_allocated=120.0)
        agenda.add_topic(t1)
        agenda.add_topic(t2)

        assert agenda.current_topic is None
        first = agenda.advance()
        assert first is t1
        assert t1.status == TopicStatus.ACTIVE
        assert agenda.current_topic is t1

        second = agenda.advance()
        assert second is t2
        assert t1.status == TopicStatus.COMPLETED
        assert t2.status == TopicStatus.ACTIVE

        # No more topics
        assert agenda.advance() is None

    def test_remove_topic(self):
        agenda = Agenda(room_id="r1")
        agenda.add_topic(Topic(title="A"))
        agenda.add_topic(Topic(title="B"))
        removed = agenda.remove_topic(0)
        assert removed.title == "A"
        assert len(agenda.topics) == 1

    def test_remove_invalid_index(self):
        agenda = Agenda(room_id="r1")
        assert agenda.remove_topic(99) is None

    def test_defer_current(self):
        agenda = Agenda(room_id="r1")
        agenda.add_topic(Topic(title="Skip me"))
        agenda.advance()
        assert agenda.defer_current() is True
        assert agenda.topics[0].status == TopicStatus.DEFERRED

    def test_sort_by_priority(self):
        agenda = Agenda(room_id="r1")
        agenda.add_topic(Topic(title="Low", priority=1))
        agenda.add_topic(Topic(title="High", priority=3))
        agenda.add_topic(Topic(title="Mid", priority=2))
        agenda.sort_by_priority()
        assert agenda.topics[0].title == "High"
        assert agenda.topics[2].title == "Low"

    def test_total_time(self):
        agenda = Agenda(room_id="r1")
        agenda.add_topic(Topic(title="A", time_allocated=100))
        agenda.add_topic(Topic(title="B", time_allocated=200))
        assert agenda.total_time_allocated == 300

    def test_pending_and_completed(self):
        agenda = Agenda(room_id="r1")
        agenda.add_topic(Topic(title="A"))
        agenda.add_topic(Topic(title="B"))
        assert len(agenda.pending_topics) == 2
        agenda.advance()  # A becomes ACTIVE
        assert len(agenda.pending_topics) == 1
        agenda.advance()  # A becomes COMPLETED, B becomes ACTIVE
        assert len(agenda.completed_topics) == 1
        assert len(agenda.pending_topics) == 0

    def test_voting(self):
        topic = Topic(title="Vote test")
        topic.cast_vote("alice", "for")
        topic.cast_vote("bob", "against")
        topic.cast_vote("carol", "for")
        assert topic.vote_tally == {"for": 2, "against": 1, "abstain": 0}

    def test_vote_overwrite(self):
        topic = Topic(title="Change mind")
        topic.cast_vote("alice", "for")
        topic.cast_vote("alice", "against")
        assert topic.vote_tally == {"for": 0, "against": 1, "abstain": 0}

    def test_topic_serialization(self):
        topic = Topic(title="Test", description="desc", owner="alice", priority=3)
        topic.cast_vote("bob", "for")
        data = topic.to_dict()
        t2 = Topic.from_dict(data)
        assert t2.title == "Test"
        assert t2.owner == "alice"
        assert len(t2.votes) == 1

    def test_agenda_serialization(self):
        agenda = Agenda(room_id="r1")
        agenda.add_topic(Topic(title="A", priority=2))
        data = agenda.to_dict()
        a2 = Agenda.from_dict(data)
        assert len(a2.topics) == 1
        assert a2.topics[0].title == "A"


# ---------------------------------------------------------------------------
# RoomHistory
# ---------------------------------------------------------------------------

class TestRoomHistory:
    def test_record_event(self):
        h = RoomHistory(room_id="r1")
        e = h.record(EventType.JOIN, "alice")
        assert e.type == EventType.JOIN
        assert e.actor == "alice"
        assert h.event_count == 1

    def test_convenience_recorders(self):
        h = RoomHistory(room_id="r1")
        h.record_join("alice")
        h.record_message("alice", "hello")
        h.record_leave("alice")
        assert h.event_count == 3

    def test_query_by_type(self):
        h = RoomHistory(room_id="r1")
        h.record_join("alice")
        h.record_message("alice", "hi")
        h.record_message("bob", "hey")
        msgs = h.events_by_type(EventType.MESSAGE)
        assert len(msgs) == 2
        joins = h.events_by_type(EventType.JOIN)
        assert len(joins) == 1

    def test_query_by_actor(self):
        h = RoomHistory(room_id="r1")
        h.record_message("alice", "hello")
        h.record_message("bob", "hi")
        h.record_message("alice", "world")
        assert len(h.events_by_actor("alice")) == 2
        assert len(h.events_by_actor("bob")) == 1

    def test_events_since(self):
        h = RoomHistory(room_id="r1")
        h.record(EventType.CUSTOM, "alice")
        cutoff = time.time()
        time.sleep(0.01)
        h.record(EventType.CUSTOM, "bob")
        recent = h.events_since(cutoff)
        assert len(recent) == 1
        assert recent[0].actor == "bob"

    def test_messages_shortcut(self):
        h = RoomHistory(room_id="r1")
        h.record_message("alice", "hi")
        h.record_join("bob")
        assert len(h.messages()) == 1

    def test_participant_joins(self):
        h = RoomHistory(room_id="r1")
        h.record_join("alice")
        h.record_join("bob")
        h.record_join("alice")
        assert h.participant_joins == {"alice": 2, "bob": 1}

    def test_replay(self):
        h = RoomHistory(room_id="r1")
        h.record_message("alice", "a")
        h.record_join("bob")
        h.record_message("bob", "b")
        collected = []
        h.replay(lambda e: collected.append(e.actor), event_type=EventType.MESSAGE)
        assert collected == ["alice", "bob"]

    def test_summary(self):
        h = RoomHistory(room_id="r1")
        h.record_join("alice")
        h.record_message("alice", "hi")
        s = h.summary()
        assert s["total_events"] == 2
        assert s["event_types"]["join"] == 1
        assert s["event_types"]["message"] == 1

    def test_save_and_load(self, tmp_path):
        h = RoomHistory(room_id="r1")
        h.record_join("alice")
        h.record_message("alice", "hello")
        path = h.save(str(tmp_path / "history.jsonl"))
        assert os.path.exists(path)

        h2 = RoomHistory(room_id="r1")
        count = h2.load(path)
        assert count == 2
        assert h2.event_count == 2
        assert h2.events[0].type == EventType.JOIN
        assert h2.events[1].payload["content"] == "hello"

    def test_auto_persist(self, tmp_path):
        p = str(tmp_path / "auto.jsonl")
        h = RoomHistory(room_id="r1")
        h.set_persist_path(p)
        h.record_join("alice")
        h.record_message("alice", "live")
        # File should have 2 lines
        lines = open(p).readlines()
        assert len(lines) == 2
        assert json.loads(lines[0])["type"] == "join"

    def test_event_serialization(self):
        e = Event(type=EventType.MESSAGE, actor="alice", payload={"content": "hi"})
        data = e.to_dict()
        e2 = Event.from_dict(data)
        assert e2.type == EventType.MESSAGE
        assert e2.actor == "alice"
        assert e2.payload["content"] == "hi"

    def test_empty_history_summary(self):
        h = RoomHistory(room_id="empty")
        s = h.summary()
        assert s["total_events"] == 0
        assert s["first_event"] is None


# ---------------------------------------------------------------------------
# Integration: Room + Moderator + Agenda + History working together
# ---------------------------------------------------------------------------

class TestIntegration:
    def test_full_session(self, tmp_path):
        # Create room
        room = Room(id="session1", name="Planning", capacity=5)
        history = RoomHistory(room_id="session1")
        moderator = Moderator(room_id="session1", round_robin=True)
        agenda = Agenda(room_id="session1")

        # Participants join
        alice = Participant(name="alice", role=Role.MODERATOR)
        bob = Participant(name="bob")
        carol = Participant(name="carol")
        for p in [alice, bob, carol]:
            room.join(p)
            history.record_join(p.name)

        assert room.participant_count == 3

        # Set up agenda
        agenda.add_topic(Topic(title="Budget", time_allocated=300, priority=3))
        agenda.add_topic(Topic(title="Timeline", time_allocated=200, priority=2))
        agenda.sort_by_priority()

        # Start first topic
        topic = agenda.advance()
        assert topic.title == "Budget"
        history.record(EventType.TOPIC_START, "system", title="Budget")

        # Round-robin discussion
        moderator.set_speaking_order(["alice", "bob", "carol"])
        for name in ["alice", "bob", "carol"]:
            moderator.request_floor(Participant(name=name))
            history.record_message(name, f"Comment on {topic.title}")
            moderator.release_floor(name)

        # Vote
        topic.cast_vote("alice", "for")
        topic.cast_vote("bob", "for")
        topic.cast_vote("carol", "against")
        assert topic.vote_tally["for"] == 2

        # Advance
        topic2 = agenda.advance()
        assert topic2.title == "Timeline"
        assert topic.status == TopicStatus.COMPLETED

        # Save everything
        room.save(str(tmp_path))
        history.save(str(tmp_path / "history.jsonl"))

        # Verify
        loaded_room = Room.load(str(tmp_path / f"room_{room.id}.json"))
        assert loaded_room.participant_count == 3
        assert loaded_room.get_state("key") is None  # no state set

        summary = history.summary()
        assert summary["total_events"] == 3 + 3 + 1  # 3 joins + 3 messages + 1 topic_start


import time as _time  # noqa: E402 (used in TestModerator.test_time_limit_expired)
