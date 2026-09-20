"""Tests for the AG-UI endpoint — grounded answers, honest negatives, SSE lifecycle."""

import json
import tempfile

import pytest
from fastapi.testclient import TestClient

from cocapn_plato.engine.engine import Fleet
from cocapn_plato.server.agui import (
    agui_event_stream,
    build_answer,
    extract_last_user_text,
)
from cocapn_plato.server.routes import create_app

TILES = [
    {
        "agent": "ccc",
        "domain": "harbor",
        "question": "What is the harbor?",
        "answer": "A coordination hub.",
        "timestamp": 1000,
    },
    {
        "agent": "oracle1",
        "domain": "forge",
        "question": "How to build?",
        "answer": "Use the anvil.",
        "timestamp": 2000,
    },
]


@pytest.fixture
def client():
    with tempfile.TemporaryDirectory() as tmp:
        with open(f"{tmp}/tiles.jsonl", "w") as f:
            for tile in TILES:
                f.write(json.dumps(tile) + "\n")
        fleet = Fleet(storage_dir=tmp)
        app = create_app(fleet_instance=fleet)
        with TestClient(app) as c:
            yield c


def test_extract_last_user_text_plain():
    msgs = [
        {"role": "assistant", "content": "earlier"},
        {"role": "user", "content": "what is the harbor?"},
    ]
    assert extract_last_user_text(msgs) == "what is the harbor?"


def test_extract_last_user_text_content_blocks():
    msgs = [
        {
            "role": "user",
            "content": [{"type": "text", "text": "how to build?"}],
        }
    ]
    assert extract_last_user_text(msgs) == "how to build?"


def test_extract_last_user_text_empty():
    assert extract_last_user_text([{"role": "assistant", "content": "hi"}]) == ""


def test_build_answer_grounded():
    out = build_answer("harbor", TILES[:1])
    assert "Q: What is the harbor?" in out
    assert "A: A coordination hub." in out
    assert "ccc, domain harbor" in out


def test_build_answer_honest_negative():
    out = build_answer("zzzz-nothing", [])
    assert "honest negative" in out
    assert "found nothing" in out


def test_build_answer_no_question():
    assert "No question received" in build_answer("", TILES)


def _collect(stream):
    return [json.loads(chunk.split("data: ", 1)[1]) for chunk in stream]


@pytest.mark.asyncio
async def test_event_stream_lifecycle():
    stream = agui_event_stream("t1", "r1", "hello world")
    events = _collect([chunk async for chunk in stream])
    types = [e["type"] for e in events]
    assert types[0] == "RUN_STARTED"
    assert types[-1] == "RUN_FINISHED"
    assert "TEXT_MESSAGE_START" in types
    assert "TEXT_MESSAGE_END" in types
    contents = [e for e in events if e["type"] == "TEXT_MESSAGE_CONTENT"]
    assert "".join(e["delta"] for e in contents) == "hello world "
    assert events[-1]["result"] == "hello world"


def test_route_descriptor(client):
    resp = client.get("/api/ag-ui")
    assert resp.status_code == 200
    body = resp.json()
    assert body["protocol"] == "ag-ui"
    assert body["run_endpoint"] == "/api/ag-ui/run"


def test_route_run_streams_grounded_answer(client):
    resp = client.post(
        "/api/ag-ui/run",
        json={
            "threadId": "t1",
            "runId": "r1",
            "messages": [{"role": "user", "content": "harbor"}],
        },
    )
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    events = [
        json.loads(line.split("data: ", 1)[1])
        for line in resp.text.splitlines()
        if line.startswith("data: ")
    ]
    types = [e["type"] for e in events]
    assert types[0] == "RUN_STARTED"
    assert types[-1] == "RUN_FINISHED"
    answer = events[-1]["result"]
    assert "coordination hub" in answer


def test_route_run_honest_negative(client):
    resp = client.post(
        "/api/ag-ui/run",
        json={
            "threadId": "t1",
            "runId": "r2",
            "messages": [{"role": "user", "content": "unrecorded-topic"}],
        },
    )
    assert resp.status_code == 200
    assert "honest negative" in resp.text
