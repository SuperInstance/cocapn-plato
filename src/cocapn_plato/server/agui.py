"""AG-UI protocol endpoint v0 — grounded chat over the fleet tile store.

Implements the AG-UI run lifecycle (CopilotKit agent-UI protocol) as an SSE
stream: RUN_STARTED -> TEXT_MESSAGE_START -> TEXT_MESSAGE_CONTENT* ->
TEXT_MESSAGE_END -> RUN_FINISHED.

The "agent" is deliberately boring: it full-text queries the local tile store
and answers ONLY from what the fleet has actually written down. No matches =
an honest negative, never an invention. Style is subtext, not costume.
"""

import json
import uuid
from collections.abc import AsyncIterator
from typing import Any


def extract_last_user_text(messages: list[dict[str, Any]]) -> str:
    """Pull the newest user message text from an AG-UI messages array."""
    for message in reversed(messages or []):
        if message.get("role") == "user":
            content = message.get("content")
            if isinstance(content, str) and content.strip():
                return content.strip()
            if isinstance(content, list):
                parts = [
                    str(p.get("text", ""))
                    for p in content
                    if isinstance(p, dict) and p.get("type") == "text"
                ]
                joined = " ".join(p for p in parts if p).strip()
                if joined:
                    return joined
    return ""


def build_answer(query_text: str, results: list[dict[str, Any]], limit: int = 5) -> str:
    """Compose a grounded answer from query hits. Honest negative when empty."""
    if not query_text.strip():
        return (
            "No question received. Send a user message and I'll search the "
            "fleet tile store for what the fleet has actually recorded."
        )
    if not results:
        return (
            f"I searched the fleet tile store for {query_text!r} and found nothing. "
            "This is an honest negative — the fleet has not recorded a tile that "
            "matches. A tile can be submitted via POST /bridge/submit."
        )

    lines = [f"From the fleet tile store, {len(results)} record(s) match {query_text!r}:"]
    for i, rec in enumerate(results[:limit], 1):
        question = str(rec.get("question", "")).strip()
        answer = str(rec.get("answer", "")).strip()
        agent = rec.get("agent", "unknown")
        domain = rec.get("domain", "unknown")
        lines.append(f"{i}. Q: {question}")
        lines.append(f"   A: {answer}")
        lines.append(f"   — {agent}, domain {domain}")
    return "\n".join(lines)


def _sse(event: dict[str, Any]) -> str:
    return f"event: {event['type']}\ndata: {json.dumps(event)}\n\n"


async def agui_event_stream(
    thread_id: str, run_id: str, answer: str
) -> AsyncIterator[str]:
    """Yield AG-UI lifecycle events for one run, text chunked by word."""
    message_id = str(uuid.uuid4())
    yield _sse({"type": "RUN_STARTED", "threadId": thread_id, "runId": run_id})
    yield _sse({"type": "TEXT_MESSAGE_START", "messageId": message_id})
    for word in answer.split(" "):
        yield _sse(
            {"type": "TEXT_MESSAGE_CONTENT", "messageId": message_id, "delta": word + " "}
        )
    yield _sse({"type": "TEXT_MESSAGE_END", "messageId": message_id})
    yield _sse(
        {"type": "RUN_FINISHED", "threadId": thread_id, "runId": run_id, "result": answer}
    )
