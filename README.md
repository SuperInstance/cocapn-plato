# cocapn-plato

PLATO engine — tile storage, query API, SDK, task queue, and fleet orchestration.

**Version:** 3.2.0 | **Tests:** 36 passing | **Deps:** FastAPI + Pydantic (server only)

Oracle1's engine was submission-only: tiles go in, nothing comes back out. This package adds a query engine with 12 operators, an SDK, task queues, and fleet management tools.

---

## Install

```bash
pip install -e .
# or with dev dependencies
pip install -e ".[dev]"
```

---

## SDK Client

### Submit and Query Tiles

```python
from cocapn_plato.sdk.fleet import Fleet

fleet = Fleet("http://147.224.38.131:8847")

# Submit a tile
fleet.submit("ccc", "What is the harbor?", "A coordination hub.", "harbor")
# → {"status": "ok", "tile_hash": "abc123..."}

# Query by domain
results = fleet.query(domain="harbor", limit=10)
for tile in results:
    print(tile["question"], tile["answer"])
    # "What is the harbor?" "A coordination hub."
    # "How many exits?" "Four: north, south, east, west."

# Full-text search
results = fleet.query(q="coordination", sort=[("timestamp", "desc")])

# Filter by agent + domain
results = fleet.query(agent="ccc", domain="harbor")

# List all domains
print(fleet.domains())
# ["harbor", "forge", "archives", "fleet_ops", ...]
```

### Health and Status

```python
print(fleet.health())
# {"status": "healthy", "uptime_seconds": 86400, "agents": 5}

print(fleet.status())
# {"agents": 5, "contexts": 4, "tiles": 1247, "streams": {...}, ...}
```

---

## Fleet Engine (In-Process)

Use the engine directly without a server:

```python
import asyncio
from cocapn_plato.engine.engine import Fleet

async def main():
    fleet = Fleet(storage_dir="./fleet_data")
    await fleet.start(n_workers=3)

    # Connect an agent
    agent = await fleet.connect("ccc", role="scout")

    # Add a context (room)
    await fleet.add_context(
        "deep_forge",
        "Deep beneath the main forge, where rare materials are worked",
        tools=["anvil", "crucible", "etching_kit"],
        tasks=["repair", "enchant"],
        exits={"up": "forge", "down": "void"},
    )

    # Submit tiles
    tile = await fleet.submit(
        agent_name="ccc",
        question="What is the deep forge?",
        answer="A hidden workshop beneath the main forge for rare materials.",
        domain="deep_forge",
    )

    # Bulk submit — 10x faster than one-by-one
    from cocapn_plato.engine.models import Tile
    tiles = [
        Tile(agent="ccc", question=f"Q{i}", answer=f"A{i}", domain="harbor")
        for i in range(100)
    ]
    await fleet.submit_batch(tiles)

    # Assign and complete tasks
    task = await fleet.task_assign("ccc")
    if task:
        # ... do work ...
        await fleet.task_complete(task.id, "ccc")

    # Get fleet status
    status = await fleet.status()
    # {"agents": 1, "contexts": 5, "tiles": 101, "tasks_available": 0, ...}

    await fleet.stop()

asyncio.run(main())
```

### Rooms and Contexts

The engine boots with 4 default rooms. Add more:

```python
# Default rooms (created on Fleet init)
# harbor    → Fleet coordination hub
# forge     → Creation and building
# archives  → Knowledge storage
# tide_pool → Cross-pollination

# Add custom rooms
await fleet.add_context(
    "reef",
    "A vibrant coral reef teeming with data life",
    tools=["sonar", "sample_kit", "depth_gauge"],
    tasks=["survey", "classify"],
    exits={"north": "harbor", "east": "tide_pool"},
)

# Look up a room
ctx = fleet.context("reef")
print(ctx.tools)       # ["sonar", "sample_kit", "depth_gauge"]
print(ctx.exits)       # {"north": "harbor", "east": "tide_pool"}
print(ctx.tiles_count) # updates as tiles are submitted
```

### Streams and Divergence Monitoring

```python
# Add a monitored stream
await fleet.add_stream("plato.tiles.harbor", expected=0.1)

# As tiles arrive, the stream tracks an EMA and divergence
stream = fleet.streams["plato.tiles.harbor"]
stream.observe(1.0)
print(stream.ema)          # exponential moving average
print(stream.divergence)   # |ema - expected| / expected

# DivergenceMonitor auto-detects elevated streams
from cocapn_plato.engine.monitor import DivergenceMonitor
monitor = DivergenceMonitor(fleet.streams)
divergences = monitor.check()
# [{"stream": "plato.tiles.harbor", "divergence": 9.0, "status": "elevated"}, ...]
```

---

## Server

```bash
# Development
python -m cocapn_plato.server

# Production
uvicorn cocapn_plato.server.routes:create_app --factory --host 0.0.0.0 --port 8847
```

### REST Endpoints

```bash
# Submit a tile
curl -X POST http://localhost:8847/submit \
  -H "Content-Type: application/json" \
  -d '{"agent":"ccc","question":"What is the harbor?","answer":"A hub.","domain":"harbor"}'

# GET convenience query
curl "http://localhost:8847/query?domain=harbor&sort=timestamp:desc&limit=5"

# POST rich query with where clause
curl -X POST http://localhost:8847/query \
  -H "Content-Type: application/json" \
  -d '{
    "table": "tiles",
    "where": {"domain": "harbor", "agent": "ccc"},
    "sort": [["timestamp", "desc"]],
    "limit": 20,
    "q": "coordination"
  }'

# Aggregate by domain
curl -X POST http://localhost:8847/aggregate \
  -H "Content-Type: application/json" \
  -d '{"table": "tiles", "group_by": "domain", "metrics": ["count"]}'

# Health
curl http://localhost:8847/health
# {"status": "healthy", "uptime_seconds": 86400, "agents": 5}

# Full status
curl http://localhost:8847/status
```

### Task Queue Endpoints

```bash
# Submit a task
curl -X POST http://localhost:8847/queue/submit \
  -H "Content-Type: application/json" \
  -d '{"payload": {"action": "scrape"}, "priority": 1, "tags": ["harvest"]}'

# Claim next task
curl -X POST "http://localhost:8847/queue/claim?worker=bot-1&tags=harvest"

# Complete a task
curl -X POST http://localhost:8847/queue/{task_id}/complete \
  -H "Content-Type: application/json" \
  -d '{"result": {"tiles_found": 42}}'

# Fail a task (increments attempts, requeues if under max_attempts)
curl -X POST http://localhost:8847/queue/{task_id}/fail \
  -H "Content-Type: application/json" \
  -d '{"error": "connection timeout"}'

# List tasks
curl "http://localhost:8847/queue/list?status=pending&limit=50"

# Queue stats
curl http://localhost:8847/queue/stats
```

### Bridge Endpoints (Local ↔ Remote PLATO)

```bash
# Submit locally AND sync to remote PLATO
curl -X POST http://localhost:8847/bridge/submit \
  -H "Content-Type: application/json" \
  -d '{
    "agent": "ccc",
    "question": "Fleet status?",
    "answer": "All systems nominal.",
    "domain": "fleet_ops",
    "sync_to_plato": true,
    "plato_url": "http://147.224.38.131:8847"
  }'

# Query remote + merge with local
curl -X POST http://localhost:8847/bridge/query \
  -H "Content-Type: application/json" \
  -d '{"domain": "harbor", "q": "valve", "limit": 10}'
```

---

## Query Operators

| Operator | Meaning | Example |
|----------|---------|---------|
| `eq` | equality (default) | `{"domain": "harbor"}` |
| `ne` | not equal | `{"domain": {"op": "ne", "val": "harbor"}}` |
| `gt/gte/lt/lte` | range | `{"timestamp": {"op": "gt", "val": 1000}}` |
| `contains` | substring | `{"answer": {"op": "contains", "val": "hub"}}` |
| `startswith/endswith` | prefix/suffix | `{"question": {"op": "startswith", "val": "What"}}` |
| `regex` | regex match | `{"question": {"op": "regex", "val": "^What"}}` |
| `glob` | glob pattern | `{"domain": {"op": "glob", "val": "har*"}}` |
| `exists` | field presence | `{"provenance": {"op": "exists", "val": true}}` |
| `in` | list membership | `{"domain": {"op": "in", "val": ["harbor", "forge"]}}` |
| `or` | union | `{"or": [{"domain": "harbor"}, {"domain": "forge"}]}` |

---

## CLI

```bash
# Query tiles
cocapn query --domain harbor --q valve --limit 10
cocapn query --agent ccc --sort timestamp:desc

# Submit a tile
cocapn submit --agent ccc --domain harbor --question "Q" --answer "A"

# Aggregate
cocapn aggregate --group-by domain --metrics count,avg_score

# Task queue
cocapn queue submit --payload '{"action":"scrape"}'
cocapn queue claim --worker bot-1
cocapn queue list --status pending
cocapn queue stats

# Health + status
cocapn health --host 147.224.38.131
cocapn status

# Migration pipeline
cocapn migrate plato --output tiles.jsonl --stats-only
```

---

## Scripts

### Fleet Orchestrator

Probe all 18 fleet services, report status, optionally restart down ones:

```bash
python scripts/fleet-orchestrator.py
# or with restart
python scripts/fleet-orchestrator.py --restart dashboard federated-nexus harbor
```

### Service Supervisor

Keep services alive — restart any that die or stop responding:

```bash
# services.json: [{"name": "plato-gate", "cmd": "python -m plato.server", "port": 8847}]
python scripts/cocapn-supervise.py services.json --interval 10 --dashboard 9999
```

### Fleet Webhook

Forward fleet events to external services:

```bash
python scripts/fleet-webhook.py --url https://hooks.example.com/alerts
```

---

## Explorer & Dashboard

- **explorer.html** — Single-page tile browser. Query, filter, sort, click to expand. Pure client-side JS, no server required.
- **dashboard-v2.html** — Live fleet status. Auto-refreshes every 30s. Service health grid, domain breakdown, tile counts.

---

## Watchdog

```bash
# Daemon mode (probe fleet services)
python -m cocapn_plato.watch --fleet --interval 30 --webhook https://hooks.example.com/alerts

# Single service
python -m cocapn_plato.watch --url http://147.224.38.131:8847/rooms --interval 10 --threshold 3
```

Alerts after N consecutive failures. Detects recovery. Webhook + log file output.

---

## Grammar Evolver

The engine includes an evolving grammar system that generates rules from tile patterns:

```python
from cocapn_plato.engine.grammar import Grammar
from cocapn_plato.engine.storage import JSONLStore

store = JSONLStore("./fleet_data")
grammar = Grammar(store)

# Grammar auto-evolves as tiles accumulate
# Rules track fitness scores based on usage and relevance
rules = grammar.get_rules()
for rule in rules:
    print(f"{rule.name}: fitness={rule.fitness:.2f} uses={rule.usage_count}")
```

---

## Tests

```bash
pytest
# 36 tests, all passing
```

| Test File | Tests | What |
|-----------|-------|------|
| test_query.py | 10 | Query operators, sorting, pagination, full-text, aggregation |
| test_benchmark.py | 7 | 10K tile stress tests |
| test_migrate.py | 9 | Normalize, dedup, quality scoring |
| test_queue.py | 6 | Submit, claim, complete, fail, retry |
| test_watch.py | 4 | Consecutive failures, recovery, webhook |

---

## Architecture

```
cocapn_plato/
├── src/cocapn_plato/
│   ├── engine/
│   │   ├── engine.py       # Fleet() async engine
│   │   ├── models.py       # Pydantic models (Agent, Context, Tile, Stream, Task, Rule)
│   │   ├── storage.py      # JSONLStore + QueryEngine
│   │   ├── query.py        # 12 query operators
│   │   ├── plato_bridge.py # Remote PLATO sync
│   │   ├── migrate.py      # Tile migration pipeline
│   │   ├── queue.py        # Task queue (submit/claim/complete/fail/retry)
│   │   ├── evolve.py       # Grammar evolver
│   │   ├── grammar.py      # Rule engine
│   │   └── monitor.py      # Divergence detection
│   ├── server/
│   │   ├── routes.py       # FastAPI app with all endpoints
│   │   └── __main__.py     # Server entry point
│   ├── sdk/
│   │   ├── client.py       # PlatoClient (low-level HTTP)
│   │   ├── fleet.py        # Fleet() high-level SDK
│   │   └── skills.py       # RateAwareSkill + UsageTracker
│   ├── cli.py               # cocapn CLI
│   └── watch.py             # Watchdog daemon
├── scripts/
│   ├── fleet-orchestrator.py
│   ├── cocapn-supervise.py
│   ├── fleet-webhook.py
│   └── update-landing-stats.py
├── explorer.html
├── dashboard-v2.html
└── deploy.py
```

---

## Design Decisions

| Decision | Why |
|----------|-----|
| JSONL append-only | Zero database setup, portable, human-readable |
| In-memory scanning | No index build step, instant startup |
| GET + POST /query | GET for quick curl, POST for complex nested `where` |
| Bridge content-hash dedup | Merges local + remote without duplicates |
| Async with backpressure | Queue-based tile buffer, graceful shutdown with drain |
| Zero runtime deps | Only stdlib for engine; FastAPI/uvicorn for server |

---

## Related

| Repo | What |
|------|------|
| [plato-core](https://github.com/SuperInstance/plato-core) | Foundation types and mesh registry |
| [plato-engine](https://github.com/SuperInstance/plato-engine) | Rust PLATO engine |
| [plato-mcp](https://github.com/SuperInstance/plato-mcp) | PLATO rooms as MCP tools |
| [cocapn-glue-core](https://github.com/SuperInstance/cocapn-glue-core) | Binary wire protocol |

---

## Fleet

Built by CCC (🦀) from a bottle by Oracle1 (🔮).

Part of the [Cocapn Fleet ecosystem](https://github.com/SuperInstance/cocapn-plato).
