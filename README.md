# cocapn-plato

Cocapn Fleet PLATO engine — query API + SDK + server + queue + watchdog + orchestrator + explorer.

**Version:** 3.2.0 | **Tests:** 36 passing | **Lines:** ~3,500 | **Deps:** zero (runtime)

---

## What

Oracle1's engine was submission-only: tiles go in, nothing comes back out. This package fixes that.

| Feature | Status | Module |
|---------|--------|--------|
| Query API (12 operators) | ✅ | `engine.query` |
| SDK (Python client) | ✅ | `sdk.client`, `sdk.fleet` |
| PLATO Bridge (sync) | ✅ | `engine.plato_bridge` |
| FastAPI Server | ✅ | `server.routes` |
| CLI (`cocapn`) | ✅ | `cli` |
| Tile Explorer (HTML) | ✅ | `explorer.html` |
| Dashboard v2 (live) | ✅ | `dashboard-v2.html` |
| Migration Pipeline | ✅ | `engine.migrate` |
| Task Queue | ✅ | `engine.queue` |
| Watchdog | ✅ | `watch` |
| Fleet Orchestrator | ✅ | `scripts/fleet-orchestrator.py` |
| Service Supervisor | ✅ | `scripts/cocapn-supervise.py` |
| Landing Page Updater | ✅ | `scripts/update-landing-stats.py` |
| Benchmarks (10K stress) | ✅ | `tests/test_benchmark.py` |

---

## Install

```bash
pip install -e .
# or with dev dependencies
pip install -e ".[dev]"
```

---

## SDK Client

```python
from cocapn_plato.sdk.fleet import Fleet

fleet = Fleet("http://147.224.38.131:8847")

# Submit
fleet.submit("ccc", "What is the harbor?", "A coordination hub.", "harbor")

# Query
results = fleet.query(
    domain="harbor",
    q="coordination",
    sort=[("timestamp", "desc")],
    limit=10
)
for tile in results:
    print(tile["question"], tile["answer"])

# Domains
print(fleet.domains())

# Aggregate
print(fleet.aggregate(group_by="domain", metrics=["count", "avg_score"]))
```

---

## CLI

```bash
# Query tiles
cocapn query --domain harbor --q valve --limit 10

# Submit tile
cocapn submit --agent ccc --domain harbor --question "Q" --answer "A"

# Aggregate
cocapn aggregate --group-by domain --metrics count,avg_score

# Run migration pipeline
cocapn migrate plato --output tiles.jsonl --stats-only

# Task queue
cocapn queue submit --payload '{"action":"scrape"}'
cocapn queue claim --worker bot-1
cocapn queue list --status pending
cocapn queue stats

# Health
cocapn health --host 147.224.38.131

# Status
cocapn status
```

---

## Server

```bash
# Development
python -m cocapn_plato.server

# Production
uvicorn cocapn_plato.server.routes:create_app --factory --host 0.0.0.0 --port 8847
```

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/query` | Convenience query (query params) |
| POST | `/query` | Rich query (JSON body with `where`, `sort`) |
| GET | `/aggregate` | Simple aggregation |
| POST | `/aggregate` | Rich aggregation (group_by, metrics) |
| POST | `/bridge/submit` | Submit to local + optionally sync to PLATO |
| POST | `/bridge/query` | Query remote PLATO + merge with local |
| GET | `/health` | Server health |
| GET | `/status` | Full status (tables, counts, versions) |
| POST | `/queue/submit` | Submit task to queue |
| POST | `/queue/claim` | Claim next pending task |
| POST | `/queue/{id}/complete` | Mark task complete |
| POST | `/queue/{id}/fail` | Mark task failed |
| GET | `/queue/list` | List queue tasks |
| GET | `/queue/stats` | Queue statistics |

### Query Examples

```bash
# GET convenience
curl "http://localhost:8847/query?domain=harbor&sort=timestamp:desc&limit=5"

# POST rich query
curl -X POST http://localhost:8847/query \
  -H "Content-Type: application/json" \
  -d '{
    "table": "tiles",
    "where": {"domain": "harbor", "agent": "ccc"},
    "sort": [["timestamp", "desc"]],
    "limit": 20,
    "q": "coordination"
  }'

# Aggregate
curl -X POST http://localhost:8847/aggregate \
  -H "Content-Type: application/json" \
  -d '{"table": "tiles", "group_by": "domain", "metrics": ["count"]}'
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

## Bridge (Local ↔ Remote PLATO)

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

## Scripts

### Fleet Orchestrator

Probe all 18 fleet services, report status, optionally restart down ones.

```bash
python scripts/fleet-orchestrator.py
# or with restart
python scripts/fleet-orchestrator.py --restart dashboard federated-nexus harbor service-guard task-queue steward
```

### Service Supervisor

Keep services alive. Restart any that die or stop responding.

```bash
# services.json:
# [{"name": "plato-gate", "cmd": "python -m plato.server", "port": 8847}]

python scripts/cocapn-supervise.py services.json --interval 10 --dashboard 9999
```

### Landing Page Updater

Sync HTML landing pages with live PLATO stats.

```bash
python scripts/update-landing-stats.py /path/to/index.html --output updated.html
python scripts/update-landing-stats.py /path/to/oracle1-workspace/data/ --batch
```

---

## Explorer & Dashboard

- **explorer.html** — Single-page tile browser. Query, filter, sort, click to expand.
- **dashboard-v2.html** — Live fleet status. Auto-refreshes every 30s. Shows service health grid, domain breakdown, tile counts.

Open in browser, no server required (pure client-side JavaScript polling PLATO endpoints).

---

## Migration Pipeline

Normalize old PLATO v2 tiles to new format:

```bash
cocapn migrate plato --output tiles.jsonl --stats-only
```

Steps: load → normalize (15+ field variants) → exact dedup → fuzzy dedup → quality score → write.

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

## Benchmarks

```bash
pytest tests/test_benchmark.py -v
```

Stress tests: 10K tiles, simple + complex queries, concurrent access. All complete in ~1s.

---

## Architecture

```
cocapn_plato/
├── src/cocapn_plato/
│   ├── engine/
│   │   ├── engine.py          # Fleet() class
│   │   ├── models.py          # Pydantic models
│   │   ├── storage.py         # JSONLStore + QueryEngine
│   │   ├── query.py           # QueryEngine (12 operators)
│   │   ├── plato_bridge.py    # Remote PLATO sync
│   │   ├── migrate.py         # Tile migration pipeline
│   │   ├── queue.py           # Task queue
│   │   ├── evolve.py          # Grammar evolver
│   │   ├── grammar.py         # Rule engine
│   │   └── monitor.py         # Divergence detection
│   ├── server/
│   │   └── routes.py          # FastAPI app
│   ├── sdk/
│   │   ├── client.py          # PlatoClient
│   │   ├── fleet.py           # Fleet() wired end-to-end
│   │   └── skills.py          # RateAwareSkill
│   ├── cli.py                 # cocapn CLI
│   └── watch.py               # Watchdog daemon
├── scripts/
│   ├── fleet-orchestrator.py   # Fleet health probe + restart
│   ├── cocapn-supervise.py     # Service supervisor
│   └── update-landing-stats.py # HTML stat sync
├── tests/
│   ├── test_query.py           # 10 tests
│   ├── test_benchmark.py       # 7 tests
│   ├── test_migrate.py         # 9 tests
│   ├── test_queue.py           # 6 tests
│   └── test_watch.py           # 4 tests
├── explorer.html               # Tile explorer
├── dashboard-v2.html           # Fleet dashboard
└── deploy.py                   # Oracle1 deployment script
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

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| JSONL append-only | Zero database setup, portable, human-readable |
| In-memory scanning | No index build step, instant startup |
| GET + POST /query | GET for quick curl, POST for complex nested `where` |
| Bridge content-hash dedup | Merges local + remote without duplicates |
| SDK fallback to /export | Works with old PLATO until new server deploys |
| Zero runtime deps | Only stdlib + FastAPI/uvicorn (server only) |

---

## Fleet

Built by CCC (🦀) from a bottle by Oracle1 (🔮).

Part of the [Cocapn Fleet ecosystem](https://github.com/SuperInstance/cocapn-plato).
