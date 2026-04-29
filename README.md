# cocapn-plato

Cocapn Fleet PLATO engine — query API + SDK + bridge.

## What

Oracle1's engine was submission-only: tiles go in, nothing comes back out. This package fixes that.

- **QueryEngine**: Rich querying with filtering, sorting, pagination, full-text search, aggregation
- **PlatoBridge**: Two-way sync between local Fleet() and remote PLATO servers
- **SDK**: Python client — `Fleet().query(domain="harbor", q="valve", sort="timestamp:desc")`
- **Server**: FastAPI with `/query`, `/aggregate`, `/bridge/submit`, `/bridge/query`

## Install

```bash
pip install -e .
# or with dev dependencies
pip install -e ".[dev]"
```

## Usage

### SDK Client

```python
from cocapn_plato.sdk.fleet import Fleet

fleet = Fleet("http://147.224.38.131:8847")

# Submit
fleet.submit("ccc", "What is the harbor?", "A coordination hub.", "harbor")

# Query
results = fleet.query(domain="harbor", q="coordination", sort=[("timestamp", "desc")], limit=10)
for tile in results:
    print(tile["question"], tile["answer"])

# Domains
print(fleet.domains())  # ['harbor', 'forge', 'archives', ...]
```

### Server

```bash
python -m cocapn_plato.server
# or
uvicorn cocapn_plato.server.routes:create_app --factory --host 0.0.0.0 --port 8847
```

### Query API

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

### Bridge (Local ↔ Remote PLATO)

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

## Architecture

```
cocapn_plato/
├── engine/
│   ├── engine.py          # Fleet() class (async, backpressure, task queues)
│   ├── models.py          # Pydantic models
│   ├── storage.py         # JSONLStore + QueryEngine
│   ├── query.py           # Rich query engine
│   ├── plato_bridge.py    # Remote PLATO sync
│   ├── evolve.py          # Grammar evolver
│   ├── grammar.py         # Rule engine
│   └── monitor.py         # Divergence detection
├── server/
│   └── routes.py          # FastAPI endpoints
└── sdk/
    ├── client.py          # PlatoClient
    ├── fleet.py           # Fleet() wired end-to-end
    └── skills.py          # RateAwareSkill base class
```

## Tests

```bash
pytest
```

## Version

3.2.0 — Query API release.

## Fleet

Built by CCC (🦀) from a bottle by Oracle1 (🔮).
