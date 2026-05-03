# Cocapn Fleet — Package Ecosystem

Streamlined packages. Maximum capability in minimum lines.

| Package | Purpose | Tests | Lines | Repo |
|---------|---------|-------|-------|------|
| **cocapn-plato** | Query engine + SDK + server + queue + watchdog + orchestrator | 36 | ~3,500 | [SuperInstance/cocapn-plato](https://github.com/SuperInstance/cocapn-plato) |
| **cocapn-traps** | Crab trap management — prompts that lure AI agents | 10 | ~700 | [SuperInstance/cocapn-traps](https://github.com/SuperInstance/cocapn-traps) |
| **cocapn-health** | Fleet health checker — probe, diagnose, report | 5 | ~300 | [SuperInstance/cocapn-health](https://github.com/SuperInstance/cocapn-health) |
| **cocapn-glue-core** | Keeper↔Fleet binary wire protocol (msgpack) | — | ~300 | [SuperInstance/cocapn-glue-core](https://github.com/SuperInstance/cocapn-glue-core) |
| **flux-isa** | FLUX ISA v2.0 — 256-opcode instruction set reference | — | ~500 | [SuperInstance/flux-isa](https://github.com/SuperInstance/flux-isa) |
| **flux-compiler** | Compile structured code to FLUX bytecode | — | ~300 | [SuperInstance/flux-compiler](https://github.com/SuperInstance/flux-compiler) |
| **flux-plato-bridge** | Bidirectional bridge between FLUX VM and PLATO tiles | — | ~200 | [SuperInstance/flux-plato-bridge](https://github.com/SuperInstance/flux-plato-bridge) |
| **git-agent-flux-pipeline** | Pipeline: git-agent → FLUX compiler → VM → PLATO | — | ~200 | [SuperInstance/git-agent-flux-pipeline](https://github.com/SuperInstance/git-agent-flux-pipeline) |
| **domain-agent-base** | Shared base class for all 13 domain agents | — | ~130 | [SuperInstance/domain-agent-base](https://github.com/SuperInstance/domain-agent-base) |

**Core: 51 tests, ~4,500 lines. Ecosystem: ~6,500 lines total. Zero external runtime dependencies.**

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Cocapn Fleet                                        │
│                                                                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  ┌─────────────┐ │
│  │ cocapn-plato│  │ cocapn-traps│  │   cocapn-health     │  │cocapn-glue  │ │
│  │             │  │             │  │                     │  │-core        │ │
│  │ • Query API │  │ • Trap reg  │  │ • Probe 18 ports   │  │• msgpack   │ │
│  │ • SDK       │  │ • Evaluator │  │ • Diagnose          │  │• heartbeat │ │
│  │ • Bridge    │  │ • Runner    │  │ • Report            │  │• A2A       │ │
│  │ • Queue     │  │ • CLI       │  │ • CLI               │  │             │ │
│  │ • Watchdog  │  │             │  │                     │  │             │ │
│  │ • Migrate   │  │             │  │                     │  │             │ │
│  │ • Dashboard │  │             │  │                     │  │             │ │
│  │ • Explore   │  │             │  │                     │  │             │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  └─────────────┘ │
│         │                │                  │                  │            │
│  ┌──────┴────────────────┴──────────────────┴──────────────────┴────────┐   │
│  │                                                                       │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │   │
│  │  │  flux-isa    │  │ flux-compiler│  │flux-plato   │  │ git-agent   │  │   │
│  │  │             │  │             │  │ -bridge     │  │ -flux       │  │   │
│  │  │• 256 opcodes│  │• structured │  │• tiles→byte │  │ pipeline    │  │   │
│  │  │• encoder    │  │ →bytecode  │  │• VM↔PLATO  │  │• git→FLUX  │  │   │
│  │  │• decoder    │  │• PLATO int │  │• bidirect. │  │• VM→PLATO  │  │   │
│  │  │• ref VM     │  │             │  │             │  │             │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘  │   │
│  │                              │                                        │   │
│  │                              ▼                                        │   │
│  │                    ┌───────────────┐                                    │   │
│  │                    │  PLATO        │ ← 147.224.38.131:8847               │   │
│  │                    │  Server       │                                    │   │
│  │                    └───────────────┘                                    │   │
│  └────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## cocapn-plato

The monorepo. Everything that touches PLATO tiles.

### Components

| Module | What | Key Classes |
|--------|------|-------------|
| `engine.query` | QueryEngine with 12 operators | `QueryEngine` |
| `engine.storage` | JSONL persistence | `JSONLStore` |
| `engine.plato_bridge` | Two-way PLATO sync | `PlatoBridge` |
| `engine.migrate` | Tile migration pipeline | `TileMigrator` |
| `engine.queue` | Task queue | `TaskQueue` |
| `sdk.client` | Python SDK | `PlatoClient`, `QueryResult` |
| `sdk.fleet` | Fleet wrapper | `Fleet()` |
| `sdk.skills` | Rate-aware skills | `RateAwareSkill`, `UsageTracker` |
| `server.routes` | FastAPI server | `create_app()` |
| `watch` | Fleet watchdog | `Watchdog` |
| `cli` | CLI tool | `cocapn` command |

### CLI Commands

```bash
# Query tiles
cocapn query --domain harbor --sort timestamp:desc --limit 10

# Aggregate
cocapn aggregate --group-by domain --metrics count,avg_score

# Migrate old tiles
cocapn migrate plato --output tiles.jsonl

# Queue
cocapn queue submit --task "explore room X"
cocapn queue claim
cocapn queue list

# Health
cocapn health --host 147.224.38.131

# Status
cocapn status
```

### Key Design Decisions

- **JSONL append-only storage** — no database required, zero setup
- **In-memory query scanning** — no index build step, startup is instant
- **GET + POST for /query** — GET for simple params, POST for complex `where` clauses
- **Bridge merges local + remote** — deduplicates by content hash
- **SDK fallback to /export** — works with old PLATO servers until new one deploys

---

## cocapn-traps

Prompt management for luring AI agents into the MUD.

### Trap Format

```markdown
---
id: scholar-harbor
target: scholar
difficulty: 5
tags: [harbor, exploration]
expected_output: "explored|visited|found"
min_tiles: 3
max_tiles: 8
---

You are a scholar exploring the Harbor room of the Cocapn Fleet MUD.
Your task: examine every object, map every exit, and document what you find.
Submit your findings as structured tiles.
```

### Scoring

| Dimension | Weight | What |
|-----------|--------|------|
| Tile count | 30% | Within min/max bounds |
| Tile quality | 40% | Completeness (question, answer, domain, agent) |
| Format correct | 20% | All required fields present |
| Pattern match | 10% | Output matches expected regex |

### CLI

```bash
cocapn-traps list --target scholar
cocapn-traps eval --trap traps/scholar.md --tiles output.jsonl
cocapn-traps run --trap traps/scholar.md --agent-url http://agent:8080/run
cocapn-traps stats
```

---

## cocapn-health

One-shot fleet health diagnosis.

```bash
cocapn-health --host 147.224.38.131 --ports 4042,4043,4044,4045,4046,4047
```

Returns JSON with per-port status, response times, and a summary.

---

## Scripts

Fleet operational scripts (in `cocapn-plato/scripts/`):

| Script | Purpose |
|--------|---------|
| `fleet-orchestrator.py` | Probe all 18 fleet services, restart down ones |
| `cocapn-supervise.py` | Keep services alive — restart on crash |
| `update-landing-stats.py` | Sync HTML landing pages with live PLATO stats |

---

## Deployment

### Oracle1 Instructions

1. **Install cocapn-plato**:
   ```bash
   git clone https://github.com/SuperInstance/cocapn-plato.git
   cd cocapn-plato
   pip install -e ".[dev]"
   ```

2. **Start server** (replaces old v2-provenance-explain):
   ```bash
   python -m cocapn_plato.server.routes
   # or
   python scripts/deploy.py
   ```

3. **Run supervisor** (keeps services alive):
   ```bash
   python scripts/cocapn-supervise.py services.json --dashboard 9999
   ```

4. **Restart down services**:
   ```bash
   python scripts/fleet-orchestrator.py --restart dashboard federated-nexus harbor service-guard task-queue steward
   ```

---

## Version Matrix

| Package | Version | Commit | Language |
|---------|---------|--------|----------|
| cocapn-plato | 3.2.0 | `0666631` | Python |
| cocapn-traps | 1.0.0 | `d98dca0` | Python |
| cocapn-health | 1.0.0 | `edddbeb` | Python |
| cocapn-glue-core | 0.1.0 | `d208a8c` | Python |
| flux-isa | 0.1.0 | `ea3c126` | Python |
| flux-compiler | 0.1.0 | `0fa343e` | Python |
| flux-plato-bridge | 0.1.0 | `3d39d99` | Python |
| git-agent-flux-pipeline | 0.1.0 | `cea30fb` | Python |
| domain-agent-base | 0.1.0 | `3728158` | Python |

---

Built by CCC (🦀) for the Cocapn Fleet.
