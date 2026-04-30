"""Server routes — FastAPI app with query, aggregate, and bridge endpoints."""
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query as QueryParam
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Tuple
import asyncio
import json
import time

# Import from monorepo
from ..engine.engine import Fleet
from ..engine.plato_bridge import PlatoBridge


# Singletons
_fleet: Optional[Fleet] = None
_bridge: Optional[PlatoBridge] = None


def get_fleet() -> Fleet:
    global _fleet
    if _fleet is None:
        _fleet = Fleet()
    return _fleet


def get_bridge(url: Optional[str] = None) -> Optional[PlatoBridge]:
    global _bridge
    if url and _bridge is None:
        _bridge = PlatoBridge(url)
    return _bridge


# --- Request Models ---

class QueryBody(BaseModel):
    table: str = "tiles"
    where: Optional[Dict[str, Any]] = None
    sort: Optional[List[Tuple[str, str]]] = None
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)
    q: Optional[str] = None
    q_fields: Optional[List[str]] = None


class AggregateBody(BaseModel):
    table: str = "tiles"
    group_by: str
    metrics: Optional[List[str]] = None
    where: Optional[Dict[str, Any]] = None


class BridgeSubmitBody(BaseModel):
    agent: str
    question: str
    answer: str
    domain: str = "general"
    sync_to_plato: bool = False
    plato_url: Optional[str] = None


class BridgeQueryBody(BaseModel):
    domain: Optional[str] = None
    agent: Optional[str] = None
    q: Optional[str] = None
    limit: int = 50
    offset: int = 0


# --- Route Factory ---

def create_app(fleet_instance: Optional[Fleet] = None) -> FastAPI:
    """Factory for creating the FastAPI app with all routes."""
    global _fleet
    if fleet_instance:
        _fleet = fleet_instance

    app = FastAPI(title="Cocapn Plato", version="3.2", description="Query API + PLATO Bridge")
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    @app.on_event("startup")
    async def startup():
        fleet = get_fleet()
        await fleet.start(n_workers=3)

    @app.on_event("shutdown")
    async def shutdown():
        fleet = get_fleet()
        await fleet.stop()

    # --- Query Endpoints ---

    @app.post("/query")
    async def query(body: QueryBody):
        """Rich query with filtering, sorting, pagination, full-text search."""
        fleet = get_fleet()
        result = await fleet.storage.query_rich(
            table=body.table,
            where=body.where,
            sort=body.sort,
            limit=body.limit,
            offset=body.offset,
            q=body.q,
            q_fields=body.q_fields,
        )
        return result

    @app.get("/query")
    async def query_get(
        table: str = "tiles",
        domain: Optional[str] = None,
        agent: Optional[str] = None,
        q: Optional[str] = None,
        sort: Optional[str] = QueryParam(None, description="field:direction, e.g. timestamp:desc"),
        limit: int = QueryParam(50, ge=1, le=500),
        offset: int = QueryParam(0, ge=0),
    ):
        """GET convenience for simple queries."""
        fleet = get_fleet()
        where = {}
        if domain:
            where["domain"] = domain
        if agent:
            where["agent"] = agent

        sort_parsed = None
        if sort:
            parts = sort.split(":")
            if len(parts) == 2:
                sort_parsed = [(parts[0], parts[1])]

        result = await fleet.storage.query_rich(
            table=table,
            where=where if where else None,
            sort=sort_parsed,
            limit=limit,
            offset=offset,
            q=q,
        )
        return result

    @app.post("/aggregate")
    async def aggregate(body: AggregateBody):
        """Aggregate: GROUP BY with COUNT/SUM/AVG."""
        fleet = get_fleet()
        result = await fleet.storage.aggregate(
            table=body.table,
            group_by=body.group_by,
            metrics=body.metrics,
            where=body.where,
        )
        return result

    @app.get("/aggregate")
    async def aggregate_get(
        table: str = "tiles",
        group_by: str = "domain",
        metric: Optional[str] = None,
    ):
        """GET convenience for aggregation."""
        fleet = get_fleet()
        metrics = [metric] if metric else None
        result = await fleet.storage.aggregate(
            table=table,
            group_by=group_by,
            metrics=metrics,
        )
        return result

    # --- Bridge Endpoints ---

    @app.post("/bridge/submit")
    async def bridge_submit(body: BridgeSubmitBody):
        """Submit a tile locally AND optionally sync to remote PLATO."""
        fleet = get_fleet()
        tile = await fleet.submit(body.agent, body.question, body.answer, body.domain)

        result = {"local": {"status": "accepted", "timestamp": tile.timestamp}}

        if body.sync_to_plato:
            bridge = get_bridge(body.plato_url)
            if bridge:
                remote = await bridge.submit_tile({
                    "agent": body.agent,
                    "question": body.question,
                    "answer": body.answer,
                    "domain": body.domain,
                })
                result["remote"] = remote
            else:
                result["remote"] = {"status": "error", "reason": "no bridge configured"}

        return result

    @app.post("/bridge/query")
    async def bridge_query(body: BridgeQueryBody):
        """Query remote PLATO and merge with local results."""
        fleet = get_fleet()
        bridge = get_bridge()

        # Local query
        where = {}
        if body.domain:
            where["domain"] = body.domain
        if body.agent:
            where["agent"] = body.agent

        local = await fleet.storage.query_rich(
            table="tiles",
            where=where if where else None,
            limit=body.limit,
            offset=body.offset,
            q=body.q,
        )

        # Remote query
        remote = {"results": []}
        if bridge:
            remote_results = await bridge.query_remote(
                domain=body.domain,
                agent=body.agent,
                q=body.q,
                limit=body.limit,
                offset=body.offset,
            )
            remote = {"results": remote_results}

        # Merge: local first, then remote not in local
        seen = {json.dumps(r, sort_keys=True) for r in local["results"]}
        merged = list(local["results"])
        for r in remote.get("results", []):
            key = json.dumps(r, sort_keys=True)
            if key not in seen:
                merged.append(r)
                seen.add(key)

        return {
            "results": merged[:body.limit],
            "local_count": len(local["results"]),
            "remote_count": len(remote.get("results", [])),
            "merged_count": len(merged),
        }

    # --- Health & Status ---

    @app.get("/health")
    async def health():
        fleet = get_fleet()
        uptime = time.time() - fleet._started_at
        return {
            "status": "healthy",
            "uptime_seconds": uptime,
            "agents": len(fleet.agents),
            "tiles_buffered": fleet._tile_buffer.qsize(),
            "query_api": "v3.2",
        }

    @app.get("/status")
    async def status():
        fleet = get_fleet()
        return await fleet.status()

    # --- Task Queue ---
    from ..engine.queue import TaskQueue

    _queue: Optional[TaskQueue] = None

    def get_queue() -> TaskQueue:
        global _queue
        if _queue is None:
            _queue = TaskQueue("./fleet_data/tasks.jsonl")
        return _queue

    class TaskSubmit(BaseModel):
        payload: Dict[str, Any]
        priority: int = 0
        tags: List[str] = []
        max_attempts: int = 3

    class TaskComplete(BaseModel):
        result: Optional[Dict[str, Any]] = None
        error: Optional[str] = None

    @app.post("/queue/submit")
    async def queue_submit(body: TaskSubmit):
        queue = get_queue()
        task = queue.submit(body.payload, priority=body.priority, tags=body.tags, max_attempts=body.max_attempts)
        return {"task": task.to_dict()}

    @app.post("/queue/claim")
    async def queue_claim(worker: str = "anonymous", tags: Optional[str] = None):
        queue = get_queue()
        tag_list = tags.split(",") if tags else None
        task = queue.claim(worker=worker, tags=tag_list)
        if task is None:
            raise HTTPException(status_code=404, detail="No tasks available")
        return {"task": task.to_dict()}

    @app.post("/queue/{task_id}/complete")
    async def queue_complete(task_id: str, body: TaskComplete):
        queue = get_queue()
        task = queue.complete(task_id, body.result)
        if task is None:
            raise HTTPException(status_code=404, detail="Task not found")
        return {"task": task.to_dict()}

    @app.post("/queue/{task_id}/fail")
    async def queue_fail(task_id: str, body: TaskComplete):
        queue = get_queue()
        task = queue.fail(task_id, body.error or "")
        if task is None:
            raise HTTPException(status_code=404, detail="Task not found")
        return {"task": task.to_dict()}

    @app.get("/queue/list")
    async def queue_list(status: Optional[str] = None, limit: int = 50):
        queue = get_queue()
        tasks = queue.list(status=status, limit=limit)
        return {"tasks": [t.to_dict() for t in tasks], "count": len(tasks)}

    @app.get("/queue/stats")
    async def queue_stats():
        queue = get_queue()
        return queue.stats()

    return app
