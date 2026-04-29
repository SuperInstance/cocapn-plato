"""PlatoBridge — Connects local Fleet engine to a remote PLATO server.

End-to-end: Fleet() class → PLATO submit → query back.
"""
import json
import asyncio
from typing import Dict, List, Any, Optional
from urllib.parse import urlencode
import urllib.request


class PlatoBridge:
    """Two-way bridge: submit tiles upstream, query tiles back."""

    def __init__(self, plato_url: str = "http://localhost:8847", timeout: float = 10.0):
        self.plato_url = plato_url.rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, data: Optional[Dict] = None) -> Dict[str, Any]:
        """Synchronous HTTP request (for use in async contexts via run_in_executor)."""
        url = f"{self.plato_url}{path}"
        headers = {"Content-Type": "application/json"}

        if data and method in ("POST", "PUT", "PATCH"):
            body = json.dumps(data).encode()
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
        else:
            req = urllib.request.Request(url, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            return {"status": "error", "reason": str(e)}

    async def submit_tile(self, tile: Dict[str, Any]) -> Dict[str, Any]:
        """Submit a single tile to PLATO."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._request, "POST", "/submit", tile)

    async def submit_batch(self, tiles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Submit multiple tiles to PLATO."""
        # PLATO batch endpoint may not exist; fallback to sequential
        loop = asyncio.get_event_loop()
        results = []
        for tile in tiles:
            r = await loop.run_in_executor(None, self._request, "POST", "/submit", tile)
            results.append(r)
        return {"status": "ok", "results": results, "accepted": sum(1 for r in results if r.get("status") == "accepted")}

    async def query_remote(
        self,
        domain: Optional[str] = None,
        agent: Optional[str] = None,
        q: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Query remote PLATO for tiles. Falls back to /export if no query endpoint exists."""
        # Try modern query endpoint first
        params = {k: v for k, v in {
            "domain": domain,
            "agent": agent,
            "q": q,
            "limit": limit,
            "offset": offset,
        }.items() if v is not None}

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, self._request, "GET", f"/query?tiles&{urlencode(params)}"
        )

        if "results" in result:
            return result["results"]

        # Fallback: fetch export and filter locally
        export = await loop.run_in_executor(None, self._request, "GET", "/export/plato-tile-spec")
        tiles = export.get("tiles", export if isinstance(export, list) else [])

        if domain:
            tiles = [t for t in tiles if t.get("domain") == domain]
        if agent:
            tiles = [t for t in tiles if t.get("agent") == agent]
        if q:
            q_lower = q.lower()
            tiles = [t for t in tiles if q_lower in t.get("question", "").lower() or q_lower in t.get("answer", "").lower()]

        return tiles[offset:offset + limit]

    async def health(self) -> Dict[str, Any]:
        """Check remote PLATO health."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._request, "GET", "/health")

    async def status(self) -> Dict[str, Any]:
        """Get remote PLATO status."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._request, "GET", "/status")
