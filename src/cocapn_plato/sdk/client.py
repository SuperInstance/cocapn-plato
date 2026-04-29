"""SDK Client — Python consumer for the Cocapn PLATO query API."""
import json
import urllib.request
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class QueryResult:
    """Typed query response."""
    results: List[Dict[str, Any]]
    total: int
    limit: int
    offset: int

    def __len__(self):
        return len(self.results)

    def __iter__(self):
        return iter(self.results)


class PlatoClient:
    """Lightweight Python client for querying PLATO tiles."""

    def __init__(self, base_url: str = "http://localhost:8847", timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, data: Optional[Dict] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

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

    def query(
        self,
        table: str = "tiles",
        where: Optional[Dict[str, Any]] = None,
        sort: Optional[List[tuple]] = None,
        limit: int = 50,
        offset: int = 0,
        q: Optional[str] = None,
    ) -> QueryResult:
        """Query tiles with rich filtering and pagination.
        
        Falls back to /export on older PLATO servers that don't have /query.
        """
        payload = {
            "table": table,
            "limit": min(limit, 500),
            "offset": offset,
        }
        if where:
            payload["where"] = where
        if sort:
            payload["sort"] = sort
        if q:
            payload["q"] = q

        # Try modern /query endpoint
        result = self._request("POST", "/query", payload)
        if "results" in result and result.get("status") != "error":
            return QueryResult(
                results=result["results"],
                total=result.get("total", len(result["results"])),
                limit=result.get("limit", limit),
                offset=result.get("offset", offset),
            )

        # Fallback: old PLATO API via /export
        export = self._request("GET", "/export/plato-tile-spec")
        tiles = export.get("tiles", export if isinstance(export, list) else [])
        if not tiles:
            # Try alternate export format
            tiles = export.get("data", []) if isinstance(export, dict) else []
        
        # Apply filters client-side
        filtered = tiles
        if where:
            for field, spec in where.items():
                if isinstance(spec, dict) and "op" in spec:
                    op, val = spec["op"], spec["val"]
                    if op == "eq":
                        filtered = [t for t in filtered if t.get(field) == val]
                    elif op == "regex":
                        import re
                        filtered = [t for t in filtered if re.search(val, str(t.get(field, "")))]
                    elif op == "contains":
                        filtered = [t for t in filtered if val in str(t.get(field, ""))]
                else:
                    filtered = [t for t in filtered if t.get(field) == spec]
        
        if q:
            q_lower = q.lower()
            filtered = [
                t for t in filtered 
                if q_lower in str(t.get("question", "")).lower() 
                or q_lower in str(t.get("answer", "")).lower()
            ]
        
        # Apply sort client-side
        if sort:
            for field, direction in reversed(sort):
                reverse = direction.lower() == "desc"
                filtered.sort(key=lambda t: t.get(field, 0), reverse=reverse)
        
        total = len(filtered)
        paginated = filtered[offset:offset + limit]
        
        return QueryResult(
            results=paginated,
            total=total,
            limit=limit,
            offset=offset,
        )

    def get_tile(self, domain: str, question: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Fetch a single tile by domain + optional question match."""
        where = {"domain": domain}
        if question:
            where["question"] = {"op": "regex", "val": question}
        result = self.query(where=where, limit=1)
        return result.results[0] if result.results else None

    def list_domains(self) -> List[str]:
        """List all unique tile domains."""
        # Uses aggregate endpoint
        result = self._request("POST", "/aggregate", {"table": "tiles", "group_by": "domain"})
        if isinstance(result, list):
            return [r["_key"] for r in result]
        return []

    def health(self) -> Dict[str, Any]:
        return self._request("GET", "/health")

    def status(self) -> Dict[str, Any]:
        return self._request("GET", "/status")

    def submit(self, agent: str, question: str, answer: str, domain: str = "general") -> Dict[str, Any]:
        """Submit a tile."""
        return self._request("POST", "/submit", {
            "agent": agent,
            "question": question,
            "answer": answer,
            "domain": domain,
        })
