"""QueryEngine — Rich querying for JSONLStore with filtering, sorting, pagination, full-text search.

Maximum capability in minimum lines. No external dependencies beyond stdlib + asyncio.
"""
import json
import re
import fnmatch
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime
from pathlib import Path


class QueryEngine:
    """Lightweight query engine over JSONL files."""

    OPERATORS = {
        "eq": lambda rec, k, v: rec.get(k) == v,
        "ne": lambda rec, k, v: rec.get(k) != v,
        "gt": lambda rec, k, v: rec.get(k) is not None and rec.get(k) > v,
        "gte": lambda rec, k, v: rec.get(k) is not None and rec.get(k) >= v,
        "lt": lambda rec, k, v: rec.get(k) is not None and rec.get(k) < v,
        "lte": lambda rec, k, v: rec.get(k) is not None and rec.get(k) <= v,
        "contains": lambda rec, k, v: v in str(rec.get(k, "")),
        "startswith": lambda rec, k, v: str(rec.get(k, "")).startswith(v),
        "endswith": lambda rec, k, v: str(rec.get(k, "")).endswith(v),
        "regex": lambda rec, k, v: bool(re.search(v, str(rec.get(k, "")))),
        "glob": lambda rec, k, v: fnmatch.fnmatch(str(rec.get(k, "")), v),
        "exists": lambda rec, k, v: (k in rec) == v,
        "in": lambda rec, k, v: rec.get(k) in v if isinstance(v, (list, tuple, set)) else False,
    }

    def __init__(self, store_dir: str, indexes: Dict[str, List[str]] = None):
        self.dir = Path(store_dir)
        self.indexes = indexes or {}

    def _path(self, table: str) -> Path:
        return self.dir / f"{table}.jsonl"

    def _scan(self, table: str, predicate: Callable[[Dict], bool]) -> List[Dict]:
        """Iterate a JSONL file, yielding records that match predicate."""
        path = self._path(table)
        if not path.exists():
            return []
        results = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                    if predicate(rec):
                        results.append(rec)
                except json.JSONDecodeError:
                    continue
        return results

    def _compile_where(self, where: Optional[Dict[str, Any]]) -> Callable[[Dict], bool]:
        """Compile a where clause into a predicate function.

        Where clause formats:
          Simple equality: {"domain": "harbor"}
          Operator: {"domain": {"op": "regex", "val": "harb.*"}}
          AND (implicit): {"domain": "harbor", "agent": "ccc"}
          OR: {"or": [{"domain": "harbor"}, {"domain": "forge"}]}
        """
        if not where:
            return lambda rec: True

        # Top-level OR
        if "or" in where:
            preds = [self._compile_clause(c) for c in where["or"]]
            return lambda rec: any(p(rec) for p in preds)

        # Top-level AND (implicit)
        preds = [self._compile_clause({k: v}) for k, v in where.items()]
        return lambda rec: all(p(rec) for p in preds)

    def _compile_clause(self, clause: Dict) -> Callable[[Dict], bool]:
        """Compile a single field clause."""
        field, spec = next(iter(clause.items()))

        if isinstance(spec, dict) and "op" in spec:
            op_name = spec["op"]
            val = spec["val"]
            op = self.OPERATORS.get(op_name)
            if not op:
                raise ValueError(f"Unknown operator: {op_name}")
            return lambda rec: op(rec, field, val)

        # Simple equality fallback
        return lambda rec: rec.get(field) == spec

    def query(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        sort: Optional[List[tuple]] = None,
        limit: int = 50,
        offset: int = 0,
        q: Optional[str] = None,
        q_fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Execute a rich query against a JSONL table.

        Args:
            table: Name of the JSONL file (without extension)
            where: Filter clause (see _compile_where)
            sort: List of (field, direction) tuples, e.g., [("timestamp", "desc")]
            limit: Max records to return (default 50, capped at 500)
            offset: Pagination offset
            q: Full-text search string (OR-matched across q_fields)
            q_fields: Fields to search for full-text. Defaults to all string fields.

        Returns:
            {"results": [...], "total": N, "limit": limit, "offset": offset}
        """
        limit = min(limit, 500)

        # Build base predicate
        predicate = self._compile_where(where)

        # Add full-text predicate if q provided
        if q:
            q_lower = q.lower()
            search_fields = q_fields or []  # auto-detect if empty

            def ft_predicate(rec: Dict) -> bool:
                if not predicate(rec):
                    return False
                fields = search_fields if search_fields else [k for k, v in rec.items() if isinstance(v, str)]
                return any(q_lower in str(rec.get(f, "")).lower() for f in fields)

            results = self._scan(table, ft_predicate)
        else:
            results = self._scan(table, predicate)

        total = len(results)

        # Sort
        if sort:
            def sort_key(rec):
                keys = []
                for field, direction in sort:
                    val = rec.get(field)
                    if val is None:
                        # None sorts last regardless of direction
                        keys.append((1, 0))
                    else:
                        keys.append((0, val))
                return keys

            reverse = any(d.lower() == "desc" for _, d in sort)
            results.sort(key=sort_key, reverse=reverse)

        # Paginate
        paginated = results[offset:offset + limit]

        return {
            "results": paginated,
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    def aggregate(
        self,
        table: str,
        group_by: str,
        metrics: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Simple aggregation: GROUP BY field with COUNT, and optional SUM/AVG metrics."""
        results = self._scan(table, self._compile_where(where))
        metrics = metrics or ["count"]

        groups: Dict[str, Dict[str, Any]] = {}
        for rec in results:
            key = str(rec.get(group_by, "_null"))
            if key not in groups:
                groups[key] = {"_key": key, "count": 0}
                for m in metrics:
                    if m.startswith("sum:"):
                        groups[key][m] = 0.0
                    elif m.startswith("avg:"):
                        groups[key][m] = {"_sum": 0.0, "_n": 0}

            groups[key]["count"] += 1

            for m in metrics:
                if m.startswith("sum:"):
                    field = m.split(":", 1)[1]
                    groups[key][m] += float(rec.get(field, 0) or 0)
                elif m.startswith("avg:"):
                    field = m.split(":", 1)[1]
                    groups[key][m]["_sum"] += float(rec.get(field, 0) or 0)
                    groups[key][m]["_n"] += 1

        # Flatten avg metrics
        out = []
        for g in groups.values():
            row = dict(g)
            for m in metrics:
                if m.startswith("avg:"):
                    data = row.pop(m)
                    row[m] = round(data["_sum"] / data["_n"], 3) if data["_n"] else 0
            out.append(row)

        out.sort(key=lambda x: x["count"], reverse=True)
        return out
