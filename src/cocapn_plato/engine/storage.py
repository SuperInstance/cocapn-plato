"""Enhanced JSONLStore with QueryEngine integration."""
import json
import os
import asyncio
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime
from .query import QueryEngine


class JSONLStore:
    """Async-aware append-only JSONL storage with in-memory indexing + rich querying."""

    def __init__(self, dir: str, index_fields: Dict[str, List[str]] = None):
        self.dir = Path(dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self._index_fields = index_fields or {}
        self._indexes: Dict[str, Dict[str, List[int]]] = {}
        self._line_offsets: Dict[str, List[int]] = {}
        self._lock = asyncio.Lock()
        self._load_existing()
        self.query_engine = QueryEngine(dir, self._index_fields)

    def _path(self, table: str) -> Path:
        return self.dir / f"{table}.jsonl"

    def _load_existing(self):
        """Build in-memory indexes from existing files on startup."""
        for table in self._index_fields:
            self._indexes[table] = {field: {} for field in self._index_fields[table]}
            self._line_offsets[table] = []
            path = self._path(table)
            if not path.exists():
                continue
            offset = 0
            with open(path, 'rb') as f:
                for line in f:
                    self._line_offsets[table].append(offset)
                    offset += len(line)
                    if not line.strip():
                        continue
                    try:
                        rec = json.loads(line)
                        for field in self._index_fields[table]:
                            val = str(rec.get(field, ''))
                            if val not in self._indexes[table][field]:
                                self._indexes[table][field][val] = []
                            self._indexes[table][field][val].append(len(self._line_offsets[table]) - 1)
                    except json.JSONDecodeError:
                        pass

    async def append(self, table: str, record: Dict[str, Any]):
        async with self._lock:
            line = json.dumps(record, default=str) + "\n"
            path = self._path(table)
            
            if table not in self._line_offsets:
                self._line_offsets[table] = []
                if table in self._index_fields:
                    self._indexes[table] = {f: {} for f in self._index_fields[table]}
            
            offset = path.stat().st_size if path.exists() else 0
            line_idx = len(self._line_offsets[table])
            self._line_offsets[table].append(offset)
            
            if table in self._index_fields:
                for field in self._index_fields[table]:
                    val = str(record.get(field, ''))
                    if val not in self._indexes[table][field]:
                        self._indexes[table][field][val] = []
                    self._indexes[table][field][val].append(line_idx)
            
            with open(path, "a") as f:
                f.write(line)

    async def query(self, table: str, **filters) -> List[Dict[str, Any]]:
        """Legacy equality-only query."""
        path = self._path(table)
        if not path.exists():
            return []
        
        if table in self._indexes and filters:
            indexed_field = None
            for field in filters:
                if field in self._indexes[table]:
                    indexed_field = field
                    break
            
            if indexed_field:
                val = str(filters[indexed_field])
                line_indices = self._indexes[table][indexed_field].get(val, [])
                results = []
                with open(path, 'rb') as f:
                    for idx in line_indices:
                        if idx < len(self._line_offsets[table]):
                            f.seek(self._line_offsets[table][idx])
                            line = f.readline()
                            rec = json.loads(line)
                            if all(rec.get(k) == v for k, v in filters.items()):
                                results.append(rec)
                return results
        
        results = []
        with open(path) as f:
            for line in f:
                if not line.strip():
                    continue
                rec = json.loads(line)
                if all(rec.get(k) == v for k, v in filters.items()):
                    results.append(rec)
        return results

    async def query_rich(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        sort: Optional[List[tuple]] = None,
        limit: int = 50,
        offset: int = 0,
        q: Optional[str] = None,
        q_fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Rich query with filtering, sorting, pagination, full-text search."""
        return self.query_engine.query(table, where, sort, limit, offset, q, q_fields)

    async def aggregate(
        self,
        table: str,
        group_by: str,
        metrics: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Aggregate query: GROUP BY with COUNT/SUM/AVG."""
        return self.query_engine.aggregate(table, group_by, metrics, where)

    async def all(self, table: str) -> List[Dict[str, Any]]:
        path = self._path(table)
        if not path.exists():
            return []
        with open(path) as f:
            return [json.loads(line) for line in f if line.strip()]

    async def count(self, table: str) -> int:
        return len(self._line_offsets.get(table, []))

    def tables(self) -> List[str]:
        return [p.stem for p in self.dir.glob("*.jsonl")]
