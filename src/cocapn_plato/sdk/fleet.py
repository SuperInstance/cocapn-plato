"""SDK Fleet — Fleet() class wired end-to-end to PLATO."""
from typing import Dict, List, Optional, Any
from .client import PlatoClient


class Fleet:
    """End-to-end Fleet: local cache + remote PLATO.
    
    Usage:
        fleet = Fleet(plato_url="http://147.224.38.131:8847")
        fleet.submit("ccc", "What is the harbor?", "A coordination hub", "harbor")
        results = fleet.query(domain="harbor", sort=[("timestamp", "desc")], limit=10)
    """

    def __init__(self, plato_url: str = "http://localhost:8847", timeout: float = 10.0):
        self.client = PlatoClient(plato_url, timeout)
        self._local_cache: List[Dict[str, Any]] = []

    def submit(self, agent: str, question: str, answer: str, domain: str = "general") -> Dict[str, Any]:
        """Submit a tile to PLATO."""
        result = self.client.submit(agent, question, answer, domain)
        # Also keep local copy
        self._local_cache.append({
            "agent": agent,
            "question": question,
            "answer": answer,
            "domain": domain,
        })
        return result

    def query(
        self,
        domain: Optional[str] = None,
        agent: Optional[str] = None,
        q: Optional[str] = None,
        where: Optional[Dict[str, Any]] = None,
        sort: Optional[List[tuple]] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Query tiles from PLATO."""
        # Build where clause from convenience args
        w = dict(where) if where else {}
        if domain:
            w["domain"] = domain
        if agent:
            w["agent"] = agent

        result = self.client.query(
            table="tiles",
            where=w if w else None,
            sort=sort,
            limit=limit,
            offset=offset,
            q=q,
        )
        return result.results

    def domains(self) -> List[str]:
        """List all tile domains."""
        return self.client.list_domains()

    def health(self) -> Dict[str, Any]:
        """Check PLATO health."""
        return self.client.health()

    def status(self) -> Dict[str, Any]:
        """Get PLATO fleet status."""
        return self.client.status()
