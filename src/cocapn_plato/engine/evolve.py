import time
import asyncio
from typing import Dict, List
from .storage import JSONLStore
from .models import Context, Stream


class Evolver:
    """Async evolution triggered by tile thresholds."""

    THRESHOLD = 10
    ADVANCED_THRESHOLD = 20

    def __init__(self, storage: JSONLStore, contexts: Dict[str, Context], streams: Dict[str, Stream]):
        self.storage = storage
        self.contexts = contexts
        self.streams = streams
        self._evolved: set = set()
        self._lock = asyncio.Lock()

    async def maybe_evolve(self, domain: str, buffered_count: int = 0):
        async with self._lock:
            tiles = await self.storage.query("tiles", domain=domain)
            count = len(tiles) + buffered_count
            
            if count < self.THRESHOLD:
                return
            
            # Phase 1: Generate tasks (only once per threshold)
            if f"{domain}_tasks" not in self._evolved:
                self._evolved.add(f"{domain}_tasks")
                topics = set()
                for t in tiles[:50]:  # Sample first 50 for speed
                    q = t.get("question", "")
                    words = q.split()[:3]
                    if words:
                        topics.add(" ".join(words).lower())
                
                for i, topic in enumerate(topics):
                    if i >= 3:
                        break
                    task_id = f"{domain}_auto_{topic.replace(' ', '_')}_{int(time.time())}"
                    await self.storage.append("tasks", {
                        "id": task_id,
                        "target": domain,
                        "description": f"Deep-dive: {topic} in {domain}",
                        "created_at": time.time(),
                        "auto": True,
                        "priority": 1,
                        "completed": False,
                    })

            # Phase 2: Advanced context (at 2x threshold)
            if count >= self.ADVANCED_THRESHOLD and f"{domain}_advanced" not in self._evolved:
                self._evolved.add(f"{domain}_advanced")
                new_id = f"{domain}_advanced"
                if new_id not in self.contexts:
                    self.contexts[new_id] = Context(
                        id=new_id,
                        description=f"Advanced topics in {domain}",
                        tools=["deep_dive", "synthesis", "cross_reference"],
                        tasks=[f"master_{domain}", f"teach_{domain}"],
                        exits={"back": domain},
                    )
                    await self.storage.append("contexts", {
                        "id": new_id,
                        "parent": domain,
                        "created_at": time.time(),
                        "auto": True,
                    })
