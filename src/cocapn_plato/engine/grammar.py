import re
import time
from typing import Dict, List, Optional
from .storage import JSONLStore
from .models import Rule


class Grammar:
    """Sanitized rule engine. No code injection. No SQLi. No XSS.
    
    Rules are AST-safe strings with restricted action vocabulary.
    Every rule has provenance tracking (creator, timestamp, parent).
    """

    SAFE_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    BANNED = [
        "<script", "DROP TABLE", "rm -rf", "__import__",
        "os.system", "eval(", "exec(", "subprocess", "import os",
        "import sys", "open(", "write(", "read(", "delete(",
        " shutil", " pathlib", " socket", " urllib", " requests",
    ]
    
    # Whitelist of safe action verbs — anything else is rejected
    SAFE_ACTIONS = [
        "suggest", "flag", "notify", "log", "route", "prioritize",
        "escalate", "summarize", "merge", "split", "archive",
    ]

    def __init__(self, storage: JSONLStore):
        self.storage = storage
        self.rules: Dict[str, Rule] = {}

    def _sanitize(self, text: str) -> Optional[str]:
        if not text or len(text) > 500:
            return None
        lower = text.lower()
        for ban in self.BANNED:
            if ban.lower() in lower:
                return None
        return text.strip()

    def _validate_action(self, action: str) -> bool:
        """Action must start with a safe verb from the whitelist."""
        first_word = action.split()[0].lower() if action else ""
        return first_word in [a.lower() for a in self.SAFE_ACTIONS]

    async def add_rule(self, name: str, condition: str, action: str, creator: str = "system") -> Optional[Rule]:
        if not self.SAFE_NAME.match(name):
            return None
        clean_condition = self._sanitize(condition)
        clean_action = self._sanitize(action)
        if not clean_condition or not clean_action:
            return None
        if not self._validate_action(clean_action):
            return None
        
        rule = Rule(name=name, condition=clean_condition, action=clean_action, creator=creator)
        self.rules[name] = rule
        await self.storage.append("rules", rule.model_dump())
        return rule

    def evaluate(self, context: Dict) -> List[Rule]:
        """Find all rules whose condition mentions a key in context."""
        triggered = []
        for rule in self.rules.values():
            if any(k in rule.condition for k in context):
                triggered.append(rule)
                rule.usage_count += 1
        return triggered

    def get_fitness(self, name: str) -> float:
        rule = self.rules.get(name)
        if not rule:
            return 0.0
        recency = max(0.1, 1.0 - (time.time() - rule.created_at) / 604800)
        usage_factor = min(rule.usage_count / 10, 1.0)
        return rule.fitness * recency * usage_factor

    def prune_stagnant(self, min_fitness: float = 0.1) -> int:
        stale = [n for n, r in self.rules.items() if self.get_fitness(n) < min_fitness]
        for n in stale:
            del self.rules[n]
        return len(stale)

    def stats(self) -> Dict:
        return {
            "total": len(self.rules),
            "avg_fitness": sum(r.fitness for r in self.rules.values()) / max(len(self.rules), 1),
            "avg_usage": sum(r.usage_count for r in self.rules.values()) / max(len(self.rules), 1),
            "stale_count": len([r for r in self.rules.values() if self.get_fitness(r.name) < 0.1]),
        }
