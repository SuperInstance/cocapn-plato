from datetime import datetime
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, ConfigDict


class Agent(BaseModel):
    model_config = ConfigDict(validate_assignment=False)
    
    name: str
    role: str = "scout"
    tiles: int = 0
    contexts_visited: List[str] = Field(default_factory=list)
    connected_at: float = Field(default_factory=lambda: datetime.now().timestamp())
    last_seen: float = Field(default_factory=lambda: datetime.now().timestamp())


class Context(BaseModel):
    model_config = ConfigDict(validate_assignment=False)
    
    id: str
    description: str
    tools: List[str] = Field(default_factory=list)
    tasks: List[str] = Field(default_factory=list)
    exits: Dict[str, str] = Field(default_factory=dict)
    tiles_count: int = 0
    created_at: float = Field(default_factory=lambda: datetime.now().timestamp())


class Tile(BaseModel):
    model_config = ConfigDict(validate_assignment=False)
    
    agent: str
    question: str = Field(max_length=2000)
    answer: str = Field(max_length=5000)
    domain: str
    timestamp: float = Field(default_factory=lambda: datetime.now().timestamp())
    provenance: Dict[str, Any] = Field(default_factory=dict)


class Stream(BaseModel):
    model_config = ConfigDict(validate_assignment=False)
    
    id: str
    expected: float = 1.0
    ema: float = 0.0
    alpha: float = 0.3
    divergence: float = 0.0
    observations: int = 0
    auto_respond: bool = False
    last_observed: float = 0.0
    
    def observe(self, value: float):
        self.observations += 1
        self.ema = self.alpha * value + (1 - self.alpha) * (self.ema or value)
        self.divergence = abs(self.ema - self.expected) / max(self.expected, 0.001)
        self.last_observed = datetime.now().timestamp()


class Task(BaseModel):
    model_config = ConfigDict(validate_assignment=False)
    
    id: str
    target: str
    description: str
    completed: bool = False
    assigned_to: Optional[str] = None
    created_at: float = Field(default_factory=lambda: datetime.now().timestamp())
    completed_at: Optional[float] = None
    last_seen: float = Field(default_factory=lambda: datetime.now().timestamp())
    auto: bool = False
    priority: int = 1  # 1=normal, 2=high, 3=critical


class Rule(BaseModel):
    model_config = ConfigDict(validate_assignment=False)
    
    name: str = Field(pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    condition: str = Field(max_length=500)
    action: str = Field(max_length=500)
    creator: str = "system"
    created_at: float = Field(default_factory=lambda: datetime.now().timestamp())
    fitness: float = 0.5
    usage_count: int = 0


class FleetStatus(BaseModel):
    agents: int
    contexts: int
    tiles: int
    streams: Dict[str, Dict[str, float]]
    divergences: List[Dict[str, Any]]
    tasks_available: int
    tasks_completed: int
    uptime_seconds: float


class TileBatch(BaseModel):
    tiles: List[Tile]
    agent: str
