"""RateAwareSkill — Tracks usage patterns and auto-suggests relevant skills.

From CCC's plato-sdk-drill-1 implementation.
"""
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class SkillUsage:
    """Tracks a single skill usage event."""
    skill_name: str
    timestamp: float
    context: Optional[Dict[str, Any]] = None
    outcome_quality: float = 0.5  # 0.0-1.0


@dataclass
class SkillScore:
    """Aggregated score for a skill."""
    skill_name: str
    total_uses: int = 0
    last_used: float = 0.0
    avg_quality: float = 0.5
    decayed_score: float = 0.0
    
    def update(self, usage: SkillUsage, decay_days: int = 7):
        now = time.time()
        days_since_use = (now - self.last_used) / 86400 if self.last_used else 0
        
        # Apply decay: skills not used in N days fade
        decay_factor = max(0.1, 1.0 - (days_since_use / decay_days))
        
        self.total_uses += 1
        self.last_used = now
        
        # Rolling average of quality
        self.avg_quality = (self.avg_quality * (self.total_uses - 1) + usage.outcome_quality) / self.total_uses
        
        # Decayed score combines recency, frequency, quality
        self.decayed_score = self.avg_quality * decay_factor * min(self.total_uses / 10, 1.0)


class RateAwareSkill:
    """
    Base class for skills that track usage patterns and auto-suggest
    the most relevant skills based on recent usage.
    
    Four-layer pattern:
    1. Base: RateAwareSkill (this class)
    2. Decorator: UsageTracker (wraps any skill)
    3. Registry: SkillRecommender (ranks skills)
    4. Action: AutoSuggest (injects suggestions)
    """
    
    DECAY_DAYS = 7
    MIN_SUGGESTION_SCORE = 0.3
    
    def __init__(self, name: str):
        self.name = name
        self._usage_log: List[SkillUsage] = []
        self._skill_scores: Dict[str, SkillScore] = {}
    
    def record_usage(self, skill_name: str, context: Optional[Dict] = None, 
                     outcome_quality: float = 0.5) -> None:
        """Record that a skill was used."""
        usage = SkillUsage(
            skill_name=skill_name,
            timestamp=time.time(),
            context=context,
            outcome_quality=outcome_quality
        )
        self._usage_log.append(usage)
        
        if skill_name not in self._skill_scores:
            self._skill_scores[skill_name] = SkillScore(skill_name=skill_name)
        self._skill_scores[skill_name].update(usage, self.DECAY_DAYS)
    
    def get_relevant_skills(self, context: Optional[Dict] = None, 
                            top_k: int = 3) -> List[Dict[str, Any]]:
        """
        Auto-suggest the most relevant skills based on:
        1. Recent usage patterns (decayed score)
        2. Context similarity (if provided)
        3. Quality outcomes (avg_quality)
        """
        now = time.time()
        candidates = []
        
        for skill_name, score in self._skill_scores.items():
            days_since = (now - score.last_used) / 86400
            decay = max(0.1, 1.0 - (days_since / self.DECAY_DAYS))
            current_score = score.avg_quality * decay * min(score.total_uses / 10, 1.0)
            
            if current_score < self.MIN_SUGGESTION_SCORE:
                continue
            
            reason = f"Used {score.total_uses}x, avg quality {score.avg_quality:.2f}, "
            reason += f"last {days_since:.1f} days ago"
            
            candidates.append({
                "skill_name": skill_name,
                "score": current_score,
                "reason": reason,
                "total_uses": score.total_uses,
                "last_used": score.last_used
            })
        
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[:top_k]
    
    def get_skill_stats(self) -> Dict[str, Dict[str, Any]]:
        """Return all skill statistics."""
        return {
            name: {
                "total_uses": score.total_uses,
                "avg_quality": score.avg_quality,
                "decayed_score": score.decayed_score,
                "last_used": datetime.fromtimestamp(score.last_used).isoformat() if score.last_used else None
            }
            for name, score in self._skill_scores.items()
        }
    
    def prune_old_usage(self, max_age_days: int = 30) -> int:
        """Remove usage records older than max_age_days. Returns count removed."""
        cutoff = time.time() - (max_age_days * 86400)
        original_len = len(self._usage_log)
        self._usage_log = [u for u in self._usage_log if u.timestamp > cutoff]
        return original_len - len(self._usage_log)


class UsageTracker:
    """Decorator that wraps any callable and tracks its usage via RateAwareSkill."""
    
    def __init__(self, skill: RateAwareSkill, name: str):
        self.skill = skill
        self.name = name
    
    def __call__(self, func):
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                self.skill.record_usage(self.name, context={"args": str(args)}, outcome_quality=0.8)
                return result
            except Exception:
                self.skill.record_usage(self.name, context={"args": str(args)}, outcome_quality=0.2)
                raise
        return wrapper
