import time
from typing import Dict, List
from .models import Stream


class DivergenceMonitor:
    """Checks all streams for divergence. Returns CRITICAL/WARN alerts."""

    CRITICAL = 5.0
    WARN = 2.0

    def __init__(self, streams: Dict[str, Stream]):
        self.streams = streams

    def check_all(self) -> List[Dict]:
        alerts = []
        for sid, stream in self.streams.items():
            if stream.divergence > self.CRITICAL:
                alerts.append({
                    "stream": sid,
                    "level": "CRITICAL",
                    "divergence": round(stream.divergence, 3),
                    "ema": round(stream.ema, 3),
                    "expected": stream.expected,
                    "observations": stream.observations,
                })
            elif stream.divergence > self.WARN:
                alerts.append({
                    "stream": sid,
                    "level": "WARN",
                    "divergence": round(stream.divergence, 3),
                    "ema": round(stream.ema, 3),
                    "expected": stream.expected,
                    "observations": stream.observations,
                })
        return sorted(alerts, key=lambda x: x["divergence"], reverse=True)

    def observe(self, stream_id: str, value: float):
        if stream_id in self.streams:
            self.streams[stream_id].observe(value)
