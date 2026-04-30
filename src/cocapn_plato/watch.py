#!/usr/bin/env python3
"""cocapn-watch — Fleet watchdog. Alerts when services change state.

Usage:
    cocapn-watch --interval 30 --webhook https://hooks.example.com/alerts
    cocapn-watch --config watch.json
"""
import argparse
import json
import time
import urllib.request
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class WatchConfig:
    services: List[Dict[str, Any]]
    interval: int = 30
    webhook: Optional[str] = None
    log_file: Optional[str] = None
    alert_on_down: bool = True
    alert_on_recover: bool = True
    consecutive_failures: int = 1  # Alert after N consecutive failures


class Watchdog:
    """Poll services and trigger alerts on state changes."""

    def __init__(self, config: WatchConfig):
        self.config = config
        self.state: Dict[str, Dict[str, Any]] = {}  # service name -> {ok, failures, last_alerted}

    def check(self) -> List[Dict[str, Any]]:
        """Check all services, return alerts triggered."""
        alerts = []
        for svc in self.config.services:
            name = svc["name"]
            url = f"http://{svc['host']}:{svc['port']}{svc.get('path', '/')}"
            timeout = svc.get("timeout", 5)
            
            ok = self._probe(url, timeout)
            
            prev = self.state.get(name, {"ok": True, "failures": 0, "alerted": False})
            
            if not ok:
                failures = prev["failures"] + 1
                alerted = prev.get("alerted", False)
                self.state[name] = {"ok": False, "failures": failures, "alerted": alerted}
                
                if failures >= self.config.consecutive_failures and not alerted:
                    alert = {
                        "time": datetime.now().isoformat(),
                        "service": name,
                        "event": "down",
                        "failures": failures,
                        "url": url,
                    }
                    alerts.append(alert)
                    self.state[name]["alerted"] = True
            else:
                if not prev.get("ok") and self.config.alert_on_recover and prev.get("alerted"):
                    alert = {
                        "time": datetime.now().isoformat(),
                        "service": name,
                        "event": "recovered",
                        "url": url,
                    }
                    alerts.append(alert)
                self.state[name] = {"ok": True, "failures": 0, "alerted": False}
        
        return alerts

    def _probe(self, url: str, timeout: float) -> bool:
        try:
            req = urllib.request.Request(url, method="HEAD")
            urllib.request.urlopen(req, timeout=timeout)
            return True
        except urllib.error.HTTPError as e:
            return e.code in (404, 400, 401)  # Service is up, just endpoint missing
        except Exception:
            return False

    def send_alerts(self, alerts: List[Dict[str, Any]]):
        for alert in alerts:
            msg = f"[{alert['time']}] {alert['service']} is {alert['event']} ({alert.get('url', '')})"
            
            if self.config.log_file:
                with open(self.config.log_file, "a") as f:
                    f.write(msg + "\n")
            
            if self.config.webhook:
                try:
                    body = json.dumps(alert).encode()
                    req = urllib.request.Request(
                        self.config.webhook,
                        data=body,
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    urllib.request.urlopen(req, timeout=10)
                except Exception as e:
                    print(f"Webhook failed: {e}")
            
            print(msg)

    def run(self):
        print(f"Watching {len(self.config.services)} services every {self.config.interval}s")
        print(f"Alerts: down={self.config.alert_on_down}, recover={self.config.alert_on_recover}")
        if self.config.webhook:
            print(f"Webhook: {self.config.webhook}")
        
        while True:
            alerts = self.check()
            if alerts:
                self.send_alerts(alerts)
            time.sleep(self.config.interval)


def load_config(path: str) -> WatchConfig:
    with open(path) as f:
        data = json.load(f)
    return WatchConfig(**data)


def main():
    parser = argparse.ArgumentParser(prog="cocapn-watch", description="Fleet watchdog")
    parser.add_argument("--config", help="JSON config file")
    parser.add_argument("--interval", type=int, default=30, help="Check interval in seconds")
    parser.add_argument("--webhook", help="Webhook URL for alerts")
    parser.add_argument("--log", help="Log file for alerts")
    parser.add_argument("--fleet", action="store_true", help="Use built-in fleet services")
    args = parser.parse_args()

    if args.config:
        config = load_config(args.config)
    elif args.fleet:
        config = WatchConfig(
            services=[
                {"name": "MUD v3", "host": "147.224.38.131", "port": 4042, "path": "/status"},
                {"name": "Arena", "host": "147.224.38.131", "port": 4044, "path": "/stats"},
                {"name": "Grammar", "host": "147.224.38.131", "port": 4045, "path": "/grammar"},
                {"name": "Dashboard", "host": "147.224.38.131", "port": 4046, "path": "/"},
                {"name": "Nexus", "host": "147.224.38.131", "port": 4047, "path": "/"},
                {"name": "Compactor", "host": "147.224.38.131", "port": 4055, "path": "/status"},
                {"name": "PLATO", "host": "147.224.38.131", "port": 8847, "path": "/rooms"},
            ],
            interval=args.interval,
            webhook=args.webhook,
            log_file=args.log,
        )
    else:
        parser.print_help()
        return

    dog = Watchdog(config)
    try:
        dog.run()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
