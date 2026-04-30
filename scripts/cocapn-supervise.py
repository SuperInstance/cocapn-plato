#!/usr/bin/env python3
"""cocapn-supervise — Minimal service supervisor. Restarts services that die.

Maximum capability in minimum lines. Zero dependencies.

Usage:
    python cocapn-supervise.py services.json
    python cocapn-supervise.py --port 9999  # status dashboard

services.json:
[
  {"name": "plato-gate", "cmd": "python -m plato.server", "port": 8847},
  {"name": "dashboard", "cmd": "python -m dashboard.server", "port": 4046}
]
"""
import json
import sys
import time
import subprocess
import urllib.request
import threading
from typing import Dict, List, Any
from datetime import datetime


class Supervisor:
    def __init__(self, services: List[Dict[str, Any]], check_interval: int = 10):
        self.services = services
        self.check_interval = check_interval
        self.processes: Dict[str, subprocess.Popen] = {}
        self.restarts: Dict[str, int] = {}
        self.last_check: Dict[str, str] = {}
        self._lock = threading.Lock()
        self._running = True

    def start_all(self):
        for svc in self.services:
            self._start(svc)

    def _start(self, svc: Dict[str, Any]):
        name = svc["name"]
        print(f"[{datetime.now().isoformat()}] Starting {name}...")
        proc = subprocess.Popen(
            svc["cmd"],
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        with self._lock:
            self.processes[name] = proc
            self.restarts[name] = self.restarts.get(name, 0)
        print(f"  PID {proc.pid}")

    def _is_alive(self, name: str) -> bool:
        with self._lock:
            proc = self.processes.get(name)
            if not proc:
                return False
            return proc.poll() is None

    def _probe(self, host: str, port: int, path: str = "/", timeout: float = 3.0) -> bool:
        try:
            url = f"http://{host}:{port}{path}"
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                resp.read(1)
                return True
        except urllib.error.HTTPError as e:
            return e.code in (404, 400, 401, 405, 500)
        except Exception:
            return False

    def check_once(self):
        for svc in self.services:
            name = svc["name"]
            port = svc.get("port")
            host = svc.get("host", "127.0.0.1")
            path = svc.get("path", "/")

            alive = self._is_alive(name)
            responding = self._probe(host, port, path) if port else alive

            self.last_check[name] = datetime.now().isoformat()

            if not alive or not responding:
                print(f"[{datetime.now().isoformat()}] {name} down (alive={alive}, responding={responding})")
                # Kill if still running
                if alive:
                    with self._lock:
                        proc = self.processes[name]
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait()
                # Restart
                with self._lock:
                    self.restarts[name] = self.restarts.get(name, 0) + 1
                self._start(svc)
            else:
                print(f"[{datetime.now().isoformat()}] {name} OK (restarts: {self.restarts.get(name, 0)})")

    def run(self):
        self.start_all()
        print(f"\nSupervisor running. Checking every {self.check_interval}s. Ctrl+C to stop.\n")
        try:
            while self._running:
                self.check_once()
                time.sleep(self.check_interval)
        except KeyboardInterrupt:
            print("\nShutting down...")
            self.stop_all()

    def stop_all(self):
        self._running = False
        with self._lock:
            for name, proc in self.processes.items():
                print(f"Stopping {name} (PID {proc.pid})...")
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "services": [
                    {
                        "name": s["name"],
                        "alive": self._is_alive(s["name"]),
                        "restarts": self.restarts.get(s["name"], 0),
                        "last_check": self.last_check.get(s["name"]),
                        "cmd": s["cmd"],
                    }
                    for s in self.services
                ],
                "total_restarts": sum(self.restarts.values()),
                "running_since": datetime.now().isoformat(),
            }


def run_dashboard(supervisor: Supervisor, port: int = 9999):
    """Run a simple HTTP status dashboard."""
    import http.server
    import socketserver

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(supervisor.status(), indent=2).encode())
            elif self.path == "/health":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                status = supervisor.status()
                all_alive = all(s["alive"] for s in status["services"])
                self.wfile.write(json.dumps({"status": "healthy" if all_alive else "degraded"}).encode())
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            pass  # Silence logs

    with socketserver.TCPServer(("", port), Handler) as httpd:
        print(f"Dashboard on http://localhost:{port}")
        httpd.serve_forever()


def main():
    parser = argparse.ArgumentParser(prog="cocapn-supervise", description="Minimal service supervisor")
    parser.add_argument("config", nargs="?", help="services.json file")
    parser.add_argument("--interval", type=int, default=10, help="Check interval in seconds")
    parser.add_argument("--dashboard", type=int, help="Dashboard port (default: none)")
    args = parser.parse_args()

    if not args.config:
        parser.print_help()
        sys.exit(1)

    with open(args.config) as f:
        services = json.load(f)

    supervisor = Supervisor(services, check_interval=args.interval)

    if args.dashboard:
        t = threading.Thread(target=run_dashboard, args=(supervisor, args.dashboard), daemon=True)
        t.start()

    supervisor.run()


if __name__ == "__main__":
    import argparse
    main()
