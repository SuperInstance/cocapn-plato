#!/usr/bin/env python3
"""Fleet deployment orchestrator — start services in dependency order with health checks.

Usage:
    python fleet-orchestrator.py --config services.json --start-all
    python fleet-orchestrator.py --config services.json --restart dashboard nexus harbor
    python fleet-orchestrator.py --diagnose

services.json example:
{
  "services": [
    {"name": "plato-gate", "port": 8847, "cmd": "python -m plato.server", "depends_on": []},
    {"name": "dashboard", "port": 4046, "cmd": "python -m dashboard.server", "depends_on": ["plato-gate"]},
    {"name": "nexus", "port": 4047, "cmd": "python -m nexus.server", "depends_on": ["plato-gate"]}
  ]
}
"""
import argparse
import json
import subprocess
import time
import urllib.request
import sys
from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class ServiceDef:
    name: str
    port: int
    cmd: str
    cwd: Optional[str] = None
    env: Optional[Dict[str, str]] = None
    depends_on: List[str] = None
    timeout: float = 30.0
    retries: int = 3


def probe(host: str, port: int, path: str = "/", timeout: float = 5.0) -> bool:
    """Check if a service is responding."""
    url = f"http://{host}:{port}{path}"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp.read(1)  # Read a byte to confirm it's really responding
            return True
    except urllib.error.HTTPError as e:
        return e.code in (404, 400, 401, 405, 500)
    except Exception:
        return False


def start_service(svc: ServiceDef, host: str = "127.0.0.1") -> Optional[subprocess.Popen]:
    """Start a service process."""
    print(f"Starting {svc.name} on port {svc.port}...")
    env = dict(os.environ)
    if svc.env:
        env.update(svc.env)
    
    proc = subprocess.Popen(
        svc.cmd,
        shell=True,
        cwd=svc.cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    
    # Wait for it to come up
    for attempt in range(svc.retries):
        time.sleep(2)
        if probe(host, svc.port, timeout=svc.timeout / svc.retries):
            print(f"  ✅ {svc.name} up on port {svc.port}")
            return proc
        print(f"  ⏳ {svc.name} not ready yet (attempt {attempt + 1}/{svc.retries})")
    
    print(f"  ❌ {svc.name} failed to start on port {svc.port}")
    return proc


def stop_service(name: str, port: int, host: str = "127.0.0.1") -> bool:
    """Stop a service by finding and killing the process on its port."""
    print(f"Stopping {name} on port {port}...")
    try:
        # Find PID using port
        result = subprocess.run(
            ["lsof", "-ti", f":{port}"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            pids = result.stdout.strip().split("\n")
            for pid in pids:
                if pid:
                    subprocess.run(["kill", "-TERM", pid], capture_output=True)
                    print(f"  Sent TERM to PID {pid}")
            # Wait for port to free
            for _ in range(10):
                if not probe(host, port, timeout=1):
                    print(f"  ✅ {name} stopped")
                    return True
                time.sleep(0.5)
            # Force kill
            for pid in pids:
                if pid:
                    subprocess.run(["kill", "-KILL", pid], capture_output=True)
            return True
        else:
            print(f"  ℹ️ No process found on port {port}")
            return True
    except FileNotFoundError:
        print(f"  ⚠️ lsof not found, trying fuser...")
        try:
            subprocess.run(["fuser", "-k", f"{port}/tcp"], capture_output=True)
            return True
        except FileNotFoundError:
            print(f"  ❌ Cannot find process killer. Install lsof or fuser.")
            return False


def load_config(path: str) -> List[ServiceDef]:
    with open(path) as f:
        data = json.load(f)
    return [ServiceDef(**svc) for svc in data.get("services", [])]


def sort_by_dependencies(services: List[ServiceDef]) -> List[ServiceDef]:
    """Topological sort by dependencies."""
    by_name = {s.name: s for s in services}
    visited = set()
    result = []
    
    def visit(svc: ServiceDef):
        if svc.name in visited:
            return
        visited.add(svc.name)
        for dep in (svc.depends_on or []):
            if dep in by_name:
                visit(by_name[dep])
        result.append(svc)
    
    for svc in services:
        visit(svc)
    return result


def diagnose(host: str = "147.224.38.131"):
    """Diagnose fleet services without starting anything."""
    known_services = [
        ("MUD v3", 4042, "/status"),
        ("The Lock v2", 4043, "/"),
        ("Arena", 4044, "/stats"),
        ("Grammar Engine", 4045, "/grammar"),
        ("Dashboard", 4046, "/"),
        ("Federated Nexus", 4047, "/"),
        ("Grammar Compactor", 4055, "/status"),
        ("Rate-Attention", 4056, "/streams"),
        ("Skill Forge", 4057, "/status"),
        ("Harbor", 4050, "/"),
        ("PLATO Terminal", 4060, "/"),
        ("PLATO Gate", 8847, "/rooms"),
        ("PLATO Shell", 8848, "/"),
        ("Matrix Bridge", 6168, "/status"),
        ("Conduwuit", 6167, "/"),
        ("Service Guard", 8899, "/"),
        ("Task Queue", 8900, "/"),
        ("Steward", 8901, "/"),
    ]
    
    print(f"\n{'Service':<20} {'Port':<6} {'Status':<10} {'Response'}")
    print("-" * 60)
    down = []
    for name, port, path in known_services:
        ok = probe(host, port, path, timeout=4)
        status = "🟢 UP" if ok else "🔴 DOWN"
        resp = "OK" if ok else "No response"
        print(f"{name:<20} {port:<6} {status:<10} {resp}")
        if not ok:
            down.append((name, port))
    
    print(f"\n{'='*60}")
    print(f"Summary: {len(known_services) - len(down)}/{len(known_services)} up, {len(down)} down")
    if down:
        print(f"\nDown services:")
        for name, port in down:
            print(f"  - {name} (port {port})")
        print(f"\nTo restart, run:")
        print(f"  python fleet-orchestrator.py --restart {' '.join(name.replace(' ', '-').lower() for name, _ in down)}")
    return down


def main():
    parser = argparse.ArgumentParser(prog="fleet-orchestrator", description="Fleet deployment orchestrator")
    parser.add_argument("--config", help="services.json config file")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind services")
    parser.add_argument("--start-all", action="store_true", help="Start all services in dependency order")
    parser.add_argument("--restart", nargs="+", help="Restart specific services")
    parser.add_argument("--stop", nargs="+", help="Stop specific services")
    parser.add_argument("--diagnose", action="store_true", help="Diagnose fleet health without starting")
    args = parser.parse_args()
    
    if args.diagnose:
        diagnose()
        return
    
    if not args.config:
        print("Error: --config required unless using --diagnose")
        sys.exit(1)
    
    services = load_config(args.config)
    by_name = {s.name: s for s in services}
    
    if args.restart:
        to_restart = []
        for name in args.restart:
            if name in by_name:
                to_restart.append(by_name[name])
            else:
                print(f"Unknown service: {name}")
        # Stop in reverse order, start in dependency order
        for svc in reversed(to_restart):
            stop_service(svc.name, svc.port, args.host)
        for svc in sort_by_dependencies(to_restart):
            start_service(svc, args.host)
    
    elif args.stop:
        for name in args.stop:
            if name in by_name:
                stop_service(by_name[name].name, by_name[name].port, args.host)
    
    elif args.start_all:
        ordered = sort_by_dependencies(services)
        for svc in ordered:
            start_service(svc, args.host)
        print(f"\n{'='*60}")
        print("All services started. Run --diagnose to verify.")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    import os
    main()
