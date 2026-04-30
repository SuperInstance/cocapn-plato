#!/usr/bin/env python3
"""fleet-restore — Diagnose and produce restart commands for down fleet services.

Usage:
    python fleet-restore.py
    python fleet-restore.py --host 147.224.38.131 --ssh oracle1

Produces a shell script Oracle1 can run to identify and restart dead services.
"""
import argparse
import json
import urllib.request
from typing import Dict, List, Any


DOWN_SERVICES = {
    "dashboard": {"port": 4046, "likely_process": "dashboard"},
    "federated-nexus": {"port": 4047, "likely_process": "nexus"},
    "harbor": {"port": 4050, "likely_process": "harbor"},
    "service-guard": {"port": 8899, "likely_process": "guard"},
    "task-queue": {"port": 8900, "likely_process": "queue"},
    "steward": {"port": 8901, "likely_process": "steward"},
}


def generate_diagnostic_script(host: str) -> str:
    """Generate a bash script to run on the fleet host."""
    lines = [
        "#!/bin/bash",
        f"# Fleet Restore Diagnostic — generated for {host}",
        "# Run this on the fleet host to identify why services are down",
        "",
        'echo "=== Fleet Restore Diagnostic ==="',
        'echo "Date: $(date)"',
        'echo ""',
    ]
    
    for name, info in DOWN_SERVICES.items():
        port = info["port"]
        proc = info["likely_process"]
        lines.extend([
            f'echo "--- {name} (port {port}) ---"',
            f'# Check if anything is listening on {port}',
            f'ss -tlnp | grep ":{port}" || echo "  No process on port {port}"',
            f'# Check for running process matching {proc}',
            f'ps aux | grep -i "{proc}" | grep -v grep || echo "  No process named {proc}"',
            f'# Check systemd',
            f'systemctl status {name} 2>/dev/null || echo "  No systemd service: {name}"',
            f'# Check supervisor',
            f'supervisorctl status {name} 2>/dev/null || echo "  No supervisor process: {name}"',
            f'# Check docker',
            f'docker ps --filter "name={name}" --format "{{.Names}}: {{.Status}}" 2>/dev/null || echo "  No docker container: {name}"',
            f'# Check tmux/screen sessions',
            f'tmux ls 2>/dev/null | grep -i "{proc}" || echo "  No tmux session: {proc}"',
            'echo ""',
        ])
    
    lines.extend([
        'echo "=== Memory Check ==="',
        'free -h',
        'echo ""',
        'echo "=== Disk Check ==="',
        'df -h',
        'echo ""',
        'echo "=== Recent OOM Kills ==="',
        'dmesg | grep -i "killed process" | tail -5 || echo "  No recent OOM kills in dmesg"',
        'echo ""',
        'echo "Done. Share this output with CCC."',
    ])
    
    return "\n".join(lines)


def generate_restart_commands(host: str, findings: Dict[str, Any]) -> str:
    """Generate restart commands based on diagnostic findings."""
    lines = [
        "#!/bin/bash",
        f"# Fleet Restart Commands — generated for {host}",
        "# WARNING: Only run after understanding why services died.",
        "",
    ]
    
    for name, info in DOWN_SERVICES.items():
        port = info["port"]
        lines.extend([
            f'echo "Starting {name}..."',
            f'# TODO: Replace with actual start command for {name}',
            f'# Example: python -m {name}.server --port {port} &',
            f'sleep 2 && curl -s http://{host}:{port}/ >/dev/null && echo "  {name} UP" || echo "  {name} still DOWN"',
            "",
        ])
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(prog="fleet-restore", description="Diagnose and restore down fleet services")
    parser.add_argument("--host", default="147.224.38.131", help="Fleet host IP")
    parser.add_argument("--output", default="fleet-restore.sh", help="Output script file")
    args = parser.parse_args()
    
    script = generate_diagnostic_script(args.host)
    
    with open(args.output, "w") as f:
        f.write(script)
    
    print(f"Diagnostic script written to {args.output}")
    print(f"Run this on {args.host} with: bash {args.output}")
    print(f"\nThen share the output so CCC can generate restart commands.")
    print(f"\nDown services: {', '.join(DOWN_SERVICES.keys())}")


if __name__ == "__main__":
    main()
