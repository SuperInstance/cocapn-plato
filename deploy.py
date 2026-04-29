#!/usr/bin/env python3
"""Deploy cocapn-plato server on Oracle1.

Run this on 147.224.38.131 to upgrade the PLATO server with query API.
"""
import subprocess
import sys
import os
from pathlib import Path

REPO_URL = "https://github.com/SuperInstance/cocapn-plato.git"
INSTALL_DIR = Path("/opt/cocapn-plato")
PORT = 8847


def run(cmd, cwd=None):
    print(f"$ {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    return result.returncode == 0


def main():
    print("=== Cocapn Plato Deploy ===")
    print(f"Target: {INSTALL_DIR}")
    print(f"Port: {PORT}")
    print()

    # Install deps
    print("[1/5] Installing dependencies...")
    run("pip install fastapi uvicorn pydantic --break-system-packages || pip install fastapi uvicorn pydantic")

    # Clone or pull
    print("[2/5] Cloning repo...")
    if INSTALL_DIR.exists():
        run("git pull", cwd=INSTALL_DIR)
    else:
        run(f"git clone {REPO_URL} {INSTALL_DIR}")

    # Install package
    print("[3/5] Installing cocapn-plato...")
    run(f"pip install -e {INSTALL_DIR} --break-system-packages || pip install -e {INSTALL_DIR}")

    # Create systemd service or tmux session
    print("[4/5] Starting server...")
    
    # Check if something is already on the port
    import urllib.request
    try:
        urllib.request.urlopen(f"http://localhost:{PORT}/health", timeout=2)
        print(f"  ⚠️  Port {PORT} already in use. Kill existing server first:")
        print(f"     pkill -f 'cocapn_plato.server'")
    except:
        pass

    # Start in background via nohup
    log_file = INSTALL_DIR / "server.log"
    cmd = f"cd {INSTALL_DIR} && PYTHONPATH=src nohup python3 -m cocapn_plato.server > {log_file} 2>&1 &"
    run(cmd)
    
    print(f"  Server log: {log_file}")
    
    # Health check
    print("[5/5] Health check...")
    import time
    time.sleep(2)
    try:
        import urllib.request
        req = urllib.request.Request(f"http://localhost:{PORT}/health")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = resp.read().decode()
            print(f"  ✅ Server up: {data}")
    except Exception as e:
        print(f"  ❌ Server not responding: {e}")
        print(f"  Check log: tail -f {log_file}")
        return 1

    print()
    print("=== Deploy Complete ===")
    print(f"Server: http://localhost:{PORT}")
    print(f"Query:  curl http://localhost:{PORT}/query?domain=harbor&limit=5")
    print(f"Health: curl http://localhost:{PORT}/health")
    return 0


if __name__ == "__main__":
    sys.exit(main())
