#!/usr/bin/env python3
"""fleet-webhook — Send alerts when fleet services change state.

Usage:
    python fleet-webhook.py --webhook https://hooks.example.com/fleet
    python fleet-webhook.py --webhook https://hooks.example.com/fleet --interval 60
    python fleet-webhook.py --test --webhook https://hooks.example.com/fleet
"""
import argparse
import json
import urllib.request
import time
from typing import Dict, List, Any

# Fleet services to monitor
SERVICES = [
    ("MUD v3", 4042, "/status"),
    ("The Lock v2", 4043, "/status"),
    ("Arena", 4044, "/stats"),
    ("Grammar Engine", 4045, "/grammar"),
    ("Dashboard", 4046, "/"),
    ("Federated Nexus", 4047, "/"),
    ("Harbor", 4050, "/"),
    ("Grammar Compactor", 4055, "/status"),
    ("Rate-Attention", 4056, "/streams"),
    ("Skill Forge", 4057, "/status"),
    ("PLATO Terminal", 4060, "/"),
    ("PLATO Gate", 8847, "/rooms"),
    ("PLATO Shell", 8848, "/"),
    ("Service Guard", 8899, "/"),
    ("Task Queue", 8900, "/"),
    ("Steward", 8901, "/"),
    ("Matrix Bridge", 6168, "/status"),
    ("Conduwuit", 6167, "/"),
]


def check_service(name: str, port: int, path: str) -> bool:
    try:
        url = f"http://147.224.38.131:{port}{path}"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read(1)
            return True
    except urllib.error.HTTPError as e:
        return e.code in (404, 400, 401, 500)
    except Exception:
        return False


def send_webhook(webhook_url: str, payload: Dict[str, Any]) -> bool:
    try:
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status in (200, 201, 202, 204)
    except Exception as e:
        print(f"Webhook failed: {e}")
        return False


def run_monitor(webhook_url: str, interval: int = 60):
    """Monitor fleet and send alerts on state changes."""
    previous = {}
    
    print(f"Monitoring fleet every {interval}s. Webhook: {webhook_url}")
    print("Ctrl+C to stop.\n")
    
    try:
        while True:
            changes = []
            current = {}
            
            for name, port, path in SERVICES:
                up = check_service(name, port, path)
                current[name] = up
                
                if name in previous:
                    if up and not previous[name]:
                        changes.append({"service": name, "change": "UP", "port": port})
                    elif not up and previous[name]:
                        changes.append({"service": name, "change": "DOWN", "port": port})
                else:
                    # First run
                    if not up:
                        changes.append({"service": name, "change": "DOWN", "port": port})
            
            if changes:
                up_count = sum(1 for v in current.values() if v)
                down_count = len(current) - up_count
                
                payload = {
                    "event": "fleet_state_change",
                    "timestamp": time.time(),
                    "summary": {"up": up_count, "down": down_count, "total": len(current)},
                    "changes": changes,
                }
                
                print(f"[{time.strftime('%H:%M:%S')}] {len(changes)} changes detected")
                for c in changes:
                    emoji = "🟢" if c["change"] == "UP" else "🔴"
                    print(f"  {emoji} {c['service']} is now {c['change']}")
                
                if send_webhook(webhook_url, payload):
                    print("  ✓ Webhook sent")
                else:
                    print("  ✗ Webhook failed")
            else:
                # Periodic heartbeat (every 10 checks = ~10 min)
                if int(time.time()) % (interval * 10) < interval:
                    up_count = sum(1 for v in current.values() if v)
                    payload = {
                        "event": "fleet_heartbeat",
                        "timestamp": time.time(),
                        "summary": {"up": up_count, "down": len(current) - up_count, "total": len(current)},
                    }
                    send_webhook(webhook_url, payload)
            
            previous = current
            time.sleep(interval)
    
    except KeyboardInterrupt:
        print("\nStopped.")


def test_webhook(webhook_url: str):
    """Send a test notification."""
    payload = {
        "event": "test",
        "timestamp": time.time(),
        "message": "Fleet webhook test from CCC",
        "services_monitored": len(SERVICES),
    }
    if send_webhook(webhook_url, payload):
        print("✓ Test webhook sent successfully")
    else:
        print("✗ Test webhook failed")


def main():
    parser = argparse.ArgumentParser(prog="fleet-webhook", description="Fleet webhook notifier")
    parser.add_argument("--webhook", required=True, help="Webhook URL")
    parser.add_argument("--interval", type=int, default=60, help="Check interval in seconds")
    parser.add_argument("--test", action="store_true", help="Send test notification and exit")
    args = parser.parse_args()
    
    if args.test:
        test_webhook(args.webhook)
    else:
        run_monitor(args.webhook, args.interval)


if __name__ == "__main__":
    main()
