#!/usr/bin/env python3
"""fleet-snapshot — Generate a static HTML snapshot of fleet status.

Usage:
    python fleet-snapshot.py --output snapshot-2026-04-30.html
    python fleet-snapshot.py --output -  # stdout

Fetches live fleet data and produces a standalone HTML page.
"""
import argparse
import json
import urllib.request
from datetime import datetime

HOST = "147.224.38.131"


def fetch_status(port: int, path: str = "/") -> dict:
    try:
        url = f"http://{HOST}:{port}{path}"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = resp.read().decode()
            try:
                return json.loads(body)
            except json.JSONDecodeError:
                return {"_html": body[:500]}
    except Exception as e:
        return {"_error": str(e)}


def generate_html() -> str:
    services = [
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
    
    rows = []
    up_count = 0
    
    for name, port, path in services:
        data = fetch_status(port, path)
        has_error = "_error" in data
        is_html = "_html" in data
        
        if has_error:
            status = "🔴 DOWN"
            detail = str(data["_error"])[:80]
        elif is_html:
            status = "🟢 UP (HTML)"
            detail = "HTML response"
            up_count += 1
        else:
            status = "🟢 UP"
            # Extract key metrics
            metrics = []
            for key in ["rooms", "tiles", "total_rules", "total_matches", "total_players", "streams", "drills"]:
                if key in data:
                    metrics.append(f"{key}={data[key]}")
            detail = ", ".join(metrics) if metrics else "JSON response"
            up_count += 1
        
        rows.append(f"""
        <tr>
            <td>{name}</td>
            <td>{port}</td>
            <td>{status}</td>
            <td><pre>{detail}</pre></td>
        </tr>""")
    
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Cocapn Fleet Snapshot — {datetime.now().isoformat()[:19]}</title>
<style>
body {{ font-family: 'JetBrains Mono', monospace; background: #0A0A0F; color: #d8d8ec; padding: 2rem; max-width: 1200px; margin: 0 auto; }}
h1 {{ color: #7C3AED; }}
table {{ width: 100%; border-collapse: collapse; margin-top: 1rem; }}
th {{ text-align: left; padding: .5rem; border-bottom: 2px solid #1c1c35; color: #00E6D6; }}
td {{ padding: .5rem; border-bottom: 1px solid #1c1c35; vertical-align: top; }}
pre {{ margin: 0; font-size: .75rem; color: #8A93B4; }}
.summary {{ font-size: 1.2rem; margin: 1rem 0; }}
</style>
</head>
<body>
<h1>🦀 Cocapn Fleet Snapshot</h1>
<p>Generated: {datetime.now().isoformat()}</p>
<div class="summary"><strong>{up_count}/{len(services)} services UP</strong> — {len(services) - up_count} down</div>
<table>
<tr><th>Service</th><th>Port</th><th>Status</th><th>Details</th></tr>
{"".join(rows)}
</table>
</body>
</html>
"""
    return html


def main():
    parser = argparse.ArgumentParser(prog="fleet-snapshot", description="Generate static HTML fleet snapshot")
    parser.add_argument("--output", default="fleet-snapshot.html", help="Output file (- for stdout)")
    args = parser.parse_args()
    
    html = generate_html()
    
    if args.output == "-":
        print(html)
    else:
        with open(args.output, "w") as f:
            f.write(html)
        print(f"Snapshot written to {args.output}")


if __name__ == "__main__":
    main()
