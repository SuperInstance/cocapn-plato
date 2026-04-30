#!/usr/bin/env python3
"""grammar-audit — Compare Grammar Engine vs Grammar Compactor rule counts.

Usage:
    python grammar-audit.py
    python grammar-audit.py --output report.json

Fetches both endpoints and reports the blind spot.
"""
import argparse
import json
import urllib.request

ENGINE_URL = "http://147.224.38.131:4045/grammar"
COMPACTOR_URL = "http://147.224.38.131:4055/status"


def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def analyze() -> dict:
    engine = fetch_json(ENGINE_URL)
    compactor = fetch_json(COMPACTOR_URL)
    
    engine_total = engine.get("total_rules", 0)
    compactor_total = compactor.get("total_rules", 0)
    delta = engine_total - compactor_total
    
    # Type breakdown
    engine_by_type = engine.get("by_type", {})
    compactor_by_type = compactor.get("by_type", {})
    
    type_comparison = {}
    all_types = set(engine_by_type.keys()) | set(compactor_by_type.keys())
    for t in sorted(all_types):
        e_count = engine_by_type.get(t, 0)
        c_count = compactor_by_type.get(t, 0)
        type_comparison[t] = {
            "engine": e_count,
            "compactor": c_count,
            "delta": e_count - c_count,
            "pct_visible": round(c_count / e_count * 100, 1) if e_count > 0 else 0,
        }
    
    return {
        "engine_total": engine_total,
        "compactor_total": compactor_total,
        "delta": delta,
        "pct_visible": round(compactor_total / engine_total * 100, 1) if engine_total > 0 else 0,
        "by_type": type_comparison,
        "severity": "CRITICAL" if delta > 200 else "HIGH" if delta > 50 else "MEDIUM",
    }


def report(data: dict) -> str:
    lines = [
        "# Grammar Audit Report",
        "",
        f"**Severity: {data['severity']}**",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Grammar Engine rules | {data['engine_total']} |",
        f"| Grammar Compactor rules | {data['compactor_total']} |",
        f"| Blind spot (invisible) | **{data['delta']}** |",
        f"| Visibility | {data['pct_visible']}% |",
        "",
        "## By Type",
        "",
        "| Type | Engine | Compactor | Delta | % Visible |",
        "|------|--------|-----------|-------|-----------|",
    ]
    
    for t, info in data["by_type"].items():
        lines.append(
            f"| {t} | {info['engine']} | {info['compactor']} | {info['delta']} | {info['pct_visible']}% |"
        )
    
    lines.extend([
        "",
        "## Assessment",
        "",
        f"The compactor only sees **{data['pct_visible']}%** of the grammar rule space.",
        f"**{data['delta']} rules** are invisible to compaction, meaning they accumulate indefinitely.",
        "",
        "## Likely Causes",
        "",
        "1. Compactor reads from a different data file or cache than the engine",
        "2. Rules are added to engine DB but compactor is not notified to refresh",
        "3. Compactor and engine have divergent storage paths",
        "",
        "## Fix",
        "",
        "Verify both services point to the same rule database file.",
        "If they use different files, configure them to share one source of truth.",
    ])
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(prog="grammar-audit", description="Compare engine vs compactor")
    parser.add_argument("--output", help="JSON output file")
    parser.add_argument("--format", choices=["markdown", "json"], default="markdown", help="Output format")
    args = parser.parse_args()
    
    data = analyze()
    
    if args.format == "json":
        out = json.dumps(data, indent=2)
    else:
        out = report(data)
    
    if args.output:
        with open(args.output, "w") as f:
            f.write(out)
        print(f"Report written to {args.output}")
    else:
        print(out)


if __name__ == "__main__":
    main()
