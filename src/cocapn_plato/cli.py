#!/usr/bin/env python3
"""cocapn — CLI for querying PLATO tiles.

Maximum capability in minimum lines.
"""
import argparse
import json
import sys
from cocapn_plato.sdk.client import PlatoClient


def main():
    parser = argparse.ArgumentParser(prog="cocapn", description="Query PLATO tiles")
    parser.add_argument("--url", default="http://localhost:8847", help="PLATO server URL")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    sub = parser.add_subparsers(dest="cmd")

    # query
    q = sub.add_parser("query", help="Query tiles")
    q.add_argument("--domain", help="Filter by domain")
    q.add_argument("--agent", help="Filter by agent")
    q.add_argument("--q", help="Full-text search")
    q.add_argument("--sort", help="Sort field:direction, e.g. timestamp:desc")
    q.add_argument("--limit", type=int, default=20)
    q.add_argument("--offset", type=int, default=0)

    # aggregate
    a = sub.add_parser("aggregate", help="Aggregate tiles")
    a.add_argument("--group-by", default="domain", help="Group by field")
    a.add_argument("--metrics", help="Comma-separated metrics, e.g. count,avg:timestamp")
    a.add_argument("--sort", help="Sort by metric, e.g. count:desc")

    # status / health
    sub.add_parser("status", help="Fleet status")
    sub.add_parser("health", help="Health check")

    # submit
    s = sub.add_parser("submit", help="Submit a tile")
    s.add_argument("--agent", required=True)
    s.add_argument("--domain", default="general")
    s.add_argument("--question", required=True)
    s.add_argument("--answer", required=True)

    args = parser.parse_args()
    client = PlatoClient(args.url)

    if args.cmd == "query":
        where = {}
        if args.domain:
            where["domain"] = args.domain
        if args.agent:
            where["agent"] = args.agent

        sort = None
        if args.sort:
            parts = args.sort.split(":")
            sort = [(parts[0], parts[1])]

        result = client.query(where=where if where else None, sort=sort, limit=args.limit, offset=args.offset, q=args.q)

        if args.json:
            print(json.dumps({"results": result.results, "total": result.total}, indent=2, default=str))
            return

        print(f"{result.total} tiles (showing {len(result)}):")
        for t in result.results:
            agent = t.get("agent", "?")
            domain = t.get("domain", "?")
            question = t.get("question", "N/A")[:60]
            print(f"  [{domain}/{agent}] {question}...")

    elif args.cmd == "aggregate":
        metrics = args.metrics.split(",") if args.metrics else None
        result = client._request("POST", "/aggregate", {
            "table": "tiles",
            "group_by": args.group_by,
            "metrics": metrics,
        })

        if args.json:
            print(json.dumps(result, indent=2, default=str))
            return

        if isinstance(result, list):
            print(f"Groups: {len(result)}")
            for r in result:
                key = r.get("_key", "?")
                count = r.get("count", 0)
                line = f"  {key}: {count}"
                for k, v in r.items():
                    if k not in ("_key", "count"):
                        line += f", {k}={v}"
                print(line)
        else:
            print(json.dumps(result, indent=2, default=str))

    elif args.cmd == "status":
        data = client.status()
        print(json.dumps(data, indent=2, default=str))

    elif args.cmd == "health":
        data = client.health()
        print(json.dumps(data, indent=2, default=str))

    elif args.cmd == "submit":
        result = client.submit(args.agent, args.question, args.answer, args.domain)
        print(json.dumps(result, indent=2, default=str))

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
