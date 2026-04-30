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

    # queue
    qu = sub.add_parser("queue", help="Task queue operations")
    qu_sub = qu.add_subparsers(dest="queue_cmd")
    qu_sub.add_parser("submit", help="Submit a task").add_argument("--payload", required=True, help="JSON payload")
    qu_sub.add_parser("claim", help="Claim a task").add_argument("--worker", default="cli")
    qu_sub.add_parser("list", help="List tasks").add_argument("--status")
    qu_sub.add_parser("stats", help="Queue stats")

    args = parser.parse_args()
    client = PlatoClient(args.url if args.cmd == "migrate" and args.input == "plato" else args.url)

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

    elif args.cmd == "migrate":
        from cocapn_plato.engine.migrate import pipeline
        import urllib.request

        if args.input == "plato":
            print(f"Fetching tiles from {args.url}...")
            req = urllib.request.Request(f"{args.url}/export/plato-tile-spec", headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = json.loads(resp.read().decode())
            if isinstance(raw, dict):
                for key in ["tiles", "data", "records"]:
                    if key in raw and isinstance(raw[key], list):
                        raw = raw[key]
                        break
        else:
            with open(args.input) as f:
                raw = [json.loads(line) for line in f if line.strip()]

        print(f"Running pipeline on {len(raw)} raw tiles...")
        result = pipeline(raw, fuzzy=args.fuzzy)
        stats = result["stats"]

        print(f"\n📊 Stats:")
        print(f"  Raw:        {stats['raw_count']}")
        print(f"  Normalized: {stats['normalized_count']}")
        print(f"  Unique:     {stats['unique_count']}")
        print(f"  Dups:       {stats['dups_removed']}")
        print(f"  Unrecoverable: {stats['unrecoverable']}")
        print(f"  Avg Quality: {stats['avg_quality']}")
        print(f"\nTop Domains: {', '.join(f'{d}({c})' for d,c in stats['top_domains'][:5])}")
        print(f"Top Agents:  {', '.join(f'{a}({c})' for a,c in stats['top_agents'][:5])}")

        if not args.stats_only:
            out = sys.stdout if not args.output else open(args.output, "w")
            for tile in result["tiles"]:
                out.write(json.dumps(tile, ensure_ascii=False) + "\n")
            if args.output:
                out.close()
                print(f"\nWrote {len(result['tiles'])} tiles to {args.output}")

    elif args.cmd == "status":
        data = client.status()
        print(json.dumps(data, indent=2, default=str))

    elif args.cmd == "health":
        data = client.health()
        print(json.dumps(data, indent=2, default=str))

    elif args.cmd == "submit":
        result = client.submit(args.agent, args.question, args.answer, args.domain)
        print(json.dumps(result, indent=2, default=str))

    elif args.cmd == "queue":
        if args.queue_cmd == "submit":
            payload = json.loads(args.payload)
            result = client._request("POST", "/queue/submit", {"payload": payload})
            print(json.dumps(result, indent=2, default=str))
        elif args.queue_cmd == "claim":
            result = client._request("POST", "/queue/claim", {"worker": args.worker})
            print(json.dumps(result, indent=2, default=str))
        elif args.queue_cmd == "list":
            params = {}
            if args.status:
                params["status"] = args.status
            result = client._request("GET", "/queue/list", params=params)
            print(json.dumps(result, indent=2, default=str))
        elif args.queue_cmd == "stats":
            result = client._request("GET", "/queue/stats")
            print(json.dumps(result, indent=2, default=str))
        else:
            qu.print_help()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
