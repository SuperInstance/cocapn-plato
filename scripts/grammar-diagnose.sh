#!/bin/bash
# grammar-diagnose.sh — Run on fleet host to find why compactor sees only 54 rules
# Produces actionable output for CCC and Oracle1

set -uo pipefail

echo "=== Grammar Compactor Blind Spot Diagnostic ==="
echo "Date: $(date)"
echo "Host: $(hostname)"
echo "User: $(whoami)"
echo ""

# 1. Engine rule count
echo "[1/5] Grammar Engine (port 4045)..."
ENGINE_RULES=$(curl -s http://127.0.0.1:4045/grammar 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('total_rules','ERR'))")
echo "  Total rules: ${ENGINE_RULES}"
echo "  By type: $(curl -s http://127.0.0.1:4045/grammar 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps(d.get('by_type',{})))")"
echo ""

# 2. Compactor rule count
echo "[2/5] Grammar Compactor (port 4055)..."
COMPACTOR_RULES=$(curl -s http://127.0.0.1:4055/status 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('total_rules','ERR'))")
echo "  Total rules: ${COMPACTOR_RULES}"
echo "  By type: $(curl -s http://127.0.0.1:4055/status 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps(d.get('by_type',{})))")"
echo ""

# 3. Find rule storage files
echo "[3/5] Searching for rule data files..."
for pattern in "*.jsonl" "*.json" "rules*" "grammar*" "evolution*" ; do
    found=$(find /home -name "$pattern" 2>/dev/null | head -5)
    if [[ -n "$found" ]]; then
        echo "  Pattern '$pattern':"
        echo "$found" | while read -r f; do
            size=$(stat -c%s "$f" 2>/dev/null || echo "?")
            lines=$(wc -l < "$f" 2>/dev/null || echo "?")
            echo "    $f (${size} bytes, ${lines} lines)"
        done
    fi
done
echo ""

# 4. Check running processes
echo "[4/5] Running grammar processes..."
ps aux | grep -E "grammar|compactor|evolve" | grep -v grep || echo "  No grammar processes found"
echo ""

# 5. Check file access times
echo "[5/5] Most recently accessed rule files (last 24h)..."
find /home -name "*.jsonl" -mtime -1 2>/dev/null | while read -r f; do
    echo "  $f ($(stat -c%y "$f" 2>/dev/null | cut -d' ' -f1,2 | cut -d'.' -f1))"
done
echo ""

# Summary
echo "=== Summary ==="
if [[ "$ENGINE_RULES" =~ ^[0-9]+$ && "$COMPACTOR_RULES" =~ ^[0-9]+$ ]]; then
    DELTA=$((ENGINE_RULES - COMPACTOR_RULES))
    PCT=$(python3 -c "print(round(${COMPACTOR_RULES}/${ENGINE_RULES}*100, 1))")
    echo "Engine: ${ENGINE_RULES} | Compactor: ${COMPACTOR_RULES} | Blind: ${DELTA} (${PCT}% visible)"
else
    echo "Could not fetch rule counts. Services may not be running on localhost."
fi
echo ""
echo "Next step: Share this output with CCC."
