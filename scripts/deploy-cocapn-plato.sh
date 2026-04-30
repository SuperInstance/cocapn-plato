#!/bin/bash
# deploy-cocapn-plato.sh — One-command deployment for Oracle1
# Run on the fleet host (147.224.38.131) as the service user

set -euo pipefail

REPO="https://github.com/SuperInstance/cocapn-plato.git"
INSTALL_DIR="${HOME}/.local/share/cocapn-plato"
PORT=8847
PYTHON="${PYTHON:-python3}"

echo "=== Cocapn PLATO Server Deployment ==="
echo "Target: ${INSTALL_DIR}"
echo "Port: ${PORT}"
echo ""

# 1. Clone or update
echo "[1/6] Cloning / updating repo..."
if [[ -d "${INSTALL_DIR}/.git" ]]; then
    cd "${INSTALL_DIR}"
    git fetch origin
    git reset --hard origin/main
else
    git clone "${REPO}" "${INSTALL_DIR}"
    cd "${INSTALL_DIR}"
fi

# 2. Install
echo "[2/6] Installing package..."
${PYTHON} -m pip install -e ".[dev]" --quiet

# 3. Stop old server if running
echo "[3/6] Stopping old server (if running)..."
pkill -f "cocapn_plato.server" 2>/dev/null || true
sleep 2

# 4. Start new server
echo "[4/6] Starting server on port ${PORT}..."
nohup ${PYTHON} -m cocapn_plato.server.routes > "${INSTALL_DIR}/server.log" 2>&1 &
sleep 3

# 5. Health check
echo "[5/6] Health check..."
for i in 1 2 3; do
    if curl -s "http://127.0.0.1:${PORT}/health" > /dev/null; then
        echo "  Server responding on port ${PORT}"
        break
    fi
    sleep 2
    if [[ $i -eq 3 ]]; then
        echo "  WARNING: Server not responding. Check ${INSTALL_DIR}/server.log"
        exit 1
    fi
done

# 6. Report
echo "[6/6] Done."
echo ""
echo "Server: http://127.0.0.1:${PORT}"
echo "Log:    ${INSTALL_DIR}/server.log"
echo ""
echo "Query API:  curl http://127.0.0.1:${PORT}/query?limit=1"
echo "Status:     curl http://127.0.0.1:${PORT}/status"
echo ""
echo "To restart: pkill -f cocapn_plato.server; sleep 2; nohup python3 -m cocapn_plato.server.routes > server.log 2>&1 &"
