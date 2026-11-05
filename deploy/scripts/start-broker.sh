#!/usr/bin/env bash
# start-broker.sh
# Manual broker launch - sources broker.env automatically; no manual env-var setting needed.
# Portable source-use launcher; scripted harness installation supports Windows only.
# Invoke by the selected installed path: bash <BrokerRoot>/scripts/start-broker.sh

set -e

# Resolve broker root (parent of scripts/) regardless of cwd
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
BROKER_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$BROKER_ROOT"

# Load broker.env
ENV_FILE="$BROKER_ROOT/broker.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: broker.env not found at $ENV_FILE" >&2
    echo "       Create a private broker.env with explicit loopback settings first." >&2
    exit 1
fi
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

# Resolve venv python (Linux uses bin/python; Git Bash on Windows uses Scripts/python.exe)
if [ -x "$BROKER_ROOT/.venv/bin/python" ]; then
    VENV_PYTHON="$BROKER_ROOT/.venv/bin/python"
elif [ -x "$BROKER_ROOT/.venv/Scripts/python.exe" ]; then
    VENV_PYTHON="$BROKER_ROOT/.venv/Scripts/python.exe"
else
    echo "ERROR: venv python not found. Expected at .venv/bin/python or .venv/Scripts/python.exe" >&2
    echo "       Run: python -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
    exit 1
fi

echo ""
echo "Launching broker:"
echo "  cwd:    $BROKER_ROOT"
echo "  python: $VENV_PYTHON"
echo "  host:   ${BROKER_HOST:-(unset)}"
echo "  port:   ${BROKER_PORT:-(unset)}"
echo "  db:     ${BROKER_DB_PATH:-(unset)}"
echo ""
echo "Ctrl+C to stop."
echo ""

export PYTHONDONTWRITEBYTECODE=1
exec "$VENV_PYTHON" -B "$BROKER_ROOT/server.py"
