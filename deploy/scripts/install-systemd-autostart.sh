#!/usr/bin/env bash
# Explicit source-use systemd user registrar (not a cross-platform harness installer).
# Install source/dependencies and a private loopback broker.env at
# $HOME/.agents/services/broker first. This command does not start the service.
# Usage: bash scripts/install-systemd-autostart.sh [--uninstall]

set -e

UNIT_NAME="agent-broker.service"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
SOURCE_UNIT="$SCRIPT_DIR/$UNIT_NAME"
USER_UNIT_DIR="$HOME/.config/systemd/user"
TARGET_UNIT="$USER_UNIT_DIR/$UNIT_NAME"
BROKER_ROOT="$HOME/.agents/services/broker"

if [ "${1:-}" = "--uninstall" ]; then
    echo "Uninstalling $UNIT_NAME..."
    systemctl --user disable "$UNIT_NAME" 2>/dev/null || true
    rm -f "$TARGET_UNIT"
    systemctl --user daemon-reload
    echo "Unit removed; a running service was not stopped."
    exit 0
fi

if [ ! -f "$SOURCE_UNIT" ]; then
    echo "ERROR: source unit file not found at $SOURCE_UNIT" >&2
    exit 1
fi

# Sanity: verify the paths inside the unit exist
if [ ! -f "$BROKER_ROOT/broker.env" ]; then
    echo "ERROR: $BROKER_ROOT/broker.env not found." >&2
    echo "       Deploy broker code + broker.env before running this installer." >&2
    exit 1
fi
if [ ! -x "$BROKER_ROOT/.venv/bin/python" ]; then
    echo "ERROR: $BROKER_ROOT/.venv/bin/python not found." >&2
    echo "       Run: cd $BROKER_ROOT && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
    exit 1
fi

# Linger note (informational; needs root to enable, can't auto-enable)
if ! loginctl show-user "$USER" 2>/dev/null | grep -q '^Linger=yes'; then
    echo "NOTE: Linger is not enabled for $USER."
    echo "      Service will only run while user is logged in (or the user session exists)."
    echo "      To survive full logout, ask root to run once: loginctl enable-linger $USER"
    echo ""
fi

mkdir -p "$USER_UNIT_DIR"
chmod 700 "$USER_UNIT_DIR"
chmod 600 "$BROKER_ROOT/broker.env"
cp "$SOURCE_UNIT" "$TARGET_UNIT"

systemctl --user daemon-reload
systemctl --user enable "$UNIT_NAME"

echo "Registered $UNIT_NAME without starting it."
echo "Activate explicitly: systemctl --user start $UNIT_NAME"
echo "Then inspect: systemctl --user status $UNIT_NAME --no-pager"
echo "Check /api/health at your retained loopback port."
