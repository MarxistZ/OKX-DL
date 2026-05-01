#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_DIR="${OKX_DAILY_CONFIG_DIR:-$HOME/.config}"
ENV_FILE="${OKX_DAILY_ENV_FILE:-$ENV_DIR/okx-lob-daily.env}"
SYSTEMD_USER_DIR="${SYSTEMD_USER_DIR:-$HOME/.config/systemd/user}"
TIMER_NAME="okx-lob-daily.timer"
SERVICE_NAME="okx-lob-daily.service"

mkdir -p "$ENV_DIR" "$SYSTEMD_USER_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
  cp "$ROOT_DIR/deploy/okx-lob-daily.env.example" "$ENV_FILE"
  echo "created env: $ENV_FILE"
else
  echo "keeping existing env: $ENV_FILE"
fi

install -m 0644 "$ROOT_DIR/deploy/systemd/$SERVICE_NAME" "$SYSTEMD_USER_DIR/$SERVICE_NAME"
install -m 0644 "$ROOT_DIR/deploy/systemd/$TIMER_NAME" "$SYSTEMD_USER_DIR/$TIMER_NAME"
echo "installed user systemd units under: $SYSTEMD_USER_DIR"

systemctl --user daemon-reload
systemctl --user enable --now "$TIMER_NAME"

if command -v loginctl >/dev/null 2>&1; then
  if ! loginctl show-user "$USER" -p Linger 2>/dev/null | grep -q 'Linger=yes'; then
    echo "enabling lingering so the user timer can run after SSH logout"
    if ! sudo loginctl enable-linger "$USER"; then
      echo "warning: failed to enable lingering; run this manually if timers stop after SSH logout:"
      echo "  sudo loginctl enable-linger $USER"
    fi
  fi
fi

echo
echo "timer status:"
systemctl --user list-timers --all "$TIMER_NAME"

echo
echo "manual run:"
echo "  systemctl --user start $SERVICE_NAME"
echo
echo "logs:"
echo "  journalctl --user -u $SERVICE_NAME -f"
echo
echo "env file:"
echo "  $ENV_FILE"
