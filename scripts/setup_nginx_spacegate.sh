#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SITES_AVAILABLE="/etc/nginx/sites-available"
SITES_ENABLED="/etc/nginx/sites-enabled"
CONF_PATH="$SITES_AVAILABLE/spacegate.conf"
LINK_PATH="$SITES_ENABLED/spacegate.conf"

SERVER_NAME="${SPACEGATE_SERVER_NAME:-_}"
WEB_DIST_DEFAULT="/data/spacegate/srv/web/dist"
WEB_DIST="${SPACEGATE_WEB_DIST:-$WEB_DIST_DEFAULT}"
API_UPSTREAM="${SPACEGATE_API_UPSTREAM:-http://127.0.0.1:8000}"

usage() {
  cat <<'USAGE'
Usage:
  sudo scripts/setup_nginx_spacegate.sh [--force]

Idempotent nginx setup for Spacegate.

Options:
  --force   Overwrite /etc/nginx/sites-available/spacegate.conf even if not managed.
USAGE
}

require_root() {
  if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
    echo "Error: this script must be run with sudo/root." >&2
    exit 1
  fi
}

require_debian() {
  if [[ ! -f /etc/os-release ]]; then
    echo "Error: /etc/os-release not found; expected Debian/Ubuntu." >&2
    exit 1
  fi
  # shellcheck disable=SC1091
  . /etc/os-release
  case "${ID:-}" in
    debian|ubuntu)
      return 0
      ;;
    *)
      if [[ "${ID_LIKE:-}" == *debian* ]]; then
        return 0
      fi
      echo "Error: unsupported OS (expected Debian/Ubuntu)." >&2
      exit 1
      ;;
  esac
}

ensure_nginx() {
  if ! command -v nginx >/dev/null 2>&1; then
    echo "Installing nginx..." >&2
    apt-get update
    apt-get install -y nginx
  fi
}

nginx_active() {
  systemctl is-active --quiet nginx
}

port_owner() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -ltnpH | awk -v port="$port" 'match($4, /:([0-9]+)$/, m) && m[1] == port {print; exit}'
  elif command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN | awk 'NR==2{print $1, $2, $9}'
  else
    return 1
  fi
}

choose_listen_port() {
  local owner
  owner="$(port_owner 80 || true)"
  if [[ -z "$owner" ]]; then
    LISTEN_PORT=80
    PORT_OWNER=""
    return 0
  fi
  if echo "$owner" | grep -qi nginx; then
    LISTEN_PORT=80
    PORT_OWNER="$owner"
    return 0
  fi
  LISTEN_PORT=8080
  PORT_OWNER="$owner"
}

ensure_managed_config() {
  if [[ -f "$CONF_PATH" ]]; then
    if ! grep -q "Managed by Spacegate setup script" "$CONF_PATH"; then
      if [[ "${FORCE_OVERWRITE:-0}" -eq 1 ]]; then
        echo "Warning: overwriting unmanaged config at $CONF_PATH due to --force." >&2
        return 0
      fi
      echo "Error: $CONF_PATH exists but is not managed by Spacegate." >&2
      echo "Refusing to overwrite. Re-run with --force to override." >&2
      exit 1
    fi
  fi
}

write_config() {
  local dist_path="$WEB_DIST"
  if [[ ! -d "$dist_path" ]]; then
    if [[ -d "$ROOT_DIR/srv/web/dist" ]]; then
      dist_path="$ROOT_DIR/srv/web/dist"
    fi
  fi

  local has_dist=0
  if [[ -d "$dist_path" ]]; then
    has_dist=1
  fi

  cat >"$CONF_PATH" <<EOF_CONF
# Managed by Spacegate setup script. Do not edit by hand.
# Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")

server {
    listen ${LISTEN_PORT};
    server_name ${SERVER_NAME};

    # Proxy API
    location /api/ {
        proxy_pass ${API_UPSTREAM};
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
EOF_CONF

  if [[ $has_dist -eq 1 ]]; then
    cat >>"$CONF_PATH" <<EOF_CONF

    # Static web UI
    root ${dist_path};
    index index.html;

    location / {
        try_files \$uri /index.html;
    }
}
EOF_CONF
  else
    cat >>"$CONF_PATH" <<EOF_CONF

    # Static web UI not found at ${dist_path}
    location / {
        return 404;
    }
}
EOF_CONF
  fi
}

ensure_symlink() {
  if [[ -e "$LINK_PATH" ]]; then
    if [[ -L "$LINK_PATH" ]]; then
      local target
      target="$(readlink -f "$LINK_PATH")"
      if [[ "$target" == "$CONF_PATH" ]]; then
        return 0
      fi
    fi
    echo "Warning: $LINK_PATH exists and is not managed by Spacegate. Leaving as-is." >&2
    return 0
  fi
  ln -s "$CONF_PATH" "$LINK_PATH"
}

validate_and_reload() {
  if ! nginx -t; then
    echo "nginx -t failed; not reloading." >&2
    exit 1
  fi

  if nginx_active; then
    systemctl reload nginx
    return 0
  fi

  # Start + enable if possible
  systemctl enable nginx >/dev/null 2>&1 || true
  systemctl start nginx
}

print_summary() {
  echo ""
  echo "Spacegate nginx setup complete."
  echo "Chosen port: $LISTEN_PORT"
  if [[ -n "$PORT_OWNER" && $LISTEN_PORT -eq 8080 ]]; then
    echo "Port 80 in use by: $PORT_OWNER"
  fi
  if [[ "$SERVER_NAME" == "_" ]]; then
    echo "Test URL: http://localhost:${LISTEN_PORT}/"
  else
    echo "Test URL: http://${SERVER_NAME}:${LISTEN_PORT}/"
  fi
  echo "API check: http://localhost:${LISTEN_PORT}/api/v1/health"
  echo "Logs:"
  echo "  sudo tail -f /var/log/nginx/access.log"
  echo "  sudo tail -f /var/log/nginx/error.log"
  echo "  sudo journalctl -u nginx -f"
}

main() {
  FORCE_OVERWRITE=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --force)
        FORCE_OVERWRITE=1
        shift 1
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "Unknown argument: $1" >&2
        usage
        exit 1
        ;;
    esac
  done

  require_root
  require_debian
  ensure_nginx

  choose_listen_port

  ensure_managed_config
  write_config
  ensure_symlink
  validate_and_reload
  print_summary
}

main "$@"
