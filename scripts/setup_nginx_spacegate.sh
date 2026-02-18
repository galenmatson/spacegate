#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SITES_AVAILABLE="/etc/nginx/sites-available"
SITES_ENABLED="/etc/nginx/sites-enabled"
CONF_PATH="$SITES_AVAILABLE/spacegate.conf"
LINK_PATH="$SITES_ENABLED/spacegate.conf"

SERVER_NAME="${SPACEGATE_SERVER_NAME:-_}"
WEB_DIST_DEFAULT="$ROOT_DIR/srv/web/dist"
WEB_DIST="${SPACEGATE_WEB_DIST:-$WEB_DIST_DEFAULT}"
API_UPSTREAM="${SPACEGATE_API_UPSTREAM:-http://127.0.0.1:8000}"
# Default web mode is container web proxy. Set SPACEGATE_WEB_UPSTREAM='' or use
# --static-web to serve local dist assets directly from host nginx.
WEB_UPSTREAM="${SPACEGATE_WEB_UPSTREAM-http://127.0.0.1:8081}"
DL_ENABLE="${SPACEGATE_DL_ENABLE:-1}"
DL_ALIAS_DIR="${SPACEGATE_DL_ALIAS_DIR:-/srv/spacegate/dl}"
WEB_CONFIG_MODE=""
API_RATE_RPS="${SPACEGATE_API_RATE_RPS:-20}"
API_RATE_BURST="${SPACEGATE_API_RATE_BURST:-40}"
API_CONN_LIMIT="${SPACEGATE_API_CONN_LIMIT:-40}"
PROXY_CONNECT_TIMEOUT="${SPACEGATE_PROXY_CONNECT_TIMEOUT:-5s}"
PROXY_READ_TIMEOUT="${SPACEGATE_PROXY_READ_TIMEOUT:-60s}"
PROXY_SEND_TIMEOUT="${SPACEGATE_PROXY_SEND_TIMEOUT:-60s}"
TLS_ENABLE="${SPACEGATE_TLS_ENABLE:-0}"
TLS_CERT_FILE="${SPACEGATE_TLS_CERT_FILE:-}"
TLS_KEY_FILE="${SPACEGATE_TLS_KEY_FILE:-}"
TLS_INCLUDE_FILE="${SPACEGATE_TLS_INCLUDE_FILE:-/etc/letsencrypt/options-ssl-nginx.conf}"
TLS_DHPARAM_FILE="${SPACEGATE_TLS_DHPARAM_FILE:-/etc/letsencrypt/ssl-dhparams.pem}"

usage() {
  cat <<'USAGE'
Usage:
  sudo scripts/setup_nginx_spacegate.sh [--force] [--container-web|--static-web]

Idempotent nginx setup for Spacegate.

Options:
  --force   Overwrite /etc/nginx/sites-available/spacegate.conf even if not managed.
  --container-web  Proxy / to container web UI (default: http://127.0.0.1:8081).
  --static-web     Serve local static web UI from srv/web/dist (host filesystem).
Environment:
  SPACEGATE_WEB_UPSTREAM  Web upstream URL (default: http://127.0.0.1:8081). Set empty for static mode.
  SPACEGATE_WEB_DIST      Static web dist path for --static-web mode.
  SPACEGATE_API_RATE_RPS  API request rate limit per IP (default: 20).
  SPACEGATE_API_RATE_BURST API burst above rate limit (default: 40).
  SPACEGATE_API_CONN_LIMIT Concurrent API connections per IP (default: 40).
  SPACEGATE_PROXY_CONNECT_TIMEOUT Proxy connect timeout (default: 5s).
  SPACEGATE_PROXY_READ_TIMEOUT    Proxy read timeout (default: 60s).
  SPACEGATE_PROXY_SEND_TIMEOUT    Proxy send timeout (default: 60s).
  SPACEGATE_TLS_ENABLE     Set to 1 to enable HTTPS server + HTTP redirect.
  SPACEGATE_TLS_CERT_FILE  TLS fullchain cert path (required when TLS enabled).
  SPACEGATE_TLS_KEY_FILE   TLS private key path (required when TLS enabled).
  SPACEGATE_TLS_INCLUDE_FILE Optional ssl include (default letsencrypt options file).
  SPACEGATE_TLS_DHPARAM_FILE Optional dhparam file path.
  SPACEGATE_DL_ENABLE     Enable /dl/ static download endpoint (default: 1).
  SPACEGATE_DL_ALIAS_DIR  Directory served at /dl/ (default: /srv/spacegate/dl).
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

warn_server_name_conflict() {
  if [[ "$SERVER_NAME" != "_" ]]; then
    return 0
  fi
  if [[ -d "$SITES_ENABLED" ]]; then
    local conflicts
    conflicts="$(grep -R "server_name _;" "$SITES_ENABLED" 2>/dev/null | grep -v "$CONF_PATH" || true)"
    if [[ -n "$conflicts" ]]; then
      echo "Warning: another nginx site uses server_name '_' on this host." >&2
      echo "Requests by IP/localhost may hit that site instead of Spacegate." >&2
      echo "Tip: set SPACEGATE_SERVER_NAME to your domain or IP and re-run this script." >&2
    fi
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

validate_tls_config() {
  if [[ "$TLS_ENABLE" != "1" ]]; then
    return 0
  fi
  if [[ -z "$TLS_CERT_FILE" || -z "$TLS_KEY_FILE" ]]; then
    echo "Error: TLS enabled but cert/key paths are missing." >&2
    echo "Set SPACEGATE_TLS_CERT_FILE and SPACEGATE_TLS_KEY_FILE." >&2
    exit 1
  fi
  if [[ ! -f "$TLS_CERT_FILE" ]]; then
    echo "Error: TLS cert file not found: $TLS_CERT_FILE" >&2
    exit 1
  fi
  if [[ ! -f "$TLS_KEY_FILE" ]]; then
    echo "Error: TLS key file not found: $TLS_KEY_FILE" >&2
    exit 1
  fi
}

write_config() {
  local dist_path="$WEB_DIST"
  local dl_alias_dir="${DL_ALIAS_DIR%/}"
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
# Rate-limit zones (http context)
limit_req_zone \$binary_remote_addr zone=sg_api_rate:10m rate=${API_RATE_RPS}r/s;
limit_conn_zone \$binary_remote_addr zone=sg_api_conn:10m;
EOF_CONF

  if [[ "$TLS_ENABLE" == "1" ]]; then
    cat >>"$CONF_PATH" <<EOF_CONF

server {
    listen 443 ssl;
    server_name ${SERVER_NAME};
    limit_req_status 429;
    ssl_certificate ${TLS_CERT_FILE};
    ssl_certificate_key ${TLS_KEY_FILE};
EOF_CONF
    if [[ -f "$TLS_INCLUDE_FILE" ]]; then
      cat >>"$CONF_PATH" <<EOF_CONF
    include ${TLS_INCLUDE_FILE};
EOF_CONF
    fi
    if [[ -f "$TLS_DHPARAM_FILE" ]]; then
      cat >>"$CONF_PATH" <<EOF_CONF
    ssl_dhparam ${TLS_DHPARAM_FILE};
EOF_CONF
    fi
  else
    cat >>"$CONF_PATH" <<EOF_CONF

server {
    listen ${LISTEN_PORT};
    server_name ${SERVER_NAME};
    limit_req_status 429;
EOF_CONF
  fi

  if [[ "$DL_ENABLE" != "0" ]]; then
    cat >>"$CONF_PATH" <<EOF_CONF

    # Public DB downloads
    location /dl/ {
        alias ${dl_alias_dir}/;
        autoindex off;
        add_header Cache-Control "public, max-age=3600";
        types { application/octet-stream 7z; }
        default_type application/octet-stream;
    }
EOF_CONF
  fi

  cat >>"$CONF_PATH" <<EOF_CONF

    # Proxy API
    location /api/ {
        limit_req zone=sg_api_rate burst=${API_RATE_BURST};
        limit_conn sg_api_conn ${API_CONN_LIMIT};
        proxy_pass ${API_UPSTREAM};
        proxy_connect_timeout ${PROXY_CONNECT_TIMEOUT};
        proxy_read_timeout ${PROXY_READ_TIMEOUT};
        proxy_send_timeout ${PROXY_SEND_TIMEOUT};
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
EOF_CONF

  if [[ -n "$WEB_UPSTREAM" ]]; then
    WEB_CONFIG_MODE="proxy:${WEB_UPSTREAM}"
    cat >>"$CONF_PATH" <<EOF_CONF

    # Proxy Web UI
    location / {
        proxy_pass ${WEB_UPSTREAM};
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF_CONF
  elif [[ $has_dist -eq 1 ]]; then
    WEB_CONFIG_MODE="static:${dist_path}"
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
    WEB_CONFIG_MODE="static-missing:${dist_path}"
    cat >>"$CONF_PATH" <<EOF_CONF

    # Static web UI not found at ${dist_path}
    location / {
        return 404;
    }
}
EOF_CONF
  fi

  if [[ "$TLS_ENABLE" == "1" ]]; then
    cat >>"$CONF_PATH" <<EOF_CONF

server {
    listen ${LISTEN_PORT};
    server_name ${SERVER_NAME};
    return 301 https://\$host\$request_uri;
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
  local scheme="http"
  if [[ "$TLS_ENABLE" == "1" ]]; then
    scheme="https"
  fi

  echo ""
  echo "Spacegate nginx setup complete."
  echo "Chosen port: $LISTEN_PORT"
  if [[ -n "$PORT_OWNER" && $LISTEN_PORT -eq 8080 ]]; then
    echo "Port 80 in use by: $PORT_OWNER"
  fi
  if [[ "$SERVER_NAME" == "_" ]]; then
    echo "Test URL: ${scheme}://localhost:${LISTEN_PORT}/"
    echo "Tip: if you see the nginx welcome page, re-run with SPACEGATE_SERVER_NAME=your.domain.or.ip"
  else
    echo "Test URL: ${scheme}://${SERVER_NAME}:${LISTEN_PORT}/"
  fi
  echo "API check: ${scheme}://localhost:${LISTEN_PORT}/api/v1/health"
  if [[ -n "$WEB_CONFIG_MODE" ]]; then
    echo "Web mode: $WEB_CONFIG_MODE"
  fi
  echo "Logs:"
  echo "  sudo tail -f /var/log/nginx/access.log"
  echo "  sudo tail -f /var/log/nginx/error.log"
  echo "  sudo journalctl -u nginx -f"
}

main() {
  FORCE_OVERWRITE=0
  CONTAINER_WEB=0
  STATIC_WEB=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --force)
        FORCE_OVERWRITE=1
        shift 1
        ;;
      --container-web)
        CONTAINER_WEB=1
        STATIC_WEB=0
        shift 1
        ;;
      --static-web)
        STATIC_WEB=1
        CONTAINER_WEB=0
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
  if [[ $STATIC_WEB -eq 1 ]]; then
    WEB_UPSTREAM=""
  elif [[ $CONTAINER_WEB -eq 1 && -z "$WEB_UPSTREAM" ]]; then
    WEB_UPSTREAM="http://127.0.0.1:8081"
  fi

  warn_server_name_conflict
  ensure_managed_config
  validate_tls_config
  write_config
  ensure_symlink
  validate_and_reload
  print_summary
}

main "$@"
