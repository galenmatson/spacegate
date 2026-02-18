# Runtime Notes Template (Host-Local)

Use this file as a template for host-specific operations notes.

Keep host-local copy outside git:

- Recommended path: `/srv/spacegate/RUNTIME.md`
- Do not commit host-private values or incident notes into the repo.

## Host Identity

- Hostname:
- Environment (prod/stage/dev):
- Public domain(s):
- Public IP:
- Last updated:

## Paths

- Repo checkout:
- Runtime data root:
- Public download directory:
- Nginx site config path:
- Fail2ban local config paths:

## Runtime Mode

- Docker compose file:
- API bind:
- Web bind:
- Public ingress:

## Runtime Environment Values

- `SPACEGATE_DATA_DIR`:
- `SPACEGATE_STATE_DIR`:
- `SPACEGATE_CACHE_DIR`:
- `SPACEGATE_API_DUCKDB_MEMORY_LIMIT`:
- `SPACEGATE_API_DUCKDB_THREADS`:
- `SPACEGATE_STATUS_PUBLIC_URL`:

## Bootstrap Source

- Metadata URL:
- Artifact base URL:
- Last promoted build id:
- Current archive symlink target:

## Start / Restart Commands

```bash
cd <repo>
SPACEGATE_DATA_DIR=<...> \
SPACEGATE_API_DUCKDB_MEMORY_LIMIT=<...> \
SPACEGATE_API_DUCKDB_THREADS=<...> \
docker compose up -d --build
```

## Nginx Apply Command

```bash
cd <repo>
sudo SPACEGATE_SERVER_NAME="<domains>" \
  SPACEGATE_TLS_ENABLE=1 \
  SPACEGATE_TLS_CERT_FILE=<fullchain.pem> \
  SPACEGATE_TLS_KEY_FILE=<privkey.pem> \
  scripts/setup_nginx_spacegate.sh --force --container-web
```

## Fail2ban

- Enabled jails:
- Last config validation:
- Notes:

## Verification Commands

```bash
cd <repo>
scripts/spacegate_status.sh --public-url https://<domain>
scripts/ops_report.sh --public-url https://<domain>
docker compose ps
curl -fsS https://<domain>/api/v1/health
sudo fail2ban-client status
```

## Backup Scope

- Data:
- Downloads:
- Nginx config:
- Fail2ban config:
- Restore drill date:

## Change Log

- YYYY-MM-DD: ...
