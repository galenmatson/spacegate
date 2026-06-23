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
- `SPACEGATE_DL_ROOT`:
- `SPACEGATE_CACHE_DIR`:
- `SPACEGATE_API_HOST_PORT`:
- `SPACEGATE_WEB_BIND`:
- `SPACEGATE_WEB_HOST_PORT`:
- `SPACEGATE_API_DUCKDB_MEMORY_LIMIT`:
- `SPACEGATE_API_DUCKDB_THREADS`:
- `SPACEGATE_STATUS_PUBLIC_URL`:

Operational note:

- `docker-compose.yml` only covers container runtime env.
- Host-side scripts such as `scripts/promote_build.sh`, `scripts/publish_db.sh`, and `scripts/push_published_db.sh` load env from `/etc/spacegate/spacegate.env`, `.spacegate.env`, and `.spacegate.local.env`.
- On server hosts, set `SPACEGATE_STATE_DIR` and `SPACEGATE_DL_ROOT` in the host env files so container runtime and host-side operational scripts agree on the same paths.
- Keep `/etc/spacegate` owned by `root:spacegate` with mode `2750`. The setgid bit makes replaced files inherit group `spacegate`, which prevents root-owned editor temp-file saves from changing `/etc/spacegate/spacegate.env` back to `root:root`.
- Keep `/etc/spacegate/spacegate.env` owned by `root:spacegate` with mode `0640`; it may contain OIDC secrets, API keys, and the session signing secret.

## Bootstrap Source

- Metadata URL:
- Artifact base URL:
- Last promoted build id:
- Current archive symlink target:

## Start / Restart Commands

```bash
cd <repo>
SPACEGATE_API_DUCKDB_MEMORY_LIMIT=<...> \
SPACEGATE_API_DUCKDB_THREADS=<...> \
scripts/compose_spacegate.sh up -d --build
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
