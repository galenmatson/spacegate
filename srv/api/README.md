# Spacegate API (v0.1)

Read-only FastAPI service for browsing the core DuckDB database.

## Run

```bash
cd /data/spacegate/srv/api
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Optional: point to a different build
export SPACEGATE_STATE_DIR=/data/spacegate/data
# or export SPACEGATE_DB_PATH=/data/spacegate/data/served/current/core.duckdb

uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Optional admin auth (v0.1.5 Checkpoint A)

Auth is disabled by default. To enable Google OIDC login for `/admin`:

```bash
export SPACEGATE_AUTH_ENABLE=1
export SPACEGATE_OIDC_PROVIDER=google
export SPACEGATE_OIDC_ISSUER=https://accounts.google.com
export SPACEGATE_OIDC_CLIENT_ID=...
export SPACEGATE_OIDC_CLIENT_SECRET=...
export SPACEGATE_OIDC_REDIRECT_URI=https://spacegates.org/api/v1/auth/callback/google
export SPACEGATE_AUTH_SUCCESS_REDIRECT=/api/v1/admin/ui
export SPACEGATE_SESSION_SECRET=... # high entropy secret
export SPACEGATE_ADMIN_ALLOWLIST_EMAILS=you@example.com
```

Optional:
- `SPACEGATE_ADMIN_DB_PATH` (default: `$SPACEGATE_STATE_DIR/admin/admin.sqlite3`)
- `SPACEGATE_ADMIN_JOBS_DIR` (default: `$SPACEGATE_STATE_DIR/admin/jobs`)
- `SPACEGATE_ADMIN_MAX_RUNNING_JOBS` (default `1`)
- `SPACEGATE_ADMIN_MAX_QUEUED_JOBS` (default `20`)
- `SPACEGATE_ADMIN_BACKUPS_DIR` (default: `$SPACEGATE_STATE_DIR/admin/backups`)
- `SPACEGATE_SESSION_TTL_HOURS` (default `12`)
- `SPACEGATE_SESSION_IDLE_MINUTES` (default `60`)
- `SPACEGATE_CSRF_ENABLE` (default `1`)

## Endpoints
- `GET /api/v1/health`
- `GET /api/v1/systems/search`
- `GET /api/v1/systems/{system_id}`
- `GET /api/v1/systems/by-key/{stable_object_key}`
- `GET /api/v1/auth/login/google`
- `GET /api/v1/auth/callback/google`
- `POST /api/v1/auth/logout`
- `GET /api/v1/auth/me`
- `GET /api/v1/admin/status` (admin only)
- `GET /api/v1/admin/actions/catalog` (admin only)
- `POST /api/v1/admin/actions/run` (admin only, CSRF required)
- `GET /api/v1/admin/actions/jobs` (admin only)
- `GET /api/v1/admin/actions/jobs/{job_id}` (admin only)
- `GET /api/v1/admin/actions/jobs/{job_id}/log` (admin only)
- `GET /api/v1/admin/actions/jobs/{job_id}/log/download` (admin only)
- `POST /api/v1/admin/actions/jobs/{job_id}/cancel` (admin only, CSRF required)
- `GET /api/v1/admin/backups` (admin only)
- `GET /api/v1/admin/audit` (admin only; audit/event feed with filters)
- `GET /api/v1/admin/ui` (admin page scaffold; preferred behind nginx)
- `GET /admin` (admin page scaffold when API is exposed directly)

## Integration test

```bash
python /data/spacegate/scripts/test_api_integration.py http://localhost:8000/api/v1
```
