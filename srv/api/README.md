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

## Endpoints
- `GET /api/v1/health`
- `GET /api/v1/systems/search`
- `GET /api/v1/systems/{system_id}`
- `GET /api/v1/systems/by-key/{stable_object_key}`

## Integration test

```bash
python /data/spacegate/scripts/test_api_integration.py http://localhost:8000/api/v1
```
