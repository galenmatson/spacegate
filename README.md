#  What Spacegate Is

  - A public, richly browseable 3D star map + worldbuilding layer grounded in real astronomy. It prioritizes fun exploration and factual,
    engaging descriptions while keeping core science data immutable and provenance‑clean.

#  Scope and Deliverables

  - Core datasets: systems, stars, planets (AT‑HYG + NASA Exoplanet Archive).
  - Optional “packs” (v2.1+): substellar, compact, superstellar, etc., as separate, read‑only artifacts.
  - Rich (v1.1+): derived artifacts like blurbs, reference links, snapshots.
  - A browser 3D map (v2) with filters and overlays.

# Data & Pipeline Model

  - data/raw/ immutable upstream artifacts.
  - data/cooked/ normalized, catalog‑shaped (no joins).
  - data/served/ queryable outputs; data/served/current points to the promoted data/out/<build_id>/.
  - Strong provenance required in all rows.
  - Default state lives under `./data` so it can be mounted as a single volume in containers.

## Schema / Rules Highlights

  - Core artifacts: DuckDB + Parquet, sorted by Morton Z‑order spatial_index.
  - Stable object keys for systems/stars/planets; strict provenance fields required 100%.
  - Planet → host matching prioritized by Gaia DR3 ID, then HIP, HD, then hostname.

## Packs Contract

  - Pack schema requires stable_object_key, object type, coordinates, and full provenance.
  - Discovered via packs_manifest.json.

## Configuration

  Spacegate uses environment variables to locate persistent data and runtime state.

### For local development, defaults live under `./data`. You may override with:
  - export SPACEGATE_STATE_DIR=./data
  - export SPACEGATE_CACHE_DIR=./data/cache
  - export SPACEGATE_LOG_DIR=./data/logs
These directories are ignored by git and may be safely deleted.
Depending on which catalogs you download the data directory can be quite large, over 100 GB.
If you intend to download all of the raw astronomical data, consider locating 
  SPACEGATE_STATE_DIR on a separate volume from the root.
  
### For production deployments, standard Linux locations are recommended:
  - /srv/spacegate            # web and api servers
  - /var/lib/spacegate        
  - /var/cache/spacegate
  - /var/log/spacegate
  - /etc/spacegate

## Quickstart (from scratch)

Running Spacegate (API + web UI) from scratch.

### 1) Install system prerequisites
You need Python, Node (18+), and basic download tools available on your PATH:

- `python3` + `pip`
- `node` (v18+) + `npm`
- `git`, `curl`, `aria2c`, `gzip`, `7z`

On Debian/Ubuntu, install base tools first, then install Node.js 20:

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip git curl aria2 gzip p7zip-full ca-certificates gnupg
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs
```

### 2) Clone and install dependencies
The installer creates virtualenvs, installs Python/web dependencies, and if needed bootstraps the current prebuilt core DB from `https://spacegates.org/dl/current.json` (with fallback to local source build).

```bash
git clone https://github.com/galenmatson/spacegate.git
cd spacegate

./install_spacegate.sh
```

Optional flags:
- `--overwrite` re-downloads installer inputs even if present.
- `--skip-web` skips the web UI dependency install.
- `--skip-build` skips the data build step.
- `--skip-db-download` skips prebuilt DB bootstrap and builds from catalogs instead.

### 3) Build the core database from source (if needed)
If you used `--skip-build`, used `--skip-db-download`, or want to rebuild:

```bash
scripts/build_core.sh
```

To fetch the currently published prebuilt DB manually:

```bash
scripts/bootstrap_core_db.sh
```

For server deploys, pass the target state dir inline so files land where Docker mounts them:

```bash
SPACEGATE_STATE_DIR=/srv/spacegate/data \
SPACEGATE_CACHE_DIR=/srv/spacegate/data/cache \
scripts/bootstrap_core_db.sh
```

### 4) Run Spacegate API (default mode)
The launcher verifies the database, then starts the API service:

```bash
scripts/run_spacegate.sh
```

Defaults:
- API: `http://0.0.0.0:8000`

For local UI development, opt in to the Vite dev server:

```bash
scripts/run_spacegate.sh --web-dev
```

Dev web default:
- Web UI: `http://0.0.0.0:5173`

### 5) Stop or restart

```bash
scripts/run_spacegate.sh --stop
scripts/run_spacegate.sh --restart
```

### 6) Status monitor (color terminal dashboard)

```bash
scripts/spacegate_status.sh
scripts/spacegate_status.sh --watch 2
```

### 7) Stress tester (load + latency gates)

```bash
scripts/spacegate_stress.sh --profile smoke --url http://192.168.1.102
scripts/spacegate_stress.sh --profile sustain --url http://192.168.1.102 --duration 600
```

### Optional configuration

```bash
# Data locations (defaults to ./data)
export SPACEGATE_STATE_DIR=/var/lib/spacegate
export SPACEGATE_CACHE_DIR=/var/cache/spacegate
export SPACEGATE_LOG_DIR=/var/log/spacegate
# Alias used by bootstrap script (if SPACEGATE_STATE_DIR is unset)
export SPACEGATE_DATA_DIR=/var/lib/spacegate

# DuckDB resources (otherwise auto-detected)
export SPACEGATE_DUCKDB_MEMORY_LIMIT=24GB
export SPACEGATE_DUCKDB_THREADS=4

# Core DB bootstrap controls
export SPACEGATE_BOOTSTRAP_DB=0
export SPACEGATE_BOOTSTRAP_META_URL=https://spacegates.org/dl/current.json

# Web runtime mode for scripts/run_spacegate.sh
# 0 = API only (default), 1 = API + Vite dev server
export SPACEGATE_WEB_ENABLE=1
```

## Nginx setup (optional)

For release deployments, use nginx in front of the API and web containers (`/api` -> `127.0.0.1:8000`, `/` -> `127.0.0.1:8081`):

```bash
sudo scripts/setup_nginx_spacegate.sh
```

Behavior:
- Uses port 80 if free or already owned by nginx.
- Falls back to port 8080 if port 80 is in use by a non‑nginx process.
- Writes `/etc/nginx/sites-available/spacegate.conf` with provenance comments.
- Symlinks to `/etc/nginx/sites-enabled/spacegate.conf` (without touching other sites).
- Proxies web UI to container upstream `http://127.0.0.1:8081` by default.
- Applies API abuse controls by default: per-IP rate limit, burst limit, and connection limit.
- Applies proxy timeouts on API upstream connections.
- Serves `/dl/` from `/srv/spacegate/dl` by default (`SPACEGATE_DL_ENABLE=0` to disable).
- Runs `nginx -t` before reload/start.

If you prefer host-served static files from `srv/web/dist`, use:

```bash
sudo scripts/setup_nginx_spacegate.sh --static-web --force
```

Tip: if you access by IP or a specific hostname, set it explicitly:

```bash
sudo SPACEGATE_SERVER_NAME="192.168.1.102" scripts/setup_nginx_spacegate.sh --force
```

Rate-limit and timeout tuning example:

```bash
sudo SPACEGATE_SERVER_NAME="spacegates.org www.spacegates.org" \
  SPACEGATE_API_RATE_RPS=15 \
  SPACEGATE_API_RATE_BURST=30 \
  SPACEGATE_API_CONN_LIMIT=30 \
  SPACEGATE_PROXY_READ_TIMEOUT=45s \
  scripts/setup_nginx_spacegate.sh --force
```

Optional HTTPS enforcement from this script (requires cert files):

```bash
sudo SPACEGATE_SERVER_NAME="spacegates.org www.spacegates.org" \
  SPACEGATE_TLS_ENABLE=1 \
  SPACEGATE_TLS_CERT_FILE=/etc/letsencrypt/live/spacegates.org/fullchain.pem \
  SPACEGATE_TLS_KEY_FILE=/etc/letsencrypt/live/spacegates.org/privkey.pem \
  scripts/setup_nginx_spacegate.sh --force
```

### HTTPS alternatives

You can either:

1. Use script-managed TLS via `SPACEGATE_TLS_ENABLE=1` and cert/key paths (shown above), or
2. Keep script-managed HTTP and let certbot edit nginx:
   ```bash
   sudo certbot --nginx -d spacegates.org -d www.spacegates.org
   ```

If you use certbot-managed TLS, re-running `scripts/setup_nginx_spacegate.sh --force` will overwrite certbot's nginx edits.

### Systemd (optional, recommended for servers)

Install and start the Spacegate API as a systemd service:

```bash
sudo scripts/install_spacegate_systemd.sh
```

This runs uvicorn with `--proxy-headers` and `--forwarded-allow-ips="*"` so nginx can forward scheme/client IP.

## Docker (optional)

Spacegate can run in Docker with two containers: API + web. The web container serves the static UI and proxies `/api` to the API container.

### Build and run

```bash
docker compose up --build
```

This exposes:
- API: `http://127.0.0.1:8000`
- Web container: `http://127.0.0.1:8081`

Note: compose now binds API/web container ports to loopback only (`127.0.0.1`) by default.
Public traffic should go through host nginx (`80/443`) only.

### Data volume

By default, compose bind-mounts `./data` from the repo into `/data` inside the API container. If you want a different host path, set `SPACEGATE_DATA_DIR` before running compose:

```bash
SPACEGATE_DATA_DIR=/data/spacegate/data docker compose up --build
```

Optional API DuckDB runtime caps for smaller hosts:

```bash
SPACEGATE_API_DUCKDB_MEMORY_LIMIT=6GB \
SPACEGATE_API_DUCKDB_THREADS=4 \
docker compose up --build
```

You still need to build the core database (once). Easiest path:

1. Run the build on the host (recommended), then start compose (the container sees `./data`).
2. Or exec into the API container and run the build scripts there (requires build tools inside the image).

If you want a dedicated “builder” container in compose, say the word and I’ll add it.

### Troubleshooting

- **`pip` missing in venv**  
  Install `python3-venv` (Debian/Ubuntu), then rerun `./install_spacegate.sh`.

- **`npm` not found or Node is too old**  
  Install/upgrade Node.js (v18+; v20 recommended), then rerun `./install_spacegate.sh`.

- **`7z` not found**  
  Install `p7zip-full` (Debian/Ubuntu), then rerun `./install_spacegate.sh`.

- **Port already in use**  
  Stop the running instance with `scripts/run_spacegate.sh --stop`, or change ports with:
  `SPACEGATE_API_PORT=8001 scripts/run_spacegate.sh`
  If using dev web mode, also set:
  `SPACEGATE_WEB_PORT=5174 scripts/run_spacegate.sh --web-dev`

- **Build verification fails**  
  Run `scripts/verify_build.sh` directly to see details. If it references an old build, rebuild with `scripts/build_core.sh --overwrite`.

# Roadmap (high level)

  - v1.1: static snapshot generation (SVG) with deterministic rendering rules.
  - v1.2: factual “facts → blurb” generation + reference links.
  - v1.2.2: precomputed 10‑nearest neighbor graph.
  - v2: browser 3D map.
  - v2.1+: optional catalogs as packs.
