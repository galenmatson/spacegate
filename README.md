#  What Spacegate Is

  - A public, richly browseable 3D star map + worldbuilding layer grounded in real astronomy. It prioritizes fun exploration and factual,
    engaging descriptions while keeping core science data immutable and provenance‑clean.

#  Scope and Deliverables

  - Gaia-first canonical datasets: systems, stars, planets (Gaia DR3 backbone + NASA Exoplanet Archive), with multiplicity and science side catalogs.
  - AT-HYG is transitional crosswalk support for naming/identifier recovery, not canonical inventory authority.
  - Optional packs and side catalogs (v1.2+): compact/remnant, superstellar, eclipsing, and lifecycle support artifacts.
  - Disc layer derivatives (legacy runtime alias: `rich`): expositions, reference links, snapshots, scores.
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
  - Display naming precedence: common/human names first, then survey/mission host labels (TRAPPIST/Kepler/TOI/WASP family), Gaia ID last fallback.
  - Separate databases for layer boundaries:
    - *galaxy*: immutable canonical science corpus
      - Gaia-first inventory with merged auxiliary science evidence (NSS/WDS/ORB6/MSC/SBX and side catalogs)
      - AT-HYG contributes transitional crosswalk enrichment only
    - *core*: the Spacegate database (fast)
      - deterministic science slice (typically <=1000 LY of Sol)
      - million-scale object counts tuned for interactive performance
      - tuned for performance, scaled for resources
    - *halo*: explicit opt-in science projection (slow)
      - complementary science rows excluded from `core` by slice policy
    - *arm*: immutable supplemental science
      - observational side tables outside core hot paths
      - Epoch transforms (for example J2000 -> J2016 propagated positions)
      - Derived kinematics and orbital parameters
      - System hierarchy inferences with confidence
      - Crossmatch confidence scores and physical-consistency flags
      - Deterministic classifications computed from core fields
    - *disc*: reproducible derivatives
      - system animations
      - factsheets
      - AI narration
      - generated imagery
      - links to external catalogs, articles, and papers
    - *rim*: editable fiction
      - lore from popular scifi
      - user creatable maps, links, economy, and narrative
      
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
The installer creates virtualenvs, installs Python/web dependencies, and if needed bootstraps the current prebuilt core DB from `https://spacegates.org/dl/current.json` (with fallback to local source build). Override with `SPACEGATE_PUBLIC_BASE_URL` or `SPACEGATE_BOOTSTRAP_META_URL` if your public host differs.

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

Before a forced full catalog refresh, run preflight:

```bash
scripts/preflight_full_refresh.sh
```

Then run a true full refresh (Gaia `delta_mode=refresh` + support catalog overwrite):

```bash
scripts/build_core.sh --full-refresh
```

For routine update runs with automatic differential/full routing, use:

```bash
scripts/refresh_core.sh
```

For multiplicity contribution analysis with MSC fixed on (`nss_off`, `nss_on`, optional `nss_on_wds_xmatch`):

```bash
scripts/run_multiplicity_modes.sh
```

Optional experimental WDS->Gaia crosswalk (for WDS-linked grouping support alongside MSC):

```bash
SPACEGATE_ENABLE_WDS_GAIA_XMATCH=1 scripts/download_core.sh --non-interactive
SPACEGATE_ENABLE_WDS_GAIA_XMATCH=1 scripts/cook_core.sh
SPACEGATE_ENABLE_WDS_GAIA_XMATCH=1 scripts/ingest_core.sh
```

When enabled, ingest applies physical-consistency gates to multi-member WDS groups before using them for system grouping.

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

### 3.1) Publish a promoted build for public download (operator task)

To package the currently promoted build and update download metadata:

```bash
scripts/publish_db.sh
```

By default this writes to `SPACEGATE_DL_ROOT` (auto-detected as `/data/spacegate/dl` when `/data/spacegate` exists, otherwise `/srv/spacegate/dl`):

- archive: `db/<build_id>.7z` (or `.tar.zst` if `7z` is unavailable)
- symlink: `current -> db/<archive>`
- metadata: `current.json`
- reports: `reports/<build_id>/{qc_report,match_report,identifier_report,alias_report,provenance_report,system_grouping_report,core_manifest}.json` when present

`current.json` includes artifact checksum/size plus report links and summary metadata used by bootstrap clients.

### 3.1a) Publish catalog mirror snapshots for bootstrap clients

To mirror catalog artifacts (raw + cooked) into `$SPACEGATE_DL_ROOT/catalogs`:

```bash
scripts/publish_catalog_mirror.py
```

Outputs:

- `catalogs/snapshots/<snapshot_id>/raw/...` (upstream raw format, unchanged)
- `catalogs/snapshots/<snapshot_id>/cooked/...` (Spacegate-normalized artifacts)
- `catalogs/snapshots/<snapshot_id>/index.json`
- `catalogs/current -> snapshots/<snapshot_id>`
- `catalogs/current.json`

Use `--catalog <name>` repeatedly to mirror a subset, or `--raw-only` to skip cooked artifacts.

### 3.2) Push published artifacts to a remote host

To copy the published DB archive, `current.json`, and referenced reports to a remote `/dl` tree:

```bash
scripts/push_published_db.sh --remote antiproton
```

The script reads local `current.json`, transfers only the referenced files, and preserves relative paths (`db/...`, `reports/...`).

By default it does **not** update the remote `current` symlink. If you still want that pointer on the remote host:

```bash
scripts/push_published_db.sh --remote antiproton --set-current-link
```

### 3.3) Deploy app code to antiproton safely (preserve remote auth env)

To sync the app and restart containers without overwriting remote secrets:

```bash
scripts/deploy_spacegate.sh --remote antiproton --expect-auth enabled
```

This deploy helper excludes these remote-local env files from rsync:
- `.spacegate.env`
- `.spacegate.local.env`

Set host-specific deploy defaults through local config rather than tracked code:

```bash
export SPACEGATE_DEPLOY_REMOTE=deploy-user@your-public-host
export SPACEGATE_DEPLOY_SSH_KEY=$HOME/.ssh/spacegate_deploy_key
export SPACEGATE_DEPLOY_PUBLIC_URL=https://your-public-host.example
```

Useful options:
- `--no-build` restart without image rebuild
- `--skip-public-check` skip public URL checks
- `--dry-run` preview sync/restart steps without changing remote files

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

### 6.1) Ops report (one-shot health summary)

```bash
scripts/ops_report.sh
scripts/ops_report.sh --public-url https://spacegates.org
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
# Optional alias (compose/scripts fallback to this when SPACEGATE_STATE_DIR is unset)
export SPACEGATE_DATA_DIR=/var/lib/spacegate

# DuckDB resources (otherwise auto-detected)
export SPACEGATE_DUCKDB_MEMORY_LIMIT=24GB
export SPACEGATE_DUCKDB_THREADS=4

# Multiplicity toggles
export SPACEGATE_ENABLE_GAIA_NSS=1
export SPACEGATE_ENABLE_MSC=1
export SPACEGATE_ENABLE_SBX=1
export SPACEGATE_ENABLE_PROXIMITY=0
export SPACEGATE_ENABLE_WDS_GAIA_XMATCH=0
export SPACEGATE_ENABLE_ECLIPSING_CATALOGS=1
export SPACEGATE_ENABLE_KEPLER_EB=0
export SPACEGATE_ENABLE_ATHYG_ALIAS_CROSSWALK=0
export SPACEGATE_ENABLE_ATHYG_SUPPLEMENT_MERGE=0

# MSC transport fallback (only if CTIO TLS chain fails in your environment)
# Preferred MSC mirror override:
# export SPACEGATE_MSC_MIRROR_URL=$SPACEGATE_PUBLIC_BASE_URL/dl/catalogs/current/raw/msc/newmsc-20240101.tar.gz
# Security note: HTTP is vulnerable to in-transit tampering.
# In HTTP mode, SHA pinning is required.
export SPACEGATE_MSC_ALLOW_INSECURE_HTTP=0
# export SPACEGATE_MSC_ALLOW_INSECURE_HTTP=1
# export SPACEGATE_MSC_FORCE_HTTP=1
# export SPACEGATE_MSC_SHA256=<expected_sha256>
# Optional cooker safety limits for MSC tar processing
# export SPACEGATE_MSC_MAX_ARCHIVE_BYTES=134217728
# export SPACEGATE_MSC_MAX_MEMBER_BYTES=67108864

# Auto-score coolness after promote_build.sh (default on)
export SPACEGATE_AUTO_SCORE_COOLNESS=1

# Multiplicity golden exam is on by default in verify_build.sh.
# Set to 0 only when intentionally bypassing this gate.
# export SPACEGATE_VERIFY_MULTIPLICITY_GOLDENS=0

# Gaia NSS fetch tuning (download_core.sh)
export SPACEGATE_GAIA_NSS_BUCKETS=53
export SPACEGATE_GAIA_NSS_TIMEOUT_S=240
export SPACEGATE_GAIA_NSS_RETRIES=4

# Gaia differential fetch controls (download_core.sh)
# resume: reuse local bucket parts whenever present (fastest, default behavior)
# delta: refresh only stale/missing buckets
# refresh: force full refetch of all buckets
export SPACEGATE_GAIA_DELTA_MODE=delta
export SPACEGATE_GAIA_DELTA_MAX_AGE_HOURS=720
# Optional per-source overrides:
# export SPACEGATE_GAIA_BACKBONE_DELTA_MODE=delta
# export SPACEGATE_GAIA_CLASSPROB_DELTA_MODE=delta
# export SPACEGATE_GAIA_NSS_DELTA_MODE=delta
# export SPACEGATE_SBX_DELTA_MODE=delta

# Optional WDS->Gaia crosswalk via CDS XMatch (download_core.sh)
export SPACEGATE_WDS_GAIA_XMATCH_DIST_ARCSEC=2.0
export SPACEGATE_WDS_GAIA_XMATCH_SELECTION=best
export SPACEGATE_WDS_GAIA_XMATCH_MAX_REC=2000000

# WDS->Gaia ingest-time gating (ingest_core.py)
export SPACEGATE_WDS_GAIA_MATCH_MAX_ARCSEC=2.0
export SPACEGATE_WDS_GAIA_GATE_MAX_DIST_SPREAD_LY=10.0
export SPACEGATE_WDS_GAIA_GATE_MAX_PM_DELTA_MASYR=25.0

# Core DB bootstrap controls
export SPACEGATE_BOOTSTRAP_DB=0
export SPACEGATE_PUBLIC_BASE_URL=https://spacegates.org
export SPACEGATE_BOOTSTRAP_META_URL=https://spacegates.org/dl/current.json

# Web runtime mode for scripts/run_spacegate.sh
# 0 = API only (default), 1 = API + Vite dev server
export SPACEGATE_WEB_ENABLE=1
```

### Persistent Local Env File (Recommended)

Instead of exporting variables for every command, create a local env file in the repo root:

```bash
cat > .spacegate.env <<'EOF'
SPACEGATE_STATE_DIR=/data/spacegate/data
SPACEGATE_CACHE_DIR=/data/spacegate/data/cache
SPACEGATE_LOG_DIR=/data/spacegate/data/logs

# Optional source-build knobs
SPACEGATE_ENABLE_PROXIMITY=0
SPACEGATE_ENABLE_GAIA_BACKBONE=1
SPACEGATE_ENABLE_GAIA_CLASSPROB=1
SPACEGATE_ENABLE_GAIA_NSS=1
SPACEGATE_ENABLE_MSC=1
SPACEGATE_ENABLE_SBX=1
SPACEGATE_ENABLE_WDS_GAIA_XMATCH=0
SPACEGATE_ENABLE_ECLIPSING_CATALOGS=1
SPACEGATE_ENABLE_KEPLER_EB=0
SPACEGATE_ENABLE_ATHYG_ALIAS_CROSSWALK=0
SPACEGATE_ENABLE_ATHYG_SUPPLEMENT_MERGE=0
SPACEGATE_GAIA_BACKBONE_BUCKETS=211
SPACEGATE_GAIA_CLASSPROB_BUCKETS=211
SPACEGATE_GAIA_NSS_BUCKETS=53
SPACEGATE_GAIA_DELTA_MODE=delta
SPACEGATE_GAIA_DELTA_MAX_AGE_HOURS=720
SPACEGATE_DUCKDB_MEMORY_LIMIT=24GB
SPACEGATE_DUCKDB_THREADS=12
EOF
```

Most scripts now auto-load these files in this precedence (lowest to highest):

1. `/etc/spacegate/spacegate.env`
2. `./.spacegate.env`
3. `./.spacegate.local.env`
4. `SPACEGATE_ENV_FILE` (if set)

Process env always wins (inline prefixes like `SPACEGATE_STATE_DIR=... scripts/...` override all files).

Note: `.spacegate.env` and `.spacegate.local.env` are ignored by git.

## Nginx setup (optional)

For release deployments, use nginx in front of the API and web containers (`/api` -> `127.0.0.1:8000`, `/` -> `127.0.0.1:8081`):

```bash
sudo scripts/setup_nginx_spacegate.sh
```

For host-local deployment notes, use:
- `docs/RUNTIME_NOTES_TEMPLATE.md` as a template
- copy it to `/srv/spacegate/RUNTIME.md` (outside git) and fill host-specific values

Behavior:
- Uses port 80 if free or already owned by nginx.
- Falls back to port 8080 if port 80 is in use by a non‑nginx process.
- Writes `/etc/nginx/sites-available/spacegate.conf` with provenance comments.
- Symlinks to `/etc/nginx/sites-enabled/spacegate.conf` (without touching other sites).
- Proxies web UI to container upstream `http://127.0.0.1:8081` by default.
- Applies API abuse controls by default: per-IP rate limit, burst limit, and connection limit.
- Applies proxy timeouts on API upstream connections.
- Serves `/dl/` from `SPACEGATE_DL_ALIAS_DIR` (defaults to `SPACEGATE_DL_ROOT`; `SPACEGATE_DL_ENABLE=0` disables `/dl/`).
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
sudo SPACEGATE_SERVER_NAME="your-public-host.example www.your-public-host.example" \
  SPACEGATE_API_RATE_RPS=15 \
  SPACEGATE_API_RATE_BURST=30 \
  SPACEGATE_API_CONN_LIMIT=30 \
  SPACEGATE_PROXY_READ_TIMEOUT=45s \
  scripts/setup_nginx_spacegate.sh --force
```

Optional HTTPS enforcement from this script (requires cert files):

```bash
sudo SPACEGATE_SERVER_NAME="your-public-host.example www.your-public-host.example" \
  SPACEGATE_TLS_ENABLE=1 \
  SPACEGATE_TLS_CERT_FILE=/etc/letsencrypt/live/your-public-host.example/fullchain.pem \
  SPACEGATE_TLS_KEY_FILE=/etc/letsencrypt/live/your-public-host.example/privkey.pem \
  scripts/setup_nginx_spacegate.sh --force
```

### HTTPS alternatives

You can either:

1. Use script-managed TLS via `SPACEGATE_TLS_ENABLE=1` and cert/key paths (shown above), or
2. Keep script-managed HTTP and let certbot edit nginx:
   ```bash
   sudo certbot --nginx -d your-public-host.example -d www.your-public-host.example
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
scripts/compose_spacegate.sh up --build
```

This exposes:
- API: `http://127.0.0.1:8000`
- Web container: `http://127.0.0.1:8081`

Note: compose now binds API/web container ports to loopback only (`127.0.0.1`) by default.
Public traffic should go through host nginx (`80/443`) only.

### Data volume

By default, compose bind-mounts `./data` from the repo into `/data` inside the API container.
For consistent env-file behavior (`.spacegate.env`, `.spacegate.local.env`), use the wrapper script:

```bash
scripts/compose_spacegate.sh up --build
```

Direct compose still works, but use an inline env prefix (or `--env-file`) when you need a custom mount:

```bash
SPACEGATE_DATA_DIR=/data/spacegate/data docker compose up --build
```

Optional API DuckDB runtime caps for smaller hosts:

```bash
SPACEGATE_API_DUCKDB_MEMORY_LIMIT=6GB \
SPACEGATE_API_DUCKDB_THREADS=4 \
scripts/compose_spacegate.sh up --build
```

You still need to build the core database (once). Easiest path:

1. Run the build on the host (recommended), then start compose (the container sees your mounted state dir at `/data`).
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
  - v1.2: additional catalogs / packs foundation before heavy enrichment.
  - v1.3-v1.5: reference links, facts → exposition, and image generation.
  - v1.6: precomputed 10-nearest neighbor graph.
  - v2: browser 3D map.
  - v2.2: lore, engagement, and community ranking overlays.
