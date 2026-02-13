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

### For production deployments, standard Linux locations are recommended:
  - /var/lib/spacegate
  - /var/cache/spacegate
  - /var/log/spacegate
  - /etc/spacegate

## Quickstart (from scratch)

This walks a new user from zero to a running Spacegate (API + web UI).

### 1) Install system prerequisites
You need Python, Node, and basic download tools available on your PATH:

- `python3` + `pip`
- `node` + `npm`
- `git`, `curl`, `aria2c`, `gzip`

On Debian/Ubuntu the following usually works:

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip git curl aria2 gzip nodejs npm
```

### 2) Clone and install dependencies
The installer creates virtualenvs, installs Python and web dependencies, and builds the database if it doesn’t exist.

```bash
git clone https://github.com/galenmatson/spacegate.git
cd spacegate

scripts/install_spacegate.sh
```

Optional flags:
- `--overwrite` re-downloads catalogs even if present.
- `--skip-web` skips the web UI dependency install.
- `--skip-build` skips the data build step.

### 3) Build the core database (if you skipped it)
If you used `--skip-build` or want to rebuild:

```bash
scripts/build_core.sh
```

### 4) Run Spacegate (API + web)
The launcher verifies the database, then starts both services:

```bash
scripts/run_spacegate.sh
```

Defaults:
- API: `http://0.0.0.0:8000`
- Web UI: `http://0.0.0.0:5173`

### 5) Stop or restart

```bash
scripts/run_spacegate.sh --stop
scripts/run_spacegate.sh --restart
```

### Optional configuration

```bash
# Data locations (defaults to ./data)
export SPACEGATE_STATE_DIR=/var/lib/spacegate
export SPACEGATE_CACHE_DIR=/var/cache/spacegate
export SPACEGATE_LOG_DIR=/var/log/spacegate

# DuckDB resources (otherwise auto-detected)
export SPACEGATE_DUCKDB_MEMORY_LIMIT=24GB
export SPACEGATE_DUCKDB_THREADS=4
```

# Roadmap (high level)

  - v1.1: static snapshot generation (SVG) with deterministic rendering rules.
  - v1.2: factual “facts → blurb” generation + reference links.
  - v1.2.2: precomputed 10‑nearest neighbor graph.
  - v2: browser 3D map.
  - v2.1+: optional catalogs as packs.
