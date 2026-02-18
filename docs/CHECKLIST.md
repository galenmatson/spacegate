# Spacegate Project Checklist

This checklist tracks deliverables against `docs/PROJECT.md`. Checked items reflect current repo state.

## Project Foundations
- [x] Define core vision, scope, and non-goals in `docs/PROJECT.md`
- [x] Define v0 schema contract in `docs/SCHEMA.md`
- [x] Document current data sources in `docs/DATA_SOURCES.md`
- [x] Add pack contract doc `docs/PACKS.md`
- [x] Add packs manifest stub generator `scripts/generate_packs_manifest.py`
- [x] Add `.gitignore` entries for generated artifacts (`/data/`, etc.)

## Environment & Layout
- [x] Document required environment variables and defaults
- [x] Document state directory structure and immutability rules
- [x] Build IDs include UTC time (`YYYY-MM-DDTHHMMSSZ_<gitsha>`)

## Core Data Acquisition (v0)
- [x] Download scripts for AT-HYG + NASA Exoplanet Archive
- [x] Resolve LFS pointers during AT-HYG download
- [x] Manifests recorded under `$SPACEGATE_STATE_DIR/reports/manifests/`
- [x] Cooked core outputs generated deterministically

## Core Ingestion (v0)
- [x] Build DuckDB core DB from cooked inputs
- [x] Normalize identifiers and types (Gaia/HIP/HD, etc.)
- [x] Parse spectral types into components while retaining raw strings
- [x] Join exoplanets to host stars/systems with match provenance + confidence
- [x] Morton spatial index (63-bit, 21 bits/axis, ±1000 ly)
- [x] Morton domain hard-fail on out-of-bounds coords
- [x] Distance invariant QC hard-fail (abs(norm(x,y,z) - dist_ly) < eps)
- [x] Parquet exports sorted by `spatial_index`
- [x] Provenance QC gate (hard-fail on missing required fields)
- [x] Build metadata table recorded in core.duckdb
- [x] System grouping: name-root + optional proximity (gated by `SPACEGATE_ENABLE_PROXIMITY=1`)
- [x] Lockfile to prevent concurrent ingest
- [x] Atomic build output staging (`$SPACEGATE_STATE_DIR/out/<build_id>.tmp/` → `$SPACEGATE_STATE_DIR/out/<build_id>/`)

## Build Outputs & Promotion
- [x] `$SPACEGATE_STATE_DIR/out/<build_id>/core.duckdb` produced
- [x] `$SPACEGATE_STATE_DIR/out/<build_id>/parquet/{stars,systems,planets}.parquet` produced
- [x] `$SPACEGATE_STATE_DIR/reports/<build_id>/` generated (QC, provenance, match, grouping)
- [x] Promotion script `scripts/promote_build.sh`
- [x] `$SPACEGATE_STATE_DIR/served/current` points to promoted build

## Tooling & Exploration
- [x] CLI explorer `scripts/explore_core.py`
- [x] Build/verify helpers (`scripts/build_core.sh`, `scripts/verify_build.sh`)

## API (v0.1)
- [x] Read-only API service implemented (FastAPI)
- [x] Endpoints per `docs/API_SPEC.md` (`/health`, `/systems/search`, `/systems/{id}`, `/systems/by-key`)
- [x] Cursor pagination + parameterized SQL
- [x] Responses include provenance + match confidence fields

## UI (v0.1)
- [x] Decide UI stack (DuckDB WASM vs. API)
- [x] Build minimal browser UI scaffold
- [x] Implement search + detail views
- [x] Add filters/sorting
- [ ] UI matches `docs/UX_SPEC.md` (provenance links, match confidence warnings, accessibility)
- [ ] Support optional packs + lore overlays in UI (core read-only)
- [x] Deploy initial UI

## Operations
- [x] Installer script (`install_spacegate.sh`) for deps + build
- [x] Prebuilt DB bootstrap script (`scripts/bootstrap_core_db.sh`)
- [x] Launcher script (`scripts/run_spacegate.sh`) for API (+ optional web dev server)
- [x] Status dashboard script (`scripts/spacegate_status.sh`)
- [x] Nginx setup script (`scripts/setup_nginx_spacegate.sh`) with safe re-run
- [x] Systemd unit installer (`scripts/install_spacegate_systemd.sh`)

## Deployment (v0.1)
- [ ] Host UI/API on Google Cloud
- [x] Public at `spacegates.org`

## Rich (v1.1+)
- [ ] Interestingness scoring + ranking stored in rich
- [ ] Snapshot generator and manifest
- [ ] Factsheets + blurbs pipeline
- [ ] Deterministic rendering rules and QC
- [ ] Rich artifact storage + versioning
- [ ] External reference links (curated authoritative sources)
- [ ] System neighbor graph (10 nearest systems per system)

## 3D Map (v2)
- [ ] 3D viewer (camera controls, selection, tooltips)
- [ ] Filters (distance bubble, spectral class, magnitude)
- [ ] Rendering toggles (planets, lore, links)
- [ ] Performance & floating-origin correctness

## Additional Catalogs / Packs (v2.1)
- [ ] Approve candidate sources
- [ ] Implement pack ingestion pipelines
- [ ] Pack QC reports + manifests
- [ ] Integrate packs into UI search/render

## Lore & Engagement (v2+)
- [ ] Lore overlay schema + tooling
- [ ] Engagement dataset (privacy-safe)
- [ ] UI affordances for lore/engagement
