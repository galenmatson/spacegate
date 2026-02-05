# Spacegate Project Checklist

This is the canonical end-to-end checklist for Spacegate. Checked items reflect current repo state.

## Project Foundations
- [x] Define core vision, scope, and non-goals in `docs/PROJECT.md`
- [x] Define v0 schema contract in `docs/SCHEMA.md`
- [x] Document current data sources in `docs/DATA_SOURCES.md`
- [x] Add pack contract doc `docs/PACKS.md`
- [x] Add packs manifest stub generator `scripts/generate_packs_manifest.py`
- [x] Add `.gitignore` entries for generated artifacts (`/reports/`, `/out/`, etc.)

## Core Data Acquisition (v0)
- [x] Download scripts for AT-HYG + NASA Exoplanet Archive
- [x] Resolve LFS pointers during AT-HYG download
- [x] Raw manifests recorded under `raw/manifests/`
- [x] Cooked core outputs generated deterministically

## Core Ingestion (v0)
- [x] Build DuckDB core DB from cooked inputs
- [x] Morton spatial index (63-bit, 21 bits/axis, ±1000 ly)
- [x] Morton domain hard-fail on out-of-bounds coords
- [x] Parquet exports sorted by `spatial_index`
- [x] Provenance QC gate (hard-fail on missing required fields)
- [x] Build metadata table recorded in core.duckdb
- [x] System grouping: name-root + optional proximity (gated by `SPACEGATE_ENABLE_PROXIMITY=1`)
- [x] Lockfile to prevent concurrent ingest
- [x] Atomic build output staging (`out/<build_id>.tmp/` → `out/<build_id>/`)
- [x] Build IDs include UTC time (`YYYY-MM-DDTHHMMSSZ_<gitsha>`)

## Build Outputs & Promotion
- [x] `out/<build_id>/core.duckdb` produced
- [x] `out/<build_id>/parquet/{stars,systems,planets}.parquet` produced
- [x] `reports/<build_id>/` generated (QC, provenance, match, grouping)
- [x] Promotion script `scripts/promote_build.sh`
- [x] `served/current` points to promoted build

## Tooling & Exploration
- [x] CLI explorer `scripts/explore_core.py`
  - [x] stats (incl. binary + multi-star counts)
  - [x] search by name
  - [x] system members
  - [x] neighbors (kNN by xyz)

## UI (v1)
- [ ] Decide UI stack (DuckDB WASM vs. API)
- [ ] Build minimal browser UI scaffold
- [ ] Implement search + detail views
- [ ] Add filters/sorting
- [ ] Deploy initial UI

## Enrichment (v1.1+)
- [ ] Snapshot generator and manifest
- [ ] Factsheets + blurbs pipeline
- [ ] Deterministic rendering rules and QC
- [ ] Enrichment artifact storage + versioning

## 3D Map (v2)
- [ ] 3D viewer (camera controls, selection, tooltips)
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
