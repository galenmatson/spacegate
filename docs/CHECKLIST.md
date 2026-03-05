# Spacegate Project Checklist

This checklist tracks deliverables against `docs/PROJECT.md`. Checked items reflect current repo state.

## Project Foundations
- [x] Define core vision, scope, and non-goals in `docs/PROJECT.md`
- [x] Define core schema contract in `docs/SCHEMA_CORE.md`
- [x] Define rich derived schema contract in `docs/SCHEMA_RICH.md`
- [x] Define lore overlay schema contract in `docs/SCHEMA_LORE.md`
- [x] Document current data sources in `docs/DATA_SOURCES.md`
- [x] Add pack contract doc `docs/PACKS.md`
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
- [x] WDS->Gaia grouping gate: enforce physical consistency thresholds (distance/proper-motion) before WDS-based grouping
- [x] Lockfile to prevent concurrent ingest
- [x] Atomic build output staging (`$SPACEGATE_STATE_DIR/out/<build_id>.tmp/` → `$SPACEGATE_STATE_DIR/out/<build_id>/`)

## Build Outputs & Promotion
- [x] `$SPACEGATE_STATE_DIR/out/<build_id>/core.duckdb` produced
- [x] `$SPACEGATE_STATE_DIR/out/<build_id>/parquet/{stars,systems,planets}.parquet` produced
- [x] `$SPACEGATE_STATE_DIR/reports/<build_id>/` generated (QC, provenance, match, grouping)
- [x] Promotion script `scripts/promote_build.sh`
- [x] `$SPACEGATE_STATE_DIR/served/current` points to promoted build
- [x] Promote flow auto-refreshes coolness scores by default (opt-out: `SPACEGATE_AUTO_SCORE_COOLNESS=0`)

## Tooling & Exploration
- [x] CLI explorer `scripts/explore_core.py`
- [x] Build/verify helpers (`scripts/build_core.sh`, `scripts/verify_build.sh`)
- [x] Catalog evaluation sampling/report tool (`scripts/catalog_eval.sh`)
- [x] Candidate catalog sample fetch tool (`scripts/fetch_catalog_samples.sh`)
- [x] Multiplicity crossmatch prototype (`scripts/multiplicity_crossmatch.sh`)

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
- [x] UI matches `docs/UX_SPEC.md` (provenance links, match confidence warnings, accessibility)
- [x] Deploy initial UI

## Operations
- [x] Installer script (`install_spacegate.sh`) for deps + build
- [x] Prebuilt DB bootstrap script (`scripts/bootstrap_core_db.sh`)
- [x] Publish/download reports alongside prebuilt DB artifacts (`qc_report.json`, `match_report.json`, `provenance_report.json`) and wire via `current.json`
- [x] Launcher script (`scripts/run_spacegate.sh`) for API (+ optional web dev server)
- [x] Status dashboard script (`scripts/spacegate_status.sh`)
- [x] Ops report script (`scripts/ops_report.sh`)
- [x] Stress tester script (`scripts/spacegate_stress.sh`)
- [x] Nginx setup script (`scripts/setup_nginx_spacegate.sh`) with safe re-run
- [x] Runtime hardening: localhost-only container binds + nginx API limits/timeouts
- [x] Systemd unit installer (`scripts/install_spacegate_systemd.sh`)
- [x] Runtime notes template (`docs/RUNTIME_NOTES_TEMPLATE.md`) for host-local `/srv/spacegate/RUNTIME.md`

## Deployment (v0.1)
- [x] Host UI/API on production cloud infrastructure
- [x] Public at `coolstars.org` (`spacegates.org` redirects)

## v0.1.5 Admin Control Plane + Auth Foundation
- [x] Draft Checkpoint A implementation spec (`docs/ADMIN_AUTH_SPEC.md`)
- [x] OIDC login for admin panel (Google-supported provider path)
- [x] Server-side admin identity allowlist enforcement
- [x] Identity schema for future user features (`users`, `auth_identities`, `sessions`, `roles`)
- [x] RBAC middleware (`admin` role active; future `user` role scaffolded)
- [x] Session hardening (secure HTTP-only cookies, CSRF, TTL, re-auth on sensitive actions)
- [x] Admin read-only operations view (build/publish/runtime/reports)
- [x] Safe allowlisted action runner for existing scripts (no arbitrary shell)
- [x] Audit log for admin actions (`who`, `what`, `when`, `params`, `result`)

## v0.2 Coolness
- [x] Coolness scoring + ranking stored in rich with score breakdown (`scripts/score_coolness.py`, including complexity/exotic-star factors for later enrichment prioritization)
- [x] Versioned weight profiles (reproducible profile id/version in outputs)
- [x] CLI preview/apply for weight profiles with diff against active profile
- [x] Admin slider/preset UI for weights
- [x] Diversity preview checks (class/type distribution before apply)
- [x] Scoring report artifact per run (top-N and distribution summary)
- [x] Profile activation audit trail + rollback to prior profile

## v1 System Visualization
- [x] Snapshot generator and manifest (`scripts/generate_snapshots.py` + `snapshot_manifest`)
- [x] Deterministic rendering rules and QC (deterministic SVG output + parameter hash + input hash)
- [x] Rich artifact storage + versioning (`out/<build_id>/snapshots/...`, `rich.duckdb`, `snapshot_report.json`)
- [x] Admin-triggered snapshot regeneration with top-coolness filtering

## v1.1 Beautification
- [x] UI visual refresh (intentional hierarchy + readability)
- [x] Theme system with persistence and accessibility guardrails
- [x] Theme naming + polish pass (`Enterprise`, `Geocities`, Mission Control/Cyberpunk/Aurora/Deep Space refinements)
- [x] Enterprise header/history UX updates (decorative left LCARS blocks + `STARS ACCESSED` linked history chips)
- [x] Search/filters ergonomics (`/` to focus search, collapsible filters with `Collapse Up`)
- [x] Data-density cleanup (snapshot-first list/detail hierarchy and cleaner card layout)

## v1.2 Additional Catalogs / Packs Foundation
- [x] Approve candidate sources for the next ingestion wave
- [x] Implement multiplicity grouping provenance scaffolding (`WDS`/`ORB6` active; `MSC` optional and disabled by default)
- [x] Add Gaia NSS core ingest path (download manifest + cooked normalization + star-level merge)
- [x] Add four-mode multiplicity contribution report (`baseline`, `NSS only`, `MSC only`, `NSS+MSC`)
- [x] Add optional WDS->Gaia XMatch bridge scaffold (`SPACEGATE_ENABLE_WDS_GAIA_XMATCH=1`, default-off)
- [x] Draft Gaia-first core architecture proposal with measured Gaia DR3 `<1000 ly` scale bands
- [ ] Implement Gaia backbone pilot ingest (`gaia_backbone` deterministic build path + report)
- [ ] Define and materialize `core_product_slice` policy over Gaia backbone for default serving
- [ ] Reintegrate NSS/MSC/WDS hierarchy evidence against Gaia backbone IDs
- [ ] Validate proton runtime/storage/latency gates for Gaia-first default switch
- [ ] Implement additional catalog / pack ingestion pipelines
- [ ] Produce pack QC reports + manifests
- [ ] Reconcile grouping/host-matching for benchmark systems after catalog expansion

## v1.3 External Reference Links
- [ ] External reference links (curated authoritative sources)

## v1.4 AI Rich Description
- [ ] Factsheets + expositions pipeline

## v1.5 Image Generator
- [ ] Image generation pipeline and artifact storage

## v1.6 System Neighbor Graph
- [ ] System neighbor graph (10 nearest systems per system)

## v1.7 Operations Dashboard and Telemetry
- [ ] Dashboard/telemetry stack beyond local scripts (metrics + alerts)

## v2 3D Map
- [ ] 3D viewer (camera controls, selection, tooltips)
- [ ] Filters (distance bubble, spectral class, magnitude)
- [ ] Rendering toggles (planets, lore, links)
- [ ] Performance & floating-origin correctness

## v2.1 System View and Generators
- [ ] Epoch/time selection with proper-motion re-rendering
- [ ] System/worldbuilder generator tools

## v2.2 Lore & Engagement
- [ ] Lore overlay schema + tooling
- [ ] Engagement dataset (privacy-safe)
- [ ] UI affordances for lore/engagement
- [ ] Public coolness profile selection / community ranking presets
