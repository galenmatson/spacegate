# Dataset Iteration History (Spacegate v1.2 Program)

This document records the major iterations that moved Spacegate from bootstrap catalogs to the current Gaia-first dataset architecture.

Use this as the historical ledger for:

- why specific catalogs were added/removed/deferred
- how duplicate, omission, and naming defects were corrected
- which safety/QC gates were introduced after each incident class

## Current End State (as of 2026-03)

- Gaia-first canonical backbone for stars
- NASA canonical baseline for planets
- mandatory multiplicity evidence includes MSC (plus WDS/ORB6 support; NSS default-on)
- deterministic alias and identifier reconciliation with quarantine and duplicate-trap QC gates
- exoplanet lifecycle overlay pipeline + differential refresh path
- host-name promotion for Gaia-fallback display names (for example TRAPPIST/Kepler/TOI/WASP)
- profile-scoped SLO verification path and promote-time rollback gate for sliced builds
- default catalog transport hardened to HTTPS for default-on sources
- Sol authority S1+S2: canonical Sol/subplanet bootstrap in core plus moon/orbit/barycenter hierarchy in arm

## Catalog Attempt Ledger (v1.2 Closeout)

### Active in default Gaia-first ingest

- Gaia DR3 backbone: canonical star inventory.
- NASA Exoplanet Archive (`pscomppars`): canonical planet inventory.
- Multiplicity core: MSC (mandatory), WDS, ORB6, Gaia NSS (default-on), SBX (default-on).
- Compact/superstellar support: ATNF, McGill magnetar, Gaia EDR3 white dwarf, open clusters, Galactic SNR.
- Eclipsing support defaults: DEBCat + TESS EB (Kepler EB is no longer default).
- Naming/crosswalk support: alias pipeline + controlled AT-HYG supplement/crosswalk path.

### Implemented but default-off / optional

- Kepler EB: retained as optional evidence source (`SPACEGATE_ENABLE_KEPLER_EB=1`) because current Gaia-slice linkage is low relative to ingest cost.
- WDS↔Gaia bridge (`wds_gaia_xmatch`): optional due confidence-gated behavior and conservative production stance.
- Exoplanet lifecycle overlays (`exoplanet.eu`, OEC, HWC): optional support layer; canonical planet rows remain NASA-rooted.
- Proximity grouping: optional/nondefault due inexact grouping risk.

### Evaluated and deferred/disregarded

- BDB / non-mirrored binary metadatabases: deferred pending stable mirror + integrity-pinned bulk path (no default dependency on fragile/insecure routing).
- SB9: disregarded for default ingest/evaluation after SBX adoption.
- EMAC TT9 endpoint: removed from active ingest (resource/tooling page, no deterministic bulk row feed).

### Pending evaluation queue

- Additional deterministic TESS-era eclipsing/variability bulk feeds beyond current TESS EB export.
- Large survey overlays requiring separate performance/retention planning (for example CatWISE full integration).

## Iteration Timeline

### 0) Bootstrap Core (AT-HYG-centered)

- Objective: stand up a working star/planet UX quickly.
- Result: early functional core, but canonical dependency on AT-HYG created long-term quality/crosswalk debt.

Representative commits:
- `3612ffe` first multiplicity ingest pass

### 1) Multiplicity Expansion + Policy Hardening

- Added Gaia NSS support and multiplicity mode reporting.
- Added WDS-Gaia optional bridge path.
- Enforced physical consistency gating for bridge-based grouping.
- Shifted proximity grouping to nondefault.
- Moved MSC from optional to mandatory policy.

Representative commits:
- `b499fae`, `413fd1d`, `abfb88a`, `e961fe8`

### 2) Gaia-First Architecture Migration

- Rewrote architecture docs around `galaxy/core/halo/arm/disc/rim`.
- Implemented Gaia backbone ingest path with resumable bucket fetch.
- Added deterministic full-refresh and recovery workflows.
- Introduced layered artifacts and promotion gates requiring `arm.duckdb`.

Representative commits:
- `5b2fe6c`, `ee73894`, `781b159`, `d63762a`, `f0b4877`, `dd3749e`, `98b49ae`

### 3) Classification Safety + New Science Side Catalogs

- Added compact/superstellar side catalogs (ATNF, magnetar, clusters, SNR).
- Added remnant classification invariants and white-dwarf safety QC.
- Added D-class handling in filters/UI.

Representative commits:
- `1119847`, `b62032b`, `83a9a7e`, `077305e`

### 4) Identifier/Crosswalk Rebuild and Duplicate Control

- Implemented full alias ingestion and search integration.
- Added deterministic identifier reconciliation with confidence/quarantine.
- Added AT-HYG supplement merge under controlled rules.
- Added duplicate-trap report and QC gate.
- Fixed known duplicate class in MSC component ingest.

Representative commits:
- `1a8b693`, `34e2a16`, `6a70ea2`, `b1dbf78`

### 5) Exoplanet Lifecycle + Differential Refresh

- Added lifecycle source ingest (exoplanet.eu, OEC, HWC); EMAC TT9 was later removed from active ingest because no deterministic bulk row feed is exposed at the endpoint.
- Added lifecycle policy fields (`is_default_visible`, `is_tombstoned`) and reports.
- Added per-source delta snapshots and impacted-row planner.
- Added selective cook + incremental planet refresh orchestration.

Representative commits:
- `ff30555`, `410eaa5`, `6e4be76`

### 6) Naming Ergonomics Refinement (Post-Gaia)

- Observed Gaia-ID-heavy display names for known hosts (for example TRAPPIST-1).
- Added host-label promotion for Gaia-fallback stars/systems.
- Added host-name alias seeding so search/display prefers human-facing labels.
- Updated schema/docs to codify naming precedence.

Representative commits:
- `c2f93cb`, `4033e29`

### 7) Source Governance and Evaluation Discipline

- Added catalog contribution/evaluation workflow with overlap reporting.
- Documented deferred/disregarded source policy (BDB-risk, SB9 superseded).
- Promoted SBX from evaluation queue into default multiplicity ingest path (toggleable via `SPACEGATE_ENABLE_SBX`).
- Added production stance on insecure transport exceptions and integrity pinning.

Representative commits:
- `423afc5`, `d9990aa`, `3a31f32`, `7e51436`

### 8) Eclipsing Policy Rebalance

- Kepler EB moved from default-on to default-off optional.
- Rationale codified: low in-slice Gaia linkage did not justify default ingest/runtime footprint.
- Ingest/report metadata now explicitly tracks `kepler_eb_enabled` state.

Representative commits:
- `ea194cf`

### 9) Reproducibility + Retirement Closeout

- Added Gaia TAP completeness guards (expected-row count checks for backbone/classprob/NSS).
- Added deterministic rerun fingerprints and verify-time comparable-build checks.
- Added explicit astrometry boundary/quality policy metadata in build metadata and QC.
- Persisted lifecycle audit tables into `arm.duckdb`.
- Switched AT-HYG compatibility toggles to default-off and added `athyg_retirement_report.json`.

Representative commits:
- `0050ade`, `ea194cf` and follow-up closeout commits

### 10) Promote Gating + Transport Hardening

- Added profile SLO evaluator (`scripts/check_profile_slo.py`) capturing search/detail p95/p99, error rate, and API memory.
- Wired promote-time SLO gate with rollback-on-failure for profile-tagged builds (`scripts/promote_build.sh`).
- Recorded proton latency evidence in per-build report (`reports/<build_id>/slo_profile_report.json`).
- Removed default FTP transport for default-on CDS feeds (Gaia UCD, VSX) by switching defaults to HTTPS mirrors.

Representative commits:
- `30906b9` and follow-up hardening commits

### 11) Sol Authority S2 Hierarchy Expansion

- Canonicalized Sol terminology to `subplanet` with `dwarf_planet` interoperability retained.
- Expanded Sol authority extraction to include deterministic S2 moon rows from JPL Horizons.
- Materialized moon nodes, satellite hierarchy/orbit edges, orbital-solution rows, and Earth-Moon/Pluto-Charon barycenters into `arm.duckdb`.
- Added verify-time Sol S2 arm gate checks to block promotion if hierarchy rows are missing.

Representative commits:
- `281f173` and follow-up S2 commit(s)

## Recurrent Defect Classes and Mitigations

1. Duplicate entities from overlapping catalogs:
- Mitigated with deterministic identifier merge, quarantine tables, and duplicate-trap QC gates.

2. False/weak multiplicity grouping:
- Mitigated with nondefault proximity grouping and physical consistency gating on bridge evidence.

3. Human-unfriendly naming regressions:
- Mitigated with alias crosswalk rebuild and host-name promotion over Gaia fallback labels.

4. Slow/full rebuild-only operations:
- Mitigated with source-delta scan, impacted-row planning, and incremental planet refresh path.

5. Transport/integrity risk for fragile upstreams:
- Mitigated with explicit insecure-transport controls, checksum pin requirements, and deferred-source policy.

6. Low-yield default catalogs increasing ingest/runtime cost:
- Mitigated by shifting low-linkage sources to explicit opt-in while preserving full pipeline support.

## Related Documents

- `docs/PROJECT.md`
- `docs/CHECKLIST.md`
- `docs/DATA_SOURCES.md`
- `docs/INGEST_RECOVERY.md`
- `docs/CATALOG_EVAL.md`
