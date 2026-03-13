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

- Added lifecycle source ingest (exoplanet.eu, OEC, HWC, EMAC TT9).
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

## Related Documents

- `docs/PROJECT.md`
- `docs/CHECKLIST.md`
- `docs/DATA_SOURCES.md`
- `docs/INGEST_RECOVERY.md`
- `docs/CATALOG_EVAL.md`
