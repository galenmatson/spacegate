# Dataset Iteration History (Spacegate v1.2 Program)

This document records the major iterations that moved Spacegate from bootstrap catalogs to the current Gaia-first dataset architecture.

Use this as the historical ledger for:

- why specific catalogs were added/removed/deferred
- how duplicate, omission, and naming defects were corrected
- which safety/QC gates were introduced after each incident class

## Current End State (as of 2026-07)

- Gaia-first canonical backbone for stars
- NASA canonical baseline for planets
- mandatory multiplicity evidence includes MSC (plus WDS/ORB6 support; NSS default-on)
- deterministic alias and identifier reconciliation with quarantine and duplicate-trap QC gates
- exoplanet lifecycle overlay pipeline + differential refresh path
- host-name promotion for Gaia-fallback display names (for example TRAPPIST/Kepler/TOI/WASP)
- profile-scoped SLO verification path and promote-time rollback gate for sliced builds
- default catalog transport hardened to HTTPS for default-on sources
- Sol authority S1+S4: canonical Sol/subplanet bootstrap in core plus moon/minor-body/artificial hierarchy+orbit overlays in arm
- Sol volatile-feed refresh runbook and staleness monitoring report
- Post-enrichment source-object reconciliation for duplicate MSC component surrogates, preserving Gaia/source identity while rolling accepted companions such as Alpha Centauri/Proxima into one physical system.

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
- Nearby ultracool completeness pass: the current Gaia-first backbone plus
  ARM-only UltracoolSheet overlay preserves objects such as Luhman 16 and
  WISE 0855-0714 in cooked/ARM support data, but does not promote unlinked
  ultracool objects into the accepted core inventory. This is a real nearby
  space blind spot and should be fixed before relying on the 10 pc census for
  public completeness claims.

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

### 12) Sol S3 Broadening + S4 Artificial Overlay

- Expanded deterministic Sol S3 small-body coverage (broader asteroid/TNO set, including Ixion-class objects).
- Added S4 curated artificial-object feed from JPL Horizons with freshness windows.
- Materialized `sol_artificial_objects` plus hierarchy/orbit/solution rows in `arm`.
- Added halo-arm projection support for Sol overlays and verify gates for S4.
- Added volatile refresh/staleness scripts (`refresh_sol_volatile.sh`, `report_sol_volatile.py`).

Representative commits:
- pending (feature/v1-2-packs-foundation working set)

### 13) Source-Object Companion Rollup

- Added a post-enrichment reconciliation stage for duplicate MSC component surrogate rows that already have stronger Gaia/accepted source rows in the same build.
- Reconciliation is keyed through deterministic identifier evidence such as HIP/HD, then guarded by distance, angular separation, compact/remnant consistency, and one-to-one ambiguity checks.
- Preserves the surviving Gaia/source star identity and deletes the duplicate MSC surrogate before root-system grouping, while carrying forward WDS/component evidence and source-catalog provenance.
- Emits `source_object_reconciliation` and `source_object_reconciliation_quarantine` artifacts plus build/report counts for auditability.
- Alpha Centauri / Proxima Centauri benchmark: Proxima now resolves into the accepted WDS 14396-6050 system with Alpha Cen A/B, while Proxima b/d remain explicitly hosted by the Proxima member star.
- Remaining orbit endpoint conflicts, display-name gaps, and ambiguous candidate merges are preserved as diagnostics/follow-up work rather than silently promoted.

Representative commits:
- pending (system identity / companion-rollup working set)

### 14) Alias and Preferred Display Name Authority v2

- Extended `system_search_terms` with target context so source/member aliases can
  resolve to the owning accepted system while preserving the matched object for
  UI focus and explanation.
- Added Gl/GJ/Gliese alias expansion from source catalog ID payloads. Component
  IDs such as `Gl 412A` now also seed searchable public variants such as
  `Gliese 412` and `GJ 412` without using names as companion-rollup evidence.
- Tightened public display-name policy so catalog IDs and abbreviated Bayer
  labels can explain a match but do not automatically become the public title
  when a better display name exists.
- Added strict exact-query guardrails for dense catalog-like and variable-star
  names to prevent misleading fuzzy substitutions, with `V1513 Cyg` ->
  `V1581 Cyg` as the benchmark failure.
- Added `alias_authority_diagnostics` build output for shared aliases,
  multi-level alias attachment, and catalog-label display fallbacks.
- Added `scripts/verify_alias_authority.py` for API-level goldens covering
  Gliese 412/GJ 412, Gliese 643, Alpha/Proxima, Alpha Librae/Zubenelgenubi,
  V1054 Oph/VB 8, and common public systems.
- Verified canonical build `20260710T181500Z_alias_v2` after patching the
  canonical emitter to preserve target-aware `system_search_terms` columns.
  Build verification, multiplicity goldens, known-system API checks, alias
  authority checks, and focused Playwright desktop/mobile checks passed.
- Performance note: the core alias-table stage in this pass took about
  34 minutes. A future ingest-performance pass should pre-materialize parsed
  Gl/GJ identifiers before variant expansion and dedupe.

Representative commits:
- pending (alias/display authority v2 working set)

### 15) Name Style Preference and Public Display-Name Policy v2

- Added an API/frontend display-name policy layer over the Alias Authority v2
  dataset. This pass does not rebuild or mutate `core` rows; it classifies
  aliases at read time into presentation styles such as `bayer_full`,
  `bayer_abbrev`, `human_catalog`, `technical_catalog`,
  `source_placeholder`, and `member_public_name`.
- Added `name_style` to public search, map, system detail, and
  simulation-scene endpoints. The default `public_full` keeps layperson-facing
  names such as `Alpha Centauri`, `Epsilon Indi`, `Mu Herculis`, and `Sirius`,
  while `astronomer_abbrev`, `catalog_compact`, and `source_technical` expose
  alternate naming preferences.
- Preserved matched aliases separately from display names: an `eps ind` query
  can report `matched_alias = Eps Ind` while displaying `Epsilon Indi` by
  default.
- Old prebuilt simulation-scene artifacts are bypassed until regenerated if
  they lack current name-style metadata, keeping `/simulation-scene` responses
  contract-complete.
- Added `scripts/verify_name_style_policy.py` for API goldens and a map
  hamburger Playwright check for persisted Name Style selection.

Representative commits:
- pending (name-style policy working set)

### 16) Source Evidence Utilization Audit + Identifier JSON Safety

- Added `scripts/audit_source_evidence_utilization.py` to compare preserved ARM
  source evidence against normalized graph/simulation contracts. The default
  audit reports MSC `orb.tsv` rows with source orbital fields that are not
  materialized into `arm.orbit_edges`, MSC `sys.tsv` orbitlike rows without
  matching graph edges, and MSC endpoint-key bridge work needed for deterministic
  evidence utilization.
- Current served build audit found preserved MSC orbit evidence that is not yet
  available to the simulator as normalized ARM orbital solutions; 70 Oph is one
  visible example where MSC masses are surfaced but MSC visual/spectroscopic
  orbit rows still fall back to presentation-only visual binary layout.
- Added `gaia_id_text`/`hip_id_text`/`hd_id_text` to system and star detail
  payloads. Public clients should display/copy these string fields instead of
  JavaScript-parsed numeric identifiers so long Gaia DR3 IDs do not lose their
  final digits.

Representative commits:
- pending (source evidence utilization / identifier safety working set)

### 17) Source Evidence Utilization + Stellar Parameter Normalization v1

- Rebuilt local side artifacts as `20260711T_source_evidence_v1_side` and
  promoted them on photon after verification. The build preserves the same
  core inventory while regenerating ARM with broader source-evidence
  utilization.
- MSC root components are now materialized for every represented WDS system,
  including simple binaries, not only systems with `subsystem_count >= 2`.
  This lets ordinary source-backed binaries such as 70 Oph attach preserved
  `sys.tsv`/`orb.tsv` evidence to normalized ARM graph edges.
- Added an `orb.tsv`-sourced deterministic edge path for MSC orbit rows whose
  host, primary endpoint, and secondary endpoint all resolve to component
  entities. Ambiguous/unmatched rows remain diagnostics rather than simulated
  facts.
- Source-evidence audit delta on the promoted local build:
  `msc_orbit_detail_rows_without_arm_orbit_edge` dropped from 143 to 6, and
  `msc_system_detail_orbitlike_rows_without_arm_orbit_edge` dropped from 131
  to 0. 70 Oph now exposes MSC source masses and an MSC source orbital period,
  eccentricity, and inclination in the system simulation payload.
- Added spectral-subclass-aware main-sequence priors for simulation support
  (`spectral_subclass_main_sequence_mass_prior_v1`). These distinguish, for
  example, K0V from K4V when source mass/radius/temperature are absent, but
  they are guarded against giants, subgiants, white dwarfs, neutron stars,
  black holes, pulsars, magnetars, and other evolved/remnant markers. The
  priors remain ARM/render support evidence, not core source facts.
- Extended the source-evidence utilization audit beyond MSC. The current local
  audit now also summarizes WDS observation utilization, orbital solution
  coverage by catalog, source stellar-parameter field coverage, normalized
  solution integrity, and nearby examples where spectral-class stars still lack
  source mass evidence. A major remaining finding is that WDS pair-observation
  rows are preserved but not yet normalized into ARM pair entities/orbit edges;
  this should become a separate WDS pair-observation utilization milestone.
- The simulation-scene hierarchy leaf path now emits `luminosity_lsun` as either
  a source quick fact or an ARM-scoped
  `stellar_luminosity_from_radius_teff_v1` derivation from available radius and
  effective temperature. This fixed TRAPPIST-1/HZ overlay inputs without
  treating derived luminosity as a core catalog fact.
- Browser verification was refreshed for the current public-full naming policy,
  cached/full search preview behavior, embedded Explorer object-list layout,
  and forced simulation-scene failure fallback behavior. The local map
  Playwright suite passes on photon.

Representative commits:
- `9d3bb82` (use preserved source evidence for stellar simulations)
- `504772a` (expand source evidence utilization audit)
- pending (hierarchy luminosity derivation and browser verification refresh)

## Recurrent Defect Classes and Mitigations

1. Duplicate entities from overlapping catalogs:
- Mitigated with deterministic identifier merge, source-object reconciliation, quarantine tables, and duplicate-trap QC gates.

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
