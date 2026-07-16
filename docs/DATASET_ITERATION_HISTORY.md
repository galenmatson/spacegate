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
- Nearby UltracoolSheet inventory bridge: vetted ultracool objects within the
  configured nearby cap can be promoted into core when the Gaia backbone misses
  them, with `source_catalog = 'ultracoolsheet'` and report/verifier coverage.

## Catalog Attempt Ledger (v1.2 Closeout)

### Active in default Gaia-first ingest

- Gaia DR3 backbone: canonical star inventory.
- NASA Exoplanet Archive (`pscomppars`): canonical planet inventory.
- Multiplicity core: MSC (mandatory), WDS, ORB6, Gaia NSS (default-on), SBX
  (default-on), with SB9 as complementary ARM component/orbit evidence.
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
- EMAC TT9 endpoint: removed from active ingest (resource/tooling page, no deterministic bulk row feed).

### Pending evaluation queue

- Additional deterministic TESS-era eclipsing/variability bulk feeds beyond current TESS EB export.
- Large survey overlays requiring separate performance/retention planning (for example CatWISE full integration).
- CatWISE/AllWISE infrared survey integration: still pending. The narrow
  UltracoolSheet bridge closes the immediate nearby accepted-inventory blind
  spot class, but it is not a replacement for a planned, volume-aware WISE
  survey ingest/crossmatch pipeline.

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

### 18) Nearby Ultracool Completeness Starter Bridge

- Added a controlled core inventory bridge for nearby UltracoolSheet rows that
  are absent from the Gaia backbone.
- Default policy is limited to rows within 10 pc with usable coordinates and
  distance/parallax. Existing Gaia backbone rows are not duplicated, and tight
  non-Gaia positional/distance duplicates are rejected.
- Promoted rows retain UltracoolSheet provenance, source URL/checksum metadata,
  spectral hints, SIMBAD-friendly aliases, and multiplicity flags. The bridge
  does not expand composite/resolved ultracool rows into invented component
  hierarchies.
- Added `nearby_ultracool_inventory_report.json` and
  `scripts/verify_nearby_ultracool_inventory.py`.
- This is intentionally a starter bridge for known nearby brown-dwarf omissions
  such as Luhman 16 and WISE 0855-0714. CatWISE/AllWISE remains a separate
  survey-scale integration.

Representative commits:
- pending (nearby ultracool inventory bridge working set)
- pending (hierarchy luminosity derivation and browser verification refresh)

### 19) WISE/CatWISE/AllWISE Evidence and IRSA Imagery v1

- Added a targeted WISE evidence path instead of making CatWISE2020 or AllWISE
  a normal core-inventory backbone.
- `scripts/collect_wise_evidence.py` queries CatWISE2020 and AllWISE around
  priority existing Spacegate objects, propagating high-proper-motion targets
  from the source coordinate epoch toward the WISE catalog epoch where possible.
- Cooked WISE artifacts are written under `state/cooked/wise/`:
  `wise_sources.csv`, `infrared_source_matches.csv`,
  `infrared_photometry.csv`, and `infrared_motion_evidence.csv`.
- `scripts/build_arm.py` materializes these artifacts into ARM support tables:
  `wise_sources`, `catwise_sources`, `allwise_sources`,
  `infrared_source_matches`, `infrared_photometry`, and
  `infrared_motion_evidence`.
- The collector also emits a narrow `infrared_candidate_queue.csv` for
  red/high-motion WISE candidates found during targeted cones; ARM materializes
  it as `infrared_candidate_queue` with review statuses, not inventory rows.
- Added `scripts/verify_wise_evidence.py` to confirm WISE evidence tables exist,
  match rows are sane, and WISE-like source catalogs have not leaked into
  `core.systems`, `core.stars`, or `core.planets`.
- Added lazy IRSA/AllWISE image-product support for system pages. The API
  queries IRSA SIA/IBE, generates W1/W2/W3 false-color PNG previews, preserves
  source-product links and attribution, and stores previews in a bounded runtime
  cache outside the repo.
- Policy retained: WISE/CatWISE/AllWISE rows are evidence and imagery support,
  not core object promotion. Missing nearby infrared-only candidates still need
  a reviewed candidate queue before core acceptance.
- First local priority seed run: 500 targets, 2,051 unique WISE sources, 2,105
  match/photometry/motion rows, 139 review candidates, 0 query errors. This was
  materialized locally as side build `20260711T_wise_v1_seed_side` and promoted
  on photon for inspection.

Representative commits:
- `fe1b7fb` (WISE cross-reference and infrared imagery v1)

### 20) System Narration Foundation v1

- Added a DISC-scoped `system_narrative_blocks` contract for public system-page
  explanatory prose.
- The served API now emits deterministic fallback narrative blocks when no
  reviewed/persisted DISC rows exist. Blocks are generated from existing
  system, hierarchy, planet, and WISE/infrared evidence and include generator
  version, evidence inputs, status, and concept hooks.
- The first block set covers: What You're Looking At, Why This System Matters,
  Infrared View, What We Know, What Remains Uncertain, and Further Exploration.
- The WISE/AllWISE infrared panel now has layperson-facing narration explaining
  false-color W1/W2/W3 survey imagery and making clear it is not an artist
  impression.
- Public narration remains presentation-layer content. It does not mutate
  `core` or `arm`, and future AAA-written replacements require reviewed
  evidence and explicit publication state.
- System Simulation now pauses/throttles when scrolled out of view so the
  simulation-first page stays efficient while readers inspect narrative and
  evidence sections.

Representative commits:
- pending (system narration foundation v1)

### 21) Multiplicity Evidence Integrity + SB9 v1

- Audited production science transforms for object-specific logic introduced
  to satisfy goldens. Named systems remain valid in verification fixtures,
  benchmark reports, and operator diagnostics, but may not alter catalog
  cooking or canonical reconciliation.
- Removed the default executable `config/core_accepted_supplements.json` path.
  Its Sirius A, Sirius B, and L 134-80 cases now live in
  `config/deferred_core_adjudications.json` as non-executable review inputs.
  `SPACEGATE_ENABLE_ACCEPTED_SUPPLEMENTS=1` now fails ingestion; the legacy
  code is retained temporarily only to make historical builds readable and is
  not an executable production path.
- The July 15 served CORE artifact predates this policy and still contains two
  inherited `athyg_accepted_supplement` rows (Sirius A/HIP 32349 and L 134-80).
  They are not silently deleted from immutable artifacts. The next full CORE
  rebuild will omit them unless a general rule or adjudication has replaced
  the retired path; this is an explicit expected delta, not a side-build
  mutation.
- Added `scripts/audit_science_transform_exceptions.py` and a narrow allowlist
  for report-only Castor/16 Cyg sample literals. The July 15 audit scans CORE,
  ARM, and canonical-ingest transforms with 11 classified report findings and
  zero unexpected result-changing literals.
- Corrected the prior assumption that SBX superseded all useful SB9 content.
  The active SBX export supplies current system-level linkage but does not
  preserve SB9's separate primary/secondary spectral-type columns. SB9 is now
  complementary default-on ARM evidence.
- Added deterministic acquisition of CDS `B/sb9` `ReadMe`, `main.dat`,
  `alias.dat`, and `orbits.dat`; the observed snapshot cooked to 4,079 systems,
  20,806 aliases, and 5,099 orbital solutions. Raw files and
  `sb9_manifest.json` preserve retrieval timestamps, byte counts, and hashes.
- Added `sb9_systems`, `sb9_aliases`, `sb9_orbits`,
  `multiple_component_evidence_matches`, and
  `multiple_component_stellar_evidence` to ARM. SB9 endpoint attachment
  requires an exact unique MSC `SB9_<sequence>` reference and two existing
  stellar graph endpoints. No name-only fallback exists.
- Added a general DEBCat endpoint rule requiring the same canonical system, a
  unique MSC period match within `max(0.01 day, 1%)`, and two existing stellar
  endpoints.
- Full ARM verification on `/tmp/spacegate-arm-sb9-v2.duckdb` accepted 855 SB9
  and 14 DEBCat binary matches. It quarantined 87 unresolved/ambiguous matches,
  excluded 43 SB9 matches with no component spectral type, and materialized
  1,104 endpoint spectral observations. Castor's six source classes are
  `A, M, A, M, M, M`; DEBCat independently supplies `M1_Ve` for both YY Gem
  endpoints.
- A second ARM build with identical build metadata reproduced all five v1
  evidence tables byte-for-row. Canonical aggregate hashes were:
  `sb9_systems=6cdf788375db`, `sb9_aliases=c02b5acd9274`,
  `sb9_orbits=dde2deb19dfa`,
  `multiple_component_evidence_matches=3ecc609a3691`, and
  `multiple_component_stellar_evidence=4df2e70a26ef`. Re-cooking the raw SB9
  snapshot also reproduced identical CSV SHA-256 values.
- Added focused SB9 fixed-width cooking tests and
  `scripts/verify_multiple_component_evidence.py`. The verifier checks source
  counts, explicit outcome accounting, duplicate evidence, allowed acceptance
  methods, and the Castor representative golden without adding Castor-specific
  build behavior. Machine-readable audit, coverage, quarantine, and determinism
  evidence is retained under
  `/data/spacegate/state/reports/verification/20260715_multiplicity_evidence_v1/`.
- Multiple-source reconciliation is intentionally a bounded monotonic pass, not
  open-ended recursion: normalize source rows, build hierarchy endpoints,
  attach exact/unique component evidence, then recompute affected derived
  classifications. Ambiguous cases are quarantined rather than repeatedly
  rematched.

Representative commits:
- `b255d1d` (generic multiple-component evidence ingestion and one-off audit)

### 22) Canonical Database Stability and General Recovery v1

- Rebuilt the full canonical database after retiring executable accepted
  supplements. The candidate contains zero `athyg_accepted_supplement` rows;
  object-specific exceptions remain prohibited in science transforms.
- Added a bounded Gaia-missing recovery rule for SBX systems represented by a
  unique AT-HYG row with exact HIP+HD agreement, no Gaia identifier, usable
  distance, source-position sanity, and SBX orbit evidence. Of 22 candidates,
  10 resolved to existing canonical objects and 12 became source-provenanced
  canonical stars; none were quarantined. Sirius A is recovered by this rule,
  not by a named-system branch.
- Added projected-J2016 WDS `AB` companion recovery using a unique Gaia
  secondary within the configured angular, distance, and proper-motion gates.
  Three companions, including Sirius B and 70 Ophiuchi B, passed; ambiguous
  candidates remain excluded.
- Generalized late source-object reconciliation to merge an identifier-less
  MSC/WDS component surrogate into a uniquely identified physical target when
  exact WDS/component scope and compact physical-consistency gates agree. This
  removed the duplicate 70 Ophiuchi B surrogate without system-specific code.
- Separated broad AT-HYG crosswalk identifiers from canonical inventory
  authority. Identifier coverage is preserved, but a crosswalk row cannot
  independently create an object.
- Preserved source-native bootstrap stable keys during canonical emission.
  Sequential `canon:system:legacy:<row>` keys and legacy-row star fallback keys
  are no longer emitted. This is a deliberate one-time stable-key namespace
  migration; representative numeric row IDs remain implementation details.
- Fixed public slicing so targeted TESS identity decisions are adjudicated once
  against the full canonical universe and projected into slices. Public builds
  can no longer convert ambiguity into acceptance merely because a competing
  object was trimmed.
- Corrected ARM evidence bindings that survived canonical reduction with stale
  collection-time identifiers. All 2,105 targeted WISE match, photometry, and
  motion rows now resolve their target, containing system, and stable key from
  the current CORE; 139 candidate-queue nearest-target contexts are normalized
  the same way. Verification fails on any remaining mismatch.
- Corrected public ARM retention predicates that dropped all 157,299 source WDS
  observation rows and all 2,105 WISE match rows. Public slices now retain WDS
  observations by retained WDS scope and WISE matches by canonical target scope;
  source-native component keys remain evidence identifiers rather than foreign
  keys into the canonical component graph.
- Full ARM refresh `20260716T0055Z_94bdab7_canonical_arm` passes the complete
  build gate and is CORE-identical to `20260715T2349Z_06ac777_b`. Its four WISE
  target-binding tables reproduce exactly across an independent validation
  rebuild after excluding build-instance metadata. Public slice
  `20260716T0100Z_94bdab7_public` retains 8,147 in-scope WDS observations and
  all 2,105 canonical WISE matches; the TESS projection comparator reports zero
  missing or unexpected rows in all four evidence tables.
- Final immutable local candidate `20260716T0103Z_94bdab7_side` adds exact
  100/250/500/1,000-light-year tile artifacts and 1,000 priority simulation
  scenes. Tile verification reports zero missing, extra, public-name, or
  representative-class mismatches at every radius; scene generation reports
  zero failures.
- Live API hierarchy goldens now identify source leaves by stable WDS-scoped
  component key rather than a mutable display-name prefix. This preserves
  Castor as a benchmark without requiring source-native `66alp Gem` leaf names
  to be rewritten by object-specific presentation logic; SB9-backed A/M, A/M,
  M/M facts are verified on those stable keys.
- Determinism comparison is now scoped to the canonical transform revision and
  hashes ARM science/lineage payloads while excluding only build-instance
  metadata. Paired full builds `20260715T2343Z_06ac777_a` and
  `20260715T2349Z_06ac777_b` reproduced exact CORE and ARM science hashes.
- The winning full build `20260715T2349Z_06ac777_b` contains 17,793,588 stars,
  17,788,043 systems, 6,311 planets, and 1,039,123 aliases. Against
  `20260713T1627Z_dd7446e`, planets and the full TESS partition are unchanged;
  the candidate has 9 more stars, 7 more systems, 247 more aliases, no
  identifier orphans, no TIC collisions, and no sequential legacy stable keys.
- Sirius is now a two-star WDS system with its A primary recovered generically
  and its Gaia white-dwarf companion retained. 70 Ophiuchi has two canonical
  members after generic surrogate reconciliation. L 134-80 remains absent and
  explicitly deferred to inspectable adjudication rather than restored by a
  one-off supplement.
- Retention was applied through `scripts/prune_state_retention.sh` only after a
  dry run, removing seven unserved obsolete/failed build directories and
  reclaiming about 164 GiB. Raw/cooked inputs, manifests, reports needed for
  verification, and the served build were preserved.

Representative commits:
- `31197e5` (generic SBX/AT-HYG recovery)
- `d9286c1` (source identifier storage correction)
- `2e609cb` (generic WDS component-surrogate reconciliation)
- `e6d74d5` (source-native canonical stable keys)
- `06ac777`, `214cb36` (transform-scoped CORE and science-payload determinism)

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
