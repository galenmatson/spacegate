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
  verification, and the served build were preserved. After public promotion,
  a second reviewed Photon retention pass removed two failed `.tmp` builds and
  reclaimed another 21.72 GiB with the same protected inputs intact.
- Public archive `20260716T0103Z_94bdab7_side.7z` was transferred resumably to
  antiproton and verified against SHA-256
  `7f2b27792f49502a8962259c23298dcc6852fe97d58920775fef7bd1b43fe450`
  before extraction. A reviewed edge-retention dry run removed two obsolete,
  unserved builds and reclaimed 22.54 GiB while preserving the active build;
  the previous runtime remains extracted for rollback after promotion.
- Antiproton now serves `20260716T0103Z_94bdab7_side`. Public API integration,
  known-system hierarchy/search goldens, auth/health checks, and 12 active
  desktop/mobile tiled-map Playwright checks pass, including all four radii,
  exact and progressive paths, search handoff, a 4K Bright render, screenshots,
  and nonblank canvas-pixel probes.
- Deployment exposed two bootstrap defects without changing science output:
  local `file://` activation copied the published 6.9-GiB archive into a second
  cache path, and promotion attempted to re-score an already materialized DISC
  build with host Python. Bootstrap now bounds local file artifacts to the
  operator-provided file base, extracts them in place, and supports explicit
  immutable promotion through `--skip-auto-score`.

Representative commits:
- `31197e5` (generic SBX/AT-HYG recovery)
- `d9286c1` (source identifier storage correction)
- `2e609cb` (generic WDS component-surrogate reconciliation)
- `e6d74d5` (source-native canonical stable keys)
- `06ac777`, `214cb36` (transform-scoped CORE and science-payload determinism)

### 22) Source Evidence Closeout + WDS Pair Policy v1

- Re-ran the source-evidence audit against a fresh ARM built from the pinned
  multiplicity inputs. The current MSC snapshot preserves 4,633 `orb.tsv`
  rows: 4,627 normalize to ARM orbit edges and six rows across five WDS scopes
  have neither a canonical system nor an MSC `sys.tsv` relationship.
- Added `msc_orbit_reconciliation` so accepted, excluded, and quarantined
  outcomes are machine-readable. The six tail rows are excluded by the general
  `source_system_not_in_canonical_inventory` rule; zero rows remain quarantined
  or unaccounted. No placeholder host or simulator endpoint is fabricated.
- Inspected the named Tegmine, Xi Scorpii, and Nu Sco warnings. Their orbit rows
  are normalized, but some source groupings partially overlap other valid
  groupings and cannot all inhabit one non-overlapping Kepler tree. They remain
  explicit presentation diagnostics pending evidence-based topology selection.
- Recooked all 157,299 WDS summary rows with case- and punctuation-preserving
  pair labels. `wds_pair_evidence` parses single-component pairs (`AB`) and
  scoped pairs (`Aa,Ab`, `AB,C`), then records unique, missing, ambiguous, or
  excluded endpoint bindings.
- The full canonical ARM accepts 2,077 unique source-scoped WDS pairs; the
  1,000-ly public slice retains 1,797. Every row is
  classified as a sky-projection measurement; bound-relationship assertions
  and simulation-ready WDS-only orbits both remain zero.
- Canonical hierarchy now carries `component_family` and `component_type`
  separately from structural `node_kind`. Full-bootstrap validation retains
  brown-dwarf, white-dwarf, pulsar, and inferred-leaf types; Castor CC is an
  inferred brown-dwarf endpoint rather than a hardcoded system exception.
- Updated the non-executable adjudication inbox: Sirius A/B record the reusable
  rules that resolved them, L 134-80 remains deferred, and Castor CC is deferred
  for classification/physical-status review without affecting catalog output.
- Bootstrap now installs checksummed reports advertised by `current.json` into
  a staged build-scoped report directory. Publisher discovery includes all
  build-scoped JSON reports rather than a fixed filename subset.
- Immutable public-slice and side builds now emit
  `derived_build_verification_report.json`. The report recomputes slice-native
  counts, identifier/stable-key uniqueness, relationship integrity, and
  required Parquet presence while hashing the applicable upstream verification
  reports. Strict verification validates the derived report against the actual
  served databases instead of relabeling full-canonical QC reports.
- Full canonical build `20260716T1229Z_43b7d24`, public slice
  `20260716T1356Z_43b7d24_public`, and side candidate
  `20260716T1410Z_43b7d24_side` pass their applicable evidence, hierarchy,
  multiplicity, extended-object, TESS, WISE, exact tile, and strict derived
  verification gates. The side candidate contains 1,000/1,000 priority
  simulation scenes with zero failures.

Representative commits:
- `794c04b` (source evidence reconciliation and hierarchy typing)
- `43b7d24` (checksummed published-report installation)
- `1797236` (slice-native derived-build verification)

### 23) Stellar Display Evidence Consistency

- Traced Castor AB/BB white-dwarf labels to a general case-destruction bug:
  source SB9 spectra `dM1e` were uppercased before parsing, turning luminosity
  prefix `d` into white-dwarf class `D`. A shared case-aware API parser now
  preserves `d`/`sd`/`esd`/`usd` notation while retaining `WD?`, DA, DB, and
  related white-dwarf notation.
- Corrected ARM component-display SQL and multiple-component evidence
  normalization with the same general rule. No Castor-specific data or build
  branch was added.
- Established public badge precedence: source spectral evidence first,
  explicit compact/object evidence second, and visual class only as fallback.
  This makes 32 Alf Leo's MSC `WD?` evidence outrank its 0.3-solar-mass
  main-sequence prior while still allowing its otherwise unclassified
  companion to display an assumed M badge.
- Public selected-object readouts no longer expose or copy internal stable
  keys. Inferred hierarchy members inherit preferred public member names and
  retain explicit component suffixes; chosen HD/HIP/Gaia catalog names remain
  valid last-resort public labels.
- Bumped prebuilt scene compatibility to `simulation_scene_artifact_v2`.
  Existing v1 scenes are rejected and rebuilt through the normal runtime path;
  the next side-artifact checkpoint must regenerate its complete bounded
  priority set before deployment.
- The first replacement side candidate exposed a reuse-path defect: copied v1
  files were reported as 1,000 reusable v2 scenes because reuse checked only
  file existence. Scene artifacts now embed the target build ID, and reuse
  requires both that ID and the current materializer version. The rejected
  candidate was not promoted or published.
- Replacement side build `20260716T154843Z_622f336_side_rebuild` regenerated
  1,000/1,000 priority scenes as v2 artifacts with zero failures or stale-build
  reuse. Strict database, multiplicity, TESS, WISE, extended-object, exact-tile,
  and derived-integrity verification passed before local promotion.
- The live Castor browser gate then exposed a brittle expectation that exactly
  two leaves would carry SB9 `dM1e` notation. The refreshed source evidence
  supplies three. The gate now tests the reusable scientific invariant instead:
  every present `dM1e` value must parse as an M star and never as a white dwarf;
  it does not prescribe how many source rows a future pinned catalog may carry.

### 24) Photon Retention Timestamp Compatibility

- Found that `scripts/prune_state_retention.sh` accepted second-resolution
  build IDs (`HHMMSSZ`) but not the minute-resolution IDs (`HHMMZ`) used by
  most current builds. The mismatch left 927 GiB under `state/out` while a dry
  run incorrectly reported no eligible builds.
- Extended the general recognizer to both timestamp forms and added an isolated
  regression test proving that active served builds and named non-build
  workspaces remain protected.
- Applied the documented Photon policy of retaining the newest 12 recognized
  builds, 24 report sets, and the active served build. The reviewed plan removed
  15 obsolete builds, reclaimed 352.52 GiB, and reduced `/data` utilization
  from 93% to 68% without touching raw/cooked catalogs, published archives,
  models, or named research workspaces.

### Shared Stellar-Leaf Display Projection (July 16, 2026)

- Added `stellar_leaf_display_classifications` as a deterministic ARM
  presentation projection over canonical hierarchy leaves. It keeps CORE source
  class first, then exact accepted/source, derived, and explicitly assumed
  component evidence; missing values remain `UNKNOWN` and conflicts retain
  their candidate set and provenance.
- The initial full projection accounts for 5,879,144 eligible leaves exactly
  once: 5,530,708 source, 3,324 derived, 7,757 assumed, and 337,355 missing,
  with zero invalid or duplicate rows. The general verifier reports 105 legacy
  `system.star_count` differences without allowing that facet to redefine
  audited hierarchy membership.
- Golden multisets are `GKMM` for HD 110067, `FKMM` for HD 79107, `FKMMU` for
  Gl 161.1, `MUUUU` for HD 18134, and `AAMMMM` for Castor. These are outputs of
  shared evidence precedence and hierarchy rules, not per-system overrides.
- Removed display-name deduplication from simulation leaf assembly, which had
  collapsed distinct HD 79107 members sharing a source label. Inferred
  nonstellar endpoints such as Castor CC no longer become stellar bodies or
  badges merely because their structural node kind says inferred leaf.
- Map tile v4 consumes the projection for both `All` and the physically ranked
  `Primary` selection, and adds a separate bounded planet-category bitmask.
  Neither presentation artifact changes CORE planet counts or canonical source
  classifications.
- Local side build `20260716T1905Z_ad13e39_side` materialized the projection,
  exact tile-v4 membership at 100/250/500/1,000 ly, and 1,000/1,000 priority
  simulation-scene v3 artifacts. Strict verification passed. Live API and
  Playwright checks confirm identical leaf-class multisets in map tiles,
  system summaries, hierarchy leaves, detail heroes, OBJECTS/scene bodies, and
  simulations for the five golden systems.
- Tile-v4 planet masks are intentionally evidence-conservative. HD 110067 and
  TRAPPIST-1 demonstrate decoded category bits, but Solar System rows currently
  lack equilibrium-temperature/insolation values and remain unbadged. The next
  refinement must use a general, provenance-bearing host-luminosity and orbital-
  distance derivation rather than a literal Sol branch.

### 25) Canonical Badge Membership and WDS Bridge Hardening

- Search result cards and System Hero now consume complete ordered object badge
  lists: duplicate stellar classes are preserved and every confirmed CORE
  planet is represented. Stable object/component keys and text IDs are retained
  so the badges can become object-detail links without changing the API shape.
- Simulation-scene artifact v4 resolves shared leaf classifications through
  unique hierarchy, canonical leaf-component, and source evidence-component
  keys. With the projection present, unprojected ARM endpoints are evidence
  only and cannot become extra render stars. HD 57041 therefore follows its
  canonical `K,WD` leaves, and the `WD?` source component renders as a white
  dwarf instead of inheriting an M-class mass prior.
- The Struve 2398 overlap exposed an upstream MSC component row claiming WDS
  `18428+5938 B` while carrying V1298 Aql/GJ 752 B identifiers and coordinates
  more than 50 degrees from the WDS field. Ingest now gates only gross WDS-field
  position mismatches (a conservative 10-degree floor, expanded for reported
  pair extent) and reports rejected rows rather than silently creating stars.
- A separate general bridge attaches an ungrouped member to an existing WDS
  system when both stars share either an exact authoritative Gliese/GJ catalog
  identifier root or an exact proper-name root and pass one-light-year,
  one-degree, and unique-best-match bounds. A full-corpus pre-build audit
  produces two catalog-root candidates: the real Gaia/AT-HYG Struve 2398 B
  member and a 37-arcsecond Gl 277 companion. No literal system name, WDS ID,
  Gaia ID, or per-system output override is present in the build logic.
- Canonical rebuild `20260716T2323Z_868b4d9` reports three rejected gross MSC
  position mismatches, 262 total WDS bridges, and exactly two catalog-root
  bridges. Canonical reduction `20260717T0035Z_868b4d9_canonical` and public
  side artifact `20260717T0057Z_868b4d9_side` pass the full build,
  multiplicity, source-evidence, shared-leaf, and tile gates. In the public
  artifact Struve 2398 resolves as one two-star M+M WDS system, the displaced
  V1298 Aql surrogate is absent, and HD 57041 retains exactly its canonical K
  and white-dwarf leaves for the shared display projection.

### 26) WDS-Supported Terminal MSC Leaves

- Public known-system verification of the first July 17 deployment exposed a
  canonical-hierarchy regression: ARM retained all seven source-native Nu Sco
  stellar endpoints, but the shared display projection omitted unmatched
  single-letter endpoint `B`. The deployed interim build remained otherwise
  healthy while a corrected immutable candidate was rebuilt.
- Canonical hierarchy now retains a terminal single-letter MSC stellar
  endpoint only when it is reachable from the source hierarchy, has no child
  containment edges, is absent from canonical root-role mappings, and a WDS
  pair with at least two observations links it to an already resolved sibling.
  The candidate set is accepted only when the MSC terminal-endpoint count
  exceeds the hierarchy's already represented physical-leaf count by enough
  capacity for every unmatched candidate. This is a general ARM display-
  hierarchy rule; it adds no CORE inventory row and contains no system name,
  WDS ID, or source ID branch.
- The full hierarchy evaluates 719 WDS-supported candidates, accepts 658, and
  explicitly suppresses 61 because canonical leaves already exhaust their
  source-tree capacity. The 1,000-ly public side retains 656 accepted support
  leaves and materializes 5,879,796 shared display leaves exactly once. Nu Sco
  remains six B-class leaves plus one unknown; Struve 2398 remains one system
  with two M-star leaves; Beta Mon does not gain a speculative singleton
  expansion. The capacity decision counts and reasons are machine-readable in
  `canonical_hierarchy_report.json`.
- Canonical emission now accepts an explicitly paired hierarchy database and
  report override. This permits hierarchy-only scientific transforms to be
  re-emitted without mutating the immutable raw build or separating an
  artifact from its provenance report.
- Known-system goldens no longer freeze source spelling variants such as
  `M1_Ve` versus `dM1e`, or raw compact token `D` versus normalized display
  class `WD`. They continue to assert stable component binding, normalized
  class, mass, and source compact-object type.
- Replacement canonical build `20260717T0557Z_f452835_canonical`, public slice
  `20260717T0607Z_f452835_public`, and side artifact
  `20260717T0614Z_f452835_side` retain the same 5,869,091 systems, 5,874,636
  stars, and 6,311 planets as the preceding public build. The earlier
  `20260717T0336Z_8bee500_side` checkpoint exposed the Struve capacity defect
  during post-deployment verification and is superseded. Strict build,
  multiplicity, TESS, WISE, extended-object, shared-leaf, exact tile, local
  API, known-system, and targeted Playwright gates pass; 1,000/1,000 scene-v4
  artifacts were regenerated with zero failures. The replacement was deployed
  to antiproton at 2026-07-17 08:23 UTC after local/remote archive SHA-256
  agreement. Public health, auth, API integration, known-system, targeted
  Struve 2398/HD 57041/Nu Sco, and map Playwright checks pass on
  `coolstars.org`; antiproton retains `20260717T0336Z_8bee500_side` as its one
  immediate rollback checkpoint.

### 27) Broad Planet Navigation Categories

- The map's single broad habitable-zone toggle was replaced with six
  independently selectable hot/temperate/cold Jupiter/terrestrial navigation
  bins. Multiple selected bins use OR semantics for both loaded tile labels and
  `/api/v1/systems/search`; no selection disables the category filter.
- Tile and API paths now import one SQL policy. The presentation label
  `Jupiter` maps internally to a broad `giant_or_enveloped` proxy and is not a
  canonical composition assertion. Radius takes precedence over mass;
  2-6 Rearth and mass-only 10-50 Mearth objects remain unclassified rather than
  being forced across the Fulton gap or into a Jovian label.
- Only confirmed, visible, nontombstoned CORE major planets contribute. TESS
  candidates and negative evidence stay in ARM. Served-build audit finds 2,742
  classifiable planets and 877 host systems with at least one map bit; 1,143
  confirmed-planet systems remain explicitly outside these filters because
  composition or environment evidence is incomplete or ambiguous.
- Environment remains the existing source equilibrium-temperature/insolation
  proxy. A host-dependent incident-flux/HZ derivation, including component
  binding and multistar irradiation policy, remains a general follow-up rather
  than a Sol-specific patch.
- Source audit also found that CORE's promoted mass fields use NASA
  `pl_masse`/`pl_massj`, while the wider `pl_bmasse` best-mass field mixes true
  mass, `M sin i`, deprojected values, and mass-radius-relation estimates. This
  broader evidence remains deferred until its `pl_bmassprov`, uncertainty, and
  limit semantics can be retained rather than mislabeled as measured mass.

### 28) Catalog-Wide Feature Utilization Audit

- The earlier source-evidence audit was expanded beyond MSC/WDS orbit flow to
  compare every active cooked catalog against the served CORE/ARM projection,
  simulation/HZ consumers, planet taxonomy, roadmap milestones, and AAA goals.
  `scripts/audit_catalog_feature_utilization.py` emits the reusable machine
  report; the first snapshot is
  `/data/spacegate/state/reports/source_catalog_utilization_report_20260717.json`.
- The largest acquisition gap is in the Gaia DR3 astrophysical-parameter table
  already used for DSC class probabilities. Official boundary-matched TAP
  queries find 3,428,436 FLAME luminosity/radius rows, 1,136,048 mass rows, and
  1,026,163 age rows, while served Gaia ARM rows contain none of those fields.
  The next evidence bundle will preserve bounded source values, uncertainty,
  flags, and evolutionary context in ARM rather than fabricating CORE facts.
- Open-cluster membership exposed a cross-release identity defect: 234,128
  Gaia DR2 membership rows are currently joined directly to Gaia DR3 IDs. A
  full-canonical `dr2_neighbourhood` reconciliation with explicit outcomes is
  required before the next rebuild.
- ORB6 preserves 4,051 cooked orbit rows but normalizes only 56 because the
  builder requires an already-existing unique binary edge. The safe accepted
  set remains unchanged; v2 must preserve all detail rows and exhaustively
  partition the unaccepted tail before expanding bindings.
- Cooked NASA planet physics, Gaia NSS orbit parameters, DEBCat component
  physics, compact-object spin/activity, white-dwarf alternative atmosphere
  fits, cluster ages/extinction, SNR radio flux, and TESS EB sector/Tmag
  evidence are now tracked as one Catalog Evidence Utilization v2 bundle. The
  explicit intent is one canonical rebuild after the evidence flows are ready,
  not successive catalog-specific rebuilds.
- HZ diagnosis confirms the renderer is evidence-gated rather than randomly
  omitting disks. Approximately 4,966,395 served CORE stars qualify through a
  guarded main-sequence prior and 2,002 through source luminosity or
  radius+Teff. Ultracool objects, remnants, unclassified leaves, and evolved
  stars without adequate physics intentionally receive no disk. Gaia FLAME can
  replace many illustrative priors, but remnant and quality guards remain
  mandatory.
- Full report and acceptance gates:
  `docs/SOURCE_CATALOG_UTILIZATION_AUDIT_2026-07-17.md`.

### 29) Evidence Lake v2 Main-Quest Promotion

- The July 17 catalog-wide utilization audit showed that the conservative v1
  collector/cooker omitted or stranded foundational Gaia physical parameters,
  source-native planet observations, multiplicity/orbit values, cluster
  cross-release identity, and compact/variability evidence. Several downstream
  derived or assumed values therefore ran when richer source evidence existed
  upstream.
- On July 18, 2026, the project promoted Evidence Lake v2 from a bounded
  catalog-utilization rebuild to the current main quest. The accepted plan is
  `docs/EVIDENCE_LAKE_V2.md`; staged delivery and acceptance are tracked as
  M8.3c-E0 through E7 in `docs/MILESTONES.md` and `docs/CHECKLIST.md`.
- This is a collector, cooker, evidence, selection, and build-compilation
  redesign, not a rejection of the permanent identity graph, CORE/ARM/DISC/RIM
  boundaries, immutable build model, provenance/quarantine rules, or the
  prohibition on named-object production transforms.
- Source model estimates remain model estimates. The new compiler will prefer
  acceptable source evidence over coarse Spacegate fallbacks while preserving
  method, assumptions, uncertainty, coherent parameter sets, alternatives, and
  conflicts instead of treating every catalog scalar as direct ground truth.
- Public evidence inspection and interactive image/spectrum/light-curve tools
  are recorded as later M8.3d/M8.3e objectives. They depend on stable evidence
  and observation-product contracts and do not expand the foundation rebuild.
- E0 completed on July 18 with a 32-source registry, exact accounting for all 48
  current manifest entries, a pinned schema baseline covering 1,795 enumerated
  fields, and fail-closed full-refresh preflight. Documented source formats not
  yet parsed as typed tables are pinned by exact artifact contract until E1.
- The E0 storage audit found two published-metadata references in the original
  retention candidate set. Retention therefore gained explicit protected-build
  inputs before apply. The reviewed plan preserved 11 lineage builds, reclaimed
  196.21 GiB of unreferenced immutable output, and increased `/data` free space
  from about 189 GiB to 385 GiB without deleting raw, cooked, catalog mirror,
  or scratch science state.
- E1 established content-addressed raw snapshots for all 25 available
  non-planned releases (392 files, about 11 GiB) and independently versioned
  typed snapshots. The first estate pass exposed and fixed two silent-loss
  hazards: NASA `pscomppars` could retain its row count while collapsing 683
  comma-delimited fields into one, and MAST chunks could let an all-null batch
  dictate an unusable Arrow type. Both now fail closed with shape or declared-
  schema checks.
- AT-HYG v33 is a two-part logical table whose second compressed file has no
  header. Its release layout is now explicit and the typed lake accounts for
  1,276,082 rows from part 1 plus 1,276,083 from part 2, retaining 34 fields.
  The corrected layout raised the pinned E0 field count from 1,795 to 1,807;
  the previous count had mistaken part 2's first data row for a header.
- The first E1 compiler checkpoint recorded 25 typed tables and 22 parser-
  pending tables. Pending formats remained preserved byte-for-byte and were
  acceptance failures, not silently accepted opaque inputs.
- E1 completed later on July 18 with 59 active raw artifacts/403 files and 68
  source-native typed tables containing 48,936,930 rows. Every raw artifact is
  represented in typed accounting and no parser remains pending.
- Official WDS/CDS schema documents replaced positional guesswork. This exposed
  and fixed a schema-parser defect that had briefly interpreted numbered CDS
  note prose as fields. MSC now retains all four documented data tables,
  including notes; ORB6 retains its complete 35-field row; ATNF retains repeated
  values and 97,472 source conflict/comment lines; and the white-dwarf lake
  retains all 161 FITS columns and alternative H/He/mixed fits.
- A clean-root reproduction gate exposed nondeterministic row-group ordering in
  parallel DuckDB writes for the large Gaia and TESS tables. Ordered single-
  thread serialization is now versioned parser behavior. All 25 releases then
  reproduced byte-for-byte, and the temporary reproduction tree was removed.
- E2 added the official Gaia DR2-to-DR3 neighborhood as a release-scoped
  evidence source rather than treating release IDs as comparable integers. A
  deterministic union of NASA, TIC, cluster, white-dwarf, and ultracool DR2
  fallbacks produced 1,542,049 targets and 1,626,847 forward candidate rows.
- A forward-only lookup could not expose a DR3 candidate with another DR2
  predecessor outside that target union. E2 therefore acquired an independent
  reverse universe: 1,625,665 DR3 targets and 1,776,331 official rows. Exact
  ADQL, chunks, timestamps, hashes, and manifests are retained for both
  directions, and the new raw/typed snapshots reproduce byte-for-byte.
- The registry consequently expanded to 34 source contracts and 63 manifest
  artifacts; the reviewed schema baseline now covers 1,824 fields. The active
  typed lake contains 27 sources and 72 tables with 55,507,822 rows and no
  pending parser artifacts.
- Identity compiler v8 consumes only its 13 declared typed inputs plus the
  current CORE as a read-only stability reference. Graph
  `c84389ad55f17081fff008b4` accounts for 226,392 accepted current-object
  bindings, 1,234,609 release mappings outside the current backbone, 79,671
  DR2 splits, 1,372 DR3 merges, and five missing targets. No forward/reverse
  payload conflict or canonical Gaia DR3 collision was found.
- Earlier compiler iterations incorrectly conflated Gaia's common
  `proper_motion_propagation` flag with high proper motion and hashed all typed
  tables into the graph ID. Those artifacts were never served. The final policy
  separates epoch-propagation safeguards from the 812 accepted stars above 500
  mas/yr and fingerprints only the 13 consumed typed tables.
- E2 materializes physical identity, containment, component/subsystem,
  observation-target, and alias/name scope separately. Existing containment is
  explicitly labeled `stability_reference_not_new_authority`; all 186,198 raw
  MSC/WDS relations remain candidate claims, with a gate proving zero canonical
  promotions. Ordered graph Parquet tables passed a clean independent compile
  comparison before E2 completion.
- The final provenance audit replaced placeholder source-family releases with
  the registered source, release, and typed-table identity on every binding and
  added the forward/reverse acquisition releases to every crossmatch edge and
  outcome. Family-level equivalence and duplicate-system gates prove that no
  attempted source family disappeared and that 18 accepted components sharing
  root systems remain distinct permanent stars.

### 30) Evidence Lake E3 Acquisition Foundation

- On July 19, E3 replaced ad hoc wide-table downloads with two data-driven
  collectors. TAP products preserve exact ADQL, official schema descriptions,
  deterministic partitions, UWS job history, exact responses, row/MAXREC
  checks, and field dispositions. HTTP products preserve exact release bytes,
  resumable Range transfers, expected sizes/checksums, and immutable snapshot
  manifests. Long job budgets are separated from bounded socket-inactivity
  recovery so route changes cannot strand resumable transfers for an hour.
  Shared manifest promotion is inter-process locked and atomic.
- Official Gaia schema enumeration found 764 columns across `gaia_source`, AP
  main/supplementary, NSS two-body orbit, variability summary, and rotation
  modulation. The measured 1,250-ly source envelope contains 31,987,126 rows;
  the expanded NSS product already increased retained orbit evidence from
  36,151 narrow rows to 50,762 complete 77-column rows.
- The first completed source slice pinned and typed the IAU WGSN page and all
  registered GCVS release files. GCVS now contributes 60,894 variable-star
  catalog rows, 226,060 cross-identifiers, 26,018 suspected variables, 25,696
  bibliography rows, and the exact classification dictionary and source schema
  documents. WGSN is no longer stored only as document lines: its validated
  semantic cook preserves 597 distinct names, all 16 source fields, source row
  identity, and linked resources, with the repeated footer and calendar table
  explicitly excluded. Raw/typed hash and artifact accounting pass.
- Review of the GCVS lexical cook found structural trailing `|` separators
  retained as values in `Exists`, `Ident`, `VarName`, and `f_NSV`. The general
  fixed-width parser now accepts a registry-scoped delimiter policy: it trims
  the slice, removes exactly one configured trailing separator, then trims
  remaining layout padding. Internal delimiters and exact raw rows are
  untouched. Snapshot `ef540a47c43892e17ddc2bae` accounts 203,740 changes;
  its typed A/B report proves identical rows, schemas, and raw checksums with no
  changes outside those four fields. Verification and clean reproduction pass
  content hash
  `7e6b3cd985b8df6df7b25eb43949ae112e3eea32ad094cc3b90ce6972639ff20`.
- NASA archive probing exposed that current Kepler KOI/TCE tables use uppercase
  legacy TAP names. E3 now explicitly preserves DR25 KOIs, supplemental and
  cumulative KOIs, DR25 threshold-crossing events, and the transit-detection
  reference table alongside complete planet, composite, host, TOI, K2, and
  name tables. The completed immutable slice contains 12 typed tables, 206,989
  rows, and all 2,093 upstream fields with zero omissions. The earlier schema
  miss is fixed generally, not by a named-object transform.
- SIMBAD is treated as an ODbL identity/naming evidence service rather than a
  catalog mirror. E3 first acquires its 14,188,016-row Gaia DR3 identifier
  bridge, then locally intersects it with the Spacegate envelope before any
  targeted basic/alias/bibliography follow-up. Large Gaia, distance, cluster,
  wide-binary, NASA, SIMBAD, and spectroscopy acquisitions remain in progress;
  no served database has changed.

### 31) Evidence Lake E4 Scientific Evidence Compiler Foundation

- On July 19, E4 established a separate immutable pre-ARM compiler artifact
  rather than writing new source semantics directly into the served ARM. The
  contract defines 22 bounded scientific domain tables plus exact source
  records, field dispositions, and explicit identity/component binding outcomes.
- The first NASA foundation build accounts 206,989 source rows as 203,932 exact
  records. It preserves 3,057 repeated identical upstream rows through duplicate
  counts and retains non-unique logical keys without manufacturing uniqueness
  from source array positions.
- All 2,093 NASA fields receive a machine-readable disposition. Checkpoint
  `cb82c09179afa740b02e2cdf` materializes 750,151 release-scoped
  identifier claims and 72,809 lifecycle claims in addition to source-record
  context. The completed NASA adapter materially represents 2,081 fields and
  deliberately excludes 12 archive spatial-index helpers from scientific
  evidence while preserving them in the immutable typed source.
  Confirmed, candidate, false-positive, false-alarm, and refuted claims retain
  distinct polarity and cannot change canonical inventory at this stage.
  Per-identifier semantic scopes create 697,952 explicit unresolved binding
  outcomes, preventing mixed planet, host, and observation-target fields in one
  archive row from sharing an accidental binding scope.
- The adapter emits 9,689,745 typed science rows, 272,355 coherent parameter
  sets, 111,084 validation products, 2,961 parsed source references, and
  4,656,423 evidence-citation links. Build identity includes compiler and
  registry hashes plus runtime versions. The NASA adapter reports `pass`; E4
  remains open for the other registered sources. A clean scratch rebuild must
  reproduce the build ID and every logical table hash exactly.

### 32) Evidence Lake E3 Probability-Bearing Wide Binaries

- The pinned Zenodo release for El-Badry, Rix, and Heintz (2021) is preserved as
  immutable snapshot `aea36fe5a6753de90be33301`: 1,817,594 main-catalog rows
  and 517,993 shifted-control rows retain all 217 and 201 source columns.
- Both published selection/neighbor method scripts are retained and typed as
  source documents. Schema/row verification and clean typed-hash reproduction
  pass. These rows remain relation evidence rather than canonical containment;
  envelope intersection and E4 probability/quality policy were completed in the
  following iteration.

### 33) Evidence Lake E4 Bounded Wide-Binary Relations

- E4 added a generic relation-claim adapter rather than a catalog- or
  named-system transform. Relation endpoints carry release-scoped identifier
  namespaces and independent `left`/`right` component binding scopes.
- The registered `buffered_parallax_3sigma_overlap_v1` policy retains a main or
  shifted-control pair when either component's three-sigma parallax interval
  overlaps the deterministic 1,250-ly buffer. From 2,335,587 FITS rows plus 360
  method-code lines, checkpoint `aaf262b1791d98ce3e9f96e7` materializes 877,307
  candidate pairs, 239,406 negative controls, and all method lines while
  explicitly reporting 1,218,874 filtered rows.
- The paper defines `R_chance_align` as the ratio of KDE-estimated
  chance-alignment density to candidate density. It only approximates a
  probability and can exceed one. The compiler therefore preserves 1,116,713
  raw and normalized confidence statistics, including 289,705 above one, and
  creates zero strict probabilities. It does not derive `1-R`, accept a binding,
  or modify canonical containment.
- All 422 source fields are dispositioned: 24 identity, relation, or method
  fields are materialized; 398 copied Gaia component fields remain losslessly
  preserved in typed Parquet and are deliberately excluded from the relation
  projection so component facts cannot leak onto relation or system scope.
- The 11,129,073,664-byte artifact reproduces logical hash
  `2b45feebcbe9bb3f18743b1043613ca2c454abf9cb393836e9a0c542d220dcaf`.
  A separate artifact audit finds zero missing endpoint claims/scopes, Gaia zero
  sentinels, invalid probabilities, orphan citations/lineage, negative
  uncertainty magnitudes, or parameter-set integrity failures.

### 34) Evidence Lake E4 ORB6 and DEBCat Scoped Evidence

- The compiler now maps typed tables to their source-native raw artifact names,
  allowing archive or legacy raw products to emit independently named typed
  tables without weakening exact raw/typed coverage checks. Product metadata
  falls back to the pinned typed source schema when an older immutable raw
  snapshot predates per-product manifests.
- ORB6 checkpoint `fcbb6466bea0a7798ae8d2ed` preserves all 4,051 rows and 37
  fields as coherent visual-orbit solutions, plus 16,397 WDS/discoverer/ADS/HD/
  HIP claims and 799 reference-code citations. No discoverer designation is
  parsed into endpoints and no relation is accepted merely because an orbit
  row exists.
- Scientific-evidence contract v2 introduces explicit component scope on
  stellar classifications, parameter sets, and measurements. Its artifact
  audit rejects parameter-set scope mismatches and component evidence without
  an explicit binding outcome.
- DEBCat checkpoint `b3a141c0caf953aa83c4e52b` preserves all 374 rows and 30
  fields. It emits 3,804 component/system physical measurements, 557 component
  classifications, 963 coherent parameter sets, 746 integrated photometry
  values, and 374 period solutions. Primary, secondary, and system evidence
  remain distinct. Missing sentinels are excluded from evidence and missing
  uncertainty sentinels become null rather than an erroneous large uncertainty.
- Both checkpoints pass clean logical-hash reproduction and the independent
  artifact audit. They do not alter the served database or canonical hierarchy.

### 35) Evidence Lake E4 Green SNR and TESS EB Evidence

- Scientific-evidence contract v3 separates extended-object geometry, distance,
  and source-native physical parameter sets. It also supports deterministic
  composite identifier claims and declarative scalar measurements without
  forcing nonnumeric source values through a numeric normalization.
- Green SNR checkpoint `d08c5aa9af7dc8bcdbf0d6c3` preserves all 310 rows and
  15 fields. Galactic longitude/latitude form 310 source identifiers such as
  `G0.0+0.0`; geometry, angular size, SNR type, aliases, detail links, and raw
  1-GHz flux/spectral-index values retain their exact uncertainty markers.
- TESS EB checkpoint `255678b2daa6e8bf46e6dcd9` preserves all 17,605 rows and
  20 fields. Raw zero-padded TIC strings normalize to ordinary decimal TIC IDs.
  The source's 4,584 `in_catalog=true` rows receive orbit solutions; 13,021
  false rows remain negative catalog-status evidence. Sector coverage,
  source/quality flags, morphology, Tmag, six astrometric quantities, and
  unresolved target-context stellar parameters are independently typed.
- Both source artifacts pass clean logical-hash reproduction and the expanded
  artifact audit, including citation, extended-geometry, identifier, scope,
  uncertainty, parameter-set, and orphan-lineage checks. Neither changes the
  served database or canonical inventory.

### 36) Evidence Lake E4 White-Dwarf Alternative Models

- The 1,280,266-row Gaia EDR3 white-dwarf catalogue is retained losslessly in
  raw/typed storage but is not projected wholesale into the hot evidence
  database. `buffered_posterior_distance_overlap_v1` retains 337,272 rows whose
  published geometric-distance posterior lower bound is within 383.245 pc,
  explicitly reporting 942,994 outside-envelope rows.
- Checkpoint `486e4975af015d4e5f5a3c9b` accounts all 161 source fields. It
  materializes WDJ/Gaia EDR3/Gaia DR2/designation identity, candidate
  probability/quality context, and 2,390,432 Teff/log-g/mass/chi-square
  measurements in 597,608 coherent hydrogen, helium, and mixed-atmosphere
  parameter sets. It selects no atmosphere winner.
- The remaining 125 copied Gaia/SDSS fields stay exact in source-native typed
  Parquet and source-record lineage, with reviewed exclusions assigning their
  scientific semantics to release-native Gaia, distance, and survey adapters.
- The first build attempt exposed a general SQL ambiguity between a catalog's
  `source_id` field and compiler lineage. It failed before promotion. Compiler
  v22 now qualifies every source-column JSON expression, preventing the same
  defect in Gaia NSS and future adapters; the failed hidden temporary directory
  remains subject to retention dry-run rather than manual deletion.
- The 2,069,114,880-byte artifact passes the independent scope, identity,
  probability, citation, compact-parameter, uncertainty, and lineage audit.
  Clean reproduction matches logical hash
  `02bfb585c0941285cf4fa10326b45f478df62685586cd76ae150883251f26278`
  and removes its scratch artifact.

### 37) Evidence Lake E4 ATNF Pulsar Parameters and Bibliography

- E4 added general ordered-table, predicate-scoped identifier, authoritative
  citation-catalog, and reference-validation contracts rather than parsing ATNF
  through a catalog-specific production branch. References are materialized
  before the parameter and glitch tables that use their source keys.
- Checkpoint `64c55c19a5a10a88877d4cd2` accounts all 190,671 typed package
  rows and all 37 table-column occurrences: 91,214 repeated parameter records,
  644 glitches, 1,210 complete references, 97,472 comments, 108 README lines,
  and 23 archive-member records. It emits 91,858 compact-object parameter
  contexts and 97,424 release-scoped ATNF/PSRJ/PSRB identity claims.
- The source's fourth parameter token is lexically overloaded. Only exact
  matches to the 1,210 authoritative reference keys populate evidence
  `reference_raw`, producing 84,388 citation links. Another 959 populated
  tokens remain visible in raw parameter JSON but receive no invented citation.
- The 286,011,392-byte artifact has logical hash
  `5bcf94b69a5a0e1a1905f2a891fd95d7f852c6c9af55531cdf6d9448f6747834`.
  The independent artifact audit and clean scratch reproduction pass. No ATNF
  row has been promoted to canonical inventory or selected as a public fact.

### 38) Evidence Lake E4 McGill Magnetar Coherent Contexts

- Compiler v24 added a general list of predicate-scoped compact-object
  parameter sets per source record. Evidence IDs include `compact_kind`, so
  timing, X-ray, distance, position, and contextual evidence cannot collide or
  masquerade as one compatible fit.
- McGill checkpoint `c599c951590451ace4248934` accounts all 31 catalog rows and
  all 47 fields. It emits 26 timing contexts, 26 X-ray contexts, 25 distance
  contexts, 31 positions, and 31 association/band/activity contexts, with each
  retaining its own source-native reference family.
- Seven names carry trailing `#`/`##` publisher footnote markers. The exact raw
  identifier remains immutable; the general
  `strip_trailing_hash_footnote_v1` policy removes only those trailing markers
  from normalized identity and leaves internal hashes untouched.
- The pinned CSV contains reference codes but not the publisher's expanded
  bibliography. E4 retains 96 distinct raw code records and 128 evidence links;
  E3/E4 must acquire and validate full citation text rather than manufacturing
  it. The 6,041,600-byte artifact has logical hash
  `9d95e4669d24ff8c0db396f253436d96be42a81c9f390d0cd2b9883cf93f2979`.
  Clean scratch reproduction and the independent artifact audit pass; no served
  or canonical database changed.

### 39) Evidence Lake E4 SB9 Scoped Relations and Linked Orbits

- The first SB9 E4 build exposed a general audit assumption: relation endpoint
  scopes were implicitly `left`/`right`. Compiler/schema v26 now stores explicit
  endpoint component scopes, and the independent audit matches each endpoint
  through its identifier claim and corresponding binding outcome. El-Badry
  remains left/right; SB9 remains primary/secondary.
- Compiler v27 adds required cross-table orbit-to-relation linkage through exact
  source logical keys. A zero- or multi-match leaves the link null and fails a
  required build. All 5,099 SB9 orbit rows link by `Seq` to exactly one of 4,078
  system relations; multiple source solutions remain separate, up to six for a
  system.
- Checkpoint `72663823963198c8fcbbe569` accounts all 30,153 rows and 62
  table-column occurrences from the pinned ReadMe, main, alias, and orbit files.
  It emits 4,079 positive binary claims, 4,079 component spectral observations
  (3,478 primary; 601 secondary), and 4,403 component magnitude measurements
  (3,978 primary; 425 secondary). Missing secondary evidence is not inferred.
- Alias handling emits 3,543 Gaia DR2 and 3,530 Gaia DR3 claims in separate
  release namespaces. Direct ADS-bibcode parsing resolves 1,807 of 1,826 source
  references and preserves every raw reference. The 95,956,992-byte artifact
  has logical hash
  `1406dc3e6c30b4b1e92bfc333abb953478d0f38b1f473ba7419c70c9750c2ddf`.
  Clean scratch reproduction and the independent audit pass. No SB9 claim is
  canonical containment, and no named-system production branch was added.

### 40) Evidence Lake E3/E4 Full SBX Evidence

- The Evidence Lake audit found that the legacy SBX collector preserved only
  10 of 29 system fields, selected five alias families, and replaced the orbit
  table with per-system counts. This was a general source-boundary loss, not a
  named-system defect. The served legacy files remain unchanged for E6 A/B.
- The same registry audit identified preserved legacy Gaia backbone/classifier/
  NSS and NASA `ps`/`pscomppars` manifests. They are now explicit disabled
  stability-reference releases rather than unregistered artifacts; the active
  full-release sources remain the only Evidence Lake compiler inputs.
- A separate `evidence-v2` acquisition profile now preserves the complete small
  rolling catalog without a spatial cut: 4,080 systems, 102,459 aliases, 261
  configurations, 5,169 full orbit solutions, and all 73 table-column
  occurrences. Immutable raw/typed snapshot `ea236790d0501967b3c30466`
  verifies and reproduces from the exact TAP queries and checksums.
- Compiler/contract v29 emits source-scoped primary/secondary binary relations,
  child/parent hierarchy relations only when a parent is asserted, complete
  coherent orbit solutions, component classifications/magnitudes, source
  astrometry with uncertainty/epoch/reference, and the full alias inventory.
  Every orbit links by exact `sn` to exactly one relation; no relation becomes
  canonical containment.
- SBX uses component-suffixed HD/HIP values as exact designations and creates
  numeric-ID claims only for purely numeric source values. This corrected 343
  invalid unsigned normalizations found by the first independent audit without
  discarding their raw identities.
- Accepted checkpoint `37ffa7255d026c8d930af6d4` accounts all 111,969 source
  rows and all fields as 71 materialized plus two explicitly excluded legacy
  coordinate strings retained in typed Parquet. It emits 4,080 binary claims,
  94 hierarchy claims, 5,169 linked orbits, 3,550 classifications, 4,498
  component magnitudes, 20,152 astrometric measurements, 2,139 citations, and
  37,530 evidence-citation links. The independent audit passes every scope,
  identity, relation, citation, uncertainty, and lineage check. Clean scratch
  reproduction matches logical hash
  `0ac0ff9babcd641446d2a4fdab0abcd7c19cc8ce7278c136e129507cb5663fc0`.

### 41) Evidence Lake E3 Survey FITS and TAP Schema Fidelity

- The first APOGEE DR17 typed cook correctly refused the source file because it
  contains three table HDUs, while the original generic FITS reader required
  exactly one. Selecting the 733,901-row allStar extension alone would have
  dropped the source's model-grid and field-version metadata and violated E1
  field preservation.
- The reusable FITS adapter now accepts an explicit HDU index, preserves scalar
  and fixed-size multidimensional columns as native Arrow types, and records
  each HDU's index, row count, field count, schema, checksum, and typed hash.
  Release-specific expected HDUs and shapes live in the source registry and
  fail on drift; no scientific winner selection occurs in this layer.
- APOGEE typed snapshot `8088671878911ad400646829` preserves 736,117 rows and
  243 table-column occurrences across allStar, model-grid metadata, and field-
  version metadata. Its three Parquet tables total 1,410,004,419 bytes and pass
  raw/typed verification plus a clean independent reproduction.
- A separate VizieR acquisition defect appeared when legal source identifiers
  contained punctuation, including `CMDCl2.5` and `_RA.icrs`. The TAP compiler
  now quotes nonregular source names, assigns deterministic regular ADQL output
  aliases, and retains the exact source/output mapping in the immutable product
  manifest. Response schemas remain strict; fields are not renamed silently or
  counted as omissions.

### 42) Evidence Lake E4 Expanded Gaia NSS Solutions

- The earlier Gaia NSS path exposed selected values for scene generation but
  did not preserve the complete fitted solution as an interrogable E4 record.
  The expanded source-native snapshot already contained 50,762 rows and all 77
  fields; the remaining gap was scientific contract materialization.
- Compiler/contract v30 extends the generic orbit materializer with a dynamic
  source model plus constant frame/reference lineage. This is reusable source
  plumbing, not a Gaia-row special case. The declarative NSS adapter keys every
  solution by exact Gaia DR3 `source_id` and `solution_id` and keeps fitted
  astrometry, Thiele-Innes elements, orbit/RV/eclipsing parameters, errors,
  correlations, observation counts, flags, and fit diagnostics coherent.
- Accepted checkpoint `e198804d34abcf04d209d116` materializes 50,762 orbital
  solutions and 101,524 release-scoped identity claims with all 77 fields
  accounted and no pending mappings. It preserves 32,808 `Orbital`, 17,440
  `AstroSpectroSB1`, 296 targeted-search, 178 validated targeted-search, and 40
  alternative solutions without manufacturing relation endpoints or canonical
  companions. Independent artifact audit and clean reproduction pass logical
  hash `cadc76e161e0042dbcb4cc7bed43e9c3fef273ede390e86b9588f8e8e5351e51`.

### 43) Evidence Lake E3 Distance and Cluster Source Fidelity

- The bounded Bailer-Jones EDR3 response set exposed a general typed-storage
  defect: VizieR legally distinguishes posterior lower and upper percentiles
  only by case (`b_rgeo` and `B_rgeo`), while DuckDB treats those identifiers
  as equal and silently suffixes one. The reusable VOTable cooker now assigns
  deterministic case-collision aliases before Parquet serialization and stores
  each exact source name in schema lineage. No catalog-specific value rewrite
  or named-object rule was introduced.
- Typed snapshot `5a60000592215924b3305095` preserves all 17,310,560 selected
  distance rows and 10 source fields in 1,407,151,086 bytes. Raw/typed
  verification and clean reproduction match content hash
  `f8e73d90cf65250e74b24b3849dc59ac9493f92ebdaf9abefdd812cd02f94aa8`.
- Hunt-Reffert typed snapshot `cbfa7c6ec8c2e3bfbc226898` preserves 7,167
  cluster rows, 1,291,929 probability-bearing membership rows, and 29,956
  literature-crossmatch rows. All 161 table-column occurrences and 1,329,052
  rows verify and clean-reproduce; E1 makes no membership or cluster winner
  selection.
- The uncertainty-envelope Gaia query remains a separate official Archive join
  and a disjoint source-row acquisition. The Archive execution path is bounded
  by the Bailer-Jones-side source ID so the join completes without treating an
  EDR3 identifier as interchangeable with a different Gaia release namespace.

### 44) Complete Gaia Envelope, SIMBAD Delta, and E4 Audit Hardening

- The disjoint Gaia uncertainty branch completed all 127 partitions with
  189,145 rows and no `MAXREC` saturation. Together with the 31,987,126-row
  hard branch, immutable raw snapshot `fcd1f77edf401a7e19c72197` and typed
  snapshot `35a41010cf74f950e61b5412` preserve 32,176,271 source-native rows in
  separate 152-field tables. The official EDR3-distance join selects scope; it
  does not merge distance estimates into Gaia facts or interchange DR2 IDs.
  A clean external-scratch recook exactly reproduces both table hashes, typed
  snapshot ID, and content hash `1e8db7b0971badce3141dac2296bfd34b7c57135f5f58e0a83bbcd81b9f16a35`;
  its temporary tree is removed only after the durable pass report is written.
- A general coverage audit found that 15 downstream Gaia products still used
  only the hard-parallax branch. AP, supplementary AP, NSS, variability,
  rotation, and official external-crossmatch products now have explicit,
  disjoint posterior-overlap companions. A checked parity contract requires
  each pair to preserve the same source, table, field selection, partition key,
  cap, and field-disposition semantics.
- The complete Gaia union changes the staged SIMBAD target from a 64-object
  pilot to 24,218 matched objects absent from the base SIMBAD slice. Target seed
  `8d940fdc1bc8eee0dc8efa7e` is checksum-pinned. The generic TAP compiler splits
  integer target IDs by the product's modulo buckets, persists every exact
  query, and independently applies the same modulo guard; no named object or
  catalog row receives production special handling. All 93 targeted queries
  complete with 24,218 basic rows, 140,962 identifiers, and 68,928 bibliography
  links. Eight-table raw snapshot `7e251164da42ef2a93627d84` and 35,321,742-row
  typed snapshot `55a9bfcaaa943ddd035df3ab` pass immutable verification and
  clean reproduction at content hash
  `d7b78dd6cb77e5ee2cd9c03771e1e7b893bb7439aa8d2489a95442c7e1182100`.
- SIMBAD E4 diagnostic v36 completed but failed independent audit because 285
  component-suffixed aliases matched the broad HIP family while failing numeric
  normalization. They remain valid full SIMBAD identifiers but are not valid
  numeric HIP claims. Compiler v37 quarantines every such failed normalization
  with source record, field, requested namespace, raw value, policy, and reason,
  while refusing to emit blank normalized identity.
- The same diagnostic reached about 65 GiB resident memory. Materialization now
  defaults to a 16-GB DuckDB limit, citation matching uses one bounded key table,
  and disposable spill can be directed to operator scratch outside the immutable
  artifact family. Two explicitly identified failed compiler temporaries were
  removed only through hash-gated retention, reclaiming 73,183,408,128 allocated
  bytes; ambiguous or immutable artifacts were preserved.
- The first complete-envelope v37 retry proved that the remaining peak was not
  the key match itself but the single expansion of every bundled astrometry
  measurement into citation links. DuckDB failed closed at 14.9 GiB used under
  the configured 16-GB cap; Photon retained over 100 GiB available and the
  temporary build was not promoted. Compiler/contract v38 processes the same
  relation in 32 exhaustive source-record hash buckets and turns off insertion-
  order preservation. Scientific identity, citation matching, and logical
  hashing are unchanged; the batching policy is recorded in every build report.
- Storage fell below the acquisition target while the closed 36.9-GB v37
  temporary and its replacement coexisted. A one-candidate, zero-age retention
  dry run proved the tree was manifestless, unreferenced, and idle; exact hash
  `0dc54bd5a607cff1da7f5315ea147a5075f6389c6cb28e29a2523215fca23204`
  authorized whole-tree removal. Raw/typed inputs, logs, reports, immutable v36,
  and the active v38 replacement were not changed.
- The bounded v38 retry completed as immutable checkpoint
  `fc5bd4e6398d72bde50ba6d5`. It materializes all 161 registered field
  occurrences, preserves 22,951,059 selected source records, and explicitly
  rejects all 285 invalid numeric HIP normalizations. Its independent artifact
  audit passes with no scope, identity, citation, or orphan failures. Clean
  reproduction matches logical hash
  `673cebbbfcc4055fb7a6a007824ba11eac75bcc7b038bb138a15abf6cf9288d7`
  with no differing sections and removes the scratch artifact.
- After that replacement was durable, the retention gate selected only the
  independently audit-failed v36 build `07230826efefffce913a3569`. Candidate
  hash `de47f05ca412b29f501f0eb1ee7e23b3be2327f7d5834de7f7114fe1f96af8f5`
  authorized whole-artifact removal and reclaimed 42,799,505,408 allocated
  bytes. Raw/typed SIMBAD snapshots, v38, and all durable reports remain.

### 45) Evidence Lake E4 Official WGSN Naming Evidence

- Compiler/contract v39 adds a source-native IAU WGSN adapter. All 597 official
  name rows and 22 typed fields become 597 exact source records and 3,847
  release-scoped identifier claims. Proper names, WGSN NEC+ records, catalog
  designations, HIP observation targets, Bayer system/component-ambiguous
  aliases, SIMBAD search spellings, and exact HR/HD/HIP/GJ designations retain
  separate namespaces and scopes.
- The adapter does not turn the naming table's coordinates or magnitude into
  astrometric/photometric authority. Cultural origin, language, constellation,
  adoption, image, coordinates, magnitude, and HTML lineage remain exact source
  context. The raw `-` designation and 28 `--` reference placeholders are
  preserved there but excluded from identifier/citation promotion.
- Required same-row citation-link accounting now counts only source records
  actually materialized after row selection. This general correction removes a
  false-failure mode for every adapter and does not require a dummy citation-
  link field disposition. WGSN materializes 91 meaningful reference texts and
  564 exact name-to-reference links.
- The scope audit proves HIP 72105 remains two claims for Izar and Pulcherrima,
  while Bayer `alpha Cen` remains three claims for Proxima Centauri, Rigil
  Kentaurus, and Toliman. These are evidence collisions, not merge or
  containment instructions. Checkpoint `0ff30b04008b93aafb3de66f` passes the
  independent artifact and scope audits and clean-reproduces logical hash
  `512b05b67ca0632bbe164b82e1b96182643e9b4e911da6b8ce9d8bdba1d37fe5`.

### 46) Evidence Lake E4 GCVS Variable-Star Evidence

- Compiler/contract v40 adds general typed sexagesimal-coordinate measurements,
  deterministic repeated-key bibliography aggregation, predicate-scoped
  composite identifiers, and lexical configured evidence that cannot be
  mistaken for a numeric measurement. All 340,839 rows in the six registered
  GCVS typed tables become exact source records.
- Checkpoint `a6f6669d2bd48eac5d6204d2` emits 705,684 release-scoped
  identifier claims, 289,892 astrometric measurements, 29,042 source spectral
  classifications, 444,566 variability observations, 21,526 citations, and
  756,305 evidence links. It aggregates 25,696 physical bibliography lines into
  21,435 source-keyed references without losing line order or provenance.
- Component identity is fail-closed: a suffixed GCVS or NSV record cannot also
  claim the base numeric key. `NSV 10360` and `NSV 10360A`, for example, remain
  separate release-scoped evidence records through a reusable predicate policy,
  not a named-object transform. All identity bindings remain unresolved.
- The source contains 1,020 B1950 NSV declinations with a positive sign column
  and an embedded negative degree token. Normalization honors the embedded sign
  while retaining both raw fields and recording `embedded_degree_sign=true`;
  no raw contradiction is erased. Variable classes remain variability evidence
  and `SpType`/`SpType2` remain stellar classification evidence.
- Independent artifact and GCVS source/scope audits pass. Clean reproduction
  matches logical hash
  `a4d78bb721d6017031a2e9a53e2b86701395d0c67ff0dd6016af639bad416967`
  with no differing sections and removes the scratch artifact.
- The retention tool now accepts only an explicit allowlist of independently
  reviewed source-audit contracts in addition to its general artifact audit.
  GCVS audits proved both provisional builds scientifically invalid; hashed
  candidate set
  `d4d63bcd3f16cea22353667725c5e1ec2bb27c6a29854f1b05c2f07ebac21ca5`
  retired those two whole artifacts and reclaimed 1,855,528,960 allocated bytes
  while preserving the accepted build, raw/typed inputs, and reports.

### 47) Evidence Lake E4 Hunt/Reffert Cluster Evidence

- Compiler/contract v41 adds general cluster-context and probability-bearing
  membership materializers. Cross-table row selection can now require both an
  exact key match and a target-table predicate, allowing member and crossmatch
  rows to follow a selected cluster boundary without a named-cluster rule.
- Source schema accounting now reconciles each exact upstream field through the
  pinned TAP query's source-to-output mapping. VizieR fields such as
  `CMDCl2.5`, `CMDCl97.5`, and `_RA.icrs` retain their exact upstream lineage
  while E4 consumes the legal typed aliases. Counts and schemas must agree; a
  positional or guessed rename cannot pass.
- Checkpoint `7e66e0690aa962c837d43a86` applies the published 16th-percentile
  distance overlap at 383.245 pc. Of 7,167 clusters, 465 qualify; all 51,017
  member rows and 451 literature crossmatches attached to those clusters are
  retained. The other 1,277,119 source rows remain accounted as boundary
  exclusions, not missing evidence.
- All 161 fields materialize into 916 coherent cluster/crossmatch contexts and
  51,017 probability-bearing membership records. The source contributes
  154,883 cluster/member/alias identity claims, one authoritative citation, and
  51,933 evidence links. Every binding remains unresolved; no membership row is
  promoted to a relation, orbit, or canonical containment edge.
- Independent artifact and cluster/scope audits pass. Clean reproduction
  matches logical hash
  `14351918254e338cd28f796b3d1837eeeed1ad094c23d0ea27d408effea8d78b`
  with no differing sections and removes the scratch artifact.

### 48) Evidence Lake E4 Extended-Object Catalog Evidence

- Compiler/contract v42 adds the active OpenNGC and constituent nebula release
  to the general extended-object domain contract. All 19,868 rows and 238 field
  occurrences are accounted: 19,012 catalog rows become extended-object
  evidence, while 856 ReadMe lines remain source-method documents.
- The adapter emits 21,107 exact OpenNGC, Messier, NGC, IC, LBN, LDN, Barnard,
  Magakian, vdB, Sharpless, Cederblad, and source-designation claims. Comma- or
  source-formatted alias lists remain raw parameter evidence for E2; E4 does not
  split them with an ad hoc parser. Cederblad's 149 component-bearing records
  claim only component designations, while 181 unsuffixed records claim the
  base designation.
- Eight catalog references link to all 19,012 object records. ReadMe rows cannot
  become objects, all bindings remain unresolved, and no extended-object row
  becomes a relation, orbit, or canonical inventory assertion.
- Checkpoint `54d1b0b6a841344c48327991` passes the independent artifact and
  extended-object scope audits. Clean reproduction matches logical hash
  `456e7a36cfd7e08ea5f7ce19c44817114de5d54d1e077ae365e2668c8191bd2d`
  with no differing sections and removes the scratch artifact.

### 49) Evidence Lake E4 MSC Evidence and Complete-Row Hashing

- Compiler v43 replaces the one-letter Parquet row alias with `source_row`.
  DuckDB identifiers are case-insensitive, so MSC's `T` periastron column had
  shadowed `to_json(t)` and made the compiler hash that scalar instead of the
  complete row. The defect collapsed 4,728 orbit inputs into 4,539 provisional
  source records. No failed artifact was promoted; a regression test now uses
  distinct rows with identical `T` values and requires distinct row hashes.
- The general relation contract now supports composite endpoints and dynamic
  source polarity, while component parameter sets support dynamic composite
  scopes. MSC therefore uses WDS plus exact component label, never a global
  `A`, `B`, or `Aa` identity. The source's `X` status is negative evidence;
  question-mark and lowercase-`c` statuses remain ambiguous; other pair rows
  remain positive evidence without becoming canonical containment.
- General numeric `zero_is_missing` semantics reject `0`, `0.0`, and signed
  zero where the source declares zero unknown. Exact source lexemes remain in
  immutable typed Parquet. Alias lists, hierarchy labels, and inconsistent
  orbit-pair punctuation are not heuristically parsed in E4.
- Accepted checkpoint `fc7e9dcabb0b27167c8f188c` accounts all 43,418 source
  rows and 73 fields one-for-one. It emits 15,748 source relation claims and
  19,366 coherent orbit records with all bindings unresolved. Generic artifact
  and MSC-specific audits pass; clean reproduction matches logical hash
  `d5fb69fba951c886b2a01d30640188f0889ecd6f8dfedab357ad90970baf4fa1`.

### 50) Evidence Lake E4 WDS Evidence and Candidate Gaia Bridge

- Compiler/contract v44 adds reusable minimum/maximum validity bounds for
  configured numeric measurements. WDS `-1` date/angle/separation sentinels,
  zero measure counts, `.` magnitudes, and position angles outside 0-359 remain
  exact in source-native Parquet but cannot become normalized evidence.
- The WDS adapter preserves all 157,299 summary rows, 177 format-document lines,
  and 24 field occurrences. It emits WDS, discoverer, Durchmusterung, and
  WDS-qualified pair claims plus relative astrometry, observation history,
  generic component photometry, source-convention proper motion, and 73,779
  opaque spectral strings. It does not split or assign the source field that may
  describe component A or two components.
- The CDS bridge preserves all 140,416 best-match rows and 19 fields. Every row
  becomes candidate positional-crossmatch evidence with its 0-2 arcsecond
  separation statistic; zero rows become strict probabilities or accepted
  identities. Copied Gaia parameters remain source-row match context and do not
  compete with release-native Gaia evidence.
- Accepted checkpoint `ad98d4e369c5a0addc6477a0` has no pending fields,
  accepted bindings, canonical containment, or WDS-derived orbit promotion.
  Generic artifact and WDS source/scope audits pass. Clean reproduction matches
  logical hash
  `7b277d9f190599a1b0cf797dabffa864b5991d956973c3ac29ff4ff3af20cba6`.

### 51) Evidence Lake E4 Gaia UCD Association Evidence

- Registry v13 corrects the source role for `J/A+A/669/A139 table4`. Its four
  published columns are Gaia identity, HMAC cluster label, BANYAN best
  hypothesis, and BANYAN probability; it contains no spectral type. The release
  therefore supports sample and association evidence, not an inferred
  classification authority.
- Compiler/contract v45 adds a general multiple-membership contract. Each
  membership family receives an explicit deterministic key, so one source row
  can retain independent methods without evidence-ID collisions. Published
  hard assignments may omit a probability; the normalized probability remains
  null rather than becoming an invented zero or one.
- Accepted checkpoint `78016b90e02689547c3f53dd` accounts all 7,630 catalog
  rows, 93 ReadMe lines, and eight typed field occurrences. It emits 7,630 Gaia
  DR3 identity claims, 6,259 HMAC assignments with null probability, and 2,840
  BANYAN memberships with source probabilities from 0.5 through 1.0. The `--`
  placeholder creates no membership.
- One parsed ADS citation links all 9,099 membership records. Every binding
  remains unresolved; no classification, generic relation, orbit, containment,
  or canonical inventory row is promoted. Generic artifact and source/scope
  audits pass, and clean reproduction matches logical hash
  `27a516ce3fbfd67062584099c9323038e9c87f4dcb81b67d3479713d6d2958a0`.

### 52) Evidence Lake E4 UltracoolSheet Evidence

- Profiling the pinned 3,890-row, 242-column sheet exposed two general defects:
  literal `nan` values could pass configured numeric predicates, and product
  lineage accepted literal `null` URLs. Compiler v49 now requires finite
  normalized numerics, supports explicit uncertainty bounds and fixed epochs,
  permits lexical modeled measurements, and applies field-specific product
  placeholder contracts.
- Two provisional builds proved the gates useful. One emitted 811 fake
  SimpleDB locators; the next exposed 17 Pan-STARRS uncertainty sentinels whose
  exact source spelling was `-999.0000`. The accepted policy rejects any
  negative uncertainty for those five fields rather than enumerating brittle
  textual spellings. Raw source values were never altered or deleted.
- Accepted checkpoint `20fdb1c95d25d441160d3bd9` accounts every source row
  and field, with 32,841 release-scoped identities, 149,636 astrometry/distance
  rows, 50,134 photometry rows over 23 bandpasses, 10,887 classifications,
  23,859 modeled/context parameters, 3,875 BANYAN memberships, and 3,079 real
  SimpleDB locators. All normalized measurements are finite.
- Direct optical/infrared types remain separate from sheet-derived numeric
  encodings and formulas. Gaia DR2/DR3 identities remain separate; list-valued
  SIMBAD aliases are not heuristically split. Endpoint-free multiplicity and
  exoplanet flags create no relation or planet evidence. Generic artifact and
  source/scope audits pass; clean reproduction matches logical hash
  `2a7cfb5f4c34df4c17cf2e6e2fa35639d1d0181b984983f7d4779407e62e1bab`.
- The fail-closed retention dry-run identified only the three independently
  failed v46-v48 artifacts. Replaying candidate-set hash
  `9db9a29f47011b94e037d8dee4e0e444e7fc9b3f2f78c403a3e9cedc26c1ea95`
  retired those whole artifacts and reclaimed 448,364,544 allocated bytes. The
  accepted v49 artifact, immutable raw/typed snapshots, and audit/reproduction
  reports remain protected.

### 53) Evidence Lake Targeted TESS Identity and Candidate Evidence

- E1 parser contract v7 adds general archive-member lineage for a typed table
  assembled from multiple source files. The targeted TESS external-crossmatch
  table now preserves its exact Hipparcos or 2MASS member path before E4 assigns
  a namespace; no identifier-format heuristic is used. Typed snapshot
  `c41373862bf6d04c13acdb78` accounts 122,772 rows and cleanly reproduces hash
  `1f2b60e6f23d31f0ac8992dfd3cc4faeeede83eae154ce3b8bc0f8007c976b06`.
- Compiler/contract v53 adds reusable multi-relation contracts, table unit
  overrides, asymmetric configured uncertainties, literal-prefix identifier
  normalization, and explicit source-row qualification. The last prevents a
  catalog column named `source_id` from colliding with compiler lineage. Exact
  lower/upper source-field names now accompany configured evidence values.
- Accepted checkpoint `11aa9bd00cc710f971b01837` accounts all 239 fields and
  preserves 27,930 target/MAST TIC records, 8,064 NASA TOIs, 29,302 official
  Gaia release-neighborhood rows, 137 external best neighbors, and 29,409 Gaia
  DR3 targets. It retains dual raw TOI forms, all TIC duplicate/split claims,
  three stellar classification families, and 131,309 asymmetric TIC
  measurements with complete field lineage.
- Lifecycle materialization keeps 1,332 confirmed/known claims positive, 5,383
  candidates as candidate evidence, and 1,346 false-positive/false-alarm claims
  negative. Every binding remains unresolved and the evidence artifact contains
  no canonical inventory tables. Generic and targeted audits pass; clean
  reproduction matches logical hash
  `5e17ca0f67e7d41a9459898ef26efc42dbd4c90f3b58e7ec4f00dd84c2a8c35a`.
- Three immutable diagnostic iterations demonstrated the gates: v50 left one
  display identifier pending, v51 exposed relation endpoint-scope mismatch, and
  v52 exposed missing uncertainty-field names. Two earlier manifestless attempts
  also remained. After v53 verification, fail-closed retention hash
  `9164bca7a24f0e9fe57d6c5930b3c9daef1f235b974e946ecc18e4320788517d`
  retired exactly those five trees and reclaimed 2,600,095,744 allocated bytes.
  Raw/typed inputs, failed-audit reports, and the accepted artifact remain.

### 54) Evidence Lake Async TAP Retry Hygiene

- A Gaia uncertainty-envelope spectroscopy acquisition exposed an operational
  defect in the general TAP collector: the one-hour client deadline submitted a
  replacement while the original ESA UWS job remained `EXECUTING`. The four
  resulting jobs were explicitly aborted; no response artifact or manifest row
  from that attempt was promoted.
- Collector lineage now retains every async attempt and its cleanup outcome,
  and a retry first aborts any nonterminal job. The 94-field spectroscopy
  supplement uses the same reviewed 17-way deterministic partition count as its
  completed hard-parallax sibling instead of three oversized partitions.
  Acquisition program `2026-07-19.e3-foundation.10` therefore produces a new
  content-addressed snapshot while completed sibling products remain reusable.
- A later transient route outage showed that attempting an abort is not enough:
  the abort request itself can fail before reaching the archive. The orphaned
  job was subsequently confirmed `ABORTED` and the operator recovery was added
  to its UWS lineage. The collector now suppresses all replacement submission
  unless nonterminal-job cleanup was positively confirmed, so an unreachable
  archive fails the bucket closed instead of creating concurrent duplicates.

### 55) Evidence Lake E4 Bailer-Jones Distance Evidence

- Compiler/contract v54 makes source-native and legal typed field names separate
  first-class lineage. This closes the general case where a storage engine must
  alias case-only source fields while field accounting and scientific evidence
  still need the publisher's exact spelling.
- Configured measurements can now carry a constant published reference and an
  explicit bound-semantic contract. The Bailer-Jones lower and upper distance
  columns are retained as 16th/84th posterior interval endpoints, not rewritten
  into symmetric uncertainty magnitudes.
- Diagnostic compile `520df722a1564ee857b1ae43` accounts all
  17,310,560 source rows and all 10 fields. It emits the same number of
  release-scoped EDR3 identity claims and coherent astrometry/distance bundles,
  containing 33,225,308 cited geometric and photogeometric measurements. Two
  copied Gaia coordinate fields are explicit exclusions from E4 evidence but
  remain byte-derived E1 typed fields; no fact is discarded.
- The build used operator-owned USB scratch for disposable DuckDB spill and
  removed that spill automatically after success, keeping `/data` bounded. Its
  logical content hash is
  `b74aabea2625f660ab85e0b723d7598a4b6cd9af6010c196d51229f743e84381`,
  and the independent generic artifact audit passes. The source-specific audit
  then found that all rows retained redundant source context: the general
  compiler selected context fields by destination and therefore included
  `exclude` dispositions whose bookkeeping destination was `source_records`.
  The first v55 correction selected only explicit `context` dispositions, but
  review caught that this would also drop intentional `lineage` context such as
  NASA release/reference fields. That attempt was stopped before promotion.
  Compiler v56 retains both context and lineage while excluding only fields
  explicitly declared `exclude`. Excluded values remain in E1 typed Parquet,
  Accepted v56 `2147d1c60f6401fdc725d96e` passes compilation and both the
  generic and source-specific audits. All source-specific checks are zero,
  including redundant context, posterior interval validity, release identity,
  source/native aliases, citations, scope, and field accounting. Its logical
  hash is
  `eceb390e97cba1b69d8a5780181b8947dfed6ed78c51167316ad4936b4506730`;
  clean reproduction matches with no differing sections and removes its USB
  scratch tree. v56 is the accepted checkpoint.
- The first generic v56 audit passed scientifically but exposed a verification-
  runtime defect by peaking at roughly 69 GiB RSS under DuckDB defaults. Generic
  and Bailer-Jones audits now default to four threads and a 16-GB memory limit,
  support operator-owned external scratch, report that execution policy, and
  remove their spill tree after close. The bounded generic rerun passes with a
  roughly 19-GB process peak instead of relying on Photon's spare memory.
- Fail-closed retention dry-run hash
  `0ed620c92b5b47ba18f4524b90383b00e8ca388de5aec4a0fbef921e55ebee5a`
  selected only failed v54 and four closed manifestless Bailer-Jones attempts.
  Exact-hash apply retired those five whole trees and reclaimed 44,452,454,400
  allocated bytes while preserving v56, all raw/typed inputs, and all reports.

### 56) Checksum-Bound External Evidence Membership

- Spectroscopic surveys are source-native evidence catalogs, not spatial
  catalogs. Loading every survey row into E4 would make a local scientific
  envelope depend on unrelated distant observations, while applying a fresh
  distance heuristic to each survey would duplicate selection policy.
- The E4 compiler now resolves external membership only through registered,
  exact raw/typed snapshots and verifies the typed-content, table, field, and
  Parquet checksums before reading. Resolved membership lineage participates in
  build identity and reproduction comparison. No contract can provide an
  arbitrary filesystem path.
- Membership groups use an explicit OR across compatible boundary tables and
  an unsigned-decimal normalization for Gaia identifiers. This lets Gaia DR3
  hard-parallax and uncertainty-supplement tables form one DR3 envelope, while
  APOGEE's explicitly EDR3 identity remains bounded by the pinned Bailer-Jones
  EDR3 table. Catalog release identifiers are not treated as interchangeable.
- Focused tests prove OR behavior, numeric normalization across storage types,
  registry and release enforcement, field validation, and content-checksum
  failure. The mechanism is general; the following survey adapters consume it
  without named-object or survey-specific production branches.

### 57) Bounded APOGEE DR17 Evidence

- APOGEE's `GAIAEDR3_SOURCE_ID` is now correctly declared as a Gaia EDR3
  namespace. Compiler/contract v58 intersects it with the exact registered
  Bailer-Jones EDR3 typed snapshot; the source, release, snapshots, content
  hash, table, field, and Parquet hash are part of build identity.
- Accepted checkpoint `efc517c3dd6f6389abab7603` retains 178,099 of 733,901
  allStar rows plus all 2,215 field-version occurrences and the model-grid row.
  It emits 3,280,268 coherent calibrated ASPCAP measurements across 27 populated
  quantities, 1,357,072 target photometry/extinction measurements, 529,676
  coordinates/RV measurements, 173,478 spectrum-product locators, and 890,495
  source/release-scoped identity claims. All bindings remain unresolved.
- All 243 table-field occurrences are accounted: 211 materialize and 32 are
  reviewed exclusions. Copied Gaia astrometry and redundant vector products
  remain byte-preserved in E1 Parquet; they are not duplicated into E4 or
  allowed to compete with release-native Gaia evidence. Fixed release citations
  link all 5,167,016 scientific measurements.
- The generic and APOGEE-specific audits pass with all checks zero. Logical hash
  `d2609ad76ea2ffc4f66d9bfd01c5fb7084aa0d88c937c513d8f416ebeced2a18`
  is the scientific A/B reference. The build required 41:53 and peaked at
  9.17 GB RSS because large branch unions repeatedly scan a wide Parquet source
  and buffer derived rows. An identical-hash selected-row cache and incremental
  insert correction is required before the larger GALAH/LAMOST adapters.
- The parallel Gaia AP spectroscopy uncertainty supplement also completed all
  17 resumable TAP buckets. The overall E3 acquisition remains open because its
  manifest still reports 12 pending products; no partial milestone is promoted
  into a false whole-program completion.

### 58) Exact Selected-Row Compiler Scaling

- A first Parquet selected-row cache failed closed: all 178,099 APOGEE rows were
  present, but three complete-row hashes changed after Parquet re-encoding. No
  artifact was promoted. The accepted implementation uses an in-process DuckDB
  temporary table, verifies every cached row against the immutable source-record
  hash set, and refuses materialization on any count or hash mismatch.
- Large configured evidence families now insert one deterministic branch at a
  time instead of buffering a multi-million-row `UNION ALL`. v60 checkpoint
  `e794324a7c7e86e80a3ea614` reproduces every v58 scientific table exactly. Its
  scientific-content hash, excluding only self-describing `evidence_build`, is
  `194eede6937b26f8c0cd508f6dd7dd0a39ef34b2a455000d1f57ee18c8a5f31b`.
  Full logical hashes still include build metadata for same-build reproduction.
- Runtime improves from 41:53 to 11:54 and peak RSS from 9.17 GB to 6.53 GB.
  Generic and APOGEE-specific audits pass. Clean reproduction completes in
  11:56 with no differing sections and removes its `/mnt/space` scratch tree.
  Fail-closed retention candidate hash
  `f5bb515adecfb310166a1cf9a89d62056795acccfbaa1c2e02ac1581823eb494`
  retired exactly three manifestless attempts from this work and reclaimed
  3,238,584,320 allocated bytes; older ambiguous trees and valid v58/v59 builds
  were not touched.

### 59) Bounded GALAH DR4 Evidence and Distance Semantics

- Compiler/contract v61 first applied checksum-bound DR3 envelope selection and
  the exact selected-row cache to GALAH DR4, retaining 117,885 of 917,588
  allStar rows. That diagnostic completed, but review of the immutable FITS
  column descriptions and official GALAH schema found a semantic error:
  `r_med`, `r_lo`, and `r_hi` are distance values used to calculate
  parallax-based gravity, not stellar-radius estimates. The artifact was not
  accepted and was not rewritten.
- Compiler/contract v62 moves the three fields to typed distance evidence with
  explicit median/lower/upper semantics. It likewise stores published
  `sb2_rv_16/50/84` as separate percentile facts instead of treating posterior
  endpoints as measurement errors. Source mass, age, bolometric correction,
  and luminosity remain a separate coherent model set from the spectroscopy
  and 31 elemental-abundance quantities.
- Accepted checkpoint `a4fc03c66ea1cfb44c25df28` accounts all 184 fields as
  169 materialized and 15 copied-Gaia/2MASS/AllWISE exclusions. It emits
  353,655 release-scoped identity claims, 4,052,282 stellar/model measurements,
  857,173 astrometry/distance/RV measurements, 973,436 extinction/interstellar
  measurements, and 623,253 hydrogen/lithium activity/line measurements. All
  object bindings remain unresolved for E5, and extreme published E(B-V)
  values remain evidence rather than being silently clamped.
- Generic and GALAH-specific audits pass with all checks zero. The 6:21 compile
  peaks at 4.90 GB RSS; clean reproduction completes in 6:25, matches logical
  hash `7c0a367810903b18dad7e408d3feade5821325bfa8a670b5e051e1534cded8db`,
  reports no differing sections, and removes external scratch. The source
  allStar product contains no spectrum URL field, so an authoritative product
  index remains an explicit E3 follow-up rather than a derived URL convention.

### 60) Bounded LAMOST DR11 Observation Evidence

- Compiler/contract v63 applies the registered Gaia DR3 hard/uncertainty
  envelope to each LAMOST DR11 v2.0 stellar release independently. Accepted
  checkpoint `a583819f0a4f3896c312f19e` retains 661,941 LRS stellar, 496,415
  LRS M-star, and 500,925 MRS stellar observations from 11,418,142 source rows,
  with no exact duplicate selected rows.
- All 185 field occurrences are accounted as 170 materialized and 15 reviewed
  copied-catalog exclusions. LRS LASP physics, M-star TiO/CaH/Na/zeta and
  H-alpha/magnetic activity, and MRS LASP/CNN physics plus raw and corrected
  B/R/combined/LASP radial velocities remain separate coherent evidence
  contexts. Source object class and spectral subtype are not collapsed.
- Official `obsid`/`mobsid` values index spectra for on-demand archive retrieval
  without manufacturing URLs. The source-declared Gaia DR3 identifier and
  conditional Gaia/Pan-STARRS/LAMOST `uid` scope remain release-specific;
  object bindings stay unresolved for E5.
- Generic and LAMOST-specific audits pass. Clean reproduction takes 28:55,
  matches logical hash
  `eeb6dd86c096100175dc92d829508c8c36636d20f507993750e1f9a0b5a73d37`
  with no differing sections, and removes its external scratch. The accepted
  compile uses a 12-GB DuckDB limit and peaks at 16.70 GB process RSS; the
  bounded source audit uses two threads/12 GB and peaks at 15.11 GB.
- Before this build, exact hash-gated retention candidate
  `ce4b84fa18cb9cef35b8adfdf102e850c8c37d2da9135128a1a5a182e65879ba`
  retired ten old manifestless, unreferenced, idle E4 staging trees and
  reclaimed 71,672,885,248 allocated bytes. After v62 acceptance, candidate
  `9f93b59b7ab0ddde233063585b0ee19c4ad2a248a2cfdd08fb20ff810a74da4a`
  retired only rejected GALAH v61 and reclaimed 5,170,827,264 allocated bytes;
  all accepted builds and source snapshots remain protected.
- A subsequent E0 audit exposed 621 GiB of legacy-name build output that the
  standard timestamp contract correctly ignored. Retention now requires both
  explicit `--include-legacy-builds` and an exact reviewed candidate hash for
  that ambiguous name class. Candidate
  `e32226b51121daf22850650296cfae330606010998a858ae30f6617c8eced540`
  removed only 18 superseded build trees (364.82 GiB), retained the newest 12,
  all 11 E0-referenced/served builds, every historical report, and every source
  or E4 artifact. The refreshed audit reports 495.6 GiB free and restores the
  acquisition gate.

### 61) Gaia Uncertainty Target Seed and E3 Acquisition Completion

- Repeating the full Gaia DR3, EDR3-distance, and source-table join for every
  uncertainty-envelope supplement did not scale reliably. Oversized activity
  plans hit Gaia VMEM; spectroscopic plans remained nonterminal under 3-, 7-,
  and 31-way partitioning. All remote jobs from stopped attempts were explicitly
  aborted, and no partial response set was promoted.
- The durable solution is general and evidence-bound rather than source-object
  specific. `scripts/build_gaia_uncertainty_target_seed.py` verifies the
  accepted `gaia_dr3_source_uncertain_distance_supplement_v1` Parquet checksum,
  requires one distinct target per source row, sorts all 189,145 Gaia DR3 IDs,
  and publishes immutable seed `638c3ff4e58abcd355029e0f`. Its target artifact,
  manifest, values hash, and build identity are inputs to every dependent query.
- Nine remaining AP-supplement, NSS, variability/rotation, and external-
  crossmatch products query their source tables directly through 31 modulo
  target buckets. Every product manifest proves the same seed build, exact
  uncertainty-envelope coverage, all 189,145 values, 31 nonempty buckets, and
  its own response-set hash. Scientific membership is unchanged; only archive
  query execution differs.
- The final E3 report passes 56/56 products, 170,253,376 rows,
  23,970,068,085 response bytes, and zero pending products. The five expanded
  Gaia raw snapshots type to 30 Parquet tables, 83,908,762 rows, 1,320 column
  occurrences, and 6,575,792,259 bytes. Aggregate verification and independent
  clean-state reproduction pass for every release. E3 acquisition is complete;
  E4 normalization, evidence materialization, scope, and selection remain open.
- The first NSS E4 audit exposed two reusable contract defects. Its hard branch
  had filtered on the fitted NSS parallax rather than authoritative
  `gaia_source.parallax`, omitting 34,962 rows and overlapping 330 uncertainty
  keys. After reacquisition, the branches contain 85,724 and 1,351 disjoint
  rows. Gaia also reuses `solution_id` across distinct per-source model rows;
  `(source_id, solution_id, nss_solution_type)` is the verified source-native
  solution key. Compiler-v65 checkpoint `1881e02d8e9f1d33a1d9b64a` accounts all 87,075
  orbits and 154 table-field occurrences with zero collisions or pending fields;
  source-specific, generic artifact, and clean-reproduction audits pass at
  logical hash
  `3aeabe350ec4e224ab9b04dceae6fab9678cdd27a5337919ed6c1c8912f51e5a`.
- Preparing the Gaia AP adapters exposed a general uncertainty-shape hazard:
  the scoped stellar-parameter compiler had interpreted every lower/upper field
  as an error magnitude. Compiler/contract v65 makes that representation
  explicit. Error columns remain absolute magnitudes, while published posterior
  lower/upper columns remain absolute interval endpoints with a required named
  bound semantic. Focused tests preserve both forms; no catalog-specific or
  named-object branch was introduced.

### 62) Official Gaia External Best-Neighbour Evidence

- E4 compiler/contract v66 preserves 24,045,693 bounded Gaia DR3 best-neighbour
  rows across AllWISE, 2MASS, Hipparcos-2, Tycho-2, and RAVE DR6. Each row emits
  separate release-scoped endpoint identifiers and a candidate relation with
  the published angular separation and match-quality context. No row is treated
  as an accepted identity merge.
- The first complete compile exposed a general scale defect: unresolved binding
  scopes were deduplicated by one release-wide `UNION DISTINCT`. The compiler
  now emits scope families separately and relies on deterministic primary keys
  with `INSERT OR IGNORE`, preserving exact output and idempotence without the
  global aggregation. A regression test covers multiple aliases sharing one
  binding scope.
- Exact primary-key maintenance for 72,137,079 binding outcomes still exceeded
  the former 16-GB cap. Photon materialization now defaults to 32 GB, one thread,
  and disabled insertion-order preservation; hashing retains its independent
  16-GB cap. Both failed manifestless staging trees were retired whole through
  reviewed exact-hash retention, reclaiming 30,327,984,128 and 30,865,903,616
  allocated bytes.
- Accepted checkpoint `81b0cc4aa29453088a62f3de` has zero pending fields or
  normalization rejections. Source-specific and generic artifact audits pass.
  Clean reproduction used `/mnt/space` for its disposable 47-GB scratch build,
  matched logical hash
  `2cd08ee00ab39b699627eb2614392a7e0c4f241fe9214a476762c6cab15d87a0`,
  and removed the scratch tree automatically.

### 63) Coherent Classifier Vectors and Domain Interval Semantics

- Preparing Gaia AP exposed two general representation gaps. Expanding every
  classifier probability into an independent row would produce hundreds of
  millions of rows and erase the fact that probabilities belong to distinct
  source models. Compiler/contract v67 instead emits one source-classification
  bundle containing named model vectors and performs no cross-model selection.
- Configured astrometry/variability evidence had also treated every lower/upper
  field as a nonnegative error magnitude. The shared contract now explicitly
  distinguishes error magnitudes from absolute interval endpoints. Independent
  audit permits signed endpoints only for the latter representation, rejects
  reversed intervals, and reports source-native central estimates outside their
  published intervals without changing them. Focused tests cover both changes;
  no Gaia-specific selection branch exists.

### 64) Gaia DR3 Main Astrophysical-Parameter Contract

- Compiler/contract v68 maps all 482 field occurrences in the ten hard-envelope
  and uncertainty-supplement AP tables. Source models remain separate: DSC and
  ESP-ELS classifier vectors, GSP-Phot, photometric FLAME, GSP-Spec atmosphere
  and abundances, GSP-Spec CN/DIB features, ESP-ELS/HS/CS/UCD, MSC system and
  component fits, and OA neuron assignments are never field-wise composited.
- MSC primary and secondary fitted parameters use explicit unresolved component
  scopes. OA neuron assignment is an unsupervised source classification, not a
  physical spectral type. Gaia source-model distances do not replace source
  astrometry, and no AP estimate is selected as a public winner at E4.
- Exact typed-schema reconciliation finds no missing configured field and no
  unassigned source field. Comparing configured units to source VOTable metadata
  corrected DIB equivalent width/profile units from nm to Angstrom and ESP-CS
  activity index from dimensionless to nm before materialization. Contract
  validation, 65 compiler tests, and real-row smoke materialization pass; the
  immutable full-release build remains pending.
- All five hard AP products use the same single `solution_id` value. It is a
  processing-solution/release marker, not an independently bindable object ID.
  The corrected contract retains it as source-record lineage, keys each table by
  Gaia `source_id`, and collapses record and identifier binding to one star scope.
  This removes 51,164,425 false duplicate identity claims and the corresponding
  redundant binding outcomes before the large build.
- The first full v68 attempt completed scientific evidence materialization but
  hit the 32-GB cap while inserting every release binding at the end. This was
  an operation-order scaling defect, not invalid AP data. Compiler/contract v69
  emits the identical deterministic binding keys after each source table, which
  bounds working state and retains idempotent primary-key deduplication. Exact
  candidate hash
  `fae1354fe191776ed93d66967563075b762d0af37d1df518e76670f9f93eccb7`
  authorized retirement of the sole 110,793,879,552-byte manifestless staging
  tree before retry.
- The first v69 retry was terminated with its terminal before report publication
  or atomic promotion. Exact candidate hash
  `0732040b55edb45e63974719f2e1b932e0a56b9d388ca93f86403e0223e41191`
  authorized retirement of its sole closed, unreferenced 128,085,159,936-byte
  staging tree. Subsequent large compiles run in a persistent tmux window with
  an external log and exit-code sentinel.
- The tmux-isolated retry proved the per-table binding fix, then exposed a
  separate execution defect: ordinary evidence-reference joins remained
  release-wide even though nested astrometry-bundle citations were already
  divided into 32 deterministic source-record hash buckets. The join failed
  closed at the 32-GB cap without promotion. Compiler/contract v70 applies the
  same bucket policy to every reference-bearing evidence table; 66 focused
  tests preserve exact link keys and counts. Exact candidate hash
  `a546c331f63e4c09e9a7b8afdeb9a46058a5d3261acde897f36449be851aa79e`
  authorized retirement of the sole 128,089,096,192-byte failed staging tree.
- Accepted compiler/contract v70 checkpoint `393b08fa1268bbd42bb40225`
  preserves all 51,164,425 source rows, all 482 field occurrences, and
  134,743,089 citation links with zero pending mappings, duplicate rows,
  exclusions, or identifier-normalization rejections. Source-specific and
  generic artifact audits pass. Clean reproduction on USB scratch matches
  logical hash
  `b84be6a482e90bd4527f498f87f4381f1439b0e67a7ec5762c19530976ec6596`
  and records `scratch_removed=true`.
- The first source audit revealed 271,975 non-bracketing but non-reversed source
  intervals. Direct queries of the pinned typed Parquet reproduce exactly
  271,968 hard and four supplement FLAME luminosity cases plus three hard
  GSP-Spec Mg/Fe cases. Most FLAME rows expose the source's `0.01 L_sun` lower
  floor with `flags_flame=20` while retaining a lower central luminosity. The
  audit now reports and pins source-native non-bracketing intervals while still
  failing reversed endpoints; no value or uncertainty is rewritten.

### 65) Gaia DR3 Supplementary Parameter Alternatives

- Contract v71 maps all 354 field occurrences across the hard-envelope and
  disjoint uncertainty-envelope supplementary AP tables. The compiler retains
  MARCS, PHOENIX, OB, and A as separate GSP-Phot posterior parameter sets,
  preserving model-specific temperature, gravity, metallicity, radius,
  distance, extinction, magnitude, posterior, and sampler evidence. Gaia's
  published best-library field remains selection lineage rather than an E4
  winner instruction.
- The spectroscopic table retains the GSP-Spec ANN atmospheric solution and
  spectroscopic FLAME radius, luminosity, mass, age, redshift, evolution-stage,
  and bolometric-correction solution separately. `solution_id` is again the
  release-wide processing marker and remains record lineage rather than object
  identity. The uncertainty supplement inherits the same scientific contract.
- Contract validation, 67 compiler tests, and a representative materialization
  of all four tables pass with zero pending fields, identifier rejections,
  orphan parameter rows, or reversed intervals. Direct typed-source inspection
  pins two hard-envelope FLAME luminosity intervals whose central estimate lies
  outside the published endpoints; they remain source-native reported anomalies.
- Full build `c4a6b5fd297f8ef9cceb6340` atomically promoted a 26,810,527,744-byte
  indexless DuckDB artifact. It accounts all 8,019,372 source records and 354
  field occurrences, 52,352,445 stellar measurements, 66,558,671 photometry
  measurements, 10,942,232 astrometric/distance measurements, 905,314
  classifications, 12,904,333 coherent parameter sets, and 53,257,759 citation
  links. Source-specific and independent artifact verification pass with zero
  discrepancies or duplicate keys. Clean scratch reproduction matches logical
  hash `a74eb79475a76af75d7a626adb56baf89de3f6978904e7c83e4619f46bf6e052`
  with no differing report sections and removes its `/mnt/space` scratch tree.

### 66) Immutable Evidence Key Audits Replace Runtime Indexes

- Table-level storage inspection of accepted main-AP checkpoint
  `393b08fa1268bbd42bb40225` found roughly 58 GiB of allocated table blocks in a
  167-GiB DuckDB file. The remainder is dominated by automatically retained ART
  indexes from primary-key and unique constraints on tens of millions of
  immutable SHA-keyed rows. Carrying that amplification into the Gaia source
  backbone and E6 shadow build would exceed Photon's practical retention budget.
- Compiler/contract v71/v72 keeps `NOT NULL` schema shape constraints but does
  not retain mutable-runtime uniqueness indexes. Unresolved binding
  deduplication is now explicit. Before hashing or atomic promotion, an exact
  key audit checks every table's deterministic ID/composite key and the
  `(source, release, table, row hash)` source-record natural key. Any duplicate
  fails the build; the independent artifact verifier repeats the audit.
- A representative supplementary-AP same-row A/B preserved every logical table
  hash and reported no duplicate key while reducing the database from 8,925,184
  to 5,255,168 bytes (41.1%). The change is execution/storage-only and does not
  weaken promoted-artifact integrity.
- The first supplementary run was exposed in the attached tmux client and was
  interrupted before promotion. Exact candidate hash
  `444bb02ec5702d9c84db491bfcd4a47338516f634c5317cc569cd2d529934675`
  authorized retirement of its sole closed 1,129,861,120-byte staging tree. A
  detached retry was intentionally stopped after the storage amplification was
  confirmed; exact candidate hash
  `4bc6ae94b04dd1346e62fc32579d90c3f35c4030081f805f4c39b028974d134f`
  authorized retirement of its sole closed 1,131,433,984-byte staging tree. No
  raw, typed, accepted, served, rollback, or report artifact was selected.

### 67) Registered E4 Source-Scope Ledger

- `config/evidence_lake/e4_source_scope.json` and
  `scripts/audit_e4_source_scope.py` make every registry entry explicit at the
  scientific-evidence boundary. The first audit accounts all 44 sources: 30
  active E4 adapters and 14 explicit boundary dispositions, with no stale,
  conflicting, unregistered, or unaccounted entry.
- Gaia DR2/DR3 neighbourhood products remain E2 release-identity evidence;
  disabled lossy projections and transitional AT-HYG remain E6 stability or
  identity references rather than scientific authorities; and source-native
  Cantat-Gaudin DR2 cluster evidence is retained while Hunt/Reffert supplies the
  active current E4 cluster authority.
- At that checkpoint the audit reported `in_progress` rather than a false pass
  while five adapter requirements remained. After the Gaia variability adapter,
  scope version 2 accounts 31 adapters and 13 explicit boundary dispositions;
  the four remaining blockers are Gaia source, VSX, natural JPL Horizons, and
  separately scoped artificial-object Horizons trajectories.

### 68) VSX Identity and Bibliography Boundary Audit

- `scripts/audit_vsx_typed_source.py` audits the exact pinned raw/typed release
  selected by the Evidence Lake registry. It accounts all 10,304,568 VSX rows,
  23 object columns plus two ReadMe columns, unique non-null OIDs, non-null
  names, valid J2000 coordinates, published status/limit/uncertainty flag
  domains, and positive periods.
- The two duplicated normalized public names remain visible collision evidence:
  `EROS2-SMC-RCB-2` and `V7646 Sgr` each identify two distinct OIDs. The E4
  adapter must therefore use `vsx_oid` as source identity and retain the public
  name only as a source-scoped designation claim.
- The audit status is deliberately `incomplete`, not `pass`. The current pinned
  acquisition contains `vsx.dat` and `ReadMe` but not the documented
  `refs.dat` OID-to-bibcode relation. That bibliography must be pinned, typed,
  and exactly linked before the VSX adapter can satisfy the naming and
  bibliography source role; no catalog-wide reference will be fabricated.
- The next acquisition now includes the official CDS `refs.dat.gz`. Direct
  inspection finds 830,415 unique OID/bibcode pairs for 586,530 OIDs, no empty
  or malformed rows, no duplicated pairs, and a maximum OID of 683,950. The
  server reports a 2022 modification date, so this remains a labeled historical
  partial bibliography alongside the 2026 rolling object table.
- The expanded audit passes a schema-driven scratch typing of that exact file:
  all 830,415 rows and 586,530 OIDs remain unique and valid, with at most 12
  references per OID. It reports rather than hides 2,072 historical OID links
  absent from the current object table and 54 source reference strings that are
  not canonical 19-character ADS bibcodes. These become explicit missing
  bindings and raw citation text, not fabricated object links or failed rows.
- The July 21 three-artifact preview creates raw snapshot
  `64f0562ef64643076d77a153` and typed snapshot
  `c5446b6ab730ffe763af12f4`. It passes complete raw/typed accounting, the VSX
  source audit, and clean reproduction with scratch removal. Current object
  coverage is 10,304,607 rows; the unchanged 830,415-row bibliography now has
  2,080 OIDs absent from that rolling inventory.
- `scripts/report_typed_table_delta.py` adds a reusable release comparison keyed
  by stable source identity. It validates key uniqueness and schema, bounds
  identity samples, and separates scientific field changes from lineage-only
  changes. For VSX it reports 47 additions, 8 removals, and 243 scientifically
  revised retained OIDs without misreporting 9.67 million shifted source line
  numbers as scientific changes.

### 69) Immutable Horizons Response Acquisition

- The former Sol collectors retained only a parsed CSV and query URL. Exact JPL
  Horizons response bodies were discarded, while reviewed operator target
  metadata and parsed JPL values were merged in one row. That prevented clean
  raw-to-typed reproduction and obscured field-level provenance.
- `scripts/horizons_snapshot.py` now gives both bounded collectors one atomic,
  content-addressed contract. Every target preserves the exact response bytes,
  query parameters and URL, checksum and size, retrieval time, reviewed target
  seed, collector checksum, response index, and parsed projection. The legacy
  CSV is refreshed atomically from the immutable snapshot for compatibility.
- An isolated artificial-source run at epoch 2026-07-21 captured 11 targets in
  snapshot `a7aae9a4aa05c3f3fcaf3274`. Evidence Lake raw snapshot
  `e65b57c609708b377045e9ae` accounts the parsed table plus 25 response/query/
  seed metadata files; typed snapshot `122af4b07d09f9c9d81f6a28` contains two
  11-row tables and passes the raw/typed verifier with zero response checksum or
  size mismatches. Photon registry and active raw state were not changed.
- `scripts/audit_jpl_horizons_typed_source.py` adds the source-specific E4 gate.
  It requires a one-to-one binding between each parsed projection and response
  index row, verifies response paths remain inside the immutable raw artifact,
  recomputes every response checksum and byte count, and checks query, target,
  center, retrieval, and operator-seed lineage. The scratch artificial snapshot
  passes all checks. Negative semimajor axes remain valid evidence for
  hyperbolic escape trajectories rather than being rejected as invalid orbits.
- Current acquisition produced natural snapshot `9e5d0b21cf6e9ad0685a8c1f`
  at the stable 2016-01-01 epoch and artificial snapshot
  `80d4cd347293efc5b2d7438b` at 2026-07-21. Both preserve every exact response
  and the same 60/11 target identities as their predecessors.
- `scripts/report_horizons_snapshot_delta.py` separates scientific changes from
  retrieval/query/hash lineage. At the unchanged natural epoch, only Neptune
  and Triton have revised orbital values from current JPL solutions. All 11
  artificial trajectories change under the newer epoch as expected. Both
  reports pass with no added, removed, blank, or duplicate source keys.
- The collectors now derive `center_target_command` from the general Horizons
  center-expression grammar while retaining the exact `center_code`. Natural
  snapshot `6ae83d9fce64f13783f05e59` and artificial snapshot
  `17fd89afbd89e4b2303b832f` preserve the same identities and values as their
  immediate predecessors. This gives E4 a provenance-bearing parent target
  identifier and avoids treating operator-written parent names as source
  relation evidence.
- Pre-cutover review found that the natural-source audit fixture called its
  response artifact `sol_system_horizons_responses`, while the production
  collector has always emitted `sol_authority_horizons_responses`. The audit
  and fixture now use the production manifest name, preventing a scratch-only
  naming convention from passing a gate the registered artifact could not.
- The resulting production-shaped E1 preview then failed the natural audit
  because the fixture had also required artificial-only `target_body_name`
  while omitting natural-only `object_class_aliases_json`. Required fields are
  now split into common, natural, and artificial contracts; preview reruns must
  pass both independently before registry cutover. The corrected preview does:
  both sources account two raw artifacts, type two complete tables, pass their
  source audits at 60/60 and 11/11 projection/response rows, reproduce exact
  typed IDs and content hashes, and remove the clean scratch tree.

### 70) Gaia DR3 Backbone Envelope Audit

- `scripts/audit_gaia_source_typed_source.py` independently audits the complete
  source-native Gaia DR3 backbone before E4 materialization. The accepted typed
  snapshot has identical 152-field schemas in its 31,987,126-row hard-parallax
  branch and 189,145-row uncertainty supplement, one unique positive Gaia DR3
  source ID per row, the expected J2016.0 epoch and solution ID, valid
  coordinates/correlations/probabilities/errors, and no identity overlap or
  parallax-boundary violation between branches.
- The source contains substantial public-feature evidence that the E4 adapter
  must retain: 2,929,216 radial-velocity rows, 5,778,039 XP-continuous product
  indexes, 548,038 epoch-photometry indexes, 206,781 RVS product indexes, and
  6,955,056 rows with the `gaia_source` GSP-Phot projection. The projected AP
  columns must be accounted against the richer AP source without silently
  becoming an independent competing parameter set.
- Gaia `*_over_error` fields are signed signal-to-noise ratios, not uncertainty
  magnitudes. Their negative values are valid source evidence and are excluded
  from the audit's nonnegative-error rule.
- The Gaia source audit now emits an exhaustive 152-field adapter ledger: 65
  astrometric, 24 photometric, 22 radial-velocity, 8 classification/membership,
  6 observation-product, 23 redundant AP-projection, 3 identity, and 1
  compiler-index fields, with no unclassified tail. The redundant GSP-Phot
  projection remains in E1 but must not compete with the richer AP source in E4.

### 71) Reproducible Gaia Variability Vector Audit

- The earlier Gaia variability source report lacked a checked-in generator.
  `scripts/audit_gaia_variability_typed_source.py` now reproduces its full
  592,197-row, 268-field-occurrence audit across the hard and uncertainty
  branches and verifies row counts, source identities, release solution,
  schema parity, periods, errors, and false-alarm probabilities.
- All 52 rotation-vector fields are parsed with source-native whitespace and
  newline handling. Their lengths match `num_segments` or `num_outliers` with
  zero malformed tokens. The report distinguishes 99 wholly absent vectors
  from 2,533,499 valid `--` masked elements; E4 will translate only those mask
  positions to typed nulls while E1 retains every exact source string.
- The representation remains one coherent variability-summary parameter set
  per source and one coherent rotation-modulation parameter set per source.
  Independent scalar expansion would lose vector covariance and materially
  amplify the evidence artifact.
- The generated report now partitions every source column exactly once as
  identity, membership flag, cardinality, scalar solution field, or masked
  vector. This makes field disposition inspectable before the E4 adapter and
  prevents silent omission during parameter-set materialization.
- Compiler/contract v72/v73 adds a reusable coherent-parameter schema contract.
  Ordered value arrays avoid repeating field names per source row; each schema
  retains field order, source name, datatype, unit, UCD, description, and any
  mask/cardinality transform. Generic verification fails on missing schemas or
  value/schema arity mismatches.
- Gaia variability build `9e934a3823f3cbcd879b3359` materializes 592,197
  coherent parameter sets, four schemas, 592,197 Gaia DR3 claims, and 592,197
  citation links with all 268 field occurrences materialized. Rotation vectors
  retain whole-vector absence separately from positional source masks. Source
  and artifact audits pass, and clean reproduction matches logical hash
  `d98283bb5477211963902e072b4aaf7095740435efeff567950dbcfe934dea2b`
  with no differing sections and removes its scratch tree.

### 72) VSX Three-Artifact E4 Cutover

- The checked-in registry now selects `rolling_snapshot_20260721` and accounts
  the official object table, ReadMe, and historical OID/reference relation.
  E4 uses VSX OID as release-scoped source identity, preserves the public name
  separately, and emits a Gaia DR3 claim only for the explicit `Gaia DR3 N`
  designation form. No public-name collision can merge source records.
- Compiler/contract v73/v74 materializes every one of the 11,135,737 source
  records and all 29 field occurrences. The object table yields 20,609,214
  J2000 coordinate facts, 5,152,350 spectral classifications, and 10,304,607
  ordered 16-field variability parameter sets without repeating field names in
  every row. It retains 29,456,421 source-scoped identifier claims and explicit
  unresolved outcomes rather than promoting variable-star inventory.
- Bibliography materialization creates 12,371 source entries plus one catalog
  citation and links all 830,415 exact OID/reference pairs. The current rolling
  inventory lacks 1,833 historical OIDs represented by 2,080 links; those links
  remain inspectable unresolved evidence. Structural ADS validation identifies
  56 links across 9 distinct noncanonical strings, preserves their exact text,
  and does not invent an ADS URL.
- Immutable build `d9780b76333132c0a05098b7` is 10,434,392,064 bytes and
  passes source-specific and generic artifact audits with no pending fields,
  normalization rejection, duplicate key, orphan, citation, scope, schema, or
  arity defect. Its logical hash is
  `1aa9577c875d2efcd6f11f59428c61f5197e184986ebd3e6ee2d372bb8891e36`.
  Clean reproduction matches it with no differing report sections and removes
  its USB scratch tree.
  Source-scope version 3 now accounts 32 adapters and 12 explicit boundary
  dispositions; the remaining blockers are Gaia source and the two separately
  scoped JPL Horizons sources.

### 73) JPL Horizons Exact-Response E4 Cutover

- Registry v16 selects the current natural and artificial snapshots with both
  parsed-object and exact-response-index artifacts. Scientific-evidence
  contract v75 uses separate adapters and release scopes while sharing typed
  relation, orbit, physical-parameter, and product-lineage contracts.
- The identity boundary is explicit. `source_pk` and `object_name` are reviewed
  operator seed claims; `horizons_command` and `center_target_command` are JPL
  claims. Only the JPL namespace may identify orbit/trajectory endpoints, so an
  operator-written parent name cannot manufacture a source relation.
- Immutable build `236a7b7822c52fef8b903d58` accounts all 142 source records
  and 67 fields, materializing 284 identity claims, 71 exact raw-response
  products, 71 linked orbital solutions, 71 center relations, 36 coherent
  physical parameter sets, 73 citations, and 178 evidence/citation links. The
  two excluded fields remain source-native and explicitly accounted.
- Generic and JPL-specific audits pass with exact response path/checksum/byte
  integrity, valid hyperbolic semantics, explicit km/kg/d/au/deg units, zero
  namespace leakage, and zero canonical promotion. Clean reproduction matches
  logical hash
  `c81a10d4f97f6dd99be09852b3b68a1f33dca852828ff18132a6e9d3362ca1bb`.
  Source-scope version 4 now accounts 34 adapters and 10 explicit boundary
  dispositions; Gaia source is the only remaining E4 adapter blocker.
- The first v76 combined compile failed before promotion because a configured
  product quality field was not table-qualified. Compiler v74 qualifies those
  fields generally. After the accepted build reproduced, fail-closed retention
  dry-run/apply hash
  `989a230ebb4219d6decb901f16ac155d6f5051454d6b6f80f35e59c228c6b573`
  retired only the 1,851,392-byte manifestless staging tree.

### 74) Gaia DR3 Source Evidence Cutover

- Scientific-evidence compiler/contract v75/v76 adds the general
  `stellar_source_parameter_sets` destination for compact, coherent survey
  source solutions. It stores one ordered typed value vector per source row and
  one reusable schema per release table instead of expanding 32 million rows
  into independent scalar facts. Schema fields retain scientific-domain
  annotations so downstream policy can select astrometry, photometry, radial
  velocity, classification/membership, and observation-product availability
  without losing source-solution context.
- Immutable build `ab7f7e6bc211bee146885987` accounts the complete buffered
  Gaia DR3 source release: 31,987,126 hard-envelope rows and 189,145 disjoint
  uncertainty-supplement rows. It materializes 32,176,271 release-scoped Gaia
  identities, unresolved star-scope outcomes, coherent source solutions, and
  citation links, with two 125-field schemas and all 304 field occurrences
  accounted as 254 materialized plus 50 reviewed exclusions.
- Copied GSP-Phot fields stay lossless in E1 but do not become a redundant E4
  model set because the richer Gaia AP release owns that evidence. The source
  audit independently verifies 2,929,216 radial-velocity rows, 5,778,039
  XP-continuous indexes, 548,038 epoch-photometry indexes, and 206,781 RVS
  indexes. Generic and source-specific audits pass logical hash
  `1863f8da12380f845983339213a28ee7c4a0af5313bc9fee586f05e1a435a962`;
  clean reproduction matches with no differing sections and removes its scratch
  tree.
- At that checkpoint source-scope version 5 accounted 35 scientific adapters
  and nine explicit E2/E3/E6 boundary dispositions across 44 registered
  releases. The exoplanet-lifecycle catalogs remained a separate E4 acquisition
  and reconciliation obligation rather than being silently treated as covered
  by the old lossy cooker; sections 75-76 record its later completion.

### 75) Exoplanet Lifecycle Source Recovery

- The lifecycle supplement registers Exoplanet.eu, OEC, and HWC as three
  independent release-scoped sources instead of feeding their heterogeneous
  records through the legacy merged overlay cooker. Each source now has an
  immutable raw snapshot, a source-native typed snapshot, exact field/row
  accounting, and a clean raw-to-typed reproduction report.
- OEC exposed a reusable identity defect during compiler review: local XML node
  paths repeat across archive members. The first diagnostic grouped those paths
  alone and collapsed thousands of distinct objects into 28 parameter sets.
  The accepted parser and E4 adapter use archive member plus local node path for
  every object, parameter, identity, and relation endpoint. A regression test
  requires identical local paths in different members to remain distinct.
- Compiler/contract v77/v78 adds an exhaustive source-native parameter routing
  ledger and normalizes lifecycle polarity without selecting a canonical
  winner. OEC contributes 5,287 confirmed, 3,844 candidate, 100 controversial,
  12 retracted negative, and 10 other ambiguous lifecycle assertions, plus
  9,252 planet parameter sets, 7,182 stellar parameter sets, 219 binary orbits,
  16,750 relations, and 127 product links. Every one of its 160,582 parameter
  rows has a typed destination or reviewed disposition.
- Exoplanet.eu contributes 8,261 positive confirmed assertions. HWC contributes
  5,599 habitability feature rows and deliberately contributes no lifecycle
  evidence; its ranking cannot confirm a planet. Final E4 builds
  `0a4d68cf938de29a229946a5`, `c2bfe4c2ea04107e81e0de20`, and
  `e94a2f86a3410bdf371ef9ef` pass source-specific, generic artifact, and clean
  reproduction gates without promoting inventory or changing canonical counts.

### 76) McGill Bibliography Completion and E4 Scope Closure

- McGill release `snapshot_20260721_with_bibliography` adds the exact publisher
  HTML, reference URLs, CDS ReadMe, and 215-row CDS reference table to the
  existing 31-row catalog. The dedicated bundle cook retains data and section
  rows, repeated object-level resource links, exact link text/URL/kind/bibcode,
  and a one-to-one index of 97 unique external reference codes.
- E4 build `99c17afd7461a9a6972a9348` retains the existing 139 coherent magnetar
  parameter contexts, links 208 current-object bibliography claims, and
  materializes all 215 CDS references. Four historical shorthand codes
  (`cdt+82`, `cwd+97`, `fmc+99`, `wkv+99b`) remain explicitly unresolved. The
  compiler does not infer an ADS record or manufacture a URL. Source, artifact,
  and clean-reproduction audits pass.
- Registry version 18 now accounts 47 sources, 148 active manifest entries,
  four retained superseded artifacts, and 6,209 machine-enumerated fields at
  baseline fingerprint
  `153280e2e3331e06541da100205f36c589a641d9b1ff0b8578a14246dcaa03b6`.
  Source-scope version 7 accounts 38 scientific adapters and nine explicit
  E2/E3/E6 boundary dispositions with no blocker, stale disposition, conflict,
  or unregistered adapter. This closes adapter coverage; it does not promote
  evidence or authorize public selection before E5/E6.

### 77) Immutable E4 Scientific Evidence Release Set

- A storage audit found 449,199,915,008 bytes across the 36 accepted source
  artifacts. Recompiling those already immutable domain tables into one
  monolithic database would consume the remaining `/data` safety margin and
  duplicate evidence without adding scientific information.
- `config/evidence_lake/e4_accepted_artifacts.json` therefore names the accepted
  artifact for every one of the 38 E4 adapters. The release-set compiler fails
  unless that policy exactly equals the contract adapter set, the adapter plus
  boundary ledgers exhaust all 47 registry sources, shared artifacts contain
  exactly their expected source/release pairs, and every manifest/database
  identity remains intact.
- Atomic release set `a188a3adc6207d3a217d54a9` contains no copied database. Its
  content-addressed manifest pins 36 artifact manifests and databases,
  172,626,230 source records, 33 populated domain-table shard lists, registry/
  contract/scope/policy hashes, and all member logical/scientific hashes. E5 can
  query the shards read-only under one reproducible input identity.
- A clean independent output-root composition produces the identical manifest
  byte-for-byte and removes its scratch tree. The active `current` pointer is
  atomic. A full integrity pass rereads all 449,199,915,008 database bytes and
  matches every accepted manifest SHA-256. The release-set retention rule
  protects every referenced shard and prevents age-based cleanup from breaking
  an active or rollback evidence set.

### 78) E5 Selected-Fact Compiler Foundation

- Selection policy `2026-07-21.e5-selection.1` pins E4 release set
  `a188a3adc6207d3a217d54a9`, E2 identity graph
  `c84389ad55f17081fff008b4`, and canonical stability reference
  `20260717T0614Z_f452835_side`. The compiler verifies the selected E4 manifest
  and database checksums before reading evidence.
- Build `237158e09fce993f1b033414` binds 2,785,923 eligible Gaia AP records and
  84,588 NASA source rows to current objects, records 4,136,484 coherent-set
  decisions, selects 12,229,171 source facts, and derives 65,204 luminosities
  only where direct selected luminosity is absent. Source facts retain exact
  E4 build/table/evidence/set/record lineage; derived facts retain both input
  selected-fact IDs and versioned supersession metadata.
- Gaia specialized UCD/hot-star, GSP-Spec, and GSP-Phot atmosphere sets and
  FLAME/fallback fundamental sets use quantity-specific authority. NASA default
  reference-specific solutions outrank composite rows without field-wise
  mixing inside a quantity group. All duplicate, missing-lineage, and
  lower-authority-winner gates pass.
- This checkpoint proves the compiler architecture but deliberately does not
  declare E5 complete. Remaining domain policies, the exhaustive legacy
  derivation/prior inventory, shared consumer cutover, and the E6 scientific
  A/B build remain open.

### 79) Coherent Gaia Source Selection and Scale Gates

- The E5 compiler reads Gaia's 125-field ordered coherent arrays through their
  E4 schemas and selects only 17 currently required astrometry, photometry,
  radial-velocity, and diagnostic quantities. It retains the full source row in
  E4, verifies field position/type/unit agreement across both envelope schemas,
  and avoids a second 32-million-row scalar evidence expansion.
- The first diagnostic, `a8a74dbc173b9566fc4d5e5c`, selected no Gaia rows
  because `component_scope='gaia_source'` is a source-solution scope label, not
  an unresolved physical component. The active policy no longer applies that
  filter and requires at least 30 million eligible source rows, 5.8 million
  accepted bindings, and 50 million selected facts.
- A global candidate sort reached 338 GB spill after 33 minutes and was stopped.
  Authoritative, noncompeting Gaia source groups now use a direct path. The
  measured complete build took 18:41; partitioned export reduced it to 16:35
  while preserving 101,363,315 facts and 27,602,864 decisions.
- A second diagnostic, `b68c1e6b5649588175854701`, exposed that prepared
  `COPY TO` paths had not created partition files. The compiler now uses bounded
  literal paths and verifies existence plus exact row counts for all 40 fact
  and nine decision partitions before promotion.
- Accepted build `e8cb1529df6dbcc7c5baadee` selects 89,068,940 Gaia-source
  facts for 5,866,595 current stars and passes all identity, duplicate,
  lineage, authority, coverage, partition, and row-accounting gates. Independent
  clean reproduction matches logical hash
  `330614599768f062123305aece47c7965f0ff5114a7f9c293498869145e9327c`
  with no differing section and removes all scratch output.
- Independent artifact audits then rejected only diagnostics
  `a8a74dbc173b9566fc4d5e5c` and `b68c1e6b5649588175854701` for their measured
  coverage and partition failures. Exact-hash retention candidate
  `7e53eabad6412f57b767c20dee777fd6da57f14c5e57728e2114c8019118d17e`
  removed those two artifacts and reclaimed 34,929,528,832 allocated bytes.
  At this checkpoint build `e8cb1529df6dbcc7c5baadee` became current; complete builds
  `5c84220e408e8fea5f4da218` and `237158e09fce993f1b033414` remain available as
  rollback/reference checkpoints.

### 80) E5 Legacy Derivation and Presentation-Prior Inventory

- Machine-readable inventory `2026-07-21.e5-legacy-inventory.1` classifies 24
  production paths spanning ARM science derivations, empirical and display
  classifications, shared projections, runtime physical fallbacks, orbit and
  planet presentation assumptions, visual scaling, map selection, planet
  categories, coolness features, and DISC assumption materialization.
- Every path records its inputs, outputs, algorithm version, applicability,
  uncertainty limitation, confidence, provenance, supersession state, and
  retirement gate. Four accepted E5 derivation keys are all mapped to the
  legacy paths they replace; presentation-only policies remain explicitly
  separate from selected scientific facts.
- `scripts/audit_legacy_derivation_inventory.py` scans 36 production files,
  verifies 28 implementation bindings, discovers 16 versioned algorithm
  markers, and rejects missing symbols/markers, unknown E5 successors, or any
  materialized method absent from the inventory. The checked stability build
  has no unaccounted ARM/DISC marker.
- The stability baseline contains 2,709 materialized physical derivations,
  21,141 stellar classification rows, and 5,879,796 shared leaf
  classifications. Its 12,248 mass-only assumed classifications, 8,299
  assumed leaf values, and 337,357 leaves lacking exact classification evidence
  are now explicit E6 fallback-reduction metrics rather than hidden UI behavior.

### 81) E5 Distance Selection and Exhaustive Binding Outcomes

- Policy `2026-07-21.e5-selection.3` adds the accepted Bailer-Jones EDR3
  distance bundles without collapsing the EDR3 and DR3 namespaces. The binding
  contract records the authoritative, release-specific EDR3-to-DR3 source-list
  relationship and cannot be reused as generic cross-release numeric equality.
- An initial 5.8-million binding floor failed before promotion at 4,662,948.
  Independent intersection accounting found 5,866,598 current Gaia DR3
  identifiers, 1,203,650 without a row in the bounded Bailer-Jones envelope,
  and 1,200,620 of that tail with Gaia parallax signal-to-noise below five. A
  second provisional fact floor exposed the source-native count: 4,662,948
  geometric plus 4,344,950 photogeometric estimates. The checked-in floors are
  below these measured pinned counts, not weakened around an unexplained loss.
- The compiler now records an explicit accepted, missing, or ambiguous outcome
  for every eligible record from every selected source. Unresolved outcomes
  carry no canonical target and cannot emit facts; independent verification
  reconciles per-source outcome totals to eligible and accepted accounting.
- Accepted build `bfe3e1da9ddc5257f79b6838` contains 57,716,013 binding
  outcomes, 110,371,213 selected facts, 36,610,762 decisions, and 65,204
  derivations. Its distance facts retain exact evidence/bundle/source lineage,
  posterior endpoint semantics, methods, models, and citation.
- Independent artifact audit passes every identity, target/status, lineage,
  authority, duplicate, partition, and row-accounting gate. Clean-state rebuild
  matches logical hash
  `372cf0c7abf642684b46b2bf6590f6f3fd275d9f328e3e0aac6f15119525fda6`
  with no differing section and removes its 53-GB scratch tree.

### 82) E5 Quality-Aware Spectroscopy Selection

- Policy `2026-07-21.e5-selection.4` and compiler v4 add a general quality-rule
  contract for EAV evidence. Bounded JSON predicates determine candidacy, and
  one numeric source-native score orders repeats only after authority,
  coherent-set completeness, uncertainty coverage, and reference coverage.
  Both winner and runner-up scores are persisted in decision lineage.
- APOGEE's 163,971 eligible records bind as 113,951 accepted and 50,020
  missing through the explicit EDR3-to-DR3 source-list contract. Its unflagged
  ASPCAP tier selects 42,743 atmosphere sets and 120,012 facts.
- GALAH's 116,549 eligible records bind as 68,887 accepted and 47,662 missing.
  Official `flag_sp=0` and CCD3 S/N greater than 30 guidance admits 47,146
  candidates; 14,198 sets and 28,396 facts win.
- LAMOST's 1,651,199 eligible records bind as 844,959 accepted, 806,226
  missing, and 14 ambiguous. Source-native S/N resolves repeated M-star, MRS
  LASP, LRS LASP, and MRS CNN observations; 208,764 sets and 524,803 facts win.
- Gaia specialized and RVS atmosphere solutions remain authoritative. Survey
  tiers precede Gaia GSP-Phot and add selected temperature/gravity for 40,647
  stars. Coherent selection does not splice Gaia `[M/H]` into two-field survey
  solutions, so selected `[M/H]` falls by 83,257 while all evidence remains in
  E4. Source `[Fe/H]` is retained as a scientifically distinct future policy.
- Accepted build `d3f255b55e4573676347b206` contains 110,369,250 facts,
  36,651,409 decisions, 59,647,732 binding outcomes, and 65,204 derivations.
  Independent audit passes with no failures. Clean reproduction matches
  logical hash
  `54cc5e9fb95ce52b8743be4336e6c0a6033a0729eb6147550aba3580613655dd`
  with no differing section and removes its scratch tree.

### 83) E5 Source-Disposition Boundary

- `config/evidence_lake/e5_source_dispositions.json` accounts every accepted
  E4 source not yet present in the selected-fact policy. The first ledger
  records seven selected sources, three explicit non-selectable identity,
  context, or negative-control roles, and 28 blocking sources with an owning
  stage and scientific reason.
- `scripts/audit_e5_source_dispositions.py` fails on an unaccounted accepted
  source, a stale disposition, a selection/disposition conflict, an unknown
  selection source, an invalid disposition, or missing ownership/reason/blocker
  metadata. The checked ledger reports `in_progress`, accurately reflecting the
  remaining classification, compact-object, variability, relation/orbit,
  naming, lifecycle, extended-object, and projection policies.
- The selected-fact compiler now runs this audit before compilation, hashes the
  ledger version and bytes into immutable build identity, and writes the audit
  status and blocker list to its report. This prevents future E4 or policy
  changes from becoming silent omissions.
- No large selected-fact build was emitted for this metadata-only boundary
  change. The next approximately 53-GB compile is reserved for a batched
  classification/applicability checkpoint, preserving `/data` headroom and
  avoiding an immediately superseded artifact.

### 84) E5 Evidence-Subject and Compact-Model Selection

- Source-disposition ledger v2 moves UltracoolSheet and the Gaia EDR3
  white-dwarf catalogue from owned blockers to active policies, leaving nine
  selected sources, three nonblocking evidence-only roles, and 26 blockers.
- Policy/compiler v6 replaces the assumption that every source has one
  unscoped binding subject. Source records, individual classification evidence,
  and scoped parameter sets now retain distinct subject identity, parent-record
  lineage, component/claim scope, applicability evidence, and exhaustive
  accepted/missing/excluded/ambiguous/quarantined/unresolved outcomes.
- UltracoolSheet policy binds 10,887 populated classification subjects to the
  current Gaia DR3 identity graph. It accepts 5,335, records 5,552 as missing,
  and selects 5,282 categorical optical/infrared spectral, gravity, age,
  literature, and youth facts without converting source context into numeric
  measurements. Two-pass focused verification matches logical hash
  `2c55c8fa8b8e48094370a8fcaa075714269db99e79d4c89d361d4201740e3f33`.
- Gaia EDR3 white-dwarf policy uses the paper's general-purpose `Pwd > 0.75`
  threshold and release-specific EDR3-to-DR3 identity equivalence. Among
  222,805 candidates with usable model quantities, 164,425 bind uniquely,
  56,388 remain missing, and 1,992 are excluded as inapplicable.
- A reusable source-model preselection records completeness, uncertainty
  coverage, published fit chi-square, selected and runner-up model, and exact
  applicability evidence. It chooses 96,744 hydrogen, 46,078 helium, and
  21,603 mixed complete models and emits 493,275 focused facts from coherent
  Teff/log-g/mass sets. All alternative models remain in E4.
- Focused white-dwarf verification runs the projection twice and reports zero
  probability, scope, completeness, fit-order, duplicate, or lineage failures,
  with logical hash
  `89c6648d6a933d8bde53902b54033c1550c126674b5744c90c57b9fa14a7408f`.
  No large selected-fact artifact is promoted yet; these two policies are
  intentionally batched into the next storage cycle.

### 85) E5 Compiler Integrity-Join Diagnosis

- The first full policy-v6/compiler-v6 diagnostic completed its scientific
  tables: 75,062,360 binding outcomes, 164,425 source-model preselections,
  36,985,305 decisions, 110,867,283 facts, 65,171 derivations, and all nine
  source-accounting rows. It did not insert the evidence-build pass row, export,
  manifest, or promote an artifact.
- After output stopped growing, one uninstrumented integrity phase saturated
  eight threads for 2 hours 25 minutes and nearly 18 CPU-hours. The process was
  terminated with the verified v4 current pointer unchanged. Inspection proved
  scientific compilation was complete and isolated the delay to verification.
- DuckDB's plan for the new subject-lineage gate used a delimiter anti-join
  correlated on source, source record, evidence, and parameter-set IDs, then a
  three-branch `OR` against accepted bindings. This was a general query-contract
  defect, not a white-dwarf or named-object exception.
- Compiler v7 carries the exact accepted `binding_id` into each source-selected
  fact and verifies that direct key instead. It also writes an incremental
  machine-readable timing report with per-source/per-phase wall and CPU time,
  rows, peak RSS, durable bytes, and peak spill so future interruptions retain
  actionable evidence.
- An exact-candidate E5 retention dry-run and apply removed only the stopped
  manifestless staging and its external spill under candidate hash
  `b714aacda3b912e8eec26a00dc6808d81e2859c085cd3b1ea72d582ea67b6998`,
  reclaiming 110,415,441,920 allocated bytes. The accepted current artifact and
  every E4/raw/typed input remained protected.
- Full compiler-v7 build `f04aa4bc9c86d0c6f97a34da` then passed in 1,441.5
  wall seconds and promoted only after all 18 integrity gates passed. The
  formerly pathological accepted-binding check completed in 4.5 seconds. Its
  independent audit passes, and the machine-readable performance analysis ranks
  the remaining productive costs before clean reproduction.
- Clean reproduction passed with identical logical and per-file Parquet hashes,
  no differing report sections, and automatic scratch removal. Its
  12-thread/48-GB profile improved wall time by 7.6% but increased CPU time by
  6.5%; the result favors the 8-thread/32-GB shared-host default and a bounded
  Gaia materialization query experiment over indiscriminate concurrency.

### 86) E5 Storage and Historical Binding-Accounting Audit

- Exact-candidate scratch retention retired eleven named, closed June/July
  diagnostic workspaces and reclaimed 57,562,382,336 allocated bytes without
  touching identity targets, active caches, reports, served builds, or accepted
  evidence artifacts.
- The current auditor was made schema-aware for historical decision-quality and
  Parquet contracts. It then proved foundation `237158e…` and Gaia-source v2
  artifacts `5c84220…` and `e8cb152…` lacked exhaustive missing-binding outcome
  accounting; the distance checkpoint `bfe3e1d…` passed and remains protected.
- An exact failed-artifact retention pass retained and hashed all six historical
  reports, removed only the three independently rejected artifacts, and
  reclaimed 112,118,509,568 allocated bytes. Current `f04aa4…`, rollback
  `d3f255…`, and passing reference `bfe3e1…` remain.
- Internal free space rose to 258.7 GiB. This is sufficient for measured E5
  compilation using external spill, but below the 300-GiB acquisition floor;
  no new large source release may be acquired in this state.

### 87) E5 Policy Batches and Gaia Variability Selection

- The source-disposition ledger now records ten selected sources, four
  nonblocking evidence roles, and 24 remaining blockers. Gaia's ultracool
  sample stays inspectable as probability-bearing membership context but has no
  independent measured classification to select.
- Policy-batch ledger `2026-07-21.e5-policy-batches.1` assigns every blocker
  exactly once across seven dependency-ordered batches. Its machine audit also
  requires completed sources to resolve to an active selection policy or an
  explicit nonblocking disposition.
- Policy v7/compiler v8 allows a coherent-array destination to contain multiple
  schemas only when every quantity group names its applicable source-native
  parameter-set kind. The same kind predicate governs duplicate checks,
  decisions, and selected facts, preventing positional field leakage between
  Gaia variability summaries and rotation-modulation solutions.
- Two focused real-source compiles each account 592,197 Gaia DR3 variability
  subjects as 269,579 accepted and 322,618 missing. They emit 523,658 decisions
  and 6,888,406 facts with zero binding, duplicate, kind, numeric, or Boolean
  failures and match logical hash
  `7c294d776d662b8af997ace9534620f4dced37cdcd42f20d6bbd286af7f19384`.
- The focused phases take about 3 seconds for identity binding and 24.25 seconds
  for direct selection per pass. This source-level verification pattern lets E5
  batch compatible policy work before incurring the measured 24-minute full
  compiler and reproduction cycle.

### 88) E5 Gaia Supplementary AP Selection and Input Attestation

- Official Gaia documentation establishes that `libname_best_gspphot` is the
  publisher's selected library and its values already appear in the main AP
  table. Supplementary MARCS, PHOENIX, OB, and A solutions therefore remain
  coherent inspectable alternatives rather than duplicate public candidates.
- ANN atmosphere and alpha evidence require the source-recommended best-quality
  flag criterion `flags_gspspec_ann < 10000` and rank behind primary GSP-Spec.
  Spectroscopic FLAME physics ranks behind primary photometric FLAME. Explicit
  channel dispositions retain model stages, distances, extinction, absolute
  magnitudes, and bolometric correction as evidence rather than silently
  dropping them.
- Focused A/B accounts 8,019,372 supplementary subjects as 2,715,345 accepted
  and 5,304,027 missing. It adds 323,433 fallback facts while recovering
  2,657,698 previously omitted primary AP alpha, projected-rotation, and
  gravitational-redshift facts. Zero GSP-Phot alternatives, invalid ANN rows,
  unrelated channels, or lower-authority winners enter the projection.
- Two clean passes match logical hash
  `54466b7b5bfdf5f0a144226f7d509b7cd9a1edb166f35475ed991c753b1a0384`
  and remove both scratch databases. The first policy batch is complete with 11
  selected sources and 23 remaining E5 blockers.
- Compiler v9 parallelizes full expected-SHA verification of independent E4
  inputs and keeps a process-local attestation bound to full stat identity.
  The first AP pass byte-verifies both large databases in 151.76 seconds; the
  unchanged second pass reuses that attestation in 0.001 seconds. New
  invocations still byte-hash inputs, preserving the immutable-input gate while
  halving this focused two-pass checkpoint's elapsed time.

### 89) E5 Variable-Star Selection and Measured Binding Cost

- Policy v9 selects VSX variable class and period only through a unique Gaia DR3
  identifier. Of 10,304,607 coherent source records, 226,017 bind and
  10,078,590 remain explicit missing outcomes; selected output contains 226,017
  source-native class facts and 22,695 period facts.
- VSX spectral strings do not become preferred spectral classifications merely
  because VSX identifies a variable. Photometric extrema, passbands, limits,
  epoch, amplitude semantics, and bibliography remain coherent evidence for a
  later bandpass-aware projection.
- GCVS/NSV remains evidence-only. Its own designation-to-cross-identifier bridge
  currently reaches only 19 current canonical stars uniquely, so no general
  name guess, coordinate cone, benchmark exception, or system-to-component copy
  was added to improve yield.
- Two focused builds and independent artifact audit pass with build
  `974303e465aff3555de85b2e` and logical hash
  `40b95a458a9190a87e0118450f8340934a2468f194c8f33bd3f27fc1fd058fd9`.
  Each build takes about 26 seconds; exact binding takes about 9.6 wall/54 CPU
  seconds and peak RSS is about 11.3 GiB. The next optimization target is a
  compact or partitioned representation of exhaustive nonaccepted outcomes,
  never omission of their accounting or lineage.

### 90) E5 Release Bridges, Naming Consensus, and Component Collision

- Compiler v10 adds general same-release identifier-bridge and multi-identifier
  consensus bindings plus categorical identifier-claim selection. These are
  policy-driven contracts and contain no SIMBAD, WGSN, or named-star production
  branches.
- SIMBAD spectral classifications traverse basic-record OID to the bounded
  release-native OID/Gaia bridge. Of 435,079 subjects, 321,584 bind, 8 remain
  ambiguous multi-target bridges, and 113,487 remain missing. SIMBAD aliases,
  astrometry, object types, and bibliography remain evidence-only.
- WGSN selects 415 official proper names through convergent HIP/HD/HR/GJ claims.
  The unique-source-target gate exposed Izar and Pulcherrima as two source
  records converging on one current canonical star. Both remain ambiguous until
  general component identity separates their HR/Bayer targets; no row-order or
  benchmark exception chooses a winner.
- Two focused compiles and independent audit pass with build
  `501fd55a0994edd298210d91`, 321,999 exact-lineage facts, and logical hash
  `d738419b1472b1dfc6bde733c734c5eda71ba2638cf61e8f21219267641605a5`.
  Each pass takes about 40 seconds. Cached byte verification of the roughly
  46-GiB SIMBAD database takes 24.8 seconds and bridge binding about 4.8 seconds,
  so compatible identity policy work should be batched within one invocation.

### 91) Compact Evidence Does Not Override Missing Object Identity

- `scripts/audit_compact_selection_scope.py` performs exact, release-scoped
  ATNF/McGill designation to SIMBAD OID to Gaia DR3 to canonical traversal. It
  finds one ATNF route among 4,482 distinct pulsar names and no McGill route
  among 55 distinct name claims.
- The ATNF route is J0437-4715 to Gaia DR3 4789864076732331648. The current
  canonical target is a single leaf with ordinary K-spectrum evidence and a
  legacy pulsar type, so the audit quarantines it as unresolved pulsar/optical-
  companion scope. Timing or spin evidence is not copied onto that leaf merely
  to produce a selected fact.
- All 91,858 ATNF parameter/glitch contexts and 139 McGill timing, X-ray,
  distance, position, and context sets remain inspectable evidence. E6 owns
  permanent non-Gaia compact-object identity within the ingestion envelope and
  must rerun this audit before compact quantities become selectable.
- The unrestricted diagnostic completed in 2.5 wall seconds but consumed 53
  CPU-seconds and about 8.3 GiB RSS. The checked-in four-thread/8-GB profile
  completes in about 6 seconds and halves CPU consumption; pushing the compact
  name filter into the SIMBAD scan did not materially reduce mapped database
  memory, so that attempted optimization is recorded rather than overstated.

### 92) Accepted-Binding Materialization Is Not a Gaia Build Shortcut

- A focused full-scale optimization run materialized the 5,866,595 accepted
  Gaia bindings once and removed the unused source-context join before direct
  fact expansion. Artifact `887e762a67ea0b432c49bdd5` contains the expected
  89,068,940 facts and 23,466,380 decisions and passes the independent audit.
- Candidate insertion nevertheless regressed from the measured 540.0-second
  baseline to 661.5 seconds; binding increased from 44.0 to 48.0 seconds. The
  15:45 run peaked at about 36.7 GiB RSS and 143.5 GiB spill allocation.
- The attempted compiler change was removed rather than rationalized into
  production. Machine timing, performance analysis, compile, and artifact-audit
  reports remain under `reports/evidence_lake_v2`. The analyzer now directs the
  next experiment toward direct fact encoding and duplicate DuckDB/Parquet
  durability instead of another accepted-binding cache.

### 93) Fast Export Is Not Deterministic Export

- A one-pass partitioned writer reduced the focused 17-partition Gaia export
  from 97.7 seconds to 64.5 and 76.4 seconds, but identical runs produced
  different Parquet hashes and byte totals. Its faster output is not a
  reproducible scientific artifact.
- A stable global order used about 33 GiB RSS, spilled about 73 GiB, and wrote
  only 11 partitions after 208 seconds. Four concurrent stable writers peaked
  near 35.2 GiB RSS and completed only four partitions in 42.1 seconds before a
  shared temporary-directory conflict. Both had already demonstrated worse
  throughput than the current sequential writer and were stopped or rejected.
- No experimental writer entered production. The next performance work must
  address the 540-second direct Gaia JSON-to-fact materialization or remove the
  duplicate DuckDB/Parquet durability cost through an explicit artifact-contract
  change; it must not weaken stable IDs, lineage, row order, or byte hashes.

### 94) Multiplicity Starts With Two Independent Endpoints

- E5 relation compiler v1 resolves the left and right endpoints separately and
  retains every accepted, missing, excluded, or ambiguous outcome. A relation
  cannot borrow a whole-system name match to populate a component endpoint.
- El-Badry artifact `c59bf6664db0b60960dc36a1` accounts all 1,116,713
  retained claims and 2,233,426 endpoint attempts. It finds 102,266 claims with
  two current canonical endpoints, including 95,045 source-defined
  `R_chance_align < 0.1` high-confidence candidates and 3,043 shifted-sky
  negative controls.
- The projection preserves the published density ratio as a confidence
  statistic, never manufactures a probability, and creates no hierarchy,
  orbit, or containment row. Independent audit and clean reproduction pass with
  identical ordered Parquet hashes.

### 95) DEBCat Physics Requires a Relation, Not a Name Guess

- Component-scope artifact `1dddf975f24d9bba9590d046` materializes 6,936
  exact WDS system outcomes, 31,347 release-scoped MSC component identities,
  and all 15,748 MSC relations. It retains 7,347 missing component anchors and
  classifies three identical-endpoint source rows as invalid self-relation
  evidence. It creates no CORE object, hierarchy edge, or containment claim.
- All 374 DEBCat systems pass through the canonical search-term normalization
  and existing priority language. Thirty-seven have one best-priority system;
  337 remain missing. Component binding then requires that system's exact WDS
  identifier and one accepted MSC day-period relation within the general
  `max(0.01 day, 1%)` neighborhood. Twenty bind, including six more than the
  legacy CORE-seeded bridge; 17 named systems have no compatible relation.
- The accepted projection exposes 216 stellar/system measurements, 32 spectral
  classifications, 74 system-integrated photometry rows, and 20 relation-period
  solutions as eligible selection inputs. Primary and secondary facts target
  release-scoped MSC components; metallicity and photometry target the system.
  Eligibility does not bypass cross-source authority selection.
- The independent audit, clean fixture, and production reproduction match all
  ten ordered Parquet hashes. The focused compiler takes about 2.3 seconds.
  Canonical WDS binding also exposes 612 missing and 51 multi-system identity-
  graph diagnostics for later E2/E6 reconciliation rather than silently using
  those pre-grouping routes.

### 96) SB9 Uses Its Explicit MSC Sequence Bridge

- Component policy v2 reads the exact `SB9_<sequence>` references already
  carried by MSC relation evidence. It does not compare system names or
  coordinates and withholds both endpoints unless exactly one referenced MSC
  relation is already accepted.
- Of 4,079 SB9 relations, 790 bind, 3,104 have no MSC reference, eight have
  duplicate references, and 177 reference an unresolved or invalid MSC
  relation. Every source relation retains one explicit outcome.
- Accepted bindings make 874 component apparent-magnitude measurements, 940
  primary/secondary spectral classifications, and 1,052 spectroscopic-orbit
  solutions eligible for later authority selection. Multiple published orbit
  solutions remain separate; the component compiler does not choose an orbit
  winner or create hierarchy/containment.
- Artifact `1dddf975f24d9bba9590d046` passes the independent audit, clean fixture,
  and byte-identical production reproduction in about 2.6 seconds per pass.

### 97) ORB6 Resolves Pairs Before It Resolves Orbits

- Component policy v3 removes the legacy requirement that an ORB6 system have
  one pre-existing binary edge. Instead, every visual-orbit row must match the
  exact WDS identifier plus the punctuation-preserving combined discoverer and
  component designation to one WDS summary record.
- The WDS field contract drives endpoint parsing: blank component scope is the
  ordinary A/B pair, simple pairs split into their two symbols, comma and hyphen
  forms preserve explicit subsystem endpoints, and the catalog's five-character
  shorthand such as `Aa1,2` expands to `Aa1,Aa2`. No system name, coordinate,
  named object, or unique-edge fallback participates.
- Of 4,051 ORB6 rows, 1,159 resolve to one accepted WDS-qualified MSC relation,
  646 lack the exact current WDS pair, and 2,246 lack an accepted MSC endpoint
  relation. Every source row and rejection reason remains inspectable.
- Artifact `6def85dff374034cfe125b6b` projects the 1,159 accepted published visual
  orbits as eligible evidence without creating canonical objects, containment,
  hierarchy, or system-level orbit rows. Independent audit, clean fixture, and
  byte-identical production reproduction pass across seventeen ordered Parquet
  files in about 3 seconds per pass.

### 98) MSC Scope Starts With Every Identity Claim

- The initial E5 component graph began from hierarchy endpoints and therefore
  omitted 1,443 valid source components or parent subsystems that appeared only
  in other MSC tables. Policy v4 instead starts from every release-native
  `msc_component` identity claim. It accounts 32,790 identities: 24,671 anchor
  to 5,369 exact canonical WDS systems and 8,119 remain missing scopes.
- Every MSC parameter set, classification, photometry row, astrometry/motion
  row, hierarchy relation, and orbit row now receives a component or relation
  outcome. Accepted scopes expose 44,130 mass/apparent-V facts, 16,182 spectral
  classifications, 54,902 photometry rows, and 62,214 astrometry/motion rows as
  later selection candidates. The 5,793 accepted relative separations remain
  context evidence because they are not intrinsic stellar quantities.
- The 14,638 hierarchy-table orbit rows follow their own relation claims. The
  separate orbit table must provide two comma-separated endpoints and match one
  accepted WDS-qualified MSC relation. Across all 19,366 orbit rows, 14,939 are
  eligible; 3,397 unresolved and three invalid hierarchy relations, 914 missing
  orbit-table relations, 110 non-two-endpoint labels, and three missing pair
  identities remain explicit.
- Artifact `bbc7f0083646dfd5a602467b` creates no canonical stars, containment,
  hierarchy, or orbit winner. Independent audit, clean fixture, and
  byte-identical reproduction pass across twenty-four ordered Parquet files in
  about 5.8 seconds per pass.

### 99) SBX Targets Systems Without Inventing Components

- Component policy v5 resolves SBX system identity through exact Gaia DR3,
  official Gaia DR2-to-DR3, HIP, HD, and TIC evidence. It deliberately excludes
  broad catalog aliases and component-qualified WDS observation-target strings
  from system identity. Of 4,080 systems, 2,354 resolve uniquely, 1,699 remain
  missing, and 27 retain conflicting canonical system candidates.
- All 8,160 primary/secondary identities remain release scoped. Accepted system
  anchors make 2,561 magnitudes, 2,208 spectral classifications, and 3,043
  linked spectroscopic-orbit solutions eligible for later quantity selection;
  no SBX component becomes a canonical star or containment assertion.
- The compiler does not assume that the Gaia or catalog observation target is
  the primary. All 20,152 astrometry rows remain system/photocenter context.
  Seventy of 94 source hierarchy claims have independently resolved endpoints,
  but remain noncanonical relation evidence.
- Combined artifact `7ae9b19a56212bfdc4f44d3b` passes the independent artifact
  audit and byte-identical reproduction. The complete five-source component
  build takes about 10.0 wall seconds, 17.9 CPU-seconds, and 1.46 GiB peak RSS,
  confirming it is not a material contributor to the 24-minute full selected-
  fact build.

### 100) WDS Pair Context Requires Exact MSC Endpoints

- Component policy v6 applies the same documented WDS notation contract used
  for ORB6 to all 157,299 WDS summary rows: blank is the ordinary A/B pair;
  simple two-symbol, comma, hyphen, and abbreviated numbered forms are parsed;
  no other syntax, coordinate, discoverer-name, or system-level fallback is
  allowed.
- Exact punctuation-preserving WDS identity plus unordered endpoints resolves
  5,282 rows to one accepted MSC relation. Another 152,016 lack an accepted
  relation and one has two candidates. All outcomes retain the exact source row
  and reason.
- WDS spectral text remains pair context because the source field may describe
  the primary or both components. Unspecified-band magnitudes, observation
  history, relative astrometry, lexical positions, and source-convention proper
  motions remain scoped context even for accepted pairs; no selected scalar,
  canonical component, containment, or hierarchy is emitted.
- Combined artifact `33f2a90275378a35be21a704` passes independent audit and
  byte-identical reproduction. It accounts 73,779 classifications, 312,727
  magnitude rows, and 1,779,163 astrometry/history rows in 18.9 wall seconds,
  38.0 CPU-seconds, and 4.78 GiB peak RSS.

### 101) Gaia NSS Fits Are Not Component Identities

- Component policy v7 accounts all 87,075 coherent Gaia NSS source/model
  solutions for 84,572 Gaia sources. Exact DR3 identity anchors 56,617 solution
  rows on 54,794 current canonical observation targets; 30,458 rows for 29,778
  sources remain outside the current reference, with zero ambiguous bindings.
- Each accepted solution preserves its source model, complete fitted
  parameter/error set, correlation vector, diagnostics, frame, and reference as
  one context. Multiple models for one Gaia source remain distinct.
- NSS has no inspectable physical-component endpoints. All 87,075 relation
  claims therefore remain null and every solution is marked for relation
  adjudication. No companion, containment edge, canonical relation, selected
  scalar, or simulation-ready orbit is manufactured.
- Combined artifact `9e59131b92205068f7246a94` passes independent audit. The
  seven-source component build takes 23.0 wall seconds, 45.5 CPU-seconds, and
  5.27 GiB peak RSS before clean reproduction.

### 102) TESS EB Targets Are Not Physical Binary Endpoints

- Component policy v8 binds Villanova TESS EB evidence only through exact
  normalized, release-scoped TIC identity. Of 17,605 rows, 6,605 bind to one
  canonical observation target and system and 11,000 remain outside the
  current reference; there are no ambiguous or identifier-less rows.
- The accepted set retains 2,228 positive and 4,377 negative catalog-membership
  records. Sectors, morphology, source/flags, Tmag, unresolved target physics,
  and astrometry stay context on the observation target. Negative evidence is
  preserved rather than filtered out.
- All 4,584 eclipse-timing solutions retain their coherent period/epoch/error
  sets. None has inspectable physical component endpoints, so every solution
  requires relation adjudication and none creates a component, containment
  edge, selected scalar, or simulation-ready orbit.
- Combined artifact `f5358c0a0983958e5d4f76c5` passes independent audit and
  byte-identical reproduction. The eight-source component build takes 24.7
  wall seconds, about 42-45 CPU-seconds, and about 5.2 GiB peak RSS. This
  completes the multiplicity E5 policy batch and confirms it is not a dominant
  contributor to the 1,441.5-second full selected-fact build.

### 103) Cluster Membership Probability Is Not Containment

- Cluster policy v1 binds Hunt/Reffert cluster identities only through exact
  published source and literature designations after a conservative
  space/hyphen normalization. It also checks the inverse mapping. This accepts
  62 of 465 source clusters, leaves 393 outside the current reference, and
  marks 10 ambiguous rather than collapsing multiple source clusters or the
  mutually claimed NGC 2451A/B identities.
- Only the 62 accepted coherent Hunt/Reffert posterior sets become eligible for
  later cluster-quantity selection. The remaining characterization and
  literature-crossmatch rows retain their exact source identity and reason.
- Member identity is resolved independently through exact Gaia DR3 IDs. Of
  51,017 source membership claims, 17,273 member endpoints resolve and 4,247
  claims have both cluster and member endpoints accepted. All HDBSCAN
  probabilities, including values of one, remain probability-bearing evidence;
  no threshold or source row creates canonical containment.
- Artifact `a6169c9ec351db81104e8518` passes independent audit and byte-identical
  reproduction in about 2.2 wall seconds at about 1.1 GiB peak RSS. This
  focused compiler is not a material contributor to the slow full E5 build.

### 104) Extended Evidence Stays Outside Stellar Facts

- Extended-object policy v1 reconciles evidence only through exact general
  catalog-source keys already represented in the canonical source bridge. It
  does not use coordinates, visual overlap, or named-object branches.
- All 310 Green SNR rows bind to one canonical extended object. Of 19,012
  OpenNGC-family rows, 17,800 bind, 803 preserve explicit exclusions, 404 remain
  quarantined as unclassified, and five redirects remain unresolved because
  their target is absent. Every row retains its exact outcome and reason.
- Geometry, distance, morphology, flux, spectral index, component context,
  names, citations, and lineage stay in the extended-object domain. Artifact
  `3790054572476ea189aaff06` emits zero stellar facts, passes independent audit,
  and reproduces byte-identically in under one wall second.

### 105) Horizons Orbits Are Epoch/Frame-Bound Evidence

- Solar System policy v1 binds 59 natural objects through exact reviewed
  `sol_authority` source keys. The canonical Sun follows the same general exact-
  identifier path through its stored `jpl_horizons_command`; no object name is
  embedded in the compiler. All 60 targets resolve uniquely.
- Orbit centers resolve independently through exact JPL commands. Fifty-nine
  solutions have physical target and center components. The Sun's command `0`
  center remains a declared Solar System barycenter reference origin rather
  than becoming a fabricated object or edge.
- The first selected projection retained its TDB epoch 2457388.5,
  ICRF/ecliptic/AU-D frame and unit context, method, model, exact query/response,
  checksum lineage, but review exposed that the source adapter parsed only four
  of the 12 standard numeric `ELEMENTS` columns. Artifact
  `64e2bc581745f1491217fd7e` is therefore retained only as a superseded
  diagnostic; passing structural and deterministic gates did not make the
  incomplete scientific projection acceptable.

### 106) Complete Horizons Elements Replace the Four-Field Projection

- One shared header-driven parser now materializes eccentricity, periapsis and
  apoapsis distance, inclination, ascending node, argument and time of
  periapsis, mean motion, mean and true anomaly, semi-major axis, orbital
  period, epoch, and calendar context. Schema drift fails closed rather than
  silently discarding a changed or newly missing column.
- Refreshed natural snapshot `164c147ee3b98ab3dab603bb` and artificial
  snapshot `32654e1013dae08f24b92cdc` reproduce from byte-identical responses.
  E4 artifact `b4edc4ea6eccba69794a92df` accounts all 142 records and 85
  registered fields; all 71 solutions contain the complete 12-element row.
- Release set `fde14e4687a853c844b0e341` pins 38 sources and 36 artifacts totaling
  449,199,915,008 bytes. Full checksum verification took approximately 300 wall
  seconds, making repeated immutable-input hashing a measured target for the
  E5/E6 performance report and safe invocation-local or metadata-backed cache
  design. No checksum or scientific-integrity gate was bypassed.
- Corrected E5 artifact `d61c6890588ee40c46ea7d56` binds all 60 natural
  targets, preserves complete coherent orbital solutions, 36 radii, and 20
  masses, and passes independent audit and byte-identical reproduction in about
  1.1 seconds with no canonical relation promotion. Solar projection itself is
  not a material contributor to the slow full E5 build.

### 107) TIC Disposition Is Row Evidence Even Without a Duplicate Endpoint

- E5 review found that E4 field accounting routed TIC `disposition` to relation
  evidence, but `SPLIT` and `ARTIFACT` rows without `duplicate_id` had no
  inspectable status. A general source-context-copy contract now retains a
  domain field in row context without changing its typed primary destination.
- Corrected TESS artifact `03acb9eb0fb2cbc0f8203dd8` preserves all 27,406
  blank, 428 `SPLIT`, 71 `DUPLICATE`, and 25 `ARTIFACT` dispositions and passes
  generic, targeted-source, and clean-reproduction gates. The current indexless
  E4 contract reduces its DuckDB from 609,497,088 to 308,555,776 bytes while
  adding the missing context.
- Release set `6c19de054e9b807674c37d3c` verifies all 38 sources and
  448,898,973,696 bytes in 358.4 wall seconds. A one-shard scientific change
  still rereads the full set, strengthening the case for integrity-preserving
  unchanged-member attestation in the E5/E6 performance work.

### 108) Supplemental Planets and TOIs Do Not Own Canonical Inventory

- Planet policy v1 binds Exoplanet.eu and HWC by unique normalized canonical
  planet name and OEC by structural object plus every source-native name. Of
  23,113 source objects, 15,378 bind, 7,731 remain missing, and four OEC alias
  conflicts remain ambiguous. All 17,514 lifecycle rows, 23,100 coherent sets,
  and 260,231 facts remain inspectable. HWC is derived comparison evidence.
- All 27,930 TIC targets receive official Gaia release-graph outcomes. All
  8,064 TOIs retain polarity: 1,332 confirmed/known, 5,383 candidate, 1,346
  negative, and three unclassified. Only 824 confirmed/known signals link to an
  existing planet through an accepted host and unique period. No row creates a
  planet or changes the 6,311-object canonical inventory.
- Artifact `86aa5553053db35d81ff26e0` preserves 24,188 transit and 39,187 TOI
  parameter facts, passes independent audit and byte-identical reproduction in
  8.7-10.2 wall seconds, and closes all four remaining source-policy blockers.

### 109) Current-Release E5 Projection and Measured Build Cost

- Policy v12/compiler v11 moves the main selected-fact projection onto E4
  release set `6c19de054e9b807674c37d3c`. Build
  `0a57f778ce13de1c2c800103` accounts 94,414,212 exhaustive bindings,
  41,078,490 decisions, 121,304,924 facts, and 65,104 derivations. Independent
  audit verifies policy version/hash lineage and every scientific gate.
- Clean reproduction matches logical hash
  `6ccec12397bbe7d64878c52ead6a06ffca52d686e75020b8fb08831e58c69628`
  and every report section, then removes its USB scratch tree. The accepted run
  takes 1,577.3 measured wall seconds and the reproduction takes 1,734.4.
- Gaia direct materialization at 543.1 seconds is the dominant optimization
  target, followed by deterministic export, Bailer-Jones binding/projection,
  immutable input verification, and global selection. The full report is
  `docs/E5_BUILD_PERFORMANCE_2026-07-22.md`.
- One alpha-abundance selection had no optional quality-order value but no
  same-authority competitor. The compiler and independent auditor now permit
  that noncompetition while failing any same-authority competition lacking a
  score. The pre-gate intermediate was independently rejected and exactly one
  74,069,770,240-byte artifact was reclaimed through fail-closed retention.

### 110) E6 Shadow Foundation Uses Typed Selected-Fact Projections

- E6 policy `2026-07-22.e6-shadow.1` pins the stability product and all seven
  accepted E5 artifacts. Build `e6_994a6301c335ac385f5dc052_shadow` copies the
  stability CORE/ARM/hierarchy/DISC, adds source-projection tables and 69
  stellar/16 planet selected-fact projections, and applies only explicit CORE
  scalar mappings. Every projected value retains its exact selected-fact ID.
- The first independent audit exposed a general Boolean routing defect: twelve
  Gaia variability membership facts store `true`/`false` in `value_raw`, so a
  numeric-only projection retained fact IDs but emitted null values. The type
  contract now projects these quantities as Boolean; no source- or object-name
  exception was added.
- The accepted shadow preserves 5,869,091 systems, 5,874,636 stars, 6,311
  planets, and every hierarchy row. It adds 65 non-primary official-name
  aliases, fills 193,923 stellar temperatures, 1,004 planet equilibrium
  temperatures, and 1,078 insolation values, and creates no canonical
  inventory, status, relation, containment, or candidate promotion.
- Independent audit passes every product-integrity, inventory, hierarchy,
  lineage, selected-value, alias, and lifecycle gate. Clean reproduction uses
  order-independent cryptographic row-multiset hashes for fifteen generated or
  mutated tables and matches every logical hash. CORE/ARM physical DuckDB bytes
  differ between runs, confirming that runtime database layout is diagnostic,
  not scientific content identity.
- The accepted foundation compiles in 128.0 wall seconds at 12 threads/48 GB,
  peaks at 35.1 GiB RSS without spill, audits in 35.7 seconds, and completes
  isolated compile/audit/logical reproduction in 247.7 seconds. DISC/public
  regeneration and the complete E6 scientific A/B remain open.

### 111) Shared Classifications Must Consume Selected Facts and Exact Scope

- The stability CORE stored 5.53 million Gaia `spectral_type_raw` values that
  were actually generated from temperature or BP-RP during ingest. Treating
  these as source classifications hid provenance and made UI surfaces disagree
  as they repeated different fallback orders.
- E6 now selects one canonical-star display class from compact-object identity,
  selected direct spectral evidence, source-native non-Gaia fallback, selected
  temperature, selected BP-RP, then selected mass. Every selected-evidence prior
  retains its fact ID; disagreements among lower-priority presentation priors
  are not mislabeled as competing source conflicts.
- Canonical and inferred hierarchy leaves bind to release-scoped MSC component
  evidence through canonical system identity and normalized component label.
  This replaces 5,683 component spectral and 8,314 component-mass legacy paths
  without any system-name exception.
- Against the stability leaf projection, 338,820 classes change, 929 unknowns
  become classified, and zero known classes become unknown. The change remains
  unserved pending the complete E6 scientific A/B and downstream rebuild.

### 112) Preserved Evidence Is Not Useful Until Its Object Scope Is Selectable

- The first E6 parameter A/B found 62 lost temperatures, 686 masses, 193 radii,
  and 195 luminosities. All but one are NASA Exoplanet Archive host-star values.
  E4 preserved the reference-specific, stellarhosts, and composite parameter
  sets correctly, but E5 policy v12 included only a planet-scoped NASA selection
  program.
- This is not repaired with a UI fallback. The durable repair is a star-scoped
  NASA host program with authoritative identifier binding, coherent parameter
  set selection, exact evidence lineage, and correct log-luminosity semantics.
- A separate 1,160-row distance loss was initially described as legacy Gaia
  inverse-parallax output. Source-level comparison corrects that interpretation:
  every row is the Gaia DR3 main-table GSP-Phot posterior model distance, with
  its published interval, method, and model already preserved in E4.

### 113) Shared Source Artifacts Require Object-Scoped Selection Programs

- NASA host-star and planet evidence intentionally share one release-scoped E4
  artifact and source ID. E5 now treats their selection programs as distinct by
  source ID, object type, and binding scope, and every binding-to-candidate join
  includes object type. Exact duplicate programs remain invalid.
- A synthetic compiler golden proves that a source record containing both host
  and planet evidence emits only stellar mass on the star and planet radius on
  the planet. The checked-in source-disposition audit likewise permits distinct
  scoped programs while rejecting conflicting duplicate dispositions.
- The final hash-verified NASA host preflight takes 7.60 seconds, including 3.62
  seconds to attest the 4.50-GB evidence shard and 1.19 seconds to compose its
  decisions against accepted v12. It selects 12,210 facts with
  zero cross-object leakage, duplicate quantities, or missing lineage. Full E5
  remains the authority-competition and deterministic-export acceptance gate.
- The preflight predicted 6,320 primary and 415 supplementary Gaia AP facts
  displaced by the targeted host policy with zero authority ties. The exact
  323,018 supplementary count then passed in the global compiler. Candidate
  `16708b8ed193aeae9b2ab995` contains 121,306,839 facts and passes the updated
  independent object-scoped audit. Its clean USB-scratch reproduction finishes
  in 24:49, matches all report sections and logical hashes with no differences,
  and removes its scratch tree; it remains unserved pending v14 and E6 A/B.

### 115) Source-Model Distances Are Evidence, Not Reciprocal-Parallax Derivations

- The checked-in GSP-Phot preflight accounts 6,955,056 positive, ordered Gaia
  model posteriors and 1,982,472 unique accepted canonical bindings. Of these,
  1,981,312 also have geometric Bailer-Jones facts; the remaining 1,160 are the
  complete E6 distance-loss tail.
- The legacy and GSP-Phot values match within 0.001 pc. The same objects have
  selected parallax S/N from 2.07 to 7.39, with none at 10 or above, so an
  inverse-parallax fallback would be scientifically weaker and mislabeled.
- E5 policy v14 therefore selects `distance_gspphot_pc` with exact evidence,
  method, model, posterior bounds, binding, and release lineage. Shared
  consumers apply geometric Bailer-Jones, photogeometric Bailer-Jones, then
  GSP-Phot precedence. Supplementary AP library fits remain alternatives rather
  than being flattened into the main-source selection.

### 117) A One-Field Policy Addition Must Not Require a Monolithic Recompile

- E5 policy v14 selects 1,982,472 official GSP-Phot distance facts and restores
  the 1,160-row legacy distance tail without reciprocal-parallax substitution.
  Unserved build `929bf92b4c5dbd5aef7e5972` passes independent audit and clean
  reproduction with 123,289,311 facts, 43,061,309 decisions, and logical hash
  `af1155454dc91f8d653735e81ae8c153cdb5c7454e93ea4ab69301ea59d4be1f`.
- The reference compile takes 30:28.86 and clean reproduction 29:54.48. Gaia
  direct materialization, deterministic export, global selection, immutable
  byte verification, final hashing, and Bailer-Jones processing dominate. The
  single independent scalar increases facts by 1.63% but total phase time by
  5.10%, exposing avoidable global-policy and rebuild coupling.
- The next compiler architecture should use content-addressed program-level
  intermediates, release-scoped reusable identity outcomes, and a direct-scalar
  lane while preserving exact lineage, authority, unresolved accounting, and
  deterministic hashes. Previously measured accepted-binding-cache and fast
  partition-export attempts remain rejected because they were slower or
  nondeterministic.

### 118) Associated-Primary Astrometry Is Not Companion Identity

- E6 coolness review found that 162 UltracoolSheet companion rows carry
  `astrom_Gaia=P`: their Gaia DR3 identifier belongs to a higher-mass primary
  used as an astrometric proxy. Treating it as the companion's identity moved
  direct ultracool classifications onto the primary; HD 3651 was a diagnostic
  example, not a production exception.
- E4 adapter v2 uses existing conditional-identifier contracts to type
  `astrom_Gaia=O` as object identity and `P` as
  `associated_primary_astrometric_proxy`. The source audit accounts 3,794
  object-owned and 323 proxy DR2/DR3 claims with zero scope mismatches; clean
  reproduction matches build `a328a9e13d6c2b44f8d57861`.
- E5 policy v15 adds a reusable source-context binding-applicability predicate
  and preserves every proxy row as evidence while excluding its object-scoped
  facts from primary-star selection. No name, system ID, or catalog-row branch
  is present. Release set `51b08e537e768acf63e554e1` pins the corrected shard.
- The first full v15 compile stopped at its exact source-accounting gate because
  the preflight forecast subtracted all 452 former proxy winners from the v14
  total. A source-native audit found 27 object/proxy quantity collisions on the
  same Gaia target; v14's invalid competition chose the proxy in 13. Removing
  the proxies correctly restores those 13 object-owned facts, so the exact v15
  expectation is 4,843 accepted bindings and 4,843 selected facts.
- Full verification of the 448.8-GB release set takes 6:17.82 and is mostly
  single-threaded. This is now recorded alongside the 30-minute E5 compiler as
  an integrity-preserving parallelization or immutable-attestation target.

### 116) E6 Diagnostics Are Retired Only Through a Reproducible Set Gate

- A dedicated E6 retention tool verifies the replacement's declared product
  bytes, independent audit, and clean reproduction, then rejects candidates
  reached by pointers, retained manifests, live processes, symlinks, hardlinks,
  or unacknowledged reports. Apply requires the exact current dry-run set hash.
- Hash `e798e3104597e985ae7ae38dd163cadaf0364260e2f4af681d9075943721b674`
  selected only four superseded unserved shadows and reclaimed 68,429,119,488
  allocated bytes. Current candidate `e6_2da376053461c8220bee06ad_shadow`, its
  transitive inputs, and all durable audit, A/B, reproduction, and performance
  reports remain protected.
- Retention reports identify retired artifacts for audit purposes but do not
  make those artifacts live dependencies. A regression test enforces that
  distinction and prevents a self-referential dry-run/apply candidate hash.

### 114) E6 Consumer Projections Belong Inside the Immutable Build

- The first v2 E6 consumer experiment materialized selected display and leaf
  projections after the compiler had already hashed and promoted its product.
  That diagnostic was scientifically useful but its manifest no longer
  described its bytes, so it is explicitly ineligible for promotion or clean
  reproduction.
- Compiler v2 hashes both auxiliary materializers into build identity and runs
  them before metadata, product hashing, manifest creation, and atomic
  promotion. The independent auditor verifies canonical-star inventory,
  terminal-leaf inventory, classification validity, uniqueness, fact lineage,
  and manifest counts rather than trusting the compiler report.
- Integrated candidate `e6_2da376053461c8220bee06ad_shadow` compiles in 166.11
  wall seconds with no spill and passes the expanded audit in 36.78 seconds.
  Its A/B changes 338,858 classes, fills 930 unknowns, loses zero known classes,
  and attributes the remaining legacy physical-value tail to retained
  lower-authority NASA composite alternatives. A per-field fallback is rejected
  because it would silently break coherent parameter-set selection.
- Clean isolated reproduction completes in 311.37 wall seconds and matches the
  build identity, compiler reports, inventory, and all eighteen generated or
  mutated logical table hashes. The USB reproduction staging tree is removed
  automatically after the gate.

### 119) Component Labels Are Case-Significant Physical Scope

- The E6 map golden exposed a third A-class Castor component, but the defect was
  not Castor-specific. E5 had lowercased MSC component labels when constructing
  release-scoped identity keys, collapsing subsystem labels such as `AB` with
  terminal stars such as `Ab`.
- The full artifact contained 238 case-fold collision groups covering 476
  accepted component entities. Of 9,162 inferred hierarchy leaves, 231 could
  bind multiple source entities and 41 could see multiple classifications.
- Component policy v9/compiler v9 preserves exact case for MSC physical
  identity and adds a zero-duplicate accepted WDS/source-label gate. Canonical
  terminal labels normalize to their source-native terminal form. Cross-table
  orbit/reference notation may case-fold only when exactly one compatible
  relation exists and ambiguity accounting passes.
- Corrected component artifact `67fea5f99500b57419ebdeb0` compiles in 24.76
  seconds, passes independent audit, and reproduces byte-identical ordered
  Parquet files in 24.72 seconds. Corrected E6 shadow
  `e6_cfcdf2d9add2cd7e2b96af68_shadow` then passes all 194 independent checks,
  clean logical reproduction, scientific A/B, and exact map coverage at 100,
  250, 500, and 1,000 ly. The production transform contains no named-system,
  display-name, or benchmark-identifier condition.

### 120) E7 Retirement Is Gated by a Clean Evidence-Lake Entry Point

- The corrected E6 candidate remains intentionally unserved and still composes
  permanent inventory and identity from stability build
  `20260717T0614Z_f452835_side`. Therefore the legacy cookers,
  `ingest_core.py`, and `build_arm.py` are not yet safe to retire even though
  their field-losing scientific role is superseded.
- `config/evidence_lake/e7_legacy_path_inventory.json` accounts seven legacy,
  transitional, diagnostic, and permanent-identity path families. No entry is
  marked retired. The canonical identity reducer is retained and adapted;
  reproducible cooked projections may retire only after a clean pinned-input
  driver, accepted scientific A/B, local atomic promotion, rollback, and
  re-promotion.
- `config/evidence_lake/gaia_dr4_adapter_plan.json` treats DR4 as a new
  release-scoped evidence family. DR3 and DR4 source IDs are never compared as
  interchangeable identifiers; official crossmatch candidates, split/merge
  ambiguity, and permanent Spacegate identities remain separate concerns.
- `scripts/verify_e7_cutover_plan.py` passes all 19 checks. Its run also exposed
  an unowned `e6_selected_consumer_projection_v3` marker; assigning that marker
  to the existing shared display-projection inventory restored the independent
  24-path legacy derivation audit to pass without suppressing discovery.

### 121) Compact Objects Need Permanent Non-Gaia Identity Before Selection

- The earlier compact audit correctly rejected the only exact canonical route:
  ATNF J0437-4715 resolves to an ordinary K-spectrum Gaia leaf representing its
  optical companion. That proved canonical scope was unsafe, but leaving every
  ATNF/McGill object evidence-only prevented legitimate source facts from
  participating in the selected-fact architecture.
- Legacy `core.compact_objects` keys are not reusable permanent identities. Six
  collision pairs erase the `+`/`-` sign in coordinate names. E5 compact policy
  v1 instead preserves ATNF signs in `compact:atnf:name:*` keys and hashes
  normalized McGill source names. ATNF's own PSRJ claims reconcile 4,482 source
  names into 4,394 physical release identities; aliases remain evidence.
- Build `f0d7273f65371efeda365611` contains 4,425 identities and outcomes for
  every one: 22 accepted by source-distance interval overlap with the 1,250-ly
  evidence envelope, 421 excluded, and 3,982 missing usable distance evidence.
  It selects 156 source-backed timing, astrometric, and dispersion facts for
  accepted objects and keeps J0437-4715 as a candidate-counterpart quarantine.
- Two compiles produce the same build ID and ordered Parquet hashes. An
  independent 34-check verifier passes. E6 v7 copies all six compact projection
  tables into ARM while preserving exact canonical system, star, planet, and
  hierarchy inventory. No named-object transform or positional identity guess
  is introduced.
- E6 v7 clean reproduction, 373-check independent audit, scientific A/B, public
  slice, exact four-radius tile coverage, bounded simulation scenes, and
  API/search gates pass. Its parsed tile payload arrays equal the
  production-browser-tested v6 candidate. A fail-closed dry-run identifies only
  the superseded v5 shadow as an 18.58-GB retention candidate; no artifact is
  deleted, and the separate v5 public product remains outside that contract.

### 122) Completion Audits Must Separate Verified Evidence from Cutover

- A live E0 schema audit found the checked-in baseline lagged the already
  reviewed JPL Horizons complete-elements snapshots. The corrected baseline
  accounts 148 active artifacts and 6,227 fields; the 18-field delta is nine
  orbital-element fields in each of the authority and artificial-object tables.
  Registry audit passes all 47 sources with four retained superseded artifacts.
- The v7 legacy-derivation audit also exposed ten unowned classification bases
  from selected facts, exact MSC components, canonical object type, and the
  explicitly labeled residual stability fallback. The inventory now owns those
  markers and again passes with zero unaccounted materialized/source markers;
  no algorithm or finding was suppressed.
- `config/evidence_lake/e0_e7_acceptance.json` and
  `scripts/audit_evidence_lake_completion.py` pin the accepted reports and
  immutable artifacts. The first audit passes 23 checkpoint checks but reports
  `incomplete` because six E7 gates remain open or blocked. This prevents a
  scientifically sound shadow candidate from being mislabeled as the sole clean
  production compiler.
- The contemporary storage audit reports about 70.6 GiB free and refuses new
  acquisition. Candidate, rollback, raw, typed, E4, and report artifacts remain
  protected; no ambiguous cleanup or additional large build is authorized.

### 123) Permanent Hierarchy Is an Identity Seed, Not Scientific Authority

- E7 now exports the reviewed canonical hierarchy as permanent identity seed
  `5c878083872c738415971864`. Its two Parquet products contain 11,759,440 nodes
  and 5,886,947 containment/component relationships while explicitly
  prohibiting mass, distance, position, temperature, spectral, orbital, and
  other scientific scalar columns.
- The first full export exposed 5,092 reused numeric edge IDs with 6,936
  collision rows. Relationship tuples themselves were unique. The accepted
  compiler deterministically rekeys complete relationship identities and keeps
  the old ID only as migration lineage, producing zero duplicate nodes, output
  edge IDs, relationships, missing endpoints, or canonical objects without a
  hierarchy node.
- Production compilation takes 30.48 seconds and produces 402.2 MiB. An
  isolated USB-backed reproduction takes 30.63 seconds, matches both Parquet
  hashes and sizes, and removes scratch. Future authoritative compilers may read
  this retained identity seed but may not reopen stability databases for
  identity or scientific values.

### 124) Selected System Placement Must Be Evidence-Backed and Measured

- Build `22e9a59dd02484454a629df7` places all 5,869,091 permanent systems exactly
  once without reading stability CORE/ARM science: 5,866,306 selected-star
  placements, 2,723 coherent MSC component placements, ten SBX target-context
  placements, 51 UltracoolSheet system-context placements, and the defined Sol
  origin. Every fallback remains explicitly scoped and carries evidence or
  derivation lineage.
- The compiler consumes the E5 per-quantity Parquet contract and verifies each
  file's bytes and hash against the parent selected-fact manifest. Its own
  content identity pins policy, compiler source, all transitive input bytes,
  and input attestation. An independent verifier reports zero inventory,
  duplicate, coordinate, Cartesian, source-count, or lineage failures.
- A 103.10-second, 26.16-GiB baseline exposed repeated broad scans and winner
  materialization. The accepted general optimization reduces production to
  63.24 seconds and 17.42 GiB. An intermediate
  84.91-second materialized-winner variant is rejected. Isolated compilation
  plus independent audit takes 71.18 seconds, matches both ordered Parquet
  hashes, and removes scratch.
- Final lineage review also caught a provisional SBX release label and a false
  uniform J2016 epoch. The compiler now joins the registered SBX release and
  retains source J1991.25, J2000, or J2016 position epochs. The correction
  changes metadata for ten rows but no geometry, representative object,
  placement winner, evidence ID, or derivation.
- These measurements begin, but do not complete, the E7 end-to-end timing
  report. The final report must include every compiler, verifier, shadow build,
  promotion, rollback, and re-promotion step with explicit cache state and
  ranked before/after optimization evidence.

### 125) Stability Cutover Requires Table-Level Ownership

- `e7_stability_table_migration.json` and its independent audit checksum and
  enumerate every one of the 74 tables in stability CORE, ARM, hierarchy, and
  DISC. No table is implicitly inherited: six table owners are verified
  artifacts, eight zero-row compatibility tables are explicit retirements, 53
  require clean compilation, and seven infrared tables remain one bounded clean
  projection blocker.
- The audit deliberately reports `incomplete` with 60 open replacements while
  passing all accounting gates. This separates complete migration planning from
  actual clean cutover and prevents a copied compatibility database from being
  mislabeled as Evidence Lake output.
- Permanent vocabulary seed `6b4fb210e1b1bcf61299fe7f` closes the alias
  migration requirement. It maps all 1,026,480 aliases to permanent object and
  system keys, emits no scientific scalars or legacy numeric target IDs, passes
  independent audit, and reproduces its 25.7-MiB Parquet hash in isolated USB
  scratch.
- A fail-closed retention refresh discovered and required acknowledgement of a
  newly added storage-report reference before proceeding. Exact set hash
  `d057da2886af4fbf19aee615d4600328b74793bac22eec2edd0263c2f5f9edf6`
  then retired only the unserved superseded v5 shadow, reclaimed
  18,582,962,176 allocated bytes, and preserved its public artifact, all seven
  reports, verified v7, rollback state, and source evidence.

### 126) Clean Inventory and Search No Longer Depend on Stability Databases

- Build `9c2d08086275ead386f71bf7` compiles permanent inventory, selected system
  placement, aliases, release-scoped identifiers, quarantine, search terms, and
  canonical hierarchy from five checksum-pinned clean artifacts. The compiler
  opens no stability database and marks identity migration inputs as
  non-scientific authority.
- Exact inventory is 5,869,091 systems, 5,874,636 stars, and 6,311 planets.
  The build emits 12,768,410 deduplicated search terms and preserves all
  6,669,279 accepted identifier bindings. It explicitly accounts 3,485 planets
  without a canonical system binding and 3,489 without a host-star binding;
  no unmatched planet is discarded or assigned an invented host.
- Production takes 68.23 seconds at 17.27 GiB peak RSS. Isolated compilation
  plus independent verification takes 74.63 seconds, matches every canonical
  Parquet hash, and removes scratch. DuckDB query-database containers are
  verified by logical schema/count/invariant checks because their internal page
  layout is not a deterministic serialization.

### 127) Selected Science and Display Classes Leave Stability CORE

- Clean science build `35eb29fa3b2a3ac518f5303a` reads only clean foundation
  `9c2d08086275ead386f71bf7` and eight accepted E5 artifacts. It copies typed
  domain projections and materializes shared stellar/planet selected-fact
  surfaces without copying any stability scientific value.
- All 5,874,636 stars receive one display classification. Direct selected
  optical/infrared/SIMBAD evidence wins, followed by labeled temperature,
  color, or mass presentation derivation, then explicit `UNKNOWN`. Counts are
  321,425 source, 2,033,572 derived, 3,176,113 assumed, and 343,526 missing;
  every non-missing output carries a selected-fact ID.
- Production takes 190.81 seconds at 37.45 GiB peak RSS. The isolated rebuild
  matches all nine canonical Parquet products, passes independent logical
  verification, and removes scratch. Its 165.23-second compiler time reflects
  shared cache state and is not claimed as an optimization.
- The stability ledger now distinguishes 30 verified selected-science
  replacements whose runtime/API compatibility cutover is pending from 23
  unbuilt clean replacements and seven infrared blockers. All 60 remain open
  until their consumer or compiler gate closes.

### 128) Targeted WISE Must Preserve Raw Responses and Negative Outcomes

- Audit found the original targeted WISE path retained derived CSVs and query
  metadata but not byte-identical IRSA responses. Those products remain useful
  stability evidence but cannot be clean compiler inputs.
- Release `spacegate_targeted_500_20260722_v1` replaces hard-coded in-code
  golden priority with a versioned quota policy and a separately inspectable
  operator evidence seed. It preserves 1,000 exact CatWISE2020/AllWISE response
  members, their URLs/checksums, target-coordinate lineage, and all source
  errors without bulk-mirroring either catalog.
- Seven CatWISE 30-arcsec cones hit IRSA density limits. The error responses are
  retained and declared 10/3-arcsec fallbacks recover them. A first failed run
  also exposed executor behavior that hid one failure until all scheduled work
  completed; the collector now accumulates every failed member and resumes only
  missing responses.
- Source-native typing carries the response-member filename into each row and
  reproduces 2,868 CatWISE plus 1,536 AllWISE rows. Clean projection
  `ec8e218402c3a4a3b55b2811` emits complete target accounting, permits only one
  unique-nearest accepted target per source, retains ambiguous/collision/
  excluded evidence, keeps CatWISE parallax-like values candidate-only, and
  creates no CORE inventory.
- Pinned target-set reuse reduced the warm acquisition check from 5.77 seconds,
  102.9 CPU-seconds, and 7.06 GiB RSS to 0.32 seconds, 2.41 CPU-seconds, and 91
  MiB. The stability ledger now marks all seven infrared tables as verified
  artifacts pending consumer cutover, not clean-projection blockers; the total
  open replacement count remains 60 until serving is migrated.

### 129) Extended-Object Identity Must Not Carry Scientific Placement

- The original extended-object CORE combined permanent public identity with
  selected geometry and distance. Copying that table into a clean compiler
  would make the stability database an undeclared scientific authority.
- Seed `555fa1890943b97dd0e4ef3d` instead preserves only 18,277 IDs/keys and
  name/type scope, 71,855 aliases, 16,726 identifiers, 21,339 reconciliation
  outcomes, and 404 quarantine decisions. Sky coordinates, angular geometry,
  distance, and Cartesian placement are prohibited by its machine policy.
- Independent audit and clean-scratch reproduction match all five Parquet
  products exactly. The stability ledger now has 56 open replacements rather
  than 60; selected extended-object geometry and distance remain deliberately
  open for an evidence-only compiler.

### 130) Geometry Can Cut Over Before Extended-Object Distance

- Clean build `a4b521d1e1de52e14afac0da` normalizes 18,110 accepted E5 geometry
  rows and preserves all candidates before selecting 16,612 canonical contexts.
  All 16,606 shared non-null coordinates equal the stability projection exactly.
- The missing geometry tail is exactly 1,665 Cantat-Gaudin-only cluster rows.
  The distance tail is 1,867 cluster distances plus 35 associated-star
  distances. They remain null rather than treating a stability projection as
  evidence or bypassing ambiguous current cluster/relation bindings.
- Row-wise DuckDB insertion made the first implementation take 36.77 seconds
  and caused about 188,000 voluntary context switches. A single Arrow batch
  reduces accepted compile time to 9.71 seconds and CPU time by 64.9% while
  reproducing every canonical Parquet hash. The rejected baseline remains an
  exact-candidate retention item.

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
