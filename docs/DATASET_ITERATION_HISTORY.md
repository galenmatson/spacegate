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
