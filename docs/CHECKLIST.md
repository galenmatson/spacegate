# Spacegate Delivery Checklist (Gaia-First)

This checklist tracks implementation against `docs/PROJECT.md` and the Gaia-first program direction.

## A) Platform Foundations

- [x] Core/arm/disc/rim layer model documented
- [x] Immutable build output model (`out/<build_id>`, promoted `served/current`)
- [x] Deterministic `download -> cook -> ingest -> promote -> verify` scripts
- [x] Arm graph artifact emitted during ingest (`arm.duckdb`) with report (`arm_report.json`)
- [x] Promote gate requires both `core.duckdb` and `arm.duckdb`
- [x] Provenance gate and QC report emission
- [x] Dataset iteration history documented (`docs/DATASET_ITERATION_HISTORY.md`)
- [x] Admin control plane + audit trail baseline
- [x] Admin dataset status panel (runtime/storage/quality metrics) for performance diagnostics
- [x] Admin dataset slicer panel (preview + policy-driven sliced rebuild action)
- [x] Catalog contribution + overlap report generation and Admin Dataset visualization
- [x] Admin v2 Runtime workspace with redacted config/secret diagnostics
- [x] Admin v2 Object Diagnostics workspace with readiness, provenance,
  graph/orbit, presentation, and simulation inspection
- [x] Admin v2 Operations/Jobs/Audit workspace with job timeline, log reader,
  correlated audit events, retention dry-run/apply safeguards, and actor
  attribution
- [x] Admin v2 Agency source allowlist management with source enable/disable,
  runtime JSON overrides, shipped-default restore, and previous-version restore
- [x] Coolness scoring pipeline with versioned profiles
- [x] Snapshot generation pipeline baseline

## B) Current Production Stability

- [x] Public deployment path active and reproducible
- [x] Promotion flow auto-scores coolness by default
- [x] Multiplicity support catalogs wired (NSS, WDS, ORB6, MSC mandatory)
- [x] Eclipsing support catalogs wired (DEBCat + TESS EB default-on; Kepler EB optional default-off)
- [x] Catalog contribution evaluator emits ranked tiered report (`scripts/evaluate_catalog_contribution.sh` + `catalog_eval`)
- [x] Optional WDS-Gaia bridge wired and default-off
- [x] Physical consistency gating on WDS bridge grouping
- [x] Proximity grouping remains nondefault for conservative production builds
- [x] Make the map's cosmetic grid overlay independently toggleable across all
  themes and persist the preference locally
- [x] Remove redundant detail and search work from map Peek startup: reuse the
  simulation-scene payload for star/planet source tooltips and defer the
  coolness-component search until the COOL tooltip is actually inspected
- [x] Add shared build-keyed compressed runtime simulation-scene caching and
  same-scene cold-request coalescing, then deploy the code-only checkpoint to
  antiproton while preserving served build
  `20260715T015659Z_e392a11_side_rebuild`; public integration, known-system,
  auth/health, and desktop map Peek checks pass
- [x] Align public/admin Docker frontend build stages with Node 22 so current
  `camera-controls` engine requirements are satisfied
- [x] Restore build-bound priority simulation-scene materialization in the
  promoted deep-map artifact pipeline; public build
  `20260716T0103Z_94bdab7_side` carries 1,000/1,000 verified priority scenes
  with zero materialization failures
- [x] Install all checksummed reports advertised by edge `current.json` into a
  staged `$SPACEGATE_STATE_DIR/reports/<build_id>` directory during bootstrap
- [x] Define and publish a slice-native QC/provenance report set for immutable
  presentation builds so antiproton can require reports without relaxed
  verification (`derived_build_verification_report.json` recomputes live slice
  integrity and hashes upstream report lineage without relabeling full-build
  reports whose counts and build IDs do not match the slice)
- [ ] Add a cheap map-selected singleton scene path plus cache telemetry and
  explicit per-process/concurrent-assembly budgets before planning for hundreds
  of concurrent exploratory users; the shared persistent cache and same-scene
  request coalescing are complete
- [ ] Reproduce and capture the remaining client-side 1,000-ly crash observed
  after rendering more than 100,000 points near the sphere edge; distinguish
  WebGL context/process failure from JavaScript heap or tile lifecycle growth

### B1. Current Data and Identity Regression Inbox

- [x] Move the Explorer selected-object readout to the lower-right by default
  and add plain-English, source-aware hover/focus explanations for distance,
  bound-star counts, planet detection, coolness contributions, and rank
- [x] Correct Nu Scorpii's simulation class presentation: hierarchy/header
  chips now show three source-backed B classifications and four unknowns;
  `mass_main_sequence_prior_v1` still colors three simulated bodies but is
  disclosed only as an assumed visual prior in their object readouts, with a
  provenance-aware browser golden
- [x] Normalize the System Page site header and search row with Star Search,
  and align System Simulation habitable-zone disks from reliable host-planet
  ecliptic evidence or the star's most local rendered parent orbit; assumed
  planet visualization priors no longer override multistar parent planes

- [ ] Investigate and repair AR Cassiopeiae / AR Cas / HD 221253 as one
  consistently searchable system with seven accepted stellar members; explain
  every additional current member, keep the documented background component E
  excluded, and add core, ARM hierarchy, API, browser-search, and simulation
  count goldens so multiplicity cannot silently return to ten
- [ ] Repair W Ursae Majoris / W UMa / HD 83950 identity and public naming so
  all three names resolve to the same contact-binary system, with the variable-
  star designation eligible for preferred public display and the matched
  component/focus context preserved
- [ ] Replace one-off common-name repairs with a reproducible alias-authority
  pipeline: evaluate SIMBAD as the broad identifier graph, GCVS/VSX for
  variable-star designations, and IAU WGSN for approved proper names; record
  provenance, alias type and scope, deterministic display precedence,
  collisions/quarantine, coverage deltas, and golden lookups before promotion
- [ ] Repair the local cooked Gaia backbone CSV row-boundary corruption found
  near line 193,309 during the TESS T2 audit; re-fetch affected partitions,
  run strict CSV parsing plus TAP row-count/hash completeness gates, and verify
  the next full build does not rely on ignored malformed rows
- [ ] Reconcile the Castor known-system API expectation with the canonical
  hierarchy: the July 13 public checkpoint preserves six source stellar leaves
  and three pairs, but the verifier receives rendered subsystem labels
  `Castor A/B/C` rather than the expected `Castor AB`; keep this non-blocking
  unless the underlying hierarchy or simulation is incorrect

## C) Gaia-First Migration Program

### C1. Architecture and Contract

- [x] Gaia-first architecture decision documented in `PROJECT.md`
- [x] Core schema contract rewritten for Gaia-first canonical inventory
- [x] Data source policy rewritten with canonical/auxiliary/transitional classes
- [x] Long-range milestone roadmap restored in `MILESTONES.md` and dependency-ordered
- [x] Slice profile catalog + draft SLO targets documented in `SLICE_PROFILES.md`
- [x] Build metadata persists `slice_profile_id` + `slice_profile_version` on sliced builds
- [x] Arm artifact contract and promotion rules documented (`arm.duckdb`)
- [x] API contract review completed for Gaia-first field semantics

### C2. Phase A - Gaia Backbone Pilot

- [x] Implement Gaia backbone downloader/cooker/ingest path (`gaia_backbone`)
- [x] Emit `gaia_backbone_report.json` (counts, quality bands, runtime, storage)
- [x] Add Gaia TAP fetch completeness guard (detect and fail on sync truncation / partial bucket responses)
- [x] Add build metadata for astrometry quality policy and boundary strategy
- [x] Verify deterministic reruns for pinned Gaia inputs
- [x] Ingest Gaia DR3 astrophysical classification probabilities needed for remnant safety (`classprob_dsc_*_whitedwarf`, ESP-ELS families)
- [x] Implement remnant classification invariant gate (explicit remnant evidence must override temperature fallback)
- [x] Add compact/superstellar side catalogs (ATNF pulsars, McGill magnetars, open clusters, Galactic SNR)
- [x] Tag Gaia stars with open-cluster memberships where catalog IDs overlap (probability thresholded)

### C3. Phase B - Core Product Slice

- [x] Define deterministic `core_product_slice` policy over backbone
- [x] Retire the old Galaxy/Halo complement plan from active docs/Admin surface
- [x] Slice public side artifacts (`arm.duckdb`, `canonical_hierarchy.duckdb`,
  and `disc.duckdb`) with the public core profile so antiproton does not carry
  the full Photon working graph
- [ ] Add guarded Admin promote/rollback controls for available immutable build
  directories
- [x] Enforce profile-specific SLO gates during promote
- [x] Validate p95/p99 search and detail latency on proton

### C4. Phase C - Multiplicity Reintegration on Gaia IDs

- [ ] Reattach NSS evidence against Gaia backbone IDs
- [ ] Reattach MSC (mandatory) against Gaia backbone IDs
- [ ] Reattach WDS/ORB6 evidence against Gaia backbone IDs
- [x] Materialize Gaia NSS unresolved-companion orbital evidence in `arm.orbital_solutions`
- [x] Materialize WDS/MSC observational detail tables in `arm` for narration/context
- [x] Materialize safely-mapped ORB6 rows in `arm.orbital_solutions`
- [x] Reattach SBX spectroscopic-binary evidence against Gaia/HIP/HD IDs (default-on toggle: `SPACEGATE_ENABLE_SBX`)
- [x] Ingest pinned CDS SB9 systems/aliases/orbits as default-on ARM evidence;
  bind component spectral types only through exact unique MSC sequence
  references and quarantine unresolved endpoints
- [x] Reconcile DEBCat component spectral evidence through unique canonical
  system + period + endpoint matches
- [ ] Evaluate Skiff and VSX composite spectral classifications for
  component-scoped corroboration; require exact identifiers/unique period and
  role binding before endpoint propagation
- [x] Add a production-transform literal audit and remove the default executable
  accepted-supplement object list; preserve former cases for AAA adjudication
- [ ] Implement/verify hierarchy confidence tiers
- [x] Draft golden-system multiplicity exam harness (`docs/MULTIPLICITY_GOLDENS.md`, `scripts/verify_multiplicity_goldens.py`, Castor fixture)
- [x] Verify path runs multiplicity goldens by default with required arm tables (`--require-arm`)
- [x] Benchmark system validation set passes (Castor, Nu Sco, Alpha Centauri,
  Sirius, TRAPPIST-1, 55 Cnc, Sol, and 16 Cyg via
  `scripts/verify_known_systems_api.py`)
- [ ] Expand multiplicity goldens with AR Cas / HD 221253 and Nu Scorpii / HD 145502; keep HD 235299 and Gamma Cas as adjudication/watchlist systems until evidence policy is explicit
- [x] Suppress singleton MSC leaf inference in canonical hierarchy to avoid Bet Mon-style overfit

### C5. Phase D - Crosswalk and Naming

- [x] Implement replacement crosswalks for names/aliases/legacy IDs
- [x] Restore broad cross-catalog identifier coverage in served rows (Gaia/HIP/HD/WDS and other selected major IDs)
- [x] Add deterministic identifier reconciliation with ambiguity quarantine + QC collision gates
- [x] Add duplicate-trap stewardship report (exact-key + near-pair checks) with optional QC high-confidence gate
- [x] Materialize narration-oriented `arm.stellar_parameters` rows from Gaia DR3 + NASA host-star payloads
- [x] Materialize deterministic source-input physical derivations in `arm.derived_physical_parameters`
- [x] Materialize `system_search_terms` and system-level browse/search facet columns for public-host performance
- [x] Use generic `arm` hierarchy payloads in detail/search paths and descendant-aware star counts for multiplicity-heavy systems
- [ ] Add common-name authority merge policy (SIMBAD identifier graph,
  GCVS/VSX variable designations, IAU WGSN proper names; precedence, scope,
  dedupe, provenance, and conflict handling)
- [ ] Add benchmarked common-name fuzzy matching for bright/common objects (for example Aldebaran-class lookups) with alias-aware ranking
- [x] Add alias-scope and preferred-display-name authority v2 so Gliese/GJ
  names, expanded Bayer names, common names, member aliases, and system aliases
  resolve consistently across Star Search, 3D Map labels, and system pages
  - [x] Verify Gliese 412 / GJ 412, Gliese 643, Alpha Librae /
    Zubenelgenubi, V1513 Cyg false-positive guard, and Alpha/Proxima
    member-context searches with `scripts/verify_alias_authority.py`
  - [x] Preserve matched alias/member context in Star Search API responses
    without forcing catalog IDs or abbreviated Bayer labels into public titles
- [x] Add configurable public display-name styles (`Public Full`,
  `Astronomer Abbrev`, `Catalog Compact`, `Source/Technical`) across Star
  Search, 3D Map, System Peek/Explorer, system detail, and simulation-scene API
  payloads
  - [x] Add `scripts/verify_name_style_policy.py` for Alpha Centauri,
    Epsilon Indi, Mu Herculis, Sirius, Gliese 412, and 55 Cnc name-style
    goldens
- [ ] Optimize alias-table materialization for future full rebuilds; Alias
  Authority v2 correctness build showed the alias stage taking about 34 minutes,
  likely from repeated Gl/GJ variant expansion and large dedupe/sort work
- [ ] Add curated catalog-ID linkout registry with full-ID copy controls,
  destination-specific resolver/search links, and build/admin validation so
  only useful external reference pills appear in public UI
- [ ] Add bright-star/compact-companion reconciliation gates so systems such as
  Sirius cannot bind primary-star aliases and HIP/HD identifiers only to the
  Gaia white-dwarf companion while the bright primary is absent
- [x] Add AT-HYG alias-crosswalk guard preventing non-compact positional alias
  rows from attaching to compact-object/white-dwarf Gaia targets or promoting
  weak positional matches into HIP/HD/HR/GL/TYC/HYG identifiers
- [x] Add compact-alias safety verifier and `verify_build.sh` hook to detect
  Sirius-class compact-object rows carrying bright-primary aliases; keep it
  warn-only until rebuilt artifacts are clean, then enable strict verification
  with `SPACEGATE_VERIFY_COMPACT_ALIAS_SAFETY=1`
- [x] Add reviewed accepted-supplement config/ingest path for Gaia-missing
  canonical inventory exceptions; seed Sirius A and a reviewed Sirius B WDS
  component link (historical; default promotion retired July 15, 2026)
- [x] Rebuild core/ARM with accepted supplements enabled; historical local build
  `20260630T_sim_beta_data_foundation` verifies Sirius as a WDS-backed A/B
  system and passes strict compact-alias safety
- [x] Replace the retired Sirius A/B supplement with general source rules:
  exact unique HIP+HD SBX/AT-HYG recovery for the Gaia-missing primary plus
  projected-J2016 WDS/Gaia companion reconciliation
- [ ] Adjudicate the remaining L 134-80 case through the inspectable AAA/human
  contract; do not restore it through an executable object-specific supplement
- [x] Materialize the direct AT-HYG Gaia alias guard; strict compact-alias
  safety passes on `20260630T_sim_beta_data_foundation`
- [x] Materialize the MSC source-leaf fix; Nu Sco exposes seven source-native
  MSC stellar leaves from ARM rather than relying on `core.systems.star_count`
- [x] Rebuild after planet stable-key de-duplication; local served build
  `20260630T_sim_beta_api_alias_v4` has unique
  `core.planets.stable_object_key` rows and passes
  `verify_orbital_normalization.py`
- [x] Restore accepted-supplement AT-HYG aliases for no-Gaia reviewed rows;
  Sirius resolves through `Sirius`, `Alpha Canis Majoris`, `Alp CMa`, `9 CMa`,
  HIP 32349, HD 48915, and WDS 06451-1643 on
  `20260630T_sim_beta_api_alias_v4`
- [x] Let member-star names participate in search result display/ranking so variable-star lookups like `AR Cas` title the correct system card
- [x] Add authoritative Sol-system bootstrap overlay (Sun + major planets + canonical aliases) so Sol is present and complete even when external catalogs are incomplete
- [x] Implement Sol S2 arm hierarchy (moon nodes, satellite orbit edges, Earth-Moon/Pluto-Charon barycenters) with verify gates
- [x] Align Sol canonical class storage to source-faithful `dwarf_planet` semantics while preserving UI structural supergroup `subplanet`
- [x] Implement Sol S3 initial named small-body arm layer (asteroid/TNO/comet families) with staleness metadata + verify gate
- [x] Expand Sol S3 deterministic small-body coverage (broader asteroid/TNO families, including Ixion-class objects)
- [x] Retire Sol S3 arm-to-halo projection path from the active product plan
- [x] Disambiguate Sol authority Horizons small-body commands and add
  source/build/API gates so Ceres/Vesta-class rows cannot resolve to major
  planets or satellites
- [x] Implement Sol S4 arm layer for curated artificial stations/probes/orbiters with verify gates
- [x] Surface Sol arm hierarchy overlays (moons, small bodies, artificial objects) in system detail API/UI
- [x] Add scheduled volatile Sol refresh + staleness monitoring scripts (`refresh_sol_volatile.sh`, `report_sol_volatile.py`)
- [ ] Preserve or improve host matching quality for planets
- [x] Promote exoplanet host labels for Gaia-fallback star/system display names (TRAPPIST/Kepler/TOI/WASP family and peers)
- [x] Preserve or improve user-facing lookup ergonomics
- [ ] Evaluate CCDM as a secondary alias/component-evidence source for Hipparcos-era identifiers and historical subsystem labels (not a primary multiplicity authority)
- [ ] Ingest ADS identifiers as historical aliases/cross-references only, not as a primary structure source

### C5.3 Phase D.5 - Exoplanet Multi-Catalog Lifecycle

- [x] Add lifecycle source downloads/manifests (exoplanet.eu, OEC, HWC)
- [x] Add per-source manifest snapshot diff report (`reports/source_delta_report.json`) with persisted baseline snapshot/history
- [x] Add impacted-row planner report (`reports/impacted_rows_plan.json`) with mode routing
- [x] Add cooked lifecycle normalization outputs (status/alias/feature rows)
- [x] Resolve per-planet lifecycle status with deterministic precedence (`confirmed/candidate/controversial/retracted`)
- [x] Materialize policy flags (`is_default_visible`, `is_tombstoned`) in `planets`
- [x] Persist lifecycle observations/history/reclassification audit tables in `arm.duckdb`
- [x] Emit `planet_catalog_delta_report.json` and `planet_reclassification_report.json`
- [x] Add verify gate for stale lifecycle/taxonomy/habitability classifier versions
- [x] Add selective cook + incremental planet ingest path for planet/lifecycle-only source deltas
- [x] Add one-command orchestrator for differential/full refresh routing (`scripts/refresh_core.sh`)

### C5.5 Phase D.6 - Planet Taxonomy and Habitability

- [ ] Implement deterministic taxonomy tags from observational data (size/mass, insolation, orbit, composition proxy, detection/host context)
- [ ] Add educational concept pages for public discovery tags with plain-language explanations, deeper science sections, representative systems, related concepts, Star Search "Find more" links, and interactive visualizations where useful
- [x] Create `docs/CONCEPTS.md` with the public concept-page route, page contract,
  first-page priorities, and science-education backlog
- [ ] Audit all public pills, chips, compact metrics, and badges for missing
  tooltips/popovers; add helpful exposition, assumption detail popups, and
  concept-page hooks where useful
- [ ] Define tag priority tiers for compact, normal, and expanded UI surfaces
  so the tagging system can be comprehensive without cluttering tight layouts
- [x] Implement `spacegate_hab_score` + confidence/reasons metadata
- [x] Implement element-richness proxy tags from stellar spectroscopy/metallicity inputs
- [ ] Add API filters/toggles for controversial lifecycle state and habitability range
- [ ] Add UI habitability slider + top-N shortcut
- [ ] Add optional filter/display for element-richness class
- [x] Investigate planet equilibrium-temperature coverage gaps surfaced in
  Admin Object Diagnostics; missing `eq_temp_k` should be distinguished between
  unavailable source data and derivable-but-not-materialized estimates.
- [x] Add classifier drift/regression checks in build verification

### C5.6 Phase D.7 - TESS Identity and Evidence

- [x] Snapshot and cook the NASA Exoplanet Archive TOI table with manifest,
  dispositions, transit fields, and source-delta reporting
- [x] Build the targeted TIC-ID universe from TOI, NASA planet hosts, TESS EB,
  and reviewed operator/AAA requests without bulk-ingesting TIC
- [x] Reconcile TIC Gaia DR2 identifiers through Gaia DR3 `dr2_neighbourhood`,
  then alternate exact identifiers and conservative astrometric matching
- [x] Materialize accepted TIC/TOI identifiers into object identifiers,
  aliases, and focus-aware system search terms
- [x] Emit the TESS missing-real-object audit and recover only reviewed,
  in-scope objects that pass duplicate, provenance, astrometry, and hierarchy
  gates
- [x] Materialize TOI candidates, dispositions, transit evidence, and history
  in ARM without creating unreviewed canonical planets
- [x] Add exact TIC/TOI API/search goldens and candidate/false-positive leakage
  gates
- [ ] Index targeted MAST observation products and external links without bulk
  light-curve, TCE, target-pixel, or FFI downloads
- [ ] Add the minimal public/admin TESS evidence surface; defer full light-curve
  storytelling to its reviewed presentation contract
- [x] Keep the TESS side quest bounded according to `docs/TESS_INTEGRATION.md`
  exit rules so it does not hold the Tiled Deep Map milestone

### C6. Phase E - AT-HYG Retirement

- [x] AT-HYG retirement comparison report generated (`reports/<build_id>/athyg_retirement_report.json`)
- [x] AT-HYG removed from canonical inventory path
- [x] AT-HYG remaining usage limited to optional compatibility tooling or removed entirely
- [x] Deprecation cleanup completed for default Gaia-first path (compatibility tooling retained behind explicit toggles)

## D) Security and Data Hygiene

- [x] No default production dependency on insecure transport sources
- [x] Source mirror strategy documented for fragile external dependencies
- [x] Retention policy documented (builds, reports, backups, large catalogs)
- [x] Automated stale build cleanup policy scripted and tested (`scripts/prune_state_retention.sh`)
- [x] Accept both minute- and second-resolution build IDs in retention;
  regression-test active-build protection and apply the corrected Photon
  12-build/24-report policy
- [x] Photon bootstrap/readiness audit scripted (`scripts/audit_photon_bootstrap.sh`)
- [x] Logged database build wrapper preserves build exit status and host state-dir environment (`scripts/build_database_logged.sh`)
- [x] Photon generous retention and bulk research storage policy documented (`/mnt/space/spacegate`)
- [x] Public antiproton deployment runbook documents sliced DB publish, activation, SSH cooldown, verification, and rollback (`docs/PUBLIC_DEPLOYMENT.md`)
- [x] API Docker runtime runs non-root with generated-state permission normalization
- [x] API Docker runtime drops capabilities, blocks privilege escalation, and uses a read-only root filesystem with explicit tmpfs scratch mounts
- [x] Create host-local private security audit log outside the public repo at `/srv/spacegate/private/security/SECURITY_AUDIT.md`
- [ ] Dedicated `spacegate-run` service user and shared group model for Admin/API runtime
- [ ] Antiproton runtime identity cleanup: migrate `/srv/spacegate/data` from legacy `ubuntu:ubuntu` ownership to a dedicated non-login `spacegate` runtime user with shared `spacegate` group access; keep `sgdeploy` as deploy/restart account only
- [ ] Reassess antiproton `sgdeploy` Docker-group membership and replace with a narrower deploy control path if feasible
- [ ] Public-edge Admin route hardening: reverse-proxy gate `/admin` and
  `/api/v2/admin/*` with VPN/Tailscale, IP allowlist, or basic auth; optional
  obscure Admin path only as bot-noise reduction
- [ ] Secret handling hardening beyond Compose-expanded environment variables
- [ ] Redacted Compose/runtime diagnostics command so operators can inspect
  configuration without printing secret-bearing environment values
- [ ] Pre-hardening/pass-close git security scan routine with OpenAI's git
  security inspector or an equivalent local secret scanner
- [ ] AI Astronomy Agency prompt-injection hardening fixture set
- [ ] Agent source-text isolation and tool-boundary enforcement tests
- [ ] Agent publication gate requiring reviewed citations, explicit claim subjects,
  and verdict state before public `disc` materialization

## E) Product Roadmap (Post-Core Migration)

- [x] Public web upgraded to React 19 for the 3D map runtime baseline
- [x] Admin v2 architecture documented (`docs/ADMIN_V2.md`)
- [x] Admin v2 React/Vite shell scaffolded at `/admin/`
- [x] Admin v2 React Overview page with status/jobs/inference health summary
- [x] Admin v2 Operations/Jobs/Audit workspace design documented
- [x] Admin v2 React Operations workspace first pass (runbook actions, jobs/logs, backups, audit filters)
- [x] Admin v2 backend operation metadata and `/admin/operations/status` API
- [x] Admin v2 React Builds workspace first pass (build state, runbook, artifacts, retention readiness)
- [x] Admin v2 React Dataset workspace first pass (science shape, source contribution, quality, runtime)
- [x] Admin v2 React Runtime workspace first pass (paths, storage, auth, config, process, endpoint diagnostics)
- [x] Admin v2 React Agency workspace first pass (portfolio flow, storage readiness, eval anomalies, interaction model)
- [x] Admin v2 Object Diagnostics first pass with system dossiers, member/component focus diagnostics, coolness contribution explanation, and read-only relation diagram
- [x] Admin v2 Object Diagnostics generalized search over arm components by common name, component key, and catalog id, returning the owning system dossier with focused component selection
- [x] Admin v2 Object Diagnostics polish pass with grouped arm-component navigation, clearer readiness cause/next-action guidance, and richer nested/link value rendering
- [x] Admin v2 Object Diagnostics Simulation tab with source/derived/assumed/missing field labeling and replacement-target guidance
- [x] Evidence Portfolio admin persistence schema and read API baseline
- [x] Evidence Portfolio seed workflow from current coolness-ranked systems with first journal entry
- [ ] Retire embedded FastAPI Admin UI after React workspace parity
- [x] Admin v2 dynamic inference endpoint registry (add/remove endpoints, auth modes, model polling, usage stats API)
- [x] Admin v2 Inference workspace role/model routing and bounded smoke tests
- [x] Admin v2 Inference workspace eval report history
- [x] Evidence Portfolio journal persistence schema for plain-language agent step history
- [ ] External reference link pipeline (curated authority sources)
- [ ] Factsheets + exposition generation with confidence metadata
- [ ] Object-scoped coolness ranking for systems, stars, and planets to drive enrichment/adjudication queues
- [ ] Fold stellar gigantism into coolness/search ranking so giants and supergiants are not buried with ordinary stars
- [ ] Retool CoolStars coolness scoring for the larger public database with explainable weighted signals, operator-visible contribution breakdowns, and reviewed profile presets
- [ ] Add correlated-signal controls and ranking goldens to the Coolness Retool:
  WISE J085510.83-071442.5 is currently rank 7 because Y-dwarf rarity,
  7.43 ly proximity, and extreme proper motion stack 27.14 of its 30.36
  points; decide explicitly whether that scientific interest merits the public
  browse rank or needs a redundancy cap/profile adjustment
- [x] Canonical ingest design doc + deterministic adjudication queue baseline for sloppy-system triage
- [x] Canonical ingest artifacts for normalized sources, identity graph, and canonical reduction
- [x] Canonical hierarchy artifact from canonical objects + arm role evidence
- [x] Canonical build emitter + runtime canonical-hierarchy fallback
- [ ] Agent evidence-link pipeline in `disc` (citations/source manifests backing factsheets and narratives)
- [ ] AAA/narration pass should evaluate whether binary/multiple-star orbital
  architecture disrupts otherwise nominal habitable zones, with Sirius A/B as a
  benchmark narrative case
- [ ] Agent adjudication tables in `arm` for ambiguity resolution and missing-field proposals
- [x] Initial role-based agent eval harness with seed golden cases and quarantined anomaly inbox reports
- [ ] Local inference adjudicator cook-off with pinned TurboQuant KV-cache evaluation for long-context profiles
- [ ] Advanced system hierarchy navigation UX (systems of systems)
- [x] 3D map pilot design documented (`docs/3D_MAP.md`)
- [x] 3D map public route scaffolded at `/map` with lazy-loaded Three.js/R3F runtime
- [x] 3D map compact 100 ly API endpoint (`/api/v1/map/systems`)
- [x] 3D map pilot baseline controls (WASD, mouse look, `Q` up, `Z` down, Shift boost)
- [x] 3D map arrow-key aliases for WASD-style desktop flight controls
- [x] 3D map v0.2 touch controls (drag-look, tap/select-reticle, two-finger pinch flight, two-finger pan)
- [x] 3D map mobile six-direction hold-to-fly arrow pad for forward/back,
  left/right, and up/down movement
- [x] 3D map v0.2 mobile HUD layout with non-overlap Playwright checks
- [x] 3D map pilot baseline orientation aids, priority labels, reticle selection, and detail handoff
- [x] 3D map Playwright visual/interaction QA across desktop and mobile
- [x] 3D map 100 ly performance budget measurement and compact render payload tuning
- [x] 3D map ephemeral route measurement overlay (right-click context menu,
  per-leg line labels, route total, undo/clear)
- [x] 3D map beta desktop declutter with compact header readouts, selection
  history pills, route leg list, and long-ID popover/copy controls
- [x] Retire the 3D map selected-system snapshot hover pill; deterministic
  snapshots remain simulator fallback/reference artifacts
- [x] Checked-in public map Playwright suite for route tools, mobile HUD, and
  live-preview smoke tests
- [x] Public system simulation scene-readiness API
  (`/api/v1/systems/{system_id}/simulation-scene`)
- [x] System simulation contract and orbital source-refresh strategy documented
  (`docs/SYSTEM_SIMULATION.md`)
- [x] Normalize host-linked planet orbits into ARM `planetary_orbit` edges and
  `source_native_planet_orbit` rows while keeping `core.planets` orbit scalars
  as promoted serving summaries
- [x] First lazy-loaded live system preview renderer on system detail pages
- [x] 3D map adaptive in-scene labels that fade by camera distance while
  keeping selected, Sol, route, direction, and high-coolness labels stickier
  without returning to a fixed Sol-neighborhood label set
- [x] 3D map label-density adaptation that admits more labels in sparse camera
  fields and fades lower-priority labels harder when the field is crowded
- [x] Add two-layer 3D map stellar point rendering with bright cores,
  spectral halos, and persisted `Discovery`/`Realistic` Star Style modes
  documented as presentation-only visual policy
- [ ] Design configurable Star Search-on-map controls: tight sidebar filters,
  top search, recent systems, dual-handle ranges for viewpoint distance, star
  count, planet count, and coolness, habitable-zone toggle with explanatory
  tooltip, spectral-class selector bar, and temperature range slider
- [ ] Make active Star Search-on-map filters override default adaptive map
  labels: matching systems materialize labels and nonmatching systems fade or
  hide without falling back to noisy Gaia-only labels by default
- [x] Fix common-name alias regression: `Castor` should resolve to
  `66alp Gem` / WDS 07346+3153 in search and system detail aliases
- [x] Add alias-search build gate covering restored AT-HYG common names,
  expanded Bayer lookups such as `Alpha Centauri`, and benchmark systems such
  as Castor, Sirius, Jabbah, and Copernicus
- [x] Reconcile Live System Preview body generation with richer hierarchy
  counts for complex multiples and planet-bearing multiple systems flagged by
  `scripts/verify_known_systems_api.py`
- [x] Add hierarchy-centered Live System Preview layout so complex multiples
  and nested planet hosts render as structured groups instead of one flat ring
- [x] Add pause/start controls, sampled eccentric/inclined orbit guide paths,
  and hover vitals to the Live System Preview
- [x] Add Live System Preview beta controls: speed, reset, orbit-trace toggle,
  click/tap pinned inspection, and copyable render/source IDs
- [x] Refactor Live System Preview motion onto a single-writer shared local
  beta simulation clock with browser diagnostics, keeping science-grade epoch
  controls pending
- [x] Add Playwright coverage proving Pause freezes and Start resumes the
  shared local simulator clock
- [x] Draw direct binary orbit traces as rendered barycentric body paths,
  mass-weighted when possible and explicitly equal-mass fallback otherwise
- [x] Render hierarchical subsystem orbit edges as group-pair guides in the
  Live System Preview instead of flattening them into direct star binaries
- [x] Add inspectable subsystem render bodies/markers for hierarchy nodes with
  multiple rendered stellar descendants, with Castor benchmark coverage
- [x] Use hierarchical group-pair edges for deterministic visual-scale
  child-cluster motion in the Live System Preview
- [x] Make hierarchical group-pair motion mass-weighted around the rendered
  barycenter when positive side masses exist, with HD 213885 and HD 79210
  browser coverage
- [x] Add `render_scene.simulation_tree` (`simulation_tree_v1`) and switch the
  Live System Preview stellar renderer to recursive root/barycenter/body
  transforms for nested systems such as HD 213885, HD 79210, and Castor
- [x] Use MSC system-row periods and projected-separation Kepler estimates
  before generic visual period fallbacks for hierarchy-pair simulator orbits,
  with eps Ind A browser/API coverage
- [x] Scale stellar orbit display radii from source/projected separation or
  period+mass estimates before generic visual fallbacks, with Alpha Centauri
  AB-C coverage
- [x] Attach planet orbit guides, planet trails, planet bodies, and host-star
  habitable zones to active `simulation_tree_v1` body positions when the host
  resolves to a tree node
- [x] Align planetless host-star HZ guides to direct binary orbit planes when
  available, and statically separate wide simulation-tree siblings without
  source orbit solutions so Alpha Centauri AB/Proxima no longer stack at the
  barycenter
- [x] Resolve deterministic MSC group endpoints such as Alpha Centauri AB-C to
  rendered barycenter leaf sets so source-backed wide hierarchical orbits
  animate through `simulation_tree_v1`
- [x] Add wide-orbit presentation diagnostics distinguishing source direct
  orbits, source group orbits, derived Kepler presentation estimates, assumed
  visual orbit fields, and visual-binary fallbacks
- [x] Verify wide-orbit presentation benchmarks for Alpha Centauri, Tegmine,
  Fomalhaut, Xi Scorpii, eps Ind, Sirius, Castor, Nu Sco, and 16 Cyg, with
  known unresolved alternate-source rows reported as warnings
- [x] Normalize habitable-zone display scaling against rendered HZ bounds as
  well as planets, and make True Bodies planet radii use Earth-to-Sun scale
  relative to star meshes
- [x] Advance animated preview bodies by mean anomaly with a Kepler solve for
  true anomaly before placement, keeping full N-body propagation out of scope
- [x] Add WebGL capability fallback from the Live System Preview to the
  deterministic system snapshot artifact
- [x] Add mobile Playwright coverage for the system-detail Live System Preview
- [x] Add reusable provenance pills and focus popovers to pinned simulator
  readout facts
- [x] Add in-scene selected-object feedback for pinned simulator stars,
  planets, and orbit paths
- [x] Add Live System Preview camera orbit/zoom/pan controls with reset-view
  Playwright coverage
- [x] Add draggable/touch-safe canvas affordance and drag-orbit browser
  coverage for the Live System Preview
- [x] Export rendered simulator assumptions as structured
  `render_scene.assumptions` records shaped for future
  `disc.simulation_assumptions` persistence
- [x] Add selected-system `disc.simulation_assumptions` materialization with
  stable assumption keys, Parquet export, and API transient/persisted status
- [x] Add deterministic procedural star/planet materials and clarity-scale
  planet radius caps/floors to the Live System Preview without persisting them
  as science data
- [x] Add `visual_scale_beta_v1` to the simulator payload and consume it in
  the browser for clarity-scaled radii/orbit spacing
- [x] Add selectable simulator scale modes for Structure, True Orbits, True
  Bodies, and Log Scale without changing science-layer values
- [ ] Add a true physical simulator scale mode where both orbit distances and
  body radii use one shared linear scale, with explicit usability warnings and
  enough zoom range to inspect compact inner systems inside wide outer systems
- [ ] Add WASD/ESDF/numpad/arrow-style flight controls to standalone System
  Simulation Explorer/detail contexts, matching Star Map keybind policy where
  practical
- [x] Add collision-safe Structure-mode star radius caps with separate visible,
  halo, and picking radii, plus browser diagnostics/coverage
- [x] Add animated planet trails for strict body-scale views and display-only
  eccentricity caps so compressed Sol orbits do not visually cross
- [x] Add toggleable, inspectable habitable-zone bands as render-scene
  presentation guides from stellar luminosity and broad flux bounds
- [x] Add optional System Simulation formation/freeze-line overlays for
  vaporization, soot, water snowline, carbon dioxide, methane/carbon monoxide,
  and nitrogen boundaries, defaulting HZ on and the extra lines off
- [x] Make HZ bands default-on, increase their visual weight, and add
  default-on object labels with a compact labels toggle
- [x] Replace Live System Preview canvas-sprite labels with Drei/Troika SDF
  text labels, camera-facing screen-size scaling, and renderer diagnostics
- [x] Correct True Orbits mode to preserve linear source semi-major-axis ratios
  without an inner readability offset, with browser diagnostics
- [x] Shrink True Orbits body meshes toward marker scale so close-in orbits
  remain outside visible stars/planets while halos, labels, trails, and pick
  radii preserve usability
- [x] Sort rendered planet bodies by orbital semi-major axis/period so Sol and
  benchmark systems display in orbital order
- [x] Carry full provenance field objects into planet-orbit hover/pinned
  readouts so orbit path evidence pills can be inspected and copied
- [x] Surface confidence, source references, notes, and assumption metadata in
  simulator provenance popovers, with Playwright coverage
- [x] Surface API-backed renderer-only planet visual class as a
  `render_scene` provenance field in simulator evidence/readouts, with
  Playwright and known-system coverage
- [x] Truncate long simulator render/source identifiers in pinned readouts
  while preserving full copy/tooltip values, with Playwright coverage
- [x] Constrain mobile simulator pinned inspection as a compact bottom sheet
  with copy/close controls and Playwright geometry coverage
- [x] Propagate nested group-pair motion through hierarchy groups in the Live
  System Preview, with Castor Playwright coverage for active nested motion
- [x] Distinguish direct binary guides, group-pair hierarchy guides, and
  subsystem handles in the Live System Preview, with Castor canvas diagnostics
- [x] Carry hosted planets on full host hierarchy-group motion in the Live
  System Preview, with Playwright coverage for multi-star hosted planets
- [x] Attach rendered planet bodies to rendered host stars with `host_body_key`
  when `core.planets.star_id` resolves cleanly, with Playwright coverage for a
  hosted planet in a multi-star scene
- [x] Reconcile simple source-native MSC A/B/C render leaves with matching core
  star vitals and bridge catalog-equivalent planet hosts to those render bodies
- [x] Reconcile planet-host leaf systems with multiplicity root systems so
  Alpha Centauri can aggregate Proxima Centauri's planets while preserving
  Proxima as a direct searchable/explorable planet-host system
- [x] Strengthen known-system simulator benchmarks for Castor orbit coverage,
  Proxima display/planet coverage, TRAPPIST-1 source-backed periods, and
  55 Cnc/Sol source-backed planet periods
- [x] Add browser simulator coverage for the Nu Sco messy hierarchy benchmark:
  seven source-native leaves, subsystem handles, direct/group orbit guides, and
  no source-like spectral inheritance on unresolved children
- [x] Add simulator inspectable-target diagnostics and browser checks for
  registered star, planet, subsystem, and orbit hover/pin coverage
- [x] Strengthen mobile simulator inspection coverage for registered planet/orbit
  targets, provenance-bearing pinned readouts, and truncated copyable IDs
- [x] Add browser benchmark render smoke for Alpha Centauri, Proxima Centauri,
  55 Cnc, and Sol with pixel-level nonblank canvas checks
- [x] Add browser fallback coverage for failed simulation-scene loads so the
  preview panel shows deterministic snapshot fallback instead of a dead canvas
- [x] Surface the Live System Preview local beta simulation day in the render
  policy summary and verify it advances, pauses, and resumes with the shared
  scene clock
- [x] Add a compact Live System Preview render-policy summary for local beta
  time, clarity scale, assumption persistence, and deterministic fallback mode
- [x] Fix simulator stellar-class readout provenance so visual/proxy classes do
  not appear as SOURCE without component-specific spectral evidence
- [x] Add simulator orbit guide/trace provenance diagnostics and wider line
  hit-testing so orbit hover/pin inspection is usable and testable
- [x] Ensure rendered planets with missing source inclination receive a
  deterministic `disc_assumption` visual inclination fallback, seeded from a
  same-host source inclination when available
- [x] Tighten API, DISC, and project docs for orbit guide/trace provenance and
  render-only inclination assumptions
- [x] Add provenance-backed subsystem component/hierarchy readout fields to
  simulator render bodies
- [x] Restore local served-build deterministic snapshot fallback coverage for
  the simulator/map beta and add a focused snapshot fallback verifier
- [x] Add `render_scene.diagnostics` API counts and strict verifier coverage
  for rendered bodies, orbit endpoint/relation kinds, field statuses, and
  assumption persistence
- [x] Simplify Presentation coolness tuning so slider scoring auto-saves a
  timestamp/hash profile version, removes visible ephemeral scoring, exposes
  recent profile reruns, and shows score/snapshot job status chips in the
  Presentation workspace
- [x] Restore Sirius A/B compact-companion representation in local source
  hierarchy and simulator benchmark coverage without fabricating renderer-only
  stars; the public deployment still needs the current sliced/rebuilt dataset
- [x] Preserve compact-object classification in simulator render bodies with
  source-backed `body_class`/`compact_type` and object-type provenance fields
- [x] Add clearly labeled `disc_assumption` visual binary fallback orbits for
  two-star scenes with no source orbit edge, with Sirius Playwright and
  known-system verifier coverage
- [x] Add simulation-tree fallback subsystem handles for stale/public slices
  that lack explicit subsystem bodies, labeled as DERIVED `render_scene`
  presentation structure with source-native handles preferred when present
- [x] Add `fields.visual_stellar_class` for simulator stars, including
  `mass_main_sequence_prior_v1` ASSUMED render priors when mass is known but
  source spectral/temperature evidence is missing
- [x] Rename the public live renderer surface to System Simulation v1 while
  preserving the `/simulation-scene` API contract
- [x] Add 3D map System Simulation Peek/Explore drill-in: Peek inspects without
  moving the map camera, Explore focuses the map camera on the selected system,
  and the simulator chunk is lazy-loaded on demand
- [x] Add client-side suggested-neighbor ranking for the map drill-in layer
  using coolness, distance, planet count, multiplicity, and readable-name
  signals from the current 100 ly payload
- [x] Minimize the 3D map drill-in UI by merging selected-system vitals into
  transparent simulation overlay pills, removing the redundant selected-system
  card, restoring scale and speed selectors over the simulator, adding 1000x
  speed, shrinking and increasing transparency on Peek, and moving capped
  `Cool Stars Nearby` pills into the collapsible selection-history tray
- [x] Fix Star Map theme-specific embedded simulator controls so Aurora and
  Enterprise/LCARS speed/scale selects remain clickable above the WebGL canvas,
  with brighter Enterprise map chrome and more opaque Simple Light/Geocities
  map overlays for readability
- [x] Rename the Star Map drill-in neighbor tray to `Cool Stars Nearby` and
  theme embedded simulator dropdown menus so their option popups keep contrast
  across map themes
- [x] Remove redundant System Simulation local-days and missing-inputs readout
  pills, and move map hover/pinned object cards away from the bottom diagnostic
  strip
- [x] Increase Star Map Explorer background opacity and separate compact
  simulator readout pills from a collapsible Diagnostics disclosure containing
  Evidence and Render Policy
- [x] Capture browser Back in Star Map Explorer so the browser back button
  returns to map flight instead of leaving the map route
- [x] Polish Star Map showcase HUD/drill-in density: compact history/nearby
  pills and drill titles no longer show inline long-ID copy/info buttons,
  desktop header chrome is denser, and mobile Peek controls no longer overlap
  Explore/Detail/Back actions
- [x] Add public map branding config (`SPACEGATE_SITE_NAME` /
  `SPACEGATE_MAP_TITLE`) with `Coolstars Map` default
- [x] Add desktop System Peek resize control with session persistence
- [x] Move Star Map theme selector into a right-header burger menu and add
  persistent `WASD` / `ESDF` / `8456` keybind selector with permanent arrow
  aliases
- [x] Limit `8456` Star Map movement controls to physical numpad keys while
  keeping arrow keys always available
- [x] Add route segment truncation by clicking a route segment or recent route
  leg row, removing the severed leg and every downstream leg
- [x] Retune the Star Map/System Explorer Cyberpunk theme toward neon
  magenta/cyan map chrome with scanlines, glow, and darker glass panels
- [x] Add mouse-wheel forward/back flight over the Star Map canvas
- [x] Add horizontal wheel truck, right-button drag truck, and middle-button
  drag pedestal controls to the Star Map canvas
- [x] Rename System Peek `Back to Map` action to `Close`
- [x] Tighten Cyberpunk/Geocities System Peek header alignment and retune
  Cyberpunk title/HUD text toward terminal green
- [x] Link the Star Map header eyebrow to `spacegates.org` as
  `Spacegate Stellar Database`
- [x] Retune Enterprise/LCARS Star Map chrome with colored LCARS block controls
  while preserving black nontransparent panels
- [x] Restore Enterprise/LCARS Peek/Explore drill layout so the simulator canvas
  remains the main pane and LCARS rails use solid color blocks
- [x] Keep Star Map `Measure` from changing selected system or refocusing the
  camera during route layout
- [x] Fix Enterprise/LCARS map menu layering, colored system-title chip, and
  continuous selected-vitals strip
- [x] Apply the same continuous LCARS strip treatment to Coolstars header stats
  and Search/Detail/menu actions in the Star Map header
- [x] Show the Star Map fullscreen action consistently across themes and remove
  low-use Capture Mouse/Stabilize buttons from the visible Flight controls
- [x] Stop mouse-wheel back/scroll-down from dismissing System Simulation Peek
  now that wheel movement is a flight control
- [x] Add compact color-coded spectral/visual-class badges above simulator star
  labels without weakening provenance boundaries
- [x] Fix Geocities Star Map tray overlap and Enterprise/LCARS history metadata
  contrast in the Selection History/Cool Stars Nearby panel
- [x] Add client-side Galactic frame toggle with optional Coreward/Rimward and
  Spinward/Antispinward direction labels as presentation transforms over ICRS
  source coordinates
- [x] Add visible Coreward/Rimward/Spinward/Antispinward direction arrows and
  keep those labels available in ICRS presentation by projecting true Galactic
  directions into the active scene axes
- [x] Add simultaneous left+right mouse drag camera orbit around the selected
  system, or Sol when no system is selected
- [x] Remove low-value Star Map flight telemetry text from the bottom-right HUD
  and hide the snapshot status chip from map Peek/Explorer drill-in
- [x] Add the Coolstars/Spacegate mark to the map title, narrow the
  Selection History tray, and replace the selected-system center circle with a
  tilted orbiting-planet accent
- [x] Remove the always-visible Selection History/Cool Stars Nearby tray from
  the free-flight 3D map; Recents/Nearby now live only inside the Search
  sidebar so the map can be all-on or all-off
- [x] Add Star Map minimal mode via header `MIN`, keyboard `M`, and `Esc`
  restore, hiding passive HUD/text overlays while leaving a small restore
  control and keeping requested Peek/Explore/context-menu actions available
- [x] Move rendered stellar-class pills into the Peek/Explorer title row using
  mass-sorted simulator star entries, with Explorer title click opening the
  system page
- [x] Change Explorer `Back` to return to Peek and add a simple `X` action to
  close back to free-flight space
- [x] Keep the Star Map burger menu above Peek/Explore and prevent right-drag
  truck context-menu suppression from closing Explore
- [x] Retune the Star Map/System Explorer Geocities theme toward stereotypical
  90s web chrome with beveled windows, bright web-safe accents, and tiled
  page overlays
- [x] Retune the Mission Control theme toward Apollo-era MOCR styling with
  olive/gray console panels, CRT-green readouts, amber pushbutton accents,
  station-label strips, and focused map browser coverage
- [x] Generalize Star Map right-click target menu to Select/Explore/Measure
  with outside-right-click dismissal for menus and Peek
- [x] Add Star Map right-click `Neighbors` tool for ephemeral 10 ly measurement
  spokes from the selected system to loaded nearby systems
- [x] Reuse the map-native search strip/sidebar visual language on standalone
  Star Search v2 while preserving catalog search behavior
- [ ] Add installer/runtime configuration prompt for public site branding so
  third-party Spacegate installs can set their own site name cleanly
- [x] Surface simulator orientation basis labels for source orientation,
  partial sky-plane orientation, assumed roll, and local-clarity layout
- [x] Keep Star Search result cards live-preview-first with a four-active
  WebGL preview budget and simulator context-loss fallback; reject bulk
  browser-rendered PNG generation as too slow/heavy for routine use
- [x] Rebuild standalone `/search` cards around bounded System Simulation
  previews with cached first-frame reuse, hover/focus live promotion, and
  deterministic snapshots demoted to fallback/reference metadata
- [x] Rework public `/systems/{system_id}` pages into a simulation-first
  progressive-disclosure layout with overview, why-it-matters, habitability
  context, future AAA narrative slot, concept explanations, and secondary
  catalog/evidence sections
- [x] Add prebuilt compressed System Simulation scene artifacts for hot Star
  Search results, served before runtime scene assembly
- [x] Add layperson spectral-class tooltips to the map-native Star Search
  spectral filter buttons
- [x] Add reusable stellar-class chips for Star Search cards, map-native
  search cards, System Hierarchy star leaves, and System Simulation readouts
- [x] Standardize stellar-class chips and filter buttons on one shared
  star-style visual treatment with long TAGS-derived tooltips
- [x] Rework System Hierarchy rows into layperson-readable object chips with
  star class pills, planet/orbit tags, compact vitals, and plain-language
  object summaries
- [x] Spell out System Hierarchy orbit parameters with tooltips for orbital
  period, semi-major axis, eccentricity, inclination, and unbound trajectories
- [x] Replace the visible system-page AAA placeholder card with staged
  what-we-know, uncertainty, and explore-more sections
- [x] Add Star Map WebGL context-loss recovery and stricter live-card preview
  activation to mitigate fast-scroll browser context eviction
- [x] Add a persisted client-side Star Map runtime diagnostics toggle in the
  burger menu showing FPS, active WebGL surfaces, preview-pool pressure,
  context-loss recoveries, and quality tier
- [x] Collapse System Simulation habitable-zone and temperature-line toggles
  into a compact Lines disclosure
- [x] Make the System Simulation Lines disclosure close on outside click while
  preserving selected overlay settings
- [x] Replace System Simulation detail-page render-counter side chips with an
  inspectable object list for rendered stars, subsystems, and planets
- [x] Increase System Simulation close-zoom range for true-orbit, true-body,
  and log scale modes without changing science-layer scale semantics
- [x] Recover live System Simulation panels from transient WebGL context loss
  by remounting the canvas instead of immediately showing static snapshot
  fallback
- [x] Show the full layperson explanation when hovering System Simulation
  formation/freeze-line overlays such as the water snowline
- [x] Make System Simulation Peek hover readouts less intrusive by hiding
  provenance pills while preserving full provenance in Explorer/detail contexts
- [x] Treat MSC endpoint `WD` spectral evidence as compact-object evidence in
  System Simulation render bodies, preventing main-sequence mass visual priors
  from masquerading as white-dwarf facts
- [x] Tighten exact/common-name search ranking so queries like `55 Cnc` prefer
  the exact named system before WDS/catalog neighbors with overlapping tokens
- [ ] Define science-grade epoch/propagation controls beyond the current
  clarity-scaled Keplerian nested group animation for multi-star systems after
  source epochs/scale policy are defined
- [ ] Restore deterministic snapshot manifest coverage on the current public
  side-sliced deployment so System Simulation fallback checks exercise real
  fallback assets on antiproton
- [ ] Deprecate and remove the old prototype snapshot generator/Admin controls
  after live simulation previews and the future high-fidelity static snapshot
  path fully cover no-WebGL, share-card, crawler, and reference needs
- [x] Materialize MSC `sys.tsv` and `orb.tsv` rows into cooked/arm artifacts so
  nested subsystems, outer pairs, and source periods/separations are preserved
  instead of flattened from subsystem counts
- [x] Strengthen multiplicity goldens: Castor must verify AB/C nesting,
  A/B outer orbit evidence, per-leaf physical evidence policy, and source
  orbital-solution coverage, not only six labels plus three inner binary pairs
- [x] Rebuild canonical `arm.duckdb` from canonical `core.duckdb` during
  canonical emit, rather than copying bootstrap ARM with pre-canonical keys
- [x] Preserve MSC inferred leaf hierarchy through canonical hierarchy emit
  after the ARM builder moved to source-native nested MSC subsystem edges
- [ ] 3D map real-device mid-tier mobile performance check
- [x] Live-WebGL runtime manager foundation for shared context budgeting,
  adaptive quality, preview pooling, and fallback-last policy across Map, Peek,
  Explorer, and Star Search result cards
- [x] Add Star Search/map-search preview tiers so ordinary singleton systems use
  cheap client previews while planet hosts, multistars, exotic systems, public
  goldens, and high-coolness systems keep full System Simulation previews
- [x] Add `search-preview` simulation-scene materialization profile for
  priority full-preview systems
- [x] Add V1054 Oph as a complex-nearby-multiple golden: verify source leaf
  count, MSC/WDS/ORB6 component-label reconciliation, no extra rendered
  endpoints, and no misleading shared-orbit layout when orbital coverage is
  incomplete
- [x] Define Stellar Physical Classification v1: source spectral class first,
  source/derived Teff and color constraints second, radius/luminosity/remnant
  guards before mass priors, with derived display classes never written into
  core spectral fields
- [x] Add and verify a side-artifact rebuild path for ARM-only/schema-adjacent
  changes: clone served core/parquet/disc artifacts, regenerate `arm.duckdb`
  from cooked catalogs, promote locally only after relaxed build verification
  and known-system/API checks pass
- [x] Enrich member-star alias/search-term coverage for complex systems such as
  V1054 Oph so names like Gliese 643 and VB 8 resolve to the containing system
  without promoting member names to false system-level canonical names
- [ ] Expand the Live-WebGL runtime manager with richer admin/dev telemetry,
  observed browser context limits, and real-device automatic quality tuning
- [x] Complete M8.1 measured Tiled Deep Map 100/250-ly pilot (`docs/TILED_MAP.md`)
- [x] Define deterministic ICRS J2016 octree, Morton IDs, immutable manifests,
  compact binary tiles, and exact-membership verification
- [x] Add profile-versioned coolness interest summaries and mixed
  spatial/high-interest coarse samples without changing spatial membership
- [x] Add static content-addressed tile delivery and renderer-independent
  browser scheduling/cache lifecycle
- [x] Pass tiled 100-ly behavioral parity and before/after performance budgets
- [x] Pass the initial public 250-ly desktop/mobile pilot while retaining
  500/1,000 ly as verification-only manifests until separate deep-radius gates
- [x] Gate tiled 100-ly labels against the authoritative root-system public-name
  policy and reject literal/encoded static tile traversal paths
- [x] Replace the visible 110-ly density shell with camera-centered blended LOD,
  deterministic Balanced/Performance policies, and opt-in Exact density
- [x] Fail map performance verification on radial density seams, missing detail
  refinement, incorrect device policy, or duplicate immutable tile requests
- [x] Add persisted Bright star style and toggleable representative map-label
  class badges using a versioned mass-proxy/brightness component policy
- [x] Analyze real-device Brave 4K/250-ly crash captures and harden idle map
  lifecycle: prevent same-class resize transport reloads, gate stationary
  telemetry/labels, serialize context recovery, expose WebGL resource counters,
  and pass a forced-GC 4K/Exact idle soak with zero idle tile requests
- [x] Pass real-device 250-ly Exact/Bright stress acceptance on Pixel 9 XL
  (whole-sphere view, rapid navigation/selection/view changes, approximately
  15 FPS minimum) and a multi-hour RTX 3090 4K fullscreen maximum-settings soak
- [x] Expose 500/1,000-ly selectors only through the M8.1.4 progressive sample
  scheduler; retain exact identity through search pins and camera-local leaves
- [x] Verify deep-map coarse-first ordering, complete depth-3 parent replacement,
  bounded request/cache behavior, camera-flight refinement, and zero eager exact
  leaf requests
- [x] Record desktop/mobile 500/1,000-ly network, point-count, frame-time,
  memory, selection, and idle acceptance evidence before declaring M8.1.4 stable
- [x] Deploy and publicly verify the stable 100/250-ly checkpoint before the
  separate 500/1,000-ly promotion; public progressive desktop/mobile checks pass
  for both deep radii on build `20260715T015659Z_e392a11_side_rebuild`
- [x] Make `publish_db.sh` reject unknown arguments and treat `--help` as a
  read-only operation so CLI inspection cannot start a multi-gigabyte publish
- [x] Make `push_published_db.sh` upload archives before metadata and publish
  selected DB/catalog `current.json` files with checksum-based replacement so
  same-sized metadata cannot remain stale after a successful push
- [x] Reconcile data-sensitive Castor, V1054 Oph, and Tegmine simulation goldens
  with the preserved science checkpoint: Castor exposes physical A/B/C
  subsystem groups while AB remains a stellar leaf, V1054 exposes five accepted
  stellar leaves plus its system object, and Tegmine preserves its unresolved
  source orbit as unattached rather than inventing a nested hierarchy
- [ ] High-fidelity static System Snapshot v2 generator for traditional Star
  Search, no-WebGL clients, crawlers/share cards, and fallback/reference
  surfaces
- [x] Define public-experience golden-system review checklist for Tau Ceti,
  TRAPPIST-1, Alpha/Proxima Centauri, Sirius, 55 Cnc, Epsilon Eridani,
  Barnard's Star, Wolf 359, Vega, and Fomalhaut
- [x] Ship Star Search v2 System Page Beta layout: compact hero, early System
  Simulation, staged narrative cards, At-a-Glance facts, compact Stars and
  Hierarchy rows, physical/orbital tooltips, and secondary Evidence and
  Technical Data
- [ ] Fix current public-search source/alias coverage gap for Vega /
  Alpha Lyrae / HD 172167 / HIP 91262 so the public UX golden resolves
  correctly
- [x] Build Nearby Ultracool Completeness v1 starter bridge: promote vetted nearby
  UltracoolSheet/WISE brown dwarfs such as Luhman 16 and WISE 0855-0714 into
  accepted inventory when Gaia backbone coverage is missing, with provenance
  and no full CatWISE ingest dependency
- [x] Implement WISE/CatWISE/AllWISE v1 targeted evidence integration:
  known-object cross-reference collector, ARM infrared evidence tables,
  verification script, no WISE-only core promotion, and documented source
  policy. Plan and follow-ups: `docs/CATWISE_ALLWISE_PLAN.md`
- [x] Re-key targeted WISE match, photometry, motion, and candidate-context
  bindings against the current canonical CORE; add verifier gates for target,
  system, and stable-key agreement
- [x] Add IRSA/WISE image integration for system pages: W1/W2/W3 cutouts,
  generated web previews, source links back to IRSA, retrieval metadata, and a
  bounded lazy cache
- [x] Add WISE/CatWISE candidate review queue scaffold for red/high-motion
  targeted-query candidates, with accepted/rejected/quarantined/needs_review
  status vocabulary and no automatic core promotion
- [ ] Expand WISE/CatWISE candidate discovery beyond targeted known-object
  cones into a reviewed nearby ultracool/brown-dwarf search workflow with AAA
  evidence packet hooks
- [ ] Plan selectable multi-wavelength sky backgrounds for the 3D map: visible
  Milky Way baseline, infrared sky, X-ray sky, and later survey layers as
  attributed presentation overlays
- [ ] Build Concept Tag Foundation: tag priority tiers, clickable concept-page
  hooks, complete tooltip/popover audit, and first `/concepts/:slug` pages
- [ ] Integrate reviewed AI Astronomy Agency public narration slots into
  Star Search v2 system pages without mixing unreviewed generated prose into
  canonical science
- [x] Add System Narration Foundation v1 API/UI scaffold: deterministic
  `narrative_blocks` fallback prose, WISE infrared explanation, DISC contract,
  AAA review policy, and offscreen System Simulation pause/throttle
- [ ] Authenticated admin map overlay for per-system/object review controls,
  evidence portfolio access, and AAA research promotion
- [x] Extended-object science foundation for Messier/NGC/IC landmarks,
  nebulae, clusters, and galaxies: pinned sources, identity, geometry, distance,
  ARM evidence, search/API, coverage, quarantine, and deterministic goldens
- [x] Build, verify, and locally promote canonical
  `20260713T1627Z_dd7446e` and public slice
  `20260713T1627Z_dd7446e_public` with extended-object evidence intact
- [x] Expose typed extended-object matches in Star Search with dedicated
  evidence pages, alias lookup goldens for M45/IC 4592/LBN 1113, and
  JavaScript-safe string serialization for 64-bit extended-object IDs
- [ ] Extended-object map presentation layer: tile/LOD integration, extents,
  imagery, selection behavior, and attributed background-sky context
- [x] Refresh MSC multiplicity ingest from the upstream June 19, 2026 archive
  and rerun hierarchy/orbit goldens
- [x] Add source-evidence utilization audit for preserved MSC `sys.tsv`/`orb.tsv`
  rows that are not yet normalized into ARM orbit edges or simulator contracts
- [x] Materialize deterministic preserved MSC orbit-detail rows into ARM
  `orbit_edges`/`orbital_solutions` where endpoint reconciliation is
  deterministic, with 70 Oph as a visible benchmark
- [x] Review and account for every MSC `orb.tsv` diagnostic: 4,627 rows
  normalize, six out-of-inventory/no-`sys.tsv` rows are explicitly excluded,
  and zero remain quarantined or unaccounted
- [x] Register Castor MSC endpoint `CC` in the non-executable adjudication
  inbox; retain its ARM-derived brown-dwarf typing without treating that as an
  accepted physical-status verdict
- [x] Reconcile canonical-hierarchy endpoint typing with ARM component typing:
  structural node kind is separate from family/type and inferred substellar
  leaves retain their ARM type
- [x] Add scalable map label stellar-class badge modes (`Off`, `Primary`,
  `All`) backed by packed tile-v3 repeated-class data
- [x] Replace surface-specific stellar badge assembly with one deterministic
  ARM hierarchy-leaf projection; preserve repeated and unknown leaves, exclude
  aggregates/nonstellar endpoints, and verify HD 110067, HD 79107, Gl 161.1,
  HD 18134, and Castor without system-specific build rules
- [x] Default map stellar-class badges to `All` and remove the redundant search
  toolbar `Close Results` action while retaining the results-pane close action
- [x] Add tile-v4 confirmed-planet category badges for hot/temperate/cold gas
  giants and terrestrial planets, capped at one per category and omitted when
  composition or environment evidence is ambiguous
- [x] Replace the map's single habitable-zone toggle with six independently
  toggleable broad confirmed-planet categories, using identical versioned
  category SQL for tile masks and API search and explicit OR semantics
- [ ] Replace the map category environment temperature proxy with an auditable
  incident-flux-versus-host-dependent-HZ derivation, including safe single-host
  recovery and multi-star irradiation/component-binding rules
- [ ] Preserve NASA composite best-mass values with `pl_bmassprov`, limits, and
  uncertainties in typed planet evidence; distinguish measured mass, `M sin i`,
  deprojected estimates, and mass-radius-relation estimates before allowing
  them to influence broad map categories
- [x] Distinguish planet map badges from stellar badges with right-of-name
  placement, smaller bodies, stronger rings, and a separate muted palette
- [x] Make the map temperature filter's complete neutral range 0-83,000 K
- [x] Separate neighbor endpoint labels from distance-line labels, continuously
  update camera-local cool-star recommendations, and add compact multiplicity-
  preserving stellar badge stacks to Recents and Cool Stars Nearby
- [x] Show every projected star and confirmed CORE planet on Star Search/map
  result cards and the System Hero; remove the four-class summary cap and keep
  object keys in the response for future object-detail links
- [x] Keep the map Search Results header and Close action visible while its card
  list scrolls, and widen the desktop Filters sidebar by about three characters
- [x] Make simulation-scene v4 use the canonical stellar-leaf projection as its
  membership authority and resolve canonical/evidence component-key aliases
  collision-safely; HD 57041 renders `K,WD` rather than an unprojected assumed
  member or an M-class white dwarf
- [x] Add a general gross-position sanity gate for MSC component surrogates and
  a physically bounded authoritative Gliese/GJ or name-root bridge into
  existing WDS systems; rebuild verification must confirm Struve 2398 A/B is
  one system and the unrelated V1298 Aql surrogate is excluded
- [x] Retain unmatched terminal single-letter MSC stellar endpoints in the
  canonical display hierarchy only when the source tree is terminal and a
  multi-observation WDS pair links the endpoint to a resolved sibling; gate Nu
  Sco at six B-class leaves plus one unknown without object-specific transform
  logic, and suppress the candidate set when already represented canonical
  leaves exhaust the source tree's terminal-leaf capacity
- [x] Deploy capacity-bounded side build `20260717T0614Z_f452835_side` to
  antiproton; verify public API integration, known systems, Struve 2398 at two
  M-star leaves, HD 57041 at K+WD, Nu Sco at seven leaves, and targeted map UI
  flows while retaining one rollback checkpoint
- [ ] Make strict edge-host verification self-contained for published derived
  builds so it validates published canonical-evidence hashes without requiring
  Photon's upstream canonical ARM path
- [ ] Extend tile-v4 planet environment derivation to confirmed planets with
  orbital distance but no published equilibrium temperature/insolation,
  including the authoritative Solar System rows, using a general auditable
  host-luminosity policy rather than a Sol-specific exception
- [x] Coalesce concurrent cold simulation-scene assembly and persist compatible
  build-keyed compressed runtime artifacts
- [x] Add side-build priority simulation-scene materialization before immutable
  promotion
- [x] Add an allowlisted Admin `Warm Simulation Scenes` background action that
  writes only to the bounded build-keyed runtime cache, allowing priority scene
  warming to be deferred until after promotion without mutating served builds
- [x] Fix general stellar-display precedence across map, Explorer, Peek, and
  simulation scenes: preserve lowercase dwarf spectra such as `dM1e`, retain
  tentative `WD?` source evidence over mass priors, use visual badges only as
  fallback, and invalidate stale v1 scene caches
- [x] Remove public stable-key copy/display controls from Explorer readouts and
  snapshot metadata; use preferred member/catalog names with component suffixes
- [x] Require simulation-scene cache reuse to match both materializer contract
  version and target build ID; regression-test copied-version and corrupt-file
  rejection
- [x] Regenerate and locally promote 1,000/1,000 v2 priority simulation scenes;
  replace the stale exact-count `dM1e` browser expectation with a general
  M-dwarf-versus-white-dwarf parsing invariant
- [x] Remove the temporary Castor-only reviewed-classification build injection
  and confirm a clean ARM rebuild matches the pre-change catalog result
- [x] Stop post-slice TESS identity re-adjudication: project full-canonical
  decisions into public builds so 32 ambiguous hosts and one removed-host case
  cannot become accepted merely because competing objects were trimmed
- [x] Preserve retained WDS observation and WISE match evidence in public ARM
  slices without treating source-native component keys as canonical foreign
  keys
- [x] Preserve `Proxima Centauri` as the rendered member display name instead of
  allowing source component shorthand `alp1 Cen C` to override the core name
- [x] Add exact member-star search fallback for served side builds missing
  materialized member terms, with `VB 8` focus metadata and alias-authority
  verification
- [x] Define WDS pair-observation utilization v1: preserve source label scope,
  materialize accepted/missing/ambiguous/excluded endpoint bindings as
  projection evidence, and prohibit WDS-only bound/orbit promotion
- [x] Add spectral-subclass-aware main-sequence stellar parameter priors for
  simulation support, guarded against giants, subgiants, remnants, and compact
  objects, without writing those priors into core
- [x] Add ARM-scoped runtime luminosity derivation for hierarchy-rendered stars
  with radius and effective temperature so HZ/temperature-line overlays do not
  lose systems such as TRAPPIST-1
- [x] Audit active source-catalog field utilization against CORE/ARM/DISC,
  simulations, map taxonomy, current milestones, and AAA goals; emit a reusable
  machine-readable report and document the single-rebuild evidence bundle in
  `docs/SOURCE_CATALOG_UTILIZATION_AUDIT_2026-07-17.md`

### E1. Evidence Lake v2 (Current Main Quest)

- [x] Accept Evidence Lake v2 as the main-quest replacement for the narrower
  Catalog Evidence Utilization v2 rebuild and document the architecture in
  `docs/EVIDENCE_LAKE_V2.md`
- [x] E0: define the source-release registry, per-domain authority matrix,
  complete upstream-field disposition registry, schema-drift policy, and
  uncertainty-aware 1,000-ly ingestion envelope
- [x] Run the retention audit before the next full build; preserve served,
  rollback, published, referenced, and unique-source artifacts while reducing
  the current 817 GB immutable `out/` footprint through the documented process
- [x] E0: set explicit raw, typed, build, report, document, and observation-
  product cache budgets; verify backup policy before large acquisition
- [x] E0: add hash-gated, whole-artifact retention for explicit interrupted
  Evidence Lake compiler temporaries; refuse manifests, links, shared files,
  and live processes, and preserve machine-readable dry-run/applied reports
- [x] E1: materialize immutable raw snapshots for all 25 available non-planned
  releases with atomic manifests, checksums, source metadata, and verification
- [x] E1: add source-specific typed parser versioning, fail-closed field/row
  accounting, and machine-readable typed coverage/error reporting
- [x] E1: close the 22 explicit fixed-width, archive, FITS, SQL-row, and
  documented-text parser gaps without dropping source-native fields
- [x] E1: implement immutable raw snapshots and source-native typed
  Parquet/Arrow cooks with exact queries, checksums, schemas, counts, units,
  uncertainty/limit semantics, citations, licenses, and clean-state determinism
- [x] E1: implement metadata-first/on-demand storage for spectra, light curves,
  atmosphere spectra, and imagery rather than bulk hot-database ingestion
- [x] E2: implement a release-scoped identity/scope graph with permanent
  Spacegate object IDs and explicit accepted/missing/excluded/ambiguous/
  quarantined outcomes
- [x] E2: reconcile Gaia DR2-to-DR3 membership and every remaining DR2 fallback
  through the official crossmatch/neighborhood path; forbid direct ID equality
- [x] E2: independently acquire the reverse Gaia DR3-to-DR2 target universe so
  release merges outside the forward target subset cannot pass as unique
- [x] E2: keep physical identity, containment, component/subsystem,
  observation-target, and alias/name claims separate; verify that MSC/WDS
  candidate relations never promote canonical containment
- [x] E2: emit ordered graph Parquet, coverage, collision, quarantine, source-
  binding, scope, high-proper-motion, and clean-compile determinism reports
- [x] E3 foundation: implement schema-enumerating TAP/UWS and resumable pinned-
  HTTP acquisition with exact responses, checksum/row/MAXREC gates,
  inter-process manifest locking, atomic promotion, and source-native
  VOTable/FITS Parquet support
- [x] E0 retention compatibility: make pre-contract `YYYYMMDDT_<label>` builds
  opt-in and exact-hash gated; preserve every E0-referenced build and all reports
  while reclaiming 364.82 GiB of superseded Photon build payloads
- [x] E3 envelope contract: pair every Gaia AP, supplementary AP, NSS,
  variability/rotation, and official external-crossmatch hard-parallax product
  with a disjoint posterior-overlap acquisition and executable parity gate
- [x] E3 naming/variability slice: pin, snapshot, type, and verify official WGSN
  plus GCVS catalog, cross-identifier, suspected-variable, bibliography, and
  source-document artifacts; validate all 16 WGSN fields and separate its 597
  catalog rows from page furniture; normalize only registry-declared GCVS
  trailing layout delimiters, preserve internal delimiters and exact raw rows,
  and pass typed A/B plus clean-reproduction gates
- [x] E3: acquire the bounded Gaia AP/FLAME/evolution/extinction/activity,
  variability/rotation, expanded NSS, official crossmatch, and distance evidence
- [x] E3 Gaia boundary checkpoint: complete, type, verify, and clean-reproduce the disjoint
  Bailer-Jones-selected Gaia uncertainty supplement; report hard, posterior,
  overlap, and union counts without treating EDR3/DR3 identity as an
  undocumented equality
- [x] E3 distance/cluster source-native checkpoint: preserve, type, verify, and
  clean-reproduce all 17,310,560 bounded Bailer-Jones distance rows plus the
  1,329,052 Hunt-Reffert cluster, membership, and literature-crossmatch rows;
  retain case-distinct percentile fields and all 171 table-column occurrences
- [x] E3: acquire/migrate current Gaia cluster and confidence-bearing wide-
  binary evidence, SIMBAD/GCVS/VSX/IAU naming/bibliography roles, and all active
  multiplicity/compact/ultracool/extended sources under the new contracts
- [x] E3 wide-binary checkpoint: pin, source-native type, verify, and clean-
  reproduce the El-Badry Gaia EDR3 main/control FITS tables and method scripts
- [x] E3 SIMBAD staged checkpoint: acquire the checksum-pinned 24,218-object
  complete-Gaia-envelope delta; preserve, verify, and clean-reproduce all eight
  active bridge/basic/identifier/bibliography tables and 35,321,742 rows
- [x] E3 SBX checkpoint: replace the 10-field/aggregate-orbit Evidence Lake
  input with a separate complete-catalog profile; preserve all 4,080 systems,
  102,459 aliases, 261 configurations, 5,169 full orbit rows, and all 73
  table-column occurrences without changing the served legacy input
- [x] E3: acquire complete relevant NASA reference-specific/composite planet,
  host, TOI, Kepler/K2 candidate/status, transit, and RV metadata plus matched
  APOGEE/GALAH/LAMOST stellar-physics evidence
- [x] E3 NASA checkpoint: preserve and verify 12 source-native tables, 206,989
  rows, and all 2,093 planet/host/TOI/K2/Kepler KOI/TCE/transit fields with zero
  omissions
- [x] E3 spectroscopy acquisition checkpoint: pin, source-native type, verify,
  and clean-reproduce APOGEE DR17, GALAH DR4, and the three LAMOST DR11 stellar
  products; preserve every configured APOGEE table HDU and fixed-size array
  rather than selecting only the primary catalog extension
- [x] E3 Gaia AP spectroscopy supplement checkpoint: complete and validate all
  17 uncertainty-envelope TAP partitions; subsequently close the registered E3
  product tail through the checksum-bound target-seed acquisition
- [x] E3 acquisition completion: derive a checksum-bound 189,145-source Gaia DR3
  uncertainty target seed; complete and account all 56 registered products;
  snapshot/type the five expanded Gaia source families; and pass per-source plus
  aggregate verification and clean-state reproduction with zero pending products
- [ ] E4: materialize typed stellar, astrometric/distance, photometric,
  spectra-product, variability/activity/rotation, multiplicity/orbit, cluster,
  planet/transit/RV, compact-object, extended-object, citation, and product-
  lineage evidence contracts
- [x] E4 APOGEE checkpoint: bind explicit Gaia EDR3 IDs through the exact
  Bailer-Jones envelope; account all 243 source fields and independently audit
  coherent stellar/abundance, photometry/extinction, coordinate/RV, identity,
  citation, and product-lineage evidence with zero pending fields
- [x] E4 compiler scaling: cache checksum-bound selected rows and insert large
  evidence branches incrementally; prove the APOGEE logical hash unchanged
  before materializing GALAH or LAMOST
- [x] E4 GALAH checkpoint: retain 117,885 exact Gaia DR3-envelope allStar rows;
  account all 184 fields; separate spectroscopic, isochrone/model, distance/RV,
  interstellar, and activity evidence; reject the initial distance-as-radius
  interpretation; pass generic/source audits and clean reproduction
- [x] E4 LAMOST checkpoint: retain 1,659,281 exact Gaia DR3-envelope observation
  rows across LRS stellar, LRS M-star, and MRS stellar; account all 185 field
  occurrences; preserve coherent LASP/CNN, molecular/activity, and RV contexts;
  index official spectrum locators; pass generic/bounded source audits and clean
  reproduction
- [x] E4 foundation: add the immutable 23-domain compiler contract, deterministic
  source-record identity, exact-duplicate accounting, explicit binding outcomes,
  field dispositions, cached-artifact checksum verification, and clean logical-
  hash reproduction; keep unmaterialized fields visibly pending
- [x] E4 SIMBAD scale checkpoint: rebuild the complete envelope with v38's
  32-bucket astrometry-citation materialization; checkpoint
  `fc5bd4e6398d72bde50ba6d5` passes independent artifact audit and clean
  logical-hash reproduction under the bounded memory policy
- [x] E4 WGSN naming checkpoint: materialize all 597 official name records and
  22 fields with proper-name, source-record, catalog, HIP, Bayer, search-
  spelling, and exact HR/HD/HIP/GJ claims; retain shared target/scope ambiguity,
  exclude raw placeholders from promotion, and pass artifact, scope, and clean-
  reproduction gates
- [x] E4 GCVS variability checkpoint: materialize all 340,839 typed rows from
  the six registered release tables as source-scoped identities, astrometry,
  source spectral classifications, variability observations, bibliography,
  and evidence links; preserve component suffixes, conflicting raw sign fields,
  and unresolved bindings; pass artifact, source/scope, and clean-reproduction
  gates
- [x] E4 Hunt/Reffert cluster checkpoint: apply the uncertainty-overlap boundary
  to retain 465 clusters, all 51,017 published probability-bearing member rows,
  and 451 literature crossmatches; materialize all 161 fields as source-scoped
  cluster/membership evidence with endpoint identities and citations; pass
  artifact, cluster/scope, and clean-reproduction gates without promoting
  membership to canonical containment
- [x] E4 extended-catalog checkpoint: materialize all 19,012 OpenNGC and
  constituent nebula-catalog rows plus 856 source-document lines and all 238
  fields; emit exact catalog identities without heuristically splitting alias
  lists or collapsing Cederblad components; pass artifact, extended-object
  scope, and clean-reproduction gates without relation/orbit promotion
- [x] E4 MSC checkpoint: preserve all 43,418 release rows and 73 fields with
  WDS-qualified component/relation scopes, source-status polarity, coherent
  elementary-binary and full-orbit parameter sets, explicit numeric-zero
  sentinel semantics, unsplit alias/pair strings, and unresolved bindings;
  pass artifact, MSC source/scope, and clean-reproduction gates
- [x] E4 WDS checkpoint: preserve all 157,476 WDS summary/method rows and
  140,416 CDS WDS-Gaia rows with all 43 fields accounted; retain only
  WDS-qualified pair keys, source-scoped observation/classification evidence,
  bounded numeric measurements, and candidate angular crossmatches with zero
  strict probabilities; pass artifact, WDS source/scope, and clean-reproduction
  gates without identity, containment, or orbit promotion
- [x] E4 Gaia UCD association checkpoint: preserve all 7,630 published Gaia
  DR3 sample rows and 93 ReadMe lines; materialize separate HMAC hard
  assignments and BANYAN probability-bearing best hypotheses without treating
  sample membership as a spectral classification; pass artifact, source/scope,
  placeholder, citation, and clean-reproduction gates
- [x] E4 UltracoolSheet checkpoint: preserve all 3,890 rows and 242 fields;
  separate direct optical/IR classifications from maintainer formulas, retain
  23-band photometry and source-specific astrometry/distance alternatives,
  reject non-finite and negative uncertainty sentinels, keep DR2/DR3 identities
  distinct and alias lists unsplit, and pass artifact, source/scope, citation,
  product-lineage, and clean-reproduction gates without relation/planet
  promotion
- [x] E4 NASA identity/lifecycle checkpoint: materialize 750,151 release-scoped
  identifier claims and 72,809 positive/candidate/negative lifecycle claims;
  preserve per-identifier semantic scope, and verify clean logical-hash
  reproduction without altering canonical inventory
- [x] E4 NASA domain checkpoint: group values with uncertainties, limits, units,
  and references into coherent parameter sets; materialize stellar, astrometric,
  photometric, rotation, classification, planet, transit, RV, product-lineage,
  and citation evidence; account all 2,093 fields with no pending tail
- [x] E4 wide-binary checkpoint: apply the versioned three-sigma 1,250-ly
  envelope, materialize candidate and shifted-control relation evidence with
  distinct endpoint scopes, preserve `R_chance_align` as a non-strict
  confidence statistic, account every field and filtered row, and pass clean
  reproduction plus the independent artifact audit without canonical promotion
- [x] E4 ORB6 checkpoint: preserve all 4,051 visual-orbit rows and 37 fields as
  coherent solutions with WDS/discoverer/ADS/HD/HIP identity evidence; leave
  combined pair scope unresolved and pass clean reproduction/artifact audit
- [x] E4 DEBCat checkpoint: preserve all 374 rows and 30 fields with separate
  primary/secondary/system parameter scopes, explicit missing sentinels, and no
  component or system leakage; pass clean reproduction/artifact audit
- [x] E4 Green SNR checkpoint: preserve all 310 rows/15 fields with Galactic
  identifiers, geometry, raw uncertain flux/index parameters, detail lineage,
  clean reproduction, and artifact-audit coverage
- [x] E4 TESS EB checkpoint: preserve all 17,605 rows/20 fields; normalize
  zero-padded TIC identity, distinguish 4,584 catalog members from 13,021
  nonmembers, and type sector/flag/morphology/Tmag/astrometry/orbit evidence
- [x] E1/E4 targeted TIC/TOI checkpoint: preserve all 122,772 rows/239 fields
  across the bounded target set, MAST TIC, official Gaia release/external
  crossmatches, targeted Gaia DR3, and NASA TOI; retain member-qualified
  external namespaces, dual raw TOI forms, asymmetric uncertainty lineage,
  duplicate/split relations, and positive/candidate/negative lifecycle evidence;
  pass source-specific, generic artifact, and clean-reproduction gates without
  canonical inventory promotion
- [x] E4 white-dwarf checkpoint: bound 1,280,266 candidates by posterior
  distance-interval overlap; preserve 337,272 candidate contexts and separate
  H/He/mixed atmosphere fits with all 161 fields accounted and no implicit
  model winner; pass clean reproduction and artifact audit
- [x] E4 ATNF checkpoint: preserve repeated pulsar parameters, glitches,
  comments, README/archive context, and the complete source bibliography;
  materialize predicate-scoped PSRJ/PSRB aliases and link only exact reference
  keys while retaining unmatched lexical tokens; pass clean reproduction and
  artifact audit
- [x] E4 McGill magnetar checkpoint: preserve all timing, magnetic-field,
  spin-down, X-ray, distance, position, association, band, and activity fields
  as separate coherent parameter contexts; retain raw footnoted names and
  reference families, and pass clean reproduction/artifact audit
- [x] E3/E4 McGill bibliography follow-up: pin and type the publisher HTML,
  exact reference links, CDS ReadMe, and 215-row reference table; resolve 97
  external codes without inventing URLs, retain four unresolved historical
  shorthand codes explicitly, and pass source/artifact/reproduction gates
- [x] E4 SB9 checkpoint: preserve the complete ReadMe/system/alias/orbit release,
  explicit primary/secondary relation endpoints, scoped component spectra and
  magnitudes, release-correct Gaia aliases, coherent ADS-linked orbit solutions,
  and deterministic cross-table relation links; pass clean reproduction and
  artifact audit without canonical containment promotion
- [x] E4 SBX checkpoint: materialize complete source-scoped astrometry,
  component spectra/magnitudes, catalog aliases, hierarchy claims, and coherent
  orbits; split component-suffixed HD/HIP designations from numeric IDs, link
  every orbit to one binary relation, and pass clean reproduction plus the
  independent artifact audit without canonical containment promotion
- [x] E4 Gaia NSS checkpoint: use authoritative Gaia source parallax for the
  boundary; preserve all 85,724 hard-envelope and 1,351 disjoint uncertainty
  rows plus 77 fields as coherent model-specific solutions; qualify solution
  keys by NSS model; and pass collision, field, source-specific, artifact, and
  clean-reproduction audits without fabricated endpoints
- [x] E4 scoped uncertainty contract: distinguish error magnitudes from absolute
  lower/upper posterior endpoints, require explicit endpoint bound semantics,
  and preserve both representations through focused compiler regression tests
- [x] E4 Gaia external-crossmatch checkpoint: preserve every bounded official
  AllWISE, 2MASS, Hipparcos-2, Tycho-2, and RAVE DR6 best-neighbour row as
  candidate relation evidence with source-scoped endpoints, separation, flags,
  neighbour counts, exhaustive field accounting, and exact clean reproduction;
  promote no crossmatch directly to accepted identity
- [x] E4 Gaia AP compiler foundation: retain multi-model classifier probability
  vectors as one coherent source bundle without winner selection; distinguish
  domain interval endpoints from error magnitudes; reject reversed endpoints
  and report source-native non-bracketing estimates without rewriting them
- [x] E4 Gaia AP main contract: account all 482 hard/supplement source fields;
  preserve coherent DSC/ESP, GSP-Phot, FLAME, GSP-Spec, MSC component/system,
  and OA contexts; validate source-native units and real-row materialization
- [x] E4 Gaia AP main build: materialize the 51,164,425-row bounded release and
  pass source-specific, generic artifact, clean-reproduction, storage, scope,
  probability, interval, and zero-pending-field gates
- [x] E4 Gaia AP scale guard: materialize unresolved bindings per source table
  and link every ordinary and nested evidence-reference table through 32
  deterministic source-record hash buckets; preserve identical primary keys and
  counts under the 32-GB Photon compiler cap
- [x] E4 Gaia AP supplementary checkpoint: the v71 contract and real-row smoke
  account all 354 fields while preserving four GSP-Phot library alternatives,
  GSP-Spec ANN, and spectroscopic FLAME without winner selection; immutable
  build `c4a6b5fd297f8ef9cceb6340` and source-specific/generic audits pass, with
  clean reproduction matching logical hash
  `a74eb79475a76af75d7a626adb56baf89de3f6978904e7c83e4619f46bf6e052`
  and removing its USB scratch tree
- [x] E4 immutable analytical-storage guard: replace retained DuckDB PK/unique
  ART indexes with explicit binding deduplication plus exact compiler and
  independent-verifier key audits; preserve logical hashes in same-row A/B and
  record measured storage reduction before further Gaia-scale materialization
- [x] E4 source-scope accounting: classify all 47 registered releases as an E4
  adapter or an explicit E2/E3/E6 boundary disposition; fail on unaccounted,
  stale, conflicting, or unregistered entries; the current ledger has 38
  scientific adapters, nine boundary dispositions, and no blocker
- [x] E4 remaining registered adapter tail: materialize Gaia source and the
  separately scoped natural and artificial JPL Horizons adapters; all pass
  immutable-build, source-audit, and generic-artifact gates, and Horizons also
  passes clean reproduction
- [x] E4 Gaia source pre-adapter audit: verify all 32,176,271 source-native rows,
  identical complete 152-field branch schemas, unique/disjoint Gaia DR3 IDs,
  envelope polarity, epochs, solution release, and numeric validity domains;
  report product-index and radial-velocity coverage and exhaustively assign all
  source columns to adapter roles with no unclassified tail
- [x] E4 Gaia source checkpoint: immutable build
  `ab7f7e6bc211bee146885987` materializes all 32,176,271 rows as coherent
  release-native source solutions, accounts 304 field occurrences with zero
  pending fields, and passes source/generic artifact audits; the independent
  clean reproduction matches logical hash
  `1863f8da12380f845983339213a28ee7c4a0af5313bc9fee586f05e1a435a962`
- [x] E1/E4 exoplanet-lifecycle source recovery: pin and independently type
  Exoplanet.eu, the Open Exoplanet Catalogue archive, and HWC; preserve OEC
  archive-member object scope, aliases, parameters, relations, candidates,
  controversial cases, retractions, limit semantics, and product links; keep
  HWC habitability features out of lifecycle evidence and canonical counts;
  pass typed/source/artifact/clean-reproduction gates
- [x] E4 immutable release-set composition: explicitly map all 38 adapters to
  36 accepted source shards, validate exact source/release membership and
  artifact manifest/database identities, index every populated domain-table
  shard, atomically promote release set `a188a3adc6207d3a217d54a9`, and reproduce
  its manifest byte-for-byte without duplicating 449.2 GB of evidence
- [x] Make the Gaia variability source audit reproducible: validate all 592,197
  rows and 52 masked vector fields, distinguishing whole-vector absence from
  valid element masks, exhaustively partition every source column by role, and
  record the coherent parameter-set decision
- [x] E4 Gaia variability checkpoint: materialize all 592,197 source rows and
  268 field occurrences through four ordered coherent schemas; preserve whole-
  vector absence versus 52 source-masked nullable vector fields, pass generic
  and source audits, and cleanly reproduce logical hash
  `d98283bb5477211963902e072b4aaf7095740435efeff567950dbcfe934dea2b`
- [x] E1 JPL collector foundation: preserve exact Horizons response bodies,
  queries, checksums, reviewed target seed, collector identity, and parsed CSV
  in immutable atomic snapshots; pass isolated artificial-source raw/typed
  accounting and response-integrity smoke verification
- [x] Add a source-specific JPL Horizons audit covering one-to-one projection/
  response identity, query and seed lineage, contained response paths, exact
  checksums and byte counts, and valid hyperbolic trajectory semantics
- [x] Acquire current exact-response Horizons snapshots for all 60 natural and
  11 artificial targets; emit machine deltas separating same-epoch JPL solution
  revisions from expected artificial-trajectory epoch changes
- [x] Preserve a syntax-derived Horizons `center_target_command` beside each
  exact center expression; verify the refreshed 60/11 snapshots change no
  scientific values and never require name-specific parent parsing
- [x] Run a production-shaped JPL Evidence Lake preview with both parsed and
  response-index artifacts; pass natural/artificial raw accounting, typed
  cooking, source audits, and clean deterministic reproduction independently
- [x] E3/E4 JPL cutover: collect a current natural snapshot and refresh the
  stale artificial trajectory tail, register both response-index artifacts,
  type and materialize scoped object/relation/orbit evidence, and keep operator
  seed claims distinct from values parsed from Horizons responses
- [x] E4 VSX pre-adapter audit: account all 10,304,568 pinned object rows;
  verify OID uniqueness, coordinates, statuses, flags, periods, field coverage,
  and public-name collisions without using names as source identity
- [x] E3/E4 VSX bibliography completion: pin and type the source-documented
  `refs.dat` object-to-bibcode table, add it to the release contract, and link
  only exact OID references before VSX E4 promotion
- [x] Add the official VSX `refs.dat.gz` endpoint to reproducible catalog
  acquisition and document that its 2022-era OID coverage is a partial
  historical bibliography rather than complete coverage of the 2026 objects
- [x] Extend the VSX audit through schema-driven bibliography typing: verify all
  830,415 exact pairs, preserve 2,080 historical links for 1,833 OIDs absent
  from the current object table, and retain 56 structurally noncanonical links
  across 9 distinct source strings without fabricated ADS URLs
- [x] Acquire and preview the July 21 three-artifact VSX release; pass complete
  raw/typed accounting, source audit, clean reproduction, and a stable-OID delta
  separating 47 additions, 8 removals, and 243 scientific revisions from
  lineage-only source-line reordering
- [x] E4 VSX checkpoint: cut over the checked-in registry, materialize all
  11,135,737 source records and 29 fields with release-scoped identity,
  astrometry, spectral classification, coherent variability, bibliography,
  explicit unresolved scope outcomes, and zero pending fields; pass source and
  generic artifact audits for build `d9780b76333132c0a05098b7`, then cleanly
  reproduce logical hash
  `1aa9577c875d2efcd6f11f59428c61f5197e184986ebd3e6ee2d372bb8891e36`
- [x] E4 Bailer-Jones distance checkpoint: materialize all 17,310,560 EDR3
  rows as coherent geometric and photogeometric posterior bundles; preserve
  case-distinct percentile fields, explicit interval-endpoint semantics,
  quality and bibliography lineage, and release-scoped EDR3 identity; pass
  bounded generic/source audits and exact clean logical-hash reproduction with
  zero pending fields or redundant excluded context
- [x] E4: preserve and exhaustively reconcile ORB6, DEBCat component physics,
  Gaia NSS fitted values, NASA uncertainties/limits/references/best-mass
  provenance, compact spin/activity, white-dwarf alternatives, cluster context,
  SNR flux, and TESS EB sector/flag/Tmag evidence
- [x] E4: reconcile default-disabled exoplanet lifecycle sources with M5.3 and
  retain negative evidence, conflicts, transitions, and tombstones
- [x] E5 foundation: compile immutable Gaia AP/NASA selected facts with coherent
  parameter-set decisions, exact evidence/derivation lineage, per-quantity
  authority, uncertainty semantics, and lower-authority-winner gates
- [x] E5 Gaia-source projection: select bounded coherent astrometry,
  photometry, radial-velocity, and diagnostic fields for current stars; enforce
  per-source coverage floors, partition existence/row accounting, and clean
  logical-hash reproduction
- [x] E5 distance projection and binding accountability: select bounded
  Bailer-Jones geometric/photogeometric posterior estimates through an explicit
  EDR3-to-DR3 release contract; preserve exact interval semantics and record an
  accepted, missing, or ambiguous outcome for every eligible source record
- [x] E5 spectroscopy atmosphere checkpoint: bind bounded APOGEE DR17, GALAH
  DR4, and LAMOST DR11 evidence; apply official/source-native quality gates,
  rank repeat observations deterministically, retain winner/runner-up quality
  lineage, and pass independent audit plus clean reproduction
- [x] E5 source-disposition accounting: classify all 38 accepted E4 sources as
  selected, explicitly non-selectable evidence, or an owned blocking policy;
  fail on omissions/conflicts and hash the ledger into compiler identity
- [x] E5 evidence-subject/classification checkpoint: bind source records,
  classification rows, and scoped parameter sets without scope leakage; select
  5,282 UltracoolSheet categorical facts and account every eligible subject
- [x] E5 white-dwarf applicability/model checkpoint: require `Pwd > 0.75`, bind
  every usable-model subject, choose one complete H/He/mixed atmosphere by
  published fit chi-square, preserve alternatives, and pass deterministic
  focused verification before the next batched full compile
- [x] E5 compiler performance accounting: record wall and CPU time, row counts,
  durable bytes, peak memory, and spill bytes for every source and compile,
  selection, derivation, integrity, export, hash, and promotion phase; publish a
  machine-readable timing report and optimize measured bottlenecks before the
  required clean reproduction
- [x] E5 end-to-end build performance report: rank every phase by wall and
  CPU time, distinguish cold and cached input verification, compare supported
  worker/memory profiles, retain query-plan evidence for the dominant costs,
  and document measured optimizations before local cutover in
  `docs/E5_BUILD_PERFORMANCE_2026-07-22.md`
- [x] E5 current-release selected-fact checkpoint: compile policy v12/compiler
  v11 against release set `6c19de054e9b807674c37d3c`, independently audit
  exact policy lineage and scientific gates, and cleanly reproduce all report
  sections and Parquet hashes for build `0a57f778ce13de1c2c800103`
- [x] E5 Gaia accepted-binding-cache experiment: run the complete 5.87-million
  object/89.07-million-fact projection, independently audit it, reject the
  cache after candidate insertion regresses from 540.0 to 661.5 seconds, and
  retain machine-readable timing/resource evidence
- [x] E5 deterministic-export experiments: measure one-pass partitioning,
  reject its byte-nondeterministic fast mode and its spill-heavy ordered mode,
  reject concurrent per-partition export after worse measured throughput, and
  retain the existing stable sequential export pending a better design
- [x] E5 relation endpoint foundation: bind both endpoints independently,
  retain exhaustive nonaccepted outcomes, preserve negative controls, forbid
  containment promotion, and pass independent deterministic artifact audit
- [x] E5 El-Badry wide-binary projection: account 1,116,713 relations and
  2,233,426 endpoints, retain `R_chance_align` as a non-probability confidence
  statistic, and identify 95,045 fully bound source-defined high-confidence
  relation-evidence rows without creating canonical hierarchy edges
- [x] E5 MSC component-scope foundation: account 6,937 WDS systems, 32,790
  release-scoped component identities, and 15,748 relations; retain missing
  endpoints and three source self-relations explicitly without creating CORE
  stars, hierarchy edges, or containment
- [x] E5 full MSC projection: bind every component parameter, classification,
  photometry, astrometry/motion, hierarchy, and orbit row to exact release-scoped
  component or relation evidence; keep relative separation context-only and
  preserve every nonaccepted scope outcome
- [x] E5 DEBCat component projection: account all 374 systems through exact
  priority-aware name resolution, require a unique WDS/period-compatible MSC
  relation for primary/secondary physics, keep integrated photometry and
  metallicity system scoped, and pass independent audit plus deterministic
  reproduction
- [x] E5 SB9 component/orbit projection: account all 4,079 relations through
  exact MSC `SB9_<sequence>` references, accept only one reference with two
  resolved endpoints, preserve missing/ambiguous/unresolved outcomes, and
  project component magnitudes, classifications, and all linked orbit solutions
- [x] E5 ORB6 component/orbit projection: account all 4,051 visual-orbit rows,
  resolve combined discoverer/pair designations through one exact WDS row and
  one WDS-qualified accepted MSC relation, retain every unresolved tail, reject
  system-level edge fallback, and pass independent audit plus deterministic
  reproduction
- [x] E5 SBX component/orbit projection: account all 4,080 systems and 8,160
  release-scoped components through exact Gaia DR3, official Gaia DR2-to-DR3,
  HIP, HD, and TIC system anchors; retain missing/conflicting identities,
  preserve astrometry as observation-target context, and forbid canonical
  component or hierarchy promotion
- [x] E5 WDS relation-context projection: account all 157,299 visual-pair
  summaries through the documented component notation and exact MSC endpoints;
  preserve opaque spectra, unspecified-band magnitudes, relative astrometry,
  epochs, positions, and source-convention proper motion as scoped context only
  and reject positional, system-level, and canonical-containment fallbacks
- [x] E5 Gaia NSS solution-context projection: account all 87,075 coherent
  source/model fits through exact Gaia DR3 observation-target identity; retain
  complete models, errors, correlations, diagnostics, frames, and references,
  require relation adjudication, and emit no fabricated companion, relation,
  containment edge, selected scalar, or simulation-ready orbit
- [x] E5 TESS EB target-context projection: account all 17,605 Villanova rows
  through exact TIC observation-target identity; preserve positive and negative
  membership, sectors, morphology, flags, Tmag, target physics, astrometry, and
  timing solutions; require relation adjudication and emit no component,
  containment edge, selected scalar, or simulation-ready orbit
- [x] E5 Hunt/Reffert cluster projection: bind source clusters only through
  exact published designations with inverse collision detection, select coherent
  posteriors only for one-to-one cluster identities, resolve Gaia DR3 member
  endpoints independently, preserve all published probabilities, and create no
  canonical membership containment
- [x] E5 extended-object projection: reconcile all Green SNR and OpenNGC-family
  evidence through exact catalog-source keys; retain explicit accepted,
  excluded, quarantined, and unresolved outcomes; and prevent every extended
  row from entering stellar selected facts
- [x] E5 JPL Horizons natural-object projection: bind targets and orbit centers
  independently through exact authoritative identifiers; keep the Solar System
  barycenter as a declared non-object reference origin; and preserve every
  selected osculating solution with all 12 standard numeric element fields and
  its exact epoch, frame, center, method, model, response, and physical-
  parameter lineage
- [x] E5 supplemental planet/TESS projection: account every Exoplanet.eu, HWC,
  OEC, targeted TIC, and TOI object; preserve lifecycle polarity, conflicts,
  coherent parameter and transit evidence, and explicit identity outcomes; link
  confirmed TOIs only through an accepted host plus unique period; and emit no
  canonical planet inventory mutation
- [x] E5 policy-batch accounting: assign every remaining blocking source to one
  dependency-ordered implementation batch, distinguish resolved sources from
  active blockers, and fail on duplicate, missing, or falsely completed work
- [x] E5 source-policy closure: account all 38 accepted E4 sources with 14
  quantity-selection policies and 24 explicit evidence projections or
  dispositions; complete all seven batches with zero blocking sources
- [x] E5 Gaia variability checkpoint: select source-native rotation,
  variability-statistic, and classification-membership evidence from its
  matching parameter-set kind; account all 592,197 subjects and pass two-pass
  focused verification before another full selected-fact build
- [x] E5 supplementary Gaia AP/FLAME checkpoint: preserve GSP-Phot library
  alternatives as evidence, select only official best-quality ANN and
  spectroscopic FLAME fallbacks, recover omitted main-AP alpha abundance,
  projected rotation, and gravitational redshift, and pass a focused A/B against
  current selected winners
- [x] E5 repeated-input verification optimization: fully byte-hash every pinned
  E4 input against its expected SHA on each invocation, parallelize independent
  hashes, and reuse an attestation within that process only while full stat
  identity remains unchanged
- [x] E5 variable-star classification checkpoint: select VSX source-native
  variability class and period only for 226,017 exact Gaia DR3 bindings, retain
  spectral/extrema/bibliography channels as evidence, keep GCVS/NSV nonselected
  until its release-native cross-identifiers resolve broadly, and pass focused
  compile, reproduction, artifact, scope, and timing gates
- [x] E5 identity/naming checkpoint: add reusable same-release identifier-bridge,
  multi-identifier consensus, identifier-claim selection, and duplicate-target
  ambiguity contracts; select 321,584 SIMBAD spectral classifications and 415
  WGSN proper names while deferring the Izar/Pulcherrima component collision
- [x] E5 compact-object scope checkpoint: audit all ATNF/McGill names through
  exact SIMBAD/Gaia identity, quarantine PSR J0437-4715 rather than leak pulsar
  facts onto its lone K-spectrum Gaia leaf, and retain both sources as complete
  evidence until distinct compact-object identity exists
- [x] E5 compact-object identity checkpoint: create 4,425 permanent
  release-scoped ATNF/McGill identities without reusing the six sign-erasing
  legacy keys; account every object as accepted, excluded, or missing against
  the 1,250-ly evidence envelope; select 156 source-backed facts for 22 accepted
  objects; preserve the J0437-4715 optical counterpart conflict as quarantine;
  and pass pinned compile, independent verification, and deterministic Parquet
  reproduction
- [ ] E5: implement versioned per-quantity selection policies that prefer
  coherent parameter sets, preserve alternatives/conflicts, and point every
  selected fact to exact evidence
- [x] E5: inventory every current derivation and presentation prior; record
  inputs, algorithm/applicability, uncertainty/confidence, provenance,
  supersession, implementation markers, and materialized-method accounting
- [ ] E5: extend lower-authority-winner gates across every selected domain so a
  fallback cannot win despite acceptable higher-authority evidence
- [ ] E5: make HZ, classifications, planet categories, simulations, search,
  map, API, tags, and future AAA packets consume the shared selected-fact
  projection rather than independent scientific fallback logic
- [x] E6 shadow foundation: materialize all accepted E5 projections into
  an immutable unserved CORE/ARM/hierarchy/DISC candidate; preserve exact
  inventory/hierarchy/planet lifecycle, attach fact lineage, pass independent
  audit, and pass isolated cryptographic logical-hash reproduction
- [x] E6 selected classification consumers: centralize canonical-star display
  classification on selected facts, reconnect canonical and inferred leaves to
  release-scoped MSC component spectra/masses, retain legacy compatibility only
  for pre-E6 builds, and pass full A/B with zero known-to-unknown regressions
- [x] E6 immutable consumer integration: run shared parameter/classification
  consumers and complete hierarchy-leaf classification before metadata,
  checkpoint, product hashing, manifest creation, and atomic promotion; include
  auxiliary compiler hashes in build identity and independently audit the
  materialized inventories and lineage
- [x] E6 integrated candidate reproduction: rebuild policy
  `2026-07-22.e6-shadow.3` in isolated USB scratch, independently audit it, and
  match build identity plus all eighteen generated/mutated logical table hashes
- [x] E5 NASA host-policy preflight: bind preserved NASA reference-specific,
  stellarhosts, and composite host parameter sets to canonical stars through
  authoritative identifiers; select coherent atmosphere/fundamental facts and
  log-luminosity semantics; account for every host binding; and pass isolated
  scope, lineage, and duplicate-selection gates
- [x] E5 NASA host selected-fact candidate: pass all 103 instrumented compiler
  phases and the object-scoped independent audit as unserved build
  `16708b8ed193aeae9b2ab995`; preserve the per-step performance report and
  exact v12 authority-impact accounting
- [x] E5 NASA host clean reproduction: rebuild candidate
  `16708b8ed193aeae9b2ab995` in isolated USB scratch, match every logical report
  section and hash with no differences, remove scratch, and record all 103 phase
  timings plus the 24:49 wall-clock runtime
- [x] E5 GSP-Phot distance-policy preflight: recognize the 1,160-row legacy tail
  as official Gaia GSP-Phot posterior model evidence, select it under a distinct
  `distance_gspphot_pc` quantity after Bailer-Jones posteriors, recover the full
  tail, and reject inverse-parallax substitution for its S/N 2.1-7.4 parallaxes
- [x] E5 v14 selected-fact checkpoint: compile and independently audit unserved
  build `929bf92b4c5dbd5aef7e5972` with 123,289,311 selected facts, then cleanly
  reproduce all 103 phases, every compared report section, and logical content
  hash `af1155454dc91f8d653735e81ae8c153cdb5c7454e93ea4ab69301ea59d4be1f`
- [x] E5 v15 proxy-scope checkpoint: compile and independently audit unserved
  build `fa4aaed18aebcffb8632d978` with 123,288,872 selected facts and
  43,060,870 decisions; account the net 439-fact delta as 452 invalid proxy
  winners removed plus 13 object-owned collision winners restored; cleanly
  reproduce all 103 phases and logical hash
  `1b4fd75c00f9a21deb69e0c2136c9c39f7b25bb082b3bd378c260487d417685e`
- [x] E5/E6 NASA host cutover: pass the complete instrumented E5 compile and
  clean E5 reproduction plus E6 shadow rebuild with the host and GSP-Phot
  policies, then account for the legacy temperature/mass/radius/luminosity
  tail as selected, superseded coherent lower-authority alternatives, or
  genuine unresolved gaps without constructing incompatible per-field
  composites
- [x] E5 MSC case-scope checkpoint: preserve exact case for physical component
  identity, distinguish subsystem `AB` from star `Ab`, eliminate 238 case-fold
  collision groups, fail on duplicate accepted WDS/source-label keys, and pass
  independent audit plus clean reproduction as component artifact
  `67fea5f99500b57419ebdeb0` without a named-system transform
- [x] E6 corrected v6 shadow checkpoint: compile selected-fact v15 and
  component-scope v9 as `e6_cfcdf2d9add2cd7e2b96af68_shadow`; pass all 194
  independent checks, clean logical reproduction, selected-consumer A/B,
  parameter-loss accounting, and coolness A/B
- [x] E6 corrected public/map checkpoint: preserve all 5,869,091 public systems
  in `e6_cfcdf2d9add2cd7e2b96af68_public` and pass exact 100/250/500/1,000-ly
  coverage, checksum, name, representative, and badge verification with zero
  missing or extra systems
- [x] E6 simulation-cache performance checkpoint: measure the same 1,000-system
  priority selection cold and warm; retain separate machine reports; account
  1,000 generated scenes with zero failures in 22:31.97 and 1,000 reused scenes
  in 1.86 seconds; identify repeated scene assembly as the bottleneck
- [x] E6 corrected API/search checkpoint: pass focused tests, direct alias
  materialization, complete API integration, and the strict twelve-system
  search/detail/hierarchy/simulation benchmark against the unpromoted v6 public
  build with no stale-slice or preview warnings
- [x] E6 map determinism checkpoint: rebuild all four public radii in isolated
  USB scratch with identical inputs, match every manifest hash exactly, retain
  the machine comparison, and remove scratch
- [x] E6 production browser checkpoint: pass twelve applicable tiled-map
  Playwright cases and all 312 fixed performance checks across 500/1,000-ly
  cold, warm, rapid-direction, desktop, mobile, and Photon profiles without
  raising budgets; retain development-runtime heap misses as diagnostics only
- [ ] E6: produce a deterministic shadow CORE/ARM/hierarchy/DISC build and
  public slice with complete inventory, identity, evidence, fallback, HZ,
  planet, orbit, API/search/map/simulation, storage, and performance A/B reports
- [ ] E6/E7 build-time closeout: publish a per-step wall/CPU/memory/I/O/artifact
  report for selected consumers, DISC, public slice, map tiles, simulation
  scenes, verification, promotion, and rollback; rank measured bottlenecks and
  record before/after evidence for every accepted optimization
- [x] E6 shadow retention: add an exact-candidate dry-run that protects the
  current candidate, served/current/rollback links, live processes, transitive
  inputs, and reports; review its set hash before reclaiming approximately 66
  GiB of superseded E6 product directories from `/data`
- [ ] E6: create or bind permanent non-Gaia compact-object identities within
  the uncertainty-aware ingestion envelope; keep pulsars/magnetars distinct
  from optical companions, then rerun the compact-scope audit and add typed
  quantity policies only for safe leaves
- [ ] E6: review and account for every scientific delta using reusable policy;
  preserve canonical planet/status integrity and prohibit named-object
  transforms used only to satisfy goldens
- [x] E7 pre-promotion plan: machine-account seven legacy path families, retain
  permanent identity work, prohibit cleanup before rollback, define atomic
  local cutover and timing steps, and verify a release-scoped Gaia DR4 adapter
  contract with 19/19 plan checks and zero unowned derivation markers
- [ ] E7: locally promote the accepted Evidence Lake v2 build atomically with a
  tested rollback, then retire or formally deprecate duplicate legacy
  collectors, cookers, schemas, and selection/fallback paths
- [ ] E7: update DATA_SOURCES, schema, ingest, retention, API, iteration-history,
  operations, and Gaia DR4 adapter documentation before any public deployment

### E2. Later Public Evidence and Observation Tools

- [ ] Add a collapsed evidence inspector low on the System Page after M8.3c:
  show the selected value and reason first, then expandable competing values,
  uncertainty, limits, conflicts, source/model/reference, component scope, and
  lineage through a bounded/paginated API
- [ ] Build a reusable source-attributed observation-product viewer contract
  for images, spectra, exoplanet atmosphere spectra, and light curves without
  placing bulk products in hot served databases
- [ ] Build the first interactive spectrum analyzer with pan/zoom, unit modes,
  uncertainty, line overlays, radial-velocity/redshift context, comparison
  spectra, and plain-language element-identification explanations
- [ ] Add scientifically safe learning missions for identifying spectral lines,
  finding transits, and comparing observations with models; keep scores/progress
  in presentation/community state and never promote visitor guesses to evidence

### E3. Remaining Product Roadmap

- [x] Refresh map/system Playwright expectations for current naming, preview,
  Explorer, and fallback policies; full local map suite passes on photon
- [x] Publish refreshed MSC archive into local Spacegate catalog mirror
- [ ] Sync refreshed MSC catalog mirror to `spacegates.org`
- [ ] Restore deterministic snapshot-manifest coverage in the current public
  side-sliced build; the July 12 local side build returns zero map snapshots
- [x] Reconcile initial system-simulation orbital-source policy across NASA
  `ps`/`pscomppars`, Gaia NSS, WDS/ORB6, SBX, MSC, and JPL Horizons/SBDB
- [x] Ingest alternate planet orbital solutions from NASA `ps` and preserve
  ranked solution candidates instead of relying only on `pscomppars` defaults
- [x] Live System Preview v0.2 renderer-ready payload, multi-star binary visual
  groups, and preview provenance pills
- [ ] Add reviewed-curation workflow and broader batch policy for
  `disc.simulation_assumptions`
- [ ] Live 3D system preview/simulation scenes with deterministic static snapshot fallback
- [ ] Rim/worldbuilding overlay expansion without core contamination
- [x] Cross-layer system graph contract documented (containment spine + relation graph, layer ownership, generator compatibility)
- [ ] Procedural system generator (rim-authored, seed/versioned, graph-safe) after M6/M7/M8/M9
