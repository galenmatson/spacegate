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
- [ ] Add common-name authority merge policy (precedence, dedupe, provenance, conflict handling)
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
  component link
- [x] Rebuild core/ARM with accepted supplements enabled; local build
  `20260630T_sim_beta_data_foundation` verifies Sirius as a WDS-backed A/B
  system and passes strict compact-alias safety
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
- [x] Photon bootstrap/readiness audit scripted (`scripts/audit_photon_bootstrap.sh`)
- [x] Logged database build wrapper preserves build exit status and host state-dir environment (`scripts/build_database_logged.sh`)
- [x] Photon generous retention and bulk research storage policy documented (`/mnt/space/spacegate`)
- [x] Public antiproton deployment runbook documents sliced DB publish, activation, SSH cooldown, verification, and rollback (`docs/PUBLIC_DEPLOYMENT.md`)
- [x] API Docker runtime runs non-root with generated-state permission normalization
- [x] API Docker runtime drops capabilities, blocks privilege escalation, and uses a read-only root filesystem with explicit tmpfs scratch mounts
- [ ] Dedicated `spacegate-run` service user and shared group model for Admin/API runtime
- [ ] Antiproton runtime identity cleanup: migrate `/srv/spacegate/data` from legacy `ubuntu:ubuntu` ownership to a dedicated non-login `spacegate` runtime user with shared `spacegate` group access; keep `sgdeploy` as deploy/restart account only
- [ ] Reassess antiproton `sgdeploy` Docker-group membership and replace with a narrower deploy control path if feasible
- [ ] Public-edge Admin route hardening: reverse-proxy gate `/admin` and
  `/api/v2/admin/*` with VPN/Tailscale, IP allowlist, or basic auth; optional
  obscure Admin path only as bot-noise reduction
- [ ] Secret handling hardening beyond Compose-expanded environment variables
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
- [ ] Enrich member-star alias/search-term coverage for complex systems such as
  V1054 Oph so names like Gliese 643 and VB 8 resolve to the containing system
  without promoting member names to false system-level canonical names
- [ ] Expand the Live-WebGL runtime manager with richer admin/dev telemetry,
  observed browser context limits, and real-device automatic quality tuning
- [ ] 3D map tiled 250 ly / 500 ly / 1000 ly runtime integration over
  Gaia-first slice
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
- [ ] Build Concept Tag Foundation: tag priority tiers, clickable concept-page
  hooks, complete tooltip/popover audit, and first `/concepts/:slug` pages
- [ ] Integrate reviewed AI Astronomy Agency public narration slots into
  Star Search v2 system pages without mixing unreviewed generated prose into
  canonical science
- [ ] Authenticated admin map overlay for per-system/object review controls,
  evidence portfolio access, and AAA research promotion
- [ ] Extended-object map layer for Messier/NGC/IC-style landmarks, nebulae,
  clusters, and background-sky context
- [x] Refresh MSC multiplicity ingest from the upstream June 19, 2026 archive
  and rerun hierarchy/orbit goldens
- [x] Add source-evidence utilization audit for preserved MSC `sys.tsv`/`orb.tsv`
  rows that are not yet normalized into ARM orbit edges or simulator contracts
- [x] Materialize deterministic preserved MSC orbit-detail rows into ARM
  `orbit_edges`/`orbital_solutions` where endpoint reconciliation is
  deterministic, with 70 Oph as a visible benchmark
- [ ] Review the remaining MSC `orb.tsv` diagnostics that still do not resolve
  to ARM orbit edges after source-native endpoint normalization
- [ ] Define WDS pair-observation utilization policy: preserve observation
  history, but decide when WDS component-pair rows should create ARM support
  pair entities, orbit/projection evidence, or diagnostics only
- [x] Add spectral-subclass-aware main-sequence stellar parameter priors for
  simulation support, guarded against giants, subgiants, remnants, and compact
  objects, without writing those priors into core
- [x] Add ARM-scoped runtime luminosity derivation for hierarchy-rendered stars
  with radius and effective temperature so HZ/temperature-line overlays do not
  lose systems such as TRAPPIST-1
- [x] Refresh map/system Playwright expectations for current naming, preview,
  Explorer, and fallback policies; full local map suite passes on photon
- [x] Publish refreshed MSC archive into local Spacegate catalog mirror
- [ ] Sync refreshed MSC catalog mirror to `spacegates.org`
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
