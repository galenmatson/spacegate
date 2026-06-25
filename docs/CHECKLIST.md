# Spacegate Delivery Checklist (Gaia-First)

This checklist tracks implementation against `docs/PROJECT.md` and the Gaia-first program direction.

## A) Platform Foundations

- [x] Galaxy/core/halo/arm/disc/rim layer model documented (legacy aliases retained during transition)
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

### C3. Phase B - Core/Halo Product Slice

- [x] Define deterministic `core_product_slice` policy over backbone
- [x] Materialize complementary `halo` artifacts from `galaxy` + sliced `core` builds
- [ ] Add explicit deep-query plumbing over `halo`/`galaxy` in API/UI
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
- [ ] Benchmark system validation set passes (Castor, 16 Cyg, Sol-neighborhood checks)
- [ ] Expand multiplicity goldens with AR Cas / HD 221253 and Nu Scorpii / HD 145502; keep HD 235299 and Gamma Cas as adjudication/watchlist systems until evidence policy is explicit
- [x] Suppress singleton MSC leaf inference in canonical hierarchy to avoid Bet Mon-style overfit

### C5. Phase D - Crosswalk and Naming

- [x] Implement replacement crosswalks for names/aliases/legacy IDs
- [x] Restore broad cross-catalog identifier coverage in served rows (Gaia/HIP/HD/WDS and other selected major IDs)
- [x] Add deterministic identifier reconciliation with ambiguity quarantine + QC collision gates
- [x] Add duplicate-trap stewardship report (exact-key + near-pair checks) with optional QC high-confidence gate
- [x] Materialize narration-oriented `arm.stellar_parameters` rows from Gaia DR3 + NASA host-star payloads
- [x] Materialize `system_search_terms` and system-level browse/search facet columns for public-host performance
- [x] Use generic `arm` hierarchy payloads in detail/search paths and descendant-aware star counts for multiplicity-heavy systems
- [ ] Add common-name authority merge policy (precedence, dedupe, provenance, conflict handling)
- [ ] Add benchmarked common-name fuzzy matching for bright/common objects (for example Aldebaran-class lookups) with alias-aware ranking
- [x] Let member-star names participate in search result display/ranking so variable-star lookups like `AR Cas` title the correct system card
- [x] Add authoritative Sol-system bootstrap overlay (Sun + major planets + canonical aliases) so Sol is present and complete even when external catalogs are incomplete
- [x] Implement Sol S2 arm hierarchy (moon nodes, satellite orbit edges, Earth-Moon/Pluto-Charon barycenters) with verify gates
- [x] Align Sol canonical class storage to source-faithful `dwarf_planet` semantics while preserving UI structural supergroup `subplanet`
- [x] Implement Sol S3 initial named small-body arm layer (asteroid/TNO/comet families) with staleness metadata + verify gate
- [x] Expand Sol S3 deterministic small-body coverage (broader asteroid/TNO families, including Ixion-class objects)
- [x] Implement Sol S3 arm-to-halo projection path for sliced builds
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
- [x] Implement `spacegate_hab_score` + confidence/reasons metadata
- [x] Implement element-richness proxy tags from stellar spectroscopy/metallicity inputs
- [ ] Add API filters/toggles for controversial lifecycle state and habitability range
- [ ] Add UI habitability slider + top-N shortcut
- [ ] Add optional filter/display for element-richness class
- [ ] Investigate planet equilibrium-temperature coverage gaps surfaced in
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

## E) Product Roadmap (Post-Core Migration)

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
- [x] Canonical ingest design doc + deterministic adjudication queue baseline for sloppy-system triage
- [x] Canonical ingest artifacts for normalized sources, identity graph, and canonical reduction
- [x] Canonical hierarchy artifact from canonical objects + arm role evidence
- [x] Canonical build emitter + runtime canonical-hierarchy fallback
- [ ] Agent evidence-link pipeline in `disc` (citations/source manifests backing factsheets and narratives)
- [ ] Agent adjudication tables in `arm` for ambiguity resolution and missing-field proposals
- [x] Initial role-based agent eval harness with seed golden cases and quarantined anomaly inbox reports
- [ ] Local inference adjudicator cook-off with pinned TurboQuant KV-cache evaluation for long-context profiles
- [ ] Advanced system hierarchy navigation UX (systems of systems)
- [ ] 3D map runtime integration over Gaia-first slice
- [ ] Rim/worldbuilding overlay expansion without core contamination
- [x] Cross-layer system graph contract documented (containment spine + relation graph, layer ownership, generator compatibility)
- [ ] Procedural system generator (rim-authored, seed/versioned, graph-safe) after M6/M7/M8/M9
