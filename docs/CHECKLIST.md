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
- [x] Reattach SBX spectroscopic-binary evidence against Gaia/HIP/HD IDs (default-on toggle: `SPACEGATE_ENABLE_SBX`)
- [ ] Implement/verify hierarchy confidence tiers
- [x] Draft golden-system multiplicity exam harness (`docs/MULTIPLICITY_GOLDENS.md`, `scripts/verify_multiplicity_goldens.py`, Castor fixture)
- [x] Verify path runs multiplicity goldens by default with required arm tables (`--require-arm`)
- [ ] Benchmark system validation set passes (Castor, 16 Cyg, Sol-neighborhood checks)

### C5. Phase D - Crosswalk and Naming

- [x] Implement replacement crosswalks for names/aliases/legacy IDs
- [x] Restore broad cross-catalog identifier coverage in served rows (Gaia/HIP/HD/WDS and other selected major IDs)
- [x] Add deterministic identifier reconciliation with ambiguity quarantine + QC collision gates
- [x] Add duplicate-trap stewardship report (exact-key + near-pair checks) with optional QC high-confidence gate
- [ ] Add common-name authority merge policy (precedence, dedupe, provenance, conflict handling)
- [x] Add authoritative Sol-system bootstrap overlay (Sun + major planets + canonical aliases) so Sol is present and complete even when external catalogs are incomplete
- [ ] Preserve or improve host matching quality for planets
- [x] Promote exoplanet host labels for Gaia-fallback star/system display names (TRAPPIST/Kepler/TOI/WASP family and peers)
- [x] Preserve or improve user-facing lookup ergonomics

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

## E) Product Roadmap (Post-Core Migration)

- [ ] External reference link pipeline (curated authority sources)
- [ ] Factsheets + exposition generation with confidence metadata
- [ ] Advanced system hierarchy navigation UX (systems of systems)
- [ ] 3D map runtime integration over Gaia-first slice
- [ ] Rim/worldbuilding overlay expansion without core contamination
