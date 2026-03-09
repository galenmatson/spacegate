# Spacegate Delivery Checklist (Gaia-First)

This checklist tracks implementation against `docs/PROJECT.md` and the Gaia-first program direction.

## A) Platform Foundations

- [x] Galaxy/core/halo/arm/disc/rim layer model documented (legacy aliases retained during transition)
- [x] Immutable build output model (`out/<build_id>`, promoted `served/current`)
- [x] Deterministic `download -> cook -> ingest -> promote -> verify` scripts
- [x] Provenance gate and QC report emission
- [x] Admin control plane + audit trail baseline
- [x] Admin dataset status panel (runtime/storage/quality metrics) for performance diagnostics
- [x] Admin dataset slicer panel (preview + policy-driven sliced rebuild action)
- [x] Coolness scoring pipeline with versioned profiles
- [x] Snapshot generation pipeline baseline

## B) Current Production Stability

- [x] Public deployment path active and reproducible
- [x] Promotion flow auto-scores coolness by default
- [x] Multiplicity support catalogs wired (NSS, WDS, ORB6; MSC optional)
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
- [ ] Arm (`aux`) artifact contract and promotion rules documented (`aux.duckdb` / compatibility aliasing)
- [ ] API contract review completed for Gaia-first field semantics

### C2. Phase A - Gaia Backbone Pilot

- [x] Implement Gaia backbone downloader/cooker/ingest path (`gaia_backbone`)
- [x] Emit `gaia_backbone_report.json` (counts, quality bands, runtime, storage)
- [ ] Add Gaia TAP fetch completeness guard (detect and fail on sync truncation / partial bucket responses)
- [ ] Add build metadata for astrometry quality policy and boundary strategy
- [ ] Verify deterministic reruns for pinned Gaia inputs
- [x] Ingest Gaia DR3 astrophysical classification probabilities needed for remnant safety (`classprob_dsc_*_whitedwarf`, ESP-ELS families)
- [x] Implement remnant classification invariant gate (explicit remnant evidence must override temperature fallback)
- [x] Add compact/superstellar side catalogs (ATNF pulsars, McGill magnetars, open clusters, Galactic SNR)
- [x] Tag Gaia stars with open-cluster memberships where catalog IDs overlap (probability thresholded)

### C3. Phase B - Core/Halo Product Slice

- [x] Define deterministic `core_product_slice` policy over backbone
- [x] Materialize complementary `halo` artifacts from `galaxy` + sliced `core` builds
- [ ] Add explicit deep-query plumbing over `halo`/`galaxy` in API/UI
- [ ] Enforce profile-specific SLO gates during promote
- [ ] Validate p95/p99 search and detail latency on proton

### C4. Phase C - Multiplicity Reintegration on Gaia IDs

- [ ] Reattach NSS evidence against Gaia backbone IDs
- [ ] Reattach MSC (optional/default-off) against Gaia backbone IDs
- [ ] Reattach WDS/ORB6 evidence against Gaia backbone IDs
- [ ] Implement/verify hierarchy confidence tiers
- [ ] Benchmark system validation set passes (Castor, 16 Cyg, Sol-neighborhood checks)

### C5. Phase D - Crosswalk and Naming

- [x] Implement replacement crosswalks for names/aliases/legacy IDs
- [x] Restore broad cross-catalog identifier coverage in served rows (Gaia/HIP/HD/WDS and other selected major IDs)
- [x] Add deterministic identifier reconciliation with ambiguity quarantine + QC collision gates
- [ ] Add common-name authority merge policy (precedence, dedupe, provenance, conflict handling)
- [ ] Preserve or improve host matching quality for planets
- [x] Preserve or improve user-facing lookup ergonomics

### C6. Phase E - AT-HYG Retirement

- [ ] Parallel-run comparison (legacy vs Gaia-first) report generated
- [ ] AT-HYG removed from canonical inventory path
- [ ] AT-HYG remaining usage limited to optional compatibility tooling or removed entirely
- [ ] Deprecation cleanup (unused code/data/docs paths) completed

## D) Security and Data Hygiene

- [ ] No default production dependency on insecure transport sources
- [ ] Source mirror strategy documented for fragile external dependencies
- [ ] Retention policy documented (builds, reports, backups, large catalogs)
- [ ] Automated stale build cleanup policy scripted and tested

## E) Product Roadmap (Post-Core Migration)

- [ ] External reference link pipeline (curated authority sources)
- [ ] Factsheets + exposition generation with confidence metadata
- [ ] Advanced system hierarchy navigation UX (systems of systems)
- [ ] 3D map runtime integration over Gaia-first slice
- [ ] Rim/worldbuilding overlay expansion without core contamination
