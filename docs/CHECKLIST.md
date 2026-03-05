# Spacegate Delivery Checklist (Gaia-First)

This checklist tracks implementation against `docs/PROJECT.md` and the Gaia-first program direction.

## A) Platform Foundations

- [x] Core/bulge/disc/rim layer model documented (legacy aliases retained during transition)
- [x] Immutable build output model (`out/<build_id>`, promoted `served/current`)
- [x] Deterministic `download -> cook -> ingest -> promote -> verify` scripts
- [x] Provenance gate and QC report emission
- [x] Admin control plane + audit trail baseline
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
- [ ] Bulge (`aux`) artifact contract and promotion rules documented (`aux.duckdb` / compatibility aliasing)
- [ ] API contract review completed for Gaia-first field semantics

### C2. Phase A - Gaia Backbone Pilot

- [ ] Implement Gaia backbone downloader/cooker/ingest path (`gaia_backbone`)
- [ ] Emit `gaia_backbone_report.json` (counts, quality bands, runtime, storage)
- [ ] Add build metadata for astrometry quality policy and boundary strategy
- [ ] Verify deterministic reruns for pinned Gaia inputs

### C3. Phase B - Core Product Slice

- [ ] Define deterministic `core_product_slice` policy over backbone
- [ ] Materialize slice artifacts and serve default API from slice
- [ ] Add explicit deep-query mode against backbone
- [ ] Validate p95/p99 search and detail latency on proton

### C4. Phase C - Multiplicity Reintegration on Gaia IDs

- [ ] Reattach NSS evidence against Gaia backbone IDs
- [ ] Reattach MSC (optional/default-off) against Gaia backbone IDs
- [ ] Reattach WDS/ORB6 evidence against Gaia backbone IDs
- [ ] Implement/verify hierarchy confidence tiers
- [ ] Benchmark system validation set passes (Castor, 16 Cyg, Sol-neighborhood checks)

### C5. Phase D - Crosswalk and Naming

- [ ] Implement replacement crosswalks for names/aliases/legacy IDs
- [ ] Preserve or improve host matching quality for planets
- [ ] Preserve or improve user-facing lookup ergonomics

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
