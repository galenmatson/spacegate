# Evidence Lake E7 Cutover and Gaia DR4 Plan

## Decision

E7 is a gated replacement of the authoritative build path, not a cleanup pass.
No legacy build code or artifact is deleted before the accepted Evidence Lake
candidate has been promoted locally, exercised through the production service
topology, rolled back to the stability build, and promoted again.

The current E6 v7 public candidate
`e6_95e7af54d69f3d9602d81e5b_public` still composes permanent inventory and
identity from stability build `20260717T0614Z_f452835_side`. Therefore the bootstrap cookers,
`ingest_core.py`, and `build_arm.py` remain required stability inputs. Calling
Evidence Lake v2 the only production compilation path before removing that
dependency would be incorrect. The canonical identity reducer is permanent
Spacegate work and will be adapted to E2 rather than retired.

Machine contracts:

- `config/evidence_lake/e7_legacy_path_inventory.json`
- `config/evidence_lake/gaia_dr4_adapter_plan.json`
- `config/evidence_lake/e0_e7_acceptance.json`
- `config/evidence_lake/e7_permanent_identity_seed.json`
- `config/evidence_lake/e7_identity_vocabulary_seed.json`
- `config/evidence_lake/e7_stability_table_migration.json`
- `config/evidence_lake/e5_system_placement_policies.json`
- `scripts/verify_e7_cutover_plan.py`
- `scripts/audit_evidence_lake_completion.py`
- `scripts/compile_permanent_identity_seed.py`
- `scripts/verify_permanent_identity_seed_reproduction.py`
- `scripts/compile_permanent_identity_vocabulary.py`
- `scripts/verify_permanent_identity_vocabulary.py`
- `scripts/audit_e7_stability_table_migration.py`
- `scripts/compile_selected_system_placements.py`
- `scripts/verify_selected_system_placements.py`
- `scripts/verify_selected_system_placement_reproduction.py`

The current completion audit passes all 35 pinned checkpoint checks and reports
`incomplete`. Its six explicit gates are the clean pinned-input authoritative
entrypoint, shared selected-fact consumer cutover, operator scientific A/B
acceptance, local atomic promotion/rollback/re-promotion, legacy retirement,
and the remaining promotion/rollback timing rows. This is the intended state;
the report must not translate passing E0-E6 evidence into premature E7 cutover.

Permanent identity seed `5c878083872c738415971864` is the one-time bridge from
the reviewed canonical hierarchy to the clean compiler. It contains only stable
identity, containment, component case, display labels, relationship confidence,
and lineage; its policy explicitly prohibits scientific scalar columns and
named-object conditions. It is not scientific authority. Production and clean
USB-backed compiles produce byte-identical Parquet products. Future canonical
compilers may consume this retained seed but may not reopen the stability CORE,
ARM, hierarchy, or DISC databases.

Vocabulary seed `6b4fb210e1b1bcf61299fe7f` is the corresponding one-time
bridge for 1,026,480 public aliases. It is identity-only, maps every row to a
permanent object and system key, and reproduces byte-for-byte without retaining
legacy numeric target IDs. The stability-table migration audit separately owns
all 74 tables and currently exposes 61 remaining replacements; therefore these
two migration seeds do not yet close the clean-entrypoint gate.

## Cutover Sequence

1. **Accept E6 science.** Close all inventory, identity, component-scope,
   selected-fact, fallback, HZ, planet, orbit, cluster, variability,
   compact-object, extended-object, API/search/map/simulation, storage, and
   performance deltas through reusable policy and evidence lineage.
2. **Freeze the candidate.** Record repository revision, source-release set,
   selected-fact and component-policy artifacts, shadow/public product hashes,
   reports, service configuration, and rollback target. Run retention dry-run;
   no cleanup is part of promotion.
3. **Promote locally and atomically.** Update only the local served pointer by
   the existing promotion mechanism. Verify pointer target, product hashes,
   database metadata, immutable tile manifests, service health, and API/UI
   build identity before accepting traffic.
4. **Exercise the production topology.** Rebuild/restart local containers and
   rerun strict build, API/search, known-system, map, simulation, Admin, desktop,
   mobile, and performance gates. Record promotion time and cold-cache effects.
5. **Rollback drill.** Atomically restore
   `20260717T0614Z_f452835_side`, restart services, and prove the same health and
   build-identity gates. Record rollback time. Then re-promote the accepted E7
   build and repeat the short smoke set.
6. **Cut over the compiler entrypoint.** Add one top-level Evidence Lake driver
   that starts from pinned raw/typed releases and invokes E2, accepted E4 release
   set, E5 selection/derivation, E6 products, verification, and optional
   promotion. It must not read legacy cooked science products or a stability
   CORE/ARM as scientific authority.
7. **Deprecate by ledger.** Transition each entry in the legacy-path inventory
   only after its own gate passes. Preserve permanent identity work and labeled
   presentation transforms. Keep compatibility wrappers during the rollback
   window; remove them only after operator documentation and automation no
   longer reference them.
8. **Retire artifacts through retention.** Delete only specifically enumerated,
   reproducible, superseded cooked/build artifacts after a reviewed dry-run.
   Preserve raw and typed evidence, manifests, citations, published reports,
   served/rollback builds, and transitive inputs.

No antiproton deployment is part of this sequence. Public deployment requires
a later explicit operator checkpoint after local cutover is stable.

## Build-Time Closeout

`docs/E6_BUILD_PERFORMANCE_2026-07-22.md` is the canonical timing report. E7
adds promotion, container-restart, production smoke, rollback, and re-promotion
rows. The report must keep wall/CPU/RSS/I/O/output measurements where the runner
exposes them and may not hide cold work inside a warm-cache total.

The measured optimization order is:

1. content-addressed program-level E5 intermediates and reusable identity
   outcomes;
2. remove broad simulation-scene prewarming from the critical build path and
   use the bounded Admin/runtime cache;
3. prove a public-slice full-identity reuse path before avoiding its 195-second
   ARM reconstruction;
4. precompute tile display names and add bounded deterministic encoding workers;
5. optimize smaller E6 verification phases only after the dominant costs.

Every accepted optimization requires unchanged scientific logical hashes,
coverage/accounting reports, and a measured before/after row.

The first E7 clean-compiler optimization is accepted for selected system
placements. Baseline build `9ccc087defca7aebc5b77d6a` took 103.10 seconds and
peaked at 26.16 GiB. Reading deterministic per-quantity Parquet projections and
removing repeated winner sorts produces build `22e9a59dd02484454a629df7`
in 63.24 seconds at 17.42 GiB. The final lineage review also replaced a
provisional SBX release label with the registered release and preserved source
position epochs; geometry and winner identity are unchanged. Its isolated
compile plus independent audit takes 71.18 seconds and leaves no USB scratch
residue. The next measured compiler
work should target shared selected-fact projection and output materialization;
immutable attestation already fell from 22.72 to 6.25 seconds and is no longer
the dominant placement phase.

Clean identity/search build `9c2d08086275ead386f71bf7` takes 68.23 seconds at
17.27 GiB peak RSS. Its leading phases are index construction (14.9 seconds),
canonical Parquet export (12.5), search materialization (9.9), and hierarchy
materialization (9.8). Isolated compile plus independent verification takes
74.63 seconds. Preserved insertion order costs roughly 6-8 seconds and is
accepted because it makes every canonical Parquet byte-identical. DuckDB
containers remain logical query projections, not deterministic scientific
serialization. This measured step is not the hour-scale critical-path source;
the selected-science, public-slice, map-tile, and scene phases remain the next
optimization targets.

## Gaia DR4 Adapter

ESA currently schedules Gaia DR4 for December 2, 2026. The date and evolving
content remain external release metadata, not assumptions embedded in code:
<https://www.cosmos.esa.int/web/gaia/release>.

DR4 is a new release-scoped evidence family. It does not replace DR3 in place.
`gaia_dr3_source_id` and `gaia_dr4_source_id` are distinct namespaces connected
only through the official DR3-to-DR4 neighborhood/crossmatch and reviewed graph
policy. Permanent Spacegate object IDs survive Gaia splits, merges, missing
sources, and future releases.

The adapter follows six stages:

1. register actual DR4 schemas, fields, authority roles, units, frames, epochs,
   quality flags, licenses, and retrieval contracts;
2. acquire a new uncertainty-aware spatial envelope with byte-identical TAP
   responses and exact query lineage;
3. type each table independently and pass raw-to-typed reproduction;
4. build bidirectional provenance-bearing DR3/DR4 transition candidates and
   account every accepted, missing, excluded, ambiguous, or quarantined target;
5. add E4 evidence adapters and per-quantity E5 policies while preserving DR3
   alternatives and coherent parameter sets;
6. run a DR3-reference/DR4-candidate scientific A/B, local promotion, and
   rollback before changing the public backbone.

Do not predict DR4 table names or copy DR3 quality rules into production before
the archive publishes the actual schemas. Observation-heavy products should be
indexed comprehensively and cached on demand rather than mirrored without a
measured use case.
