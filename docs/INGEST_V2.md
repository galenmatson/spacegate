# Spacegate Ingest V2 Plan

This document defines the clean-slate canonicalization pipeline that will replace the remaining AT-HYG-era assumptions in the current ingest flow.

Status:
- architecture approved
- implementation started in `feature/ingest-v2-canonicalization`
- agent integration is explicitly **not** a prerequisite for ingest v2

## Goals

- normalize every source natively before cross-source reconciliation
- build canonical stars/systems/planets from explicit identity evidence instead of patching late duplicates
- materialize hierarchy only from canonical objects
- quarantine ambiguous cases instead of guessing
- emit a deterministic adjudication queue that future agents or humans can work from

## Non-Goals

- no direct agent writes into `core`
- no one-off corner-case patches in public canonical tables
- no opaque heuristic merges without explainable evidence

## Pipeline Shape

Current bootstrap artifacts on `feature/ingest-v2-canonicalization`:

- `out/<build_id>/ingest_v2/normalized_sources.duckdb`
- `out/<build_id>/ingest_v2/identity_graph.duckdb`
- `out/<build_id>/ingest_v2/canonical_reduction.duckdb`
- `out/<build_id>/ingest_v2/canonical_hierarchy.duckdb`
- `out/<preview_build_id>/core.duckdb` emitted by `scripts/ingest_v2/emit_preview_build.py`
- `out/<preview_build_id>/canonical_hierarchy.duckdb` copied beside preview `core.duckdb`
- `reports/<build_id>/normalized_sources_report.json`
- `reports/<build_id>/identity_graph_report.json`
- `reports/<build_id>/canonical_reduction_report.json`
- `reports/<build_id>/canonical_hierarchy_report.json`
- `reports/<preview_build_id>/canonical_preview_report.json`

Bootstrap note:

- the initial graph uses a clearly-labeled `legacy_core_crosswalk` bridge from the current deterministic build
- that bridge is transitional and exists only to connect Gaia rows to HIP/HD/WDS-oriented catalogs while ingest v2 crosswalk rules are still being rebuilt source-natively
- canonical reduction must be designed so this bridge can be removed later without changing the public contract

### Stage 1: Source Normalization

Each source gets its own normalization pass:

- Gaia backbone
- NASA Exoplanet Archive
- WDS
- MSC
- ORB6
- SBX
- Sol authority
- support catalogs

Output:
- source-native normalized tables with provenance intact

Rules:
- preserve source identifiers exactly
- preserve source topology exactly
- do not merge objects across sources here

### Stage 2: Identity Graph

Build an explicit identity graph over normalized rows.

Node classes:
- source star row
- source system row
- source planet row
- provisional subsystem / pair row

Edge classes:
- `same_star`
- `same_system`
- `same_planet`
- `planet_hosts_star`
- `component_of`

Evidence classes:
- exact Gaia ID
- exact HIP/HD agreement
- WDS component label agreement
- host-name + identifier agreement
- constrained positional/astrometric agreement

Every edge must carry:
- match method
- confidence score
- confidence tier
- evidence summary

### Stage 3: Canonical Reduction

Reduce only high-confidence equivalence classes into canonical objects.

Outputs:
- canonical `systems`
- canonical `stars`
- canonical `planets`
- explicit quarantine tables for unresolved ambiguity

Rules:
- one canonical public object per accepted identity class
- all rejected/ambiguous alternatives remain visible in `arm`/reports
- duplicate stable keys are hard failures

Bootstrap status:

- current reducer emits canonical star/system/planet mapping tables plus explicit quarantine tables
- duplicate bundles from the current legacy build are surfaced intentionally instead of being hidden
- the reducer still depends on the transitional `legacy_core_crosswalk` bridge for some Gaia/HIP/HD propagation

### Stage 4: Hierarchy Build

Build hierarchy from canonical objects plus accepted scientific pair/subsystem evidence.

Rules:
- one canonical containment spine
- typed non-containment relation edges for cross-links
- planets attach to resolved host stars when host confidence is high
- unresolved host attachment remains explicit and flagged

Bootstrap status:

- current hierarchy artifact emits a canonical system-rooted containment spine
- top-level canonical stars attach beneath canonical systems
- canonical planets attach beneath canonical host stars when host mapping is resolved
- MSC inferred leaves attach beneath top-level stars only when the arm `member_role` mapping is unique

### Stage 5: Artifact Emission

Emit:
- `core`: canonical public slice/input tables only
- `arm`: scientific side tables plus adjudication candidates and missing-field proposals
- `disc`: citations, factsheets, narratives, and reproducible enrichment artifacts
- `reports`: deterministic quality and queue outputs

Preview-emission note:

- during transition, ingest v2 may emit a full preview build before it becomes the default production emitter
- preview builds keep representative legacy row ids where practical, but replace `stable_object_key` with canonical ingest_v2 keys
- preview builds ship a sibling `canonical_hierarchy.duckdb`; the API should prefer that hierarchy when present and fall back to legacy `arm` hierarchy otherwise
- preview builds are for local proton validation and golden testing, not proof that ingest v2 is ready to replace the main build path

## Adjudication Queue

Ingest v2 must emit a queue of “sloppy systems” before any agent exists.

Purpose:
- rank unresolved or low-quality systems for later adjudication
- make ambiguity visible and measurable
- provide clean inputs for agent-assisted review

Required initial outputs:
- `reports/<build_id>/adjudication_queue.json`
- `out/<build_id>/parquet/adjudication_queue.parquet`

Initial queue columns:
- `priority_rank`
- `build_id`
- `system_id`
- `stable_object_key`
- `system_name`
- `wds_id`
- `dist_ly`
- `coolness_rank` (nullable when unavailable)
- `coolness_score` (nullable when unavailable)
- `queue_priority` (`adjudication|review|watch`)
- `severity_score`
- `severity_label`
- `issue_type_count`
- `issue_types_json`
- `issue_summary`
- duplicate metrics:
  - `dup_planet_key_count`
  - `dup_planet_extra_row_count`
  - `dup_star_gaia_groups`
  - `dup_star_hip_groups`
  - `dup_star_hd_groups`
  - `dup_star_name_groups`
- hierarchy context:
  - `core_star_count`
  - `msc_root_star_count`
  - `planet_count`

Initial queue issue families:
- duplicate planet stable keys
- duplicate Gaia-backed stars in a system
- duplicate HIP/HD-backed stars in a system
- duplicate star names within a system
- partial MSC hierarchy relative to the current canonical star set

## Agent Integration Point

The agent consumes adjudication queue rows after ingest v2 exists.

Agent inputs:
- canonical object bundle
- source evidence bundle
- issue summary from queue
- current `core`/`arm`/`disc` context

Agent outputs:
- `disc` citation rows
- `disc` factsheets / narratives
- `arm` adjudication candidates
- `arm` missing-field proposals

Hard rule:
- agents never write directly into canonical `core`

## First Goldens

Required early goldens:
- Castor
- 16 Cyg
- Sol
- Sirius
- Alpha Centauri
- 55 Cancri
- TRAPPIST-1

16 Cyg specifically must evolve from a mere “presence” golden into a structure golden:
- no duplicate planets by stable key
- no duplicate A/B star identities surviving canonical reduction
- host planet nested under 16 Cyg B
- hierarchy reflects the best available evidence for inner pair vs outer companion

## Implementation Order

1. Document ingest v2 contracts.
2. Emit first adjudication queue from current builds.
3. Build source normalization modules.
4. Build identity graph + canonical reducer.
5. Replace hierarchy build with canonical-object inputs.
6. Expand goldens and hard invariants.
7. Slot agent-assisted adjudication onto the queue.

## Success Criteria

- corner cases become queue rows, not hidden corruption
- `core` duplicates fall sharply under hard invariants
- hierarchy reflects canonical objects instead of parallel source duplicates
- agent work is cleanly optional and additive
