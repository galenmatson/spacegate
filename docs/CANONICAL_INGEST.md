# Spacegate Canonical Ingest Plan

## Evidence Lake Preflight

M8.3c E0 adds a required full-refresh preflight over
`config/evidence_lake/source_releases.json` and its pinned schema baseline.
Before source acquisition, `scripts/preflight_full_refresh.sh` verifies source
and manifest registration, schema/field accounting, current artifacts, storage
budgets, and the acquisition free-space floor. Set
`SPACEGATE_EVIDENCE_REGISTRY_GATE=0` only for isolated legacy diagnosis; it is
not an accepted production build configuration.

E1 source-native Parquet, the E2 release-scoped identity/scope graph, and the 38
registered E4 scientific adapters now have explicit compiler contracts. They
remain unserved compiler inputs: the current canonical build path stays
operational until E5 selected facts are complete and E6 passes the shadow
scientific A/B review. The preflight,
typed-lake, identity graph, and E4 evidence contracts do not promote registry
metadata, source relations, release crossmatches, or coherent Gaia source
solutions into CORE by themselves.
The combined E4 identity is release set `a188a3adc6207d3a217d54a9`, an atomic
manifest over 36 immutable read-only source shards rather than another copied
database. Canonical ingest must consume E5 selected facts derived from that set,
not attach an arbitrary collection of whichever E4 artifacts happen to exist.

This document defines the clean-slate canonicalization pipeline that will replace the remaining AT-HYG-era assumptions in the current ingest flow.

Status:
- architecture approved
- production full-build wrappers emit and promote canonical database builds after the bootstrap science projection
- agent integration is explicitly **not** a prerequisite for canonical ingest

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

Current bootstrap artifacts:

- `out/<build_id>/ingest/normalized_sources.duckdb`
- `out/<build_id>/ingest/identity_graph.duckdb`
- `out/<build_id>/ingest/canonical_reduction.duckdb`
- `out/<build_id>/ingest/canonical_hierarchy.duckdb`
- `out/<canonical_build_id>/core.duckdb` emitted by `scripts/ingest/emit_canonical_build.py`
- `out/<canonical_build_id>/canonical_hierarchy.duckdb` copied beside `core.duckdb`
- `reports/<build_id>/normalized_sources_report.json`
- `reports/<build_id>/identity_graph_report.json`
- `reports/<build_id>/canonical_reduction_report.json`
- `reports/<build_id>/canonical_hierarchy_report.json`
- `reports/<canonical_build_id>/canonical_build_report.json`

Bootstrap note:

- the initial graph uses a clearly-labeled `legacy_core_crosswalk` bridge from the current deterministic build
- that bridge is transitional and exists only to connect Gaia rows to HIP/HD/WDS-oriented catalogs while canonical ingest crosswalk rules are still being rebuilt source-natively
- canonical reduction must be designed so this bridge can be removed later without changing the public contract

### Stage 1: Source Normalization

Each source gets its own normalization pass:

- Gaia backbone
- NASA Exoplanet Archive
- WDS
- MSC
- ORB6
- SBX
- SB9 (ARM component/orbit evidence; no independent inventory promotion)
- DEBCat (ARM endpoint evidence only after unique system/period reconciliation)
- Sol authority
- support catalogs

Output:
- source-native normalized tables with provenance intact

Rules:
- preserve source identifiers exactly
- preserve source topology exactly
- do not merge objects across sources here
- never add object-specific transform branches to satisfy a golden; defer the
  case to the adjudication queue when a general evidence rule cannot resolve it

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
- missing one-letter root roles may be represented as unresolved hierarchy components when WDS pair evidence and MSC multi-leaf evidence agree; these nodes remain outside canonical `core.stars`
- singleton MSC subdivisions are suppressed in the canonical hierarchy so sparse role evidence does not masquerade as a resolved pair

### Stage 5: Artifact Emission

Emit:
- `core`: canonical public slice/input tables only
- `arm`: scientific side tables plus adjudication candidates and missing-field proposals
- `disc`: citations, factsheets, narratives, and reproducible enrichment artifacts
- `reports`: deterministic quality and queue outputs

Canonical emission note:

- production full-build wrappers run the bootstrap science projection, then run `scripts/ingest/build_canonical.sh`
- emitted canonical builds keep representative bootstrap row ids where practical, but replace `stable_object_key` with canonical ingest keys
- canonical system and fallback-star keys preserve a source-native bootstrap
  key when one exists (for example Gaia, WDS, Sol, or UltracoolSheet identity);
  sequential bootstrap row numbers must not be used in public stable keys
- representative numeric `system_id`/`star_id` values remain storage and join
  details; routes, selections, comparisons, and long-lived references use
  `stable_object_key`
- `canonical_transform_git_sha` records the transform revision separately from
  the bootstrap source revision so deterministic comparisons never accept
  materially different transform code as a rerun
- emitted canonical builds ship a sibling `canonical_hierarchy.duckdb`; the API should prefer that hierarchy when present and fall back to `arm` hierarchy otherwise
- canonical hierarchy nodes carry structural `node_kind` separately from
  `component_family` and `component_type`. Canonical and inferred stellar
  endpoints remain in the `star` storage family while retaining types such as
  `brown_dwarf`, `white_dwarf`, or `pulsar`; inferred status is never erased by
  classification.
- emitted canonical builds materialize compact `system_search_terms` so validation exercises the fast search path instead of the alias/star fallback scans
- the bootstrap build remains an immutable input artifact, but `scripts/build_database.sh` and full `scripts/refresh_core.sh` promotion target the canonical build id

## Adjudication Queue

Canonical ingest must emit a queue of “sloppy systems” before any agent exists.

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

The agent consumes adjudication queue rows after canonical ingest exists.

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

1. Document canonical ingest contracts.
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

## Extended-Object Boundary

Non-stellar catalog regions and physical objects use the separate
`extended_objects` identity domain documented in `docs/EXTENDED_OBJECTS.md`.
Their identifiers never become star/system aliases, and their source records do
not create canonical stellar components or planets. Exact relations to existing
stars/systems remain ARM evidence unless a separately reviewed canonical policy
requires promotion.
