# Spacegate Disc Schema

This document is the source of truth for Spacegate's **derived astronomy artifacts**.
Disc data is reproducible and regenerated from core/arm/packs; it is not edited in place.

Schema family:
- `docs/SCHEMA_CORE.md`: immutable scientific astronomy data
- `docs/SCHEMA_ARM.md`: immutable science evidence/support graph, orbit, and
  deterministic derivative rows
- `docs/SCHEMA_DISC.md`: disc artifacts for ranking, UX, and enrichment (this document)
- `docs/SCHEMA_RIM.md`: rim overlays

## Purpose and Boundaries

Purpose:
- Rank and prioritize objects (`coolness_scores`)
- Provide derived navigation helpers (`system_neighbors`, `system_tags`)
- Provide references and generated content manifests (`snapshot_manifest`, `factsheets`, `expositions`)

Hard constraints:
- Disc must never overwrite or alter core rows.
- Disc must not promote assumptions into science claims; stronger `core` or
  `arm` evidence supersedes disc presentation defaults.
- Disc rows must be reproducible from explicit inputs and versions.
- Cultural/fictional metadata must not alter scientific core values.

## Primary Artifacts

- `$SPACEGATE_STATE_DIR/out/<build_id>/disc.duckdb`
- `$SPACEGATE_STATE_DIR/out/<build_id>/disc/*.parquet`
- Snapshot/image assets under:
  - `$SPACEGATE_STATE_DIR/out/<build_id>/snapshots/...`
  - `$SPACEGATE_STATE_DIR/out/<build_id>/images/...`

## Key Contracts and Type Normalization

Required cross-dataset join key:
- `stable_object_key TEXT` (required on all object-scoped disc rows)

Build/version keys:
- `build_id TEXT` (required)
- `generator_version TEXT` (required for generated artifacts)

ID normalization:
- Core surrogate IDs (`system_id`, `star_id`, `planet_id`) are `BIGINT`.
- If denormalized into disc, keep them `BIGINT`.
- For cross-build stability and cross-database joins, use `stable_object_key` as canonical.

Rule:
- If both `system_id` and `stable_object_key` are present in a disc row, they must refer to the same core object for that `build_id`.

## Implementation Status

Status labels:
- `implemented`: currently emitted/used by runtime scripts/services
- `planned`: approved design, not fully emitted/used yet

Current status snapshot:
- `coolness_scores`: implemented (`scripts/score_coolness.py`)
- `object_coolness_scores`: planned
- `system_neighbors`: planned
- `system_tags`: planned
- `snapshot_manifest`: implemented (`scripts/generate_snapshots.py`)
- `simulation_assumptions`: implemented for selected-system simulator
  materialization (`scripts/materialize_simulation_assumptions.py`); broader
  reviewed curation workflow remains planned
- `external_reference_links`: planned
- `source_evidence_links`: planned
- Evidence Portfolio operational store: implemented in the Admin DB as mutable
  admin workflow state; public `disc` materialization remains planned
- `factsheets`: planned
- `expositions`: planned
- `generated_images`: planned

## Tables

## Evidence Portfolio Operational Store

Active agent workflow state currently lives in the Admin SQLite database, not
inside served build artifacts. This keeps conversations, source retrieval,
findings, and journal entries mutable and auditable without editing immutable
`out/<build_id>/disc.duckdb` or `served/current`.

Implemented admin tables:

- `agent_object_dossiers`
  - operator-facing object: Evidence Portfolio
  - tracks target object, lifecycle status, queue reason/priority, freshness,
    review state, publication state, and metadata
- `agent_source_documents`
  - operator-facing object: Source File
  - tracks canonical URL, domain, source kind, allowlist tier, trust score,
    retrieval status, archive path, content hash, and metadata
  - source domain, allowlist tier, trust score, and enabled state should be
    validated against the current Agency source allowlist at retrieval/context
    assembly time
- `agent_claim_bundles`
  - operator-facing object: Source File or Extraction Set, depending on
    `bundle_kind`
  - tracks extraction method, endpoint/model/prompt metadata, token limits,
    hashes, status, and metadata
- `agent_extracted_claims`
  - operator-facing object: Finding
  - tracks subject binding, claim family, predicate, value, unit, qualifier,
    confidence, schema fit, rigor tier, review status, citations, and reasoning
- `agent_portfolio_journal_entries`
  - operator-facing object: Journal Entry
  - tracks plain-language stage history, actor, outcome, linked records,
    machine payload, model/prompt metadata, and token usage

Materialization rule:

- Admin workflow rows are the hot mutable case file.
- Future public `disc` rows such as `source_evidence_links`, `factsheets`, and
  `expositions` must be generated deliberately from reviewed evidence and
  explicit generator versions.
- Agent conversations must not directly mutate `core` or silently publish
  public `disc` artifacts.

## coolness_scores

Deterministic scoring output used for ranking and prioritization.

Current required columns:
- `rank BIGINT`
- `system_id BIGINT`
- `stable_object_key TEXT`
- `system_name TEXT`
- `build_id TEXT`
- `profile_id TEXT`
- `profile_version TEXT`
- `score_total DOUBLE`
- feature and score breakdown columns from scoring pipeline
- signal-count columns from the scoring pipeline, including nice/weird planet
  counts and the nice-planet evidence basis counts

Planned extension columns:
- `dominant_category TEXT` (planned)
- `requires_extrapolation_flag BOOLEAN` (planned)

Note:
- The two planned columns above are not currently emitted by `scripts/score_coolness.py`.
- `nice_planet_count` is a presentation/prioritization signal, not a canonical
  habitability claim. It prefers source-native equilibrium temperature, then
  source-native insolation, then a lower-confidence stellar-class luminosity
  proxy when a planet has a high-confidence host link and semi-major axis but
  lacks temperature/insolation fields. Basis-specific count columns must be
  preserved so operators can distinguish source-backed signals from proxy
  signals.

## object_coolness_scores (planned)

Object-scoped prioritization output for enrichment/adjudication queues.

Required columns:
- `object_type TEXT` (`system|star|planet`)
- object ID fields:
  - `system_id BIGINT` (nullable)
  - `star_id BIGINT` (nullable)
  - `planet_id BIGINT` (nullable)
- `stable_object_key TEXT`
- `build_id TEXT`
- `profile_id TEXT`
- `profile_version TEXT`
- `rank BIGINT`
- `score_total DOUBLE`
- `queue_priority TEXT` (`enrichment|adjudication|review`)
- feature and score breakdown columns from scoring pipeline

Rules:
- object coolness is derived/prioritization metadata only
- it does not change canonical scientific truth
- default enrichment order should prefer systems, then stars, then planets unless the queue policy explicitly overrides it

## system_tags

Semantic labels attached to systems for filtering/discovery.

| Column            | Type      | Description |
|-------------------|-----------|-------------|
| system_id         | BIGINT    | Core system ID for same-build convenience |
| stable_object_key | TEXT      | Canonical cross-build join key |
| tag_key           | TEXT      | Machine tag in `snake_case` |
| tag_type          | TEXT      | `derived` or `pack` |
| tag_source        | TEXT      | `profile_id`/generator or `pack_id` |
| confidence        | DOUBLE    | `1.0` for deterministic tags |
| build_id          | TEXT      | Build that generated tag |
| created_at        | TIMESTAMP | Creation timestamp |

Constraints:
- Derived tags are deterministic and reproducible.
- Pack tags must reference pack metadata.
- No manual edits to deterministic derived tags.

Indexes:
- `(tag_key)`
- `(system_id)`
- `(stable_object_key)`
- `(tag_type)`

## system_neighbors

Nearest-neighbor graph for fast navigation.

Required columns:
- `system_id BIGINT`
- `neighbor_rank BIGINT` (1..10)
- `neighbor_system_id BIGINT`
- `distance_ly DOUBLE`
- `method TEXT`
- `build_id TEXT`
- `generator_version TEXT`
- `created_at TIMESTAMP`

## simulation_assumptions

Reproducible presentation/simulation defaults used when a visual or 3D scene
needs a value but no source-backed or defensible `arm` derivative is available.

Required columns:
- durable identity:
  - `assumption_key TEXT`
- object binding:
  - `object_type TEXT` (`system|star|planet|component|orbit`)
  - `system_id BIGINT` (nullable)
  - `star_id BIGINT` (nullable)
  - `planet_id BIGINT` (nullable)
  - `orbit_edge_id BIGINT` (nullable)
  - `stable_object_key TEXT` (nullable)
  - `stable_component_key TEXT` (nullable)
  - `render_key TEXT` (nullable)
  - `display_name TEXT` (nullable)
- assumption:
  - `parameter_key TEXT` (for example `eccentricity`, `inclination_deg`,
    `render_albedo`, `surface_palette`, `cloud_fraction`)
  - `value_json TEXT`
  - `unit TEXT` (nullable)
  - `assumption_kind TEXT` (`visual_default|simulation_default|classification_hint`)
  - `assumption_method TEXT`
  - `assumption_version TEXT`
  - `input_context_json TEXT`
  - `replacement_target TEXT`
  - `visibility_label TEXT` (`assumed|illustrative|placeholder`)
  - `layer TEXT`
  - `seed TEXT` (nullable)
  - `confidence DOUBLE` (nullable)
  - `confidence_tier TEXT` (nullable)
  - `notes TEXT` (nullable)
  - `field_json TEXT`
- build/version:
  - `build_id TEXT`
  - `generator_version TEXT`
  - `source_scene_schema_version TEXT`
  - `render_scene_schema_version TEXT`
  - `materialization_version TEXT`
  - `created_at TIMESTAMP`

Rules:
- Disc assumptions are not measurements and must be labeled in Admin/public
  object explorers when surfaced.
- Assumptions must be deterministic for the same input context and generator
  version.
- A stronger `core` source value or accepted `arm.derived_physical_parameters`
  row must supersede the assumption in downstream presentation.
- Agency enrichment should treat active assumptions as search targets for real
  values, but conversations or generated prose must not silently convert an
  assumption into a science claim.
- Live System Preview v0.2 emits deterministic `procedural_prior_v1`
  assumption fields in the API `render_scene` payload and mirrors every
  rendered `status="assumed"` field into `render_scene.assumptions` using this
  table's object-binding shape. `scripts/materialize_simulation_assumptions.py`
  persists selected system records into this table and exports
  `disc/simulation_assumptions.parquet`; those rows remain presentation-layer
  assumptions, not science ingest.
- The beta renderer may also use transient visual layout values such as scene
  radii, deterministic phase, and cluster guide placement for `group_pair`
  orbit edges. These are presentation defaults unless backed by source or
  derived ARM fields.
- Render-scene planets may receive deterministic low-tilt inclination
  fallbacks when no source/renderable inclination is available. These rows use
  `parameter_key='inclination_deg'`, `assumption_kind='simulation_default'`,
  `visibility_label='assumed'`, and `layer='disc_assumption'`; they are only
  camera/trace legibility defaults and do not repair the missing source
  orbital element.
- Rendered stars may receive a transient visual stellar-class prior from mass
  when source spectral/temperature/class evidence is missing. The current
  simulator exposes this as `fields.visual_stellar_class` with
  `basis='mass_main_sequence_prior_v1'`, `status='assumed'`, and
  `layer='render_scene'`. If these visual priors are later persisted, they
  belong in `disc` as presentation assumptions, not in core spectral fields or
  ARM science classifications. Remnant, evolved, metallicity, and unresolved
  multiple alternatives remain explicitly not excluded by this prior.
- Browser orbit guide/readout helpers may expose provenance fields such as
  `orbit_guide_trace`, `planet_orbit_trace`, and `binary_body_paths` to explain
  sampled path geometry and mass-weighted/equal-mass display choices. Persist
  those only if they are needed as presentation assumptions; never promote them
  to ARM orbit evidence.
- Two-rendered-star scenes with no source orbit edge may receive a transient
  `visual_binary_fallback` orbit whose period, eccentricity, inclination,
  phase, and visual separation are `disc_assumption` fields. This is a
  legibility device for scenes such as Sirius and must be superseded by
  reviewed ARM orbit evidence when available.
- The beta renderer may use deterministic procedural star/planet
  surface materials and bounded visual-scale radius transforms. Persisting
  those choices requires `simulation_assumptions` rows with generator version,
  seed/input context, replacement target, and visible `ASSUMED`/illustrative
  labeling.

## snapshot_manifest

Manifest for deterministic snapshot assets.

Snapshot generators may use read-only science inputs from `core` plus
persisted, provenance-bearing `arm` derivatives when core source fields are
missing. The manifest `source_build_inputs_hash` must include any resolved
derived values and their source/derived status so regenerated artifacts remain
traceable and deterministic.

Required columns:
- `stable_object_key TEXT`
- `system_id BIGINT`
- `object_type TEXT`
- `view_type TEXT`
- `params_json TEXT`
- `params_hash TEXT`
- `artifact_path TEXT`
- `artifact_mime TEXT`
- `build_id TEXT`
- `generator_version TEXT`
- `width_px INTEGER`
- `height_px INTEGER`
- `source_build_inputs_hash TEXT`
- `created_at TIMESTAMP`

## external_reference_links (v1.3+)

Curated links only; no copied article text.

Required columns:
- `stable_object_key TEXT`
- `url TEXT`
- `domain TEXT`
- `source_type TEXT`
- `authority_score DOUBLE`
- `specificity_score DOUBLE`
- `link_rank BIGINT`
- `build_id TEXT`
- `created_at TIMESTAMP`

## source_evidence_links (planned)

Structured citation rows backing factsheets, narratives, and agent dossiers.

Required columns:
- `stable_object_key TEXT`
- `citation_id TEXT`
- `url TEXT`
- `domain TEXT`
- `source_kind TEXT` (`paper|catalog_doc|archive|mission|encyclopedia|other`)
- `title TEXT`
- `publisher TEXT`
- `published_at TIMESTAMP` (nullable)
- `accessed_at TIMESTAMP`
- `authority_score DOUBLE`
- `relevance_score DOUBLE`
- `evidence_scope TEXT` (`identity|hierarchy|planet_host|physical_params|narrative|image_context`)
- `build_id TEXT`
- `generator_version TEXT`
- `created_at TIMESTAMP`

Rules:
- citations must point to the external source; disc stores links and metadata, not copied article bodies
- generated factsheets/expositions should reference `citation_id` values rather than embedding uncited claims

## factsheets (v1.4+)

Structured factual summaries used as source-of-truth inputs for exposition.

Required columns:
- `stable_object_key TEXT`
- `facts_json TEXT`
- `facts_hash TEXT`
- `citation_ids_json TEXT`
- `build_id TEXT`
- `generator_version TEXT`
- `created_at TIMESTAMP`

Rules:
- factsheets must be derivable from explicit evidence rows and generator logic
- any claim that is not directly sourced from `core`/`arm` should point to supporting `source_evidence_links`

## expositions (v1.4+)

Generated scientific exposition derived from factsheets.

Required columns:
- `stable_object_key TEXT`
- `facts_hash TEXT`
- `text_markdown TEXT`
- `citation_ids_json TEXT`
- `model_id TEXT`
- `prompt_version TEXT`
- `build_id TEXT`
- `generated_at TIMESTAMP`

## generated_images (v1.5+)

Image generation metadata; binary assets are stored on filesystem/object storage.

Required columns:
- `stable_object_key TEXT`
- `asset_path TEXT`
- `prompt_text TEXT`
- `model_id TEXT`
- `build_id TEXT`
- `generated_at TIMESTAMP`

## Invariants

- Disc tables are append/regenerate artifacts, not manual-edit datasets.
- Any logic change that affects outputs must bump `generator_version`.
- Every disc row must be attributable to a build and generation method.
