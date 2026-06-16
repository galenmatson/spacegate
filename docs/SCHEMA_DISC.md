# Spacegate Disc Schema

This document is the source of truth for Spacegate's **derived astronomy artifacts**.
Disc data is reproducible and regenerated from core/arm/packs; it is not edited in place.

Schema family:
- `docs/SCHEMA_CORE.md`: immutable scientific astronomy data
- `docs/SCHEMA_ARM.md`: immutable supplemental science graph/orbit derivatives
- `docs/SCHEMA_DISC.md`: disc artifacts for ranking, UX, and enrichment (this document)
- `docs/SCHEMA_RIM.md`: rim overlays

## Purpose and Boundaries

Purpose:
- Rank and prioritize objects (`coolness_scores`)
- Provide derived navigation helpers (`system_neighbors`, `system_tags`)
- Provide references and generated content manifests (`snapshot_manifest`, `factsheets`, `expositions`)

Hard constraints:
- Disc must never overwrite or alter core rows.
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
- `external_reference_links`: planned
- `source_evidence_links`: planned
- `factsheets`: planned
- `expositions`: planned
- `generated_images`: planned

## Tables

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

Planned extension columns:
- `dominant_category TEXT` (planned)
- `requires_extrapolation_flag BOOLEAN` (planned)

Note:
- The two planned columns above are not currently emitted by `scripts/score_coolness.py`.

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

## snapshot_manifest

Manifest for deterministic snapshot assets.

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
