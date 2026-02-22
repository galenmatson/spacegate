# Spacegate Rich Schema

This document is the source of truth for Spacegate's **derived astronomy artifacts**.
Rich data is reproducible and regenerated from core/packs; it is not edited in place.

Schema family:
- `docs/SCHEMA_CORE.md`: immutable scientific astronomy data
- `docs/SCHEMA_RICH.md`: derived artifacts for ranking, UX, and enrichment (this document)
- `docs/SCHEMA_LORE.md`: editable fictional overlays

## Purpose and Boundaries

Purpose:
- Rank and prioritize objects (`coolness_scores`)
- Provide derived navigation helpers (`system_neighbors`, `system_tags`)
- Provide references and generated content manifests (`snapshot_manifest`, `factsheets`, `expositions`)

Hard constraints:
- Rich must never overwrite or alter core rows.
- Rich rows must be reproducible from explicit inputs and versions.
- Cultural/fictional metadata must not alter scientific core values.

## Primary Artifacts

- `$SPACEGATE_STATE_DIR/out/<build_id>/rich.duckdb`
- `$SPACEGATE_STATE_DIR/out/<build_id>/rich/*.parquet`
- Snapshot/image assets under:
  - `$SPACEGATE_STATE_DIR/out/<build_id>/snapshots/...`
  - `$SPACEGATE_STATE_DIR/out/<build_id>/images/...`

## Key Contracts and Type Normalization

Required cross-dataset join key:
- `stable_object_key TEXT` (required on all object-scoped rich rows)

Build/version keys:
- `build_id TEXT` (required)
- `generator_version TEXT` (required for generated artifacts)

ID normalization:
- Core surrogate IDs (`system_id`, `star_id`, `planet_id`) are `BIGINT`.
- If denormalized into rich, keep them `BIGINT`.
- For cross-build stability and cross-database joins, use `stable_object_key` as canonical.

Rule:
- If both `system_id` and `stable_object_key` are present in a rich row, they must refer to the same core object for that `build_id`.

## Implementation Status

Status labels:
- `implemented`: currently emitted/used by runtime scripts/services
- `planned`: approved design, not fully emitted/used yet

Current status snapshot:
- `coolness_scores`: implemented (`scripts/score_coolness.py`)
- `system_neighbors`: planned
- `system_tags`: planned
- `snapshot_manifest`: implemented (`scripts/generate_snapshots.py`)
- `external_reference_links`: planned
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

## external_reference_links (v1.2+)

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

## factsheets (v1.3+)

Structured factual summaries used as source-of-truth inputs for exposition.

Required columns:
- `stable_object_key TEXT`
- `facts_json TEXT`
- `facts_hash TEXT`
- `build_id TEXT`
- `generator_version TEXT`
- `created_at TIMESTAMP`

## expositions (v1.3+)

Generated scientific exposition derived from factsheets.

Required columns:
- `stable_object_key TEXT`
- `facts_hash TEXT`
- `text_markdown TEXT`
- `model_id TEXT`
- `prompt_version TEXT`
- `build_id TEXT`
- `generated_at TIMESTAMP`

## generated_images (v1.4+)

Image generation metadata; binary assets are stored on filesystem/object storage.

Required columns:
- `stable_object_key TEXT`
- `asset_path TEXT`
- `prompt_text TEXT`
- `model_id TEXT`
- `build_id TEXT`
- `generated_at TIMESTAMP`

## Invariants

- Rich tables are append/regenerate artifacts, not manual-edit datasets.
- Any logic change that affects outputs must bump `generator_version`.
- Every rich row must be attributable to a build and generation method.
