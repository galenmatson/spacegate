# Spacegate Lore Schema

This document defines Spacegate's **editable fictional overlay schema**.
Lore data is intentionally separate from core science and rich derived artifacts.

Schema family:
- `docs/SCHEMA_CORE.md`: immutable scientific astronomy data
- `docs/SCHEMA_RICH.md`: reproducible derived artifacts
- `docs/SCHEMA_LORE.md`: editable fictional/user-authored overlays (this document)

## Purpose and Boundaries

Purpose:
- Store fictional worldbuilding metadata and entities
- Allow user/project namespaces
- Support optional cultural/franchise overlays

Hard constraints:
- Lore must never modify scientific values in core.
- Lore may reference core objects, but core remains authoritative.
- Lore must be optional to load/render.

## Primary Artifacts

Recommended:
- `$SPACEGATE_STATE_DIR/out/<build_id>/lore.duckdb`
- Optional export: `$SPACEGATE_STATE_DIR/out/<build_id>/lore/*.parquet`

## Key Contracts and Type Normalization

Canonical object reference key:
- `stable_object_key TEXT` (matches core/rich exactly; no rewriting)

Namespace isolation key:
- `namespace TEXT` (required for all editable lore rows)

Version/build keys:
- `build_id TEXT` for build-bound overlays
- `updated_at TIMESTAMP` for edit history

ID normalization:
- Local lore entity IDs use `BIGINT` surrogate keys.
- Cross-database joins should use `stable_object_key`.

## Tables

## lore_entities

Primary fictional entity table.

| Column             | Type      | Description |
|--------------------|-----------|-------------|
| lore_entity_id     | BIGINT    | Surrogate primary key |
| namespace          | TEXT      | User/project namespace |
| entity_type        | TEXT      | e.g. faction, station, gate, colony, route |
| entity_key         | TEXT      | Stable key unique within namespace |
| stable_object_key  | TEXT      | Optional anchor to core object |
| anchor_mode        | TEXT      | `anchored` \| `free` |
| x_helio_ly         | DOUBLE    | Optional explicit position |
| y_helio_ly         | DOUBLE    | Optional explicit position |
| z_helio_ly         | DOUBLE    | Optional explicit position |
| lore_json          | TEXT      | Flexible lore payload |
| source             | TEXT      | user, import pack, generator |
| created_at         | TIMESTAMP | Created time |
| updated_at         | TIMESTAMP | Last update time |

Constraints:
- `namespace + entity_key` unique.
- If `anchor_mode='anchored'`, `stable_object_key` is required.
- If `anchor_mode='free'`, explicit coordinates are required.

## lore_relationships

Graph edges between lore entities and/or anchored objects.

| Column               | Type      | Description |
|----------------------|-----------|-------------|
| lore_relationship_id | BIGINT    | Surrogate primary key |
| namespace            | TEXT      | Namespace |
| from_entity_key      | TEXT      | Source entity key |
| to_entity_key        | TEXT      | Target entity key |
| relation_type        | TEXT      | e.g. owns, allied_with, trade_lane, gate_link |
| relation_json        | TEXT      | Optional edge metadata |
| created_at           | TIMESTAMP | Created time |
| updated_at           | TIMESTAMP | Updated time |

Constraints:
- `namespace + from_entity_key + to_entity_key + relation_type` unique.

## lore_references

Optional cultural/franchise references.

| Column            | Type      | Description |
|-------------------|-----------|-------------|
| lore_reference_id | BIGINT    | Surrogate primary key |
| namespace         | TEXT      | Namespace |
| stable_object_key | TEXT      | Core/rich object reference |
| reference_type    | TEXT      | franchise, film, novel, game |
| source_name       | TEXT      | Franchise/source name |
| description       | TEXT      | Lore text |
| citation_url      | TEXT      | Source/provenance URL |
| pack_id           | TEXT      | Optional lore pack source |
| created_at        | TIMESTAMP | Created time |
| updated_at        | TIMESTAMP | Updated time |

Constraints:
- References are optional overlays only.
- References must not alter `coolness_total` or scientific measurements.

## Invariants

- Lore datasets are editable and namespace-scoped.
- Lore rendering can be toggled off without impacting core or rich behavior.
- Any lore import pipeline must preserve provenance (`source`, `pack_id`, `citation_url`).
