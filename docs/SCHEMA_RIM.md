# Spacegate Rim Schema

This document defines Spacegate's **editable fictional overlay schema**.
Rim data is intentionally separate from core science and disc derived artifacts.

Schema family:
- `docs/SCHEMA_CORE.md`: immutable scientific astronomy data
- `docs/SCHEMA_DISC.md`: reproducible disc artifacts
- `docs/SCHEMA_RIM.md`: editable fictional/user-authored rim overlays (this document)

## Purpose and Boundaries

Purpose:
- Store fictional worldbuilding metadata and entities
- Allow user/project namespaces
- Support optional cultural/franchise overlays

Hard constraints:
- Rim must never modify scientific values in core.
- Rim may reference core objects, but core remains authoritative.
- Rim must be optional to load/render.
- Rim should reuse shared graph vocabulary from `docs/SYSTEM_GRAPH_ARCHITECTURE.md` where possible.

## Primary Artifacts

Recommended:
- `$SPACEGATE_STATE_DIR/out/<build_id>/rim.duckdb`
- Optional export: `$SPACEGATE_STATE_DIR/out/<build_id>/rim/*.parquet`

## Key Contracts and Type Normalization

Canonical object reference key:
- `stable_object_key TEXT` (matches core/disc exactly; no rewriting)

Namespace isolation key:
- `namespace TEXT` (required for all editable rim rows)

Version/build keys:
- `build_id TEXT` for build-bound overlays
- `updated_at TIMESTAMP` for edit history

ID normalization:
- Local rim entity IDs use `BIGINT` surrogate keys.
- Cross-database joins should use `stable_object_key`.

## Tables

## rim_entities

Primary fictional entity table.

| Column             | Type      | Description |
|--------------------|-----------|-------------|
| rim_entity_id      | BIGINT    | Surrogate primary key |
| namespace          | TEXT      | User/project namespace |
| entity_type        | TEXT      | e.g. faction, station, gate, colony, route |
| entity_key         | TEXT      | Stable key unique within namespace |
| stable_object_key  | TEXT      | Optional anchor to core object |
| anchor_mode        | TEXT      | `anchored` \| `free` |
| x_helio_ly         | DOUBLE    | Optional explicit position |
| y_helio_ly         | DOUBLE    | Optional explicit position |
| z_helio_ly         | DOUBLE    | Optional explicit position |
| rim_json           | TEXT      | Flexible rim payload |
| source             | TEXT      | user, import pack, generator |
| created_at         | TIMESTAMP | Created time |
| updated_at         | TIMESTAMP | Last update time |

Constraints:
- `namespace + entity_key` unique.
- If `anchor_mode='anchored'`, `stable_object_key` is required.
- If `anchor_mode='free'`, explicit coordinates are required.

## rim_relationships

Graph edges between rim entities and/or anchored objects.

| Column               | Type      | Description |
|----------------------|-----------|-------------|
| rim_relationship_id  | BIGINT    | Surrogate primary key |
| namespace            | TEXT      | Namespace |
| from_entity_key      | TEXT      | Source entity key |
| to_entity_key        | TEXT      | Target entity key |
| relation_type        | TEXT      | e.g. owns, allied_with, trade_lane, gate_link |
| relation_json        | TEXT      | Optional edge metadata |
| created_at           | TIMESTAMP | Created time |
| updated_at           | TIMESTAMP | Updated time |

Constraints:
- `namespace + from_entity_key + to_entity_key + relation_type` unique.

Recommended relation families:
- placement/anchor: `anchored_to`, `located_in`
- transit/infrastructure: `gateway_to`, `route_to`, `stabilized_by`
- governance/social: `controls`, `allied_with`, `hostile_to`

Note:
- cyclical rim graphs are allowed.
- any canonical navigation tree behavior should be implemented with explicit primary-parent metadata, not inferred from arbitrary rim edges.

## rim_references

Optional cultural/franchise references.

| Column            | Type      | Description |
|-------------------|-----------|-------------|
| rim_reference_id  | BIGINT    | Surrogate primary key |
| namespace         | TEXT      | Namespace |
| stable_object_key | TEXT      | Core/disc object reference |
| reference_type    | TEXT      | franchise, film, novel, game |
| source_name       | TEXT      | Franchise/source name |
| description       | TEXT      | Rim overlay text |
| citation_url      | TEXT      | Source/provenance URL |
| pack_id           | TEXT      | Optional rim pack source |
| created_at        | TIMESTAMP | Created time |
| updated_at        | TIMESTAMP | Updated time |

Constraints:
- References are optional overlays only.
- References must not alter `coolness_total` or scientific measurements.

## Invariants

- Rim datasets are editable and namespace-scoped.
- Rim rendering can be toggled off without impacting core or disc behavior.
- Any rim import pipeline must preserve provenance (`source`, `pack_id`, `citation_url`).
