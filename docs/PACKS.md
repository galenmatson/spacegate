# Spacegate Packs Contract (v2.1+)

This document defines the minimal contract for **optional object packs** (substellar/compact/superstellar/etc.).
Packs are **read-only**, versioned artifacts that augment core data without mutating it.

## Goals
- Allow the UI and tools to discover packs deterministically.
- Ensure packs can join to core via `stable_object_key`.
- Keep provenance and licensing explicit and reproducible.

## Pack directory layout
Each build may include zero or more packs:

- `out/<build_id>/packs/<pack_name>/*.parquet`
- `out/<build_id>/packs/<pack_name>.duckdb` (optional)
- `out/<build_id>/packs_manifest.json` (discovery index)

## Pack schema contract (minimum)
Every pack table must include these fields:

- `stable_object_key` (TEXT, required)
- `object_type` (TEXT, required) — e.g., `substellar`, `compact`, `superstellar`, `cluster`, `nebula`, ...
- `x_helio_ly`, `y_helio_ly`, `z_helio_ly` (DOUBLE, required when positional)
- `x_gal_ly`, `y_gal_ly`, `z_gal_ly` (DOUBLE, optional)
- `dist_ly` (DOUBLE, optional if xyz present)
- `ra_deg`, `dec_deg` (DOUBLE, optional)

### Provenance (required)
Each row must include the same provenance fields as core:
- `source_catalog`, `source_version`, `source_url`, `source_download_url`
- `source_doi` (nullable if not applicable)
- `source_pk`
- `source_row_id` **or** `source_row_hash`
- `license`, `redistribution_ok`, `license_note`
- `retrieval_etag` and/or `retrieval_checksum` (when available)
- `retrieved_at`, `ingested_at`
- `transform_version`

## `packs_manifest.json`
A small index file for pack discovery. Example:

```
{
  "schema_version": "v1",
  "build_id": "2026-02-04T202142Z_73ea6e7",
  "generated_at": "2026-02-04T20:55:00Z",
  "packs": [
    {
      "name": "pack_substellar",
      "artifact_path": "packs/pack_substellar",
      "format": "parquet",
      "schema_version": "v1",
      "source_catalogs": ["ultracoolsheet"],
      "row_count": 12345
    }
  ]
}
```

Fields are additive; the UI should ignore unknown keys.

## Joins
- Packs should join to core via `stable_object_key` when possible.
- If no core object exists, the pack entry is still valid as a standalone object.

## Constraints
- Packs never mutate core data.
- Packs are regenerated, not edited in place.
- Any schema changes bump pack `schema_version`.
