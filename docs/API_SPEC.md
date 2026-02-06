# Spacegate v0.1 API Spec (Read-only)

Base URL: `http://<host>:8000/api/v1`

## Principles
- Read-only access to core data.
- All SQL is parameterized; no user input is concatenated into SQL.
- Pagination uses cursor-based keyset pagination.
- Responses include provenance and match confidence fields.

## Common Types

### Provenance
```json
{
  "source_catalog": "athyg",
  "source_version": "v3.3",
  "source_url": "https://...",
  "source_download_url": "https://...",
  "source_doi": null,
  "source_pk": 196694,
  "source_row_id": 196694,
  "source_row_hash": null,
  "license": "CC BY-SA 4.0",
  "redistribution_ok": true,
  "license_note": "https://...",
  "retrieval_etag": null,
  "retrieval_checksum": "...",
  "retrieved_at": "2026-02-04T17:22:39Z",
  "ingested_at": "2026-02-04T20:21:42Z",
  "transform_version": "73ea6e7"
}
```

### Error
```json
{
  "error": {
    "code": "not_found",
    "message": "System not found",
    "details": {"system_id": 123},
    "request_id": "req_..."
  }
}
```

## Pagination
Query params:
- `limit` (int, default 50, max 200)
- `cursor` (opaque, base64url-encoded JSON)

Cursor format (decoded JSON):
- For `sort=name`:
  ```json
  {"sort":"name","name":"alpha centauri","id":42}
  ```
- For `sort=distance`:
  ```json
  {"sort":"distance","dist":4.37,"id":42}
  ```

If a cursor is invalid or does not match the requested sort, the API returns `400` with `code=invalid_cursor`.

## Endpoints

### GET /health
Returns service status and build metadata.

Response 200:
```json
{
  "status": "ok",
  "build_id": "2026-02-04T202142Z_73ea6e7",
  "db_path": "/data/spacegate/served/current/core.duckdb",
  "time_utc": "2026-02-05T00:00:00Z"
}
```

### GET /systems/search
Search and browse systems with filters and cursor pagination.

Query params:
- `q` (string, optional)
- `max_dist_ly` (float, optional)
- `min_dist_ly` (float, optional)
- `spectral_class` (comma list, optional; values O,B,A,F,G,K,M,L,T,Y)
- `has_planets` (bool, optional)
- `sort` ("name" | "distance", default "name")
- `limit` (int, default 50, max 200)
- `cursor` (string, optional)

Matching rules (when `q` is provided):
1. Exact match on `system_name_norm` or star `star_name_norm`
2. Prefix match
3. Token-and match (all tokens present)
4. Identifier match (HD/HIP/Gaia patterns like `HD 10700`, `HIP 8102`, `Gaia 123`)

Responses include `match_rank` and are sorted by:
`match_rank` asc, `dist_ly` asc, `system_name_norm` asc.

Response 200:
```json
{
  "items": [
    {
      "system_id": 1,
      "stable_object_key": "system:gaia:19316224572460416",
      "system_name": "268 G. Cet",
      "system_name_norm": "268 g cet",
      "match_rank": 1,
      "dist_ly": 23.5765,
      "ra_deg": 2.601357,
      "dec_deg": 6.88687,
      "x_helio_ly": 18.1838,
      "y_helio_ly": 14.7363,
      "z_helio_ly": 2.8271,
      "gaia_id": 19316224572460416,
      "hip_id": 12114,
      "hd_id": 16160,
      "star_count": 1,
      "planet_count": 0,
      "spectral_classes": ["G"],
      "provenance": {"source_catalog":"athyg", "source_version":"v3.3", "license":"CC BY-SA 4.0", "redistribution_ok":true, "retrieved_at":"...", "transform_version":"...", "source_url":"...", "source_download_url":"...", "source_pk":196694, "source_row_id":196694, "source_row_hash":null, "license_note":"...", "retrieval_etag":null, "retrieval_checksum":"...", "ingested_at":"...", "source_doi":null}
    }
  ],
  "next_cursor": "<opaque>",
  "has_more": true
}
```

### GET /systems/{system_id}
Fetch a system with its stars and planets.

Path params:
- `system_id` (int)

Response 200:
```json
{
  "system": { /* same fields as search result + full provenance */ },
  "stars": [
    {
      "star_id": 10,
      "system_id": 1,
      "stable_object_key": "star:gaia:...",
      "star_name": "268 G. Cet",
      "component": "A",
      "spectral_type_raw": "G2V",
      "spectral_class": "G",
      "spectral_subtype": "2",
      "luminosity_class": "V",
      "spectral_peculiar": null,
      "dist_ly": 23.5765,
      "vmag": 6.12,
      "gaia_id": 19316224572460416,
      "hip_id": 12114,
      "hd_id": 16160,
      "catalog_ids": {"gaia":..., "hip":..., "hd":..., "tyc":"..."},
      "provenance": { /* full provenance */ }
    }
  ],
  "planets": [
    {
      "planet_id": 100,
      "system_id": 1,
      "star_id": 10,
      "stable_object_key": "planet:nasa:...",
      "planet_name": "...",
      "disc_year": 2014,
      "discovery_method": "Transit",
      "orbital_period_days": 12.3,
      "semi_major_axis_au": 0.1,
      "eccentricity": 0.02,
      "radius_earth": 1.4,
      "mass_earth": 3.2,
      "eq_temp_k": 800,
      "insol_earth": 50,
      "host_name_raw": "...",
      "host_gaia_id": 19316224572460416,
      "match_method": "gaia",
      "match_confidence": 1.0,
      "match_notes": null,
      "provenance": { /* full provenance */ }
    }
  ]
}
```

### GET /systems/by-key/{stable_object_key}
Fetch a system by stable key, with stars and planets.

Response 200: same as `/systems/{system_id}`.

## Error Codes
- `bad_request` (400)
- `invalid_cursor` (400)
- `not_found` (404)
- `internal_error` (500)

## Notes
- All values are derived from core data; no speculative enrichment.
- All SQL is parameterized. Sort options are mapped to fixed SQL orderings.
- `catalog_ids` is parsed from `stars.catalog_ids_json`.
