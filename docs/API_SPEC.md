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

### GET /auth/login/google
Starts OIDC login flow when auth is enabled (`SPACEGATE_AUTH_ENABLE=1`).

Query params:
- `next` (optional local path; default `/admin`)

Response:
- `302` redirect to Google authorization endpoint.

### GET /auth/callback/google
OIDC callback endpoint. Validates state/nonce, verifies ID token, enforces admin allowlist, and creates a session.

Query params:
- `code` (required)
- `state` (required)

Response:
- `302` redirect to admin page on success.
- `400/401/403` on invalid state/auth/allowlist failures.

### POST /auth/logout
Revokes active session and clears auth cookies.

Security:
- Requires authenticated session.
- Requires CSRF header (`X-CSRF-Token`) for mutating request.

Response:
- `204 No Content`

### GET /auth/me
Returns auth/session summary.

Response when auth disabled:
```json
{"auth_enabled": false, "authenticated": false}
```

Response when auth enabled but unauthenticated:
```json
{
  "auth_enabled": true,
  "authenticated": false,
  "csrf": {"cookie_name":"__Host-spacegate_csrf","header_name":"X-CSRF-Token"}
}
```

### GET /admin/status
Admin-only operational status endpoint.

Response:
- `200` for authenticated admins.
- `401` unauthenticated.
- `403` authenticated non-admin.

### GET /admin/ui
Admin UI scaffold served by the API (under `/api/v1/admin/ui`).

Notes:
- Prefer `/api/v1/admin/ui` behind nginx/container deployments to avoid web route conflicts.
- `/admin` remains available when API is exposed directly.

### GET /admin/actions/catalog
Returns allowlisted admin actions and parameter schemas.

Notes:
- Includes `display_name` and `category` fields for admin UI grouping (for example `operations` vs `coolness`).

Response:
- `200` for authenticated admins.
- `401` unauthenticated.
- `403` authenticated non-admin.

### POST /admin/actions/run
Starts an allowlisted admin action as a background job.

Request body:
```json
{
  "action": "verify_build",
  "params": {"build_id": "2026-02-19T221543Z_2774126"},
  "confirmation": "RUN verify_build"
}
```

Security:
- Requires authenticated admin session.
- Requires CSRF header (`X-CSRF-Token`).
- Action-level role checks are enforced server-side.
- High-risk actions require an exact confirmation phrase from catalog metadata.

Response:
- `200` with created job metadata.
- `400` invalid action/params.
- `409` job-capacity conflict (runner busy).

### GET /admin/actions/jobs
Lists recent admin jobs.

Query params:
- `limit` (default 20, max 200)

### GET /admin/actions/jobs/{job_id}
Returns metadata for a specific admin job.

Response:
- `200` when found.
- `404` when missing.

### GET /admin/actions/jobs/{job_id}/log
Returns a log chunk for polling/streaming.

Query params:
- `offset` (byte offset, default 0)
- `limit` (bytes to read, default 65536, max 1048576)

Response shape:
```json
{
  "job_id": "job_...",
  "offset": 0,
  "next_offset": 1024,
  "chunk": "...",
  "eof": false,
  "status": "running"
}
```

### GET /admin/actions/jobs/{job_id}/log/download
Returns full job log text as a downloadable attachment.

Response:
- `200 text/plain` with `Content-Disposition: attachment; filename="<job_id>.log"`
- `404` when missing.

### POST /admin/actions/jobs/{job_id}/cancel
Cancels a queued job.

Security:
- Requires authenticated admin session.
- Requires CSRF header (`X-CSRF-Token`).

Behavior:
- Cancels only jobs in `queued` status.
- Returns `409` if the job is already running or terminal.

### GET /admin/backups
Lists available admin backup artifacts (admin DB snapshots and release metadata snapshots).

Query params:
- `limit` (default 100, max 500)

### GET /admin/audit
Lists recent admin/auth audit events from the admin auth database.

Query params:
- `limit` (default 50, max 500)
- `before_audit_id` (optional keyset pagination anchor; returns rows with `audit_id < before_audit_id`)
- `event_type` (optional exact match, e.g. `auth.login.denied`)
- `event_prefix` (optional prefix match, e.g. `admin.action.`)
- `result` (optional: `success|deny|error`)
- `request_id` (optional exact match)
- `actor_user_id` (optional exact match)

Response shape:
```json
{
  "items": [
    {
      "audit_id": 7,
      "actor_user_id": null,
      "event_type": "auth.login.denied",
      "result": "deny",
      "request_id": "req_efc0733fbb33",
      "route": "/api/v1/auth/callback/google",
      "method": "GET",
      "correlation_id": "job_20260220T190001Z_a1b2c3d4e5",
      "details": {"email":"galen.matson@archittec.com","reason":"allowlist"},
      "created_at": "2026-02-20T17:55:21Z"
    }
  ],
  "next_before_audit_id": 7
}
```

### GET /health
Returns service status and build metadata.

Response 200:
```json
{
  "status": "ok",
  "build_id": "2026-02-04T202142Z_73ea6e7",
  "db_path": "$SPACEGATE_STATE_DIR/served/current/core.duckdb",
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
