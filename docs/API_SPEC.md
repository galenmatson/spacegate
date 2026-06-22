# Spacegate API Spec (Gaia-first)

Base URL: `http://<host>:8000/api/v1`

Admin/auth v2 base URL: `http://<host>/api/v2`

Note: `/api/v1/auth/*` and `/api/v1/admin/*` are deprecated compatibility routes. New Admin clients and OIDC redirect URIs should use `/api/v2`.

## Principles
- Read-only access to core data.
- All SQL is parameterized; no user input is concatenated into SQL.
- Pagination uses cursor-based keyset pagination.
- Responses include provenance and match confidence fields.

## Gaia-First Contract Review (2026-03-16)
- canonical star/system provenance examples now reflect Gaia DR3-first inventory.
- alias examples now reflect authority/cross-catalog alias ingestion (not AT-HYG-origin placeholders).
- search/detail field semantics remain backward-compatible for the public UI and admin tooling.

## Common Types

### Provenance
```json
{
  "source_catalog": "gaia_dr3",
  "source_version": "dr3_gaia_source_parallax_gte_3.26156",
  "source_url": "https://...",
  "source_download_url": "https://...",
  "source_doi": null,
  "source_pk": 19316224572460416,
  "source_row_id": 19316224572460416,
  "source_row_hash": null,
  "license": "ESA Gaia Archive terms",
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
Admin UI scaffold served by the API (under `/api/v2/admin/ui`).

Notes:
- Prefer `/api/v2/admin/ui` behind nginx/container deployments to avoid web route conflicts.
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
      "route": "/api/v2/auth/callback/google",
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
  "time_utc": "2026-02-05T00:00:00Z"
}
```

### GET /systems/search
Search and browse systems with filters and cursor pagination.

Query params:
- `q` (string, optional)
- `max_dist_ly` (float, optional, `>= 0`)
- `min_dist_ly` (float, optional, `>= 0`)
- `min_star_count` (int, optional, `>= 0`)
- `max_star_count` (int, optional, `>= 0`)
- `min_planet_count` (int, optional, `>= 0`)
- `max_planet_count` (int, optional, `>= 0`)
- `min_temp_k` (float, optional, `>= 0`; matches systems having at least one star with `teff_k >= value`)
- `max_temp_k` (float, optional, `>= 0`; matches systems having at least one star with `teff_k <= value`)
- `min_coolness_score` (float, optional)
- `max_coolness_score` (float, optional)
- `spectral_class` (comma list, optional; values O,B,A,F,G,K,M,L,T,Y,D)
- `has_planets` (`true|false`, optional)
- `has_habitable` (`true|false`, optional)
- `sort` (`name` | `distance` | `coolness`, default `name`)
- `limit` (int, default 50, max 200)
- `include_total` (`true|false`, optional, default `false`)
- `cursor` (string, optional)

Matching rules (when `q` is provided):
1. Exact match on canonical system name / stable key and materialized search terms (`system_search_terms.term_norm` when present; fallback to canonical name + `aliases.alias_norm`)
2. Prefix match on materialized system search terms (or canonical name + aliases on legacy builds)
3. Token-and match (all tokens present) on materialized system search terms (or canonical name + aliases on legacy builds)
4. Identifier match (HD/HIP/Gaia patterns like `HD 10700`, `HIP 8102`, `Gaia 123`)
5. Plain long numeric queries (`10+` digits) are treated as Gaia IDs

Implementation notes:
- rebuilt Gaia-first production builds may ship `system_search_terms` as a search accelerator so public search does not need to rescan the full alias corpus at request time.
- rebuilt Gaia-first production builds may ship precomputed `systems` facets (`star_count`, `planet_count`, `star_teff_count`, `min_star_teff_k`, `max_star_teff_k`, `spectral_classes_json`, `spectral_class_mask`) so result cards and common filters avoid runtime `stars` aggregation.
- temperature filters use system-level bounds as a pruning step and may still confirm against per-star rows for exact interval semantics.
- when `arm` exposes a richer multiplicity root (for example WDS/MSC synthetic system roots), star-count filters and returned `star_count` values use the larger effective descendant-star count instead of only counting direct `core.stars` rows.

Responses include `match_rank` and are sorted by:
`match_rank` asc, `dist_ly` asc, `system_name_norm` asc.

When `q` is not provided:
- `sort=name`: `system_name_norm` asc, `system_id` asc
- `sort=distance`: `dist_ly` asc nulls last, `system_id` asc
- `sort=coolness`: `coolness_rank` asc, `system_name_norm` asc, `system_id` asc

Validation and availability behavior:
- Logical range inversions (for example `min_dist_ly > max_dist_ly`) return `400 bad_request`.
- Invalid enum/filter values (for example `sort=foo`, `has_planets=maybe`, `spectral_class=ZZ`) return `400 bad_request`.
- Requesting temperature filters when the current build does not expose `core.stars.teff_k` returns `409 conflict`.
- Requesting coolness sort/score filters when `disc` coolness data (legacy table path `disc.coolness_scores`) is unavailable returns `409 conflict`.
- Framework-level bound checks (for example negative `min_dist_ly`, `limit > 200`) return `422`.

Pagination and totals:
- `next_cursor` is an opaque keyset token for deterministic pagination.
- `total_count` is returned as an integer only when `include_total=true` (or `null` otherwise).

Response 200:
```json
{
  "items": [
    {
      "system_id": 1,
      "stable_object_key": "system:gaia:19316224572460416",
      "system_name": "268 G. Cet",
      "system_name_norm": "268 g cet",
      "display_name": "268 G. Cet",
      "display_aliases": ["HIP 12114", "HD 16160"],
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
      "gaia_id_text": "19316224572460416",
      "hip_id_text": "12114",
      "hd_id_text": "16160",
      "star_count": 1,
      "planet_count": 0,
      "star_teff_count": 1,
      "min_star_teff_k": 5772.0,
      "max_star_teff_k": 5772.0,
      "spectral_classes": ["G"],
      "coolness_rank": 97,
      "coolness_score": 18.4412,
      "coolness_nice_planet_count": 0,
      "coolness_weird_planet_count": 0,
      "coolness_dominant_spectral_class": "G",
      "snapshot": {
        "build_id": "2026-02-19T221543Z_2774126",
        "view_type": "system_card",
        "artifact_path": "snapshots/system_card/system_gaia_19316224572460416/ea9f1d1a15216a90.svg",
        "params_hash": "ea9f1d1a15216a90",
        "width_px": 980,
        "height_px": 560,
        "url": "/api/v1/snapshots/2026-02-19T221543Z_2774126/snapshots/system_card/system_gaia_19316224572460416/ea9f1d1a15216a90.svg"
      },
      "provenance": {"source_catalog":"gaia_dr3", "source_version":"dr3_gaia_source_parallax_gte_3.26156", "license":"ESA Gaia Archive terms", "redistribution_ok":true, "retrieved_at":"...", "transform_version":"...", "source_url":"...", "source_download_url":"...", "source_pk":19316224572460416, "source_row_id":19316224572460416, "source_row_hash":null, "license_note":"...", "retrieval_etag":null, "retrieval_checksum":"...", "ingested_at":"...", "source_doi":null}
    }
  ],
  "next_cursor": "<opaque>",
  "has_more": true,
  "total_count": 123456
}
```

### GET /systems/{system_id}
Fetch a system with its stars and planets.

Path params:
- `system_id` (int)

Response 200:
```json
{
  "system": {
    /* same fields as search result + full provenance */
    "display_name": "Sirius",
    "display_aliases": ["Alp CMa", "HIP 32349", "HD 48915"],
    "arm_evidence_summary": {
      "stars_with_arm_evidence": 1,
      "catalog_counts": {"vsx": 1},
      "high_variability_stars": 0,
      "ultracool_overlay_stars": 0
    },
    "aliases": [
      {
        "alias_raw": "Sirius",
        "alias_norm": "sirius",
        "alias_kind": "member_proper_name",
        "alias_priority": 21,
        "is_primary": false,
        "source_catalog": "name_authority",
        "source_version": "2026-03"
      }
    ]
  },
  "stars": [
    {
      "star_id": 10,
      "system_id": 1,
      "stable_object_key": "star:gaia:...",
      "star_name": "268 G. Cet",
      "display_name": "268 G. Cet",
      "display_aliases": ["HIP 12114", "HD 16160"],
      "component": "A",
      "spectral_type_raw": "G2V",
      "spectral_class": "G",
      "spectral_subtype": "2",
      "luminosity_class": "V",
      "spectral_peculiar": null,
      "dist_ly": 23.5765,
      "vmag": 6.12,
      "teff_k": 5772.0,
      "gaia_id": 19316224572460416,
      "hip_id": 12114,
      "hd_id": 16160,
      "catalog_ids": {"gaia":..., "hip":..., "hd":..., "tyc":"..."},
      "arm_catalogs": ["vsx", "ultracoolsheet"],
      "arm_evidence": {
        "catalogs": ["vsx", "ultracoolsheet"],
        "vsx": {
          "vsx_match_count": 2,
          "primary_variability_type_raw": "EA",
          "primary_variability_family": "eclipsing",
          "primary_amplitude_mag": 0.22,
          "primary_period_days": 1.23,
          "any_high_variability": false,
          "confidence_tier": "high"
        },
        "ultracoolsheet": {
          "match_count": 1,
          "object_name": "TRAPPIST-1",
          "age_category": "field",
          "youth_evidence": null,
          "spectral_type_opt": "M8",
          "match_confidence": 1.0
        }
      },
      "aliases": [
        {
          "alias_raw": "Sirius A",
          "alias_norm": "sirius a",
          "alias_kind": "proper_name",
          "alias_priority": 1,
          "is_primary": false,
          "source_catalog": "name_authority",
          "source_version": "2026-03"
        }
      ],
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
  ],
  "hierarchy": {
    "preferred_root_key": "comp:msc_system:wds:07346+3153",
    "root_keys_considered": ["comp:system:system:wds:07346+3153", "comp:msc_system:wds:07346+3153"],
    "counts": {
      "stars": 6,
      "nodes": 10,
      "direct_children": 3,
      "type_counts": {"system": 1, "subsystem": 3, "star": 6}
    },
    "root": {
      "stable_component_key": "comp:msc_system:wds:07346+3153",
      "component_type": "system",
      "display_name": "Castor",
      "total_star_count": 6,
      "collapsed_by_default": false,
      "children": [
        {
          "stable_component_key": "synthetic:orbit:12345",
          "component_type": "subsystem",
          "display_name": "Castor A",
          "total_star_count": 2,
          "collapsed_by_default": false,
          "orbit": {
            "period_days": 342.5,
            "semi_major_axis_au": 5.1,
            "eccentricity": 0.12,
            "inclination_deg": 71.0,
            "confidence_tier": "high",
            "source_catalog": "msc"
          },
          "children": [
            {"stable_component_key": "comp:msc:wds:07346+3153:aa", "component_type": "star", "display_name": "Castor AA"},
            {"stable_component_key": "comp:msc:wds:07346+3153:ab", "component_type": "star", "display_name": "Castor AB"}
          ]
        }
      ]
    }
  }
}
```

Display-name behavior:
- `display_name` prefers human-friendly naming over Gaia placeholders.
- Alias precedence is deterministic: proper/common name, Bayer, Flamsteed, then major catalog IDs (Gl/HIP/HD/HR/TYC/HYG/WDS), with Gaia identifiers last.
- `arm_catalogs` and `arm_evidence` are star-level overlays from `arm.duckdb` and do not mutate core provenance rows.
- `hierarchy` is the generic nested system graph payload assembled from `arm` component, containment, and orbit records.
- `system.star_count` and search `star_count` filters are descendant-aware when `hierarchy` exposes more stars than the flat `core.stars` member list.
- the flat `stars` array remains the canonical direct core membership list; it is not guaranteed to enumerate every nested scientific leaf shown in `hierarchy`.

### GET /systems/by-key/{stable_object_key}
Fetch a system by stable key, with stars and planets.

Response 200: same as `/systems/{system_id}`.

## Error Codes
- `bad_request` (400)
- `invalid_cursor` (400)
- `conflict` (409)
- `not_found` (404)
- `validation_error` (422 framework-generated payload)
- `internal_error` (500)

## Notes
- All values are derived from core data; no speculative enrichment.
- All SQL is parameterized. Sort options are mapped to fixed SQL orderings.
- `catalog_ids` is parsed from `stars.catalog_ids_json`.
