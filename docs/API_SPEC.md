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

### GET /admin/runtime/status
Returns read-only runtime and configuration diagnostics for the Admin Runtime
workspace.

Includes:
- active build id and git head
- auth/OIDC runtime status without secrets
- container-visible path and storage checks
- redacted environment configuration status
- configured/missing flags for sensitive keys
- API process and host/container runtime metrics
- inference endpoint last-probe summaries

Notes:
- Secret values are never returned.
- Container-visible path checks can differ from host paths when a directory is
  not mounted into the API container.
- Docker container health is not queried unless the Docker socket is
  deliberately mounted into the API container.

Response:
- `200` for authenticated admins.
- `401` unauthenticated.
- `403` authenticated non-admin.

### GET /admin/runtime/diagnostics
Returns a redacted JSON diagnostics bundle suitable for copying, downloading,
or opening in a browser tab.

Query params:
- `download` (bool, default `false`): when true, returns the same JSON with a
  `Content-Disposition: attachment` filename.

Includes:
- filesystem summary, alerts, path health, owner/mode, and disk checks
- runtime hardening observations
- auth/container/host/API process summaries
- inference endpoint reachability summaries
- environment configured/missing/alias status without variable values

Notes:
- Secret values are never returned.
- Non-secret environment values are also omitted to keep shared diagnostics
  conservative.
- Requires authenticated admin access.

Response:
- `200` for authenticated admins.
- `401` unauthenticated.
- `403` authenticated non-admin.

### GET /admin/builds/status
Returns read-only build pipeline diagnostics for the Admin Builds workspace.

Includes:
- container-visible raw/cooked/out/reports/served path health
- `served/current` target and resolved build id
- recent immutable `out/<build_id>/` artifact summaries
- per-build required report presence, basic verification gate summaries, and
  snapshot/coolness report summaries
- `snapshot_control` metadata for the selected/latest snapshot job: progress
  counts, elapsed time, output root/size, footprint estimate, storage health,
  latest warning/error, safety warnings, and queued-only cancellation state
- temporary `out/*.tmp` directories for failed/in-progress ingest diagnosis
- retention readiness, blockers, and a dry-run candidate plan with estimated
  reclaimable bytes
- recommended next operator actions derived from active jobs, filesystem state,
  served build state, verification reports, snapshot reports, and retention
  readiness

Notes:
- This endpoint does not mutate build artifacts or reports.
- Admin retention apply is exposed as a high-risk action, not through this
  status endpoint. It requires a recent matching dry-run job, unchanged
  candidate hash, and explicit confirmation before deleting exact candidate
  directories.
- Snapshot reports distinguish missing reports, generated artifacts, reused
  artifacts, zero-generated runs, and explicit null results where requested,
  generated, reused, and manifest-upserted rows are all zero.
- Snapshot generation emits structured `[snapshot-progress]` log lines for new
  jobs. Running snapshot jobs are monitor-only in Admin v2; queued jobs can be
  cancelled through `POST /admin/actions/jobs/{job_id}/cancel`.
- Secret values are never returned.

Response:
- `200` for authenticated admins.
- `401` unauthenticated.
- `403` authenticated non-admin.

### GET /admin/objects/search
Searches systems and system-owned components for the Admin Object Diagnostics
workspace.

Query params:
- `q` optional system name, alias, catalog id, stable key, `system <id>`,
  `star <id>`, `planet <id>`, or arm component name/key such as `Ganymede`
- `limit` optional result limit, default 20, max 50

Response includes candidate systems from the current served core projection plus
disc snapshot URL hints when available. Exact `star <id>` and `planet <id>`
queries return the owning system candidate with a `diagnostic_focus` hint for
Admin clients. Arm component matches, including moons, minor bodies, and
artificial objects, also return the owning system candidate with
`diagnostic_focus: {"type":"component","key":"..."}` and an `object_match`
summary.

Response:
- `200` for authenticated admins.
- `401` unauthenticated.
- `403` authenticated non-admin.

### GET /admin/objects/systems/{system_id}
Returns read-only, layer-aware diagnostics for one system.

Includes:
- public system detail payload reused from the v1 system detail contract
- core identity, aliases, stars, planets, eclipsing binaries, and hierarchy
- provenance completeness diagnostics for system/star/planet rows
- per-planet environment evidence basis and derived candidate
  temperature/insolation fields for Admin triage
- disc coolness, signal counts, coolness contribution explanation, and snapshot
  manifest rows
- expanded arm component, hierarchy-edge, orbit-edge, orbital-solution, and
  stellar-parameter diagnostics, including display-name labels where available
- simulation readiness diagnostics for stars and planets, with checked fields
  labeled as `source`, `derived`, `assumed`, or `missing`; rows include value,
  unit, basis, confidence tier, intended layer, and replacement target
- readiness rows for public detail, coolness, snapshots, arm graph, orbital
  solutions, simulation readiness, and provenance; each row may include `why`,
  `next_action`, and `workspace` fields to explain missing/not-applicable states
  and point the operator toward the relevant Admin workspace

Notes:
- This endpoint does not mutate core, arm, disc, or rim.
- The endpoint is system-dossier scoped. Admin clients may provide direct
  star/planet/component focus views from the returned member and arm rows
  without introducing object mutations or cross-layer writes.
- `diagnostics.arm.components.items` includes core-linked components plus
  connected parent/child endpoint components referenced by returned hierarchy
  edges, such as moons, minor bodies, and artificial objects.
- core-linked planet components include selected core scalar fields such as
  semi-major axis for Admin sorting/navigation; orbit edges and orbital
  solutions include `edge_label` and display-name helper fields when component
  labels are available.
- Admin clients should treat readiness rows as operator guidance, not as hard
  science gates. For example, `orbital_solutions=not_applicable` can be a valid
  result when no connected dynamic orbit edge exists.
- Simulation readiness values are diagnostics unless already source-backed.
  Defensible numeric derivatives should be materialized in `arm`; visual
  defaults and placeholders belong in `disc`.
- When `diagnostics.arm.derived_physical_parameters.items` is present,
  Simulation readiness should prefer those persisted arm rows over runtime
  fallback derivations.

Response:
- `200` for authenticated admins.
- `401` unauthenticated.
- `403` authenticated non-admin.
- `404` if the system id is not found.

### GET /admin/dataset/status
Returns read-only science artifact diagnostics for the Admin Dataset workspace.

Includes:
- current build and layer artifact metadata
- inventory, source contribution, spectral, multiplicity, and compact-object
  summaries
- planet environment coverage, including source equilibrium-temperature rows,
  source-insolation-only rows, proxy-derivable rows, broad HZ environment
  candidates, nice-planet-like candidates, missing environment counts, and gap
  examples
- QC, determinism, catalog lifecycle, and runtime footprint summaries

Notes:
- This endpoint does not mutate build artifacts.
- Proxy-derivable planet environment rows are diagnostics and presentation
  triage signals, not canonical habitability facts.

Response:
- `200` for authenticated admins.
- `401` unauthenticated.
- `403` authenticated non-admin.

### GET /admin/ui
Admin UI scaffold served by the API (under `/api/v2/admin/ui`).

Notes:
- New Admin UI work should target the React/Vite app served at `/admin/`.
- `/api/v2/admin/ui` is retained as a temporary embedded fallback during the
  Admin migration.
- The FastAPI-served `/admin` route remains available only when the API is
  exposed directly; nginx deployments reserve `/admin/` for the React app.

### GET /admin/inference/endpoints
Lists dynamic model endpoint registry records, cached models, and latest probe
status. Secrets are never returned; `api_key_configured` only reports whether a
stored key or configured environment variable is present.

Response:
- `200` for authenticated admins.
- `401` unauthenticated.
- `403` authenticated non-admin.

### GET /admin/inference/credential-envs
Lists provider credential environment variable names visible to the API runtime
for use by inference endpoints with `auth_mode=env`.

Response items include:
- `env_key`
- `provider`
- `label`
- `configured`
- `preferred`
- `source` (`known` or `discovered`)

Notes:
- Secret values are never returned.
- Named provider credentials should use `SPACEGATE_*_API_KEY` names and live in
  `/etc/spacegate/spacegate.env`.
- Endpoints may still reference arbitrary env var names for future credentials.

Response:
- `200` for authenticated admins.
- `401` unauthenticated.
- `403` authenticated non-admin.

### POST /admin/inference/endpoints
Creates an inference endpoint registry record.

Request body:
```json
{
  "endpoint_key": "photon-vllm",
  "display_name": "Photon vLLM",
  "provider": "openai_compatible",
  "base_url": "http://127.0.0.1:8001/v1",
  "auth_mode": "none",
  "api_key_env": null,
  "api_key": null,
  "default_model": "gemma-4-31b-it-qat-w4a16-ct",
  "role_defaults": {},
  "timeout_s": 30,
  "enabled": true,
  "notes": "local bulk endpoint"
}
```

Security:
- Requires authenticated admin session.
- Requires CSRF header (`X-CSRF-Token`).
- `api_key` is encrypted at rest when `auth_mode=stored` and is not returned.

### PATCH /admin/inference/endpoints/{endpoint_id}
Updates endpoint metadata, auth settings, default model, role defaults, timeout,
enabled state, notes, or stored API key. Send `clear_api_key: true` to remove a
stored key.

Security:
- Requires authenticated admin session.
- Requires CSRF header (`X-CSRF-Token`).

### DELETE /admin/inference/endpoints/{endpoint_id}
Soft-removes an endpoint from the active registry and disables it.

Security:
- Requires authenticated admin session.
- Requires CSRF header (`X-CSRF-Token`).

### POST /admin/inference/endpoints/{endpoint_id}/poll-models
Polls the provider model-list endpoint (`/v1/models` for OpenAI-compatible
providers, provider equivalent for others), records probe status, and updates
the model cache.

Security:
- Requires authenticated admin session.
- Requires CSRF header (`X-CSRF-Token`).

Response:
- `200` with endpoint, latest probe, and cached models.
- `404` when the endpoint is missing.
- `502` when the upstream provider cannot be reached or returns malformed data.

### POST /admin/inference/endpoints/{endpoint_id}/smoke-test
Runs a bounded generation smoke test against an endpoint using the selected
role/model routing.

Request body:
```json
{
  "role": "discover",
  "model_id": "optional-model-override",
  "prompt": "Spacegate inference smoke test. Reply with exactly: spacegate inference smoke ok",
  "temperature": 0,
  "max_tokens": 32
}
```

Notes:
- If `model_id` is omitted, the server uses the endpoint role default for
  `role`, then the endpoint default model.
- Supports OpenAI-compatible chat completions and Google Gemini
  `generateContent` provider shapes.
- Prompt text is sent to the provider but is not persisted in the usage event
  or audit payload.
- Records `inference_usage_events` with `request_kind=smoke_test`, role, model,
  success/failure, latency, and token counts when available.

Security:
- Requires authenticated admin session.
- Requires CSRF header (`X-CSRF-Token`).

Response:
- `200` with endpoint, selected role/model, latency, usage, and an output
  excerpt.
- `404` when the endpoint is missing.
- `502` when the upstream provider cannot be reached, rejects the request, or
  returns malformed data.

### GET /admin/inference/stats
Returns aggregate usage counters recorded by the inference runner, grouped by
endpoint and model. This endpoint is useful before generation routing is
automated because it defines the stats contract the runner must write.

Response fields include request count, prompt/completion/total tokens, average
latency, and last-used timestamp.

### GET /admin/inference/eval-reports
Returns read-only agent evaluation report history for model suitability review.

Query params:
- `limit` integer, default `24`, max `200`

Notes:
- Reads `agent_eval_*.json` files from runtime report locations, currently
  `$SPACEGATE_STATE_DIR/reports/agent_eval` and repo-local `reports/agent_eval`.
- Returns searched directories, latest reports, per-report role scores, a
  role-suitability summary, and quarantined anomaly inbox items.
- Eval reports are experimental model-selection evidence. They do not mutate
  `core`, `arm`, `disc`, or any production science layer.

Security:
- Requires authenticated admin session.

### GET /admin/actions/catalog
Returns allowlisted admin actions and parameter schemas.

Notes:
- Includes `display_name` and `category` fields for admin UI grouping (for example `operations` vs `coolness`).
- Includes `group_key` and `operator_guidance` fields for Admin v2 runbook
  layout, prerequisites, outputs, warnings, and recommended next actions.

Response:
- `200` for authenticated admins.
- `401` unauthenticated.
- `403` authenticated non-admin.

### GET /admin/operations/status
Returns an Admin v2 operations summary.

Includes:
- job runner capacity and current queued/running counts
- recent jobs, active jobs, latest failures, and latest high-risk action
- latest admin DB and release metadata backups
- served/current target, recent build artifact summaries, incomplete builds,
  and temporary ingest output directories
- retention readiness and default retention values
- action group metadata for runbook layout

Response:
- `200` for authenticated admins.
- `401` unauthenticated.
- `403` authenticated non-admin.

### GET /admin/agency/status
Returns a read-only Admin v2 agency summary.

Includes:
- Evidence Portfolio workflow stages and predecessor/successor relationships
- admin operational store readiness for dossier, source-file, extraction-set,
  finding, and journal rows
- disc/arm table readiness for later citation, factsheet, exposition, and
  proposal-support materialization surfaces
- latest agent eval report summaries and quarantined anomaly inbox items found
  in runtime report locations
- storage model guidance for hot normalized rows and cold dossier archives
- recommended portfolio-scoped agent interaction model
- source allowlist summary and runtime policy path

Response:
- `200` for authenticated admins.
- `401` unauthenticated.
- `403` authenticated non-admin.

### GET /admin/agency/source-allowlist
Returns the Agency source allowlist policy used by retrieval and future
portfolio-context assembly.

Notes:
- Loads `$SPACEGATE_STATE_DIR/config/agent_source_allowlist.json` when present.
- Falls back to `config/agent_source_allowlist.json` from the repo/image.
- Includes source counts by tier and the loaded/default/runtime paths.

### POST /admin/agency/source-allowlist/sources
Adds or updates one source-domain allowlist entry.

Request body:
```json
{
  "domain": "ui.adsabs.harvard.edu",
  "tier": 1,
  "org": "NASA ADS / Harvard",
  "source_type": "literature_index",
  "trust_score": 1.0,
  "allowed_uses": ["paper discovery", "citations", "bibcodes"],
  "notes": "Best starting point for papers",
  "enabled": true
}
```

Security:
- Requires authenticated admin session.
- Requires CSRF header (`X-CSRF-Token`).
- Audits the source-domain change.

### DELETE /admin/agency/source-allowlist/sources/{domain}
Removes one source-domain allowlist entry from the runtime JSON policy.

Security:
- Requires authenticated admin session.
- Requires CSRF header (`X-CSRF-Token`).
- Audits the source-domain removal.

### GET /admin/agency/seed-candidates
Returns ranked candidate targets for creating new Evidence Portfolios.

Query params:
- `limit` integer, default `50`, max `200`

Notes:
- Read-only.
- Current implementation reads `disc.coolness_scores` from the served build's
  sibling `disc.duckdb`.
- Candidates include rank/score metadata, source build id when available, and
  existing active dossier ids so the UI can avoid duplicate seeding.
- If `disc.duckdb` or `coolness_scores` is absent, the endpoint returns `200`
  with an empty list and explanatory message.

### GET /admin/agency/portfolios
Lists Evidence Portfolio rows from the admin operational store.

Query params:
- `limit` integer, default `50`, max `200`
- `status` optional dossier lifecycle status filter

Notes:
- Read-only.
- Returns counts for attached Source Files, Extraction Sets, Findings, and
  Journal Entries.
- These rows are admin workflow state, not public served `disc` materialization.

### POST /admin/agency/portfolios
Creates a seeded Evidence Portfolio in the admin operational store.

Request body:
```json
{
  "stable_object_key": "gaia_dr3:123",
  "object_type": "system",
  "display_name": "Example System",
  "queue_reason": "coolness_rank",
  "queue_priority": "high",
  "source_build_id": "2026-06-23T120000Z_example",
  "source": "coolness_scores",
  "metadata": {"rank": 7, "score_total": 91.5}
}
```

Security:
- Requires authenticated admin session.
- Requires CSRF header (`X-CSRF-Token`).

Notes:
- Creates one `agent_object_dossiers` row and one
  `agent_portfolio_journal_entries` row.
- Does not run retrieval, extraction, model generation, claim creation,
  proposal creation, `arm` writes, `disc` materialization, or publication.
- Duplicate active portfolios for the same `stable_object_key` and
  `object_type` return `409 conflict`.

### GET /admin/agency/portfolios/{dossier_id}
Returns one Evidence Portfolio with attached Source Files, Extraction Sets,
Findings, and Journal Entries.

Notes:
- Read-only.
- JSON payload fields from storage columns are parsed into structured response
  objects.
- Returns `404` when the portfolio does not exist.

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

Job responses include `requested_by_user_id` and `requested_by` display
metadata when the requesting user still exists in the admin auth database.
Detailed job responses also include best-effort `artifact_hints` for expected
or detected outputs such as build report directories, release metadata,
snapshots, retention summaries, backups, and profile files. These hints are
derived at read time from job parameters, logs, and current filesystem state;
they are operator guidance, not immutable provenance records.

### GET /admin/actions/jobs/{job_id}/audit
Lists audit entries correlated with a job id.

Query params:
- `limit` (default 50, max 200)

Response:
- `200` with `job_id` and `items` using the same item shape as `GET /admin/audit`
- `404` when the job is missing.

### GET /admin/actions/jobs/{job_id}/events
Lists structured lifecycle events for a job.

Query params:
- `limit` (default 100, max 500)

Response:
- `200` with `job_id` and `items`
- `404` when the job is missing.

Event items:
```json
{
  "event_id": 12,
  "job_id": "job_...",
  "event_type": "started",
  "event_status": "running",
  "message": "Started Verify Build.",
  "details": {"action": "verify_build"},
  "created_at": "2026-06-24T15:30:00Z",
  "synthetic": false
}
```

Older jobs may return synthetic events derived from job timestamps when no
structured event rows exist.

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

### GET /admin/actions/jobs/{job_id}/log/text
Returns full job log text for inline browser viewing.

Response:
- `200 text/plain` with `Content-Disposition: inline; filename="<job_id>.log"`
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
- `correlation_id` (optional match against correlated detail payloads, commonly `job_...`)

Response shape:
```json
{
  "items": [
    {
      "audit_id": 7,
      "actor_user_id": null,
      "actor": null,
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

When `actor_user_id` is present, `actor` contains display metadata from the
admin auth database:

```json
{
  "actor": {
    "user_id": 1,
    "email": "admin@example.com",
    "display_name": "Admin User",
    "roles": ["admin"]
  }
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
