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

### GET /public-config
Returns nonsecret public runtime branding used by browser clients.

Source:
- `SPACEGATE_SITE_NAME` (default `Coolstars`)
- optional `SPACEGATE_MAP_TITLE`; when unset the API returns
  `<SPACEGATE_SITE_NAME> Map`
- optional `SPACEGATE_STELLAR_DATABASE_URL`; when unset the API returns the
  current request origin plus `/search`

Response:
```json
{
  "site_name": "Coolstars",
  "map_title": "Coolstars Map",
  "spacegate_url": "https://coolstars.org/search",
  "branding_source": "environment"
}
```

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

### GET /map/systems
Returns a compact system-point payload for the first public 3D map runtime.

Query params:
- `max_dist_ly` (float, default `100`, max `100`): pilot radius cap.
- `limit` (int, default `20000`, max `50000`): safety cap for returned rows.
- `compact` (bool, default `false`): when true, returns the browser render
  profile with rounded coordinates and without diagnostic-only fields.

Response:
```json
{
  "scope": "systems",
  "frame": "heliocentric_icrs_j2016",
  "max_dist_ly": 100,
  "limit": 20000,
  "total_available": 10513,
  "returned": 10513,
  "truncated": false,
  "planet_systems": 310,
  "multi_star_systems": 443,
  "spectral_counts": {"M": 6000},
  "items": [
    {
      "system_id": 1,
      "stable_object_key": "canon:system:...",
      "system_name": "Example",
      "dist_ly": 12.3,
      "x_helio_ly": 1.0,
      "y_helio_ly": 2.0,
      "z_helio_ly": 3.0,
      "star_count": 1,
      "planet_count": 0,
      "spectral_classes": ["M"],
      "dominant_spectral_class": "M",
      "coolness_rank": 42,
      "coolness_score": 18.5,
      "has_snapshot": true
    }
  ]
}
```

### GET /systems/{system_id}/simulation-scene
Returns the beta scene-readiness payload for live 3D system previews and future
system simulation rendering.

This endpoint is read-only. It assembles existing public detail rows,
hierarchy, arm graph/orbit rows, and simulation-readiness diagnostics. It does
not persist generated assumptions and does not promote Agency output.

Runtime may serve a prebuilt compressed scene artifact from
`disc/simulation_scenes/` before falling back to in-process cache or live
assembly. The JSON contract is unchanged. Responses expose
`X-Spacegate-Simulation-Scene-Cache` with `prebuilt`, `hit`, or `miss` for
diagnostics.

Prebuilt artifacts are compatible only with the current renderer contract. If
an artifact lacks required current diagnostics, such as
`render_scene.diagnostics.membership_reconciliation`, the API may skip it and
serve a dynamic `miss` response until artifacts are regenerated.

Response:
```json
{
  "schema_version": "simulation_scene_v0",
  "scope": "system_simulation_scene",
  "generated_at_utc": "2026-06-28T00:00:00Z",
  "frame": "heliocentric_icrs_j2016",
  "system": {},
  "bodies": {
    "stars": [],
    "planets": []
  },
  "hierarchy": {},
  "arm": {
    "components": {"count": 0, "items": []},
    "hierarchy_edges": {"count": 0, "items": []},
    "orbit_edges": {"count": 0, "items": []},
    "orbital_solutions": {"count": 0, "items": []},
    "msc_system_details": {"count": 0, "items": []},
    "stellar_parameters": {"count": 0, "items": []},
    "derived_physical_parameters": {"count": 0, "items": []}
  },
  "simulation_readiness": {
    "score": 0.0,
    "counts": {"source": 0, "derived": 0, "assumed": 0, "missing": 0},
    "required_field_count": 0,
    "status": "missing",
    "stars": [],
    "planets": []
  },
  "render_scene": {
    "schema_version": "render_scene_v0.2",
    "assumption_generator_version": "procedural_prior_v1",
    "preferred_visualization": "live_3d",
    "fallback_visualization": "deterministic_snapshot",
    "visual_scale": {
      "schema_version": "visual_scale_beta_v1",
      "scale_mode": "clarity_scaled_not_physical",
      "default_scale_mode": "structure",
      "available_scale_modes": [
        {
          "mode": "structure",
          "label": "Structure/Clarity",
          "preserves": "nested hierarchy readability and inspectable bodies",
          "sacrifices": "physical body-size and orbit-spacing ratios"
        },
        {
          "mode": "true_orbits",
          "label": "True Orbits",
          "preserves": "relative planet semi-major-axis spacing within the scene",
          "sacrifices": "body-size realism and close-in orbit readability"
        },
        {
          "mode": "true_bodies",
          "label": "True Bodies",
          "preserves": "more physical body-size contrast than clarity mode",
          "sacrifices": "small-body visibility and practical physical orbit scale"
        },
        {
          "mode": "log",
          "label": "Log Scale",
          "preserves": "rank order across large size and orbit ranges",
          "sacrifices": "linear physical ratios"
        }
      ],
      "scene_unit": "arbitrary_scene_unit",
      "presentation_only": true,
      "collision_policy": {
        "applies_to_modes": ["structure", "log"],
        "star_radius_fraction_of_nearest_sep": 0.28,
        "min_visible_star_radius_scene": 0.045,
        "min_halo_radius_scene": 0.16,
        "min_pick_radius_scene": 0.28
      }
    },
    "bodies": {"stars": [], "planets": [], "subsystems": []},
    "orbits": [],
    "simulation_tree": {
      "schema_version": "simulation_tree_v1",
      "root_node_key": "root:system",
      "nodes": {
        "root:system": {
          "node_key": "root:system",
          "node_type": "root",
          "children": []
        }
      },
      "diagnostics": {
        "node_count": 0,
        "body_node_count": 0,
        "orbit_node_count": 0,
        "root_child_count": 0,
        "nested_orbit_count": 0,
        "unattached_orbit_count": 0,
        "warnings": []
      }
    },
    "assumptions": [],
    "assumption_count": 0,
    "persisted_assumption_count": 0,
    "diagnostics": {
      "body_counts": {"stars": 0, "planets": 0, "subsystems": 0},
      "subsystem_handle_counts": {
        "source_native": 0,
        "simulation_tree_fallback": 0
      },
      "orbit_counts": {
        "total": 0,
        "by_endpoint_kind": {},
        "by_relation_kind": {}
      },
      "field_status_counts": {"source": 0, "derived": 0, "assumed": 0, "missing": 0},
      "assumption_persistence_counts": {"persisted": 0, "transient": 0},
      "simulation_tree": {}
    },
    "provenance_legend": {
      "source": "Catalog/source value from core or arm.",
      "derived": "Deterministic derived value; should be reviewed before stronger science claims.",
      "assumed": "Deterministic presentation/visualization prior only.",
      "missing": "Required value not available."
    }
  },
  "policy": {
    "canonical_layer": "core",
    "derived_layer": "arm",
    "presentation_assumption_layer": "disc",
    "fiction_overlay_layer": "rim",
    "time_policy": "static_epoch_scene_until_client_simulation_clock_contract",
    "missing_orbit_policy": "do_not_invent_canonical_orbits",
    "agency_policy": "unreviewed_agency_output_must_not_write_core"
  }
}
```

Contract notes:

- `core` supplies canonical/source-faithful rows.
- `arm` supplies deterministic hierarchy, orbit, stellar-parameter, and derived
  science support rows.
- Planet render fields prefer ARM `planetary_orbit` +
  `orbital_solutions` values for period, semi-major axis, eccentricity, and
  inclination. If a linked ARM solution is absent, the endpoint may fall back to
  promoted `core.planets` scalar summaries and labels that basis explicitly.
- Rank-1 ARM planet orbital solutions are the default API/simulation source.
  NASA Exoplanet Archive `ps` alternates, when present, are retained as
  additional ranked `arm.orbital_solutions` rows for future diagnostics and
  explicit solution-selection UI, not silently promoted over `pscomppars`.
- If a planet lacks a renderable source inclination, `render_scene` may replace
  the missing render field with a deterministic `status="assumed"`,
  `layer="disc_assumption"` visual fallback. In multi-planet systems this
  fallback should prefer a coplanar prior seeded from same-host source
  inclinations when available, and only fall back to centered low-tilt when no
  planet source inclination exists. This does not change the underlying
  `simulation_readiness` missingness and must not be treated as a source
  orbital element.
- `disc` is the future home for visualization-only assumptions and static
  fallback artifacts.
- Browser clients should use `render_scene` when WebGL/R3F is available and
  fall back to the detail payload's deterministic `snapshot.url` when WebGL is
  unavailable or the live preview fails to initialize.
- `render_scene` is an additive renderer-ready view over the source payload.
  It uses direct core rows and ARM orbit endpoints first, then reconciles
  renderer-ready bodies against canonical hierarchy when hierarchy exposes
  nested stars or planets not present in the direct selected-system body lists.
  Single-star render bodies may use a system proper-name/common-name alias for
  display while preserving the canonical star key; `source.display_name_basis`
  identifies whether the rendered label came from a system alias or core row.
  Stellar render bodies keep `object_type="star"` as the renderer role and add
  `body_class`, nullable `compact_type`, and `fields.object_type` for
  provenance-aware physical class inspection. Compact companions such as white
  dwarfs remain stellar render bodies while carrying their source compact class.
  Stellar render bodies also expose `fields.visual_stellar_class`, the
  presentation class used for simulator material/color choices. This is
  separate from catalog spectral fields: source spectral evidence wins when
  available, compact-object evidence overrides main-sequence priors, Teff/color
  constraints may produce derived renderer classes, and mass-only
  main-sequence priors use `basis="mass_main_sequence_prior_v1"`,
  `status="assumed"`, and `layer="render_scene"`. Clients must label those
  mass priors as visual/render assumptions and must not display them as source
  spectral classes.
  Planet render bodies include `host_star_id` from the canonical planet row
  where available and `host_body_key` when that host resolves to a rendered
  star body. `source.host_resolution` records whether the linkage came from a
  direct `core.planets.star_id` match, a catalog-equivalent core star already
  represented by a source-native render component, a singleton render-star
  fallback, or remained missing/ambiguous. Planet render bodies are emitted in
  orbital order by source semi-major axis when available, then by period, with
  `sort_index` rewritten to the final render order. They also include
  `fields.planet_visual_class`, a `render_scene` provenance field used for
  renderer material selection; it is presentation classification only, not a
  canonical planet taxonomy.
  Subsystem render bodies are inspectable presentation handles over canonical
  hierarchy/ARM component rows. They expose `child_body_keys` for rendered
  descendant stars plus provenance-backed `component_label`,
  `hierarchy_basis`, and derived child-count fields, but they do not create new
  core stars or ARM orbit facts.
  When a served slice lacks explicit hierarchy-backed subsystem bodies, the API
  may synthesize subsystem handles from `simulation_tree_v1` barycenter nodes.
  These fallback handles carry `fallback_subsystem=true`,
  `node_kind="simulation_tree_fallback"`, `source.layer="render_scene"`, and
  `source.basis="simulation_tree_fallback_subsystem"`. They are runtime
  presentation handles only. Source-native subsystem handles remain preferred
  and suppress this fallback when present.
  Orbit rows include `endpoint_kind`; `star_pair` entries animate/render direct
  body pairs, while `group_pair` entries represent hierarchical subsystem
  edges with `primary_child_body_keys` and `secondary_child_body_keys` for
  cluster orbit guides and browser-side visual child-cluster transforms. Browser
  orbit readouts should use the same field provenance objects as body readouts
  for SOURCE/DERIVED/ASSUMED/MISSING pills and copyable provenance.
  `simulation_tree_v1` is the preferred client-side stellar animation contract.
  It is derived from the emitted render bodies and orbit rows: body nodes point
  at rendered stars, barycenter nodes point at one orbit row, and root nodes
  collect the top-level connected components. If a later orbit side's rendered
  leaf set exactly matches an earlier orbit node, the later node references
  that barycenter, making nested systems such as HD 213885 and Castor explicit
  without creating new core stars or ARM orbit facts. The diagnostics report
  node counts, nested orbit count, unattached orbit count, and warnings so
  clients and tests can distinguish a complete tree from a fallback layout.
  Stellar hierarchy periods are selected in order: `arm.orbital_solutions`,
  MSC `sys.tsv`/`msc_system_details` period rows, projected-separation Kepler
  estimates when distance and endpoint masses are available, then explicit
  `disc_assumption` visual fallback periods. The estimate/fallback path must
  remain labeled and must not be promoted into ARM as a fitted orbit solution.
  `display_radius_scene` for stellar orbit rows is presentation scale, but it
  should preserve broad separation order using source semi-major axis,
  source/projected angular separation plus distance, or a period+mass Kepler
  estimate before falling back to generic visual radii. This keeps Alpha
  Centauri AB-C-style long-period companions visibly outside compact inner
  binaries without claiming physical scene units.
  Clients that activate `simulation_tree_v1` should use tree body positions for
  hosted planet/HZ presentation overlays when `host_body_key` resolves to a
  tree body node.
  Browser orbit readouts add presentation-only guide/trace provenance fields
  such as `orbit_guide_trace`, `planet_orbit_trace`, and
  `binary_body_paths`; these explain how the visible line/path was sampled and
  whether body paths are mass-weighted or equal-mass visual fallbacks. They are
  render-scene provenance, not new ARM orbit evidence.
  Two-rendered-star systems with no source orbit edge may emit one
  `relation_kind="visual_binary_fallback"` `star_pair` orbit so compact
  companion scenes such as Sirius are structurally legible. Those orbit fields
  are `status="assumed"`, `layer="disc_assumption"` presentation defaults and
  must not be interpreted as ARM orbital solutions.
- `render_scene.visual_scale` documents the beta renderer's selectable
  presentation transforms for star radii, planet radii, planet orbit spacing,
  and binary orbit display radii. Scene units are arbitrary presentation units,
  not physical distance or radius units; source science values remain in the
  individual field provenance objects and core/arm rows. The default
  `structure` mode is hierarchy-first and collision-safe for visible stellar
  meshes. `true_orbits` preserves linear rendered planet-orbit ratios from
  source semi-major axes inside the scene envelope; it does not add an inner
  readability offset. `true_bodies` and `log` are also client presentation
  modes and must not mutate core, ARM, DISC, or RIM data.
- `render_scene.assumptions` is an additive export of every rendered field with
  `status="assumed"`. Each record is shaped for
  `disc.simulation_assumptions` materialization and includes `assumption_key`,
  object binding, `parameter_key`, `value_json`, `assumption_kind`,
  `assumption_method`, `assumption_version`, `replacement_target`,
  `visibility_label`, seed, generator version, persistence status, and the
  original field provenance object. `persistence_status` is `transient` until
  the selected-system materializer writes a matching
  `disc.simulation_assumptions` row, then `persisted`.
  `persisted_assumption_count` reports how many current rendered assumptions
  matched the served `disc` table.
  Group-pair display transforms are presentation scale only; source orbital
  evidence remains in `arm.orbit_edges` and `arm.orbital_solutions`.
  It may include deterministic procedural fields with `status="assumed"`,
  `layer="disc_assumption"`, `basis="procedural_prior_v1:..."`, and a stable
  `seed`. These values are visual defaults only; they are not canonical science.
- `render_scene.diagnostics` summarizes the final renderer-ready payload:
  body counts, orbit counts by endpoint/relation kind, field status counts, and
  assumption persistence counts, plus subsystem handle source/fallback counts
  and a copy of the simulation-tree diagnostics. It also includes
  `membership_reconciliation`, which reports source hierarchy leaf count,
  rendered stellar body count, active membership gate, and unmatched
  detail/orbit endpoint keys when source hierarchy membership constrains
  rendered stars.
  It is an audit aid derived from the emitted scene objects, not a replacement
  for the objects or their field provenance.
- Builds may also include core reconciliation audit tables
  `source_object_reconciliation` and
  `source_object_reconciliation_quarantine`. These explain source surrogate
  merges that happened before root-system grouping; they are not returned by
  default in public scene payloads, but scene membership must reflect their
  accepted result.
- `rim` remains excluded from this science endpoint.
- Missing orbital elements are exposed as missing/assumed readiness fields
  rather than silently filled as canonical data.

Notes:
- This endpoint is a map render/selection contract, not a general search API.
- The pilot is intentionally capped to 100 ly until tile/LOD loading exists.
- Coordinates are core heliocentric positions at the canonical build epoch.
- `compact=true` preserves `system_id`, display name, heliocentric render
  coordinates, counts, dominant spectral class, coolness rank/score, and
  snapshot availability. It omits `stable_object_key`, `spectral_classes`,
  detailed temperature fields, and nice/weird planet counters because the v0.2
  map renderer does not consume them.

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

### GET /admin/status/dataset
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
- Includes source counts by tier, the loaded/default/runtime paths, and
  restorable versions.
- Each source includes an `enabled` flag. Disabled sources remain visible for
  audit/review, but retrieval and future portfolio-context assembly must treat
  them as not allowed.

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

### POST /admin/agency/source-allowlist/restore-default
Restores the shipped Spacegate source allowlist by removing the runtime
override after first snapshotting the current runtime JSON when present.

Use this when operator edits have damaged, over-pruned, or otherwise confused
the runtime policy. The shipped default remains in the repo/image and is never
mutated by Admin.

Security:
- Requires authenticated admin session.
- Requires CSRF header (`X-CSRF-Token`).
- Audits the restore.

### POST /admin/agency/source-allowlist/restore-version
Restores one previous runtime source allowlist snapshot.

Request body:
```json
{"version_id": "20260626T190000Z_abcdef123456_1234abcd.json"}
```

Security:
- Requires authenticated admin session.
- Requires CSRF header (`X-CSRF-Token`).
- Audits the restore.

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
- `stable_object_key` optional exact object filter
- `object_type` optional exact type filter (`system`, `star`, or `planet`)

Notes:
- Read-only.
- Returns counts for attached Source Files, Extraction Sets, Findings, and
  Journal Entries.
- These rows are admin workflow state, not public served `disc` materialization.
- Object Diagnostics uses the exact object filters to show whether the focused
  object already has an Evidence Portfolio.

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
- `sort` (`match` | `name` | `distance` | `coolness` | `planet_count` | `star_count` | `hottest` | `coolest`, default `name`; public UI uses `match` for named searches and falls back to `coolness` for blank browsing)
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
- search responses include presentation-scoped preview policy fields so clients
  can avoid expensive `/simulation-scene` requests for ordinary singleton
  systems. These fields are runtime/display policy, not canonical science.
- current Gaia-first production builds use transitional AT-HYG alias
  crosswalks plus deterministic Bayer expansion for public name coverage; this
  supports lookups such as `Castor`, `Alpha Geminorum`, `Alpha Centauri`,
  `Toliman`, `Sirius`, `Jabbah`, and `Copernicus` without making AT-HYG a
  canonical inventory authority.
- accepted ATHYG supplement rows without Gaia IDs can still contribute reviewed
  proper/Bayer/Flamsteed aliases through HIP/HD/source-row resolution; Sirius A
  is the first reviewed exception and restores `Alpha Canis Majoris` without
  assigning that alias only to Sirius B.
- source-object reconciliation can cause a member alias to resolve to the
  owning accepted physical system. For example, `Proxima Centauri` should open
  the accepted Alpha Centauri system while preserving Proxima as the focused
  member/source planet host for Proxima b/d.
- temperature filters use system-level bounds as a pruning step and may still confirm against per-star rows for exact interval semantics.
- Star Search `star_count` filters and `sort=star_count` use the materialized search facet for fast, stable public browsing. Detail, hierarchy, and simulation payloads may expose richer descendant-aware multiplicity counts from `arm`; a later build-normalization pass should promote the best audited hierarchy count into the search facet.
- `sort=hottest` and `sort=coolest` use system-level stellar-temperature facets (`max_star_teff_k` and `min_star_teff_k`) when available, falling back to per-star aggregation on legacy builds. Systems without temperature evidence sort last.

Responses include `match_rank` and are sorted by:
`match_rank` asc, `dist_ly` asc, `system_name_norm` asc.

When `q` is not provided:
- `sort=name`: `system_name_norm` asc, `system_id` asc
- `sort=distance`: `dist_ly` asc nulls last, `system_id` asc
- `sort=coolness`: `coolness_rank` asc, `system_name_norm` asc, `system_id` asc
- `sort=planet_count`: materialized browse `planet_count` desc, `system_name_norm` asc, `system_id` asc
- `sort=star_count`: materialized browse `star_count` desc, `system_name_norm` asc, `system_id` asc
- `sort=hottest`: `max_star_teff_k` desc nulls last, `system_name_norm` asc, `system_id` asc
- `sort=coolest`: `min_star_teff_k` asc nulls last, `system_name_norm` asc, `system_id` asc

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
      "preview_tier": "lightweight_singleton",
      "preview_basis": ["single_or_unresolved_star", "no_planets", "low_preview_complexity"],
      "is_lightweight_preview_safe": true,
      "has_prebuilt_simulation_scene": false,
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

Client contract:
- Star Search v2 treats `snapshot.url` as fallback/reference metadata. Capable
  browsers should prefer the `/systems/{system_id}/simulation-scene` contract
  for live or simulation-derived previews, with bounded concurrent WebGL
  contexts and cached frame reuse in result lists.
- `preview_tier` values:
  - `lightweight_singleton`: render a cheap client-side singleton preview from
    search fields only; do not fetch `/simulation-scene` until the user opens
    Peek, Explore, or the system page.
  - `prebuilt_simulation_scene`: full System Simulation preview is appropriate
    and the API has a compressed scene artifact available.
  - `dynamic_simulation_scene`: full System Simulation preview is appropriate
    but the API will assemble the scene dynamically unless it is already in the
    in-process runtime cache.
- `preview_basis` explains the policy trigger, for example `planet_host`,
  `multistar_system`, `compact_or_exotic_class`, `high_coolness`, or
  `low_preview_complexity`.
- Search cards should keep catalog IDs copyable but visually secondary.

### GET /systems/{system_id}
Fetch a system with its stars, planets, aliases, hierarchy, provenance, and
fallback snapshot metadata. Public system pages should be simulation-first:
the live `/systems/{system_id}/simulation-scene` payload is the primary visual
anchor when WebGL is available, while this detail payload supplies inventory,
copyable identifiers, table rows, hierarchy, and evidence/provenance sections.

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
- Alias precedence is deterministic: proper/common name, Bayer including
  expanded Greek-letter/constellation forms, Flamsteed, then major catalog IDs
  (Gl/HIP/HD/HR/TYC/HYG/WDS), with Gaia identifiers last.
- `arm_catalogs` and `arm_evidence` are star-level overlays from `arm.duckdb` and do not mutate core provenance rows.
- `hierarchy` is the generic nested system graph payload assembled from `arm` component, containment, and orbit records.
- `system.star_count` in detail payloads is descendant-aware when `hierarchy` exposes more stars than the flat `core.stars` member list. Star Search browse filters use the materialized search facet for public-performance reasons.
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
