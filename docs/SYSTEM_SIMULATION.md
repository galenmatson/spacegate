# Spacegate System Simulation Contract

This document defines the beta contract for live system simulation scenes. The
goal is to let the public web client render deterministic stars, planets, and
orbits while preserving Spacegate's layer boundaries.

## Current Readiness

Already in place:

- public system detail rows with stable `system_id` and `stable_object_key`
- `arm.component_entities`, `arm.system_hierarchy_edges`, `arm.orbit_edges`,
  and `arm.orbital_solutions`
- Admin Object Diagnostics simulation-readiness fields that classify inputs as
  `source`, `derived`, `assumed`, or `missing`
- additive `render_scene_v0.2` payload for live previews with renderer-ready
  star bodies, planet bodies, binary orbit groups, and provenance-bearing fields
- `render_scene_v0.2` reconciles body lists against canonical hierarchy when
  hierarchy exposes richer physical membership than direct core rows or ARM
  orbit endpoints alone
- ARM `planetary_orbit` edges and `source_native_planet_orbit` solutions for
  currently host-linked NASA Exoplanet Archive and Sol authority planet rows
- Sol hierarchy/orbit arm rows for planets, moons, selected small bodies, and
  curated artificial objects
- React 19 + Three.js/R3F runtime foundations from the 3D map

Not ready yet:

- a client simulation clock and propagation policy
- source-refresh verification for stale multiplicity/orbital inputs
- uncertainty visualization
- final scale policy for stellar radii, planetary radii, orbital distances,
  labels, and time acceleration
- persisted `disc` assumption rows for visualization-only defaults

## Public API

Initial endpoint:

- `GET /api/v1/systems/{system_id}/simulation-scene`

Response shape:

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
    "bodies": {"stars": [], "planets": []},
    "orbits": []
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

Rules:

- `core` rows are source-faithful canonical inventory/projection rows.
- `arm` rows are deterministic science support, adjudication, hierarchy, and
  orbital-solution rows. Arm may hold source-native rows when they are evidence
  for a canonical model rather than canonical inventory themselves.
- `disc` rows may hold generated rendering assumptions and reproducible
  presentation artifacts.
- `rim` rows are fictional/worldbuilding overlays only.
- Missing orbital data must not be silently replaced with canonical-looking
  values. Visualization defaults belong in `disc` and must be labeled.
- `render_scene_v0.2` may emit transient deterministic assumptions using
  `procedural_prior_v1`; persistent assumption rows remain future
  `disc.simulation_assumptions` work.
- Unreviewed Agency output may propose evidence or assumptions, but must not
  write directly into `core`.

## Simulation Model

The first renderer should treat the endpoint as a scene-description contract,
not as a physics engine.

Minimum body model:

- stable object identity
- display name and aliases
- object type (`star`, `planet`, `moon`, `subsystem`, etc. as rows become
  available)
- parent/child containment from hierarchy edges
- preferred dynamic relation from orbit edges
- physical render inputs with source/derived/assumed/missing status:
  - stellar effective temperature, luminosity, mass, radius
  - planet orbital period, semi-major axis, eccentricity, radius, mass,
    incident flux, equilibrium temperature
- provenance source catalog/version/row key where available

Minimum orbit model:

- attach orbit elements to an `orbit_edge_id`, not only to a body
- include units, reference epoch, and confidence/provenance when present
- support absent elements without fabricating a canonical solution
- treat planet orbits like multiplicity orbits: promoted/default scalar fields
  may exist on core planet rows for serving, but full source-native solutions,
  alternate fits, epochs, uncertainty, and simulation-ready elements belong in
  `arm.orbital_solutions`
- start with static ellipses and clearly labeled uncertainty
- defer true N-body simulation; client animation should be Keplerian/teaching
  grade until a stronger numerical contract exists

## Source Refresh Strategy

Near-term source priorities:

- NASA Exoplanet Archive `ps` / `pscomppars`: canonical confirmed planet
  baseline, with `ps` useful for source-specific solution rows and
  `pscomppars` useful for display defaults.
- Gaia DR3 NSS: current Gaia non-single-star baseline until Gaia DR4 is
  available. Gaia DR4 is scheduled for December 2, 2026, so this should become
  a planned ingest transition rather than an assumption that DR3 remains final.
- WDS and ORB6: visual-binary support evidence and orbital solutions, mapped
  only when Spacegate can attach rows to a unique, confidence-gated system edge.
- MSC: mandatory multiplicity hierarchy evidence. Spacegate now targets the
  upstream June 19, 2026 archive (`newmsc-20260619.tar.gz`). The current archive
  places `sys.tsv`, `orb.tsv`, and `comp.tsv` at the archive root; the cooker
  accepts both that layout and the older `export/*.tsv` layout. Local canonical
  build `20260628T1210Z_msc20260619` promoted on June 28, 2026 and passed the
  required multiplicity golden checks. Spacegate now preserves `sys.tsv` as
  `msc_system_details` and `orb.tsv` as `msc_orbit_details`, materializing MSC
  hierarchy/orbit edges and source-native `arm.orbital_solutions` where
  supported endpoint keys exist. Castor is the primary regression benchmark for
  nested AB/C hierarchy, inner binary periods, and no unsafe spectral
  inheritance. Canonical hierarchy emit must bridge source-native nested MSC
  subsystem edges back into descendant-aware canonical hierarchy leaves, so
  systems such as Nu Sco retain effective star counts even when some source
  leaves are represented as inferred MSC components rather than direct core
  star rows.
- SBX: current spectroscopic-binary support source. Keep SB9 as historical
  context only; default ingestion should prefer SBX where licensing and format
  checks pass.
- JPL Horizons / SBDB: Sol-system orbital authority for planets, moons, small
  bodies, and selected artificial objects.

Agency-compatible ingest plan:

1. Fetch and checksum current source snapshots into raw/cooked manifests.
2. Normalize source-native rows without adjudication.
3. Materialize deterministic `arm` candidates for hierarchy/orbit/parameter
   evidence with provenance and confidence.
4. Run golden-system checks before promotion.
5. Let Agency/literature workflows propose fixes, but require reviewed
   citations and verdict state before any public `arm` or `disc` materialization.
6. Keep rejected/conflicting proposals in audit tables rather than deleting
   evidence.

## Next Implementation Milestone

Build a system preview renderer for a small target set:

- Sol
- Alpha Centauri
- TRAPPIST-1
- one compact multi-star system with an ORB6/SBX solution
- one messy hierarchy watchlist system

Success criteria:

- each scene renders without blocking the map
- the renderer is lazy-loaded from system detail pages
- browser QA covers at least one live-preview scene path
- planet motion uses source orbital periods when present and deterministic
  seeded phases for reproducible non-aligned starting positions
- TRAPPIST-1 benchmark scenes source period, semi-major axis, eccentricity, and
  inclination from `arm.orbital_solutions` before falling back to promoted
  `core.planets` summary scalars
- multi-star previews render ARM hierarchy/orbit evidence as barycentric visual
  groups when connected binary edges are available
- source/derived/assumed/missing fields surface as visible provenance pills
- every rendered assumption is visible in the readiness payload
- static snapshots remain the fallback for browsers/devices without usable 3D
- no `rim` artifacts or fictional orbits are mixed into science scenes
