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
- the first browser renderer uses the hierarchy tree for stable visual cluster
  centers and hosts nested planets around their render host/body group
- the first browser renderer has a pauseable local animation clock, sampled
  eccentric/inclined orbit guide paths, and hover vitals for rendered bodies
- the beta browser renderer adds speed control, reset, orbit-trace visibility
  toggle, camera orbit/zoom/pan controls with reset-view support, click/tap
  pinned inspection, copyable render/source identifiers, and touch-safe canvas
  gesture handling
- the beta scene contract includes `visual_scale_beta_v1`, an explicit
  clarity-scale policy for star radii, planet radii, planet orbit spacing, and
  binary/group orbit display radii
- selected-system `disc.simulation_assumptions` materialization is available
  through `scripts/materialize_simulation_assumptions.py`; the API annotates
  matching rendered assumptions with `persistence_status="persisted"`
- the browser renderer checks WebGL capability before mounting R3F and falls
  back inside the preview panel to the deterministic snapshot artifact when 3D
  is unavailable or the live scene load fails
- `render_scene_v0.2` includes `endpoint_kind='group_pair'` orbit entries for
  hierarchical subsystem pairs such as Castor A-B and AB-C; these render as
  cluster orbit guides rather than false individual-star binaries
- ARM `planetary_orbit` edges and `source_native_planet_orbit` solutions for
  currently host-linked NASA Exoplanet Archive and Sol authority planet rows
- Sol hierarchy/orbit arm rows for planets, moons, selected small bodies, and
  curated artificial objects
- React 19 + Three.js/R3F runtime foundations from the 3D map

Not ready yet:

- a full client simulation clock, epoch controls, and propagation policy
- source-refresh verification for stale multiplicity/orbital inputs
- uncertainty visualization
- physical-scale/precision display modes for stellar radii, planetary radii,
  orbital distances, labels, and time acceleration
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
    "visual_scale": {
      "schema_version": "visual_scale_beta_v1",
      "scale_mode": "clarity_scaled_not_physical",
      "scene_unit": "arbitrary_scene_unit"
    },
    "bodies": {"stars": [], "planets": []},
    "orbits": [],
    "assumptions": [],
    "assumption_count": 0,
    "persisted_assumption_count": 0
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
- `render_scene_v0.2` may emit deterministic assumptions using
  `procedural_prior_v1`; selected systems can be persisted into
  `disc.simulation_assumptions` by `scripts/materialize_simulation_assumptions.py`.
- `render_scene_v0.2` also exports every rendered `status="assumed"` field in
  `render_scene.assumptions` using the `disc.simulation_assumptions`
  object-binding shape. API records include a stable `assumption_key` and
  `persistence_status`; persisted rows remain presentation assumptions and do
  not become science facts.
- `visual_scale_beta_v1` is a presentation contract. It tells clients how the
  beta renderer exaggerates/normalizes radii and orbit spacing for clarity, and
  must not be interpreted as source physical scale.
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
- hierarchical subsystem orbit edges render as distinct group-pair guides so
  nested structure is visible without flattening the system into sibling stars
- group-pair edges also drive deterministic visual-scale child-cluster motion in
  the browser preview; this is a presentation transform over ARM evidence, not a
  source-scaled barycentric solution
- single-star scenes can render a human system alias as the body display name
  while preserving the canonical star key and exposing the display-name basis
- users can pause/start, change speed, reset the local clock, hide/show orbit
  traces, orbit/zoom/pan the preview camera, reset the view, hover
  bodies/orbits, and pin a copyable object/orbit readout
- pinned stars, planets, and orbit paths also receive in-scene visual feedback
  so the selected readout has a visible target in the 3D view
- hover and pinned readouts use the same source/derived/assumed/missing
  evidence-pill affordance as the summary panel, including focusable provenance
  popovers in pinned readouts
- star and planet surfaces use deterministic procedural renderer materials
  based on existing scene fields and stable object keys; they are visual
  presentation only, not source surface maps or persisted assumptions
- planet radii use bounded clarity-scale caps/floors so compact systems remain
  inspectable in the beta preview; `visual_scale_beta_v1` documents the active
  transform and physical-scale rendering remains future work
- WebGL-disabled browsers receive the deterministic system snapshot in the live
  preview panel instead of a blank or broken canvas
- source/derived/assumed/missing fields surface as visible provenance pills
- every rendered assumption is visible in the readiness/render payloads
- every rendered assumption is exported as a structured render-scene assumption
  record suitable for selected-system `disc.simulation_assumptions`
  materialization
- benchmark simulator assumptions are materialized in the current DISC artifact
  with `simulation_assumptions_materializer_v1`; broader reviewed curation and
  batch policy remain future work
- static snapshots remain the fallback for browsers/devices without usable 3D
- no `rim` artifacts or fictional orbits are mixed into science scenes

Known benchmark blocker:

- Sirius is not yet a valid compact-object system benchmark. The current public
  side-sliced build resolves `Sirius` to a single Gaia white-dwarf row
  (`Gaia DR3 2947050466531873024`) carrying Sirius/Alpha CMa/HIP/HD aliases,
  with Sirius A absent from core inventory and no WDS/MSC hierarchy or A-B orbit
  edge in ARM. The ingest guard now prevents non-compact AT-HYG positional rows
  from attaching aliases/identifiers to compact-object targets on future builds,
  but served artifacts need a rebuild and Sirius A needs an accepted inventory
  source/supplement path. The simulator must continue to render the payload
  honestly; the fix belongs in bright-star/common-name authority and
  compact-companion source reconciliation, not in renderer-only fabrication.
  `scripts/verify_compact_alias_safety.py` detects this class of bad artifact
  during build verification; it should become a strict gate after the next clean
  rebuild.
