# Spacegate 3D Map

This document tracks the first 3D map runtime and the architectural path from
the 100 ly pilot to deep tiled navigation, system simulations, and rim overlays.

## Pilot Contract

The first map milestone is a Sol-centered, browser-native 3D view over systems
within 100 ly.

Runtime choices:

- React 19 public web app
- Three.js through React Three Fiber
- WebGL-first, with future WebGPU experiments isolated to rendering hot paths
- lazy-loaded `/map` route so search/detail visitors do not pay the 3D bundle
  cost

Controls:

- WASD: forward/back/left/right
- mouse look through pointer lock
- `Q`: translate up
- `Z`: translate down
- Shift: boost
- stabilized vertical on by default
- desktop free-cursor mode:
  - left-click selects the system under the cursor ray
  - left-drag free-looks without pointer lock
- right-click opens an ephemeral route/distance context menu
- selection history is shown as compact pills with distance/class metadata
- long catalog identifiers are truncated in the HUD and expandable via
  hover/focus/tap popovers with copy actions
- captured-pointer mode:
  - reticle selects the nearest center-view target
  - `Esc` releases pointer lock
- touch-first mobile controls:
  - one-finger drag looks around
  - tap or `Select reticle` selects the nearest reticle target
  - two-finger pinch flies forward/back
  - two-finger drag pans laterally/vertically

Frame note:

- current map coordinates are heliocentric, ICRS/J2016-derived positions from
  core fields
- scene axes are a presentation transform over canonical heliocentric ICRS:
  - scene X = canonical `x_helio_ly`
  - scene Y = canonical `z_helio_ly`
  - scene Z = negative canonical `y_helio_ly`
- scene vertical is intentionally a stable navigation convention, not a
  galactic-north claim
- future galactic-frame rendering/markers must apply an explicit ICRS-to-
  galactic transform and should not reuse the scene-up convention implicitly

## Data Contract

The map uses a dedicated public endpoint:

- `GET /api/v1/map/systems`

Pilot constraints:

- maximum radius: 100 ly
- maximum limit: 50,000 rows
- default request: 100 ly / 20,000 rows / `compact=true`
- not a replacement for `/api/v1/systems/search`

Returned rows are compact render/selection records:

- `system_id`
- `stable_object_key`
- `system_name`
- `dist_ly`
- `x_helio_ly`, `y_helio_ly`, `z_helio_ly`
- star/planet counts
- temperature/spectral summary fields
- coolness rank/score and nice/weird planet counts when `disc.duckdb` is
  present
- snapshot availability when `disc.snapshot_manifest` is present

`compact=true` is the browser-render profile. It rounds render coordinates to
six decimal places and omits fields not currently used by the map renderer
(`stable_object_key`, detailed temperature fields, `spectral_classes`, and
nice/weird planet counts). Full records remain available by omitting
`compact=true` for diagnostics.

Rules:

- map selection must use stable object identity, not point-array index
- map data must not mix science, generated presentation, and rim rows into one
  truth layer
- future rim and extended-object rendering must be separate map layers

## v0.2 Measurements

Photon local API measurements for the 100 ly / 20,000 row request:

- full diagnostic payload: about 5.3 MB JSON
- `compact=true` render payload: about 3.0 MB JSON
- local API response time remained under 1 second for both forms during v0.2
  checks

Browser QA covers:

- desktop WebGL render, compact endpoint request, selected-system link, and
  nonblank canvas
- mobile WebGL render, non-overlapping HUD sheets, hidden pointer-lock controls
  on coarse pointers, touch drag/pinch event handling, `Select reticle`, and
  map-detail handoff

## Rendering Layers

Initial layers:

- science point cloud
- Sol marker
- distance rings
- sparse priority labels
- reticle and selection marker
- ephemeral client-side route measurement overlay:
  - right-click target system and choose `Measure from selected`
  - rendered route segments show per-leg light-year distance and total route
  - not persisted and not a Rim/worldbuilding route record
- HUD panels for selection, controls, status, and priority contacts
- compact beta HUD:
  - dense header instrument strip with system/planet-host/multiple counts
  - selection history pills instead of wide priority-contact cards
  - selected-system title link to public detail
  - long-ID metadata popovers and copy controls
  - selected-system snapshot pill that lazy-loads the deterministic snapshot
    preview on hover/focus when the map payload says a snapshot is ready
  - route summary with recent leg list, total distance, undo, and clear
- System Simulation drill-in layer:
  - clicking/selecting a star opens a framed `Peek` overlay without moving the
    map camera
  - Peek owns its own canvas gestures, so mouse/touch pan, orbit, and zoom
    inspect the system simulation rather than moving the star map
  - `Explore` promotes the same selected system to the focus path, flies the map
    camera toward that system, and expands the System Simulation into the main
    inspection layer
  - `Esc`, browser Back, or `Back to Map` returns from Explorer to flight; the
    current implementation keeps the star map and system simulation as
    coordinated layers rather than one continuous LY-to-AU canvas
  - suggested nearby systems are computed client-side from the 100 ly payload
    using coolness, distance, planet count, multiplicity, and human-readable
    naming signals
  - selected-system vitals are merged into transparent pills over the
    simulation canvas, replacing the former selected-system card entirely while
    preserving distance, class, star/planet counts, coolness, rank, and
    snapshot status
  - selection history and `Cool Stars Nearby` suggestions share the same
    left-side tray as collapsible sections, expanded by default, with both
    lists capped at eight compact pills
  - the map drill-in presentation keeps Pause/Start and Reset as transparent
    simulator overlays, restores the Structured/Orbit/Body/Log scale selector,
    keeps the speed selector including 1000x for long-period orbit inspection,
    uses a shorter/thinner transparent Peek panel, and lets the canvas fill the
    window
  - Explorer mode uses a less-transparent simulator shell/canvas than Peek, and
    separates compact one-line readout pills from a Diagnostics disclosure that
    contains Evidence and Render Policy, so diagnostic panels cannot stretch the
    pills
  - theme-specific map overlay styling keeps the embedded simulator controls in
    a floating layer above the WebGL canvas; LCARS/Enterprise uses black
    nontransparent cards with bright yellow borders and no glow, while Simple
    Light and Geocities use more opaque map overlay surfaces for readability
- public system-simulation scene-readiness API:
  - `GET /api/v1/systems/{system_id}/simulation-scene`
  - exposes current hierarchy, arm graph/orbit rows, and readiness fields for
    future live 3D system previews
- System Simulation v1 renderer:
  - lazy-loaded on system detail pages
  - lazy-loaded from the 3D map only when Peek/Explore opens, so initial map
    flight does not pay the simulator chunk cost
  - renders source/derived/assumed scene bodies from `simulation-scene`
  - consumes additive `render_scene_v0.2` bodies/orbits when available
  - uses barycentric visual groups for connected binary/multiple-star edges
  - places star clusters from the hierarchy tree when available, so sparse
    orbit evidence does not collapse complex systems into one flat ring
  - places planet orbits around their render host/body group when the scene
    exposes nested planet hosts such as Proxima inside Alpha Centauri
  - animates planets from source `orbital_period_days` when present
  - samples orbit-guide paths from the same eccentric/inclined position math
    used by animated planets, so guide lines match traced motion
  - supports start/pause controls for the local preview clock
  - exposes hover vitals for rendered stars and planets inside the preview
  - assigns deterministic per-body starting phases so planets do not begin
    aligned while keeping reloads reproducible
  - surfaces source/derived/assumed/missing provenance pills in the preview
  - deterministic snapshots remain the fallback/reference artifact
  - summarizes orbit-orientation evidence as source orientation, partial
    sky-plane orientation, assumed roll, or local-clarity layout; this is an
    audit label, not a galactic-alignment guarantee

Planned layers:

- tiled science point clouds for 250 ly and 1000 ly
- extended objects such as nebulae, clusters, and galaxy landmarks
- system simulation meshes for stars and planets
- sky/background layer beyond the local 1000 ly sphere
- rim meshes, routes, and infrastructure overlays

Route-measurement boundary:

- map route lines are temporary browser state
- they do not write `rim` rows
- they may later become the seed for rim routes after an explicit save/export
  workflow exists

Checked-in browser QA:

- `srv/web/tests/map/map.spec.js`
- `npm run test:map`
- covers route create/undo/clear, selected snapshot hover preview, mobile HUD
  compaction, and the System Simulation Peek/Explore smoke path

## Deferred Decisions

Tiling and octrees:

- not required for the 100 ly pilot
- required before deep 250 ly / 1000 ly navigation
- should use the existing Morton/spatial-index direction where practical

Extended objects:

- Messier labels are useful public landmarks but should not become the sole
  scientific authority
- evaluate modern CDS/VizieR/NASA/ESA sources for canonical extended-object
  ingest
- nebula rendering should begin with impostor/volume-style shader experiments;
  true raymarched volumetric fog is a later performance milestone

Skybox/background:

- use only license-compatible imagery/assets
- Celestia may be useful as a reference but GPL/assets require explicit license
  review before reuse

Time:

- proper motion, orbital motion, and rim simulations are out of scope for the
  pilot
- future time flow should run primarily client-side and keep canonical stored
  coordinates fixed to the build epoch
