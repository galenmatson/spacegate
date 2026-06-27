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

Frame note:

- current map coordinates are heliocentric, ICRS/J2016-derived positions from
  core fields
- scene vertical maps canonical `z_helio_ly` to Three.js Y for a stable first
  navigation frame
- true galactic-frame rendering is a future transform and must be explicit

## Data Contract

The map uses a dedicated public endpoint:

- `GET /api/v1/map/systems`

Pilot constraints:

- maximum radius: 100 ly
- maximum limit: 50,000 rows
- default request: 100 ly / 20,000 rows
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

Rules:

- map selection must use stable object identity, not point-array index
- map data must not mix science, generated presentation, and rim rows into one
  truth layer
- future rim and extended-object rendering must be separate map layers

## Rendering Layers

Initial layers:

- science point cloud
- Sol marker
- distance rings
- sparse priority labels
- reticle and selection marker
- HUD panels for selection, controls, status, and priority contacts

Planned layers:

- tiled science point clouds for 250 ly and 1000 ly
- extended objects such as nebulae, clusters, and galaxy landmarks
- system simulation meshes for stars and planets
- sky/background layer beyond the local 1000 ly sphere
- rim meshes, routes, and infrastructure overlays

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
