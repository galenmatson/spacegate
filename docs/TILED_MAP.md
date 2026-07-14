# Tiled Deep Map v1

Status: M8.1 implementation contract, 250-ly pilot (2026-07-14).

## Scope

Tiled Map v1 replaces the browser's monolithic 100-ly JSON transport with
immutable, content-addressed map artifacts. The public selector exposes 100 and
250 ly. Builds also generate 500 and 1,000-ly verification manifests, but those
radii are not public until later measured acceptance work.

This is a presentation artifact. Authoritative coordinates and object identity
remain in `core`; DISC coolness influences scheduling and sampled LODs but never
canonical membership.

## Spatial Contract

- frame: heliocentric ICRS, epoch J2016
- origin: Sol at `(0, 0, 0)`
- axes: core `x_helio_ly`, `y_helio_ly`, `z_helio_ly`
- units: light years
- root: half-open cube `[-1024, 1024)` ly on every axis
- children: lower/upper x, then y, then z; child bits are interleaved into a
  stable 3D Morton value
- boundary policy: exact lower-bound inclusion and upper-bound exclusion; the
  root's positive outer boundary is clamped into the final cell
- tile ID: `d<level>-<zero-padded-lowercase-Morton-hex>`
- exact leaves: depth 4 (128-ly cells), adaptively split to depth 5 (64-ly
  cells) when a depth-4 cell exceeds 32,768 systems

The canonical core Morton index is a related spatial index but is not the map
artifact identifier. Changing either contract requires a version change and a
new artifact family, not reinterpretation of existing files.

## Tile Families

`systems-v1` is the only implemented family. Future extended objects, survey
imagery, and Rim overlays use separate manifests and encodings while sharing
the coordinate and spatial-address contract. They must not be inserted into
system tiles.

## Manifest Contract

`map_tiles/index.json` is the promoted build's bounded radius index. Each
`radius-<n>/manifest.json` records:

- build ID and Git identity
- schema, encoding, manifest, and tile-family versions
- coordinate/root/subdivision/boundary policy
- radius and public exposure state
- coolness profile ID, version, and hash
- exact and sampled counts, bytes, and compression
- every tile's level/Morton ID, cell, bounds, origin, parent/children, exact or
  sampled status, represented/emitted counts, SHA-256, and artifact URL
- bounded interest summary: max/top-K mean coolness, planet richness,
  multiplicity, rare class count, and compact feature flags

Manifest hashes exclude retrieval/build timestamps. Tile gzip streams set
`mtime=0`, so identical inputs and code produce identical hashes.

## Binary Tile v1

Each gzip tile contains a JSON header, fixed-width 72-byte little-endian rows,
and a UTF-8 string table. Map rows include only stable system identity,
cell-relative float32 position, distance, four existing display-name styles,
stable object key,
coolness score/rank, dominant spectral class, star/planet counts, compact
flags, and maximum stellar temperature. `system_id` is uint64 on disk and is
decoded as a decimal string when it exceeds JavaScript's exact integer range.

Selection and handoff use `system_id` and `stable_object_key`, never row or
point-buffer position. Authoritative positions are not rewritten from tiles.

## LOD and Interest

Exact leaves account for every eligible public system exactly once. Coarse
sample tiles are clearly marked and report both represented and emitted counts.
Their bounded sample is half high-interest and half deterministic spatially
uniform context, with stable identity tie-breaking. A sample is never a claim
of complete local population.

Interest metadata is derived independently from the active, hashed DISC
coolness profile. Future coolness tuning regenerates interest/sample artifacts
without changing octree membership or tile identity rules.

The 250-ly browser presentation uses `camera_blended_interest_spatial_v2`.
There is no Sol-centered hard density boundary. Planet hosts, multiples, and
high-interest systems remain persistent; ordinary background systems use a
stable identity sample. An exact camera-centered detail bubble blends smoothly
from full nearby density to that background sample and recenters only after a
bounded hysteresis distance. Detail refreshes replay only intersecting exact
tiles and replace the prior bubble atomically.

The map menu exposes three deterministic density policies:

- `Balanced`: 1/7 ordinary background, full detail through 45 ly from the
  camera, smooth transition through 105 ly, and 18-ly recenter hysteresis
- `Performance`: 1/11 ordinary background, 32/82-ly transition, and 16-ly
  hysteresis; this is the default on constrained/touch clients
- `Exact`: all 230,181 catalog systems at 250 ly with bounded labels

Search can materialize and pin an omitted ordinary system by stable identity.
The UI and canvas diagnostics report catalog, rendered, and camera-detail
counts separately, so LOD is not presented as a complete local population.

Tile schema v2 uses its compact class byte for a representative system class.
The deterministic presentation policy compares an object/spectral/evolutionary
mass proxy, then intrinsic brightness, then `star_id`. This makes Sirius use its
A star, allows a white dwarf to represent a typical WD+M-dwarf system, and
gives giants an evolutionary floor rather than treating them as dwarfs of the
same spectral class. It is explicitly a presentation heuristic, not measured
mass. Users can disable the label badges without changing map data or labels.

Photon build `20260714T191900Z_d873067_side_rebuild` verifies schema v2 with
zero missing, extra, or public-name-mismatched systems at all four artifact
radii. The 250-ly artifact contains 230,181 exact systems, including 7,662 whose
representative class is white dwarf. Verification additionally requires Sirius
to resolve to class A and the real WD+M-dwarf system LAWD 25 to resolve to WD.
The final Playwright run passes 8 desktop and mobile checks with 4 intentional
device-specific skips, including a nonblank 3840x2160 Bright-mode canvas
capture. Machine reports live under
`/data/spacegate/state/reports/20260714T191900Z_d873067_side_rebuild/` and
`/data/spacegate/state/reports/map_playwright/20260714T_m812_dominance_final/`.

## Delivery and Promotion

- hashed tile files: `/map-tiles/radius-<n>/tiles/<sha256>.sgtile.gz`
- radius manifest: `/map-tiles/radius-<n>/manifest.json`
- current index: `/map-tiles/index.json`
- hashed tiles receive immutable one-year caching
- manifests are short-lived pointers into the atomically promoted served build
- nginx accepts only the four bounded radius names and 64-character lowercase
  SHA-256 artifact names
- the web container mounts only the promoted `map_tiles` subtree read-only; it
  has no static-server mount of core, ARM, DISC, reports, raw data, or state
- DuckDB is not queried during camera flight
- missing/corrupt files are visible failures; the browser does not silently
  substitute incomplete monolithic science

Map tiles live inside immutable `out/<build_id>/map_tiles`. Promotion occurs by
the existing atomic `served/current` symlink swap. Never delete a served or
retained build's tiles independently.

## Browser Runtime

The renderer-independent `MapTileManager` owns manifest resolution, request
concurrency, aborts, deterministic priority, decoded-tile cache, retries/error
reporting, and LRU eviction. Three.js consumes map-compatible point batches and
does not construct Morton paths.

Priority order is selected/searched position, coarse context and nearby visible
geometry, direction of travel, distance, bounded interest bonus, then bounded
queue aging. Interest cannot displace an explicitly requested or nearby cell;
aging prevents indefinite starvation. Exact rows replace sampled copies by
stable identity, preserving selection, pinned labels, Peek, Explorer, routes,
and system-page handoff across refinement.

`?map_transport=monolithic` is a temporary diagnostic comparison for the
100-ly endpoint and is formally deprecated. It is not a second renderer
architecture and should be removed after the measured observation window.

## Verification

Build reports cover exact membership, boundary behavior, duplicates, missing
membership, per-LOD counts, payload/compression, and hashes. Verification must
confirm exact 100/250 membership against served core, deterministic rebuild
hashes, nonblank desktop/mobile canvas pixels, stable labels/selections, and
cold/warm/flight/search performance budgets. Reports belong under
`reports/<build_id>/`; reproducible browser scripts remain in the repository.

## M8.1 Acceptance Record

Photon build `20260714T145242Z_4c43799_side_rebuild` passed:

- exact unique membership: 10,239 / 230,181 / 2,332,003 / 5,869,087 systems at
  100 / 250 / 500 / 1,000 ly
- independent deterministic 100/250 rebuild manifest-hash equality
- tiled 100 exact payload: 0.63 MB compressed; tiled 250 exact payload: 13.09 MB
- cold 100-ly usable: 0.78-1.12 s, versus 2.93-3.32 s monolithic baseline
- cold 250-ly usable: 0.82-1.50 s; exact settle: 2.35-3.12 s
- cold 250-ly heap: 60-86 MB; selection: 208-256 ms
- cold/rapid headless 250-ly median frame time: 16.7-33.4 ms; p95: 50-66.7 ms
- 141/141 performance budget checks, static path security checks, and
  desktop/mobile canvas pixel/screenshots pass
- 100-ly tile labels match the authoritative public API naming policy for all
  10,239 systems; literal and encoded tile-path traversal attempts return 404
- bounded interest trace improves distant high-interest median request rank
  from 48 to 43 without changing nearby-tile p95 rank (34)

Machine reports live under
`/data/spacegate/state/reports/20260714T145242Z_4c43799_side_rebuild/` and
`/data/spacegate/state/reports/map_benchmarks/20260714T_m81_after/`.

## M8.1.1 Seamless Density Refinement

The July 14 Photon refinement removes the visible 110-ly sphere created by the
v1 hard inclusion rule without changing exact tile membership or manifests.

- balanced desktop: 40,232 rendered systems, including 4,067 camera-detail
  additions; 110-ly shell-density ratio 1.40
- constrained/mobile: 26,136 rendered systems, including 1,764 camera-detail
  additions; shell-density ratio 1.05
- cold 250-ly usable: 1.18-1.46 s; settle: 2.43-3.07 s
- cold heap: 81-139 MB; median frame time 16.7-33.3 ms; p95 33.3-66.7 ms
- 72 unique tile requests with zero tile replays in cold, warm, and rapid
  direction traces
- Exact-mode Playwright stress check renders all 230,181 systems while keeping
  labels below the bounded policy
- 186/186 performance and seam checks pass

Reports live under
`/data/spacegate/state/reports/map_benchmarks/20260714T_m811_no_replay_final/`
and `map_performance_m811_acceptance.json` in the promoted build report folder.
