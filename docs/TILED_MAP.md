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

The 250-ly browser presentation uses `mixed_exact_interest_spatial_v1`: every
system through 110 ly, every planet host and multiple, high-interest ranked
systems, a stable ordinary-star sample, and all coarse context samples. Photon
currently emits 40,238 desktop and 33,250 constrained/mobile points while the
manager verifies all 230,181 exact identities. Search can materialize and focus
an omitted ordinary system by stable identity. The UI and canvas diagnostics
report catalog and rendered counts separately, so LOD is not presented as a
complete local population.

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
