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

## Delivery and Promotion

- hashed tile files: `/map-tiles/radius-<n>/tiles/<sha256>.sgtile.gz`
- radius manifest: `/map-tiles/radius-<n>/manifest.json`
- current index: `/map-tiles/index.json`
- hashed tiles receive immutable one-year caching
- manifests are short-lived pointers into the atomically promoted served build
- nginx accepts only the four bounded radius names and 64-character lowercase
  SHA-256 artifact names
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
100-ly endpoint. It is not a second renderer architecture and should be removed
after the measured parity window.

## Verification

Build reports cover exact membership, boundary behavior, duplicates, missing
membership, per-LOD counts, payload/compression, and hashes. Verification must
confirm exact 100/250 membership against served core, deterministic rebuild
hashes, nonblank desktop/mobile canvas pixels, stable labels/selections, and
cold/warm/flight/search performance budgets. Reports belong under
`reports/<build_id>/`; reproducible browser scripts remain in the repository.
