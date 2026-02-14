# Spacegate: Local Stellar + Exoplanet Worldbuilding Database
I've seen 3D star maps on the internet but they are all awful. The ones in space games are way better, but not technically more sophisticated, just better designed. I want to make one that is fun to browse, fun to click on stars and objects and read about them, and fun to play with. I want to do more than just list sterile data from the database. 

I've worked for a long time on a fictional scifi universe, drawing elaborate star maps of trade lanes. While the entire universe is fully hard scifi and sublight until 2500 or so, for space opera and grand adventure I had to make a concession for faster than light travel. 

The system uses space contraction between two 'spacegates' at different points in space. The spacegates, separated by several light years, must achieve an unfathomably perfect alignment and synchronization and both must annihilate a prohibitively expensive amount of antimatter. 

The antimatter requirement increases quadratically with distance. Jumps between far systems are extremely expensive and infrequent and quite few whereas close or clustered systems are better connected. Jump distances are generally limited to around 4 or 5 light years. There should be no absolute limit but economics would exclude jumps over 11 light years or so. 

This does leave a lot of systems outside the FTL network (but there's a substantial amount of STL (< .2 c) traffic). While the breakthroughs behind the spacegates are known by the mid 2100s the energy requirement is so extravagant that economically beneficial interstellar FTL isn't available until energy production rises to meet it. And that is basically done by damming starlight with massive solar arrays that generate antimatter. 

The surface area requirements result in dyson swarms of solar collectors around the brightest stars, with the antimatter production for the same collector area rising with the Stephan Boltzman law, making bright A, B, and O type stars the most desirable for energy production. They fuel the spacegates and antimatter rocket based sublight interstellar travel and form the energy core of empires in the future. 

My desire is to have an interactive 3D map rendered in a browser which draws an accurate star map using the latest parallax measurements and build a space empire mapping tool on top of it. Create an agent to generate engaging but factual English blurbs for each system. Starting with the most important (Sirus, Alpha Centauri, etc) and unique (bright stars, exoplanets in habitable zones, inferno planets, trinary stars, dust rings, etc.) systems. It should be interesting and informative in a way that people will just read about space stuff for fun and keep exploring.

The world building features of the map should allow for things like trade lanes, spacegate links, or other connections to be drawn between stars. Spheres of control around owned/occupied systems that form the 3D shape of interstellar empires. Place megastructures like solar collectors, foundaries, shipyards, Dyson swarms, colonies, momentum banks, space elevators, mines, mass drivers, space stations, etc. on planets, in orbit of them, in stellar orbit, or galactic orbit (unbound to stars).

These objects should be definable so I can create immense solar collectors that output 1 gram of antimatter per year or something like that. Or a mine that produces x kilotons of 18% aluminum 12% iron ore per day. And an ore processor that separataes and concentrates that to 78% grade aluminum ore and 68% iron ore. And an aluminum smelter that outputs x amount of pure aluminum. And a foundry that outputs x tons of steel per day. And a space elevator that can bring x tons up to orbit per day. And a ship yard that consumes x tons of aluminum per day as it produces a ship that contains x tons of aluminum and steel. For example.


# Purpose
- Build a rich stellar database with a user friendly interface for learning, exploration, and imagination.
- Collect, organize, and enrich public astronomical data distributed across many systems, databases, and organizations in a single high utility, high accessibility tool for the public.
- Avail these collated and consolidated datasets and all of this project's code to the public.
- Use AI to generate vivid descriptions and imagery of systems and exoplanets that is scientifically accurate and compelling to the public.
- Tools for worldbuilding: trade lanes, empires, cultures, megastructures, spacegates, space stations, space elevators, Dyson spheres, and other metadata for authors and anyone else that wants to fantasize about exploring the visible stars in the night sky.
- Moveable, zoomable, rotatable, recenterable, interactive 3D map of the our region of Orion's Arm within 1000 LY which visualizes both real astronomical objects and fictional overlays; links, bubbles, borders, etc. for scifi world building.

## Primary deliverables over time:
- A versioned core astronomical dataset (stars/systems/planets) with provenance.
- Optional object packs (substellar/compact/superstellar) that can be toggled in search/render/download.
- An enriched content set built from that data and accurate scientific knowledge.
- A public browser UI with excellent UX and factual, engaging descriptions and depictions of objects based on that enriched data.
- A free browser based, navigable, 3D map of nearby space.
- A lore layer for building fiction about what could be atop what is known.


# Filesystem Layout & Environment Variables

Spacegate does not assume a fixed filesystem layout. All persistent state locations are defined via environment variables to support:

- reproducible deployments
- read-only code checkouts
- Docker and containerized execution
- separation of code and data
- large external data volumes

### Repo layout (code checkout)
Example (recommended for servers):
/
└── srv
    └── spacegate
        ├── srv
        │   ├── api
        │   └── web
        ├── scripts
        ├── docs
        ├── configs
        └── data          # default local-dev state dir (optional)

### State layout (runtime data, FHS-aligned)
Example (valid for small/typical installs):
/
├── data                  # This should be a large volume if you will use the expanded dataset (>100 GB)
│   └── spacegate
│       └── data          # SPACEGATE_STATE_DIR
│           ├── raw
│           ├── cooked
│           ├── out
│           ├── served
│           └── reports
├── var
│   ├── cache
│   │   └── spacegate     # SPACEGATE_CACHE_DIR
│   └── log
│       └── spacegate     # SPACEGATE_LOG_DIR
└── etc
    └── spacegate         # SPACEGATE_CONFIG_DIR

### Large-data recommendation (preferred)
For large datasets (Gaia, full catalogs, etc.), **do not** use `/var` if it is on the root disk.
Instead, place `SPACEGATE_STATE_DIR` on a large, fast volume (e.g., `/data/spacegate`, `/mnt/spacegate`, or any custom path).

Example:
```
export SPACEGATE_STATE_DIR=/data/spacegate
export SPACEGATE_CACHE_DIR=/data/spacegate/cache
export SPACEGATE_LOG_DIR=/data/spacegate/logs
```

Note: For local development, the default state directory is `./data` inside the repo.

## Required Environment Variables

| Variable               | Description              | Default         |
|------------------------|--------------------------|-----------------|
| `SPACEGATE_STATE_DIR`  | Astro catalogs, databases| `./data`        |
| `SPACEGATE_CACHE_DIR`  | Download and build cache | `./data/cache`  |
| `SPACEGATE_LOG_DIR`    | Application logs         | `./data/logs`   |
| `SPACEGATE_CONFIG_DIR` | Runtime configuration    | `./configs`     |

For production deployments, set these to standard Linux locations
(e.g., `/var/lib/spacegate`, `/var/cache/spacegate`, `/var/log/spacegate`, `/etc/spacegate`).

## State Directory Structure

Within `SPACEGATE_STATE_DIR`, Spacegate maintains the following structure:

| Directory | Description                           | Location        |
|-----------|---------------------------------------|-----------------|
| raw       | Source datasets (immutable)           | `./data/raw`    |
| cooked    | Cleaned and normalized datasets       | `./data/cooked` |
| out       | Build outputs (DuckDB, Parquet, aso)  | `./data/out`    |
| served    | "current" build (symlink or directory)| `./data/served` |
| reports   | QC, provenance, and validation reports| `./data/reports`|

Build outputs SHOULD be treated as immutable. Promotion between builds SHOULD be done via atomic directory swaps or symlink updates.


# Data Sources
## Primary sources (v0 only)
- AT-HYG stellar catalog CSV (stars <= 1000 ly)
- NASA exoplanets CSV (pscomppars; host matching limited by core star coverage)

## Optional packs (v2.1+)
Deferred until after the v1 UI. See **v2.1 Additional catalogs** below.

## Source protection / reproducibility
- $SPACEGATE_STATE_DIR/raw/** source files should be preserved as read-only once downloaded.
- Raw data is only updated with newer raw data through the catalog download process with an update to the manifest.
- The file operations to retrieve, decompress, and combine the source data should be logged.
- All builds must be reproducible from pinned versions + checksums/etags where possible.


# Data model and artifacts

## Terminology
- **Core astronomy**: real objects intended for general browsing in the ≤1000 ly sphere (systems, stars, planets) + strict provenance. These are downloaded and normalized by scripts and background functions but are not changeable by the user app.
- **Expanded astronomy**: additional *real* object categories (substellar/compact/superstellar/etc.) that can be toggled in search/render/download. Like the core astronomy, read-only except by the catalog management functions.
- **Rich astronomy**: derived, regenerable content generated from core/packs (e.g., deterministic snapshot manifests, fact sheets, generated blurbs, generated imagery). Not edited in-place; regenerate instead.
- **Engagement**: user feedback, click counts, likes. Informs the enrichment AI on systems of most interest. Linking to external forum threads on specific topics.
- **Lore**: user-authored fictional metadata and fictional entities. Editable. Stored separately so core data stays shareable.

## Output artifacts (versioned per build)
All artifacts are produced under a versioned build directory: `$SPACEGATE_STATE_DIR/out/<build_id>/...` (see “Build layout and versioning”).

### 1) Core astronomy dataset (v0+)
**Purpose:** the clean, shareable, authoritative “real astronomy” foundation.

- DuckDB (authoritative query format for the app/API):
  - `$SPACEGATE_STATE_DIR/out/<build_id>/core.duckdb`
- Parquet export (for publishing/sharing, tooling interoperability):
  - `$SPACEGATE_STATE_DIR/out/<build_id>/parquet/{systems,stars,planets}.parquet`

Core tables (minimum):
- `systems`
- `stars` (including components where available)
- `planets` (NASA-derived)
- plus required provenance fields on all rows.

Rule: **No blurbs, images, or lore** stored in core.
Rule: **Spatial sorting** All core Parquet files must be sorted by `spatial_index` (Morton Z-order). This enables high-performance "range scans" for 3D queries without reading the entire file.

### 2) Expanded astronomy dataset (v2.1+)
**Purpose:** keep additional object categories optional and independently maintainable.

- Pack artifacts are separate from core:
  - DuckDB (optional): `$SPACEGATE_STATE_DIR/out/<build_id>/packs/<pack_name>.duckdb`
  - Parquet (recommended): `$SPACEGATE_STATE_DIR/out/<build_id>/packs/<pack_name>/*.parquet`
  - Manifest: `$SPACEGATE_STATE_DIR/out/<build_id>/packs_manifest.json`

Planned pack names:
- `pack_substellar` (brown dwarfs, ultracool dwarfs, rogue/free-floating planets when available)
- `pack_compact` (white dwarfs, neutron stars, pulsars, magnetars; curated nearby BH list if it exists)
- `pack_superstellar` (extended objects within or near the local sphere: nebulae, SNRs, clusters)

Note: “superstellar” objects are often extended, not point sources. This pack must support angular size and rendering primitives (billboards/volumes), not just xyz points.

Rule: packs are **read-only** inputs to search/render/download. Each pack has its own provenance.

### 3) Rich dataset (v1.1+)
**Purpose:** derived artifacts that make the UI engaging while remaining strictly traceable to source facts.

- DuckDB (authoritative query format for the app/API):
  - `$SPACEGATE_STATE_DIR/out/<build_id>/rich.duckdb`
- Parquet export:
  - `$SPACEGATE_STATE_DIR/out/<build_id>/rich/*.parquet`

Rich tables (initial):
- `snapshot_manifest` (deterministic system visualization snapshots; see v1.1)
- `factsheets` (structured JSON facts per object, with provenance pointers)
- `blurbs` (engaging but factual descriptions generated strictly from factsheets; see v1.2)
- `system_neighbors` (10 nearest systems per system; see v1.2.2)

Rules:
- Rich is **not edited in-place**. If content is wrong or the generator changes, regenerate rich with a new `generator_version` / build.
- Each rich row must be traceable:
  - factsheets: include `facts_hash`, `generator_version`, and pointers to source rows/fields
  - blurbs: include `facts_hash`, `model_id`, `prompt_version`, `generated_at`
  - snapshots: include `params_hash`, `params_json`, `generator_version`, `source_build_inputs_hash`

### 4) Engagement signals (v1.3+)
**Purpose:** capture minimal, privacy-respecting signals of collective human curiosity to improve discovery and prioritization — without analytics, profiling, or monetization.

This dataset exists to give the people what they want. If they are explicit in their interest of goldilocks planets or white dwarf trinaries or hell worlds or whatever, we should have a method of capturing that interest and feeding it into the interestingness algorithm that prioritizes data enrichment.

This is not monetizable. It is explicitly not behavioral tracking. Store no personal data.

- DuckDB (optional for local builds / analysis):
  - `$SPACEGATE_STATE_DIR/out/<build_id>/engagement.duckdb`
- Parquet export (for transparency / research use):
  - `$SPACEGATE_STATE_DIR/out/<build_id>/engagement/*.parquet`

### 5) Lore overlays dataset (v2)
**Purpose:** editable worldbuilding overlays and free-floating fictional entities, stored separately so the base data stays shareable.

- DuckDB (recommended):
  - `$SPACEGATE_STATE_DIR/out/<build_id>/lore.duckdb` (typically per user/namespace)
- Optional Parquet export for sharing:
  - `$SPACEGATE_STATE_DIR/out/<build_id>/lore/*.parquet`

Minimum lore tables:
- `lore_entities(entity_type, entity_key, namespace, lore_json, updated_at, source)`
- Lore entities may be:
  - anchored to a real object (system/star/planet) via `stable_object_key`
  - free-floating (absolute heliocentric coords in ly; or relative offsets from an anchor)

Rules:
- Lore is editable.
- Lore never mutates core/packs/rich tables.

## Snapshot assets vs snapshot manifest (v1.1)
Deterministic snapshot images are stored as **asset files**, referenced by a manifest row in the rich dataset.

- Snapshot assets (files; filesystem or object storage):
  - `$SPACEGATE_STATE_DIR/out/<build_id>/snapshots/<view_type>/<stable_object_key>/<params_hash>.svg`
- Snapshot manifest (table; in `rich.duckdb` and exported to Parquet):
  - `snapshot_manifest(stable_object_key, object_type, view_type, params_json, params_hash, generator_version, build_id, artifact_path, created_at, source_build_inputs_hash, ...)`

Rule: snapshots are derived artifacts; do not store image bytes in DuckDB.


# IDs, matching, and provenance
## IDs
- Core tables use internal surrogate *_id (BIGINT) for performance and FK sanity.
- Also store stable external keys where available:
  - gaia_id, hip_id, hd_id, etc.
- Also compute a stable_object_key for rebuild stability and lore/packs joining:
  - Prefer authoritative catalog IDs when present.
  - Fallback: coordinate hash with versioned precision buckets + normalized name.
## Match provenance (planet → host)
- match_method (gaia | hip | hd | hostname | fuzzy | manual)
- match_confidence (0..1)
- match_notes (optional)
## Row lineage provenance (required for all derived rows)
Every derived row must include:
- source_catalog
- source_version
- source_url
- source_pk (primary key from source if available, e.g., Gaia source_id)
- source_row_id (or source_row_hash)
- license
- redistribution_ok (bool)
- license_note
- retrieval_checksum (when possible) and/or retrieval_etag
- retrieved_at
- ingested_at
- transform_version (git SHA or pipeline version)

# Units and coordinates
- Distances displayed in light-years (ly) by default (selectable later).
- Store 3D Cartesian coordinates in light-years.
Coordinate storage:
- Store heliocentric coordinates as the primary working frame for the local sphere:
  - x_helio_ly, y_helio_ly, z_helio_ly
- Optionally store galactocentric coordinates for galaxy-scale views:
  - x_gal_ly, y_gal_ly, z_gal_ly
Rendering rule:
- The renderer always rebases around the selected object (“floating origin”) before sending to GPU (float32 safety).


# QC gates
These checks run every build. Some fail hard; some warn.
Hard failures:
- any rows missing required provenance
- sanity check: abs(norm(x,y,z) - dist_ly) < eps for rows where both are present
Warnings (thresholds to refine):
- match rate drops by > 0.5% absolute from previous build
- unmatched planets increases by > 25 from previous build
- large shifts in distance distribution or magnitude distribution beyond set thresholds
- spikes in duplicate stable keys



# Milestones

## v0: Core ingestion
Success criteria:
- Create functions to download <1000 LY objects from core, authoritative public databases
  - Should be rerunnable for updates with recover and retry on failure.
- Build a DuckDB database from core astronomical data.
  - store in data/
- **Implement Spatial Indexing:**
  - Compute 63-bit Morton (Z-order) spatial_index for all objects from heliocentric xyz (ly), stored as signed BIGINT.
  - Use 21 bits per axis with a domain-parameterized cube (v0 default: ±1000 ly).
  - scale = (2^21 - 1) / (2 * MORTON_MAX_ABS_LY); quantize with round(); clamp defensively to [0, 2^21 - 1].
  - Ingestion fails hard if any star coordinate exceeds the domain.
  - Ensure Parquet exports are physically sorted by spatial_index.
- Normalize identifiers and types. 
  - For instance, Gaia DR3 keys are stored as strings sometimes with 'Gaia DR3 ' prefix. 
  - They should be stripped down to the ID and stored as high precision integers.
- Join exoplanets to host stars/systems with match provenance + confidence.
- Parse spectral types into components while retaining raw strings.
- Export Parquet artifacts for core tables.
- Produce reports:
  - match report (counts by method, unmatched rows, suspicious cases)
  - provenance coverage report
  - basic QC sanity checks

### v0.1: Public database browser UI
Success criteria:
- “Spacegate browser” UI
- Hosted on Google Cloud
- Public at spacegates.org
- Attractive, searchable, filterable interface
- Reads core data + optional packs + lore overlays (lore editable; core read-only)

### v0.2: 'Interestingness' Initial Enrichment
This ensures our compute resources for v1.2 blurb and image generation are spent on systems with high narrative and scientific "yield".The following features should be aggregated into a final Interestingness Score stored in the rich database:
- Extreme Luminosity (Economic Value): High-mass stars (O, B, A types) are heavily weighted due to their necessity for antimatter production via Dyson swarms.
- High Proper Motion (Kinetic Interest): Objects with significant angular movement across the sky are prioritized as "runaway" stars or nearby high-velocity neighbors.
- Stellar Multiplicity (Architectural Complexity): Points scale with the number of stars in the system; hierarchical trinaries or quaternaries rank significantly higher than simple binaries.
- Nice Exoplanets: Biological potential or high colonization targets. Known Earth like planets: habitable zone or close, not too big, not too small, stable star. Eye planets (tidally locked but in the habitable zone).
- Weird Exoplanets: Strange atmospheric composition, water worlds, diamond worlds, extreme size, acid worlds, lava worlds, "hell worlds" (ultra-short periods) or planets being devoured by their stars. High eccentricity planets that spend time in the habitable zone (like Trisolaris) and freeze/thaw. 
- Metallicity (Fe/H) (Industrial Capability): High-metal stars are prioritized as likely hubs for mining, foundries, and heavy industry.
- Compact Remnants: White dwarfs, neutron stars, pulsars, or magnetars adds a rarity multiplier due to their unique physics and "graveyard system" narrative.
- Anomalous Features: Specific data flags for high eccentricity, extreme stellar flares, or circumstellar dust rings.
- Proximity to Sol: The most colonizable with sublight technology. This bonus should decay quickly (inverse square of interestingness).
- Science Fiction: Wolf 359 is where the Federation made its final stand against the Borg in Star Trek: The Next Generation. The exotic moon "Pandora" from the movie Avatar orbits a gas giant in the Alpha Centauri system. Vega is famous for its role in Carl Sagan's Contact.

**Ranking by Narrative Density:** By combining these, a system like Sirius (high luminosity + White Dwarf companion) or Alpha Centauri (trinary + proximity) naturally rises to the top, while a lonely Red Dwarf at 800 light-years remains at the bottom of the stack. With these rankings stored in the rich database the later enrichment (narrative, depiction) steps will prioritize interest over row order as we enhance the dataset.

---

## v1 System Visualization
Goal: produce **deterministic, cacheable “system snapshot” images** that make the browser fun immediately, without requiring the full v2 3D map.

### Success criteria
- A reproducible snapshot generator that emits small, fast assets per system:
  - **Local neighborhood (10 ly)** view: nearby stars relative to the selected system.
  - **Regional neighborhood (50 ly)** view: nearby “interesting” stars (configurable filter; default keeps Sol visible).
  - **Inner system orbit view (<5 AU)** for systems with known planets (if any).
  - **Outer system orbit view (>30 AU)** when wide-orbit planets exist (if any).
- Snapshots are deterministic and **stable across rebuilds** given the same inputs:
  - same object IDs, same generator version, same parameters ⇒ byte-identical SVG (or visually identical PNG if raster).
- Snapshots are available to the v1 UI:
  - list + detail pages can show the relevant snapshot(s) with zero extra computation at request time (after warm cache).

### Deterministic rendering rules (no “artistic drift”)
- Coordinate inputs:
  - Use **heliocentric XYZ** as the primary working frame for local ≤1000 ly views (double precision in generation).
  - Always render snapshots in a **recentered frame** (origin = selected system) for numeric stability.
- Camera conventions (fixed):
  - Neighborhood views: **“top-down” from Galactic North** (standard orientation, consistent axes, labeled).
  - Provide a legend/scale bar and a small “Sol” marker when Sol is in-frame (or optionally always, via inset).
- Visual encoding (fixed mapping):
  - Star marker size = function(apparent magnitude or absolute magnitude; choose one and document it).
  - Color = function(spectral class (OBAFGKM…) when available; otherwise neutral).
  - Optional toggles become **parameters** (and therefore part of the cache key), not ad-hoc.
- Output formats:
  - Default: **SVG** for crisp zoom + tiny size.
  - Optional: PNG thumbnails derived from SVG for fast grids.

### Storage model (future-proof; keeps core astro immutable)
Snapshots are **derived artifacts** (like reports), not part of the immutable core astronomy tables.

- Store binary image blobs in object storage / filesystem (preferred):
  - `$SPACEGATE_STATE_DIR/out/<build_id>/snapshots/<view_type>/<stable_object_key>/<params_hash>.svg`
- Store a manifest table (Parquet and/or DuckDB table) that the UI can query:
  - `snapshot_manifest` fields:
    - `stable_object_key`
    - `object_type` (system/star/planet)
    - `view_type` (neighbors_10ly, neighbors_50ly, orbits_inner, orbits_outer, …)
    - `params_json` (the exact parameter set used)
    - `params_hash` (cache key)
    - `generator_version` (git SHA or semantic version)
    - `build_id`
    - `artifact_path` (relative path or signed URL target)
    - `created_at`
    - `source_build_inputs` (hash of the relevant input rows / tables)
- Rationale:
  - Core astronomy remains shareable and clean.
  - Snapshots can be regenerated, swapped, or replaced without touching core data.

### Generation strategy (don’t precompute everything blindly)
- Implement **lazy generation + caching**:
  - On first request (or during an offline “warm cache” job), generate and write the snapshot + manifest row.
- Define a “warm set” for initial usability:
  - Systems with known exoplanets.
  - Bright / nearby / named systems (configurable).
  - Then expand coverage opportunistically.

### Guardrails
- Snapshots must be generated strictly from:
  - core data + enabled optional packs (if used) + documented parameters.
- No “invented” planets, orbits, colors, or relationships.
  - If required fields are missing, render a minimal view with a clear “insufficient data” note.
- Any change to rendering rules must bump `generator_version` and invalidate cache by construction.


## v1.1: External reference links (curated web sources)
Goal: augment rich with **high-quality, per-object reference links** to authoritative pages (e.g., Wikipedia, SIMBAD, NASA Exoplanet Archive) for deeper reading.

Method (proposed):
- **Discovery**: for each object, generate candidate queries from stable identifiers and common names (e.g., primary name, catalog IDs).
- **Source allowlist** (default): Wikipedia, SIMBAD, NASA Exoplanet Archive, ESA/Gaia docs, IPAC/IRSA, CDS, Exoplanet.eu, relevant observatory pages.
- **Quality scoring**: rank candidates by:
  - Authority (domain allowlist > others)
  - Specificity (object page vs generic topic page)
  - Content richness (presence of sections like “Physical characteristics”, “Discovery”, “Orbit”)
  - Recency/maintenance signals (last updated if available)
  - Licensing suitability (links allowed even when content cannot be reproduced)
- **Human override**: optionally pin or blacklist specific links in a small manual overrides file.

Reasonable limits (initial defaults):
- **Max links per object**: 3 (1 authoritative catalog + 1 encyclopedia + 1 optional observatory/mission page).
- **Max candidates evaluated per object**: 10.
- **Domain cap**: 2 links from the same domain per object.
- **Coverage budget**: only “interestingness” top N objects in v1.2; full coverage later.
- **Refresh cadence**: re-check links only on build regeneration or every 6–12 months.
- **Strictly link-only**: store URLs + metadata only; no copying page text into rich.

## v1.2: AI rich description (“facts → blurb”)
Success criteria:
- Generate fact sheets (structured JSON) per selected object with sources/provenance.
- Generate engaging but factual descriptions derived from known facts.
- Store generated content as derived artifacts with:
  - model/version, generated_at, prompt, and fact-sheet hash
  - display small text notification if fact sheet hash no longer matches (indicating new information)
- Target voice: science outreach personality (factual, enthusiastic; no fabrication).
- Counter prompt is used to evaluate the factuality of the blurb and discard hallucinations


## v1.3: Image generator
Based on the descriptions from the blurbs, generate instructions for image
generator model create vivid imagery of planets, stars, and systems.
- Shareable versions with text captions at the bottom
- link an image generator that will generate system images on interestingness or first visit
- tooltip with prompt that generated the image
- generate more images in systems wtih lots of visits
- up/down voting, reorder with most popular
- popular images push system to be featured on front page and suggested in system links
- most popularist images get expanded in size and resolution to full page background images, 
  - free download as (up to) 4k desktop backgrounds
- generate short, captivating meme text like "Hell World HD 189733_b where it rains glass sideways in Earth sized cyclones."
- make recaptioning and sharing easy, link back

### Star/System view
  - interestingness score should prioritize complex systems and exotic stars
  - center close binaries and planets
  - show distant companions in background
  - aim for accuracy but exagerate slightly if necessary to make dim companions visible
  - star is accurate to spectrum, surface temp, and size
  - flare stars depicted with erupting solar flares
  - pulsars depicted with polar jets though they might be invisible without gas/dust to scatter/emit)
  - magnetars show powerful rotating magnetic fields (despite being invisible or show them interacting with something)

### v1.3.1 Planets
#### Global view
  - planet in the foreground and the star behind
  - show flare stars scorching them with glowing prominences
  - eyeball planets for tidally locked planets at right distance
  - maybe young systems or lots of dust detected show asteroid/comet strikes
  - add moons to large planets even if none detected
  - include subtext that these images are extrapolated from scientific data, not observation, and reality is likely quite different.

#### v1.3.2 Surface view
This should show what it might feel like to visit this planet and stand on its surface.
This requires the most license of all. We can ground the view in some facts that should
make it clear to visitors the link between orbit, composition, and climate.
Aim to inspire.
- Volcanic worlds shows lava spewing smoke belching volcanoes
  - powerful lightning strikes
  - dark, oppressive skies
  - vivid, steaming lava fields
  - volcanic bombs smashing into the landscape 
- Water worlds
  - got lots'a water
  - colossal waves
- Ice worlds
  - ice mountains with different compositions depending on the expected temperature
  - rocky if in close and high metallicity
  - methane/CO2/nitrogen ice depending on how cold
  - cryovolcanism
- Desert planets
- Hell worlds 
  - close star
  - molten surface
  - thick atmosphere
  - heavy molecular composition
  - glass rain (https://en.wikipedia.org/wiki/HD_189733_b)
- Acid worlds
  - thick, sulfurous atmosphere
  - melting landscape
- Ringed planets (https://en.wikipedia.org/wiki/J1407b)
- Dead worlds
  - sterilized with no hope of life 
  - surface stripped by supernova
  - scoured by massive radiation
  - orbiting dead/remnant stars
And much more! The more creative we are with descriptions while linking everything to real science the more viewers will be inspired. So lets come up with awesome fantasy planets defendably grounded in facts.




## v1.4: System neighbor graph (10 nearest systems)
Goal: precompute nearest-neighbor relationships between systems for fast UI queries and navigation.

Success criteria:
- For every core `systems` row, compute the 10 nearest *other* systems by 3D Euclidean distance (ly).
- Store results in rich as a stable, reproducible derived artifact.
- Deterministic ordering for ties (distance, then `neighbor_system_id` asc).

Storage model:
- New rich table `system_neighbors`:
  - `system_id` (core FK)
  - `neighbor_rank` (1..10)
  - `neighbor_system_id` (core FK)
  - `distance_ly` (FLOAT)
  - `method` (e.g., `knn_exact`, `knn_indexed`)
  - `generator_version`, `build_id`, `created_at`

Rules:
- Use **core systems only** (exclude packs/lore).
- Exclude self-matches; always 10 neighbors unless fewer than 11 systems exist.
- Distances are computed from canonical core coordinates (J2000 xyz in ly).
- Results must be exact (indexing acceleration is OK, but output must match exact kNN within numeric tolerance).

---

## v2: 3D map (browser)
Success criteria:
- Lightweight browser-based 3D viewer (likely three.js; evaluate alternatives later).
- Smooth controls: zoom/rotate/pan/recenter; selection; tooltips.
- Filters (distance bubble, spectral class, magnitude, etc.)
- Optional rendering toggles: planets, packs, lore layers, neighbor links, spacegate links.

## v2.1 Additional catalogs
Success criteria:
- Create optional “object packs” as separate artifacts:
  - pack_substellar, pack_compact, optional pack_superstellar (local extended objects)
- Each pack has its own staging + provenance.
- Compute dist_ly, helio and galactic coordinates.
- Export Parquet pack artifacts + pack QC reports.
- Request approval for each new source before ingestion.

Candidate sources (curated objects):
- UltracoolSheet (UCDs / substellar; CSV)
- DwarfArchives brown dwarfs (VOTable)
- Gaia DR3 UCD sample (CDS; fixed-width)
- Gaia EDR3/DR3 white dwarf catalogs (FITS)
- ATNF pulsar catalog (psrcat)
- McGill magnetar catalog (CSV)

Detection catalogs (raw survey detections; not “objects”):
- CatWISE2020 full tiles (bulk detections; very large)
  - If used later, treat as a sources pack (not object pack) and keep separate from “unique object” tables.

## v2.2: System view and generators
- The data epoch is J2000, add feature to select date. Recompute, rerender stars for different points in time based on proper motion.
- 3D Exoplanet render (plausible visualizations based on data)
- World builder tools (procedural generation with sliders)

## v3 Aspirational
- procedural ground generation of a planet/moon surface based on known planet / exoplanet data
- dark mode with a slider, a sun on one side and moon on the other side of the slider
- add political maps from popular scifi franchises like Star Trek and BATTLETECH.

# Status (as of 2026-02-04)
- Core ingestion pipeline complete (AT-HYG + NASA exoplanets).
- Morton indexing implemented (21 bits/axis, ±1000 ly), Parquet outputs sorted by spatial_index.
- `$SPACEGATE_STATE_DIR/served/current` promoted to latest build.
- CLI explorer available: `scripts/explore_core.py`.
- Optional packs deferred to v2.1.


# On completion, prune dependencies
