# Spacegate: 3D Space Exploration and Worldbuilding Database
I've seen 3D star maps on the internet but they are all awful to browse. The ones in space games are way better, not technically more sophisticated, just better designed. I want to make one that is fun to browse, fun to click on stars and objects and read about them, and fun to play with. I want to do more than just list sterile data from the database. 

My desire is to have an interactive 3D map rendered in a browser which draws an accurate star map using the latest parallax measurements and build a space empire mapping tool on top of it. Create an agent to generate exciting, factual English exposition for each system grounded in real scientific data. Starting with the most important (Sirius, Alpha Centauri, etc) and unique (bright stars, exoplanets in habitable zones, inferno planets, trinary stars, dust rings, etc.) systems. It should be interesting and informative in a way that people will just read about space stuff for fun and keep exploring.

## The Rule of Cool
### Crazy Places
- Sextuple star systems: Castor is 3 binary pairs with a brown dwarf orbiting one of them. The physics are wild.
- "Hell worlds": tidally locked, ultra-short-period planets like WASP-12b (being eaten by its star) or HD 189733 b (raining glass sideways).
- Water and ice worlds: candidates like Kepler-22b or "eyeball planets" that are habitable only on the twilight terminator.
- Pulsar planets: systems like PSR B1257+12, where planets orbit a neutron star.

### Backyard Bonus
We should also prioritize places with a good "go outside and look" factor. For example Andromeda and the Orion Nebula, potentially with a recommended minimum telescope size.

## World Building
The world building features of the map should allow for things like trade lanes, spacegate links, or other connections to be drawn between stars. Spheres of control around owned/occupied systems that form the 3D shape of interstellar empires. Place megastructures like solar collectors, foundries, shipyards, Dyson swarms, colonies, momentum banks, space elevators, mines, mass drivers, space stations, etc. on planets, in orbit of them, in stellar orbit, or galactic orbit (unbound to stars). This content is stored in a completely separate database from the immutable scientific data.


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

## Schema Documents
- `docs/SCHEMA_CORE.md`: immutable scientific astronomy schema (authoritative source for core tables/types/invariants).
- `docs/SCHEMA_RICH.md`: derived/reproducible enrichment schema (coolness, tags, snapshots, factsheets, exposition, links).
- `docs/SCHEMA_LORE.md`: editable fictional overlay schema (namespaced lore entities/relationships/references).

Cross-dataset key rule:
- `stable_object_key` is the canonical cross-database join key and is required for object-scoped joins across core, rich, and lore.
- Numeric surrogate IDs (`system_id`, `star_id`, `planet_id`) remain `BIGINT` and are valid as same-build convenience keys.

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
├── data                  # This should be a large volume if you will use the expanded dataset (>150 GB)
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
- **Rich astronomy**: derived, regenerable content generated from core/packs (e.g., deterministic snapshot manifests, fact sheets, generated expositions, generated imagery). Not edited in-place; regenerate instead.
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

Rule: **No expositions, images, or lore** stored in core.
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

### 3) Rich dataset (v1+)
**Purpose:** derived artifacts that make the UI engaging while remaining strictly traceable to source facts.

- DuckDB (authoritative query format for the app/API):
  - `$SPACEGATE_STATE_DIR/out/<build_id>/rich.duckdb`
- Parquet export:
  - `$SPACEGATE_STATE_DIR/out/<build_id>/rich/*.parquet`

Rich tables (initial):
- `snapshot_manifest` (deterministic system visualization snapshots; see v1)
- `factsheets` (structured JSON facts per object, with provenance pointers)
- `expositions` (exciting factual descriptions generated strictly from factsheets; see v1.3)
- `system_neighbors` (10 nearest systems per system; see v1.5)

Rules:
- Rich is **not edited in-place**. If content is wrong or the generator changes, regenerate rich with a new `generator_version` / build.
- Each rich row must be traceable:
  - factsheets: include `facts_hash`, `generator_version`, and pointers to source rows/fields
  - expositions: include `facts_hash`, `model_id`, `prompt_version`, `generated_at`
  - snapshots: include `params_hash`, `params_json`, `generator_version`, `source_build_inputs_hash`

### 4) Engagement signals (v1.4+)
**Purpose:** capture minimal, privacy-respecting signals of collective human curiosity to improve discovery and prioritization — without analytics, profiling, or monetization.

This dataset exists to give the people what they want. If they are explicit in their interest of goldilocks planets or white dwarf trinaries or hell worlds or whatever, we should have a method of capturing that interest and feeding it into the coolness algorithm that prioritizes data enrichment.

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

## Snapshot assets vs snapshot manifest (v1)
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
- Keep all original parallax measurements referencable.
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
- Hosted on production cloud infrastructure
- Public at spacegates.org
- Attractive, searchable, filterable interface
- Reads core data in v0.1 (optional packs and lore overlays are later milestones)

### v0.1.5: Admin Control Plane + Authentication Foundation
Goal: add a secure, minimal admin panel that can replace high-value script workflows while establishing the auth foundation for future per-user lore features.

Implementation reference:
- `docs/ADMIN_AUTH_SPEC.md` (Checkpoint A concrete auth/RBAC schema and flow)

Success criteria:
- Admin panel route (`/admin`) protected by login.
- OIDC-based sign-in (Google-supported first) with strict identity allowlist for admin access.
- General identity model for future user features:
  - users, auth identities, sessions, roles (RBAC), and audit log.
- Security defaults:
  - HTTP-only secure session cookies, CSRF protection, short session TTL, and re-auth for sensitive actions.
  - Identity-based controls are primary (no static IP dependency required).
- Read-only operations views in admin:
  - current build id, served pointer target, publish metadata (`current.json`), reports, process health.

Checkpoints:
1. Checkpoint A: Auth + RBAC skeleton
   - schema + login/logout + role checks implemented.
   - admin allowlist enforced server-side.
2. Checkpoint B: Read-only admin operations dashboard
   - expose current build/publish/runtime status in UI without mutation actions.
3. Checkpoint C: Safe action runner for existing scripts
   - allowlisted actions only (`build_core`, `verify_build`, `publish_db`, restart services).
   - streamed job logs per run, explicit run parameters, no arbitrary shell execution.
4. Checkpoint D: Audit + hardening completion
   - action audit trail (`who`, `what`, `when`, `params`, `result`), CSRF tests, session expiry tests.

Current status (2026-02-21):
- Checkpoints A-D implemented in the admin API/UI.
- Admin login, allowlist enforcement, action runner, and audit log are active.

## v0.2: Coolness
This ensures compute resources for v1.3+ enrichment are spent on systems with high narrative and scientific yield. Features are aggregated into a final Coolness Score stored in the rich dataset.
- The default page, the first thing a new visitor sees, should be ordered by coolness.
- Extreme Luminosity (Economic Value): High-mass stars (O, B, A types) are heavily weighted due to their necessity for antimatter production via Dyson swarms.
- High Proper Motion (Kinetic Interest): Objects with significant angular movement across the sky are prioritized as "runaway" stars or nearby high-velocity neighbors.
- Stellar Multiplicity (Architectural Complexity): Points scale with the number of stars in the system; hierarchical trinaries or quaternaries rank significantly higher than simple binaries.
- Nice Exoplanets: Biological potential or high colonization targets. Known Earth like planets: habitable zone or close, not too big, not too small, stable star. Eye planets (tidally locked but in the habitable zone).
- Weird Exoplanets: Strange atmospheric composition, water worlds, diamond worlds, extreme size, acid worlds, lava worlds, "hell worlds" (ultra-short periods) or planets being devoured by their stars. High eccentricity planets that spend time in the habitable zone (like Trisolaris) and freeze/thaw. 
- Metallicity (Fe/H) (Industrial Capability): High-metal stars are prioritized as likely hubs for mining, foundries, and heavy industry.
- Compact Remnants: White dwarfs, neutron stars, pulsars, or magnetars adds a rarity multiplier due to their unique physics and "graveyard system" narrative.
- Anomalous Features: Specific data flags for high eccentricity, extreme stellar flares, or circumstellar dust rings.
- Proximity to Sol: The most colonizable with sublight technology. This bonus should decay quickly (inverse square of coolness).
- Science Fiction: Wolf 359 is where the Federation made its final stand against the Borg in Star Trek: The Next Generation. The exotic moon "Pandora" from the movie Avatar orbits a gas giant in the Alpha Centauri system. Vega is famous for its role in Carl Sagan's Contact. How this ranks is TBD. **Fiction must not contaminate the hard science data** Cultural and fictional importance should be stored with lore.

**Ranking by Narrative Density:** By combining these, a system like Sirius (high luminosity + White Dwarf companion) or Alpha Centauri (trinary + proximity) naturally rises to the top, while a lonely Red Dwarf at 800 light-years remains at the bottom of the stack. With these rankings stored in the rich database the later enrichment (narrative, depiction) steps will prioritize interest over row order as we enhance the dataset.

### Coolness Scoring + Tuning

The Coolness Score determines which systems receive computationally expensive enrichment (v1.3+ narrative + depiction). The objective is to prioritize systems with high scientific information density and narrative yield while preserving strict data integrity.

Coolness scoring must be:
- Deterministic
- Reproducible
- Derived strictly from core and approved pack data
- Independent of fictional, cultural, or editorial significance

The score is stored as a derived artifact in the rich dataset.

---

### Design Principles

1. **Scientific Sovereignty**
   - Only measurable or directly derivable astrophysical properties influence base scoring.
   - Fictional or cultural references must not modify `coolness_total`.

2. **Narrative Density**
   - Systems expressing multiple independent astrophysical phenomena rank higher than single-feature outliers.
   - Implemented via Shannon entropy diversity bonus.

3. **Non-linear Scaling**
   - Log or sigmoid scaling is preferred for count-based features.
   - Hard caps prevent domination by any single category.

4. **Explicit Extrapolation Policy**
   - Physically plausible extrapolations (e.g., potential habitable moons around gas giants) are allowed only when:
     - Based strictly on known planetary mass/orbital data.
     - Clearly flagged as inferred.
     - Labeled in enrichment outputs.
   - No invented measurements are permitted.

---

### Coolness Categories

Each category produces a bounded subscore.

- **Extreme Luminosity**
  - O/B/A stars
  - Unusual stellar radii or temperatures
  - Rare stellar evolutionary stages

- **High Proper Motion**
  - Nearby high-velocity or runaway stars

- **Stellar Multiplicity**
  - Binary, trinary, hierarchical systems
  - Points scale non-linearly with architectural complexity

- **Habitability Signals**
  - Confirmed HZ planets
  - Earth-sized planets in plausible temperature ranges
  - Tidally locked "eyeball" planets
  - Gas giants in HZ with plausible habitable-moon potential (flagged extrapolation)

- **Weird Exoplanets**
  - Ultra-short-period planets
  - Extreme eccentricity
  - Lava worlds, evaporating planets
  - Atmospheric anomalies
  - High-energy flare environments

- **Metallicity (Fe/H)**
  - Exceptionally high or low metallicity values
  - Must be verified measurements

- **Compact Remnants**
  - White dwarfs
  - Neutron stars
  - Pulsars
  - Magnetars

- **Anomalous Features**
  - Circumstellar disks
  - Extreme flaring
  - Rare astrophysical flags

- **Proximity to Sol**
  - Distance-based bonus with rapid decay (inverse-square or exponential)
  - Capped to prevent overshadowing extreme systems

---

### Diversity Bonus (Shannon Entropy)

To reward multi-dimensional systems:

Let:

p_i = category_score_i / total_score_before_entropy

H = - Σ p_i log2(p_i)

Normalized:

H_norm = H / log2(N_categories)

Final:

coolness_total = total_before_entropy + (entropy_weight * H_norm)

This promotes systems exhibiting multiple independent interesting properties.

---

### Front Page Selection (Separate from Scoring)

Scoring and featuring are distinct stages.

Selection algorithm:

1. Rank systems by `coolness_total`.
2. Select top N (default 500).
3. Determine dominant category per system.
4. Enforce diversity constraints:
   - Minimum 4 distinct dominant categories represented.
   - No category exceeds 40% of featured slots.
5. Fill remaining slots by rank.

This preserves scientific integrity while preventing single-category domination.

---

### Cultural Overlay (Non-Scoring)

Fictional or cultural significance is stored in a separate optional pack.

- May influence search relevance.
- May influence curated featuring layer.
- Must not alter `coolness_total`.

---

### Success Criteria

- `coolness_scores` derived artifact created in rich outputs.
- Score breakdown stored per system.
- Versioned weight profiles (profile_id + profile_version).
- Profile parameters stored in artifact JSON.
- Deterministic scoring pipeline.
- Admin panel supports:
  - Adjustable category weights
  - Sub-feature weights
  - Feature toggles
  - Entropy weight tuning
  - Proximity decay configuration
  - Named presets
- Preview mode:
  - Top-N ranking preview
  - Category distribution preview
  - Diff vs current profile
- Publish/apply flow:
  - Explicit profile activation
  - Audit record
  - Rollback capability

---

### Checkpoints

1. Checkpoint A: Deterministic scoring pipeline + report artifact.
2. Checkpoint B: Versioned profile storage + CLI preview/apply/diff.
3. Checkpoint C: Admin slider UI + visualization panel.
4. Checkpoint D: Promotion flow with audit trail and rollback.

---

### Profile Storage Contract (Implemented)

Storage location:
- `$SPACEGATE_STATE_DIR/config/coolness_profiles/`

Contract rules:
- Profile versions are immutable (`profile_id + profile_version`).
- Active profile pointer is stored separately from profile definitions.
- Activation history is append-only.
- Audit events are append-only.
- Score reports persist profile id/version/hash and resolved weights.

Store files:
- `profiles/<profile_id>/<profile_version>.json` (immutable definitions)
- `active.json` (current active pointer)
- `activations.jsonl` (activation/rollback history)
- `audit.jsonl` (profile + scoring audit events)

CLI commands:
- `scripts/score_coolness.py list`
- `scripts/score_coolness.py preview --profile-id <id> --profile-version <ver>`
- `scripts/score_coolness.py diff --right-weights-json '{\"weird_planets\":0.2}'`
- `scripts/score_coolness.py apply --profile-id <id> --profile-version <ver> --weights-json '{...}'`
- `scripts/score_coolness.py rollback --steps 1`

### Current Status (2026-02-21)

- Checkpoint A implemented (`scripts/score_coolness.py` + `coolness_report.json`).
- Checkpoint B implemented (versioned profile store + list/preview/diff/apply/rollback CLI).
- Checkpoint C implemented (admin slider/preset UI + diversity preview checks before apply).
- Profile activation audit trail + rollback implemented.
- Next milestone: Checkpoint D (promotion workflow hardening + rollback drill validation).

---

## v0.2.2: System Tagging Framework

Tags provide a lightweight semantic layer over astrophysical objects.  
They enable filtering, browsing, search enhancement, and narrative hooks.

Tags are deterministic and reproducible unless explicitly defined as pack-based.

### Design Principles

- Tags are cheap, indexable, and composable.
- Derived tags must be generated deterministically from measurable data.
- Tag generation must be versioned and tied to build_id.
- Tags must not alter base coolness scoring unless explicitly defined in the scoring profile.
- Cultural or fictional references must never exist as derived tags.

---

### Tag Categories

#### 1. Derived Physics Tags (v0.2)

Generated automatically during scoring or ingestion.

Examples:

Stellar:
- high_luminosity
- compact_remnant
- flare_star
- runaway_star
- metal_rich
- metal_poor
- multi_star_system
- hierarchical_system
- nearby_system

Planetary:
- habitable_zone_candidate
- earth_sized_planet
- gas_giant
- ultra_short_period
- high_eccentricity
- lava_world
- evaporating_planet
- water_world
- extreme_temperature

System-Level:
- architecturally_complex
- high_narrative_density
- anomalous_disk
- extreme_orbital_dynamics

These must be:
- Explicitly defined in scoring logic
- Stored as derived artifacts
- Recomputable

---

## v1 System Visualization
Goal: produce **deterministic, cacheable “system snapshot” images** that make the browser fun immediately, without requiring the full v2 3D map.

Canonical v1 progression (dependency-first):
1. v1.0 System visualization snapshots
2. v1.1 UI beautification
3. v1.2 External reference links
4. v1.3 AI exposition
5. v1.4 Image generation
6. v1.5 System neighbor graph
7. v1.6 Operations dashboard and telemetry

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

### Implementation checkpoint (current)
- Implemented baseline deterministic snapshot pipeline:
  - `scripts/generate_snapshots.py` and `scripts/generate_snapshots.sh`
  - SVG artifacts at `$SPACEGATE_STATE_DIR/out/<build_id>/snapshots/system_card/<stable_object_key>/<params_hash>.svg`
  - `snapshot_manifest` persisted in `rich.duckdb` and exported to `rich/snapshot_manifest.parquet`
  - report at `$SPACEGATE_STATE_DIR/reports/<build_id>/snapshot_report.json`
  - API exposure through `GET /api/v1/snapshots/{build_id}/{artifact_path}`
  - public UI list/detail now render these snapshots when available
- Remaining v1.0 expansion:
  - additional view types (`neighbors_10ly`, `neighbors_50ly`, `orbits_inner`, `orbits_outer`)
  - dedicated QC report for numeric/render invariants

---

## v1.1: Beautification

The UI must evolve from functional prototype to intentional interface.

Goals:

- Sleek, modern, and visually coherent
- Fast to scan and cognitively lightweight
- Data-dense without being overwhelming
- Designed around narrative clarity (what makes this system interesting?)

Beautification must not compromise:
- Performance
- Accessibility
- Readability
- Scientific integrity

---

### Core UI Principles

1. **Clarity Over Ornament**
   - Visual hierarchy must make it immediately obvious:
     - What system am I looking at?
     - Why is it interesting?
     - What are its most important properties?
   - Avoid decorative elements that do not improve comprehension.

2. **Progressive Disclosure**
   - Show summary first.
   - Reveal deeper parameters on demand.
   - Avoid dumping full row-level parameter tables by default.

3. **Visual Hierarchy**
   - System name (largest text)
   - Coolness breakdown / key tags (secondary emphasis)
   - High-impact metrics (luminosity class, star count, planet count, distance)
   - Detailed astrophysical parameters (collapsed by default)

4. **Consistency**
   - All interactive elements must behave predictably.
   - Spacing, margins, and typography must follow a unified scale system.

5. **Accessibility**
   - All themes must meet WCAG AA contrast minimums.
   - Keyboard navigable.
   - No information conveyed by color alone.
   - Motion must be optional (respect reduced-motion preference).

---

### Layout Refinement

System Detail Page:

- Header Section:
  - System name
  - Distance
  - Dominant category tag(s)
  - Coolness score visualization (subtle but informative)

- Highlight Panel:
  - 3–5 "Why This Is Interesting" bullet summaries derived from scoring breakdown.
  - Compact visual bars for category contributions.

- Structural Overview:
  - Star count
  - Multiplicity diagram (minimal schematic)
  - Planet count

- Expandable Sections:
  - Stellar parameters
  - Planetary parameters
  - Metallicity and spectral details
  - Provenance + match confidence

Search View:

- Clean list layout
- Left column: system name + summary tags
- Right column: distance + coolness
- Hover preview optional
- No full-parameter overload in list view

---

### Theme System

Themes are cosmetic layers only.  
They must not change layout structure or behavior.

Themes implemented via CSS variables and design tokens.

#### 1. Simple Dark
- Neutral dark background
- Minimal accent color
- Subtle shadows
- Modern sans-serif typography
- Clean card layouts

#### 2. Simple Light
- Neutral light background
- Minimal accent color
- High readability
- Slightly reduced visual noise
- Professional scientific aesthetic

#### 3. Cyberpunk
- Very dark background
- High contrast neon accent palette
- Monospaced fonts for data blocks
- Sharp borders
- Angular dividers
- Subtle scanline or console aesthetic (optional, lightweight)
- Avoid heavy glow effects that reduce readability

#### 4. Enterprise (LCARS-inspired)
- Black background
- Rounded rectangular “pill” panels
- Color-coded information bars
- Warm muted tones:
  - Mauve / purple
  - Orange
  - Tan
  - Yellow
  - Teal
- High-contrast layout
- Large typographic labels
- Flat, panel-based interface
- No skeuomorphic gradients
- Must remain usable even without franchise familiarity

Note:
This theme is an homage, not a replica. Avoid copyrighted UI replication. Capture the design language, not the exact layout.

#### 5. Mission Control
- Black background
- Monochrome green or amber text
- Monospace everywhere
- No gradients
- Clean ASCII-inspired
Appeals to:
- programmers
- hacker aesthetic fans
- minimalists
- This feels “mission control terminal.”

#### 6. Aurora
- Deep navy background
- Soft gradient accent colors (teal → violet)
- Subtle glow edges
- Smooth rounded UI
- Slightly modern Apple-ish
Appeals to:
- mainstream modern users
- design-conscious crowd
- 20–40 demographic
- Safe, modern

#### 7. Geocities (Retro ’90s)
Leans into intentionally klunky early-web nostalgia while staying usable:
- Soft gray background
- Light beveled panels
- Clean but nostalgic
- Subtle pixel font for headers only
Appeals to:
- 35–55 nostalgic users
- early web vibe
- memberberries

#### 8. Deep Space Minimal (Black Void)
- Pure black background
- Almost no borders
- Content floats
- Sparse accent color
- Feels like UI in darkness
Appeals to:
- people who love minimalism
- OLED screen users
- night browsing

#### Current Theme/UX Implementation Notes
- Theme selection is persisted in browser storage and applied globally.
- Search shortcut is `/` (focuses search input when not typing in a field).
- Search filters support `Collapse Up`; when collapsed, results expand to full width.
- Enterprise theme includes LCARS-style header telemetry, decorative left chips, and linked history chips under `STARS ACCESSED`.

---

### Data Density Strategy

Avoid parameter overload.

Instead:

- Display derived, narrative-relevant highlights.
- Collapse raw numeric tables behind expandable sections.
- Use icons sparingly and meaningfully.
- Use tooltips for definitions (spectral class, Fe/H, etc.).

---

### Motion & Interaction

- Animations must be subtle and fast (<200ms).
- No parallax.
- No excessive glow.
- No auto-rotating elements.
- Hover states must clearly indicate interactivity.

---

### Performance Constraints

- Themes must not significantly increase bundle size.
- Avoid heavy background images.
- No large shader effects.
- Must render cleanly on mid-tier laptops and mobile devices.

---

### Success Criteria

- UI redesign applied without breaking API contracts.
- Theme switching is instantaneous and persistent.
- Most important system information is scannable in <3 seconds.
- Parameter overload reduced in default views.
- Lighthouse performance score remains high.

---
  
## v1.2: External reference links (curated web sources)
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
- **Coverage budget**: only top-N coolness objects in v1.3; full coverage later.
- **Refresh cadence**: re-check links only on build regeneration or every 6–12 months.
- **Strictly link-only**: store URLs + metadata only; no copying page text into rich.

## v1.3: AI rich description (“facts → exposition”)
Success criteria:
- Generate fact sheets (structured JSON) per selected object with sources/provenance.
- Generate engaging but factual descriptions derived from known facts.
- Store generated content as derived artifacts with:
  - model/version, generated_at, prompt, and fact-sheet hash
  - display small text notification if fact sheet hash no longer matches (indicating new information)
- Target voice: science outreach personality (factual, enthusiastic; no fabrication).
- Counter prompt is used to evaluate the factuality of the exposition and discard hallucinations


## v1.4: Image generator
Based on the descriptions from the expositions, generate instructions for image
generator model create vivid imagery of planets, stars, and systems.
- Shareable versions with text captions at the bottom
- link an image generator that will generate system images on coolness or first visit
- tooltip with prompt that generated the image
- generate more images in systems wtih lots of visits
- up/down voting, reorder with most popular
- popular images push system to be featured on front page and suggested in system links
- most popularist images get expanded in size and resolution to full page background images, 
  - free download as (up to) 4k desktop backgrounds
- generate short, captivating meme text like "Hell World HD 189733_b where it rains glass sideways in Earth sized cyclones."
- make recaptioning and sharing easy, link back

### Star/System view
  - coolness score should prioritize complex systems and exotic stars
  - center close binaries and planets
  - show distant companions in background
  - aim for accuracy but exagerate slightly if necessary to make dim companions visible
  - star is accurate to spectrum, surface temp, and size
  - flare stars depicted with erupting solar flares
  - pulsars depicted with polar jets though they might be invisible without gas/dust to scatter/emit)
  - magnetars show powerful rotating magnetic fields (despite being invisible or show them interacting with something)

### v1.4.1 Planets
#### Global view
  - planet in the foreground and the star behind
  - show flare stars scorching them with glowing prominences
  - eyeball planets for tidally locked planets at right distance
  - maybe young systems or lots of dust detected show asteroid/comet strikes
  - add moons to large planets even if none detected
  - include subtext that these images are extrapolated from scientific data, not observation, and reality is likely quite different.

#### v1.4.2 Surface view
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




## v1.5: System neighbor graph (10 nearest systems)
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

## v1.6: Operations dashboard and telemetry
Goal: after rich content is working, add an at-a-glance operations view so service health and usage can be assessed in seconds.

Success criteria:
- Single dashboard view with clear green/yellow/red status.
- Service/runtime status:
  - nginx status and active config mode (container web vs static web).
  - API and web process/container status, uptime, and restart counts.
- Endpoint checks:
  - `GET /` and `GET /api/v1/health` through nginx.
  - Direct API health check.
- Build state:
  - current `build_id`, active DB path, and `served/current` pointer target.
- Usage and reliability metrics:
  - request rate and endpoint mix (especially search endpoints),
  - error rate (4xx/5xx),
  - basic latency percentiles (p50/p95) for key API endpoints.
- Capacity snapshot:
  - CPU, memory, and disk usage for host + containers.

Implementation notes:
- Start with a local terminal monitor (`scripts/spacegate_status.sh`).
- Then expose metrics from API/nginx and add a dashboard stack (e.g., Prometheus + Grafana) with basic alerts.

---

## v2: 3D map (browser)
This is the ultimate goal. An intuitive and inviting interface that makes it easy to explore space.

Success criteria:
- Lightweight browser-based 3D viewer (likely three.js; evaluate alternatives later).
- Smooth controls: zoom/rotate/pan/recenter; selection; tooltips.
- Filters (distance bubble, spectral class, magnitude, etc.)
- Optional rendering toggles: planets, packs, lore layers, neighbor links, spacegate links.
- Easy zoom and transition:
    - Click a system, zoom to extents: furthest separation of stars or planets
    - Smooth zoom from the starfield to the system
    - Display the 3D system with labeled components, system card, description, and linked list of objects
    - Click a system object, if it's a subsystem, smooth zoom to its extents
    - Display a system card with scrollable cards for subobjects in the system ordered by coolness
    - Continue navigating down hierarchies 

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

# Status (as of 2026-02-21)
- Core ingestion pipeline complete (AT-HYG + NASA exoplanets).
- Morton indexing implemented (21 bits/axis, ±1000 ly), Parquet outputs sorted by spatial_index.
- `$SPACEGATE_STATE_DIR/served/current` promoted to latest build.
- CLI explorer available: `scripts/explore_core.py`.
- Public deployment live at `spacegates.org` (provider details intentionally excluded from repo docs).
- Published bootstrap metadata (`current.json`) includes artifact checksums and report references.
- v0.1.5 Admin Control Plane checkpoints A-D implemented (OIDC auth, allowlist, action runner, audit).
- v0.2 Coolness checkpoints A and B implemented (scoring outputs, profile contract, CLI preview/diff/apply/rollback, report/audit provenance).
- Optional packs deferred to v2.1.


# On completion, prune dependencies
