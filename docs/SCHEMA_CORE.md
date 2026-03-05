# Spacegate Core Schema

This document is the source of truth for the **core astronomical data model** and its invariants.
It is written as an **executable contract**: ingestion, QC, export, and API behavior should follow this.

Schema family:
- `docs/SCHEMA_CORE.md`: immutable scientific astronomy data (this document)
- `docs/SCHEMA_RICH.md`: reproducible derived artifacts (coolness, snapshots, factsheets, exposition, links)
- `docs/SCHEMA_LORE.md`: editable fictional overlays and user-authored worldbuilding content

Hard boundary:
- Core remains scientific and immutable.
- Rich and lore data are stored in separate databases/artifacts and must never mutate core rows.

## Scope (Core)

Current baseline ingests:

- **AT-HYG** star catalog CSV (local sphere subset already in repo)
- **NASA Exoplanet Archive** CSV (pscomppars; host matching limited by core star coverage)
- **WDS** + **ORB6** multiplicity support catalogs
- **Gaia DR3 NSS support extracts** (`non_single_star`, `nss_two_body_orbit`) for star-level multiplicity evidence

Optional (default-off) multiplicity ingest:
- **MSC** component insertion (`SPACEGATE_ENABLE_MSC=1`)

Core produces a **pure astronomy** dataset. No lore, expositions, or generated imagery live in core.

Optional packs (substellar/compact/extended objects) and editorial/lore layers are separate artifacts.
Derived “rich” artifacts are out of scope for this schema and are documented in `docs/SCHEMA_RICH.md`.

---

## Primary artifact

- `$SPACEGATE_STATE_DIR/out/<build_id>/core.duckdb` (DuckDB database)
- `$SPACEGATE_STATE_DIR/out/<build_id>/parquet/{systems,stars,planets}.parquet`
- `$SPACEGATE_STATE_DIR/reports/<build_id>/...` (match/QC/provenance reports)

### Build ID
`build_id = YYYY-MM-DDTHHMMSSZ_<gitshortsha>`

---

## Coordinate conventions and units

### Units
- Raw and cooked catalog products should preserve source-native units until canonical normalization.
- Canonical normalized distance/position storage should be in **parsecs (pc)**.
- Derived/materialized convenience columns in **light-years (ly)** are recommended for API/query/render efficiency and user-facing ergonomics.
- Orbital SMA is stored in **AU** (from NASA).

Current runtime note:
- The v0 implementation currently materializes LY columns only.
- v1.2 should add canonical parsec columns without removing LY convenience columns.

### Frames
We store two cartesian frames:

1) **Heliocentric (primary for local sphere)**
- `x_helio_ly, y_helio_ly, z_helio_ly`
- Origin at Sol
- Used for neighborhood queries and rendering with floating origin on the client.

2) **Galactocentric (optional; nullable in current baseline)**
- `x_gal_ly, y_gal_ly, z_gal_ly`
- Used for galaxy-scale views later.

### Reference epoch
- Core coordinates are stored at a build-scoped reference epoch recorded in `build_metadata`.
- Current project standard: `J2016.0`, aligned with Gaia-era proper-motion observations.
- Future epoch rendering/projection must derive new positions from the stored base coordinates plus motion fields; it must not overwrite the canonical stored coordinates for that build.
- When non-Gaia source astrometry is projected to the build epoch, preserve the source epoch and normalization method at row level once mixed-source astrometry is introduced.

### Required invariant
For rows with both `dist_ly` and heliocentric xyz present:

`abs(sqrt(x^2 + y^2 + z^2) - dist_ly) < eps`

Default `eps = 1e-3 ly` (adjust if source rounding is coarser).

### RA/Dec
- Stored in degrees (`ra_deg`, `dec_deg`) if available from source.
- Heliocentric xyz may be sourced directly (AT-HYG provides xyz) or derived from RA/Dec+dist when needed.
- Gaia astrometry should be preferred over AT-HYG positional values when an approved Gaia-linked record exists.

## Spatial Indexing (Morton Z-Order)

To optimize 3D range queries (e.g., "find all stars within 10 ly of Earth") and file scanning, we strictly order data on disk using a Spatial Index.

### Implementation
- **Algorithm:** 63-bit Morton Code (Z-Order Curve) stored in signed 64-bit `BIGINT`.
- **Coordinate Space:** Heliocentric `x, y, z` in light-years.
- **Bits per axis:** 21 (total interleaved bits = 63; guaranteed to fit in signed BIGINT).
- **Domain:** build-scoped cube centered on Sol with half-width `MORTON_MAX_ABS_LY` (v0 default: 1000.0 ly).
- **Quantization:**
  - Let `N = 2^21 - 1`.
  - Let `scale = N / (2 * MORTON_MAX_ABS_LY)`. (Computed once per build.)
  - For each axis `coord ∈ [-MORTON_MAX_ABS_LY, +MORTON_MAX_ABS_LY]`:
    - `q = round((coord + MORTON_MAX_ABS_LY) * scale)`
    - clamp `q` into `[0, N]` (defensive guard only).
- **Interleave:** bit-interleave `qx, qy, qz` (21 bits each) into a 63-bit Morton integer.
- **Storage:** stored as signed `BIGINT` for portability; interpret logically as an unsigned bitset.

### Invariants
- All output Parquet files (`stars.parquet`, `systems.parquet`) must be **physically sorted** by `spatial_index`.
- Ingestion must **hard-fail** if any star coordinate exceeds the configured domain:
  - `max(|x|,|y|,|z|) > MORTON_MAX_ABS_LY` ⇒ abort with clear error message.
- Morton parameters (`bits_per_axis`, `MORTON_MAX_ABS_LY`, `scale`, quantization rule) must be recorded as build metadata for reproducibility.

---

## Stable keys and IDs

### Internal IDs
- `system_id`, `star_id`, `planet_id` are surrogate integer primary keys (BIGINT).
- They may change between builds.
- All cross-artifact joins should use `stable_object_key`.

### stable_object_key (required)
A stable identifier used to join:
- core ↔ optional packs
- core ↔ editorial content/images
- core ↔ lore overlay



**Rule of thumb: prefer authoritative catalog IDs; fall back to deterministic hashes.**

#### Stars
Preferred order:
1) Gaia DR3 source id: `star:gaia:<gaia_id>`
2) HIP: `star:hip:<hip_id>`
3) HD: `star:hd:<hd_id>`
4) WDS component key when an approved multiplicity ingest creates a star with no better exact ID: `star:wds:<wds_id>:<component>`
5) fallback: `star:hash:<hash>`

Fallback hash input (deterministic):
- normalized name (if any)
- rounded RA/Dec (e.g., 1e-5 deg)
- rounded dist_ly (e.g., 1e-3 ly)

#### Systems (Clustering & IDs)
Systems are logical groupings of stars. Aggregation is performed during ingestion:

1. **WDS-linked Grouping:** Stars carrying the same `wds_id` are grouped first when approved multiplicity catalogs provide an explicit relationship key.
2. **Name-based Grouping:** Remaining stars sharing a `proper_name` root (e.g., "Sirius A", "Sirius B") are grouped.
3. **Proximity-based Grouping:** Remaining stars within **0.25 ly** (~3000 AU) are grouped if they do not already share an explicit multiplicity or name grouping.
   - This grouping is transitive (A near B, B near C ⇒ A/B/C grouped).
   - In v0, proximity grouping is optional and gated by `SPACEGATE_ENABLE_PROXIMITY=1`.
   - Proton benchmark builds currently keep proximity disabled by default (`SPACEGATE_ENABLE_PROXIMITY=0`).
   - When disabled, ungrouped stars are treated as singleton systems.
**Stable Key Generation:**
The System prefers an explicit multiplicity key when present; otherwise it inherits identity from the **Primary Star** (brightest by Vmag) in the group.

- `system:wds:<wds_id>` when `grouping_basis = 'wds'`
- `system:gaia:<primary_star_gaia_id>`
- `system:hip:<primary_star_hip_id>`
- `system:hash:<hash>` (fallback)

**Coordinates:**
- `system.x/y/z` = `primary_star.x/y/z` (Barycenter calculation deferred to v2).

#### Planets
- From NASA planet name primarily:
  - `planet:nasa:<normalized_pl_name>`
- If collisions occur, include host normalized name or NASA row id in hash.

---

## Normalization rules

### Name normalization
`*_name_norm`:
- lowercase
- strip punctuation
- collapse whitespace
- normalize apostrophes/diacritics to ASCII where possible

### Spectral parsing
Store both raw and structured fields:

- `spectral_type_raw` (exact source string)
- `spectral_class` (O/B/A/F/G/K/M/L/T/Y/...; nullable)
- `spectral_subtype` (0–9.x; nullable)
- `luminosity_class` (I/II/III/IV/V/VI/VII; nullable)
- `spectral_peculiar` (flags like e/p/m/n/var, composite markers like +, etc.)

Parsing rules:
- Parse the **primary** component when composite (e.g., `K1III+DA2` → primary `K1III`, peculiar `+DA2`)
- If ambiguous/unparseable, keep raw and leave structured null.

## Field precedence (v1.2 target)

Field precedence for catalog expansion is defined in `docs/V1_2_SOURCE_MATRIX.md`.

Contract rules:
- Use approved source families column-by-column rather than treating any one catalog as globally authoritative.
- Prefer Gaia-linked astrometry/kinematics when available.
- Use AT-HYG as fallback where preferred sources are absent.
- Inferred astrophysical values (for example spectral-type-derived `Teff`) do not belong in core; keep them in rich and flag them as inferred there.
- When canonical coordinates are normalized from a source epoch to the build epoch, the source epoch and normalization method must be preserved once row-level mixed-source astrometry metadata is added.

---

## Provenance (required on every derived row)

Every row in `systems`, `stars`, `planets` must include provenance fields. These are mandatory in core.

Minimum set:

- `source_catalog` (e.g., `athyg`, `nasa_exoplanets`)
- `source_version` (string, may be build-time constant)
- `source_url` (where the raw file was retrieved)
- `source_download_url` (direct file URL used for the download)
- `source_doi` (DOI if applicable)
- `source_pk` (authoritative PK if available, e.g., Gaia source_id)
- `source_row_id` (row identifier if present) OR `source_row_hash` (hash of raw row)
- `license`, `redistribution_ok`, `license_note`
- `retrieval_etag` and/or `retrieval_checksum` (when available)
- `retrieved_at`, `ingested_at`
- `transform_version` (git SHA or pipeline version string)

QC: provenance completeness must be 100% for required fields.

---

## Tables (Core)

This doc describes the current core table contract used by ingestion and API services.

### systems

Represents a star system (single or multiple stars). Systems are the top-level “place” users navigate.

Key columns:
- `system_id` (PK)
- `spatial_index` (BIGINT, distinct, cluster key)
- `stable_object_key` (unique, join key)
- `system_name`, `system_name_norm`
- `wds_id` (nullable explicit multiplicity/grouping key when available)
- `grouping_basis`, `grouping_confidence`, `grouping_source_catalogs_json`
- `has_gaia_nss_evidence`
- `has_msc_evidence`, `has_wds_evidence`, `has_orb6_evidence`
- planned v1.2 additive: `dist_pc`
- `ra_deg`, `dec_deg`, `dist_ly` (best available; may represent anchor star)
- planned v1.2 additive: `x_helio_pc,y_helio_pc,z_helio_pc`
- `x_helio_ly,y_helio_ly,z_helio_ly` (anchor position)
- optional `x_gal_ly,y_gal_ly,z_gal_ly` (nullable)
- planned v1.2 additive: row-level astrometry source epoch / normalization metadata
- external IDs where applicable (`gaia_id`, `hip_id`, `hd_id`)
- provenance fields

### build_metadata

Build metadata for reproducibility.

Key columns:
- `key` (TEXT)
- `value` (TEXT)

Required keys (core):
- `build_id`
- `git_sha`
- `coordinate_epoch`
- `morton_bits_per_axis`
- `morton_max_abs_ly`
- `morton_scale`
- `morton_quantization`
- `morton_frame`

Indexes:
- unique on `stable_object_key`
- xyz index for radius queries

### stars

Represents individual stars (including components A/B/C where possible).

Key columns:
- `star_id` (PK)
- `spatial_index` (BIGINT, distinct, cluster key)
- `system_id` (FK to systems)
- `stable_object_key` (unique, join key)
- `star_name`, `star_name_norm`, `component`
- `wds_id` (nullable multiplicity/grouping key)
- `multiplicity_match_method`, `multiplicity_match_confidence`, `multiplicity_source_catalogs_json`
- `gaia_non_single_star` (bool exact Gaia NSS flag)
- `gaia_nss_solution_count` (count of Gaia NSS two-body solutions for the star)
- `gaia_nss_solution_types_json` (distinct solution types as JSON array)
- `gaia_nss_significance_max` (max two-body significance from Gaia NSS extract)
- note: these fields may be populated sparsely until additional multiplicity sources are approved for active builds
- planned v1.2 additive: `dist_pc`, `x_helio_pc`, `y_helio_pc`, `z_helio_pc`
- coordinates (ra_deg, dec_deg, dist_ly, x/y/z_helio_ly)
- planned v1.2 additive: row-level astrometry source epoch / normalization metadata
- `pm_ra_mas_yr`, `pm_dec_mas_yr` (proper motion)
- `radial_velocity_kms` (required for v2 epoch projection)
- radial velocity
- spectral fields (raw + parsed)
- optional photometry/physicals (nullable)
- catalog IDs and `catalog_ids_json`
- provenance fields

Indexes:
- unique on `stable_object_key`
- index on `system_id`
- xyz index for radius queries
- index on `gaia_id`

### planets

Represents confirmed exoplanets from NASA (as of CSV export).

Key columns:
- `planet_id` (PK)
- `spatial_index` (BIGINT, cluster key)
- `stable_object_key` (unique)
- `system_id` (FK, nullable until matched)
- `star_id` (FK, nullable until matched)
- `planet_name`, `planet_name_norm`
- discovery fields (method/year/facility; nullable)
- orbital parameters: `orbital_period_days`, `semi_major_axis_au`, `eccentricity`, `inclination_deg`
- planet properties: mass/radius/temp/insolation fields (nullable)
- host identifiers as seen in NASA row:
  - `host_name_raw`, `host_name_norm`, `host_gaia_id`, `host_hip_id`, `host_hd_id`
- match provenance:
  - `match_method`, `match_confidence`, `match_notes`
- provenance fields

Indexes:
- unique on `stable_object_key`
- indexes on `star_id`, `system_id`, `host_name_norm`

### planet_host_match_audit (optional but recommended)

Append-only audit records describing match decisions.

Key columns:
- `planet_id` (FK)
- `star_id`, `system_id` (FKs; nullable)
- `match_method`, `match_confidence`, `match_notes`
- `decided_at`, `transform_version`

---

## Planet → host matching algorithm (core baseline)

Goal: assign `star_id` and `system_id` for as many planets as possible, with explainable provenance.

### Matching priority (highest to lowest)

1) **Gaia DR3 ID match**
- NASA `gaia_dr3_id` → `stars.gaia_id`

2) **HIP match**
- NASA `hip_name` parsed to integer → `stars.hip_id`

3) **HD match**
- NASA `hd_name` parsed to integer → `stars.hd_id`

4) **Host name match**
- NASA `hostname` normalized → compare against:
  - `stars.star_name_norm`
  - `systems.system_name_norm`
- If multiple candidates, break ties by proximity if `sy_dist` exists.

5) (Optional in v0) **Fuzzy name match**
- Only if enabled; must record method `fuzzy` and lower confidence.

### Confidence scoring (suggested)
- Gaia exact: 1.00
- HIP exact: 0.95
- HD exact: 0.90
- Hostname exact unique: 0.80
- Hostname ambiguous tie-broken by distance: 0.70
- Fuzzy: <= 0.60

### Unmatched handling
- Leave `planets.star_id` and `planets.system_id` NULL.
- Set `match_method = 'unmatched'`, `match_confidence = 0`, notes include reason if known.

---

## QC gates (core)

Hard failures:
- Required provenance fields are non-null in 100% of rows.
- xyz/dist invariant holds for rows with both present (within eps).
- `stable_object_key` is unique within each table.

Warnings:
- planet match rate changes by > 0.5% absolute vs previous build
- unmatched planets increases by > 25 vs previous build
- duplicate host name mappings exceed threshold

Reports produced each build:
- `$SPACEGATE_STATE_DIR/reports/<build_id>/match_report.json`
- `$SPACEGATE_STATE_DIR/reports/<build_id>/qc_report.json`
- `$SPACEGATE_STATE_DIR/reports/<build_id>/provenance_report.json`

---

## Exports

Parquet exports should preserve:
- column names and types
- `stable_object_key`
- provenance columns
- match columns on planets

Export location:
- `$SPACEGATE_STATE_DIR/out/<build_id>/parquet/`

---

## Forward-compatible changes

Allowed without bumping major schema:
- add new **nullable** columns
- add new indexes
- add new optional tables

Breaking changes (avoid; require schema version bump):
- renaming columns
- changing meaning of `stable_object_key`
- changing units of stored fields

Schema versioning:
- Keep this document as the current core contract.
- If a breaking change is required, create a versioned successor (`SCHEMA_CORE_v1.md`, etc.).
