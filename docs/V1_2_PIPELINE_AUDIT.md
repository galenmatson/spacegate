# Spacegate v1.2 Pipeline Audit

This document audits the current `download -> cook -> ingest` core pipeline against the v1.2 field hierarchy in `docs/PROJECT.md`.

Status: audit snapshot as of 2026-03-02.

## Current pipeline shape

Current build flow:

1. `scripts/download_core.sh`
2. `scripts/cook_core.sh`
3. `scripts/ingest_core.sh` / `scripts/ingest_core.py`
4. `scripts/promote_build.sh`
5. `scripts/verify_build.sh`

Current reality:

- The pipeline is still a two-catalog core ingest: AT-HYG + NASA `pscomppars`.
- Optional catalog URLs already exist in `scripts/catalogs.sh`, but they are not part of a field-precedence merge plan yet.
- The ingest path is AT-HYG-centric for `systems` and `stars`, with NASA joined afterward for `planets`.

## Findings

### 1. Download is catalog-aware, not field-aware

Relevant files:
- `scripts/catalogs.sh`
- `scripts/download_core.sh`

Observations:

- Catalog retrieval is already modular enough to add more sources.
- Manifests are organized per catalog group, but there is no notion of:
  - source family
  - field precedence
  - approval status
  - scope (`core canonical` vs `pack` vs `raw detection only`)
- `--core` currently expands only to `athyg` and `nasa_exoplanet_archive`.

Implication for v1.2:

- The downloader needs a source registry that distinguishes:
  - canonical core astrometry/kinematics
  - canonical core multiplicity/name sources
  - pack-only sources
  - catalogs that must remain raw detections or sidecar references

### 2. Cook is shallow file cleanup only

Relevant file:
- `scripts/cook_core.sh`

Observations:

- AT-HYG cooking is only concatenation of the split CSV files.
- NASA cooking is only newline/BOM normalization.
- Cooked outputs remain raw-ish files with almost no typed normalization.
- Source-native units/epochs are not explicitly preserved as schema-level cooked fields; they are simply left implicit in the source files.

Implication for v1.2:

- Cook should become per-catalog normalization, not just byte cleanup.
- Each cooked catalog should preserve:
  - source identifiers
  - source-native units
  - source astrometry epoch/frame when known
  - typed normalized columns for later merge
- Cross-catalog joins should still be deferred until ingest.

### 3. Ingest is monolithic and AT-HYG-centric

Relevant file:
- `scripts/ingest_core.py`

Observations:

- Star ingestion, system grouping, source selection, unit conversion, and provenance assignment are all collapsed into one AT-HYG-only stage.
- `systems` inherit the primary star position and provenance from the same AT-HYG path.
- `planets` are joined afterward using identifier/name matching against the already-built star table.
- There is no source-merge phase where one catalog can win for astrometry while another wins for names or physical parameters.

Implication for v1.2:

- Ingest should be split conceptually into:
  1. per-source staging
  2. canonical star merge
  3. system assembly / multiplicity resolution
  4. planet host attachment
  5. reports/QC/export

### 4. Canonical distance/xyz storage is currently LY-only

Relevant file:
- `scripts/ingest_core.py`

Observations:

- AT-HYG `dist`, `x0`, `y0`, `z0` are ingested in parsecs and immediately converted to `dist_ly` and `x/y/z_helio_ly`.
- The parsec-valued columns are then discarded from the canonical tables.
- Downstream APIs and scripts rely on the LY columns only.

Implication for v1.2:

- This is not ideal stewardship for core astronomy data.
- Canonical normalized storage should keep parsec-valued columns.
- LY columns can remain as deterministic convenience/materialized columns for API, UX, and rendering.

### 5. Epoch handling is only build-level today

Relevant files:
- `scripts/ingest_core.py`
- `docs/SCHEMA_CORE.md`

Observations:

- `build_metadata.coordinate_epoch` now exists, which is the correct build-level contract.
- The row-level source epoch and normalization method are not stored on `stars` or `systems`.
- That is fine for the current AT-HYG-only baseline, but it is not sufficient once mixed-source astrometry is introduced.

Implication for v1.2:

- A Gaia-first/J2016 canonical build is reasonable.
- But non-Gaia rows need explicit handling:
  - if proper motion/radial velocity permit projection to `J2016.0`, store the normalization method
  - if they do not, do not silently pretend they are native `J2016.0` coordinates
- Add row-level astrometry provenance fields before mixed-epoch merging becomes real.

### 6. Provenance is row-level, not field-level

Relevant file:
- `scripts/ingest_core.py`

Observations:

- Current provenance fields are correct for a single-source row origin.
- They are not yet expressive enough for a merged row where:
  - Gaia wins for astrometry
  - AT-HYG wins for names
  - a spectroscopic survey wins for `Teff`

Implication for v1.2:

- Row-level provenance remains necessary.
- Field-level lineage should be added as structured sidecar metadata or JSON per row for merged columns, at least for high-value fields.

### 7. Host matching has no v1.2 merge hooks yet

Relevant file:
- `scripts/ingest_core.py`

Observations:

- Match priority is `Gaia -> HIP -> HD -> exact hostname`.
- There is no opt-in fuzzy stage yet.
- There is no post-merge re-resolution step after multiplicity improvements.

Implication for v1.2:

- Planet host matching should run after canonical star/system merge, not before v1.2 enrichment inputs are finalized.

## Recommended refactor sequence

### Phase A: Source registry and approval metadata

- Add a small source registry describing:
  - source family
  - intended scope (`core`, `pack`, `detection-only`)
  - field coverage
  - precedence tier
  - epoch/frame expectations

### Phase B: Cooked source normalization

- Emit typed cooked outputs per catalog.
- Preserve source-native units and epochs explicitly in cooked outputs.
- Do not merge catalogs during cook.

### Phase C: Canonical star merge

- Build a canonical star staging table keyed by preferred identifiers.
- Resolve field precedence column-by-column.
- Preserve canonical parsec columns and materialize LY columns.
- Normalize astrometry to the build epoch where justified.

### Phase D: Canonical system assembly

- Build systems from explicit multiplicity relationships first.
- Fall back to name-root and then proximity only where explicit relationships are absent.
- Recompute system anchor fields from the canonical merged star table.

### Phase E: Planet attach and QC

- Re-run host matching against the merged star/system table.
- Add fuzzy matching as explicit opt-in with lower confidence and auditing.
- Add QC for mixed-source field coverage and epoch normalization coverage.

## Immediate design constraints for the next implementation pass

- Do not let AT-HYG remain the implicit winner for all stellar fields once Gaia or other approved sources are introduced.
- Do not discard parsec values during canonical ingest.
- Do not claim a global `J2016.0` canonical epoch for rows that were not actually normalized to that epoch.
- Do not let pack catalogs silently change canonical core IDs or group semantics without an explicit migration rule.
