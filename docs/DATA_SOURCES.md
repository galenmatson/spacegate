# Data Sources

This document defines **all external data sources** used by the Spacegate project, how they are retrieved, and how they move through the pipeline.

The goal is strict provenance, reproducibility, and clear separation between:

- **what the universe gave us** (`raw/`)
- **what we assembled and normalized** (`cooked/`)
- **what we actively serve and query** (`served/`)

Nothing in this file is aspirational. Everything here reflects either current v0 reality or explicitly optional future packs.

---

## Directory semantics (normative)

### `raw/` — immutable inputs

**Purpose**: Preserve exact upstream artifacts.

- Files are downloaded verbatim from authoritative sources
- Never edited by hand
- Only written by downloader scripts
- Can always be re-fetched from source URLs

```
raw/
├── <catalog>/
├── manifests/
```

### `cooked/` — assembled, normalized, file-based products

**Purpose**: Deterministic preparation for ingestion.

- Built exclusively from `raw/`
- Still catalog-shaped (CSV, FITS-derived tables, etc.)
- No joins across catalogs
- No inference or enrichment

Everything in `cooked/` is disposable and regenerable.

```
cooked/
├── <catalog>/
```

### `served/` — queryable data products

**Purpose**: Efficient consumption.

- Built exclusively from `cooked/`
- Optimized for querying, filtering, joining
- Used by applications, analysis, visualization

Formats include DuckDB and Parquet.

```
served/
├── spacegate.duckdb
├── *.parquet
```

---

## Manifests

Manifests live in:

```
raw/manifests/
```

They record **provenance of raw inputs only**.

Each manifest entry contains:
- `source_name`
- `url`
- `dest_path`
- `retrieved_at`
- `checked_at`
- `sha256`
- `bytes_written`

Manifests are rewritten when download scripts are re-run.

---

## Core catalogs (v0)

These sources are required for v0 ingestion and must always be present.

### 1. AT-HYG (Astronexus HYG / AT-HYG)

**Authority**: Astronexus / HYG Database

**Raw inputs**:
- `raw/athyg/athyg_part1.csv.gz`
- `raw/athyg/athyg_part2.csv.gz`

**Source URL**:

- `https://codeberg.org/astronexus/hyg`

**Cooked outputs**:
- `cooked/athyg/athyg.csv.gz` (concatenated, column-normalized)

**Format**: gzipped CSV

**Contents**:
- Stellar positions
- Distances
- Identifiers (HIP, HD, Gaia where available)
- Photometry and spectral types

**Download script**: `raw/download_core.sh`

**Manifest**: `raw/manifests/core_manifest.json`

---

### 2. NASA Exoplanet Archive (TAP export)

**Authority**: NASA Exoplanet Archive

**Raw input**:
- `raw/nasa_exoplanet_archive/pscomppars.csv`

**Source URL**:
- `https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query=select+*+from+pscomppars&format=csv`

**Cooked output**:
- `cooked/nasa_exoplanet_archive/pscomppars_clean.csv`

**Format**: CSV

**Contents**:
- Confirmed exoplanets
- Host star identifiers
- Orbital parameters
- Planetary mass/radius where available

**Acquisition method**:
- TAP synchronous query
- Table: `pscomppars`

**Download script**: `raw/download_core.sh`

**Manifest**: `raw/manifests/core_manifest.json`

---

## Optional catalogs (packs, v0.1+)

These catalogs are not required for v0 but are retrieved via the same reproducible mechanism.

### Binary/Multiplicity — Washington Double Star Catalog (WDS)

**Authority**: US Naval Observatory (USNO)

**Raw input**:
- `raw/wds/wds.sum.gz`

**Source URL**:
- `http://www.astro.gsu.edu/wds/wds.sum.gz`

**Cooked output**:
- `cooked/wds/wds_clean.csv`

**Format**: Fixed-width text (custom)

**Contents**:
- Orbital elements for binary/trinary systems
- Separation distances and position angles
- Component identifiers (A, B, C...) matching HIP/HD stars

---

### Star Clusters — Gaia DR2 Clusters (Cantat-Gaudin et al. 2020)

**Authority**: CDS / Astronomy & Astrophysics

**Raw input**:
- `raw/clusters/cantat_gaudin_2020.fits` (Vizier table J/A+A/640/A1)

**Source URL**:
- `ftp://cdsarc.u-strasbg.fr/pub/cats/J/A+A/640/A1/table1.dat.gz`

**Cooked output**:
- `cooked/clusters/cluster_memberships.csv`

**Format**: FITS / Gzipped Text

**Contents**:
- Cluster names (e.g., Hyades, Pleiades) and membership probabilities
- Links between Gaia Source IDs and specific clusters

---

### Variable Stars — AAVSO International Variable Star Index (VSX)

**Authority**: AAVSO

**Raw input**:
- `raw/vsx/vsx.dat.gz`

**Source URL**:
- `ftp://cdsarc.u-strasbg.fr/pub/cats/B/vsx/vsx.dat.gz` (CDS Mirror)

**Cooked output**:
- `cooked/vsx/variables_classified.csv`

**Format**: CSV

**Contents**:
- Variability types (Flare stars, Cepheids, Eclipsing Binaries)
- Periodicity and amplitude
- "Hazard" metadata for systems

---

### Supernova Remnants — Green’s Catalogue of Galactic SNRs

**Authority**: MRAO Cambridge (D.A. Green)

**Raw input**:
- `raw/snr/snrs.list`

**Source URL**:
- `https://www.mrao.cam.ac.uk/surveys/snrs/snrs.list`

**Cooked output**:
- `cooked/snr/snr_boundaries.csv`

**Format**: Fixed-width text

**Contents**:
- Galactic coordinates of supernova remnants
- Angular size (extent)
- Type (Shell, Plerion, Composite)

---

### Pulsars — ATNF Pulsar Catalogue

**Authority**: Australia Telescope National Facility (ATNF)

**Raw input**:
- `raw/atnf/psrcat_pkg.tar.gz`

**Source URL**:
- `https://www.atnf.csiro.au/research/pulsar/psrcat/downloads/psrcat_pkg.tar.gz`

**Cooked output**:
- `cooked/atnf/pulsars_clean.csv`

**Format**: tar.gz package

**Contents**:
- Pulsar positions
- Periods, derivatives
- Distance estimates where available

---

### Magnetars — McGill Online Magnetar Catalog

**Authority**: McGill University

**Raw input**:
- `raw/magnetar/TabO1.csv`

**Source URL**:
- `http://www.physics.mcgill.ca/~pulsar/magnetar/TabO1.csv`

**Cooked output**:
- `cooked/magnetar/magnetars_clean.csv`

**Format**: CSV

**Contents**:
- Magnetar positions
- Spin periods and Pdot
- Distance estimates and references

---

### Ultracool dwarfs — UltracoolSheet

**Authority**: UltracoolSheet Team

**Raw input**:
- `raw/ultracoolsheet/UltracoolSheet - Main.csv`

**Source URL**:
- `http://bit.ly/UltracoolSheet` (Redirects to Google Sheet export)

**Cooked output**:
- `cooked/ultracoolsheet/ultracool_main_clean.csv`

**Format**: CSV

**Contents**:
- Ultracool dwarf astrometry
- Photometry
- Spectral types
- Binarity and references

---

### Gaia ultracool dwarf sample (CDS / A&A)

**Authority**: CDS / Gaia Collaboration

**Raw input**:
- `raw/gaia_ucd/table4.dat.gz`

**Source URL**:
- `ftp://cdsarc.u-strasbg.fr/pub/cats/J/A+A/657/A69/table4.dat.gz`

**Cooked output**:
- `cooked/gaia_ucd/gaia_ucd_clean.csv`

**Format**: fixed-width text table

**Contents**:
- Gaia DR3 ultracool dwarf sample
- Gaia source identifiers

---

### White dwarfs — Gaia EDR3 WD Catalogue (Gentile Fusillo et al. 2021)

**Authority**: University of Warwick / MNRAS

**Raw input**:
- `raw/white_dwarf/gaiaedr3_wd_main.fits.gz`

**Source URL**:
- `https://warwick.ac.uk/fac/sci/physics/research/astro/research/catalogues/gaiaedr3_wd_main.fits.gz`

**Cooked output**:
- `cooked/white_dwarf/white_dwarfs_clean.csv`

**Format**: FITS

**Contents**:
- Gaia EDR3 white dwarf candidates
- Astrometry and photometry

---

### DwarfArchives (conditional)

**Authority**: DwarfArchives.org

**Raw input (when enabled)**:
- `raw/dwarfarchives/dwarfarchives.data`

**Source URL**:
- `http://dwarfarchives.org/` (Requires scraping)

**Cooked output**:
- `cooked/dwarfarchives/dwarfarchives_parsed.csv`

**Format**: upstream-defined

**Notes**:
- Download controlled by environment variable
- Treated as opaque raw blob

---

### CatWISE2020 full tiles (conditional, large)

**Authority**: IPAC / IRSA

**Raw inputs**:
- `raw/catwise_full/<tile>/*.tbl.gz`

**Source URL**:
- `https://irsa.ipac.caltech.edu/data/WISE/CatWISE/2020/catwise_2020.html` (Base URL for file list)

**Cooked output**:
- `cooked/catwise_full/catwise_detections.parquet` (Partitioned by tile)

**Format**: gzipped IPAC tables

**Contents**:
- CatWISE2020 survey detections

**Notes**:
- Tile URLs provided via external list file
- Not deduplicated into objects at raw stage

---

## Download workflow

### Catalog Script

- Catalogs: `scripts/catalogs.sh`

Scripts features:
- Selection menu for catalogs to download or update
- Use `aria2c` for resumable, parallel downloads
- Show concurrent status
- Display status with common catalog name, not URL
- Verify byte counts
- Compute SHA-256 hashes
- Write manifest entries
- Log begin/end, error, complete, totals, etc. to logs/catalogs.log

---

## Invariants (non-negotiable)

- `raw/` is immutable
- `cooked/` is fully disposable
- `served/` depends only on `cooked/`
- No manual edits to `raw/`
- All provenance lives in `raw/manifests/`

If a file violates these rules, it is in the wrong directory.