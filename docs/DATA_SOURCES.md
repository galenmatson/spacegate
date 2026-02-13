# Data Sources

This document defines **all external data sources** used by the Spacegate project, how they are retrieved, and how they move through the pipeline.

The goal is strict provenance, reproducibility, and clear separation between:

- **what the universe gave us** (`$SPACEGATE_STATE_DIR/raw/`)
- **what we assembled and normalized** (`$SPACEGATE_STATE_DIR/cooked/`)
- **what we actively serve and query** (`$SPACEGATE_STATE_DIR/served/`)
- **what we log and validate** (`$SPACEGATE_STATE_DIR/reports/`)

Nothing in this file is aspirational. Everything here reflects current v0 reality.

---

## Directory semantics (normative)

### `$SPACEGATE_STATE_DIR/raw/` — immutable inputs

**Purpose**: Preserve exact upstream artifacts.

- Files are downloaded verbatim from authoritative sources
- Never edited by hand
- Only written by downloader scripts
- Can always be re-fetched from source URLs
- `$SPACEGATE_STATE_DIR/raw/` is runtime state and is not tracked in git

```
data/
├── raw/
│   ├── <catalog>/
```

### `$SPACEGATE_STATE_DIR/cooked/` — assembled, normalized, file-based products

**Purpose**: Deterministic preparation for ingestion.

- Built exclusively from `$SPACEGATE_STATE_DIR/raw/`
- Still catalog-shaped (CSV, FITS-derived tables, etc.)
- No joins across catalogs
- No inference or enrichment

Everything in `$SPACEGATE_STATE_DIR/cooked/` is disposable and regenerable.

```
data/
├── cooked/
│   ├── <catalog>/
```

### `$SPACEGATE_STATE_DIR/served/` — queryable data products

**Purpose**: Efficient consumption.

- Built exclusively from `$SPACEGATE_STATE_DIR/cooked/`
- Optimized for querying, filtering, joining
- Used by applications, analysis, visualization
- `served/current` is a symlink to the promoted `$SPACEGATE_STATE_DIR/out/<build_id>/` directory

Formats include DuckDB and Parquet.

```
data/
├── served/
│   ├── current -> ../out/<build_id>/
```

### `$SPACEGATE_STATE_DIR/reports/` — logs, QC, and download manifests

**Purpose**: Record download provenance and QC outputs.

- Build reports live under `$SPACEGATE_STATE_DIR/reports/<build_id>/`
- Download manifests live under `$SPACEGATE_STATE_DIR/reports/manifests/`

---

## Manifests

Manifests live in:

```
$SPACEGATE_STATE_DIR/reports/manifests/
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

`dest_path` is relative to `$SPACEGATE_STATE_DIR`.

Manifests are rewritten when download scripts are re-run.

Manifests are generated locally and are not tracked in git.

---

## Core catalogs (v0)

These sources are required for v0 ingestion and must always be present.

### 1. AT-HYG (Astronexus HYG / AT-HYG)

**Authority**: Astronexus / HYG Database

**Raw inputs**:
- `$SPACEGATE_STATE_DIR/raw/athyg/athyg_v33-1.csv.gz`
- `$SPACEGATE_STATE_DIR/raw/athyg/athyg_v33-2.csv.gz`

**Source URL**:

- `https://codeberg.org/astronexus/athyg`

**Cooked outputs**:
- `$SPACEGATE_STATE_DIR/cooked/athyg/athyg.csv.gz` (concatenated, column-normalized)

**Notes**:
- AT-HYG files are stored via Git LFS on Codeberg; the downloader resolves LFS pointers.

**Format**: gzipped CSV

**Contents**:
- Stellar positions
- Distances
- Identifiers (HIP, HD, Gaia where available)
- Photometry and spectral types

**Download script**: `scripts/download_core.sh`

**Manifest**: `$SPACEGATE_STATE_DIR/reports/manifests/core_manifest.json`

---

### 2. NASA Exoplanet Archive (TAP export)

**Authority**: NASA Exoplanet Archive

**Raw input**:
- `$SPACEGATE_STATE_DIR/raw/nasa_exoplanet_archive/pscomppars.csv`

**Source URL**:
- `https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query=select+*+from+pscomppars&format=csv`

**Cooked output**:
- `$SPACEGATE_STATE_DIR/cooked/nasa_exoplanet_archive/pscomppars_clean.csv`

**Format**: CSV

**Contents**:
- Confirmed exoplanets
- Host star identifiers
- Orbital parameters
- Planetary mass/radius where available

**Acquisition method**:
- TAP synchronous query
- Table: `pscomppars`

**Download script**: `scripts/download_core.sh`

**Manifest**: `$SPACEGATE_STATE_DIR/reports/manifests/core_manifest.json`

---

## Optional catalogs (packs, v2.1+)

No optional pack sources are active yet. This section will be updated when sources are approved and added.

---

### Variable Stars — AAVSO International Variable Star Index (VSX)

**Authority**: AAVSO

**Raw input**:
- `$SPACEGATE_STATE_DIR/raw/vsx/vsx.dat.gz`

**Source URL**:
- `ftp://cdsarc.u-strasbg.fr/pub/cats/B/vsx/vsx.dat.gz` (CDS Mirror)

**Cooked output**:
- `$SPACEGATE_STATE_DIR/cooked/vsx/variables_classified.csv`

**Format**: CSV

**Contents**:
- Variability types (Flare stars, Cepheids, Eclipsing Binaries)
- Periodicity and amplitude
- "Hazard" metadata for systems

---

### Supernova Remnants — Green’s Catalogue of Galactic SNRs

**Authority**: MRAO Cambridge (D.A. Green)

**Raw input**:
- `$SPACEGATE_STATE_DIR/raw/snr/snrs.list`

**Source URL**:
- `https://www.mrao.cam.ac.uk/surveys/snrs/snrs.list`

**Cooked output**:
- `$SPACEGATE_STATE_DIR/cooked/snr/snr_boundaries.csv`

**Format**: Fixed-width text

**Contents**:
- Galactic coordinates of supernova remnants
- Angular size (extent)
- Type (Shell, Plerion, Composite)

---

### Pulsars — ATNF Pulsar Catalogue

**Authority**: Australia Telescope National Facility (ATNF)

**Raw input**:
- `$SPACEGATE_STATE_DIR/raw/atnf/psrcat_pkg.tar.gz`

**Source URL**:
- `https://www.atnf.csiro.au/research/pulsar/psrcat/downloads/psrcat_pkg.tar.gz`

**Cooked output**:
- `$SPACEGATE_STATE_DIR/cooked/atnf/pulsars_clean.csv`

**Format**: tar.gz package

**Contents**:
- Pulsar positions
- Periods, derivatives
- Distance estimates where available

---

### Magnetars — McGill Online Magnetar Catalog

**Authority**: McGill University

**Raw input**:
- `$SPACEGATE_STATE_DIR/raw/magnetar/TabO1.csv`

**Source URL**:
- `http://www.physics.mcgill.ca/~pulsar/magnetar/TabO1.csv`

**Cooked output**:
- `$SPACEGATE_STATE_DIR/cooked/magnetar/magnetars_clean.csv`

**Format**: CSV

**Contents**:
- Magnetar positions
- Spin periods and Pdot
- Distance estimates and references

---

### Ultracool dwarfs — UltracoolSheet

**Authority**: UltracoolSheet Team

**Raw input**:
- `$SPACEGATE_STATE_DIR/raw/ultracoolsheet/UltracoolSheet - Main.csv`

**Source URL**:
- `http://bit.ly/UltracoolSheet` (Redirects to Google Sheet export)

**Cooked output**:
- `$SPACEGATE_STATE_DIR/cooked/ultracoolsheet/ultracool_main_clean.csv`

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
- `$SPACEGATE_STATE_DIR/raw/gaia_ucd/table4.dat.gz`

**Source URL**:
- `ftp://cdsarc.u-strasbg.fr/pub/cats/J/A+A/657/A69/table4.dat.gz`

**Cooked output**:
- `$SPACEGATE_STATE_DIR/cooked/gaia_ucd/gaia_ucd_clean.csv`

**Format**: fixed-width text table

**Contents**:
- Gaia DR3 ultracool dwarf sample
- Gaia source identifiers

---

### White dwarfs — Gaia EDR3 WD Catalogue (Gentile Fusillo et al. 2021)

**Authority**: University of Warwick / MNRAS

**Raw input**:
- `$SPACEGATE_STATE_DIR/raw/white_dwarf/gaiaedr3_wd_main.fits.gz`

**Source URL**:
- `https://warwick.ac.uk/fac/sci/physics/research/astro/research/catalogues/gaiaedr3_wd_main.fits.gz`

**Cooked output**:
- `$SPACEGATE_STATE_DIR/cooked/white_dwarf/white_dwarfs_clean.csv`

**Format**: FITS

**Contents**:
- Gaia EDR3 white dwarf candidates
- Astrometry and photometry

---

### DwarfArchives (conditional)

**Authority**: DwarfArchives.org

**Raw input (when enabled)**:
- `$SPACEGATE_STATE_DIR/raw/dwarfarchives/dwarfarchives.data`

**Source URL**:
- `http://dwarfarchives.org/` (Requires scraping)

**Cooked output**:
- `$SPACEGATE_STATE_DIR/cooked/dwarfarchives/dwarfarchives_parsed.csv`

**Format**: upstream-defined

**Notes**:
- Download controlled by environment variable
- Treated as opaque raw blob

---

### CatWISE2020 full tiles (conditional, large)

**Authority**: IPAC / IRSA

**Raw inputs**:
- `$SPACEGATE_STATE_DIR/raw/catwise_full/<tile>/*.tbl.gz`

**Source URL**:
- `https://irsa.ipac.caltech.edu/data/WISE/CatWISE/2020/catwise_2020.html` (Base URL for file list)

**Cooked output**:
- `$SPACEGATE_STATE_DIR/cooked/catwise_full/catwise_detections.parquet` (Partitioned by tile)

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
- Log begin/end, error, complete, totals, etc. to `$SPACEGATE_STATE_DIR/logs/catalogs.log`

---

## Invariants (non-negotiable)

- `$SPACEGATE_STATE_DIR/raw/` is immutable
- `$SPACEGATE_STATE_DIR/cooked/` is fully disposable
- `$SPACEGATE_STATE_DIR/served/` depends only on `$SPACEGATE_STATE_DIR/cooked/` (via promoted `$SPACEGATE_STATE_DIR/out/<build_id>/` builds)
- No manual edits to `$SPACEGATE_STATE_DIR/raw/`
- Download provenance manifests live in `$SPACEGATE_STATE_DIR/reports/manifests/`

If a file violates these rules, it is in the wrong directory.
