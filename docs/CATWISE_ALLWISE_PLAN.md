# CatWISE / AllWISE Integration Plan

Purpose: close Spacegate's brown-dwarf and infrared-source blind spots without
turning the canonical star inventory into an unreviewed all-sky infrared source
dump.

## Source Roles

### CatWISE2020

Primary role:

- high-proper-motion and faint ultracool/brown-dwarf discovery support
- infrared photometry and motion evidence for objects weak or absent in Gaia
- crossmatch support for nearby-census completeness

Source surfaces:

- IRSA CatWISE2020 contributed-product catalog
- IRSA bulk/catalog services
- NERSC-hosted CatWISE2020 files where appropriate

Scale:

- approximately 1.89 billion all-sky source rows
- too large for default core ingest
- must be tiled, filtered, or staged as an evidence artifact

### AllWISE

Primary role:

- four-band WISE photometry (`W1`, `W2`, `W3`, `W4`)
- independent infrared identity/photometry support
- useful for source confirmation and colors, less ideal than CatWISE for
  precision proper motions

Scale:

- approximately 747 million source rows
- too large for direct default core ingest

## Layer Policy

Default policy:

- raw survey rows do not enter `core`
- survey catalogs are preserved as source/evidence artifacts
- normalized crossmatch evidence belongs in `arm`
- reviewed nearby brown-dwarf promotions may enter `core` only through a
  deterministic accepted-inventory bridge with provenance

Promotion candidates:

- nearby L/T/Y dwarfs absent from Gaia/core
- high-proper-motion infrared sources with strong literature or vetted catalog
  support
- known public-interest objects such as Luhman 16 and WISE 0855-style systems

Non-goals:

- no all-CatWISE root-system creation in core
- no name-similarity-only merges
- no fiction/Rim data
- no unreviewed AI-generated promotions

## Proposed Pipeline

### Phase 0: Survey Planning Report

Add a script that inspects available IRSA/AWS/NERSC product manifests and emits:

- source product versions
- file/tile counts
- expected compressed and expanded size
- available columns
- viable filter columns
- recommended local cache path under `/mnt/space/spacegate` or `/data/spacegate`

Output:

- `reports/<build_id>/catwise_allwise_planning_report.json`

### Phase 1: Targeted Nearby Candidate Pull

Avoid downloading all rows first. Query or pull only candidates likely relevant
to Spacegate's nearby-star mission:

- known UltracoolSheet/Simple-BD names and coordinates
- high-proper-motion CatWISE rows
- color-selected W1/W2 brown-dwarf candidates
- objects within current Spacegate spatial volume when parallax/distance is
  available from a vetted source

Output:

- cooked candidate rows
- source manifest with retrieval checksums
- no core promotions yet

### Phase 2: ARM Evidence Tables

Create normalized ARM support tables:

- `catwise_sources`
- `allwise_sources`
- `infrared_source_matches`
- `infrared_photometry`
- `infrared_motion_evidence`
- `brown_dwarf_candidate_evidence`

Required fields:

- source catalog/version
- source row key
- RA/Dec and epoch
- proper motion if available
- WISE magnitudes/uncertainties/quality flags
- match target and match method
- angular separation
- confidence tier
- conflict status
- provenance JSON

### Phase 3: Accepted Nearby Promotion Queue

Build a deterministic review queue rather than automatic bulk promotion:

- candidates absent from core
- candidates with literature/vetted catalog support
- candidates with usable astrometry
- candidates passing duplicate/crossmatch guardrails

Output:

- `nearby_infrared_promotion_candidates`
- accepted/rejected/quarantined status
- benchmark list including Luhman 16, WISE 0855, UGPS J0722, and other nearby
  T/Y dwarfs

### Phase 4: Core Bridge Expansion

Only after Phase 3:

- promote accepted candidates into `core.stars`
- retain source catalog provenance
- preserve unresolved/composite multiplicity flags
- create no invented component hierarchy
- feed aliases into Alias Authority v2

## Storage and Performance

Recommended defaults:

- keep full survey bulk artifacts off the root filesystem
- prefer `/mnt/space/spacegate/catalogs/wise` for large raw/cache artifacts if
  the USB SSD is mounted and healthy
- keep only filtered/cooked candidate artifacts under `/data/spacegate/state`
- make full CatWISE/AllWISE downloads opt-in

Do not make a full 1.89B-row CatWISE scan part of the normal public build.
Spacegate needs a repeatable targeted path first, then an optional heavyweight
survey mode.

## Verification

Goldens:

- Luhman 16
- WISE 0855-0714
- UGPS J072227.51-054031.2
- WISE 0350-5658
- eps Ind Ba/Bb
- brown-dwarf companions already represented through MSC/WDS

Checks:

- no duplicate Gaia-backed rows
- no false core promotion from infrared-only low-confidence candidates
- exact alias resolution for common WISE forms
- source photometry is ARM evidence, not core fact
- promoted rows keep source catalog and retrieval metadata

## Open Questions

- Should Spacegate mirror selected CatWISE/AllWISE tiles for reproducibility, or
  rely on IRSA/cloud access with manifest pinning?
- Which color/proper-motion cuts are conservative enough for an automatic
  candidate queue?
- Should the eventual full survey cache live on the USB bulk SSD or a dedicated
  internal volume?
- What minimum evidence allows a non-Gaia infrared source into accepted core
  inventory?
