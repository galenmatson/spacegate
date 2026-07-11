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

Initial data-shape probe, 2026-07-11:

- A small cone around Luhman 16 returns a CatWISE source with source name,
  source ID, W1/W2 magnitudes, SNRs, proper motion, parallax-like `par_pm`,
  artifact flags, AllWISE crossmatch count, and AllWISE match distance.
- A 10 arcsec cone around WISE 0855 returns multiple nearby CatWISE candidates,
  including the expected very red W1-W2 source. Matching must rank candidates
  using position, expected proper motion, color, SNR, artifact flags, and
  source identity, not blindly select the nearest row.
- Four 5 arcmin cone probes returned hundreds to thousands of rows per field:
  - high-latitude sample: 1574 rows; 1158 clean W1/W2 SNR>5 rows; 21 red
    W1-W2>0.8 rows; 9 red+motion rows
  - mid-latitude sample: 656 rows; 387 clean rows; 76 red rows; 35 red+motion
    rows
  - galactic-plane sample: 1755 rows; 1111 clean rows; 17 red rows; 9
    red+motion rows
  - Luhman 16 field: 1699 rows; 1246 clean rows; 24 red rows; 16 red+motion
    rows
- These samples rule out naive broad-cone querying around every Spacegate
  object as a default build path. Even basic red-color plus motion cuts produce
  far too many candidates for automatic core promotion.
- CatWISE parallax-like fields (`par_pm`, `par_stat`) are useful candidate
  evidence but are not Gaia-grade distance authority. Treat them as ARM
  evidence/diagnostics until corroborated by vetted literature or stronger
  astrometry.

### AllWISE

Primary role:

- four-band WISE photometry (`W1`, `W2`, `W3`, `W4`)
- independent infrared identity/photometry support
- useful for source confirmation and colors, less ideal than CatWISE for
  precision proper motions

Scale:

- approximately 747 million source rows
- too large for direct default core ingest

Initial data-shape probe, 2026-07-11:

- A 30 arcsec cone around Luhman 16 returns AllWISE designations, W1/W2/W3/W4
  photometry, per-band SNRs, apparent motion, photometric quality flags,
  contamination flags, extension flags, and blend/deblend fields.
- AllWISE should therefore be used first as four-band infrared photometry,
  source identity, and image-era cross-reference evidence. CatWISE2020 remains
  the better first source for motion-based ultracool candidate discovery.

### WISE Images

Primary role:

- system-page sky context and science-source affordance
- infrared discovery/explanation visuals for ultracool dwarfs, dusty systems,
  debris disks, and crowded fields
- future multi-wavelength sky context for the 3D map and concept pages

Source surfaces:

- IRSA SIA v2 image search endpoint
- IRSA Image Server cutouts
- AllWISE Atlas images and, where useful, NEOWISE-R products
- AWS-hosted public WISE products where appropriate

Initial policy:

- pre-cache WISE cutouts/composites for public UX goldens and top-coolness
  systems
- lazy-load and cache on first view for other systems
- keep the cache capped and retention-managed; default target should be a few
  GiB, not an unbounded image mirror
- store generated web images outside the repo, preferably under
  `/mnt/space/spacegate` for larger caches or `/data/spacegate/state` for
  smaller presentation artifacts
- preserve source URL, collection, band, center, cutout size, retrieval time,
  and required attribution metadata
- link visible image panels back to IRSA

Cutout feasibility:

- IRSA SIA v2 returns AllWISE image metadata with direct `access_url` fields.
- IRSA cutouts can be requested by appending `center=<ra>,<dec>deg` and
  `size=<angle>` query parameters to FITS URLs served by `/ibe/data/`.
- Spacegate can convert FITS cutouts into web-friendly PNG/JPEG/WebP previews
  while retaining original FITS metadata for evidence/debug views.
- A first implementation should cache small W1/W2/W3 false-color products
  rather than full Atlas frames.

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
- WISE/CatWISE/AllWISE cross-references for existing Spacegate objects,
  especially public UX goldens, planet hosts, multistar systems, ultracool
  objects, compact objects, and high-coolness systems

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
- source designation/source name, including WISE, WISEA, CWISE, CatWISE source
  IDs, and AllWISE designations
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
- keep WISE image/cutout caches capped and retention-managed

Do not make a full 1.89B-row CatWISE scan part of the normal public build.
Spacegate needs a repeatable targeted path first, then an optional heavyweight
survey mode.

## Practical Retrieval Tiers

Tier 1: Known-object cross-reference.

- Query CatWISE/AllWISE around existing Spacegate objects with appropriate
  epoch/proper-motion handling.
- Store high-confidence WISE IDs and infrared photometry as ARM evidence.
- Highest value, lowest risk first implementation.
- v1 implementation: `scripts/collect_wise_evidence.py` writes targeted cooked
  WISE source, match, photometry, and motion CSV artifacts; `scripts/build_arm.py`
  materializes them into `arm.wise_sources`,
  `arm.infrared_source_matches`, `arm.infrared_photometry`, and
  `arm.infrared_motion_evidence`; `scripts/verify_wise_evidence.py` checks that
  WISE rows remain ARM evidence and do not leak into core source catalogs.

Tier 2: Priority imagery.

- Pre-cache WISE cutouts for top-coolness systems and public UX goldens.
- Lazy-cache additional system-page requests with a bounded local cache.
- Directly improves the public product without requiring a huge catalog ingest.
- v1 implementation: `/api/v1/systems/{system_id}/infrared` and
  `/api/v1/systems/{system_id}/infrared/preview.png` lazily query IRSA SIA/IBE,
  generate W1/W2/W3 false-color PNG previews, preserve source-product links,
  and enforce the bounded WISE image cache.

Tier 3: Candidate queue.

- Run selective CatWISE color/motion searches for nearby ultracool candidates.
- Emit review queues with candidate ranking, quality flags, and crossmatch
  diagnostics.
- Do not auto-promote candidates without stronger corroborating evidence.
- v1 implementation: targeted known-object cone queries emit a narrow
  `infrared_candidate_queue.csv` and `arm.infrared_candidate_queue` row when
  red W1-W2 color, high apparent motion, sufficient W2 SNR, and clean artifact
  flags suggest an ultracool/brown-dwarf candidate. This is a scaffold for
  review, not a full all-sky candidate search.

Tier 4: Heavy survey mode.

- Optional tile-level CatWISE/AllWISE mirror or cloud query workflow for deep
  research builds.
- Disabled in normal public builds and documented as hardware/storage
  intensive.

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
- WISE/CatWISE/AllWISE identifiers copy with prefixes and are attached to the
  correct target level
- source photometry is ARM evidence, not core fact
- promoted rows keep source catalog and retrieval metadata
- WISE image panels link back to IRSA and retain retrieval metadata

## Open Questions

- Should Spacegate mirror selected CatWISE/AllWISE tiles for reproducibility, or
  rely on IRSA/cloud access with manifest pinning?
- Which color/proper-motion cuts are conservative enough for an automatic
  candidate queue?
- Should the eventual full survey cache live on the USB bulk SSD or a dedicated
  internal volume?
- What minimum evidence allows a non-Gaia infrared source into accepted core
  inventory?
- Should WISE PNG/WebP image derivatives be generated during builds, or lazily
  generated on first request and retained with a cache cap?
