# Spacegate Data Sources (Gaia-First)

This document defines active, optional, and transitional data sources for Spacegate.

It is normative for:

- downloader/cooker behavior
- source provenance requirements
- security/transport policy

## Directory Semantics

Within `$SPACEGATE_STATE_DIR`:

- `raw/`: immutable upstream snapshots
- `cooked/`: deterministic source-shaped typed outputs
- `out/<build_id>/`: immutable build artifacts
- `served/current`: promoted build pointer
- `reports/manifests/`: source retrieval manifests
- `reports/<build_id>/`: build QC/provenance reports

## Layer Terminology

Spacegate layer names:

- `galaxy`: canonical immutable full astronomy inventory
- `core`: canonical immutable fast astronomy projection
- `halo`: canonical immutable opt-in astronomy projection (complement to core)
- `bulge`: immutable supplemental science artifacts (legacy alias: `aux`)
- `disc`: reproducible derived artifacts (legacy alias: `rich`)
- `rim`: editable worldbuilding overlays (legacy alias: `lore`)

Compatibility note:

- current runtime artifact names still use legacy `rich`/`lore` paths in several scripts.
- naming migration should preserve backward compatibility until scripts/services are fully moved.

## Source Classification

Each source is classified as one of:

1. `canonical`:
   - defines canonical inventory fields
2. `auxiliary`:
   - enriches IDs, hierarchy, or confidence
3. `transitional`:
   - temporary migration support
4. `deferred`:
   - intentionally not in default ingest path

## Mandatory Retrieval Metadata

All downloader-manifest entries must include:

- `source_name`
- `url`
- `dest_path`
- `retrieved_at`
- `checked_at`
- `bytes_written`
- `sha256` and/or integrity equivalent (etag/retrieval tag)

## Core Canonical Sources

## 1) Gaia DR3 (`gaia_source`)

Classification: `canonical`

Role:

- canonical star inventory substrate
- canonical astrometry and photometry fields

Required policy:

- `<1000 ly` boundary from parallax policy
- explicit quality tiers (`parallax_over_error`, `ruwe`, etc.)
- canonical epoch/frame metadata in `build_metadata`

Source endpoint:

- ESA Gaia Archive TAP
- `https://gea.esac.esa.int/tap-server/tap/sync`

## 2) NASA Exoplanet Archive (`pscomppars`)

Classification: `canonical` (for current confirmed exoplanet layer)

Role:

- planet records and planetary parameters
- host matching against canonical stars/systems

Source endpoint:

- `https://exoplanetarchive.ipac.caltech.edu/TAP/sync?...`

## Core Auxiliary Multiplicity Sources

## 3) Gaia DR3 NSS support extracts

Classification: `auxiliary`

Role:

- star-level multiplicity evidence
- hierarchy confidence support

Datasets:

- `non_single_star`
- `nss_two_body_orbit`

Source endpoint:

- ESA Gaia Archive TAP

## 4) WDS (Washington Double Star)

Classification: `auxiliary`

Role:

- broad multiplicity evidence and grouping support

Policy:

- WDS-based grouping from bridge paths is confidence-gated
- default production path keeps conservative thresholds

Source endpoint:

- USNO/GSU WDS published data

## 5) ORB6

Classification: `auxiliary`

Role:

- orbit-quality support evidence for multiplicity confidence

Source endpoint:

- USNO ORB6 export

## Optional/Deferred Multiplicity Sources

## 6) MSC (Tokovinin Multiple Star Catalog)

Classification: `auxiliary` (optional, default-off)

Role:

- explicit hierarchy candidate source

Policy:

- keep optional (`SPACEGATE_ENABLE_MSC=1` to enable)
- quantify contribution in comparison reports

Security/transport note:

- historical retrieval context requires explicit caution
- do not make default production path depend on insecure transport

## Transitional Sources

## 7) AT-HYG

Classification: `transitional`

Role:

- migration compatibility for names/legacy crosswalk ergonomics only

Not allowed in target state:

- AT-HYG defining canonical star inventory existence

Retirement condition:

- remove when replacement crosswalk/naming coverage and benchmark quality gates are satisfied.

## Deferred Sources

Examples:

- BDB/ILB and other non-mirrored high-risk dependencies

Policy:

- no default dependency on sources lacking stable mirror/integrity strategy

## Current Manifest Files

Typical manifest files:

- `reports/manifests/core_manifest.json`
- `reports/manifests/gaia_nss_manifest.json`
- `reports/manifests/wds_manifest.json`
- `reports/manifests/orb6_manifest.json`
- `reports/manifests/msc_manifest.json` (when enabled)
- `reports/manifests/wds_gaia_xmatch_manifest.json` (when enabled)

## WDS-Gaia Bridge Policy

Bridge source:

- CDS XMatch (`vizier:B/wds/wds` -> `vizier:I/355/gaiadr3`)

Classification: `auxiliary` (optional/default-off)

Grouping policy:

- multi-member WDS groups must pass physical consistency gates before grouping:
  - distance spread threshold
  - proper-motion spread threshold
  - angular distance threshold

This path remains optional while false-positive/false-negative tradeoffs are actively tuned.

## Security Requirements

1. Source integrity evidence must be recorded in manifests.
2. If transport is insecure or unreliable, source must be optional or mirrored.
3. Production default ingest must avoid fragile/insecure dependencies.
4. License and redistribution constraints must be documented per source family.

## Provenance Expectations by Build

Each served row in `core` must map back to source lineage:

- source family
- source version snapshot
- retrieval metadata
- transform version

Any missing required provenance is a build failure.

## Notes on Storage Planning

Gaia-first builds at `<1000 ly` are multi-million-row scale.

Operational expectations:

- plan storage for backbone + product slice + reports + backups
- avoid root-disk-bound state paths for large runs
- keep retention policy explicit (build count, backup cadence, archive compression)
