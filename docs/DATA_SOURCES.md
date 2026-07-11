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

- `core`: canonical immutable served astronomy inventory/projection
- `arm`: immutable supplemental science artifacts
- `disc`: reproducible derived artifacts
- `rim`: editable worldbuilding overlays

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

## Operational Source Policy Matrix

Interpretation note:

- this matrix reflects the Gaia-first production profile (`SPACEGATE_ENABLE_GAIA_BACKBONE=1`), not every legacy code path.

| Source family | Policy status | Default state | Primary toggle(s) | Why |
| --- | --- | --- | --- | --- |
| Gaia DR3 backbone (`gaia_source`) | mandatory | on in Gaia-first profile | `SPACEGATE_ENABLE_GAIA_BACKBONE=1` | canonical star inventory substrate |
| Sol authority bootstrap (`sol_authority`) | mandatory (S1/S2 release gate) | on | `SPACEGATE_ENABLE_SOL_AUTHORITY` | guarantees Sol/Sun/major-planet coverage plus arm moon/barycenter hierarchy from authoritative JPL source |
| Sol artificial overlay (`sol_artificial`) | default-on (`arm` overlay) | on | `SPACEGATE_ENABLE_SOL_ARTIFICIAL` | curated Sol stations/probes/orbiters with freshness windows for arm/UI overlays |
| NASA Exoplanet Archive (`ps` / `pscomppars`) | mandatory | on | (always in core catalog set) | canonical planet baseline; use `ps` for source-specific solutions and `pscomppars` for display/default composites |
| MSC | mandatory | on | `SPACEGATE_ENABLE_MSC` (must remain `1`) | required multiplicity hierarchy evidence; ingest blocks when off |
| WDS | mandatory (current default science ingest) | on | (always in Gaia-first core catalog set) | broad multiplicity support evidence |
| ORB6 | mandatory (current default science ingest) | on | (always in Gaia-first core catalog set) | orbit-quality support evidence |
| Gaia class probabilities | default-on | on | `SPACEGATE_ENABLE_GAIA_CLASSPROB` | remnant-safe classification guardrails |
| Gaia UCD memberships (`J/A+A/669/A139 table4`) | default-on | on | `SPACEGATE_ENABLE_GAIA_UCD` | ultracool dwarf cluster/membership tags (HMAC/BANYAN) for star enrichment |
| VSX variability index | default-on | on | `SPACEGATE_ENABLE_VSX` | variable-star evidence overlay in `arm` (exact Gaia joins only) |
| UltracoolSheet | default-on | on | `SPACEGATE_ENABLE_ULTRACOOLSHEET` | ultracool/youth/kinematics overlay in `arm` (Gaia DR3/DR2 linked) |
| Gaia NSS | default-on | on | `SPACEGATE_ENABLE_GAIA_NSS` | Gaia-linked multiplicity evidence |
| SBX (ULB spectroscopic binaries) | default-on | on | `SPACEGATE_ENABLE_SBX` | spectroscopic-binary multiplicity evidence via exact Gaia/HIP/HD joins |
| DEBCat | default-on | on | `SPACEGATE_ENABLE_ECLIPSING_CATALOGS` | eclipsing-binary enrichment/validation |
| Kepler EB catalog | deferred (optional) | off | `SPACEGATE_ENABLE_KEPLER_EB` (with `SPACEGATE_ENABLE_ECLIPSING_CATALOGS=1`) | Kepler-era eclipsing support with low in-slice linkage in Gaia-first core |
| Compact-object bundle (`ATNF`, `magnetar`, `white_dwarf`) | default-on | on | `SPACEGATE_ENABLE_COMPACT_OBJECT_CATALOGS` | compact/remnant support evidence (includes cooked+ingested Gaia EDR3 white-dwarf catalog) |
| Superstellar bundle (`clusters`, `snr`) | default-on | on | `SPACEGATE_ENABLE_SUPERSTELLAR_CATALOGS` | open-cluster and remnant-nebula context |
| Exoplanet lifecycle support (`exoplanet.eu`, OEC, HWC) | optional | off | `SPACEGATE_ENABLE_EXOPLANET_LIFECYCLE_CATALOGS` | status overlays and derived-tag support; OEC alias bridge improves lifecycle matching; canonical planets stay NASA-rooted |
| WDS↔Gaia bridge (`wds_gaia_xmatch`) | optional | off | `SPACEGATE_ENABLE_WDS_GAIA_XMATCH` | useful for bridge experiments, but confidence-gated and conservative by default |
| Proximity grouping | optional runtime behavior | off | `SPACEGATE_ENABLE_PROXIMITY` | nondefault to avoid weak/inexact grouping in production |
| AT-HYG | transitional | alias crosswalk on by default; supplement merge opt-in | `SPACEGATE_ENABLE_ATHYG_ALIAS_CROSSWALK`, `SPACEGATE_ENABLE_ATHYG_SUPPLEMENT_MERGE` | migration compatibility for names/legacy IDs; not canonical inventory authority |
| BDB/ILB-like non-mirrored sources | deferred/disregarded for default ingest | off | n/a | high-risk dependency until mirrored + integrity-pinned |
| SB9 | disregarded for default ingest/eval | off | n/a | superseded by SBX policy |
| TESS EB catalog (Villanova) | default-on | on | `SPACEGATE_ENABLE_TESS_EB` | eclipsing/variability expansion beyond Kepler with deterministic paginated export capture |

### Exoplanet Lifecycle Notes

- `exoplanet.eu`: status overlay source; currently observed as predominantly/entirely confirmed in recent snapshots.
- `HWC`: habitability reference and supplemental planet evidence; used as non-canonical feature support.
- `OEC`: alias/crosswalk source for lifecycle matching; improves match coverage by resolving naming drift across catalogs.
- `EMAC TT9`: removed from active ingest pipeline because current endpoint is a resource page without deterministic bulk candidate rows.

### 2026 Source-Refresh Watchlist

- Gaia DR3 remains the current Spacegate backbone; Gaia DR4 is scheduled for
  December 2, 2026 and should become an explicit transition milestone.
- MSC is mandatory hierarchy evidence. Spacegate now targets the upstream June
  19, 2026 archive (`newmsc-20260619.tar.gz`) and verifies insecure fallback
  downloads with an explicit SHA-256 pin when CTIO TLS is not usable. Local
  canonical build `20260628T1210Z_msc20260619` was promoted on June 28, 2026
  after passing required multiplicity golden checks. Local bootstrap mirror
  snapshot `20260628T1210Z_msc20260619` now contains refreshed MSC raw and
  cooked artifacts; sync this mirror to `spacegates.org` during the next public
  deployment.
- SBX is the active spectroscopic-binary support path; keep SB9 as historical
  context only unless an explicit regression/comparison task needs it.
- WDS and ORB6 remain default visual-binary support sources, but ORB6 rows must
  only attach to unique, confidence-gated binary edges.
- JPL Horizons/SBDB remain the Sol-system orbital authority path for volatile
  small-body and satellite data.

## Mandatory Retrieval Metadata

All downloader-manifest entries must include:

- `source_name`
- `url`
- `dest_path`
- `retrieved_at`
- `checked_at`
- `bytes_written`
- `sha256` and/or integrity equivalent (etag/retrieval tag)

Gaia TAP extracts (`gaia_backbone`, `gaia_classprob`, `gaia_nss`) additionally persist:

- `count_query`
- `expected_row_count`
- `row_count_match`

`row_count_match=false` indicates potential partial/truncated retrieval and should be treated as a build blocker.

## Core Canonical Sources

## 1) Gaia DR3 (`gaia_source`)

Classification: `canonical`

Role:

- canonical star inventory substrate
- canonical astrometry and photometry fields
- arm-side source-native stellar-parameter enrichment (`stellar_parameters`) for narration, filters, and uncertainty-aware inference

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
- arm-side host-star physical parameter rows for narration/evidence display when matched back to canonical stars

Source endpoint:

- `https://exoplanetarchive.ipac.caltech.edu/TAP/sync?...`

## 2b) Sol authority bootstrap (`sol_authority`)

Classification: `canonical` (Sol-specific authoritative override layer)

Role:

- ensure Sol and Sun are always present and linked
- ensure required Sol major-planet coverage independent of exoplanet catalogs
- provide S2 moon hierarchy/orbit/barycenter evidence in `arm`
- provide S3 named small-body evidence in `arm` (asteroid/TNO/comet families with staleness metadata)
- provide deterministic Sol provenance for release gating

Source endpoint:

- JPL Horizons API: `https://ssd.jpl.nasa.gov/api/horizons.api`

Implementation:

- downloader: `scripts/fetch_sol_authority.py`
- contract doc: `docs/SOL_AUTHORITY.md`

## 2c) Sol artificial overlay (`sol_artificial`)

Classification: `auxiliary` (`arm` supplemental science overlay)

Role:

- provide curated Sol artificial-object rows (station/probe/orbiter classes) in `arm`
- support Sol hierarchy/UI evidence without polluting `core` canonical inventory tables
- attach explicit freshness windows + staleness fields for volatile-feed monitoring

Source endpoint:

- JPL Horizons API: `https://ssd.jpl.nasa.gov/api/horizons.api`

Implementation:

- downloader: `scripts/fetch_sol_artificial.py`
- volatile refresh runbook: `scripts/refresh_sol_volatile.sh`
- freshness monitor: `scripts/report_sol_volatile.py`

## 3) Gaia DR3 astrophysical classifier probabilities

Classification: `canonical` (classification safety support for canonical stars)

Role:

- remnant-safe classification support (`classprob_dsc_*_whitedwarf` and related families)
- prevents temperature fallback from mislabeling remnant objects

Source endpoint:

- ESA Gaia Archive TAP (`gaiadr3.astrophysical_parameters`)

## 3b) Gaia ultracool dwarf memberships (`J/A+A/669/A139`, `table4`)

Classification: `auxiliary`

Role:

- Gaia DR3 source-level ultracool dwarf support tags
- cluster/membership enrichment fields (`HMACcl`, `BANYANcl`, `BANYANprob`)
- evidence-only enrichment; does not define canonical star existence

Source endpoint:

- CDS HTTPS mirror (`https://cdsarc.cds.unistra.fr/ftp/J/A+A/669/A139/table4.dat`)

## Core Auxiliary Multiplicity Sources

## 4) Gaia DR3 NSS support extracts

Classification: `auxiliary`

Role:

- star-level multiplicity evidence
- hierarchy confidence support

Datasets:

- `non_single_star`
- `nss_two_body_orbit`

Source endpoint:

- ESA Gaia Archive TAP

## 5) WDS (Washington Double Star)

Classification: `auxiliary`

Role:

- broad multiplicity evidence and grouping support

Policy:

- WDS-based grouping from bridge paths is confidence-gated
- default production path keeps conservative thresholds

Source endpoint:

- USNO/GSU WDS published data

## 6) ORB6

Classification: `auxiliary`

Role:

- orbit-quality support evidence for multiplicity confidence

Source endpoint:

- USNO ORB6 export

## 7) DEBCat (Detached Eclipsing Binary Catalogue)

Classification: `auxiliary`

Role:

- high-quality detached eclipsing binary physical parameter table
- benchmark and enrichment support for orbital/mass/radius validation

Source endpoint:

- https://www.astro.keele.ac.uk/jkt/debcat/debs.dat

## 8) Kepler Eclipsing Binary Catalog

Classification: `deferred` (optional)

Role:

- large eclipsing-binary candidate/phenomenology dataset (period, morphology, KIC IDs)
- supplementary evidence set for binary behavior and follow-up crossmatching

Policy:

- default-off in Gaia-first production profile
- opt-in only when explicit Kepler-focused analysis is needed (`SPACEGATE_ENABLE_KEPLER_EB=1`)
- rationale: current Gaia-slice overlap/linkage is low, so default ingest cost is not justified

Source endpoint:

- https://keplerebs.villanova.edu/ (CSV export workflow)

## 9) MSC (Tokovinin Multiple Star Catalog)

Classification: `auxiliary` (mandatory)

Role:

- explicit hierarchy candidate source

Policy:

- required in default science ingest and multiplicity derivation
- ingest/promotion fail if MSC retrieval/cook/manifest lineage is missing
- still quantify contribution in comparison reports for observability

Security/transport note:

- historical retrieval context requires explicit caution
- preferred mirror path, when published on the public bootstrap host, is `SPACEGATE_PUBLIC_BASE_URL/dl/catalogs/current/raw/msc/newmsc-20260619.tar.gz`
- default code should support overriding MSC retrieval to that mirror without changing source provenance
- preserve the CTIO/NOIRLab MSC export URL as authoritative source provenance
- maintain mirrored/pinned retrieval strategy for production stability

## 10) VSX (AAVSO Variable Star Index)

Classification: `auxiliary`

Role:

- variable-star observational overlay (type/family/amplitude/period)
- confidence-tiered variability summaries for narrative and query filtering
- stored in `arm` to preserve core query performance

Source endpoint:

- CDS HTTPS mirror (`https://cdsarc.cds.unistra.fr/ftp/B/vsx/vsx.dat`)

## 11) UltracoolSheet

Classification: `auxiliary`

Role:

- ultracool object metadata and youth indicators
- Gaia DR3/DR2-linked arm overlay for detailed UCD context
- supports later disc enrichment without widening core hot-path tables
- known limitation: ARM overlay rows are not currently accepted inventory
  roots when the Gaia backbone misses them. Nearby non-Gaia or unlinked
  brown dwarfs such as WISE 0855-0714, and Gaia-ID-bearing rows absent from
  the current Gaia backbone such as Luhman 16, require a dedicated vetted
  ultracool-inventory promotion pass.

Source endpoint:

- Google Sheets published CSV endpoint (pinned URL in `scripts/catalogs.sh`)

## Transitional Sources

## 12) AT-HYG

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

## Additional Orbital Repositories (Evaluation Queue)

These are credible orbital-parameter repositories not yet wired into default ingest:

1. Additional TESS follow-on mission catalogs (beyond current TESS EB search export)
   - role candidate: post-Kepler variability and cadence expansion
   - status: evaluate once deterministic bulk export + licensing path is pinned

SB9 policy:

- SB9 is superseded by SBX and is not targeted for default ingest/evaluation while SBX remains available.
- SB9 may be referenced only for historical reproducibility checks.

## Current Manifest Files

Typical manifest files:

- `reports/manifests/core_manifest.json`
- `reports/manifests/gaia_backbone_manifest.json`
- `reports/manifests/gaia_classprob_manifest.json`
- `reports/manifests/gaia_nss_manifest.json`
- `reports/manifests/sbx_manifest.json`
- `reports/manifests/wds_manifest.json`
- `reports/manifests/orb6_manifest.json`
- `reports/manifests/debcat_manifest.json`
- `reports/manifests/kepler_eb_manifest.json`
- `reports/manifests/tess_eb_manifest.json`
- `reports/manifests/msc_manifest.json` (required)
- `reports/manifests/wds_gaia_xmatch_manifest.json` (when enabled)
- `reports/manifests/atnf_manifest.json`
- `reports/manifests/magnetar_manifest.json`
- `reports/manifests/clusters_manifest.json`
- `reports/manifests/snr_manifest.json`
- `reports/manifests/vsx_manifest.json`
- `reports/manifests/ultracoolsheet_manifest.json`

Source-delta tracking files:

- `reports/source_delta_report.json` (latest per-source delta summary)
- `reports/source_delta_snapshot.json` (current baseline signatures)
- `reports/source_delta_history/*.json` (run history)
- `reports/impacted_rows_plan.json` (domain/row impact plan + execution mode)

Differential refresh scripts:

- `scripts/scan_source_deltas.py` (manifest snapshot diff)
- `scripts/plan_impacted_rows.py` (impact planner + mode routing)
- `scripts/cook_delta.sh` (selective cook for planet/lifecycle-only deltas)
- `scripts/ingest_incremental_planets.py` (incremental rebuild of planets + lifecycle side tables)
- `scripts/refresh_core.sh` (end-to-end orchestrator: differential or full path + promote + verify)

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
2. If transport is insecure or unreliable, source must be mirrored/pinned before default dependency.
3. Production default ingest must avoid fragile/insecure dependencies.
4. License and redistribution constraints must be documented per source family.

## Catalog Mirror Workflow (spacegates bootstrap)

Mirror target:

- `$SPACEGATE_DL_ROOT/catalogs` (auto-default: `/data/spacegate/dl` when `/data/spacegate` exists, else `/srv/spacegate/dl`)

Host env requirement:

- set `SPACEGATE_STATE_DIR` and `SPACEGATE_DL_ROOT` in the server host env files (`/etc/spacegate/spacegate.env`, `.spacegate.env`, or `.spacegate.local.env`)
- do not rely on `docker-compose.yml` alone for these values; host-side publishers/promoters also read them through `scripts/lib/env_loader.sh`

Snapshot publisher:

- `scripts/publish_catalog_mirror.py`

Recommended run sequence after successful ingest/promote/verify:

```bash
scripts/publish_catalog_mirror.py
scripts/publish_db.sh
```

Behavior:

- publishes immutable snapshot at `dl/catalogs/snapshots/<snapshot_id>/`
- updates `dl/catalogs/current` symlink and `dl/catalogs/current.json`
- preserves **original raw upstream artifacts** exactly as downloaded
- publishes **cooked Spacegate-normalized artifacts** as convenience layer for downstream bootstrap users

Operational rule:

- never replace raw artifacts with cooked variants; raw remains canonical provenance evidence.

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
