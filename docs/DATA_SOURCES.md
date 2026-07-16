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
| UltracoolSheet | default-on | on | `SPACEGATE_ENABLE_ULTRACOOLSHEET`, `SPACEGATE_ENABLE_NEARBY_ULTRACOOL_INVENTORY` | ultracool/youth/kinematics overlay in `arm`; narrow nearby core-inventory bridge when Gaia misses vetted UCDs |
| Gaia NSS | default-on | on | `SPACEGATE_ENABLE_GAIA_NSS` | Gaia-linked multiplicity evidence |
| SBX (ULB spectroscopic binaries) | default-on | on | `SPACEGATE_ENABLE_SBX` | spectroscopic-binary multiplicity evidence via exact Gaia/HIP/HD joins |
| SB9 (CDS `B/sb9`) | default-on ARM evidence | on | `SPACEGATE_ENABLE_SB9` | component-specific spectral types, aliases, and spectroscopic orbits; exact MSC sequence references bind evidence to graph endpoints |
| DEBCat | default-on | on | `SPACEGATE_ENABLE_ECLIPSING_CATALOGS` | eclipsing-binary enrichment/validation; unique canonical-system + period matches may bind component evidence in ARM |
| Kepler EB catalog | deferred (optional) | off | `SPACEGATE_ENABLE_KEPLER_EB` (with `SPACEGATE_ENABLE_ECLIPSING_CATALOGS=1`) | Kepler-era eclipsing support with low in-slice linkage in Gaia-first core |
| Compact-object bundle (`ATNF`, `magnetar`, `white_dwarf`) | default-on | on | `SPACEGATE_ENABLE_COMPACT_OBJECT_CATALOGS` | compact/remnant support evidence (includes cooked+ingested Gaia EDR3 white-dwarf catalog) |
| Superstellar bundle (`clusters`, `snr`) | default-on | on | `SPACEGATE_ENABLE_SUPERSTELLAR_CATALOGS` | open-cluster and remnant-nebula context |
| Extended-object bundle (OpenNGC, LBN, LDN, Barnard, Magakian, vdB, Sharpless, Cederblad) | default-on | on | `SPACEGATE_ENABLE_EXTENDED_OBJECTS` | separate catalog identity, geometry, distance, and evidence for nebulae, clusters, galaxies, and other non-stellar landmarks; see `docs/EXTENDED_OBJECTS.md` |
| Exoplanet lifecycle support (`exoplanet.eu`, OEC, HWC) | optional | off | `SPACEGATE_ENABLE_EXOPLANET_LIFECYCLE_CATALOGS` | status overlays and derived-tag support; OEC alias bridge improves lifecycle matching; canonical planets stay NASA-rooted |
| WDS↔Gaia bridge (`wds_gaia_xmatch`) | optional | off | `SPACEGATE_ENABLE_WDS_GAIA_XMATCH` | useful for bridge experiments, but confidence-gated and conservative by default |
| Proximity grouping | optional runtime behavior | off | `SPACEGATE_ENABLE_PROXIMITY` | nondefault to avoid weak/inexact grouping in production |
| AT-HYG | transitional | alias crosswalk on by default; supplement merge opt-in | `SPACEGATE_ENABLE_ATHYG_ALIAS_CROSSWALK`, `SPACEGATE_ENABLE_ATHYG_SUPPLEMENT_MERGE` | migration compatibility for names/legacy IDs; not canonical inventory authority |
| BDB/ILB-like non-mirrored sources | deferred/disregarded for default ingest | off | n/a | high-risk dependency until mirrored + integrity-pinned |
| TESS EB catalog (Villanova) | default-on | on | `SPACEGATE_ENABLE_TESS_EB` | eclipsing/variability expansion beyond Kepler with deterministic paginated export capture |
| NASA TESS Objects of Interest + targeted TIC/MAST/Gaia metadata | default-on identity + `arm` evidence | on | `SPACEGATE_ENABLE_TESS_EVIDENCE` | exact TIC/TOI lookup, missing-object audit, candidate/transit evidence, and targeted Gaia reconciliation without bulk TIC ingestion; see `docs/TESS_INTEGRATION.md` |

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
- SBX and SB9 are complementary. SBX supplies current Gaia/HIP/HD-linked
  spectroscopic-binary system evidence, while SB9 preserves primary and
  secondary component spectral types that are absent from the current SBX
  export. Neither catalog creates canonical component inventory by itself.
- A bounded canonical recovery path may use an exact unique HIP+HD agreement
  between SBX and the transitional AT-HYG crosswalk for Gaia-missing systems,
  but only with no Gaia identifier on the source rows, usable distance,
  positional sanity, and SBX orbit evidence. This is a reusable catalog rule;
  it does not make AT-HYG a canonical inventory authority and it cannot accept
  named-system exceptions.
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
- narrow accepted-inventory completeness bridge for nearby rows missing from
  the Gaia backbone. Controlled by
  `SPACEGATE_ENABLE_NEARBY_ULTRACOOL_INVENTORY` and
  `SPACEGATE_NEARBY_ULTRACOOL_INVENTORY_MAX_DIST_PC` (default 10 pc).
- examples of intended coverage: WISE 0855-0714 and Luhman 16. This bridge
  admits vetted nearby ultracool objects as `core.stars` with
  `source_catalog = 'ultracoolsheet'`; it does not expand composite or binary
  ultracool rows into invented component hierarchies.
- limitation: this is not a full WISE/CatWISE/AllWISE survey ingest. Large
  infrared survey integration remains a separate performance-planned milestone.
  See `docs/CATWISE_ALLWISE_PLAN.md`.

Source endpoint:

- Google Sheets published CSV endpoint (pinned URL in `scripts/catalogs.sh`)

Core bridge diagnostics:

- `reports/<build_id>/nearby_ultracool_inventory_report.json`
- `scripts/verify_nearby_ultracool_inventory.py --build-dir <out/build_id>`

## 12) TESS Identity and TOI Evidence

Classification: `core identity authority` plus `arm evidence`

Default toggle: `SPACEGATE_ENABLE_TESS_EVIDENCE=1`

Authoritative inputs:

- NASA Exoplanet Archive `toi` TAP table
- targeted MAST TIC rows for TOI hosts, NASA planet hosts, TESS EB targets, and
  reviewed operator/AAA seeds
- Gaia DR3 `dr2_neighbourhood`, targeted `gaia_source` rows, and targeted
  Hipparcos/Tycho-2/2MASS best-neighbor crossmatches

Artifacts:

- content-addressed raw snapshots under
  `$SPACEGATE_STATE_DIR/raw/tess_evidence/snapshots/<snapshot_id>/`
- normalized inputs and append-only disposition history under
  `$SPACEGATE_STATE_DIR/cooked/tess_evidence/`
- `reports/manifests/tess_evidence_manifest.json`
- `reports/tess_source_delta_report.json`
- per-build identity coverage, resolution, and missing-object reports

Policy:

- never bulk ingest TIC, CTL, TCE, or TESS observation products
- the targeted-universe manifest records the versioned operator seed plus the
  NASA confirmed-host and TESS EB dependency paths/checksums and per-family
  target counts, including zero-count families
- never assume Gaia DR2 and DR3 source IDs are interchangeable
- TIC artifact/split/duplicate rows remain excluded or quarantined
- TOI candidates and negative dispositions remain ARM evidence
- independently supported missing real objects remain deferred until a reusable
  canonical reconciliation rule or an inspectable adjudication record accepts
  them; a local object-specific supplement file is not sufficient

## 13) WISE / CatWISE2020 / AllWISE

Classification: `auxiliary`

Role:

- infrared identity, photometry, color, and motion support for existing
  Spacegate objects
- targeted cross-reference source for public goldens, high-coolness systems,
  planet hosts, multistars, ultracool dwarfs, compact objects, and
  AAA-promoted research targets
- IRSA-backed WISE image cutouts for system pages and future evidence
  portfolios
- conservative candidate review queue for missing nearby ultracool/brown-dwarf
  objects

Source endpoints:

- IRSA Gator CatWISE2020 catalog (`catwise_2020`)
- IRSA Gator AllWISE Source Catalog (`allwise_p3as_psd`)
- IRSA SIA / IBE AllWISE image products for W1/W2/W3 cutouts

Policy:

- CatWISE2020 and AllWISE are evidence sources, not primary core-inventory
  backbones.
- WISE-only rows must not be bulk-promoted into `core`.
- CatWISE parallax-like fields are candidate evidence only and must not be
  treated as Gaia-grade distances.
- WISE/CatWISE/AllWISE identifiers are secondary metadata unless no better
  public name exists.
- Generated image previews are lazy, bounded cache products outside the repo.

Artifacts:

- `state/cooked/wise/wise_sources.csv`
- `state/cooked/wise/infrared_source_matches.csv`
- `state/cooked/wise/infrared_photometry.csv`
- `state/cooked/wise/infrared_motion_evidence.csv`
- `state/cooked/wise/infrared_candidate_queue.csv`
- `arm.wise_sources`, `arm.catwise_sources`, `arm.allwise_sources`
- `arm.infrared_source_matches`
- `arm.infrared_photometry`
- `arm.infrared_motion_evidence`
- `arm.infrared_candidate_queue`
- runtime cache: `$SPACEGATE_STATE_DIR/cache/wise_images`

Scripts:

- `scripts/collect_wise_evidence.py`
- `scripts/verify_wise_evidence.py`

## Transitional Sources

## 14) AT-HYG

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

1. TESS rotation/time-series high-level products beyond TOI and current TESS EB
   - role candidate: rotation, activity, flare, variability, and narration
     evidence
   - status: deferred until the bounded identity/candidate/product-index goals
     in `docs/TESS_INTEGRATION.md` are complete

SB9 policy:

- acquire the CDS `B/sb9` `ReadMe`, `main.dat`, `alias.dat`, and `orbits.dat`
  together and preserve hashes in `sb9_manifest.json`
- cook source-native systems, aliases, component spectral types, and orbital
  solutions without merging them into canonical inventory
- accept component bindings only when MSC contains an exact `SB9_<sequence>`
  reference, that sequence is unique in the MSC source rows, and both MSC
  endpoints exist in the ARM component graph
- quarantine missing, ambiguous, or unresolved endpoint matches; do not fall
  back to name-only or coordinate-only component assignment
- DEBCat component evidence uses a separate unique canonical-system + period
  match and the same endpoint-existence gates

## Current Manifest Files

Typical manifest files:

- `reports/manifests/core_manifest.json`
- `reports/manifests/gaia_backbone_manifest.json`
- `reports/manifests/gaia_classprob_manifest.json`
- `reports/manifests/gaia_nss_manifest.json`
- `reports/manifests/sbx_manifest.json`
- `reports/manifests/sb9_manifest.json`
- `reports/manifests/wds_manifest.json`
- `reports/manifests/orb6_manifest.json`
- `reports/manifests/debcat_manifest.json`
- `reports/manifests/kepler_eb_manifest.json`
- `reports/manifests/tess_eb_manifest.json`
- `reports/manifests/tess_evidence_manifest.json`
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
