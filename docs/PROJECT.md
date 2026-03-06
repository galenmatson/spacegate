# Spacegate Project Plan (Gaia-First)

## Mission
Spacegate is a public astronomy and worldbuilding platform:

- scientifically grounded, provenance-preserving astronomy data
- accessible discovery UX for non-specialists
- deterministic enrichment (scores, snapshots, factsheets, exposition)
- optional fictional overlays kept separate from science data

Primary product goals:

1. Make nearby space browsable, understandable, and compelling.
2. Keep the scientific core auditable and reproducible.
3. Support advanced worldbuilding without contaminating canonical astronomy.

## Current Direction
Spacegate is moving from an AT-HYG-centric bootstrap architecture to a Gaia-first core architecture.

Reason:

- Gaia DR3 has substantially better astrometry and coverage.
- AT-HYG was useful for early velocity and UX bootstrap, but should not remain the canonical inventory substrate if better crosswalks are available.
- Continuing to treat AT-HYG as canonical risks long-term schema and ingest debt.

## Core Principles

1. Canonical inventory first, enrichment second.
2. Provenance on every served row.
3. Deterministic builds and deterministic promotion.
4. Clear layer boundaries:
   - `galaxy`: immutable canonical science corpus
   - `core`: fast default science projection
   - `halo`: explicit opt-in science projection (complement to core)
   - `bulge`: immutable supplemental science (observational side tables outside core hot paths)
   - `disc`: reproducible derivatives
   - `rim`: editable fiction
5. Explicit confidence for joins/groupings; avoid silent inference.
6. Security-first ingestion: no required insecure transport dependencies.

## Data Layers (`galaxy` / `core` / `halo` / `bulge` / `disc` / `rim`)

### Galaxy (immutable canonical astronomy)
Authoritative full-science inventory per build:

- all scientific rows retained
- full provenance contract enforced
- not the default hot-path serving tier

### Core (immutable astronomy)
Fast default serving projection for common browse/search/detail traffic.

Core is generated deterministically from `galaxy` and a versioned slice profile.

### Halo (immutable astronomy complement)
Explicit opt-in serving projection containing scientific rows excluded from core by policy.

Rules:

- `core` and `halo` are complementary projections from the same `galaxy` build
- no destructive row movement; projection rebuilds only
- same `stable_object_key` identity across tiers

### Core/Halo canonical tables

- `systems`
- `stars`
- `planets`
- `build_metadata`

Core/halo must remain free of generated prose/images/rim overlays.

### Bulge (immutable supplemental science)

- observational/support datasets that are still scientific and provenance-bound
- examples: variability families, dense diagnostics, and other non-hot-path science tables

Bulge rows follow the same immutability and provenance rules as core, but are separated to keep core performant.

### Disc (rebuildable derived artifacts)

- coolness scoring
- snapshots
- factsheets / expositions
- external links
- optional neighbor graph

Disc is always regenerable from core/bulge plus pinned generators.

### Rim (editable overlays)
User/worldbuilder entities and relationships keyed by `stable_object_key`.

### Compatibility aliases (transition period)

- `aux` -> `bulge`
- `rich` -> `disc`
- `lore` -> `rim`

Script/file names may continue using legacy `rich`/`lore` terms until runtime migration is complete.

## Gaia-First Architecture

Spacegate will use a three-tier astronomy runtime:

1. `galaxy` (canonical inventory substrate; Gaia-first)
2. `core_product_slice` (default served subset for UX/performance)
3. `halo_complement` (opt-in long-tail subset)

Catalog crosswalks and multiplicity catalogs attach to backbone IDs; they are not primary object inventory sources.

### Measured Gaia Scale (<1000 ly)
Gaia DR3 query date: March 5, 2026 (`parallax >= 3.26156 mas`)

- raw stars: `17,785,548`
- `parallax_over_error >= 5`: `9,188,313`
- `parallax_over_error >= 10`: `6,031,770`
- `parallax_over_error >= 5` and `ruwe < 1.4`: `6,681,580`
- brightness cuts:
  - `G <= 18`: `4,401,648`
  - `G <= 19`: `5,939,410`
  - `G <= 20`: `9,334,894`

Implication: this is a multi-million object architecture. Product slice and deep-query pathways must be deliberate.

## Astrometry Standard

- Canonical frame: `ICRS`
- Canonical epoch: `J2016.0`
- Build metadata must record:
  - `coordinate_frame`
  - `coordinate_epoch`

Future epoch rendering (thousands/millions of years) is a derived operation. Canonical stored coordinates remain fixed for the build epoch.

## Identity Strategy

### Canonical star identity
`stable_object_key` priority:

1. `star:gaia:<source_id>`
2. fallback deterministic hash only where Gaia ID is unavailable (rare edge paths)

### Canonical system identity
`systems` are derived from multiplicity hierarchy/grouping materialization:

- explicit catalog hierarchy edges preferred
- conservative fallback grouping where hierarchy evidence is weak

System keys must be deterministic and stable across identical rebuild inputs.

## Multiplicity Strategy

Multiplicity evidence sources (current policy):

1. Gaia NSS (exact Gaia-linked evidence)
2. MSC (optional; default-off)
3. WDS/ORB6 (broad support evidence, confidence-gated)

Current rules:

- proximity grouping stays nondefault
- WDS-Gaia path stays optional and confidence-gated
- physical consistency gating is required for WDS-linked grouping via bridge:
  - distance spread threshold
  - proper-motion spread threshold
  - match angular-distance threshold

### Systems of systems
Architecture target:

- represent explicit hierarchy (parent/child subsystem relationships)
- allow navigation both upward and downward
- preserve inspectability of each subsystem as an analyzable entity

Hierarchy confidence must be explicit and queryable.

## Planet Host Matching

Host matching must run against canonical Gaia-backed stars/systems.

Priority:

1. Gaia source ID
2. high-confidence catalog crosswalk IDs
3. deterministic name fallback (flagged lower confidence)

No hidden fuzzy merge into canonical rows.

## Unit Policy

- Preserve source-native units/fields in raw and cooked stages.
- Canonical core should store parsec-native distance/position.
- Store LY convenience columns for serving efficiency and UX.
- Avoid repeated runtime unit conversion in hot paths.

## Ingestion and Build Contract

Pipeline:

1. download (`raw/`)
2. cook (`cooked/`)
3. ingest (`out/<build_id>/core.duckdb` + parquet)
4. promote (`served/current`)
5. verify (QC + provenance + contract checks)

Rules:

- raw files are immutable snapshots
- cooked outputs are deterministic and disposable
- build outputs are immutable by build ID
- promotion is atomic

## Security and Transport Policy

1. Insecure transport may be used only as an explicitly acknowledged exception.
2. No production default build may require insecure transport.
3. Each source must document:
   - license
   - retrieval integrity path (checksum/etag/signature)
   - transport caveats
4. Public-facing hosts must prefer mirrored/pinned upstreams when source reliability or geopolitical routing is risky.

Current exception note:

- MSC source transport history requires explicit caution; keep optional/default-off until robust mirror/transport policy is locked.

## Runtime and Host-Specific Documentation

Host-specific runtime config is documented outside git at `/srv/spacegate/RUNTIME.md`.

Required runtime notes:

- antiproton public-host specifics (TLS, nginx, auth, deployment)
- proton development specifics
  - OAuth redirect workaround (tunnel `:8080` to `:80` on proton for admin panel OAuth flow)

## Operational Observability (Admin Status Panel)

Spacegate now includes a dedicated admin status panel for build/runtime diagnostics and dataset governance.

API endpoint:

- `GET /api/v1/admin/status/dataset`

Panel purpose:

- quantify served dataset scale and slice behavior
- identify likely bottlenecks (memory / CPU / IO signals)
- expose storage footprint by major data area
- show multiplicity and source-combination coverage
- surface spectral/exotic/object breadth indicators for quality review
- keep admin diagnostics visually consistent with active site theme
- make status interpretation fast under large builds (humanized rows, bars, concise summaries)

Minimum metrics exposed:

- counts: rows/systems/stars/planets/multi-star systems
- slice metrics: backbone input rows, sliced-in stars, sliced-out rows/percent
- source breakdowns: stars by source catalog; multiplicity evidence alone/in combination
- object breakdowns: spectral class distribution; exotic-star heuristics; exoplanet + candidate habitable counts
- astrophysical breakdowns: standard spectral buckets (`O/B/A/F/G/K/M/L/T/Y/D/unknown`) and inferred compact-object counts
- runtime health: API RSS + peak RSS, host memory/load, DuckDB runtime memory/database figures
- storage health: project/state/build/core/rich/parquet/raw/cooked/reports sizes and disk usage
- query-timing probes for major status queries
- percentage capacity bars where current vs maximum is known (disk, host memory, API RSS/peak vs host, DuckDB memory vs limit)
- concise humanized status summary plus raw payload for deep debugging

Implementation constraints:

- status endpoint is admin-only
- heavy aggregates are cached briefly in-process to avoid repeated full scans
- status metrics are diagnostic, not canonical science tables
- admin UI defaults to the Status subpage for immediate operational visibility
- admin IA split: `Status` (performance/health) and `Dataset` (composition/slice controls)

## Dataset Slice Policy (Admin Dataset Panel)

Definition:

- A **slice** is a deterministic row-selection policy applied at ingest to produce `core` and complementary `halo` from `galaxy`.
- Slice policy is recorded in `build_metadata` and emitted to `reports/<build_id>/slice_policy_report.json`.

Current slice controls (admin):

- distance (`max_distance_ly`)
- astrometry quality (`min_parallax_over_error`, `max_parallax_error_mas`, `max_ruwe`)
- completeness (`require_spectral_class`, `require_color_index`)
- class selection (`allowed_spectral_classes`)

Execution model:

- Preview endpoint estimates retained/sliced counts against current served build.
- Build action applies policy through `scripts/build_core_slice.sh` and publishes a new immutable build set.
- Projection reversibility is handled by rebuilding from `galaxy` with a different slice profile, not by mutating rows.

Performance model:

- To improve runtime latency, slicing should materialize a smaller served build.
- Keeping all rows in the same served table and only adding query-time gating generally does not provide equivalent scan performance.

## Slice Profiles and SLO Targets

Authoritative slice profile and performance gates are tracked in:

- `docs/SLICE_PROFILES.md`

Rules:

- `core` profile must be selected by explicit name/version.
- promotion gates require SLO pass for the active profile.
- `halo` remains queryable only with explicit user intent.

## Milestones (Gaia-First Program)

### Phase A: Gaia Backbone Pilot

- implement deterministic Gaia backbone ingest (`<1000 ly`)
- include quality tiers (`poe`, `ruwe`, astrometry solved flags)
- emit `gaia_backbone_report.json`:
  - counts by quality bands
  - runtime/memory
  - storage footprint

### Phase B: Product Slice

- define deterministic slice policy over backbone
- serve default UI/API from slice
- add explicit deep-query mode against backbone

### Phase C: Multiplicity Reintegration

- attach NSS/MSC/WDS/ORB6 evidence against Gaia IDs
- materialize hierarchy with confidence tiers
- preserve benchmark system quality (Castor, etc.)

### Phase D: Crosswalk and Naming

- replace AT-HYG convenience naming/crosswalk dependency with dedicated crosswalk sources
- maintain or improve user-facing name quality and lookup ergonomics

### Phase E: Enrichment Expansion

- coolness/factsheets/exposition/snapshots driven by Gaia-first core
- confidence captions for derived animations/visualizations

## AT-HYG Retirement Policy

AT-HYG should be removed from canonical inventory once these pass:

1. host-match quality is not worse than current production
2. benchmark system hierarchy quality is not worse than current MSC-enabled baseline
3. user-facing naming and alias coverage is preserved via replacement crosswalks
4. no critical API fields regress

AT-HYG may remain as an optional compatibility/crosswalk input during migration, but not as canonical star inventory.

## Acceptance Gates for Gaia-First Default

1. Determinism: repeated runs produce identical canonical outputs for pinned inputs.
2. Performance: proton ingest/verify and API p95/p99 are within operational budget.
3. Storage: backup/restore and retention policy validated for multi-million-row cadence.
4. Data quality:
   - boundary and astrometry confidence flags implemented
   - multiplicity confidence tiers queryable
5. Security:
   - no required insecure transport in default build path
   - provenance completeness gate enforced

## What We Are Not Doing

- no mutation of canonical astronomy rows by user edits
- no rim mixed into core or disc scientific derivations
- no hidden model inference of physical stellar parameters in core
- no unbounded proximity-based grouping in default production builds

## Documentation Map

- `docs/SCHEMA_CORE.md`: canonical core schema contract
- `docs/SCHEMA_RICH.md`: disc contract (legacy filename retained)
- `docs/SCHEMA_LORE.md`: rim contract (legacy filename retained)
- `docs/SLICE_PROFILES.md`: slice profile catalog and SLO acceptance gates
- `docs/DATA_SOURCES.md`: source inventory and retrieval policy
- `docs/CHECKLIST.md`: executable delivery tracker
- `docs/MILESTONES.md`: dependency-ordered roadmap, restored ideation backlog, and long-range goals

## Immediate Next Actions

1. Implement `galaxy` artifact contract and paired `core`/`halo` projection materialization.
2. Wire slice profile selection (`profile_id@version`) into build metadata and admin controls.
3. Enforce SLO gating in promotion for active core profile.
4. Re-run multiplicity comparison modes against Gaia-backed `galaxy` IDs.
