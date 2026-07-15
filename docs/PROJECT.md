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
Spacegate now operates on a Gaia-first canonical core with deterministic
canonical reduction, explicit ARM evidence/hierarchy/orbit support, a public
100 ly 3D map, simulation-first system pages, and Star Search v2. AT-HYG is no
longer a canonical inventory source; its remaining role is transitional alias
and compatibility enrichment.

The current main quest is to scale exploration beyond the 100 ly monolithic
map through versioned spatial tiles and LOD loading. In parallel, the AI
Astronomy Agency must advance through a secure, citation-backed, reviewed
vertical slice before any broad autonomous ingestion or public narration.

## Core Principles

1. Canonical inventory first, enrichment second.
2. Provenance on every served row.
3. Deterministic builds and deterministic promotion.
4. Clear layer boundaries:
   - `core`: immutable served canonical science inventory/projection and
     selected hot-path scalar facts
   - `arm`: immutable science evidence/support layer (source-native support
     rows, adjudication candidates, graph/orbit evidence, deterministic
     science derivatives outside core hot paths)
   - `disc`: reproducible presentation derivatives and labeled assumptions
   - `rim`: editable fiction
5. Explicit confidence for joins/groupings; avoid silent inference.
6. Security-first ingestion: no required insecure transport dependencies.
7. Classification safety invariants: explicit remnant evidence must override temperature-derived stellar class labels.
8. Graph discipline:
   - one acyclic containment spine for navigation
   - additional typed relation edges for cross-links and dynamic relationships
   - no requirement that all node/edge vocabulary lives in `core`
9. Classification discipline:
   - source classifications in `core` must remain source-faithful
   - deterministic derived display/physical classifications may live in `arm`
     only with provenance, source-supersession, confidence, and remnant guards
   - UX supergroups (for example `subplanet`) are allowed but may not overwrite canonical science fields

Operational runbook:

- ingest failure recovery + runtime tuning: `docs/INGEST_RECOVERY.md`
- 3D map runtime: `docs/3D_MAP.md`
- system simulation contract: `docs/SYSTEM_SIMULATION.md`

## Data Layers (`core` / `arm` / `disc` / `rim`)

Layer ownership is decided by **role in Spacegate**, not only by whether a value
comes directly from a source catalog.

- `core` owns conservative canonical inventory/projection rows and promoted
  hot-path scalar facts.
- `arm` owns source-native evidence/support rows, alternate solutions,
  confidence-gated graph/orbit structures, and deterministic science
  derivatives that are not canonical inventory.
- `disc` owns reproducible presentation products, prioritization, generated
  artifacts, and explicitly labeled assumptions.
- `rim` owns optional fictional/user-authored overlays.

Promotion rule:

`source catalog -> cooked source rows -> arm evidence/support graph -> reviewed
promotion -> selected canonical core facts`

Core must not become a dump of every source catalog's native ontology. Arm may
store source-native science rows because they are evidence for Spacegate's
canonical model, not automatically canonical Spacegate object facts.

Planet orbits follow that rule. `core.planets` may keep promoted scalar orbit
summaries for search/detail stability, but source-native orbit edges,
simulation-ready orbital solutions, fit metadata, uncertainty, epochs, and
future alternate solutions belong in `arm`.

### Galaxy (immutable canonical astronomy)
Retired as an active database layer. The earlier `galaxy.duckdb` plan treated a
full canonical science corpus as a separate build artifact, with `core` and
`halo` projected from it. Current Spacegate builds instead materialize the
served canonical inventory directly as `core.duckdb`, with source-native
evidence and graph/orbit support in `arm.duckdb`.

The term may be reused later for a different large-scale or full-sky product,
but it is not an active Admin/API layer.

### Core (immutable astronomy)
Fast default serving projection for common browse/search/detail traffic.

Core is generated deterministically from pinned source snapshots, cooked source
rows, and a versioned slice/profile policy.
It should stay conservative: accepted object identity, accepted host/membership
links, and selected hot-path scalar facts only.

### Halo (immutable astronomy complement)
Retired as an active complement-to-core database layer. The `halo` name is
reserved for possible future use, likely a larger extended/full-Gaia astronomy
store rather than the old complement projection.

### Core canonical tables

- `systems`
- `stars`
- `planets`
- `build_metadata`

Core must remain free of generated prose/images/rim overlays.

### Arm (immutable science evidence/support)

- source-native evidence/support datasets that are still scientific and
  provenance-bound
- deterministic science derivatives and normalized graph/orbit structures used
  for adjudication, diagnostics, simulation, and narration
- examples: MSC/WDS/ORB6/SBX/Gaia NSS support rows, variability families, dense
  diagnostics, source-native orbital solutions, WISE/CatWISE/AllWISE infrared
  cross-reference evidence, and non-hot-path science tables

Arm rows follow the same immutability and provenance rules as core, but are
separated because they are support/evidence, may contain alternate or
confidence-ranked claims, and should not imply canonical Spacegate inventory
acceptance.

### Disc (rebuildable derived artifacts)

- coolness scoring
- object-scoped coolness prioritization (systems first, then stars and planets)
- snapshots
- factsheets / expositions
- external links
- optional neighbor graph

Disc is always regenerable from core/arm plus pinned generators.

Coolness is a core CoolStars discovery surface, not a cosmetic add-on. The
current ranking model was designed against an earlier, smaller database and
must be retooled for the larger public build. The next version should combine
explainable weighted signals such as proximity, luminosity, proper motion,
multiplicity, stellar rarity, giant/supergiant status, planet interest,
system complexity, data quality, and narrative/scientific value. It should
show per-object contribution breakdowns so operators can tune weights without
turning the ranking into an opaque popularity score.

## Agent-Assisted Enrichment Policy

Agent outputs must not mutate `core` directly.

Allowed destinations:

- `disc` for citations, source manifests, factsheets, narratives, and other reproducible generated artifacts
- `arm` for agent-proposed missing-field fills, ambiguity dossiers, and adjudication candidates

Required behavior:

- prioritize work by coolness and scientific/narrative value
  - systems first
  - then stars
  - then planets
- maintain a separate ambiguity-resolution queue so problematic systems are not starved by pure popularity/coolness ordering
- prefer primary and high-rigor public sources
  - mission/archive pages
  - peer-reviewed papers
  - reputable catalog documentation
- persist source links/citation context in `disc` so generated narratives and factsheets remain auditable
- persist agent-proposed resolutions and missing-field candidates in `arm` with confidence and provenance
- abstain when evidence is conflicting or weak; uncertain cases must remain proposals, not canonical truth
- never allow untrusted publication text or model output to directly command
  tools, mutate canonical science rows, or bypass source/adjudication gates

Design implication:

- `core` remains deterministic and conservative
- `arm` becomes the staging area for machine-assisted scientific adjudication
- `disc` becomes the reproducible presentation layer for source-linked enrichment

Implementation note:

- canonical ingest is documented in `docs/CANONICAL_INGEST.md`; full build wrappers promote the canonical database build emitted from the bootstrap science projection
- the first deterministic sloppy-system queue baseline is emitted by `scripts/ingest/build_adjudication_queue.py`
- public system-simulation scene readiness is exposed through
  `/api/v1/systems/{system_id}/simulation-scene`; it assembles current core
  detail, hierarchy, arm graph/orbit rows, and readiness diagnostics without
  persisting visualization assumptions
- public name display is centralized behind a `name_style` API/UI preference:
  `public_full` is the layperson-readable default, while abbreviated,
  catalog-compact, and source-technical styles remain available without
  changing source identity or accepted system membership
- System Simulation is lazy-loaded on system detail pages and from the 3D
  map's Peek/Explore drill-in layer, and uses the scene-readiness endpoint;
  `render_scene_v0.2` adds renderer-ready
  bodies/orbits and provenance-bearing source/derived/assumed/missing fields;
  hierarchical subsystem orbit edges are exposed as group-pair guides rather
  than collapsed into direct binary star orbits; subsystem hierarchy nodes with
  rendered descendants are also exposed as inspectable UI handles; the payload
  includes `simulation_tree_v1`, a derived root/barycenter/body tree that lets
  the browser animate nested stellar systems recursively from the emitted orbit
  rows instead of applying ad hoc offsets to a flat layout; hierarchy-pair
  period fallbacks now prefer MSC system-row periods and projected-separation
  Kepler estimates before generic visual assumptions, and hosted planets/HZ
  overlays can attach to active tree body positions; stellar orbit display
  radii preserve broad source/projected separation order, HZ display scaling
  includes rendered HZ bounds, and True Bodies planet meshes use Earth-to-Sun
  scale relative to stars; this remains a
  deterministic presentation-scale Keplerian preview, not source-scaled epoch
  propagation or N-body dynamics; the
  payload now exports every rendered `ASSUMED` value as a structured
  `render_scene.assumptions` record with stable assumption keys and a
  selected-system `disc.simulation_assumptions` materialization path; broader
  reviewed assumption curation remains future work; `visual_scale_beta_v1`
  explicitly labels the live preview as presentation-scaled rather than
  physically scaled and now advertises selectable Structure, True Orbits, True
  Bodies, and Log Scale modes; True Orbits uses a pure linear
  semi-major-axis-to-scene transform with no fixed inner readability offset,
  while Structure mode caps visible stellar radii against nearest rendered
  separation and keeps halo and picking radii as separate readability aids;
  rendered planet bodies now carry `host_body_key` when their canonical
  host star, or a catalog-equivalent source-native component, resolves into the
  rendered scene; two-star scenes without source orbit edges may use clearly
  labeled `disc_assumption` visual binary fallback orbits for legibility; the
  simulator now exposes renderer-only `fields.visual_stellar_class` so missing
  spectral classes can still produce useful star colors/readouts from source
  spectral/temperature evidence, compact-object evidence, or clearly labeled
  mass-based visual priors without promoting those priors into catalog facts;
  stale public slices that lack explicit subsystem bodies can also derive
  inspectable subsystem handles from `simulation_tree_v1` barycenters, labeled
  as `render_scene` runtime structure while source-native hierarchy handles
  remain preferred; the
  public preview now supports pause/start, speed, reset, orbit visibility,
  camera orbit/zoom/pan with reset-view support, hover, pinned copyable
  readouts with in-scene selected-object feedback, SDF text object labels for
  zoom-readable names, and deterministic procedural star/planet surface
  materials for visual clarity; orbit paths now carry renderer guide/trace
  provenance and missing planet inclinations may use deterministic low-tilt
  `disc_assumption` render fallbacks; these materials, labels, path guides, and
  fallbacks are transient presentation transforms over existing scene fields,
  not source surface maps or ARM orbital evidence; deterministic snapshots
  remain the fallback/reference artifact. In the 3D map, selecting a star opens
  System Simulation Peek without moving the map camera; Explore flies the map
  camera toward the selected system and expands the same simulation layer. This
  is the first production bridge between browsable local-space flight and
  system-level inspection, but it remains a coordinated two-layer runtime
  rather than a single continuous galaxy-to-AU physical simulation.
- Star Search v2 is the structured catalog counterpart to the immersive 3D
  map. It keeps the existing public search/filter API but presents results as
  readable system cards with bounded live System Simulation previews, cached
  first-frame reuse, and deterministic snapshots only as fallback/reference
  artifacts. Public system pages are now simulation-first: the top of the page
  stages a visual simulation, quick system facts, plain-language overview,
  "why it matters", habitability context, what-we-know notes, uncertainty
  notes, and explore-more prompts before exposing hierarchy, catalog rows,
  evidence, and technical provenance. These sections are the public scaffold
  that future AAA-reviewed narration can enrich without replacing source
  evidence. The
  hierarchy section is an object tree for readers first: rows show stellar
  class pills, object kind, orbit tags, compact vitals, and a short
  plain-language description before technical identifiers and catalog rows.
  Stellar-class chips are reusable educational affordances across Star Search,
  the 3D map, System Hierarchy, and System Simulation object readouts; unknown
  classes are labeled explicitly rather than guessed. This is presentation
  structure over existing `core`/`arm`/`disc` contracts; it does not promote
  visual assumptions into catalog facts.

### Rim (editable overlays)
User/worldbuilder entities and relationships keyed by `stable_object_key`.

`arm` is now canonical (no `aux` compatibility artifact).

## Gaia-First Architecture

Spacegate uses a layered astronomy runtime:

1. `core` (served canonical inventory/projection; Gaia-first)
2. `arm` (source-native evidence/support, graph/orbit contracts, diagnostics)
3. `disc` (deterministic presentation products and explicit assumptions)
4. `rim` (fiction/worldbuilding overlays)

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

### Classification Coverage Reality Check (Gaia DR3, <=1000 ly)
Gaia archive checks on March 6, 2026:

- scope rows: `17,785,548`
- `teff_gspphot` null: `13,786,856` (~77.5%)
- `bp_rp` null: `2,439,891` (~13.7%)

Implication:

- temperature-only spectral inference cannot be the primary physical classifier
- compact/remnant classification requires dedicated evidence columns and cross-catalog support

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
2. MSC (mandatory)
3. WDS/ORB6/SBX (broad support evidence, confidence-gated)

Current rules:

- proximity grouping stays nondefault
- WDS-Gaia path stays optional and confidence-gated
- MSC is mandatory in default science ingest (missing MSC is a build/promotion blocker)
- MSC source constants target the June 19, 2026 upstream archive
- MSC `comp.tsv`, `sys.tsv`, and `orb.tsv` are preserved into cooked artifacts;
  ARM materializes source-native subsystem details, hierarchy/orbit edges, and
  MSC orbital solutions where endpoint keys are supported. Endpoint labels named
  by MSC `sys.tsv`/`orb.tsv` materialize as ARM support leaves before any
  count-expanded fallback leaves are used, unless the label is an exact source
  subsystem parent; these support leaves do not become flat `core.stars` rows
  without separate canonical inventory evidence. MSC root components are
  materialized even for simple binaries so source-backed systems such as 70 Oph
  can attach `sys.tsv` masses and `orb.tsv` orbital solutions to normalized ARM
  edges without inventing new core stars
  (`newmsc-20260619.tar.gz`); CTIO TLS failures require explicit
  SHA-256-pinned fallback handling; local canonical build
  `20260628T1210Z_msc20260619` promoted on June 28, 2026 and passed required
  multiplicity golden checks
- SBX is default-on support evidence (`SPACEGATE_ENABLE_SBX`) for spectroscopic-binary coverage
- physical consistency gating is required for WDS-linked grouping via bridge:
  - distance spread threshold
  - proper-motion spread threshold
  - match angular-distance threshold
- post-enrichment source-object reconciliation runs before system grouping:
  - duplicate MSC component surrogates may be reconciled onto enriched
    Gaia/accepted source stars only through strong HIP/HD evidence plus
    physical sanity checks
  - accepted reconciliations are persisted in
    `core.source_object_reconciliation`
  - ambiguous candidates are persisted in
    `core.source_object_reconciliation_quarantine`, not merged
  - Alpha Centauri / Proxima Centauri is the benchmark: Proxima remains the
    direct Gaia/source planet host while rolling into the accepted Alpha
    Centauri physical system through MSC/WDS component-C evidence

### Systems of systems
Architecture target:

- represent explicit hierarchy (parent/child subsystem relationships)
- allow navigation both upward and downward
- preserve inspectability of each subsystem as an analyzable entity

Hierarchy confidence must be explicit and queryable.

Implementation pattern:

- containment edges (`contains`) form a tree for stable traversal
- loops and bridges are represented as non-containment typed edges (for example `anchored_to`, `gateway_to`)
- structural node/edge vocabulary is shared across layers; storage location depends on layer policy (`arm` science graph vs `rim` editable overlays)

## Planet Host Matching

Host matching must run against canonical Gaia-backed stars/systems.

Priority:

1. Gaia source ID
2. high-confidence catalog crosswalk IDs
3. deterministic name fallback (flagged lower confidence)

No hidden fuzzy merge into canonical rows.

## Planet Inventory and Orbit Boundary

Planet rows may be canonical inventory once Spacegate accepts the source
lifecycle state and host match, but detailed planet orbital solutions are not
automatically core facts.

Core may carry selected source-native/hot-path planet scalars such as period,
semi-major axis, eccentricity, radius, or mass when they are the promoted
display/default values for the canonical planet row. Competing source solutions,
full element sets, reference epochs, fit quality, uncertainty, derived
simulation state, and historical source observations belong in `arm` as
evidence rows attached to graph/orbit structures.

For NASA Exoplanet Archive planets, `pscomppars` is the promoted one-row
display/default source while `ps` is retained as source/reference-specific
alternate orbital evidence in ARM. Consumers that need normal public behavior
should use rank-1/default solutions; diagnostics and future simulation controls
may expose ranked alternate candidates explicitly.

This mirrors the multiplicity policy: a planet can be in canonical inventory
without every source-native orbit solution becoming canonical core.

## Exoplanet Lifecycle Policy

Canonical policy:

- NASA Exoplanet Archive remains the canonical confirmed baseline.
- additional sources can contribute lifecycle state, alias/crosswalk support, and derived/diagnostic fields.

Status handling:

- `candidate`: included by default
- `controversial`: stored and queryable, default-off in UI/API
- `retracted`: excluded from default science views and retained as tombstoned lineage rows for audit and rim continuity

Lifecycle transitions must be reversible and lineage-complete (no destructive hard-delete of identity history).

## Derived Planet Classification Contract

Derived planet fields are deterministic and versioned per build:

- taxonomy tags (size/mass, insolation/temperature, orbit class, composition proxy, detection tags, host-context tags)
- `spacegate_hab_score` and confidence/reason metadata
- stellar-spectroscopy-informed element-richness proxy tags for rim/search use

Element-richness policy:

- inferred from host stellar spectroscopy/metallicity evidence unless direct planetary composition evidence exists
- explicitly labeled as proxy/inferred when not directly measured
- intended for ranking/filtering and rim context, not as a substitute for direct compositional measurement

## Catalog Delta and Re-Evaluation (Exoplanets)

Catalog refreshes must trigger deterministic delta analysis and selective recomputation.

Required behavior:

1. diff each source snapshot using deterministic source keys
2. classify transitions (`new`, `changed`, `missing`, `promoted`, `demoted`, `retracted`)
3. recompute lifecycle/taxonomy/habitability/resource-richness fields for impacted rows:
   - changed rows
   - rows whose host-star inputs changed (for example metallicity or luminosity inputs)
   - rows impacted by cross-source precedence changes
4. emit build reports for catalog deltas and reclassification coverage

No promoted build may serve stale derived-tag versions.

Current implementation status:

- per-source snapshot diff is now emitted at download stage:
  - report: `reports/source_delta_report.json`
  - baseline snapshot: `reports/source_delta_snapshot.json`
  - history: `reports/source_delta_history/*.json`
- this stage detects source-level `new` / `changed` / `missing` / `unchanged` transitions from manifest signatures.
- impacted-row planning is now emitted after download/cook planning:
  - planner output: `reports/impacted_rows_plan.json`
  - planner script: `scripts/plan_impacted_rows.py`
- selective differential execution path is now available:
  - selective cook: `scripts/cook_delta.sh`
  - incremental planet/lifecycle ingest: `scripts/ingest_incremental_planets.py`
  - orchestrator: `scripts/refresh_core.sh`
- routing contract:
  - if only planet/lifecycle sources changed, execute selective cook + incremental planet refresh
  - otherwise execute full cook + full ingest

## Sol Volatile Overlay Refresh

Sol has two authoritative tracks:

- `sol_authority` (S1/S2/S3 canonical bootstrap + natural bodies)
- `sol_artificial` (S4 curated artificial probes/stations/orbiters)

Operational policy:

1. Refresh volatile Sol feeds on a regular cadence independent of full Gaia refresh.
2. Track freshness/staleness in report form before promotion decisions.
3. Keep Sol volatile overlays in `arm`; never bypass canonical `core` provenance rules.

Runbook:

- refresh + normalize + report:
  - `scripts/refresh_sol_volatile.sh`
- report-only (for monitoring/cron checks):
  - `scripts/report_sol_volatile.py`

Output:

- `reports/sol_volatile_report.json`

## Unit Policy

- Preserve source-native units/fields in raw and cooked stages.
- Canonical core should store parsec-native distance/position.
- Store LY convenience columns for serving efficiency and UX.
- Avoid repeated runtime unit conversion in hot paths.

## Ingestion and Build Contract

Pipeline:

1. download (`raw/`)
2. cook (`cooked/`)
3. bootstrap science projection (`out/<build_id>_bootstrap/core.duckdb` + `out/<build_id>_bootstrap/arm.duckdb` + parquet)
4. canonicalization + emission (`out/<build_id>/core.duckdb` + `out/<build_id>/arm.duckdb` + `canonical_hierarchy.duckdb` + parquet)
5. promote (`served/current`)
6. verify (QC + provenance + contract checks)

Rules:

- raw files are immutable snapshots
- cooked outputs are deterministic and disposable
- build outputs are immutable by build ID
- promotion is atomic

## Classification Stewardship Rule (Core)

Non-negotiable rule:

- if explicit remnant evidence exists, Spacegate must not classify the object as a normal stellar spectral bucket by temperature fallback alone.

Minimum remnant evidence sources:

- Gaia DR3 astrophysical probabilities (`classprob_dsc_*_whitedwarf`)
- source-native spectral remnant signatures (for example `D*` white-dwarf notation)
- authoritative remnant catalogs (when integrated)

Required behavior:

1. classify by evidence-first precedence, not temperature-first fallback
2. preserve raw evidence and confidence for auditability
3. separate `object_family` from user-facing color/temperature rendering
4. keep source spectral class, ARM-derived display/physical class, and
   render-scene visual priors as distinct fields with distinct provenance

Hard QC gate:

- build fails if a row has remnant-positive evidence but is emitted as a non-remnant stellar family without an explicit override record.

## Security and Transport Policy

1. Insecure transport may be used only as an explicitly acknowledged exception.
2. No production default build may require insecure transport.
3. Each source must document:
   - license
   - retrieval integrity path (checksum/etag/signature)
   - transport caveats
4. Public-facing hosts must prefer mirrored/pinned upstreams when source reliability or geopolitical routing is risky.

Current exception note:

- MSC source transport history requires explicit caution; maintain mirrored/pinned retrieval and integrity checks for production reliability.

## Runtime and Host-Specific Documentation

Host-specific runtime config is documented outside git at `/srv/spacegate/RUNTIME.md`.

Required runtime notes:

- antiproton public-host specifics (TLS, nginx, auth, deployment)
- photon development/build specifics
  - source `/srv/spacegate/photon.env` before host-side Spacegate tasks
  - keep bulky research/source-document cache under `/mnt/space/spacegate`
    when mounted; keep auditable metadata and hashes in internal state
- proton fallback/reference specifics only where still relevant

Operational config note:

- `/etc/spacegate` should be `root:spacegate` with mode `2750` so files
  replaced by root-owned editor save paths inherit group `spacegate`.
- `/etc/spacegate/spacegate.env` should be `root:spacegate` with mode `0640`.
  It is the preferred host-local location for OIDC secrets, provider API keys,
  session secrets, and server-side Spacegate runtime secrets.

## Public 3D Map Runtime

The first 3D map pilot is active as a public web runtime slice.

Design source:

- `docs/3D_MAP.md`

Current contract:

- public route: `/map`
- tiled artifact contract: `docs/TILED_MAP.md`
- public slice: selectable Sol-centered 100 and 250-ly radii through immutable
  octree tiles; `GET /api/v1/map/systems` remains a temporary 100-ly diagnostic
  comparator
- rendering stack: React 19 + Three.js through React Three Fiber
- controls: selectable `WASD`, `ESDF`, or `8456` flight layouts plus permanent
  arrow-key flight, mouse look, Shift boost, and stabilized vertical by
  default; v0.2 adds touch-first phone/tablet controls with
  drag-look, tap/select-reticle, two-finger pinch flight, and two-finger pan
- public map branding comes from `GET /api/v1/public-config`, backed by
  `SPACEGATE_SITE_NAME` / `SPACEGATE_MAP_TITLE`, with `Coolstars Map` as the
  default until installer prompts are added
- ephemeral route measurement: desktop right-click can measure from the
  selected system to a target, draw client-side per-leg distance lines, and
  show total route length; this is a map tool, not persisted Rim route data
- performance profile: deterministic binary tiles keep DuckDB out of continuous
  camera flight, provide coarse contextual LOD, and refine through a smooth
  camera-centered detail bubble; Balanced, Performance, and Exact density modes
  preserve stable identity and bounded labels while machine-readable Photon
  browser reports define acceptance
- presentation profile: Discovery remains the default; Bright increases star
  visibility on large/high-resolution displays, and bounded labels can carry
  one toggleable deterministic representative system-class badge
- map overlay themes are handled in the Star Map layer: Enterprise/LCARS uses
  black nontransparent map cards with bright yellow borders and no glow, while
  Simple Light and Geocities use more opaque map overlays; embedded System
  Simulation controls, dropdown option menus, and vitals must remain
  readable/clickable over WebGL
- System Simulation Peek is a lightweight map overlay and can be resized on
  desktop for the current browser session
- Star Search v2 pages preserve map-to-system return context when opened from
  the map, while `/search` remains the article-like catalog/search surface for
  users arriving by name, catalog ID, or general curiosity.
- Public UX goldens are tracked separately from ingestion/multiplicity goldens
  in `docs/PUBLIC_UX_GOLDENS.md`; they benchmark whether recognizable systems
  resolve, render, and explain well for ordinary visitors.

Layer rules:

- science points come from `core` plus read-only `disc` presentation priority
  fields
- generated visuals, extended objects, and rim overlays must remain separate
  render layers
- map selection must hand off by `system_id`/`stable_object_key`, never by
  point-array position
- 500 and 1,000-ly verification manifests use the same contract but remain
  nonpublic until the measured 250-ly pilot passes

## Operational Observability (Admin v2)

Spacegate now includes a dedicated Admin v2 console for build/runtime
diagnostics, dataset governance, object diagnostics, inference configuration,
Agency source policy, jobs, audit, and operational runbooks.

Primary UI/API:

- UI: `/admin/`
- API base: `/api/v2/admin`
- Dataset status endpoint: `GET /api/v2/admin/status/dataset`
- Runtime status endpoint: `GET /api/v2/admin/runtime/status`
- Operations status endpoint: `GET /api/v2/admin/operations/status`

Panel purpose:

- quantify served dataset scale and slice behavior
- identify likely bottlenecks (memory / CPU / IO signals)
- expose storage footprint by major data area
- show multiplicity and source-combination coverage
- surface spectral/exotic/object breadth indicators for quality review
- keep admin diagnostics visually consistent with active site theme
- make status interpretation fast under large builds (humanized rows, bars, concise summaries)
- expose star-level `arm` evidence overlays in system detail (currently VSX + UltracoolSheet, with stellar-parameter/orbital overlays as the next narration-facing payload)
- expose persisted `arm.derived_physical_parameters` rows and clearly label
  source, derived, and assumed simulation inputs for review
- expose runtime spectral-subclass main-sequence priors as low-authority
  simulation support only, with guards for evolved/remnant/compact objects and
  no writes into `core` source fields

Minimum metrics exposed:

- counts: rows/systems/stars/planets/multi-star systems
- slice metrics: backbone input rows, sliced-in stars, sliced-out rows/percent
- source breakdowns: stars by source catalog; multiplicity evidence alone/in combination
- object breakdowns: spectral class distribution; exotic-star heuristics; exoplanet + candidate habitable counts
- astrophysical breakdowns: standard spectral buckets (`O/B/A/F/G/K/M/L/T/Y/D/unknown`) and inferred compact-object counts
- runtime health: API RSS + peak RSS, host memory/load, DuckDB runtime memory/database figures
- storage health: project/state/build/core/arm/disc/parquet/raw/cooked/reports sizes and disk usage
- query-timing probes for major status queries
- percentage capacity bars where current vs maximum is known (disk, host memory, API RSS/peak vs host, DuckDB memory vs limit)
- deterministic rerun compare status (`match` / `mismatch` / `no baseline`) against prior comparable build fingerprints
- concise humanized status summary plus raw payload for deep debugging

Implementation constraints:

- status endpoint is admin-only
- heavy aggregates are cached briefly in-process to avoid repeated full scans
- status metrics are diagnostic, not canonical science tables
- Admin v2 uses task-oriented workspaces: Overview, Builds, Dataset, Object
  Diagnostics, Inference, Agency, Runtime, Operations/Jobs, Audit
- Admin v2 routes mutating work through authenticated, CSRF-protected,
  allowlisted jobs with audit records
- Public-edge Admin should receive a reverse-proxy hardening pass before broader
  promotion: gate `/admin` and `/api/v2/admin/*` with an outer control such as
  VPN/Tailscale, IP allowlist, or basic auth. Obscuring the visible Admin path
  is acceptable as bot-noise reduction only, not as the security boundary.

## Dataset Slice Policy (Admin Dataset Panel)

Definition:

- A **slice** is a deterministic row-selection policy applied at ingest to produce
  the served `core` build for a target runtime profile.
- Slice policy is recorded in `build_metadata` and emitted to `reports/<build_id>/slice_policy_report.json`.

Current slice controls (admin):

- distance (`max_distance_ly`)
- astrometry quality (`min_parallax_over_error`, `max_parallax_error_mas`, `max_ruwe`)
- completeness (`require_spectral_class`, `require_color_index`)
- class selection (`allowed_spectral_classes`)

Execution model:

- Preview endpoint estimates retained/sliced counts against current served build.
- Build action applies policy through `scripts/build_database_slice.sh` and publishes a new immutable build set.
- Projection reversibility is handled by rebuilding from pinned raw/cooked source
  snapshots with a different slice profile, not by mutating served rows.

Performance model:

- To improve runtime latency, slicing should materialize a smaller served build.
- Keeping all rows in the same served table and only adding query-time gating generally does not provide equivalent scan performance.

## Slice Profiles and SLO Targets

Authoritative slice profile and performance gates are tracked in:

- `docs/SLICE_PROFILES.md`

Rules:

- `core` profile must be selected by explicit name/version.
- promotion gates require SLO pass for the active profile.
- constrained public hosts may use a documented public slice profile that keeps a `parallax_over_error` floor but avoids a hard RUWE gate when that gate removes important multiplicity/remnant companions.

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

- attach NSS/MSC/WDS/ORB6/SBX evidence against Gaia IDs
- materialize hierarchy with confidence tiers
- run golden-system multiplicity exam post-ingest (Castor-first)
- preserve benchmark system quality (Castor, etc.)

### Phase D: Crosswalk and Naming

- implement replacement alias/crosswalk layer on top of Gaia canonical IDs
- maintain or improve user-facing name quality and lookup ergonomics

### Phase D.5: Exoplanet Multi-Catalog Lifecycle

- ingest exoplanet lifecycle/support layers from multiple catalogs with deterministic precedence
- include `candidate` by default, keep `controversial` default-off, tombstone `retracted`
- run per-source catalog diff and impacted-row reclassification on refresh
- emit contribution/overlap and lifecycle transition reports per build

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

Operational status:

- default Gaia-first builds keep AT-HYG out of canonical inventory, but keep
  the AT-HYG alias crosswalk enabled as transitional naming/legacy-ID
  enrichment until stronger replacement authorities are ingested.
- ingest emits `reports/<build_id>/athyg_retirement_report.json` to track residual AT-HYG contribution and retirement readiness.

## Acceptance Gates for Gaia-First Default

1. Determinism: repeated runs produce identical canonical outputs for pinned inputs.
2. Performance: proton ingest/verify and API p95/p99 are within operational budget.
3. Storage: backup/restore and retention policy validated for multi-million-row cadence.
4. Data quality:
   - boundary and astrometry confidence flags implemented
   - multiplicity confidence tiers queryable
   - remnant classification invariant enforced (no WD->A/B/F/... fallback mislabels)
5. Security:
   - no required insecure transport in default build path
   - provenance completeness gate enforced
6. Reproducibility diagnostics:
   - `reports/<build_id>/determinism_report.json` emitted and compared against prior comparable builds during verify

## What We Are Not Doing

- no mutation of canonical astronomy rows by user edits
- no rim mixed into core or disc scientific derivations
- no hidden model inference of physical stellar parameters in core
- no unbounded proximity-based grouping in default production builds

## Documentation Map

- `docs/SCHEMA_CORE.md`: canonical core schema contract
- `docs/SCHEMA_ARM.md`: science evidence/support graph/orbit contract
- `docs/SCHEMA_DISC.md`: disc contract
- `docs/SCHEMA_RIM.md`: rim contract
- `docs/SLICE_PROFILES.md`: slice profile catalog and SLO acceptance gates
- `docs/PUBLIC_DEPLOYMENT.md`: Photon-to-antiproton public deployment runbook
- `docs/DATA_SOURCES.md`: source inventory and retrieval policy
- `docs/TESS_INTEGRATION.md`: bounded TIC/TOI identity, inventory recovery,
  candidate evidence, and observation-product plan
- `docs/SOL_AUTHORITY.md`: Sol-system authoritative ingest contract and release gates
- `docs/SYSTEM_GRAPH_ARCHITECTURE.md`: cross-layer node/edge model, containment-vs-relation rules, and generator compatibility
- `docs/DATASET_ITERATION_HISTORY.md`: dataset iteration timeline (changes, fixes, and mitigation history)
- `docs/MULTIPLICITY_GOLDENS.md`: post-ingest hierarchy/orbit exam contract
- `docs/EXOPLANET_LIFECYCLE_IMPLEMENTATION.md`: concrete DDL and execution plan for lifecycle/taxonomy/habitability
- `docs/CHECKLIST.md`: executable delivery tracker
- `docs/MILESTONES.md`: dependency-ordered roadmap, restored ideation backlog, and long-range goals

## Scientific Coverage Expansion (March 6, 2026)

Implemented in the ingest/cook pipeline:

- Gaia DR3 classifier probability ingest path (`gaia_classprob`) wired for remnant safety (`classprob_dsc_*_whitedwarf`).
- Compact-object side ingestion:
  - ATNF pulsar catalog
  - McGill magnetar catalog
  - output table: `compact_objects`
- Superstellar side ingestion:
  - Cantat-Gaudin open clusters (`table1` + membership table)
  - Green Galactic SNR catalog
  - output tables: `open_clusters`, `open_cluster_memberships`, `superstellar_objects`
- Core star enrichment:
  - `stars.object_family` / `stars.object_type`
  - `stars.classification_evidence_json`
  - `stars.open_cluster_tags_json`

Notes:

- Open-cluster memberships currently use direct Gaia DR2 ID overlap against Gaia IDs in core (high precision where IDs align; cross-release completeness can be improved with explicit DR2<->DR3 crosswalk tables).
- Compact catalog entries are preserved even when unmatched to core stars; matched rows can upgrade star object family to neutron-star classes with strict positional confidence.
- Alias/crosswalk layer now emitted as immutable `aliases` table:
  - star-level aliases (common/Bayer/Flamsteed naming + major cross-catalog IDs)
  - system-level aliases (WDS/HIP/HD + member-derived naming aliases)
  - normalized alias keys (`alias_norm`) for deterministic search resolution
- Transitional AT-HYG alias crosswalk is required for current public name
  coverage. It restores common names and legacy IDs without changing canonical
  star existence rules.
- Bayer aliases are expanded into deterministic Greek-letter forms and
  constellation-genitive forms for public search ergonomics, for example
  `Alp Cen` -> `Alpha Cen` -> `Alpha Centauri`.
- Search acceleration now materialized explicitly in `core`:
  - `system_search_terms` denormalizes canonical system names plus every alias already resolved to a `system_id`
  - `systems` carries hot-path browse/search facets (`star_count`, `planet_count`, `star_teff_count`, `min_star_teff_k`, `max_star_teff_k`, `spectral_classes_json`, `spectral_class_mask`)
  - public/API search should prefer these system-side artifacts before falling back to row-level scans
- Alias authority v2 extends search-term materialization with target context:
  - member/star aliases can resolve to the accepted owning system while carrying
    `matched_target_type`, `matched_target_id`, `matched_star_id`, and
    `focus_object_key`
  - source Gl/GJ identifiers emit common variants such as `Gliese 412` and
    `GJ 412` from component IDs such as `Gl 412A`
  - exact-like catalog and variable-star queries suppress fuzzy substitution
    when no real authority hit exists, preventing confusing public matches such
    as `V1513 Cyg` -> `V1581 Cyg`
  - catalog IDs remain matched/copyable secondary metadata and should not become
    public display titles when better names exist
- Public/detail hierarchy semantics now prefer the generic `arm` graph:
  - system detail consumes a generic nested `hierarchy` payload assembled from `component_entities`, `system_hierarchy_edges`, and `orbit_edges`
  - Sol and non-Sol systems should use the same recursive hierarchy UI instead of special-case structural views
  - effective public `star_count` should reflect the richer descendant count when `arm` exposes more stars than the flat `core.stars` membership rows
- Exoplanet host-designation promotion now applies for Gaia-fallback rows:
  - when a star/system would otherwise display as Gaia ID, promoted host labels from NASA hostnames are applied
  - precedence for promoted host labels favors human/common labels, then survey/mission-style labels (for example `TRAPPIST`, `Kepler`, `TOI`, `WASP`), then legacy catalog labels
  - Gaia ID remains last-resort fallback only
- Gaia-first builds apply AT-HYG crosswalk enrichment against Gaia IDs, plus a constrained positional fallback for named AT-HYG rows missing Gaia IDs, to recover HIP/HD/common naming coverage without changing canonical star existence rules.
- Build verification includes an alias-search gate
  (`scripts/verify_alias_search.py`) covering broad alias-corpus size,
  proper-name coverage, expanded Bayer coverage, and benchmark lookups such as
  Castor, Alpha Geminorum, Alpha Centauri, Toliman, Sirius, Jabbah, and
  Copernicus.
- Build verification also includes a compact-alias safety verifier
  (`scripts/verify_compact_alias_safety.py`) for Sirius-class hazards where a
  compact-object row without a non-compact sibling carries bright-primary
  AT-HYG aliases plus HD/WDS or non-proper primary aliases. Local build
  `20260630T_sim_beta_api_alias_v4` passes this verifier in strict mode with
  `SPACEGATE_VERIFY_COMPACT_ALIAS_SAFETY=1`.
- Sol authority source refresh and build/API verification now explicitly guard
  Horizons small-body target resolution. Asteroid/TNO/dwarf-small-body rows
  use small-body selector commands, and Ceres/Vesta-class sentinel checks stop
  ambiguous numeric Horizons commands from producing major-planet or
  satellite-like orbital solutions in core, ARM, or the simulator.
- Reviewed accepted supplements are allowed as narrow Gaia-first inventory
  exceptions when a well-established nearby object is absent from Gaia but
  required for a truthful system graph. The default list lives in
  `config/core_accepted_supplements.json`; rows remain source-provenanced and
  visibly distinct from the disabled broad AT-HYG supplement merge. Sirius A is
  the first accepted supplement, paired with a reviewed WDS component link for
  the Gaia Sirius B row. Local build `20260630T_sim_beta_api_alias_v4` first
  materialized this as a two-member Sirius system and restored reviewed no-Gaia
  ATHYG aliases such as `Alpha Canis Majoris`; current served build
  `20260630T_sim_beta_sol_smallbody_v1` retains that simulator benchmark while
  adding Sol small-body source-refresh fixes.
- Gaia-first builds now include an explicit AT-HYG supplement reconciliation pass with deterministic precedence:
  - exact Gaia ID
  - Gaia legacy remap via unique HIP/HD agreement
  - direct unique HIP/HD
  - constrained positional fallback (single-candidate only)
- Ambiguous or conflicting rows are materialized in `identifier_quarantine` (not auto-merged).
- Identifier edges are persisted in `object_identifiers` with method/confidence/source provenance.
- Duplicate source-object surrogates that become provable only after late
  identifier enrichment are reconciled in `source_object_reconciliation` before
  root-system grouping; rejected candidates are retained in
  `source_object_reconciliation_quarantine`.
- QC gates now fail the build when identifier ambiguity or namespace-collision thresholds are exceeded.
- Duplicate stewardship now includes a dedicated duplicate-trap pass:
  - exact-key duplicate checks (`gaia_id`, `hip_id`, `hd_id`, `wds_id+component`, `source_catalog+source_pk`, `stable_object_key`)
  - near-duplicate pair detection within multi-star systems using configurable angular/distance/proper-motion thresholds
  - emitted audit report: `duplicate_trap_report.json`
  - optional hard gate: `SPACEGATE_DUPLICATE_FAIL_ON_HIGH=1` with `SPACEGATE_DUPLICATE_HIGH_PAIR_MAX`
  - multiplicity hard gate (default strict): `SPACEGATE_MULTIPLICITY_GAIA_DUPLICATE_MAX` and `SPACEGATE_MULTIPLICITY_WDS_COMPONENT_DUPLICATE_MAX` (default `0`)
- Retention hygiene:
  - `scripts/prune_state_retention.sh` provides dry-run/apply cleanup for stale `out/<build_id>` and `reports/<build_id>` paths
  - policy reference: `docs/RETENTION.md`
  - current served build is always retained
  - raw/cooked catalog caches are intentionally untouched by retention prune

## Immediate Next Actions

1. Treat local build `20260714T191900Z_d873067_side_rebuild` as the current
   Photon foundation. It preserves the verified TESS/extended-object science
   sidecars from `20260713T1627Z_dd7446e_public`, the measured M8.1 map
   artifacts, and the verified class-badge correction. The M8.1.3 application
   runtime and real-device stability checks are complete; deploy this stable
   checkpoint before beginning the deeper-radius streaming phase.
2. Return to the main quest after resolving only stability regressions that
   materially affect the deployed checkpoint. Castor, V1054 Oph, and Tegmine
   simulation goldens now match the preserved science checkpoint without
   changing canonical membership or manufacturing unresolved hierarchy edges.
3. Treat the completed M8.2a extended-object science foundation as the typed
   data boundary for later non-stellar map layers; keep rendering deferred.
4. Observe the completed 100/250-ly Tiled Deep Map locally and keep 500/1,000 ly
   verification-only. Do not expose another radius until a separately measured
   milestone establishes its LOD, memory, label, and interaction budgets.
5. Build the Concept Tag Foundation so compact/normal/expanded tag priority and
   concept slugs are stable before reviewed Agency narration is published.
6. Implement one secure AAA evidence-portfolio vertical slice with source-text
   isolation, typed claims, citations, human review, and explicit publication
   state.
7. Restore deterministic snapshot-manifest coverage in the next public sliced
   build; the current local side build has live/no-WebGL behavior but no map
   snapshot rows.

## Layered Restabilization Status (March 6, 2026)

Retired direction:

- `galaxy` alias materialization and `halo` complement materialization were
  useful prototypes, but are no longer the active database architecture.
- sliced core profile metadata wiring in ingest remains active:
  - `slice_profile_id`
  - `slice_profile_version`
  - `build_layer`
  - `source_galaxy_build_id`

Current operational pattern:

1. rebuild/promote sliced `core` with explicit profile id/version
2. rebuild `arm`, `disc`, and canonical hierarchy artifacts against that served
   core
3. publish/deploy the verified build artifact set

Side-artifact rebuild pattern:

- For ARM/schema-adjacent changes that do not require recooking source catalogs
  or changing core object inventory, use `scripts/rebuild_side_artifacts.py`.
- The script clones the currently served build's `core.duckdb`, parquet,
  `disc.duckdb`, `disc/`, and snapshots, updates cloned build metadata, and
  regenerates `arm.duckdb` from cooked catalogs with the current code.
- This is appropriate for deterministic ARM derivations such as
  `derived_stellar_classifications`; it is not a substitute for full ingest
  when source catalog rows, canonical identity, search terms, or core inventory
  must change.
- Promotion remains explicit and should be followed by `verify_build.sh`,
  known-system API checks, and focused browser checks before deployment.
