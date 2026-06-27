# Spacegate Milestones (Gaia-First Roadmap)

This document restores and organizes long-range goals, design intent, and idea backlog into a dependency-driven execution plan.

Authoritative architecture and contracts remain in:

- `docs/PROJECT.md`
- `docs/SCHEMA_CORE.md`
- `docs/SLICE_PROFILES.md`
- `docs/SCHEMA_DISC.md` (disc contract)
- `docs/SCHEMA_RIM.md` (rim contract)

## North-Star Product Intent

Spacegate should be:

1. Scientifically trustworthy.
2. Genuinely fun to explore.
3. Useful for both curious non-experts and serious worldbuilders.

### Rule of Cool (Content Prioritization)

High-interest object types should be explicitly prioritized in enrichment and presentation:

- complex multi-star systems
- unusual planets (ultra-short-period, hell worlds, water/ice worlds, eyeball candidates)
- compact-object systems (pulsar planets, exotic remnants)
- nearby "go outside and observe" targets

### Backyard Bonus

Objects visible to amateurs should get special UX treatment:

- practical observing context
- quick-reference viewing notes
- optional telescope guidance where defensible

### Worldbuilding Constraint

Fiction overlays are first-class product features, but must remain fully separated from canonical astronomy.

## Dependency Graph (High Level)

1. Gaia-first canonical `galaxy`
2. Deterministic `core` + `halo` projections and API performance
3. Multiplicity hierarchy reliability
4. Disc factual layer (scores/facts/links)
5. Visual storytelling (snapshots + generated imagery)
6. 3D runtime and deep navigation
7. Rim/worldbuilding tooling
8. Procedural system generation tools
9. Community/engagement overlays

Downstream milestones must not bypass upstream quality gates.

## Milestone Plan

### M0. Baseline Stability (Completed)

Status: largely complete.

Scope:

- reproducible build/promote/verify pipeline
- admin/auth controls
- coolness and snapshots baseline
- operational deployment path

Exit criteria:

- deterministic reruns
- provenance and QC gate enforcement
- stable public serving path

### M0.5. Admin Runtime Hardening (Near-Term Backlog)

Goal:

- keep Photon Admin safe enough for LAN operations while preserving development
  velocity on Admin v2.

Dependencies:

- M0 complete

Delivered:

- API container runs as the invoking host UID/GID instead of root
- generated/admin state permission normalizer with dry-run default
- API container drops Linux capabilities, blocks privilege escalation, and uses
  a read-only root filesystem with explicit tmpfs scratch mounts

Remaining hardening backlog:

- move from operator UID to a dedicated `spacegate-run` service user with shared
  `spacegate` group access
- evaluate Docker `userns-remap` or rootless Docker on Photon, accounting for
  NVIDIA/vLLM compatibility
- move OIDC/provider secrets out of expanded Compose environment where feasible
  and into mounted secret files or a secrets manager pattern
- add a redacted Compose/runtime diagnostics command so operators do not
  accidentally print secret-bearing `docker compose config` output
- decide whether public-edge `antiproton` needs a stricter Compose profile than
  Photon development

### M1. Gaia Galaxy Backbone Pilot (Current Critical Path)

Goal:

- establish Gaia as canonical star inventory substrate for `<1000 ly` `galaxy`.

Dependencies:

- M0 complete

Deliverables:

- `gaia_backbone` download/cook/ingest path
- immutable `galaxy` artifact contract for build outputs
- quality-tier metadata (`poe`, `ruwe`, astrometry flags)
- `gaia_backbone_report.json` with counts/runtime/storage

Success criteria:

- deterministic repeated builds
- clear quality-band accounting
- acceptable proton runtime/memory envelope

### M2. Core/Halo Product Slice and API Transition

Goal:

- serve fast, stable UX from deterministic `core` with opt-in `halo`.

Dependencies:

- M1 complete

Deliverables:

- `core_product_slice` policy and `halo_complement` materialization
- versioned slice profile catalog and profile selection contract
- profile-scoped SLO gates wired into promotion decisions
- API compatibility review and migration notes
- deep-query mode against `halo`/`galaxy`

Success criteria:

- default API latency targets met
- no functional regressions in search/detail workflows
- explicit documented slice/SLO policy and pass/fail evidence per promoted build

### M3. Multiplicity and Systems-of-Systems

Goal:

- move from flat grouping heuristics to explicit hierarchy confidence.

Dependencies:

- M1, M2 complete

Deliverables:

- NSS/MSC/WDS/ORB6 reintegration on Gaia IDs
- confidence-tiered hierarchy materialization
- parent/child subsystem navigation model

Success criteria:

- benchmark systems (for example Castor-class complexity) improve or hold
- grouping confidence is queryable and user-visible
- conservative defaults avoid silent over-grouping

### M4. Crosswalk and Naming Quality

Goal:

- replace AT-HYG convenience dependence with stronger dedicated crosswalks.

Dependencies:

- M1, M2, M3 complete enough for comparative evaluation

Deliverables:

- replacement alias/ID crosswalk ingestion
- common-name authority ingestion and merge policy:
  - bright-star/common-name authority source with explicit provenance
  - deterministic precedence, dedupe, and conflict handling across proper/common names, Bayer/Flamsteed, and catalog IDs
  - fuzzy/common-name lookup acceptance set for benchmark objects (for example Aldebaran-class misses)
- authoritative Sol-system bootstrap ingestion (Sun + 8 planets + key dwarf planets with source-faithful scientific classes + UI supergroup compatibility) with fixed high-confidence provenance
- host-match quality verification (planets)
- search/display naming quality parity or improvement

Success criteria:

- no critical identifier regressions
- benchmark common-name lookups resolve reliably with fuzzy matching and alias-aware ranking
- benchmark lookup ergonomics maintained or improved

### M5. AT-HYG Retirement

Goal:

- remove AT-HYG from canonical inventory path.

Dependencies:

- M4 acceptance criteria met

Deliverables:

- parallel-run comparison report (legacy vs Gaia-first)
- removal of AT-HYG canonical inventory dependence in `galaxy` build path
- cleanup of deprecated code paths/docs

Success criteria:

- objective parity/improvement gates pass
- no production dependency on AT-HYG for canonical rows

### M5.3 Exoplanet Multi-Catalog Lifecycle Ingest

Goal:

- ingest broader exoplanet source layers while preserving a single canonical policy for status and provenance.

Dependencies:

- M2 and M3 stable
- M4 host/crosswalk quality accepted

Deliverables:

- multi-catalog exoplanet layer ingestion:
  - NASA Exoplanet Archive remains canonical confirmed baseline
  - exoplanet.eu status layer (`candidate`, `controversial`, `retracted` tracking)
  - OEC supplemental alias/architecture layer
  - HWC comparison/feature layer (non-canonical score ownership)
- deterministic lifecycle policy materialization:
  - `candidate` included by default
  - `controversial` stored and queryable but default-off
  - `retracted` excluded from science defaults and retained as tombstoned lineage for audit/rim continuity
- overlap/contribution accounting for each catalog source and source-combination
- source-delta workflow:
  - per-source snapshot diff
  - impacted-row re-evaluation planning
  - end-of-run lifecycle delta report

Success criteria:

- status precedence is deterministic and documented
- lifecycle transitions are reversible and lineage-complete
- catalog contribution reports are generated on every refresh
- no canonical/provenance ambiguity introduced by multi-catalog overlap

### M5.5 Planet Taxonomy and Habitability

Goal:

- classify planets with observation-grounded tags and expose a deterministic habitability ranking/search surface.

Dependencies:

- M2 and M3 stable
- M4 host/crosswalk quality accepted
- M5.3 accepted

Deliverables:

- lifecycle materialization and pruning policy:
  - `candidate` included by default
  - `controversial` stored but default-off via UI/API toggle
  - `retracted` excluded from science defaults, retained as tombstoned provenance for audit and rim references
- planet taxonomy tagger using observational/derived science fields (for example):
  - size/mass class (`sub-Earth`, `super-Earth`, `mini-Neptune`, `Neptune`, `Jovian`, `super-Jovian`)
  - insolation/temperature class (`hot`, `warm`, `cold`, inferno/ice thresholds)
  - orbital class (`USP`, short/long period, eccentric, circumbinary when evidenced)
  - detection-method tags and host-context tags
  - stellar-spectroscopy-informed element-richness proxy tags for rim/search context
- Spacegate habitability scorer (`spacegate_hab_score`) with confidence and reason flags
- searchable score controls:
  - habitability slider
  - quick query for top-N most habitable planets
- comparison report against external habitability references (for example HWC) without delegating canonical score ownership

Success criteria:

- taxonomy tags are deterministic and reproducible across rebuilds
- lifecycle toggles and pruning behavior are auditable and reversible via lineage
- default product views match policy (`candidate` on, `controversial` off, `retracted` hidden)
- habitability ranking query latency remains within core slice SLO targets

### M5.7 Local Inference Adjudication Bench

Goal:

- establish a repeatable local-model evaluation harness for scientific adjudication, with small/medium models handling routine extraction and review while the strongest available local model handles only the ambiguous high-impact tail.

Dependencies:

- M5.3 and M5.5 queue surfaces sufficiently stable
- local inference runtime on Photon available through an OpenAI-compatible endpoint

Deliverables:

- benchmark "astronomer adjudicator" cook-off over fixed golden dossiers, including Castor-class multiplicity, exoplanet host binding, lifecycle conflicts, and source-contradiction cases
- role-based agent evaluation harness with tracked golden cases and reproducible reports
- quarantined anomaly inbox for catalog conflicts, source conflicts, schema gaps, stale consensus, plausibility failures, and interesting hypotheses discovered during evaluation or later agent runs
- cost/latency budget model for overnight final-adjudication batches, including tokens/sec, wall time per case, context length, and accepted/rejected/deferred outcomes
- model-role routing policy:
  - fast extractor/reviewer model for most source triage and structured claim checks
  - medium model for narrative/factsheet drafting and contradiction summaries
  - strongest local model for final ambiguous adjudication packets
  - frontier/cloud escalation only when local models abstain or disagree on high-impact cases
- pinned local inference metadata for every generated/reviewed output: model id, quantization, runtime, prompt version, context limit, temperature, token limits, and generation metadata
- TurboQuant KV-cache evaluation pinned as a future optimization experiment for longer-context adjudication profiles, without treating it as a substitute for model-weight compression

Success criteria:

- golden adjudication cases are reproducible across reruns
- local-model review catches seeded unit, identity-binding, and source-conflict faults
- overnight batch profile has an explicit throughput floor and stop condition
- accepted claims remain backed by source IDs and reviewed evidence, not opaque model reasoning

### M5.75 Agent Prompt-Injection and Tool-Security Hardening

Goal:

- harden the AI Astronomy Agency against malicious or adversarial source text,
  prompt injection, tool misuse, and unreviewed publication paths.

Dependencies:

- M5.7 evaluation harness
- M5.8 Admin source-policy controls

Deliverables:

- malicious-source fixture set covering prompt injection, citation spoofing,
  tool-call coercion, schema smuggling, hidden instructions, and cross-object
  claim poisoning
- source-text isolation rules for retriever/extractor/reviewer prompts
- allowlist and source-trust enforcement tests for every retrieval and context
  assembly path
- tool-boundary policy so agent-generated text cannot directly trigger shell,
  database, filesystem, deployment, or publication actions
- publication gate requiring reviewed citations, claim subjects, and verdict
  state before any public `disc` materialization
- Admin-visible security findings and anomaly inbox routing for suspicious
  sources or model behavior

Success criteria:

- seeded prompt-injection fixtures fail closed
- agent roles preserve source text as evidence, not instructions
- no agent workflow can mutate `core`, publish public claims, or execute tools
  from untrusted source content

### M5.8 Admin v2 Control Plane

Goal:

- rebuild Admin as the operating console for builds, dataset state, inference,
  evidence portfolios, review, and audit.

Dependencies:

- Admin auth/API v2 baseline
- M5.7 evaluation harness for inference workspace grounding

Deliverables:

- dedicated Admin v2 frontend shell instead of the large embedded FastAPI HTML page
- Overview, Builds, Dataset, Object Diagnostics, Inference, Agency, Runtime,
  Operations/Jobs, and Audit workspaces as defined in `docs/ADMIN_V2.md`
- Inference workspace with endpoint probes, model-role routing, generation smoke tests, and eval report history
- Evidence Portfolio journal surface where each agent step is captured as a plain-language, linkable, source-backed entry
- Agency source allowlist management backed by repo defaults, runtime JSON
  overrides, source enabled/disabled state, and restore/version controls
- bulk research/source-document storage rooted at `/mnt/space/spacegate`, with durable hashes and references in admin/disc state

Success criteria:

- existing auth, CSRF, job runner, and audit behavior preserved
- operators can understand current build, dataset, inference, and review state from Admin without shell access
- agent activity is inspectable as a chronological evidence narrative rather than opaque model output
- no Admin workflow can mutate `core` or publish unreviewed scientific overlays

### M6. External Links and Factual Disc Layer

Goal:

- provide deeper exploration context with strict factual grounding.

Dependencies:

- M2+ stable

Deliverables:

- curated external reference links (authority allowlist)
- structured factsheets with provenance pointers
- exposition generation from factsheets with factuality guardrails

Success criteria:

- no uncited generated claims in user-facing exposition
- clear model/version/prompt provenance

### M7. Visual Storytelling Expansion

Goal:

- extend deterministic snapshots into richer but traceable generated visuals.

Dependencies:

- M6 factual layer

Deliverables:

- snapshot coverage expansion policy
- generated image pipeline with prompt/provenance storage
- confidence/accuracy labeling for generated visualizations and animations

Success criteria:

- deterministic artifact identity and reproducibility metadata
- explicit user-visible confidence tags

### M8. 3D Map Runtime

Goal:

- deliver a performant, navigable browser 3D map over Gaia-first slice/backbone.

Dependencies:

- M2, M3, M6 foundationally complete

Deliverables:

- browser 3D viewer (camera controls, selection, tooltips)
- level-of-detail strategy and floating-origin handling
- system detail navigation from map selection

Success criteria:

- interactive performance on mid-tier consumer hardware
- stable object selection and context handoff to detail views

### M8.0. 3D Map Pilot

Goal:

- ship a beautiful, Sol-centered 100 ly map that proves the public navigation,
  rendering, and selection model before deep tiling.

Dependencies:

- M2 stable public serving path
- React 19 public web upgrade

Delivered baseline:

- React 19 + Three.js/R3F stack selected for the public map
- dedicated `/api/v1/map/systems` compact 100 ly endpoint
- lazy-loaded `/map` route
- guided flight controls: WASD, mouse look, `Q` up, `Z` down, Shift boost
- stabilized vertical, Sol marker, distance rings, sparse priority labels,
  reticle selection, HUD summary, priority contacts, and detail-page handoff
- Playwright desktop/mobile visual checks confirm a nonblank WebGL canvas, HUD
  rendering, selected-system state, and no page errors

Remaining pilot polish:

- performance budget measurement on Photon and a mid-tier browser
- tighten map label priority and theme-specific HUD styling after visual QA
- document the ICRS-to-scene vertical mapping and future galactic-frame
  transform in public-facing copy where needed

Success criteria:

- interactive 100 ly map renders reliably without blocking search/detail users
- visitors can fly, select, and open system details without orientation loss
- implementation remains layer-ready for future tiled science, extended-object,
  system-simulation, and rim overlays

### M8.1. Tiled Deep Map

Goal:

- expand the map to 250 ly and 1000 ly using explicit tile/LOD loading rather
  than one large browser payload.

Deliverables:

- tile manifest/artifact contract, likely aligned with Morton/spatial indexing
- nearby detailed tiles plus far coarse/priority samples
- selection handoff that remains stable across tile unload/reload
- tile cache and loading telemetry

### M8.2. Extended Objects and Background Sky

Goal:

- add non-stellar public landmarks without conflating them with core star
  systems.

Deliverables:

- evaluated source policy for Messier/NGC/IC-style objects and nearby nebulae,
  clusters, and galaxies
- separate extended-object map layer with object type, position, extent,
  confidence, provenance, and presentation assets
- license-reviewed sky/background layer beyond the 1000 ly local sphere

### M8.3. System Simulation Scenes

Goal:

- replace static browser snapshots with live 3D system previews where supported,
  while keeping deterministic snapshots as fallback artifacts.

Deliverables:

- mesh/material scene components for stars and planets
- explicit assumptions for generated planet surfaces and missing values
- fallback rules for browsers or devices that cannot support 3D previews

### M8.4. Time and Rim-Ready Rendering

Goal:

- prepare the map for client-side time flow, proper-motion/orbit presentation,
  and rim infrastructure meshes.

Deliverables:

- client-side simulation clock contract
- epoch/proper-motion rendering policy that never overwrites canonical stored
  coordinates
- rim render layers for gates, stations, orbital rings, elevators, routes,
  ships, facilities, and namespace visibility controls

### M9. Rim and Worldbuilding Tooling

Goal:

- support creative overlays without contaminating scientific core.

Dependencies:

- M8 map/runtime baseline

Deliverables:

- rim namespace/entity tooling
- map-editable overlay primitives:
  - trade lanes
  - spacegate links
  - spheres of control
  - megastructure placement

Success criteria:

- strict data-layer separation enforced
- rim operations do not mutate core/disc canonical science rows

### M10. Procedural System Generator (God Tools)

Goal:

- allow users to generate/alter full system scaffolding while preserving scientific-layer immutability.

Dependencies:

- M6 factual disc layer
- M7 visual storytelling baseline
- M8 3D map/runtime baseline
- M9 rim overlay/entity tooling

Deliverables:

- generator contract for rim-authored structures anchored to canonical system nodes
- deterministic generator metadata (`seed`, `model_version`, `parameter_json`, provenance)
- generated object/edge families:
  - planets, moons, belts/cloud regions, infrastructure scaffolds
  - containment spine + relation-edge overlays (no containment cycles)
- policy controls to prevent scientific-row mutation and preserve canonical references

Success criteria:

- generated systems remain queryable/navigable through the same graph model as canonical systems
- containment tree integrity holds (`contains` acyclic, one canonical parent)
- generated overlays can be fully disabled without impacting canonical science behavior

### M11. Engagement and Community Overlay

Goal:

- incorporate public curiosity signals safely and transparently.

Dependencies:

- M6+ mature enough to prioritize enrichment from demand signals

Deliverables:

- privacy-safe engagement schema
- ranking overlays and public profile presets (non-canonical)
- moderation and abuse controls for shared overlays

Success criteria:

- no personal tracking creep
- canonical scientific ranking remains isolated from social overlays by design

## Idea Backlog (Restored and Organized)

These are preserved product ideas from earlier planning notes, reordered by likely dependency:

### Enrichment and Discovery

- prioritize exotic, high-narrative systems for early enrichment
- "Backyard bonus" fast-path for observable nearby targets
- highlight category-based discovery paths (habitable candidates, inferno worlds, compact-object systems)

### Visualization and Image Direction

- system-level generated visuals centered on dominant dynamics
- planet-level generated views (global and speculative surface interpretation)
- explicit captions describing known data vs inferred visualization elements

### Map and Interaction

- deep hierarchy navigation (system -> subsystem -> component)
- confidence-aware animation controls with visible parameterization
- optional orbit/motion projection controls over long timescales

### Worldbuilding Features

- worldbuilder overlays for:
  - trade lanes
  - imperial borders/spheres
  - infrastructure/megaproject annotations
- free-floating rim entities not anchored to real objects

### Restored Concept Notes (Curated from Prior Backlog)

- System rendering priorities:
  - emphasize complex and exotic systems in default discovery surfaces
  - center close binaries/planetary dynamics while still depicting distant companions
  - preserve scientific grounding while allowing visibility-oriented exaggeration in derived visuals
- Planet visualization motif set (derived, clearly labeled as inferred):
  - volcanic worlds
  - water worlds
  - ice worlds
  - desert worlds
  - hell worlds
  - acid worlds
  - ringed worlds
  - dead worlds
- Generated media UX ideas:
  - shareable captioned outputs
  - confidence/accuracy badge on every generated artifact
  - prompt/provenance tooltip visibility
  - optional popularity ranking in non-canonical experience layers
- Worldbuilding object examples to retain in rim layer planning:
  - solar collectors
  - foundries
  - shipyards
  - Dyson swarms
  - colonies
  - momentum banks
  - space elevators
  - mines and mass drivers
  - stations and gates
- Community-facing backlog (non-canonical):
  - public coolness profile presets
  - community ranking overlays isolated from canonical science ranking

## Governance Rule

No milestone in M6+ should compromise M1-M5 scientific integrity gates.

If there is conflict:

1. protect core correctness
2. keep derived content explicitly labeled
3. delay feature launch rather than blur canonical truth boundaries
