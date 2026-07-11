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

1. Gaia-first canonical `core`
2. Deterministic served slices and API performance
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

- maintain host-local private security findings outside the public repo at
  `/srv/spacegate/private/security/SECURITY_AUDIT.md`; tracked docs should
  reference this audit log but not duplicate sensitive host details or secrets
- move from operator UID to a dedicated `spacegate-run` service user with shared
  `spacegate` group access
- evaluate Docker `userns-remap` or rootless Docker on Photon, accounting for
  NVIDIA/vLLM compatibility
- move OIDC/provider secrets out of expanded Compose environment where feasible
  and into mounted secret files or a secrets manager pattern
- add a redacted Compose/runtime diagnostics command so operators do not
  accidentally print secret-bearing `docker compose config` output
- add a pre-hardening/pass-close security scan routine using OpenAI's git
  security inspector or another local secret scanner, followed by manual staged
  diff review
- decide whether public-edge `antiproton` needs a stricter Compose profile than
  Photon development
- gate public-edge Admin routes at the reverse proxy layer: protect `/admin`
  and `/api/v2/admin/*` with an outer control such as VPN/Tailscale, IP
  allowlist, or basic auth; optional path obfuscation can reduce bot noise but
  must not be treated as the security boundary
- normalize `antiproton` runtime identity and data ownership:
  current production hotfix pins containers to the legacy `ubuntu:ubuntu`
  data-owner UID/GID so SQLite admin state can write WAL/shm files; the final
  state should use a dedicated non-login `spacegate` runtime user,
  `spacegate` group-owned `/srv/spacegate/data`, and `sgdeploy` only as the
  deployment/restart account
- reassess `sgdeploy` membership in the Docker group; broad Docker access is
  effectively root-equivalent and should eventually become a narrower deploy
  control path if practical

### M1. Gaia Core Backbone Pilot (Current Critical Path)

Goal:

- establish Gaia as canonical star inventory substrate for `<1000 ly` core
  builds.

Dependencies:

- M0 complete

Deliverables:

- `gaia_backbone` download/cook/ingest path
- immutable `core` artifact contract for build outputs
- quality-tier metadata (`poe`, `ruwe`, astrometry flags)
- `gaia_backbone_report.json` with counts/runtime/storage

Success criteria:

- deterministic repeated builds
- clear quality-band accounting
- acceptable proton runtime/memory envelope

### M2. Core Product Slice and API Transition

Goal:

- serve fast, stable UX from deterministic `core` slices.

Dependencies:

- M1 complete

Deliverables:

- `core_product_slice` policy
- versioned slice profile catalog and profile selection contract
- profile-scoped SLO gates wired into promotion decisions
- API compatibility review and migration notes
- documented path for future larger/deeper data products without changing the
  default public hot path

Success criteria:

- default API latency targets met
- no functional regressions in search/detail workflows
- explicit documented slice/SLO policy and pass/fail evidence per promoted build

Current status:

- June 29, 2026 public slice build `20260629T_public_aliasfix_v3_side`
  applies `core.public@v3` trimming to `core.duckdb` and also slices
  `arm.duckdb`, `canonical_hierarchy.duckdb`, and `disc.duckdb` side artifacts
  for antiproton deployment. The build retains about 5.87M systems/stars,
  reduces ARM from 5.5 GiB to about 2.9 GiB, canonical hierarchy from 1.4 GiB
  to about 568 MiB, and disc from about 971 MiB to about 597 MiB while passing
  Sol S2-S4, Castor multiplicity, alias-search, and known-system API gates.
- `docs/PUBLIC_DEPLOYMENT.md` now documents the Photon-to-antiproton public
  path: publish archive, activate runtime DB, deploy code, verify public API,
  keep rollback build, and use SSH cooldown discipline for UFW/fail2ban.

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
- transitional AT-HYG alias crosswalk remains enabled for production-quality
  naming coverage until replacement common-name and identifier authorities are
  ready; AT-HYG does not contribute canonical inventory rows
- deterministic Bayer expansion for search/display ergonomics, including
  Greek-letter plus constellation-genitive forms such as `Alpha Centauri`
- common-name authority ingestion and merge policy:
  - bright-star/common-name authority source with explicit provenance
  - deterministic precedence, dedupe, and conflict handling across proper/common names, Bayer/Flamsteed, and catalog IDs
  - fuzzy/common-name lookup acceptance set for benchmark objects (for example Aldebaran-class misses)
  - alias scope handling so member aliases, subsystem aliases, planet-host
    aliases, and whole-system aliases can all be searched without promoting a
    member-only name into a false system display name
  - preferred display-name policy shared by Star Search, the 3D map, system
    pages, and API summaries; recognizable common/proper names and expanded
    Bayer/Flamsteed forms should outrank terse catalog labels when evidence
    supports them
  - golden lookups for Gliese/GJ nearby-star names, Bayer/common-name pairs,
    variable-star names, WDS identifiers, and public UX names such as Gliese
    412, Alpha Librae/Zubenelgenubi, Alpha Centauri/Toliman, Barnard's Star,
    Wolf 359, Vega, Fomalhaut, Gliese 643, and VB 8
  - guardrails for bright primary plus compact companion systems where Gaia may
    include the companion but not the naked-eye primary; Sirius currently
    exposes this failure mode because the served build binds Sirius/Alpha CMa
    aliases, HIP 32349, and HD 48915 to the Gaia white dwarf row while Sirius A
    is absent
  - June 29, 2026 ingest guard prevents non-compact AT-HYG positional alias
    rows from matching compact-object/white-dwarf Gaia targets and prevents
    weak positional AT-HYG matches from promoting HIP/HD/HR/GL/TYC/HYG
    identifiers; a rebuild is still required to repair served Sirius rows
  - June 29, 2026 compact-alias safety verifier
    (`scripts/verify_compact_alias_safety.py`) detects Sirius-class builds
    where a compact-object row with no non-compact sibling carries
    bright-primary AT-HYG aliases plus HD/WDS or non-proper primary aliases.
    `scripts/verify_build.sh` runs it in warn-only mode by default; set
    `SPACEGATE_VERIFY_COMPACT_ALIAS_SAFETY=1` after rebuilding clean artifacts
    to make the gate strict.
  - June 29, 2026 accepted supplement path adds
    `config/core_accepted_supplements.json` plus ingest support for reviewed
    Gaia-missing core inventory exceptions. Sirius A is the first accepted
    supplement and Sirius B has a reviewed WDS component link; a rebuild is
    still required before served artifacts reflect the repair.
  - June 30, 2026 unpromoted test rebuild
    `20260629T_sirius_accepted_supplement_test` verified that Sirius becomes a
    two-star WDS-backed system with Sirius A from
    `athyg_accepted_supplement` and Sirius B from Gaia without HIP/HD leakage.
    The build is not promotable yet: compact-alias safety still reports
    44/Zeta Per, Phi Per, 17 Vul, and 105 Tau hazards. A direct AT-HYG Gaia
    alias guard has a focused SQL reproduction pass for those four hazards, but
    a full rebuild is still required to materialize it. Nu Sco also exposed a
    general ARM materialization gap: source-native MSC endpoint labels such as
    `B`, `C`, `Ac`, `Da`, and `Db` must become deterministic ARM leaf nodes
    when they appear in `sys.tsv`/`orb.tsv`, while `core.systems.star_count`
    remains a hot-path summary rather than the authority for nested MSC leaf
    counts. A scratch ARM rebuild verifies the corrected seven-leaf Nu Sco
    shape.
  - June 30, 2026 full local rebuild
    `20260630T_sim_beta_data_foundation` materialized the Sirius accepted
    supplement, direct AT-HYG Gaia alias guard, and Nu Sco source-leaf fix. It
    passed strict compact-alias safety and multiplicity goldens, but is not
    promotable: simulator-oriented orbital normalization found duplicate
    rank-1 planet orbit solutions caused by duplicate `core.planets` rows with
    the same `stable_object_key`. The planet builder now ranks source rows and
    host-match fan-out before writing `core.planets`, and `verify_build.sh`
    now gates duplicate planet stable keys.
  - June 30, 2026 local served build
    `20260630T_sim_beta_api_alias_v4` materialized the planet stable-key
    de-duplication, accepted-supplement no-Gaia alias restoration, and
    source-native ARM hierarchy API preference. It passes strict build
    verification, orbital normalization, multiplicity goldens, alias search,
    and `scripts/verify_known_systems_api.py` for Castor, Nu Sco, Alpha
    Centauri, Sirius, Proxima Centauri, TRAPPIST-1, 55 Cnc, Sol, and 16 Cyg.
- authoritative Sol-system bootstrap ingestion (Sun + 8 planets + key dwarf planets with source-faithful scientific classes + UI supergroup compatibility) with fixed high-confidence provenance
- catalog-ID linkout registry:
  - collapsed catalog-ID section on system pages with full-ID copy controls and
    curated outbound links
  - deterministic URL templates for reliable destinations such as CDS/SIMBAD
    object resolution, VizieR/catalog searches, Gaia Archive source pages or
    queries, NASA Exoplanet Archive host/planet lookups, and other catalog
    authorities where the identifier is valid for that target
  - optional build/admin validation job with cached status and TTL so UI link
    pills are only shown when the destination is expected to produce useful
    results
  - linkout artifacts belong in `disc` or presentation build outputs, not
    canonical `core`; they are research/navigation affordances rather than
    source facts
- host-match quality verification (planets)
- search/display naming quality parity or improvement

Success criteria:

- no critical identifier regressions
- benchmark common-name lookups resolve reliably with fuzzy matching and alias-aware ranking
- benchmark lookup ergonomics maintained or improved

Current status:

- June 29, 2026 local canonical build `20260628T234531Z_da18d11_aliasfix`
  restored AT-HYG common-name alias coverage, materialized expanded Bayer
  aliases, promoted 1,017,911 aliases and 18,603,929 system search terms, and
  passed the alias-search gate for Castor, Alpha Geminorum, Alpha Centauri,
  Toliman, Sirius, Jabbah, and Copernicus.
- The alias-search gate now runs from `scripts/verify_build.sh` so future builds
  fail when the broad alias corpus or benchmark common-name lookups regress.
- Full common-name authority ingestion and conflict policy remain future work;
  restored AT-HYG aliases are a transitional compatibility layer, not the final
  naming authority.

### M5. AT-HYG Retirement

Goal:

- remove AT-HYG from canonical inventory path.

Dependencies:

- M4 acceptance criteria met

Deliverables:

- parallel-run comparison report (legacy vs Gaia-first)
- removal of AT-HYG canonical inventory dependence in the core build path
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
- v0.2 mobile controls: one-finger drag-look, tap/select-reticle selection,
  two-finger pinch flight, and two-finger pan
- stabilized vertical, Sol marker, distance rings, sparse priority labels,
  reticle selection, compact header readouts, selection history pills, and
  detail-page handoff
- beta HUD declutter: long-ID truncation with hover/tap metadata popovers and
  copy controls, selected-name detail links, and tighter desktop/mobile layouts
- deterministic snapshots remain system-simulation fallback/reference
  artifacts; the map no longer shows a selected-system snapshot status pill
- ephemeral route measurement overlay: right-click a target system, measure
  from the selected system, draw per-leg distance lines, show recent leg list
  and total distance, and undo/clear without persisting Rim/worldbuilding route
  data
- public system simulation scene-readiness endpoint:
  `/api/v1/systems/{system_id}/simulation-scene`
- first lazy-loaded live system preview panel on system detail pages, backed by
  the scene-readiness endpoint and covered by browser smoke tests
- Playwright desktop/mobile visual checks confirm a nonblank WebGL canvas, HUD
  rendering, selected-system state, and no page errors
- compact browser map profile (`compact=true`) trims the 100 ly local JSON
  payload from about 5.3 MB to about 3.0 MB while preserving full diagnostic
  records without the compact flag
- orientation contract documented: scene up is canonical `z_helio_ly` mapped
  onto Three.js Y and is not a galactic-north claim
- two-layer GPU point rendering for the stellar cloud, with circular bright
  core sprites, spectral halo sprites, and persisted `Discovery`/`Realistic`
  Star Style modes. Discovery emphasis is browser presentation only and must
  never be interpreted as physical luminosity or written back into science
  layers.

Remaining pilot polish:

- tighten map label priority after visual QA
- improve fallback Gaia-label display priority for mobile selection sheets and
  in-scene labels
- refine route measurement dismissal/edit affordances after public beta use
- measure real-device public load on a mid-tier mobile browser

Success criteria:

- interactive 100 ly map renders reliably without blocking search/detail users
- visitors can fly, select, and open system details without orientation loss
- implementation remains layer-ready for future tiled science, extended-object,
  system-simulation, and rim overlays

### M8.0a. Live-WebGL Runtime Manager

Goal:

- make Spacegate exploration live-WebGL-first while keeping static artifacts as
  last-resort fallback, not the normal presentation path.

Deliverables:

- global WebGL runtime/context budget across Star Map, Peek, Explorer, and Star
  Search result-card previews
- reusable live-preview pool for search cards so visible cards do not create
  unbounded independent WebGL contexts
- adaptive quality profile for device class, viewport, DPR, context-loss
  frequency, and user-selected map radius
- lifecycle rules that pause/unmount lower-priority previews when Peek or
  Explorer is active
- client telemetry overlay and diagnostics for FPS, active WebGL contexts,
  context-loss recoveries, preview pool pressure, and dropped quality tier
- fallback-last policy: live WebGL recovers/remounts after transient context
  loss; deterministic/static artifacts remain for no-WebGL, failed scene-load,
  crawler/share surfaces, and repeated unrecoverable failures

Implemented foundation:

- Star Map owns centralized runtime state for active WebGL surfaces, a
  search-card live-preview pool, context-loss recovery count, and adaptive
  quality tier
- runtime diagnostics overlay is toggled from the map burger menu and reports
  FPS, active WebGL surfaces, preview-pool pressure, recovery count, and quality
  tier
- search-card previews unmount when Peek/Explorer is active and remount from
  the pool when the user returns to flight/search browsing
- map and System Simulation canvases receive adaptive DPR profiles from the
  runtime quality tier

Future deep-map constraint:

- design the runtime manager to support user-selectable 100/250/500/1000 ly map
  radii, but do not ship those radii as a single monolithic payload. Larger
  radii require tile/LOD loading, label density controls, priority sampling,
  and public-edge slice/SLO checks before promotion.

Success criteria:

- fast-scrolling Star Search result cards no longer crash the background map or
  active Peek/Explorer canvas
- users can enable live FPS diagnostics from the map menu
- the interface remains smooth on tested mobile hardware at the current 100 ly
  radius and degrades quality rather than failing when context pressure rises

### M8.0b. Star Search v2 and Simulation-First System Pages

Goal:

- make `/search` the structured public catalog/search counterpart to the 3D
  map and make `/systems/{system_id}` readable for laypeople without hiding
  scientific evidence.

Implemented foundation:

- standalone Star Search result cards now use bounded System Simulation
  previews with first-frame capture reuse and hover/focus live promotion,
  avoiding unbounded WebGL contexts in long result lists
- deterministic snapshot rows remain fallback/reference metadata rather than
  the preferred capable-browser visual
- public system pages now lead with System Simulation, quick facts,
  plain-language overview, why-it-matters notes, habitability context,
  what-we-know notes, uncertainty notes, explore-more prompts, and concept
  explanations that AAA can enrich later
- the System Hierarchy section now presents a reader-facing object tree with
  stellar class pills, planet/orbit tags, compact vitals, and short explanatory
  summaries before lower-level catalog/evidence tables
- raw catalog rows, eclipsing evidence, snapshot metadata, and provenance are
  still available in secondary/collapsible sections
- `sort=match`/Relevance is available for named Star Search queries while
  blank browsing falls back to coolness/name/distance modes
- stellar-class education chips now appear across Star Search cards, map-native
  search cards, system hierarchy leaves, and System Simulation readouts. The
  first pass covers O/B/A/F/G/K/M/L/T/Y, Wolf-Rayet, white dwarfs, neutron
  stars, pulsars, magnetars, black holes, and an explicit unknown class.
- July 10, 2026 System Page Beta pass tightens `/systems/{system_id}` around a
  compact hero, early System Simulation, reader-facing overview cards,
  At-a-Glance facts, denser Stars and Hierarchy rows, full-name orbital
  parameter chips, physical-fact tooltips, and secondary Evidence and
  Technical Data disclosures. Raw catalog rows and coordinates remain
  available without dominating the first viewport.

Future work:

- define public-experience golden review cases distinct from ingestion goldens:
  Tau Ceti, TRAPPIST-1, Alpha Centauri/Proxima Centauri, Sirius, 55 Cnc,
  Epsilon Eridani, Barnard's Star, Wolf 359, Vega, and Fomalhaut
- integrate reviewed AAA public narration into the reserved system-page slots:
  summary, why it matters, what we know, what remains uncertain, worlds and
  orbits, and evidence/further reading
- add clickable educational concept pages for discovery tags such as white
  dwarfs, habitable-zone planets, multistar systems, ultracool dwarfs,
  exoplanets, and planet taxonomy classes. Each concept page should start with
  a plain-language explanation, progressively deepen into the science, link to
  representative systems and related concepts, and offer a "Find more" Star
  Search path with the relevant filter active. The initial backlog, route, and
  page contract are tracked in `docs/CONCEPTS.md`.
- Nearby Ultracool Completeness v1 starter bridge is implemented: vetted
  UltracoolSheet rows within the configured nearby distance cap can enter
  `core.stars` when the Gaia backbone misses them, with source provenance and
  diagnostics. This addresses the immediate Luhman 16 / WISE 0855-0714 blind
  spot class without making CatWISE a default dependency.
- Next data milestone: CatWISE/AllWISE infrared survey integration planning and
  implementation. Decide retrieval footprint, IRSA/AWS source strategy,
  crossmatch policy, storage/retention posture, and whether the products serve
  only ARM evidence or also drive a reviewed nearby brown-dwarf core promotion
  queue. Initial plan: `docs/CATWISE_ALLWISE_PLAN.md`.
- WISE imagery milestone: add an IRSA-backed infrared image panel to system
  pages, with pre-cached cutouts/composites for public UX goldens and
  high-coolness systems, lazy bounded cache for other systems, source links
  back to IRSA, and retained retrieval metadata. This should start with
  AllWISE W1/W2/W3 cutouts and web-friendly generated previews while preserving
  FITS/source metadata for evidence/debug views.
- Multi-wavelength sky-context milestone: design selectable 3D-map sky
  backgrounds, starting with a basic Milky Way visible-light sky and later
  adding infrared, X-ray, and other survey layers. Keep these as presentation
  overlays with source attribution and avoid mixing skybox imagery with the
  canonical object inventory.
- concept pages should include interactive visualizations where they materially
  teach the concept. Candidate examples include a SuperPuff planet model whose
  diffuse envelope expands and escapes as stellar irradiation rises, a
  core-collapse toy model that supernovas when support energy is removed, and
  a compact-remnant mass slider that crosses white-dwarf, neutron-star, and
  black-hole thresholds with clear caveats.
- add optional future RIM/pop-culture hooks as clearly separated overlays, not
  mixed into canonical science
- complete a tooltip and explanatory-popover audit across Star Search, the 3D
  map, System Simulation, and Admin-facing diagnostics. Every visible pill,
  chip, badge, and compact metric should either be self-evident or expose
  helpful context. Examples: assumption-count pills should list the assumptions
  and provenance; spectral-class pills should explain the class in one readable
  paragraph; compact orbital and habitability tags should teach the underlying
  concept and link toward future concept pages.
- formalize tag priority tiers so tight spaces show only the highest-value
  public tags while richer pages can expose fuller taxonomy and evidence
  breakdowns
- finish high-fidelity static System Snapshot v2 for no-WebGL clients,
  crawlers, share cards, and intentional low-intensity catalog contexts
- deprecate the old prototype deterministic snapshot generator and Admin
  snapshot controls once live simulation previews, cached first-frame captures,
  and the future high-fidelity static generator fully cover fallback needs

Success criteria:

- users arriving by star name or catalog ID can understand a system without
  reading raw table fields first
- system pages clearly distinguish source facts, derived values, assumptions,
  missing data, and presentation-only render choices
- Star Search remains fast and stable on ordinary browsers while using live
  simulation where it adds value

Current golden framework:

- `docs/PUBLIC_UX_GOLDENS.md` defines the public-experience and technical
  stress golden sets
- `srv/web/tests/fixtures/publicExperienceGoldens.mjs` is used by the
  Playwright map suite for search-resolution and system-page anatomy checks
- Vega / Alpha Lyrae / HD 172167 / HIP 91262 is a known current source/alias
  coverage gap on the served build and should be fixed in the data
  reconciliation milestone rather than papered over in frontend presentation

### M8.1. Tiled Deep Map

Goal:

- expand the map to selectable 250 ly, 500 ly, and 1000 ly radii using explicit
  tile/LOD loading rather than one large browser payload.

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

- public scene-readiness contract over current detail, hierarchy, arm graph,
  orbital solutions, and readiness diagnostics
- renderer-ready `render_scene_v0.2` contract with source/derived/assumed/
  missing provenance fields
- deterministic body hierarchy model using containment edges plus separate
  dynamic orbit edges
- orbital-source refresh plan for NASA Exoplanet Archive `ps`/`pscomppars`,
  Gaia DR3 NSS now and Gaia DR4 transition planning, WDS/ORB6, MSC, SBX, and
  JPL Horizons/SBDB
- ARM-normalized planet orbit edges and `source_native_planet_orbit` solutions
  for host-linked NASA Exoplanet Archive and Sol authority planet rows, with
  `core.planets` orbital scalars retained as promoted serving summaries
- NASA Exoplanet Archive `ps` alternate orbital solution ingestion into ARM
  as rank-2+ source-native candidates attached to the existing planet orbit
  edge, while `pscomppars` remains the rank-1 promoted default
- mesh/material scene components for stars and planets
- explicit assumptions for generated planet surfaces and missing values
- visible provenance pills for preview fields, including deterministic
  `ASSUMED` visual priors
- persisted `disc` assumption rows for visualization-only defaults
- defensible wide-orbit presentation policy: source-backed direct/group
  orbits first, DERIVED projected-separation/Kepler presentation estimates
  second, and ASSUMED DISC/render-scene visual orbit fields only as labeled
  fallbacks
- orbit diagnostics that count source direct orbits, source group orbits,
  derived Kepler presentation estimates, assumed visual orbit fields, and
  visual-binary fallbacks for API/browser verification
- fallback rules for browsers or devices that cannot support 3D previews

### M8.3a. High-Fidelity Static System Snapshot Generator

Goal:

- replace the prototype concentric-ring static snapshots with high-quality,
  deterministic, object-level system representations for traditional Star
  Search, no-WebGL clients, crawlers, sharing, and fallback/reference surfaces.

Deliverables:

- snapshot generator that consumes the same `simulation-scene` contract and
  visual scale policy as System Simulation
- deterministic layout that shows multi-star hierarchy, compact objects,
  planets, orbit ordering, habitable/temperature-line context where useful, and
  clear labels without pretending to be physical scale
- theme-neutral SVG or raster output suitable for search result cards and social
  preview metadata
- provenance metadata tying each snapshot to served build, system id,
  render-scene schema, visual-scale version, generator version, and fallback
  limitations
- admin Presentation job for snapshot refresh with bounded batch controls and
  status reporting

Success criteria:

- static search cards are visually useful enough to stand on their own when
  live previews are disabled, unavailable, or intentionally not mounted
- snapshot generation is much lighter than browser-rendered WebGL captures and
  can be run routinely for hot/coolness-selected systems

Deprecation note:

- the original concentric-ring deterministic snapshot generator is now legacy
  fallback/reference infrastructure. Do not expand its Admin control surface.
  Remove or hide those controls after System Snapshot v2 or an equivalent
  no-WebGL/share-card path is available.

### M8.3b. Source Evidence Utilization and Stellar Parameter Normalization v1

Status: locally implemented on photon build
`20260711T_source_evidence_v1_side`.

Goal:

- use source evidence Spacegate already preserves before falling back to
  presentation assumptions in simulation and system-readout paths.

Completed:

- MSC roots now materialize for simple binaries as well as higher-order
  multiples.
- Deterministic MSC `orb.tsv` endpoint pairs materialize into
  `arm.orbit_edges`/`arm.orbital_solutions`; unresolved rows remain audit
  diagnostics.
- 70 Oph is the visible benchmark: endpoint masses and MSC orbital period,
  eccentricity, and inclination now reach `/simulation-scene`.
- Spectral-subclass main-sequence priors now distinguish safe types such as
  K0V and K4V for simulation support while avoiding giants, subgiants,
  remnants, and compact objects.
- Hierarchy-rendered stars now expose `luminosity_lsun` when it is available
  as a source quick fact, or as an ARM-scoped derived
  `stellar_luminosity_from_radius_teff_v1` value when radius and effective
  temperature are present. This restores HZ/temperature-line overlays for
  systems such as TRAPPIST-1 without promoting derived luminosity into core.
- `scripts/audit_source_evidence_utilization.py` now reports broader source
  coverage metrics in addition to MSC gaps.
- Browser Playwright expectations were refreshed for public-full naming,
  cached/full preview policy, embedded Explorer object lists, and failed
  simulation-scene fallback behavior; the full map suite passes locally.

Remaining:

- inspect the six remaining MSC `orb.tsv` rows that do not resolve to ARM
  orbit edges.
- inspect known wide-orbit warning cases where source rows are preserved but
  not yet attached to the simulation tree: Tegmine `orbit:3074`, Xi Scorpii
  `orbit:13178`, and Nu Sco `orbit:5630`, `orbit:5632`, `orbit:13216`.
- design WDS pair-observation utilization separately. The expanded audit shows
  WDS observation rows are preserved, but Spacegate has not yet decided which
  rows should become ARM pair support entities, projection evidence, orbit-edge
  candidates, or diagnostics only.
- decide whether a capped/materialized ARM policy for stellar subclass priors
  is worth the storage cost, or whether runtime provenance remains sufficient.

### M8.4. Admin Map Overlay and AAA Research Promotion

Goal:

- give authenticated admins operational/research controls directly over the 3D
  map and system surfaces without exposing write controls to public users.

Deliverables:

- authenticated admin overlay mode for Star Map and System Explorer
- per-system/object controls for `Needs review`, `Promote to AAA research`,
  evidence portfolio, source-refresh notes, and simulation-readiness gaps
- research queue state separated from public science facts; admin writes must
  not mutate `core`/`arm` source evidence directly
- V1513 Cyg/Wolf 1130 benchmark item for AAA research-promotion flow because it
  exposes compact-remnant, M-subdwarf, T-dwarf, variability, and classification
  presentation nuance
- audit trail for who promoted/reviewed a system and why

Success criteria:

- admins can promote a visible system from map/explorer context into the AAA
  research pipeline
- public visitors see no admin controls and no unreviewed agency output in
  canonical science layers

Readiness gaps:

- MSC source constants now target the upstream June 19, 2026 archive; local
  canonical build `20260628T1210Z_msc20260619` promoted and passed required
  hierarchy/orbit goldens on June 28, 2026
- MSC `sys.tsv` and `orb.tsv` materialization is implemented in the ARM builder:
  scratch verification on June 28, 2026 passed strengthened Castor goldens for
  nested containment, source orbital periods, and endpoint physical-evidence
  policy
- Planet orbit normalization scratch verification on June 28, 2026 passed for
  TRAPPIST-1 source periods/order, 55 Cancri canonical planet coverage, Sol
  planet/moon orbit rows, Castor regression coverage, API smoke checks, and no
  duplicate planet-orbit solution fanout
- Canonical build `20260628T210227Z_bef21ee_fix1` verified on June 28, 2026:
  canonical emit now rebuilds ARM against canonical core keys, full
  `verify_build.sh` passes, TRAPPIST-1 exposes seven ARM planetary orbit
  solutions through `/simulation-scene`, and Nu Sco effective star count is
  preserved at seven through the MSC leaf hierarchy bridge
- Known-system API benchmark `scripts/verify_known_systems_api.py` now checks
  search, aliases, detail hierarchy, vital hierarchy facts, and simulation
  scene availability for Castor, Nu Sco, Alpha Centauri, Sirius, TRAPPIST-1,
  55 Cnc, Sol, and 16 Cyg.
- June 29, 2026 Live System Preview API patch makes `render_scene_v0.2`
  reconcile renderer-ready star/planet bodies against canonical hierarchy when
  hierarchy is richer than direct core membership. Local strict benchmark now
  passes for Castor, Nu Sco, Alpha Centauri, TRAPPIST-1, 55 Cnc, Sol, and
  16 Cyg; Proxima Centauri is tracked as a separate planet-host benchmark.
- June 29, 2026 renderer pass uses hierarchy branches as stable visual cluster
  centers, keeps binary orbit animation within those clusters, and hosts nested
  planet orbits around their render body/group. Playwright now covers hosted
  planet preview paths without requiring Alpha Centauri and Proxima to be
  folded into one rendered system.
  - July 10, 2026 source-object reconciliation pass fixes the Alpha/Proxima
  host/root gap in local canonical build `20260710T144149Z_7989433`: Proxima
  remains the direct Gaia/source planet host for Proxima Cen b/d, while the
  enriched Gaia row inherits MSC/WDS component-C evidence and rolls into the
  accepted Alpha Centauri physical system. The cooker persists accepted
  surrogate merges in `core.source_object_reconciliation` and ambiguous
  candidates in `core.source_object_reconciliation_quarantine`; local build,
  multiplicity goldens, and known-system API checks pass.
- July 10, 2026 Alias and Preferred Display Name Authority v2 implementation
  starts from the post-rollup dataset and adds target-aware
  `system_search_terms`, Gl/GJ/Gliese variant expansion from source catalog IDs,
  and strict exact-query guardrails for dense catalog/variable names. Benchmarks
  include Gliese 412/GJ 412, Gliese 643, Alpha Librae/Zubenelgenubi,
  Alpha/Proxima member context, and the `V1513 Cyg` false-positive guard.
  Canonical build `20260710T181500Z_alias_v2` verifies locally after patching
  the canonical emitter to preserve search-term target context.
  External linkout resolution and full SIMBAD/CDS alias enrichment remain
  separate future milestones.
- July 10, 2026 Name Style Preference and Public Display-Name Policy v2 adds
  `public_full`, `astronomer_abbrev`, `catalog_compact`, and
  `source_technical` display styles to search, map, system detail, and
  simulation-scene APIs. Public Full is the default for lay audiences
  (`Alpha Centauri`, `Epsilon Indi`, `Mu Herculis`), while matched aliases
  remain separate explanation fields. This is a presentation/search policy and
  does not merge/split systems or alter canonical source identity.
- June 30, 2026 subsystem inspection pass adds renderer-ready subsystem bodies
  and subtle hierarchy-center markers for nodes with multiple rendered stellar
  descendants. Castor now verifies inspectable subsystem bodies such as Castor
  AB/A/B/C without creating new science-layer stars or orbit solutions.
- June 29, 2026 interaction pass adds a pauseable local preview clock, sampled
  eccentric/inclined orbit guide paths that match planet motion, and hover
  vitals for rendered stars and planets.
- June 29, 2026 beta simulator pass adds speed control, reset, orbit-trace
  visibility, click/tap pinned inspection with copyable render/source IDs, and
  `group_pair` orbit-guide rendering for hierarchical subsystem edges such as
  Castor A-B and AB-C. Known-system API benchmarks now assert Castor six-star
  render coverage, five Castor orbit entries, TRAPPIST-1 source-backed planet
  periods, and source-backed 55 Cnc/Sol rendered planet periods.
- June 29, 2026 nested-motion pass applies `group_pair` orbit entries as
  visual-scale client transforms for their child clusters. Castor's A/B inner
  binaries can now ride the A-B group motion while the AB cluster rides the
  outer AB-C relation, without rewriting ARM evidence or treating display scale
  as science.
- June 29, 2026 fallback/mobile pass adds live-preview WebGL capability
  detection, in-panel deterministic snapshot fallback, and mobile Playwright
  coverage for the system-detail preview canvas/readout/pin flow.
- June 29, 2026 benchmark-label pass preserves canonical star keys while using
  the best system human alias for single-star preview display names. Proxima
  Centauri now has explicit simulator benchmark coverage for its rendered star
  label and two source-backed planet periods.
- June 29, 2026 provenance-readout pass moves simulator hover/pinned facts onto
  the reusable evidence-pill UI so inspected values expose SOURCE, DERIVED,
  ASSUMED, or MISSING status and pinned readouts provide focusable provenance
  popovers.
- June 29, 2026 visual-material pass adds deterministic procedural star and
  planet surface materials plus bounded visual-scale planet sizing so live
  system previews read as deliberate 3D scenes. These are renderer-only
  presentation transforms over scene fields; they are not source texture maps
  and are not persisted as science-layer values.
- June 29, 2026 selection-feedback pass ties pinned simulator readouts back to
  the WebGL scene: selected stars/planets receive a soft pulsing halo and
  selected orbit traces brighten without changing simulation data.
- June 29, 2026 camera-control pass adds orbit/zoom/pan view controls with
  reset-view coverage to the Live System Preview so dense systems can be
  inspected from useful angles without changing the simulation scene contract.
- June 29, 2026 camera-affordance pass adds draggable/touch-safe canvas
  behavior and browser coverage for both wheel zoom and drag orbit.
- June 29, 2026 assumption-export pass adds `render_scene.assumptions`, a
  structured transient export of every rendered `ASSUMED` field using the
  planned `disc.simulation_assumptions` binding shape, plus API benchmark
  coverage for TRAPPIST-1 phase assumptions.
- June 29, 2026 visual-scale policy pass adds `visual_scale_beta_v1` to
  `render_scene`, makes the browser consume the policy for star radius, planet
  radius, planet orbit spacing, and group orbit motion scale, and labels the
  preview readout as clarity-scaled rather than physical scale.
- June 30, 2026 inspection diagnostics pass adds canvas-level counts for
  registered star, planet, subsystem, and orbit inspectable targets, with
  Playwright coverage for TRAPPIST-1 and Nu Sco.
- June 30, 2026 mobile inspection coverage pass verifies touch-pinned simulator
  readouts expose provenance pills and truncated copyable IDs within the compact
  preview layout.
- June 30, 2026 benchmark render-smoke pass enables drawing-buffer sampling for
  the system preview canvas and adds nonblank browser render checks for Alpha
  Centauri, Proxima Centauri, 55 Cnc, and Sol.
- June 30, 2026 fallback coverage pass verifies failed
  `/simulation-scene` loads render the deterministic snapshot fallback rather
  than leaving the system preview with a broken canvas.
- June 30, 2026 visible-clock pass surfaces the shared local beta simulation
  day in the system preview render-policy summary and verifies it advances,
  pauses, and resumes with the scene clock.
- June 30, 2026 scale-mode pass adds client-selectable Structure, True Orbits,
  True Bodies, and Log Scale simulator modes, advertises them in
  `render_scene.visual_scale`, and keeps the transforms presentation-only.
- June 30, 2026 collision-safe structure pass caps visible stellar radii in
  Structure mode against nearest rendered stellar separation while keeping
  glow and picking radii separate; Castor browser coverage verifies
  non-overlapping multi-star rendering diagnostics.
- June 30, 2026 planet-trail/eccentricity pass adds animated planet trails for
  strict body-scale views and caps displayed planet-orbit eccentricity when
  compressed presentation spacing would otherwise make neighboring Sol orbits
  visually cross. Source eccentricity remains unchanged in provenance readouts.
- June 30, 2026 habitable-zone overlay pass adds a toggleable, inspectable
  render-scene HZ band based on stellar luminosity and broad 0.35-1.70 Earth
  flux bounds; browser coverage verifies the Sol overlay registers in the
  rendered scene.
- July 5, 2026 formation-line overlay pass adds default-off simulator toggles
  for vaporization, soot, water snowline, carbon dioxide, methane/carbon
  monoxide, and nitrogen freeze boundaries with explanatory tooltips, while HZ
  remains default-on.
- June 30, 2026 simulator label pass makes HZ bands default-on, increases HZ
  contrast, and adds default-on billboarding labels below rendered stars,
  planets, and subsystem handles with a compact labels toggle.
- June 30, 2026 SDF label pass replaces simulator canvas-sprite labels with
  Drei/Troika SDF text billboards, camera-facing screen-size scaling, outlines,
  non-picking labels, and Playwright renderer diagnostics. Full dense-scene
  label priority/collision management remains future work.
- June 30, 2026 True Orbits correction removes the fixed inner orbit offset
  from that scale mode so rendered planet-orbit radii preserve linear
  semi-major-axis ratios; browser diagnostics now assert the AU-to-scene scale
  remains proportional.
- June 30, 2026 render-policy summary pass adds compact policy fields for
  local beta time, clarity scale, assumption persistence, and deterministic
  snapshot fallback mode.
- July 1, 2026 System Simulation naming pass promotes the former Live System
  Preview terminology to `System Simulation v1` in public/runtime docs while
  keeping `/simulation-scene` as the API contract.
- July 1, 2026 map drill-in pass adds a lazy-loaded System Simulation
  Peek/Explore layer to the 3D map. Peek inspects the selected system in a
  framed overlay without moving the map camera; Explore flies the map camera
  toward the selected system and expands the same simulation layer. Suggested
  nearby systems are ranked from the current 100 ly map payload by coolness,
  distance, planets, multiplicity, and readable names.
- Future Star Search-on-map milestone should rebuild search as a map-native
  exploration layer rather than a separate page: a tight sidebar for filters
  and recents, top search, dual-handle ranges for viewpoint-relative distance,
  star count, planet count, and coolness, a habitable-zone toggle, and a
  compact spectral/temperature selector bar. Active filters should override
  adaptive label selection so matching systems materialize labels directly in
  the 3D field while nonmatches fade or hide.
- Future System Simulation scale/control milestone should add a true physical
  scale mode with one shared linear scale for bodies and orbits, improve zoom
  range for compact inner-system inspection inside wide systems, and extend
  Star Map-style keyboard flight controls into Explorer/detail simulations.
- July 1, 2026 map minimization pass removes the redundant selected-system
  card entirely, moves selected vitals into transparent simulation overlay
  pills, places Pause/Start/Reset plus speed and Structured/Orbit/Body/Log
  selectors over the simulator canvas, shrinks and increases transparency on
  Peek, and combines selection history plus `Cool Stars Nearby` suggestions
  into collapsible tray sections capped at eight compact pills each.
- July 1, 2026 Star Map theme pass fixes embedded System Simulation speed/scale
  selects in Aurora and Enterprise/LCARS by moving the controls into a floating
  overlay layer above the WebGL canvas, brightens transparent LCARS borders, and
  makes Simple Light/Geocities map overlays more opaque for readability.
- July 1, 2026 Star Map LCARS polish pass gives Enterprise black
  nontransparent map cards with bright yellow borders, removes glow effects,
  and adds theme-aware dropdown option colors for simulator speed/scale menus.
- July 1, 2026 System Simulation readout simplification removes redundant
  local-days and missing-inputs pills from the visible readout, keeps the beta
  day in Render Policy, and moves map hover/pinned object cards away from the
  bottom evidence/policy strip.
- July 1, 2026 Star Map Explorer polish increases Explorer shell/canvas opacity
  and separates compact simulator readout pills from a collapsible Diagnostics
  disclosure containing Evidence and Render Policy, so diagnostics cannot
  stretch the pills. Browser Back now exits Explorer back to map flight.
- July 1, 2026 simulator orientation transparency pass surfaces source
  orientation, partial sky-plane orientation, assumed roll, or local-clarity
  labels in the simulator policy readout so map-to-system alignment remains
  auditable instead of implied.
- July 2, 2026 showcase polish densifies the Star Map header, removes inline
  long-ID copy/info buttons from compact history/nearby pills and the map
  drill title, and moves mobile Peek simulator speed/scale controls below the
  Explore/Detail/Back row so phone portrait layouts stay readable.
- July 2, 2026 map install-branding and control polish adds
  `GET /api/v1/public-config`, derives the `/map` title from
  `SPACEGATE_SITE_NAME` / `SPACEGATE_MAP_TITLE` with `Coolstars Map` as the
  default, adds desktop arrow-key flight aliases, and makes the System
  Simulation Peek panel resizable for the current browser session. A later
  installer/runtime configuration milestone should prompt for the public site
  name instead of relying on operators to edit env files manually.
- July 2, 2026 Star Map header menu pass moves the theme selector into a
  right-side burger menu and adds persistent `WASD`, `ESDF`, and `8456`
  keybind layouts while keeping arrow keys as always-available flight aliases.
- July 2, 2026 Star Map interaction/theme pass limits `8456` flight controls
  to physical numpad keys, adds route leg truncation from clicked route
  segments/recent leg rows, and gives the map/explorer Cyberpunk theme
  stronger neon magenta/cyan chrome, scanlines, and dark glass panels.
- July 2, 2026 Star Map control/theme polish adds mouse-wheel flight over the
  map canvas, renames Peek's `Back to Map` action to `Close`, tightens
  Cyberpunk/Geocities Peek header alignment, and shifts Cyberpunk map title and
  HUD text toward terminal green with a sharper futuristic font stack.
- July 2, 2026 Star Map mouse/LCARS polish adds horizontal wheel truck,
  right-button drag truck, middle-button drag pedestal controls, links the map
  header eyebrow to `spacegates.org` as `Spacegate Stellar Database`, and
  retunes Enterprise/LCARS map chrome with solid colored LCARS block controls
  while preserving black nontransparent panels.
- July 2, 2026 LCARS map drill fix restores absolute Peek/Explore header
  positioning after the Enterprise theme pass so the System Simulation canvas
  remains the dominant pane instead of being squeezed below the controls.
- July 2, 2026 Star Map route/LCARS usability pass keeps `Measure` from
  changing selected system or refocusing the camera, moves the Enterprise menu
  below the header bar, recolors the Peek/Explorer system-title chip, and
  groups selected-system vitals into a continuous LCARS control strip.
- July 2, 2026 Star Map/System Simulation polish extends the continuous LCARS
  strip treatment to header Coolstars stats plus Search/Detail/menu actions,
  and tightens Orbit scale body meshes toward marker scale so true-orbit
  inspection no longer swallows the inner system inside oversized bodies.
- July 2, 2026 Star Map control simplification makes the fullscreen action
  visible across themes and removes low-use Capture Mouse/Stabilize buttons
  while keeping stabilized vertical as the default navigation behavior.
- July 2, 2026 Star Map/System Simulation polish removes wheel-scroll Peek
  dismissal, adds compact color-coded spectral/visual-class badges above
  simulator stars, lowers the Geocities history/nearby tray below its taller
  header, and restores dark LCARS metadata text on light history pills.
- July 2, 2026 Star Map orientation pass adds a client-side Galactic frame
  toggle using an explicit ICRS-to-Galactic presentation transform, optional
  Coreward/Rimward/Spinward/Antispinward labels, burger-menu stacking above
  Peek/Explore, and right-drag truck suppression that no longer closes Explore.
- July 2, 2026 Star Map orientation/label pass adds visible direction arrows
  to Coreward/Rimward/Spinward/Antispinward labels, keeps those Galactic
  direction labels available in ICRS by projecting the true Galactic vectors
  into the active scene frame, adds simultaneous left+right mouse drag camera
  orbit around the selected system or Sol, removes low-value flight telemetry
  text from the bottom-right HUD, hides the snapshot chip from map drill-in, and
  replaces the fixed Sol-neighborhood label set with camera-distance plus
  coolness-priority fading labels.
- July 2, 2026 Star Map display-density pass makes the in-scene label budget
  respond to the current camera field: sparse views admit more labels, crowded
  views keep fewer strong labels and fade lower-coolness labels harder. The
  same pass narrows the Selection History tray, adds the Coolstars/Spacegate
  mark beside the configurable map title, and changes the selected-system
  marker from a bright circle to a tilted orbiting-planet accent.
- July 2, 2026 Mission Control theme pass retunes the Star Browser and Star
  Map theme toward Apollo-era MOCR references: olive/gray metal console
  surfaces, CRT-green readouts, amber pushbutton accents, station-label strips,
  and hard rectangular panel chrome. Focused Playwright coverage now checks
  the map-side Mission Control styling.
- June 30, 2026 class-provenance hardening pass makes stellar class readouts
  use field provenance and adds browser diagnostics for source-like classes
  without component-specific spectral evidence.
- June 30, 2026 orbit-inspection pass adds provenance-bearing guide/trace
  readouts for orbit paths plus wider shared raycaster line hit-testing.
- June 30, 2026 planet-inclination fallback pass keeps render-scene planet
  inclinations source-backed when present and uses deterministic
  `disc_assumption` fallbacks when absent; the fallback now prefers a
  same-host coplanar visual prior seeded from source inclinations before using
  the older centered low-tilt prior.
- June 30, 2026 local snapshot fallback restoration generated 100 deterministic
  `system_card` snapshots for served build
  `20260630T_sim_beta_sol_smallbody_v1`, restored `disc.snapshot_manifest`
  coverage for the first map page, and added
  `scripts/verify_snapshot_fallback.py` to prove served fallback assets resolve.
- June 29, 2026 simulator assumption materialization pass adds
  `scripts/materialize_simulation_assumptions.py`, creates
  `disc.simulation_assumptions` rows and Parquet for selected/benchmark
  systems, and annotates API assumption records as transient or persisted.
- full client epoch/time controls, uncertainty visualization, reviewed
  assumption curation/batch policy, science-grade ephemeris propagation, and
  physical-scale/precision display modes are not implemented yet. The browser
  preview now has a presentation-scale `simulation_tree_v1` for nested
  barycentric stellar motion.
- Sirius became a valid compact-companion benchmark on local build
  `20260630T_sim_beta_api_alias_v4` and remains valid on current served build
  `20260630T_sim_beta_sol_smallbody_v1`: Sirius A is a reviewed
  `athyg_accepted_supplement` core row, Sirius B remains the Gaia white-dwarf
  row, WDS components are linked, and no bright-primary aliases are attached
  only to the compact object. Public antiproton must receive a sliced/rebuilt
  deployment before this local fix is public.
- NASA `ps` alternate solution ingestion is implemented in the ARM builder;
  public datasets need a refreshed NASA `ps` download/cook/rebuild before
  those candidate rows appear in served production builds.
- Castor remains a hierarchy-quality watch item for browser rendering and
  source-scale orbital realism, but nested-subsystem animation now uses the
  same `simulation_tree_v1` contract as HD 213885/HD 79210; the general MSC
  materialization gap is no longer a known data-loss issue in the builder.
- Local served build has restored deterministic snapshot fallback coverage for
  the first map page; the public side-sliced deployment still needs equivalent
  `disc.snapshot_manifest` restoration/deployment before public System
  Simulation fallback checks can exercise real fallback assets again.
- June 30, 2026 simulator API patch attaches core planet render bodies to
  rendered host stars with `host_body_key` when `core.planets.star_id` resolves
  cleanly. Local Playwright now covers the 16 Cyg hosted-planet scene, and API
  checks confirm Proxima b/d use direct source host linkage and 16 Cyg B b
  resolves through catalog-equivalent host linkage.
- June 30, 2026 source-leaf reconciliation patch lets simple MSC A/B/C render
  leaves reuse matching core star vitals and bridges catalog-equivalent planet
  host IDs onto those source-native render components. This removes the
  duplicate Gaia-host plus MSC-leaf rendering failure for 16 Cyg while keeping
  the planet host auditable as a catalog-equivalent match.
- June 30, 2026 compact-companion preview patch adds a
  `visual_binary_fallback` render orbit for two-star scenes with no source
  orbit edge. Sirius now has local Playwright and known-system verifier
  coverage for A + white-dwarf B rendering with ASSUMED `disc_assumption`
  visual orbit fields; this remains a presentation fallback, not ARM science.
- June 30, 2026 Sol authority source-refresh patch disambiguates Horizons
  asteroid/TNO/dwarf-small-body commands with the small-body selector and adds
  sentinel source/build/API checks for Ceres, Vesta, Pallas, Juno, Hebe, Iris,
  Interamnia, and Hector. This fixes a simulator-visible failure where bare
  numeric commands made Ceres/Vesta/Pallas/Juno/Hebe/Iris/Interamnia/Hector
  inherit major-planet or satellite-like orbital solutions.
- June 30, 2026 simulator ordering/inspection patch emits rendered planets in
  orbital order by semi-major axis/period and passes provenance field objects
  through planet-orbit readouts, so orbit paths have the same inspectable
  SOURCE/DERIVED/ASSUMED/MISSING pills as rendered bodies.
- June 30, 2026 nested-motion patch makes the R3F renderer consume
  `render_scene.orbits` independently of planet bodies and propagates
  group-pair offsets through containing hierarchy groups. Castor browser
  coverage now checks active nested group motion instead of only API orbit
  presence.
- June 30, 2026 hosted-planet motion patch carries planet orbit guides and
  bodies on the full host hierarchy-group offset, with 16 Cyg browser coverage
  for hosted planets in multi-star simulator scenes.
- June 30, 2026 simulator inspection patch truncates long render/source IDs
  in pinned readouts while preserving full copy/tooltip values and coverage for
  Gaia-backed object IDs.
- June 30, 2026 mobile inspection patch constrains pinned simulator readouts
  into a compact mobile bottom sheet with reachable copy/close controls and
  browser geometry coverage.
- June 30, 2026 hierarchy-visual patch differentiates direct binary orbit
  guides, group-pair hierarchy guides, and subsystem handles in the live
  simulator, with Castor diagnostics covering all three visual classes.
- June 30, 2026 provenance popover patch surfaces confidence, source
  references, notes, and procedural assumption metadata from existing
  simulation field provenance objects.
- June 30, 2026 shared-clock patch refactors live preview stars, planets,
  orbit guides, and subsystem handles onto one single-writer local beta
  animation clock with browser diagnostics, while leaving science-grade epoch
  controls pending. Browser coverage now verifies that Pause freezes this
  shared clock and Start resumes it.
- June 30, 2026 direct-binary trace patch changes binary orbit traces from a
  full relative-separation guide to rendered barycentric body paths, using
  source mass ratios when available and explicit equal-mass visual fallback
  otherwise.
- June 30, 2026 hierarchical motion patch changes group-pair subsystem motion
  from equal-offset animation to mass-weighted barycentric motion using summed
  positive side masses where available, treats non-positive MSC endpoint masses
  as missing in the render contract, and advances animated bodies via
  mean-anomaly Kepler solves. HD 213885 and HD 79210 are browser regression
  benchmarks.
- June 30, 2026 simulation-tree patch adds `render_scene.simulation_tree`
  (`simulation_tree_v1`) with root, barycenter, and body nodes derived from the
  emitted orbit rows. The R3F renderer now uses this recursive tree for stellar
  bodies, orbit traces, and subsystem handles, so compact triples such as
  HD 213885 render as `(AA+AB)+B` rather than a fixed star plus crossing
  sibling offsets. Focused Playwright checks require tree activation and
  nested-orbit diagnostics for HD 213885/HD 79210.
- June 30, 2026 eps Ind A simulator patch makes hierarchy-pair period fallback
  prefer MSC system-row periods and projected-separation Kepler estimates
  before generic visual assumptions. The renderer also attaches planet orbit
  guides, trails, bodies, and host-star HZ bands to active simulation-tree body
  positions, fixing planet/HZ overlays drifting away from tree-rendered hosts.
  Focused browser coverage now includes eps Ind A's long MSC A-B period and
  tree-hosted planet diagnostic.
- June 30, 2026 Alpha Centauri scale patch makes stellar orbit display radii
  separation-aware, so Proxima/AB-C renders outside the compact A-B pair
  instead of near it despite the very long source period. HZ display scaling now
  includes rendered HZ bounds, and True Bodies planet radii use Earth-to-Sun
  scale relative to star meshes.
- June 30, 2026 compact-body render patch preserves source-backed compact
  object classification in stellar render bodies through `body_class`,
  `compact_type`, and object-type provenance fields, with Sirius B as the
  white-dwarf benchmark.
- June 30, 2026 Nu Sco browser benchmark patch adds Playwright coverage for
  seven source-native rendered leaves, five subsystem handles, direct/group
  orbit-guide counts, and missing spectral facts on unresolved children.
- June 30, 2026 planet-class inspection patch exposes renderer-only planet
  visual class as an API-backed `render_scene` provenance field in simulator
  evidence and planet readouts, with generator/basis coverage.
- June 30, 2026 render diagnostics patch adds API-emitted counts for final
  renderer bodies, orbit endpoint/relation kinds, field statuses, and
  assumption persistence, with strict benchmark verifier coverage.
- July 1, 2026 simulator robustness patch adds `simulation_tree_v1` fallback
  subsystem handles for stale/public slices where explicit subsystem bodies are
  missing, while preserving source-native ARM hierarchy handles when present.
- July 1, 2026 visual stellar class policy patch adds renderer-only
  `fields.visual_stellar_class`, with source spectral/temperature evidence
  preferred, compact-object evidence overriding main-sequence priors, and
  mass-only `mass_main_sequence_prior_v1` values labeled as ASSUMED
  `render_scene` presentation priors rather than catalog spectral facts.
- Agency-suggested orbital/physical parameters must remain proposals until
  reviewed and materialized through `arm`/`disc` gates

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

### Coolness Ranking Retool

Coolness ranking is central to CoolStars discovery and should be revisited as a
dedicated milestone. The first ranking model was useful for the prototype, but
the current database is much larger and more comprehensive. The next scoring
system should support reviewed profile presets, object-scoped scoring for
systems/stars/planets, explainable weighted feature families, operator-visible
result breakdowns after weight changes, and clear separation from canonical
science. Candidate signal families include proximity, luminosity, proper
motion, multiplicity, stellar rarity, giant/supergiant status, planet interest,
system complexity, source quality, public recognizability, and narrative or
Agency prioritization value.

### System Scene Runtime Cache

July 6, 2026 Star Search performance pass adds prebuilt compressed
`disc/simulation_scenes/system_<system_id>.json.gz` artifacts plus API serving
ahead of runtime assembly. This keeps `/simulation-scene` as the public
contract, avoids repeating expensive ARM/readiness/render-scene assembly for
materialized systems, and preserves runtime assembly as the fallback for cold
or unmaterialized systems.

July 8, 2026 preview-tier pass adds a presentation/runtime policy for Star
Search and map search cards: ordinary singleton no-planet/no-exotic systems use
a lightweight client-rendered preview from result fields, while planet hosts,
multistars, compact/exotic systems, high-coolness systems, public UX goldens,
and prebuilt scene artifacts remain on the full System Simulation path. The
simulation-scene materializer now has a `--priority-profile search-preview`
selector for these high-value full-preview targets. This is a prerequisite for
larger streamed 250/500/1000 ly map radii, where most displayed systems should
not trigger dynamic scene assembly.

### Stellar Physical Classification v1

The July 2026 V1054 Oph review exposed two related needs. First, complex MSC
systems can disagree between source hierarchy leaves and renderer endpoint
materialization if MSC detail rows, WDS component labels, and ORB6 orbit pairs
are not reconciled through the same source-native membership graph. V1054 Oph
should join the complex-nearby-multiple golden set with checks for five source
stellar leaves, Gliese 643/VB 8 alias coverage, no unmatched rendered
endpoints, and explicit incomplete-orbit layout diagnostics.

Second, Spacegate needs a reusable physical-class derivation policy that is
stronger than renderer-only color priors but still does not contaminate
canonical source facts. The policy should preserve source spectral type/class
first, derive display/physical class from source or derived effective
temperature and color evidence when safe, use radius/luminosity/log-g and
compact-object catalog evidence as remnant/evolved-star guards, and fall back
to mass-based main-sequence priors only as clearly labeled assumptions. Derived
or assumed classes may support filters, labels, and render materials, but must
remain separate from core spectral fields and carry provenance such as
`teff_visual_class_prior_v1`, `mass_radius_physical_class_prior_v1`, or
`mass_main_sequence_prior_v1`.

Implementation note, July 9, 2026: `render_scene_v0.2` now gates rendered
stellar bodies to source hierarchy leaves when those leaves exist, and reports
unmatched MSC detail/orbit endpoints in
`render_scene.diagnostics.membership_reconciliation`. The API bypasses stale
prebuilt scene artifacts that lack this diagnostic. ARM now emits
`derived_stellar_classifications` for core stars and reachable MSC component
endpoints; source spectral facts remain in core/source fields, while derived
display classes stay in ARM and render-scene visual fields remain
presentation-facing. Member-name alias/search enrichment for Gliese 643/VB 8
remains a follow-up.

Local artifact note, July 9, 2026: `scripts/rebuild_side_artifacts.py` was
added for schema/ARM-side rebuilds that should not trigger a full Gaia
download/cook run. It clones the served core/parquet/disc/snapshot surface,
updates cloned build metadata, regenerates `arm.duckdb` from cooked catalogs
with the current builder, writes an ARM report and side-rebuild report, and
leaves promotion explicit. Full local side build
`20260709T_v1054_classification_v1_side` materializes the new ARM table; public
slice `20260709T_v1054_classification_v1_public_side` carries it into the
antiproton-sized `core.public@v3` profile and passes relaxed build
verification, multiplicity goldens, known-system API benchmarks, and the V1054
System Simulation OBJECTS browser regression.

## Governance Rule

No milestone in M6+ should compromise M1-M5 scientific integrity gates.

If there is conflict:

1. protect core correctness
2. keep derived content explicitly labeled
3. delay feature launch rather than blur canonical truth boundaries
