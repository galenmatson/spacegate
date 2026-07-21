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

1. Release-scoped source lake, identity graph, and typed evidence compiler
2. Gaia-first canonical `core`
3. Deterministic served slices and API performance
4. Multiplicity hierarchy reliability
5. Disc factual layer (scores/facts/links)
6. Visual storytelling (snapshots + generated imagery)
7. 3D runtime and deep navigation
8. Rim/worldbuilding tooling
9. Procedural system generation tools
10. Community/engagement overlays

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

### M1. Gaia Core Backbone Pilot (Completed)

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

- NSS/MSC/WDS/ORB6/SBX reintegration on Gaia IDs, with SB9 and DEBCat
  component evidence attached in ARM only through deterministic endpoint rules
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
  - federated authority sources with explicit provenance: SIMBAD for the broad
    identifier graph, GCVS/VSX for variable-star designations, and IAU WGSN for
    approved proper names; no source is treated as a universal public-display
    authority
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
- Current regression inbox includes AR Cassiopeiae / AR Cas / HD 221253
  (reported as ten members instead of the canonical seven, plus failed name
  resolution) and W Ursae Majoris / W UMa / HD 83950 (HD lookup succeeds while
  the public variable-star name does not). These are acceptance cases for the
  general multiplicity and alias-authority fixes, not one-off data patches.

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

### M5.6 TESS Identity, Candidate, and Observation Evidence v1

Goal:

- make TIC/TOI identity first-class and searchable, recover real in-scope TESS
  targets missing from canonical inventory, and preserve TOI/transit evidence
  without bulk-ingesting TIC or promoting unreviewed candidates.

Plan:

- `docs/TESS_INTEGRATION.md`

Dependencies:

- M4 identity and naming authority
- M5.3 planet lifecycle contract
- canonical quarantine and inspectable adjudication paths

Deliverables:

- reproducible NASA TOI snapshot and targeted TIC retrieval
- TIC -> Gaia DR2 -> Gaia DR3 and alternate-ID reconciliation with quarantine
- exact `TIC`/`TOI` aliases and focus-aware search terms
- missing-real-object audit and narrow reviewed inventory recovery
- ARM TOI evidence/disposition history and candidate/negative-evidence policy
- targeted MAST observation-product index and external links

Success criteria:

- every targeted TIC ID is accepted, missing, excluded, or quarantined with an
  explicit reason
- paper lookups containing only TIC/TOI identifiers reach accepted objects
- confirmed TOIs do not duplicate planets; candidates and false positives do
  not contaminate canonical/default planet counts
- full TIC, CTL, TCE, and light-curve corpora remain outside default ingest
- unresolved tails do not block the subsequent Tiled Deep Map milestone

Current status (July 13, 2026):

- T0-T3 is merged to `master` at checkpoint `ac3511d`; canonical build
  `20260712T_tess_evidence_v3` passes full build and live API verification
- public `core.public@v3` build
  `20260713T_tess_evidence_v1_public_side` is deployed to antiproton and
  verified through public API plus desktop/mobile Playwright TIC searches
- pinned acquisition currently targets 27,930 TIC IDs and 8,064 TOIs without
  bulk TIC ingestion; an identical rerun produced zero TOI/TIC row deltas
- deterministic TIC identity, quarantine, aliases/search terms, missing-object
  audit, TOI evidence/history, and leakage gates are implemented
- final coverage is 10,418 accepted, 242 ambiguous/quarantined, 531 excluded,
  16,739 missing, and zero source-missing TICs; 836 confirmed/known TOIs link
  to canonical planets and the canonical planet count remains 6,311
- the missing-real-object tail approved L 134-80 / TIC 150320610 / TOI-6725
  through the reviewed AT-HYG supplement path using published parallax support;
  TOI-798 remains unresolved because its TIC/Gaia rows lack sufficient
  parallax or distance provenance
- canonical emission now remaps object identifiers, enforces unique entity IDs,
  and retains valid IDs for unhosted planets; no targeted identifier points to
  a missing canonical entity
- T4 presentation and T5 observation-product indexing remain subsequent work
  and do not block the T0-T3 checkpoint

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
- WISE/CatWISE/AllWISE v1 targeted evidence integration is implemented:
  `scripts/collect_wise_evidence.py` performs priority known-object
  cross-reference queries with epoch/proper-motion handling,
  `scripts/build_arm.py` materializes infrared evidence tables in ARM,
  `scripts/verify_wise_evidence.py` enforces the no-WISE-core-promotion policy,
  and system pages can lazy-load IRSA-backed W1/W2/W3 image previews through a
  bounded cache. It also includes a narrow red/high-motion targeted-query
  candidate queue with accepted/rejected/quarantined/needs_review status
  vocabulary. Plan and follow-ups: `docs/CATWISE_ALLWISE_PLAN.md`.
- Next WISE milestone: expand candidate discovery beyond targeted known-object
  cones into a reviewed nearby ultracool/brown-dwarf search workflow with AAA
  evidence packet hooks.
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

- replace monolithic map transport with a deterministic, immutable octree
  architecture and deliver a measured selectable 100/250-ly pilot. Generate
  500/1,000-ly verification manifests without exposing those radii publicly.

Deliverables:

- [x] versioned ICRS J2016 octree, Morton tile IDs, binary tile encoding, and
  content-addressed manifest contract (`docs/TILED_MAP.md`)
- [x] exact leaves plus clearly marked deterministic spatial/interest samples
- [x] independently regenerable, profile-hashed DISC coolness interest summary
- [x] renderer-independent browser scheduler with bounded coolness bonus,
  flight/search priority, starvation aging, cancellation, cache, and telemetry
- [x] measured tiled 100-ly parity and 250-ly desktop/mobile pilot acceptance;
  186 machine performance checks plus desktop/mobile pixel and interaction
  goldens pass on Photon
- [x] M8.1.1 removes the fixed 110-ly density shell with deterministic
  camera-blended Balanced/Performance LOD, opt-in Exact density, recenter
  hysteresis, and duplicate-request/seam-ratio acceptance gates
- [x] M8.1.2 adds a high-visibility Bright star style and deterministic
  toggleable representative stellar-class badges to bounded 3D map labels;
  badge dominance uses a versioned mass-proxy then intrinsic-brightness policy
- [x] M8.1.3 stabilizes 4K/250-ly idle operation after real-device Brave traces
  exposed viewport-triggered transport reinitialization, redundant stationary
  React/label work, and context-loss recovery storms; same-class resizes now
  preserve the active tile manager, idle work is movement-gated, recoveries are
  serialized, and WebGL resource counters are inspectable on the map canvas
- [x] M8.1.4 exposes measured 500/1,000-ly deep-map modes through progressive
  global sample frontiers and camera-local exact refinement. Depth-2 context
  must paint first, a complete depth-3 frontier must atomically replace it,
  dense depth-4 samples may refine additively, and no deep mode may eagerly
  request all exact leaves. Photon acceptance passes 312 cold/warm/rapid
  performance checks plus a 60-second 4K parked soak with zero retained heap,
  request, label, telemetry, or WebGL-resource growth
- [x] deploy M8.1.4 to antiproton through two observable checkpoints: verify
  public 100/250-ly behavior on `20260714T191900Z_d873067_side_rebuild`, then
  promote `20260715T015659Z_e392a11_side_rebuild`; the canonical database
  stability follow-up `20260716T0103Z_94bdab7_side` is now the public build.
  Public desktop/mobile Playwright checks pass for exact 100/250-ly and
  progressive 500/1,000-ly completion with zero tile failures, including a 4K
  Bright render and canvas-pixel probes
- [x] formally deprecate the monolithic public transport; retain
  `?map_transport=monolithic` only as a bounded 100-ly diagnostic during the
  observation window
- [x] M8.1.5 replaces the map's single broad habitable-zone control with six
  independently toggleable hot/temperate/cold Jupiter/terrestrial navigation
  bins. Tile labels and API search share one conservative confirmed-planet
  policy; ambiguous planets and TESS candidate/negative evidence remain
  explicit and do not contaminate canonical counts

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

#### M8.2a. Extended-Object Science Foundation v1

Status: complete July 13, 2026. Local immutable-artifact verification, full ARM
integration, public-slice propagation, and deterministic rerun checks pass.

Promoted local checkpoint:

- canonical: `20260713T1627Z_dd7446e`
- public `core.public` v3 slice: `20260713T1627Z_dd7446e_public`
- verified and locally served on Photon July 13, 2026; not deployed to antiproton

- pinned OpenNGC and CDS/VizieR acquisition with checksums and manifests
- deterministic ICRS normalization and explicit identifier reconciliation
- separate CORE identity/search tables and ARM geometry/distance/relation evidence
- conservative associated-star distance policy and explicit placement domains
- typed extended-object and unified object-search API contracts
- Star Search typed catalog-object results and evidence detail pages make
  names and aliases such as M45, IC 4592, and LBN 1113 directly inspectable;
  64-bit object IDs remain exact across browser JSON consumers
- deterministic rerun and IC 4592/M45/Barnard 33/M31 goldens
- batched associated-star resolution performs one typed HD lookup scan rather
  than repeated full-table probes; identical canonical table hashes verified

#### M8.2b. Extended-Object Presentation

Deferred until M8.1 establishes the tile/LOD contract. This phase owns map
rendering, object extent/selection behavior, imagery, and attributed background
survey layers; it must consume M8.2a without redefining canonical science.

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
- Castor's literature-weak MSC `CC` endpoint remains visible in membership
  diagnostics but is an allowlisted non-blocking warning; unexpected unmatched
  endpoints still fail the wide-orbit verifier.
- Renderer member-name precedence preserves the core `Proxima Centauri` name
  over source component shorthand such as `alp1 Cen C`.
- July 16, 2026 stellar-display consistency fixes a general spectral parser
  defect that interpreted lowercase luminosity notation such as `dM1e` as
  white-dwarf class `D`. Source spectral and compact evidence now outrank
  visual fallbacks in badges, inferred members inherit preferred public names
  with explicit suffixes, and Explorer no longer exposes internal stable keys.
- Exact member-star runtime fallback preserves targets such as `VB 8` on older
  side builds whose materialized `system_search_terms` omitted member rows;
  canonical emitters continue to materialize those rows for new builds.
- July 15, 2026 multiplicity-evidence integrity pass adds pinned CDS SB9
  acquisition and source-native system/alias/orbit cooking. Exact unique MSC
  `SB9_<sequence>` references attach primary/secondary spectral evidence to
  existing ARM endpoints; ambiguous and unresolved matches are quarantined.
- DEBCat component spectral types attach through a separate unique canonical
  system + period + endpoint rule. The initial full ARM verification accepted
  855 SB9 binary matches and 14 DEBCat binary matches, producing 1,104
  component spectral observations. Castor resolves as A/M, A/M, M/M from SB9,
  with YY Gem independently corroborated by DEBCat.
- The default executable accepted-supplement list was retired. Former Sirius
  A/B and L 134-80 cases are preserved in a non-executable adjudication inbox,
  and a static production-transform audit rejects new object literals unless
  they are explicitly classified as report-only diagnostics.
- The full canonical stability rebuild replaced the former Sirius supplement
  with reusable source rules: exact unique HIP+HD SBX/AT-HYG recovery restores
  the Gaia-missing primary, and projected-J2016 WDS/Gaia reconciliation retains
  the compact companion. Generic exact WDS/component reconciliation also
  removes the duplicate 70 Ophiuchi B MSC surrogate. L 134-80 remains deferred.
- Canonical emission now preserves source-native bootstrap stable keys rather
  than generating sequential legacy-row keys. Paired builds
  `20260715T2343Z_06ac777_a` and `20260715T2349Z_06ac777_b` reproduce exact
  CORE and ARM science payloads.
- TESS identities are now adjudicated in the full canonical universe and
  projected into public slices. The winning full candidate preserves the
  10,418 accepted, 242 ambiguous, 531 excluded, and 16,739 missing partition
  and leaves the 6,311 canonical planet count unchanged.
- ARM WISE bindings are now re-keyed from collection-time IDs onto current CORE
  target, system, and stable keys. The public slicer preserves WDS observation
  rows by retained WDS scope and WISE matches by canonical target scope instead
  of applying an invalid source-component-key conjunction.
- Verified checkpoint: full ARM refresh
  `20260716T0055Z_94bdab7_canonical_arm` is CORE-identical to the deterministic
  canonical candidate; public slice `20260716T0100Z_94bdab7_public` retains
  8,147 WDS observations, 2,105 exact canonical WISE matches, and a zero-delta
  canonical TESS projection.
- Final local promotion candidate `20260716T0103Z_94bdab7_side` passes the full
  database gate with exact four-radius map membership and 1,000/1,000 cached
  priority simulation scenes.
- The same immutable candidate was promoted to antiproton on July 16, 2026.
  Archive checksum, public health/auth, API integration, known-system goldens,
  exact/progressive desktop/mobile map checks, 4K Bright rendering, screenshots,
  and canvas-pixel probes pass against the public service.
- Source-evidence closeout accounts for all 4,633 current MSC orbit rows:
  4,627 normalize to ARM edges and six rows across five WDS scopes are excluded
  because neither canonical inventory nor MSC `sys.tsv` contains the system;
  zero rows remain quarantined or unaccounted.
- Canonical hierarchy v2 typing separates structural `node_kind` from
  `component_family`/`component_type`. Across the full bootstrap hierarchy this
  preserves 2,748,018 brown dwarfs, 176,698 white dwarfs, three pulsars, and
  171 inferred brown-dwarf leaves without changing stellar storage family or
  inferred status.
- WDS pair-evidence v1 preserves source-native endpoint scope and partitions
  157,299 rows into deterministic binding outcomes. The validation build binds
  2,077 rows to unique endpoint pairs in the full canonical ARM while asserting
  zero bound relationships and creating zero simulation-ready orbits. The
  1,000-ly public slice retains 1,797 accepted pairs.
- Sirius A/B adjudication inbox entries are marked resolved by their reusable
  catalog rules. L 134-80 remains deferred; Castor CC is now an explicit
  non-executable classification/physical-status deferral.
- Edge bootstrap now validates and installs every report advertised by
  `current.json`; report paths are bounded relative JSON paths and are staged
  atomically under the published build ID.
- Public-slice and side-artifact builders emit a slice-native
  `derived_build_verification_report.json`. It records live slice counts and
  integrity checks plus hashed upstream report lineage, and strict verification
  recomputes those checks against the candidate database. Presentation builds
  no longer require relaxed verification or mislabeled full-build reports.

Remaining:

- preserve the known Tegmine, Xi Scorpii, and Nu Sco alternative/overlapping
  source orbits as explicit simulation-tree diagnostics until an evidence-based
  topology can select among incompatible groupings; do not force every source
  solution into one non-overlapping Kepler tree.
- run the remaining L 134-80 deferred case through the inspectable AAA/human
  adjudication contract; Sirius has been resolved by general catalog rules.
- evaluate Skiff spectral classifications and VSX composite spectral strings
  as corroborating evidence only after a source-native component-scope rule is
  defined. Do not split a composite type across graph endpoints by name alone.
- decide whether a capped/materialized ARM policy for stellar subclass priors
  is worth the storage cost, or whether runtime provenance remains sufficient.

July 17, 2026 catalog-wide follow-up:

- `docs/SOURCE_CATALOG_UTILIZATION_AUDIT_2026-07-17.md` expands the earlier
  orbit-focused audit across every active source family and the current
  simulation, planet, compact-object, cluster, variability, and AAA goals.
- The reproducible measurement script is
  `scripts/audit_catalog_feature_utilization.py`; the initial machine report is
  stored outside the repo under
  `/data/spacegate/state/reports/source_catalog_utilization_report_20260717.json`.
- The audit found one identity defect (Gaia DR2 open-cluster membership joined
  directly to DR3 IDs), one major upstream acquisition gap (Gaia
  AP/FLAME/evolution/extinction/activity), and several cooked-but-stranded
  evidence families. These should land as one evidence bundle before another
  full canonical rebuild, not as one-field rebuilds.

### M8.3c. Evidence Lake v2 (Current Main Quest)

Status: active on `feature/evidence-lake-v2`. E0-E2 completed July 18, 2026;
registered E3 acquisition completed July 20, 2026; E4 is in progress.

Goal:

- replace the conservative, field-losing catalog collector/cooker path with a
  release-scoped scientific evidence compiler that preserves the richest useful
  source data, selects public facts reproducibly, and uses derivations or
  assumptions only when acceptable evidence is absent.

Plans:

- `docs/EVIDENCE_LAKE_V2.md`
- `docs/SOURCE_CATALOG_UTILIZATION_AUDIT_2026-07-17.md`

Preserved foundations:

- permanent Spacegate identity and hierarchy contracts
- CORE/ARM/DISC/RIM ownership boundaries
- immutable snapshots/builds, provenance, quarantine, and deterministic gates
- no named-object production transforms
- current public build as the A/B stability reference

#### M8.3c-E0. Registry, Retention, and Storage Foundation

Deliverables:

- source-release registry with authority roles, licenses, retrieval contracts,
  schema snapshots, cadence, completeness gates, and storage classes
- field-disposition registry accounting for every upstream field as preserved,
  normalized, index-only, or deliberately omitted with a reason
- retention audit and explicit raw/typed/build/report/product-cache budgets
- fail-closed retention for explicit interrupted scientific-evidence compiler
  temporaries, with immutable tree identity, live-process and manifest refusal,
  reviewed candidate-set hashes, and durable whole-artifact apply reports
- uncertainty-aware ingestion-envelope policy around the public 1,000-ly sphere
- schema-drift and source-release delta reports

Exit criteria:

- the next acquisition cannot silently omit an upstream field or change schema
- served, rollback, published, referenced, and unique-source artifacts remain
  protected before storage is reclaimed

Current status (July 18, 2026):

- machine-readable registry covers 44 source releases and the pinned baseline
  covers all 67 currently materialized manifest entries, including the disabled
  legacy SBX, Gaia, and NASA projections retained only for E6 A/B comparison
  manifest entries after the E2 Gaia forward/reverse additions; the pinned
  baseline accounts for 1,824 machine-enumerated
  fields and exact source-format contracts for E1 parser work
- full-refresh preflight now gates registry validity, manifest completeness,
  schema drift, field accounting, and the 300 GiB acquisition floor
- July 20 reviewed the additive E3 schema delta and repinned the registry at 139
  active entries and 5,962 fields under fingerprint
  `7e862068e7e4a18516e84bfc3a7d2e2dbb7dd0e4d780d1b62708869a33aa9752`;
  three checksum-declared superseded SIMBAD pilot entries remain retained
  lineage and no longer masquerade as active unregistered sources
- retention added explicit protected-build inputs, preserved 11 referenced
  lineage builds, reclaimed 196.21 GiB, and left raw/cooked/catalog/scratch
  science state untouched; `/data` now has about 385 GiB free
- a separate compiler-temporary retention gate now handles only explicit hidden,
  manifest-less artifacts; its first reviewed apply removed two reproducible
  SIMBAD citation-join diagnostics and reclaimed 73,183,408,128 allocated bytes
  without touching immutable raw or typed evidence
- current source-delta baseline accounts for 63 manifest entries with zero
  unexplained changes after baseline update

#### M8.3c-E1. Immutable Raw and Source-Native Typed Lake

Deliverables:

- byte-preserving raw snapshots with exact queries/URLs, timestamps, checksums,
  schemas, counts, manifests, citations, and licenses
- independently cooked typed Parquet/Arrow for each source release, preserving
  record identity, parameter-set/reference grouping, component scope, epochs,
  frames, units, flags, uncertainty, and limit semantics
- clean-state raw-to-typed reproduction, row accounting, and determinism gates
- metadata-first/on-demand policy for bulk spectra, light curves, and imagery

Exit criteria:

- source-native data can be reproduced and inspected without CORE, ARM, or a
  source-selection policy
- DuckDB remains a replaceable compiler/query engine rather than the sole
  durable scientific representation

Current status (July 18, 2026):

- all 25 available non-planned releases have immutable raw snapshots with 59
  artifacts, 403 files, and 10.41 GiB of active content; mutable legacy aliases
  were copied rather than hard-linked into the immutable store
- all fixed-width, archive, FITS, SQL-row, and documented-text gaps are closed;
  68 typed tables account for 48,936,930 rows and all 59 raw artifacts with zero
  pending tables
- NASA 683-column CSV shape, TESS MAST schema drift across null-only batches,
  and AT-HYG two-part continuation behavior now have regression tests and
  fail-closed accounting
- official WDS/CDS schemas drive lexical parsing; MSC notes, all 35 ORB6
  fields, ATNF repeats/comments/glitches/references, Green SNR limits, and all
  161 white-dwarf FITS columns and alternative atmosphere fits are retained
- the clean-state reproduction gate matches content, schema, row-count, and
  Parquet hashes for all 25 releases; deterministic single-thread ordered
  serialization fixed the nondeterministic large Gaia/TESS output it exposed
- metadata-first observation-product indexing and bounded on-demand cache
  behavior are pinned by
  `config/evidence_lake/observation_product_policy.json`; source-specific
  product indexes are acquired in E3 and normalized in E4

#### M8.3c-E2. Release-Scoped Identity and Scope Graph

Deliverables:

- source/release-scoped identifier nodes and provenance-bearing crossmatch edges
- permanent Spacegate object IDs independent of any Gaia release identifier
- separate outcomes for physical identity, system containment, component scope,
  observation target scope, and alias/public-name scope
- full Gaia DR2-to-DR3 neighborhood/crossmatch reconciliation for clusters and
  every remaining DR2 fallback path
- exhaustive accepted/missing/excluded/ambiguous/quarantined reports

Exit criteria:

- no DR2 identifier is treated as a DR3 identifier by direct equality
- every attempted binding has an explicit outcome and no source relation claim
  silently becomes canonical containment

Current status (July 18, 2026):

- official forward acquisition accounts for 1,542,049 targeted DR2 IDs in 155
  exact TAP queries and 1,626,847 neighborhood rows; the independent reverse
  acquisition accounts for all 1,625,665 DR3 candidates in 163 exact queries
  and 1,776,331 rows
- the active typed estate expanded to 27 sources, 63 raw artifacts, and 72
  tables containing 55,507,822 rows; registry/schema accounting passes for 34
  registered sources, 63 manifest entries, and 1,824 fields
- graph `c84389ad55f17081fff008b4` has one explicit outcome for every target:
  226,392 accepted, 1,234,609 excluded from the current canonical backbone,
  81,043 split/merge ambiguities, and five missing; no target is unaccounted
- the graph contains permanent object nodes, release-scoped identifiers,
  official crossmatch candidates, source-record bindings, collision
  diagnostics, quarantine, and separate physical identity, containment,
  component/subsystem, observation-target, and alias/name scope claims
- 5,877,462 existing containment links are labeled as stability references;
  186,198 MSC/WDS relations remain candidates and zero were promoted into
  canonical containment
- edge, outcome, and source-family bindings retain exact source/release/table
  lineage; all family-level and duplicate-system guards pass, including 18
  accepted component bindings that share systems but remain distinct stars
- ordered Parquet artifacts, full hashes, reverse-universe accounting, and a
  clean independent compile comparison pass; current served CORE remains
  unchanged

#### M8.3c-E3. Foundational Source Acquisition

Deliverables:

- bounded Gaia DR3 physical-parameter, NSS, variability/activity/rotation, and
  official external-crossmatch source releases
- documented distance-estimate evidence for boundary and uncertain-astrometry
  use without replacing source astrometry or identity
- SIMBAD/GCVS/VSX/IAU source roles for aliases, bibliography, and name scope
- current Gaia cluster membership/physical evidence and probability-bearing
  Gaia wide-binary evidence
- complete relevant NASA planet, host, candidate/status, TOI, transit, and RV
  metadata
- matched Gaia/APOGEE/GALAH/LAMOST stellar-physics evidence with source-native
  flags and parameter sets
- existing multiplicity, compact-object, ultracool, variability, and extended-
  object sources migrated into the same source contract

Exit criteria:

- acquisition coverage and deliberate omissions are machine-accounted by
  source, field, object scope, and scientific objective
- large observation products are discoverable and retrievable without requiring
  an unbounded local mirror

Current status (July 20, 2026, acquisition complete):

- deterministic TAP and HTTP acquisition programs now preserve exact queries or
  release bytes, upstream schemas, source field dispositions, timestamps,
  checksums, row counts, UWS jobs, resumable partial transfers, and atomically
  locked shared manifests
- asynchronous TAP retry is cleanup-confirmation gated: if a nonterminal UWS
  abort cannot be confirmed during a route outage, the bucket fails closed and
  no replacement job is submitted
- the hard-parallax branch of the 1,250-ly Gaia envelope contains 31,987,126
  source rows and is atomically published with all 152 `gaia_source` fields;
  its disjoint uncertainty supplement now accounts all 189,145 posterior-
  overlap rows across 127 uncapped partitions, giving a 32,176,271-row union;
  combined raw snapshot `fcd1f77edf401a7e19c72197` and typed snapshot
  `35a41010cf74f950e61b5412` are immutable, verified, and clean-reproduced from
  external scratch with exact table, snapshot-ID, and content-hash agreement
- all 764 fields across the six foundational Gaia table families are assigned
  to source-native products. Corrected expanded NSS contains 85,724 hard-
  envelope and 1,351 disjoint uncertainty rows, all with 77 source fields
- the separately acquired Bailer-Jones EDR3 distance envelope now preserves
  17,310,560 rows and all 10 published fields; Hunt-Reffert preserves 7,167
  cluster rows, 1,291,929 membership rows, and 29,956 literature-crossmatch
  rows across all 161 table-column occurrences. Both releases pass immutable
  raw/typed verification and clean reproduction
- official WGSN and GCVS release artifacts are pinned and pass raw/typed
  verification; WGSN preserves 597 distinct named rows, all 16 declared
  columns, source row identity, and linked resources under a schema-drift gate;
  GCVS preserves 60,894 catalog rows, 226,060 cross-identifiers, 26,018
  suspected variables, 25,696 bibliography rows, and its source
  documents/dictionaries. GCVS parser v2 accounts exactly 203,740 structural
  trailing-delimiter normalizations while retaining internal delimiters and
  exact raw rows; typed A/B and clean-reproduction gates pass
- registered acquisition contracts now cover Bailer-Jones distances,
  Hunt-Reffert clusters, El-Badry confidence-bearing wide binaries, staged
  SIMBAD identity/naming evidence, complete relevant NASA KOI/TCE/TOI/K2 and
  planet/host/transit products, official high-value Gaia external crossmatches,
  and APOGEE/GALAH/LAMOST physical-parameter releases
- the NASA slice is acquired, typed, and verified: 12 source-native tables
  preserve 206,989 rows and all 2,093 upstream fields with zero omissions,
  including the uppercase legacy Kepler KOI/TCE products missed by the earlier
  schema probe; clean raw-to-typed reproduction passes
- the pinned El-Badry Gaia EDR3 confidence-bearing wide-binary release is
  source-native typed and reproducible: 1,817,594 main rows/217 columns,
  517,993 shifted-control rows/201 columns, and both published method scripts;
  E4 bounded relation materialization now passes separately
- the pinned SIMBAD target bridge/basic/alias/bibliography slice, GALAH DR4,
  and all three LAMOST DR11 stellar products are source-native typed, verified,
  and clean-reproducible; together they preserve 47,474,126 typed rows before
  APOGEE is counted
- final SIMBAD target seed `8d940fdc1bc8eee0dc8efa7e` accounts the 24,218
  matched objects absent from the base slice. All 93 checksum-bound targeted
  queries complete with 24,218 basic rows, 140,962 identifiers, and 68,928
  bibliography links. Raw snapshot `7e251164da42ef2a93627d84` and typed snapshot
  `55a9bfcaaa943ddd035df3ab` preserve 35,321,742 rows across eight active tables;
  verification and clean reproduction pass exact content hash
  `d7b78dd6cb77e5ee2cd9c03771e1e7b893bb7439aa8d2489a95442c7e1182100`
- all 15 Gaia AP, supplementary-AP, NSS, variability/rotation, and official
  external-crossmatch hard-parallax products now have registered, disjoint
  posterior-overlap companions guarded by an executable source/field/predicate
  parity test
- Gaia AP multiple-object analysis now uses 31 deterministic partitions after
  the first 17-way attempt hit `MAXREC` in every bucket; all 7,862,084 rows are
  present in canonical snapshot `9a262636fd0c7b48d8063169`, with no partition
  above 254,593 rows
- APOGEE DR17 now uses a general registry-gated multi-HDU FITS contract rather
  than silently selecting one extension: 733,901 allStar rows/234 fields, the
  one-row six-field model-grid definition, and 2,215 field-version rows/three
  fields all pass typed verification and clean reproduction
- repeated archive-side uncertainty-envelope joins proved operationally
  unsuitable: Gaia rejected oversized plans for VMEM or left 3-, 7-, and
  31-way plans nonterminal. A general checksum-bound target compiler now derives
  the exact 189,145 DR3 source IDs from the accepted typed envelope. Seed
  `638c3ff4e58abcd355029e0f` drives 31 direct target buckets for the remaining
  AP-supplement, NSS, variability/rotation, and external-crossmatch products
  without altering envelope membership
- the final acquisition report passes 56/56 registered products with
  170,253,376 rows, 23,970,068,085 response bytes, and zero pending products;
  all nine direct-target products retain seed/hash/count/coverage lineage and
  complete 31-bucket accounting
- the five expanded Gaia releases are immutable raw snapshots and 30 typed
  Parquet tables containing 83,908,762 rows, 1,320 column occurrences, and
  6,575,792,259 bytes. All five source verifications and independent clean-state
  reproductions pass. E3 acquisition is complete; E4 scientific contracts,
  normalization, scope, and selection remain open

#### M8.3c-E4. Typed Evidence Contracts

Deliverables:

- domain tables for stellar parameters/classifications, astrometry/distances,
  photometry/extinction, spectra indexes, variability/activity/rotation,
  relation claims/orbits, clusters, planets/transits/RV, compact objects,
  extended objects, citations, and observation-product lineage
- source record, component binding, method/model, reference, epoch, uncertainty,
  quality, raw value/unit, normalized value/unit, and lineage on every evidence
  contract where applicable
- exhaustive ORB6 reconciliation, DEBCat component physics, Gaia NSS fitted
  parameters, NASA physical observations, compact spin/activity, white-dwarf
  alternatives, cluster context, SNR flux, and TESS EB metadata
- explicit M5.3 lifecycle-policy reconciliation and negative/tombstone evidence

Exit criteria:

- no scientifically useful active cooked evidence remains stranded without a
  typed destination or deliberate exclusion reason
- component- and parameter-set scope prevent system-to-member leakage and
  accidental field-wise composites

Current status (July 19, 2026, in progress):

- the immutable scientific-evidence compiler and 23-domain schema contract are
  implemented with deterministic source records, explicit binding outcomes,
  field dispositions, logical per-table hashes, cached-artifact integrity
  checks, and clean scratch reproduction
- external evidence-envelope selection is now registry-resolved and checksum
  bound to exact raw/typed snapshots, typed manifests, tables, and fields. Its
  lineage changes immutable build identity and reproduction comparison; OR
  groups and unsigned-decimal Gaia-ID normalization support bounded survey
  joins without release-ID conflation or source-specific distance heuristics
- APOGEE checkpoint `efc517c3dd6f6389abab7603` passes generic and source-
  specific audits with 178,099 bounded allStar rows, 3,280,268 coherent stellar
  measurements, 1,357,072 photometry/extinction measurements, 529,676
  coordinate/RV measurements, 173,478 product locators, all 243 field
  occurrences accounted, and no pending fields. Its logical hash is
  `d2609ad76ea2ffc4f66d9bfd01c5fb7084aa0d88c937c513d8f416ebeced2a18`.
  Compile runtime (41:53) and peak RSS (9.17 GB) make selected-row caching and
  incremental evidence insertion a required identical-hash optimization before
  GALAH/LAMOST are materialized
- optimized APOGEE v60 `e794324a7c7e86e80a3ea614` passes both audits and
  exactly matches v58 across every scientific table at content hash
  `194eede6937b26f8c0cd508f6dd7dd0a39ef34b2a455000d1f57ee18c8a5f31b`.
  Exact-hash temporary selection and incremental inserts reduce runtime to
  11:54 and peak RSS to 6.53 GB; this is the accepted compiler path for the
  pending GALAH/LAMOST adapters. Clean reproduction passes in 11:56 with no
  differing sections and removes its external scratch tree
- GALAH DR4 v62 checkpoint `a4fc03c66ea1cfb44c25df28` retains 117,885
  checksum-bound Gaia DR3 envelope rows and accounts all 184 source fields as
  169 materialized and 15 copied-catalog exclusions. It preserves spectroscopy
  and 31 elemental abundances as a coherent set, keeps mass/age/luminosity in a
  separate source-model set, and emits source-native distance/RV, extinction,
  interstellar-line, hydrogen/lithium diagnostic, identity, quality, and
  citation evidence. A v61 semantic diagnostic was rejected when the official
  source description proved `r_med/r_lo/r_hi` are distance bounds rather than
  stellar radii. Generic/source audits and clean reproduction pass at logical
  hash `7c0a367810903b18dad7e408d3feade5821325bfa8a670b5e051e1534cded8db`;
  all bindings remain unresolved and the broader E4 tail remains open
- LAMOST DR11 v63 checkpoint `a583819f0a4f3896c312f19e` independently
  materializes 1,659,281 selected observations across LRS stellar, LRS M-star,
  and MRS stellar products from 11,418,142 source rows. It accounts all 185
  field occurrences as 170 materialized and 15 copied-catalog exclusions;
  preserves LASP, CNN, molecular-index/activity, and raw/corrected RV contexts;
  and indexes official `obsid`/`mobsid` spectrum products for on-demand
  retrieval. Generic and bounded source audits plus clean reproduction pass at
  logical hash `eeb6dd86c096100175dc92d829508c8c36636d20f507993750e1f9a0b5a73d37`.
  Bindings and winner selection remain E5 work, and E4 remains in progress
- Bailer-Jones EDR3 distance diagnostic `520df722a1564ee857b1ae43`
  materializes all 17,310,560 rows as release-scoped EDR3 identities and
  coherent geometric/photogeometric posterior bundles. Its 33,225,308
  measurements retain published 16th/84th-percentile endpoint semantics,
  quality flags, units, model/method labels, and one exact bibliography link
  per measurement. All 10 source fields are accounted as eight materialized
  and two reviewed Gaia-coordinate exclusions; the generic artifact audit
  passes with zero pending fields. The source-specific audit correctly rejects
  v54 because excluded coordinates were redundantly copied into every source
  context. A v55 attempt was stopped when review found it would also remove
  intentional lineage context. Compiler v56 preserves context and lineage while
  omitting only excluded fields. Accepted checkpoint `2147d1c60f6401fdc725d96e`
  passes generic and source-specific audits with all checks at zero and logical
  hash `eceb390e97cba1b69d8a5780181b8947dfed6ed78c51167316ad4936b4506730`.
  Clean reproduction matches with no differing sections and removes its USB
  scratch tree; v56 is the accepted Bailer-Jones E4 checkpoint.
- the NASA foundation accounts 206,989 rows, 203,932 exact records, 3,057
  repeated identical row occurrences, and all 2,093 fields without using source
  array positions as identity
- NASA checkpoint `cb82c09179afa740b02e2cdf` materializes 750,151
  source-scoped identifier claims and 72,809 lifecycle claims. Confirmed,
  candidate, false-positive, false-alarm, and refuted evidence remain explicitly
  separated and do not change canonical planet counts. Per-identifier semantic
  scopes produce 697,952 separate unresolved binding outcomes instead of
  allowing mixed planet/host/target rows to leak scope.
- the NASA adapter now accounts all 2,093 fields as 2,081 materially represented
  and 12 reviewed archive-index exclusions, with no declared-pending tail. It
  emits 9,689,745 typed science rows, 272,355 coherent parameter sets, 111,084
  validation-product locators, 2,961 citations, and 4,656,423 citation links.
  This source adapter passes while the overall E4 milestone remains in progress.
- El-Badry checkpoint `aaf262b1791d98ce3e9f96e7` applies the registered
  three-sigma 1,250-ly envelope and retains 877,307 candidate relations plus
  239,406 shifted-sky negative controls. It accounts 1,218,874 filtered rows,
  3,974,770 release-scoped identifier claims, two endpoint binding scopes per
  retained relation, all 422 fields, and 1,116,713 evidence-citation links.
- the earlier SBX core collector was measured as field-losing: it retained 10
  of 29 system fields, five selected alias families, and only an aggregate
  orbit count. The separate Evidence Lake profile now acquires the complete
  rolling catalog without a spatial cut: 4,080 systems, 102,459 aliases, 261
  configurations, and 5,169 full orbit rows. Immutable snapshot
  `ea236790d0501967b3c30466` is typed and verified; the served legacy input is
  unchanged.
  `R_chance_align` remains a non-strict confidence statistic, including 289,705
  values above one; zero strict probabilities or canonical containment rows are
  fabricated. Clean reproduction and the independent artifact audit pass.
- ORB6 checkpoint `fcbb6466bea0a7798ae8d2ed` preserves all 4,051 published
  visual-orbit rows and all 37 source/lineage fields as coherent source-native
  solutions. It emits 16,397 WDS/discoverer/ADS/HD/HIP identity claims and 799
  reference-code citations without parsing combined pair labels into canonical
  endpoints or promoting an orbit into containment.
- DEBCat checkpoint `b3a141c0caf953aa83c4e52b` preserves all 374 rows and all
  30 fields. Explicit primary, secondary, and system scopes produce 3,804
  accepted physical measurements, 557 component classifications, 963 coherent
  parameter sets, 746 integrated-photometry values, and 374 binary-period
  solutions. Source `-9.99`, `-1.00`, and `none` sentinels remain in immutable
  source rows but cannot become measurements, classifications, or inflated
  uncertainties. Component bindings remain unresolved for E2/E5 reconciliation.
- Green SNR checkpoint `d08c5aa9af7dc8bcdbf0d6c3` preserves all 310 rows and
  15 fields as extended-object geometry and source-native parameter sets,
  including uncertain 1-GHz flux and spectral-index strings. It constructs 310
  deterministic Galactic `G...` identifiers without converting uncertain raw
  values into overconfident numerics.
- TESS EB checkpoint `255678b2daa6e8bf46e6dcd9` preserves all 17,605 rows and
  20 fields. Decimal normalization makes zero-padded TIC identities searchable;
  4,584 positive catalog members receive orbit solutions, while 13,021
  nonmembers remain explicit catalog-status evidence. Sector, flag, morphology,
  Tmag, astrometry, and unresolved target-context physics are typed separately.
- Targeted TIC/TOI checkpoint `11aa9bd00cc710f971b01837` preserves all
  122,772 rows and 239 fields from the bounded target set, MAST TIC, official
  Gaia release/external crossmatches, targeted Gaia DR3, and NASA TOI. Exact
  target coverage, archive-member provenance, DR2/DR3 separation, duplicate
  relations, asymmetric uncertainty lineage, lifecycle polarity, unresolved
  binding, artifact integrity, and clean logical-hash reproduction all pass.
  No candidate, false positive, or unresolved target changes canonical
  inventory.
- White-dwarf checkpoint `486e4975af015d4e5f5a3c9b` uses the published
  geometric-distance posterior lower bound to retain 337,272 of 1,280,266
  candidates whose interval overlaps the 1,250-ly buffer. It emits 2,390,432
  measurements in 597,608 separate H/He/mixed atmosphere parameter sets,
  1,348,802 identity claims, and 337,272 compact-candidate contexts. All 161
  fields are materialized or deliberately assigned to release-native Gaia,
  distance, or SDSS adapters; no atmosphere model is silently selected.
- ATNF checkpoint `64c55c19a5a10a88877d4cd2` preserves 190,671 rows from
  the source package as 91,214 repeated parameter occurrences, 644 glitches,
  1,210 full bibliography records, 97,472 catalog comments, 108 README lines,
  and 23 archive-member records. It emits 97,424 release-scoped identity claims
  and 91,858 compact-object parameter contexts. Exact source reference keys
  create 84,388 evidence-citation links; 959 non-bibliographic lexical tokens
  remain in raw parameter JSON and cannot become placeholder citations. Clean
  logical reproduction and the independent artifact audit pass.
- McGill magnetar checkpoint `c599c951590451ace4248934` accounts all 31 rows
  and 47 fields as 139 coherent compact-object contexts: 26 timing, 26 X-ray,
  25 distance, 31 position, and 31 source-context parameter sets. Each context
  retains its own reference family. Seven trailing source footnote markers are
  removed only from normalized search identity, while exact names remain raw.
  The source codes produce 96 distinct reference records and 128 links; full
  bibliography expansion remains an explicit E3/E4 follow-up. Clean
  reproduction and the independent artifact audit pass.
- SB9 checkpoint `72663823963198c8fcbbe569` preserves all 30,153 rows and 62
  table-column occurrences across the ReadMe, system inventory, aliases, and
  orbit tables. It emits 4,079 positive spectroscopic-binary claims with
  explicit primary/secondary endpoint scopes, 5,099 coherent orbit solutions,
  4,079 component spectral classifications, and 4,403 component magnitude
  measurements. Every orbit links deterministically by source `Seq` to exactly
  one relation; multiple published solutions remain separate.
- SB9 alias evidence keeps 3,543 Gaia DR2 and 3,530 Gaia DR3 claims in distinct
  release namespaces. Of 1,826 references, 1,807 direct ADS bibcodes receive
  deterministic ADS links. No SB9 relation becomes canonical containment;
  clean reproduction and the independent audit pass.
- SBX checkpoint `37ffa7255d026c8d930af6d4` accounts all 111,969 rows and all
  73 table-column occurrences from the complete source. It emits 4,080
  primary/secondary spectroscopic-binary claims, 94 explicitly scoped
  hierarchy claims, 5,169 complete orbit solutions linked to exactly one
  system relation, 3,550 source component classifications, 4,498
  component-magnitude measurements, and 20,152 astrometric measurements.
- The complete alias inventory contributes 102,459 raw and catalog-qualified
  designations plus release-correct Gaia DR1/DR2/DR3, TIC, HIP, HD, WDS, ADS,
  2MASS, TYC, GJ, HR, and WISE claims. Component-suffixed HD/HIP values remain
  exact designations; only purely numeric values enter numeric-ID namespaces.
  All 71 useful fields are materialized and two legacy coordinate strings are
  explicitly excluded while remaining in source-native Parquet. Clean
  reproduction matches logical hash
  `0ac0ff9babcd641446d2a4fdab0abcd7c19cc8ce7278c136e129507cb5663fc0`,
  and the independent audit passes without canonical promotion.
- MSC checkpoint `fc7e9dcabb0b27167c8f188c` accounts all 43,418 rows and all
  73 field occurrences across archive lineage, ReadMe, component, elementary-
  binary, orbit, and note tables. WDS-qualified component keys prevent global
  `A`/`B` label collisions; 15,748 source relations retain positive,
  ambiguous, or negative observing status; all 4,728 full orbit rows and
  14,638 period/separation summaries remain coherent evidence rather than
  canonical containment. Compiler v43 also closes the general DuckDB `t`/`T`
  alias-shadowing defect and implements catalog-declared numeric-zero missing
  semantics. Artifact, MSC source/scope, and clean-reproduction gates pass.
- WDS checkpoint `ad98d4e369c5a0addc6477a0` accounts all 157,476 WDS
  summary/format rows and 140,416 CDS WDS-Gaia candidate matches with all 43
  field occurrences materialized. WDS-qualified pair keys prevent global
  component-label collisions; observation history, relative astrometry,
  source-convention proper motions, generic magnitudes, and opaque spectral text
  remain unresolved evidence. Configured numeric bounds keep WDS `-1`, `.`,
  zero-count, and impossible-angle sentinels out of normalized measurements
  without deleting their source rows. The bridge emits 140,416 candidate
  positional relations with angular-distance statistics, zero probabilities,
  and no accepted identity, containment, or orbit. Artifact, WDS source/scope,
  and clean-reproduction gates pass.
- Gaia UCD association checkpoint `78016b90e02689547c3f53dd` accounts all
  7,630 catalog rows, 93 source-document lines, and eight field occurrences.
  Compiler v45 can emit multiple independently keyed cluster memberships from
  one source row: 6,259 HMAC assignments retain null probability, while 2,840
  BANYAN best hypotheses retain their published 0.5-1.0 probabilities. The
  table does not contain spectral types, so sample membership creates no
  classification. All 7,630 Gaia identities remain release scoped and
  unresolved; placeholder, citation, artifact, scope, and clean-reproduction
  gates pass at logical hash
  `27a516ce3fbfd67062584099c9323038e9c87f4dcb81b67d3479713d6d2958a0`.
- UltracoolSheet checkpoint `20fdb1c95d25d441160d3bd9` accounts all 3,890
  pinned rows and 242 fields. Compiler v49 adds finite-number, uncertainty-
  bound, lexical-measurement, fixed-epoch, multiple-membership, and product-
  placeholder contracts. The artifact contains 32,841 identities, 149,636
  astrometry/distance rows, 50,134 measurements across 23 photometric bands,
  10,887 direct/context classifications, 23,859 maintainer-derived/context
  parameters, 3,875 BANYAN memberships, and 3,079 real SimpleDB locators.
  Gaia DR2/DR3 remain separate, source formulas never outrank direct evidence
  at E4, list-valued aliases remain unsplit, and multiplicity/exoplanet flags
  create no endpoint-free relations. Artifact, source/scope, placeholder,
  sentinel, citation, and clean-reproduction gates pass at logical hash
  `2a7cfb5f4c34df4c17cf2e6e2fa35639d1d0181b984983f7d4779407e62e1bab`.
- Gaia NSS checkpoint `e198804d34abcf04d209d116` materializes all 50,762
  expanded two-body rows as distinct coherent orbital solutions. All 77 fields
  are accounted as 75 solution/model/quality fields plus Gaia source and NSS
  solution identity; 101,524 identifier claims remain release scoped.
  `Orbital`, `AstroSpectroSB1`, targeted-search, validated-search, and
  alternative models stay distinct, and no missing companion endpoint is
  fabricated. Clean reproduction and the independent artifact audit pass.
- corrected NSS compiler-v65 checkpoint `1881e02d8e9f1d33a1d9b64a` uses authoritative Gaia
  source parallax for the hard boundary and materializes 85,724 hard plus 1,351
  disjoint uncertainty rows. Model-qualified `(source_id, solution_id,
  nss_solution_type)` keys eliminate 2,322 false collisions; source-specific,
  generic artifact, and clean-reproduction audits pass logical hash
  `3aeabe350ec4e224ab9b04dceae6fab9678cdd27a5337919ed6c1c8912f51e5a`
- compiler/contract v65 distinguishes uncertainty error magnitudes from absolute
  posterior interval endpoints in the shared scoped stellar-parameter adapter;
  interval endpoints require explicit bound semantics and focused regression
  coverage before Gaia AP parameter-set materialization
- Gaia external-crossmatch checkpoint `81b0cc4aa29453088a62f3de` preserves all
  24,045,693 bounded official AllWISE, 2MASS, Hipparcos-2, Tycho-2, and RAVE DR6
  best-neighbour rows as candidate relations with angular-separation and quality
  context. All 62 fields are accounted, no relation becomes accepted identity,
  and source, artifact, and clean-reproduction gates pass logical hash
  `2cd08ee00ab39b699627eb2614392a7e0c4f241fe9214a476762c6cab15d87a0`
- compiler/contract v67 preserves multi-model classification probabilities as
  coherent source vectors without choosing a winner, distinguishes configured
  domain interval endpoints from error magnitudes, rejects reversed endpoints,
  and reports rather than rewrites source-native non-bracketing estimates
- Gaia AP compiler/contract v68 accounts all 482 main/supplement field
  occurrences and separates every Gaia pipeline/model context without a
  field-wise composite. Exact typed-schema reconciliation and real-row smoke
  materialization pass; immutable build, source audit, generic artifact audit,
  and clean reproduction remain the next E4 gate
- the first full Gaia AP build failed closed only at release-wide unresolved
  binding insertion. Compiler/contract v69 emits identical scopes per source
  table under the same 32-GB limit. A tmux-isolated retry proved that fix and
  then exposed a separate unbounded ordinary evidence-citation join.
  Compiler/contract v70 applies the existing deterministic 32-bucket policy to
  every evidence-reference table without changing scientific keys or counts.
  Checkpoint `393b08fa1268bbd42bb40225` passes source, artifact, and clean
  reproduction gates for all 51,164,425 rows and 482 fields at logical hash
  `b84be6a482e90bd4527f498f87f4381f1439b0e67a7ec5762c19530976ec6596`;
  failed manifestless staging trees were retired by exact-hash retention
- contract v71 accounts all 354 field occurrences in the four Gaia
  supplementary-parameter tables. It preserves MARCS, PHOENIX, OB, and A
  GSP-Phot alternatives without selecting or compositing them, and separates
  GSP-Spec ANN from spectroscopic FLAME parameter/evolution contexts. Immutable
  build `c4a6b5fd297f8ef9cceb6340`, the source audit, and the independent artifact
  audit now pass across all 8,019,372 source rows and 354 fields with no
  duplicate keys; clean reproduction matches logical hash
  `a74eb79475a76af75d7a626adb56baf89de3f6978904e7c83e4619f46bf6e052`
  with no differing sections and removes its USB scratch tree
- compiler/contract v71/v72 replaces retained runtime uniqueness indexes with
  exact pre-promotion and independent key-integrity audits for every immutable
  evidence table. A representative same-row A/B has identical logical hashes,
  zero duplicate keys, and 41.1% lower storage; main-AP block accounting shows
  retained ART indexes consumed most of the 167-GiB artifact beyond its roughly
  58-GiB table allocation. Full Gaia supplementary materialization will use the
  audited indexless contract before additional large source families land
- the E4 source-scope ledger accounts all 47 registry releases without silently
  treating identity-only, superseded, disabled, or transitional inputs as
  scientific adapters. Thirty-eight scientific adapters and nine explicit
  boundary dispositions now exist; the registered-source audit has no adapter
  blocker, stale disposition, conflict, or unregistered adapter
- Gaia source build `ab7f7e6bc211bee146885987` materializes all 32,176,271
  hard-envelope and uncertainty-supplement rows as release-scoped Gaia DR3
  identities and compact coherent source solutions. Its two ordered schemas
  retain 125 scientific fields divided into astrometry, photometry, radial
  velocity, classification/membership, and observation-product availability;
  23 copied GSP-Phot fields defer to the richer AP release, with all exclusions
  explicit. Source and generic artifact audits pass logical hash
  `1863f8da12380f845983339213a28ee7c4a0af5313bc9fee586f05e1a435a962`;
  clean reproduction matches with no differing sections and removes its USB
  scratch tree
- Exoplanet.eu, OEC, and HWC are now release-pinned, independently typed, and
  independently materialized rather than routed through the legacy merged
  lifecycle cooker. OEC preserves archive-member object scope, 160,582
  exhaustively routed parameters, 16,750 relations, confirmed/candidate/
  controversial/retracted evidence, limits, and product links. HWC contributes
  habitability features but zero lifecycle assertions. All three pass source,
  artifact, and clean-reproduction gates without changing canonical counts
- McGill build `99c17afd7461a9a6972a9348` completes its bibliography follow-up:
  publisher HTML and CDS references are byte-pinned, 97 exact external codes
  and 215 CDS references are preserved, and four unresolved historical codes
  remain explicit with no fabricated URL. Source, artifact, and clean
  reproduction audits pass
- E4 release set `a188a3adc6207d3a217d54a9` atomically composes all 38 accepted
  adapters across 36 immutable artifacts and 449,199,915,008 database bytes.
  Exact source/release membership, manifest identity, database size/hash,
  logical/scientific hashes, populated table-shard indexes, registry coverage,
  and nine boundary dispositions are fail-closed. Clean composition produces
  an identical manifest without a 449.2-GB duplicate database
- VSX release `rolling_snapshot_20260721` is checked into the registry with all
  three official artifacts. Build `d9780b76333132c0a05098b7` accounts
  11,135,737 source records and all 29 fields; it materializes 29,456,421
  release-scoped identifier claims, 10,304,607 coherent variability sets,
  5,152,350 spectral classifications, and 36,896,586 exact citation links.
  Source and artifact audits pass at logical hash
  `1aa9577c875d2efcd6f11f59428c61f5197e184986ebd3e6ee2d372bb8891e36`;
  clean reproduction matches it with no differing sections and removes its
  USB scratch tree
- the VSX collector declares the official CDS `refs.dat.gz` endpoint. Its
  830,415 exact OID/bibcode links are a historical
  partial relation covering 586,530 OIDs through OID 683,950, not complete
  bibliography for the 10.3-million-row 2026 object table; uncovered rows must
  remain reported rather than receiving inferred citations
- the July 21 VSX release pins all three official artifacts in raw snapshot
  `64f0562ef64643076d77a153` and types 10,304,607 object rows, 715 ReadMe
  lines, and 830,415 bibliography links in snapshot
  `c5446b6ab730ffe763af12f4`. Raw/typed verification, the complete source
  audit, and clean reproduction pass. Its source delta records 47 added OIDs,
  8 removed OIDs, and only 243 scientifically revised retained OIDs; bulk
  source-line movement remains lineage-only. E4 preserves 2,080 historical
  bibliography links for 1,833 OIDs absent from the current object inventory;
  56 structurally noncanonical links across 9 distinct strings remain raw
  citation evidence with no fabricated ADS URL
- the shared Horizons snapshot writer closes the E1 raw-preservation gap in the
  natural and artificial JPL collectors: exact response bytes, query records,
  checksums, reviewed target seed, collector identity, and parsed projections
  are immutable and atomically manifested. An isolated 11-target artificial
  run passes two-artifact raw/typed accounting and all response checksum/size
  checks; Photon registry cutover and E4 materialization remain pending
- current immutable Horizons acquisition preserves the same 60 natural and 11
  artificial target identities. Machine deltas isolate Neptune/Triton
  same-epoch solution revisions and expected changes across all 11 current
  artificial trajectories. Follow-up snapshots `6ae83d9fce64f13783f05e59`
  and `17fd89afbd89e4b2303b832f` add a generic parsed
  `center_target_command` beside the exact center expression; zero scientific
  values changed. A production-shaped temporary-registry preview creates raw
  snapshots `1905bbe6c368e5f6a321194a` / `677018070eec1039a43e2652`
  and typed snapshots `c8652fd839feaa7b59104ad3` /
  `b9e3f2cced96b8f2b071b7ea`; raw/typed accounting, source audits, and clean
  reproduction all pass
- registry/contract v16/v75 cuts both Horizons releases over as separate E4
  adapters. Build `236a7b7822c52fef8b903d58` accounts 142 source records and
  all 67 fields, preserving 71 exact responses, 71 linked orbit/trajectory
  solutions, 71 center relations, 36 coherent physical parameter sets, 73
  citations, and 178 evidence links. Operator seed identities remain distinct
  from parsed JPL target identities, and only JPL identities form relation
  endpoints. Generic/source audits and clean reproduction pass logical hash
  `c81a10d4f97f6dd99be09852b3b68a1f33dca852828ff18132a6e9d3362ca1bb`
- the Gaia DR3 backbone pre-adapter audit passes all 32,176,271 rows across its
  disjoint hard-parallax and uncertainty-envelope branches, with identical
  complete 152-field schemas, unique identities, correct boundary polarity,
  explicit RV/XP/epoch-photometry/RVS product coverage, and an exhaustive
  zero-unclassified source-column role ledger for E4
- the checked-in Gaia variability audit passes 592,197 rows and every one of 52
  rotation-vector fields with exact length/mask semantics and an exhaustive
  source-column role ledger, establishing coherent per-source variability and
  rotation parameter sets for E4. Compiler/contract v72/v73 materializes all
  592,197 rows through four ordered schemas with 268/268 field occurrences and
  592,197 citations accounted; artifact/source audits and clean reproduction
  pass logical hash
  `d98283bb5477211963902e072b4aaf7095740435efeff567950dbcfe934dea2b`
- the complete-envelope SIMBAD v37 diagnostic failed closed, without host OOM
  or artifact promotion, when one bundled-astrometry citation join reached the
  configured 16-GB DuckDB cap. Compiler/contract v38 partitions that general
  operation into 32 exhaustive source-record hash buckets, disables unnecessary
  insertion-order preservation, and retains ordered duplicate-sensitive
  logical hashes. Checkpoint `fc5bd4e6398d72bde50ba6d5` passes the independent
  artifact audit and clean reproduction at logical hash
  `673cebbbfcc4055fb7a6a007824ba11eac75bcc7b038bb138a15abf6cf9288d7`.
- WGSN checkpoint `0ff30b04008b93aafb3de66f` materializes all 597 official
  name records and 22 fields into 3,847 source-scoped identity claims, 91
  meaningful references, and 564 evidence links. Shared HIP and Bayer values
  remain distinct ambiguous-scope evidence; no merge or containment is inferred.
  Artifact/scope audits and clean reproduction pass logical hash
  `512b05b67ca0632bbe164b82e1b96182643e9b4e911da6b8ce9d8bdba1d37fe5`.
- GCVS checkpoint `a6f6669d2bd48eac5d6204d2` materializes all 340,839
  rows from the six registered release tables as 705,684 source-scoped identity
  claims, 289,892 astrometric measurements, 29,042 source spectral
  classifications, 444,566 variability observations, 21,526 citations, and
  756,305 evidence links. Component suffixes remain distinct, variable type is
  not conflated with stellar spectral type, and no claim is promoted to a
  canonical binding. Artifact/source-scope audits and clean reproduction pass
  logical hash
  `a4d78bb721d6017031a2e9a53e2b86701395d0c67ff0dd6016af639bad416967`.
- Hunt/Reffert checkpoint `7e66e0690aa962c837d43a86` retains 465 clusters
  whose published distance posterior overlaps the 1,250-ly evidence boundary,
  plus all 51,017 probability-bearing member rows and 451 literature
  crossmatches attached to those clusters. All 161 fields materialize as 916
  cluster contexts, 51,017 membership records, 154,883 endpoint identity
  claims, and 51,933 citation links. All bindings remain unresolved and no
  membership becomes canonical containment. Artifact/cluster-scope audits and
  clean reproduction pass logical hash
  `14351918254e338cd28f796b3d1837eeeed1ad094c23d0ea27d408effea8d78b`.
- Extended-catalog checkpoint `54d1b0b6a841344c48327991` accounts all 19,868
  pinned OpenNGC/nebula rows and 238 fields as 19,012 extended-object records,
  856 source-document lines, and 21,107 exact catalog identity claims. Raw
  alias lists remain source parameters rather than guessed individual claims;
  component-bearing Cederblad records do not also claim the base designation.
  All bindings remain unresolved and no object evidence becomes a relation or
  orbit. Artifact/extended-scope audits and clean reproduction pass logical hash
  `456e7a36cfd7e08ea5f7ce19c44817114de5d54d1e077ae365e2668c8191bd2d`.
- build identity hashes compiler and registry bytes, contract, runtime versions,
  and raw/typed inputs; clean logical-hash reproduction remains required

#### M8.3c-E5. Selection and Derivation Compiler

Foundation checkpoint (July 21, 2026):

- policy `2026-07-21.e5-selection.1` and compiler v1 produce immutable build
  `237158e09fce993f1b033414` from the pinned E4 release set, E2 identity graph,
  and canonical stability reference
- the build selects 12,229,171 exact Gaia/NASA source facts and derives 65,204
  missing stellar luminosities from selected radius/temperature inputs; all
  12,294,375 output facts retain evidence or derivation lineage
- 4,136,484 coherent-set decisions pass duplicate, missing-lineage, and
  lower-authority-winner gates; checked-in fixture coverage exercises source
  authority, exact interval endpoints, error magnitudes, unique-name binding,
  and derivation supersession JSON
- this foundation checkpoint did not close E5: remaining evidence domains,
  legacy fallback inventory, shared consumer migration, and E6 shadow review
  were mandatory; the inventory is now complete below, while the other gates
  remain open
- coherent Gaia-source checkpoint `e8cb1529df6dbcc7c5baadee` adds 89,068,940
  selected astrometry, photometry, radial-velocity, and diagnostic facts for
  5,866,595 current stars without copying all 125 source fields; total E5 output
  is 101,363,315 facts and 27,602,864 decisions
- 40 deterministic fact partitions and nine decision partitions account every
  output row, and a clean `/mnt/space` reproduction matches logical hash
  `330614599768f062123305aece47c7965f0ff5114a7f9c293498869145e9327c`
  with no differing section and complete scratch removal
- selected-fact checkpoint `bfe3e1da9ddc5257f79b6838` binds the registered
  Bailer-Jones EDR3 distance envelope through an explicit authoritative
  EDR3-to-DR3 release relationship, selecting 4,662,948 geometric and 4,344,950
  photogeometric distance facts without treating other Gaia releases as
  interchangeable
- the compiler now records one accepted, missing, or ambiguous outcome for all
  57,716,013 eligible binding attempts across the four selected sources; the
  12,647,612 Bailer-Jones rows outside current canonical identity remain
  inspectable and cannot emit selected facts
- the current artifact contains 110,371,213 facts and 36,610,762 decisions;
  independent artifact audit passes and clean reproduction matches logical hash
  `372cf0c7abf642684b46b2bf6590f6f3fd275d9f328e3e0aac6f15119525fda6`
  with complete 53-GB scratch removal
- audited inventory `2026-07-21.e5-legacy-inventory.1` accounts 24 legacy
  science, empirical, projection, ranking, and presentation paths; all 28
  implementation bindings, 16 discovered versioned markers, four E5 successor
  derivations, and materialized ARM methods are accounted with zero remainder
- the inventory exposes duplicate runtime/map/coolness selection as explicit
  cutover or retirement work; it does not declare those paths authoritative or
  close the remaining domain-policy and shared-consumer gates

Deliverables:

- versioned, per-quantity authority and applicability policies
- coherent parameter-set selection with alternatives and conflicts preserved
- selected-fact lineage pointing to exact evidence or derivation records
- derivation inventory covering every current inferred/assumed value, with
  inputs, algorithm version, applicability, uncertainty/confidence, and
  supersession state
- automatic detection when an empirical or presentation fallback wins despite
  acceptable higher-authority evidence
- shared selected-fact consumers for HZ, planet categories, classification,
  simulations, search, map, API, tags, and later AAA packets

Exit criteria:

- source, derived, empirical, and presentation-prior values cannot be confused
- UI and renderer code no longer make independent scientific selections
- improved evidence measurably reduces unnecessary fallback utilization without
  converting model estimates into falsely labeled direct measurements

#### M8.3c-E6. Shadow Canonical Build and Scientific A/B Review

Deliverables:

- deterministic shadow CORE/ARM/hierarchy/DISC build and public slice
- machine and human-reviewable A/B reports for inventory, identity, hierarchy,
  aliases, stellar parameters, classifications, fallback use, luminosity/HZ,
  planets, orbits, clusters, variability, compact objects, APIs, search, map,
  simulations, storage, and performance
- named goldens used only as observations of general policy, never as transform
  conditions
- clean-state reproducibility and source/field/identity accounting reports

Exit criteria:

- every inventory or selected-fact delta is attributable to a reusable policy
  and evidence lineage
- canonical planet counts/status remain uncontaminated
- deterministic rebuild, public slice, API/search/map/simulation, and storage
  budgets pass

#### M8.3c-E7. Promotion, Cutover, and Legacy Retirement

Deliverables:

- reviewed atomic local promotion with the previous build retained for rollback
- stable public deployment checkpoint only after explicit operator review
- removal or formal deprecation of old collectors, cookers, stranded schemas,
  and duplicated selection/fallback paths
- updated source, schema, ingest, retention, iteration-history, API, and
  operational documentation
- Gaia DR4 release adapter plan using the same release-scoped contracts rather
  than tying permanent identities or evidence schemas to DR3

Success criteria:

- Evidence Lake v2 is the only production scientific compilation path
- a clean build can reproduce the served projection from pinned inputs
- every selected public fact is inspectable back to evidence or a versioned
  derivation
- no deployment to antiproton occurs until E0-E6 pass and the user accepts the
  scientific A/B checkpoint

### M8.3d. Public Evidence Inspector (Later)

Goal:

- make Spacegate's selected values, competing evidence, and provenance
  inspectable without overwhelming the primary System Page narrative.

Dependencies:

- M8.3c stable public evidence API and selected-fact lineage
- Concept Tag presentation vocabulary stable enough for plain-language context

Deliverables:

- collapsed evidence section low on the System Page
- selected value and reason, uncertainty, method/model, source, reference, and
  retrieval/release lineage
- expandable competing values, conflicts, limits, negative evidence, and
  superseded derivations
- object/component focus, accessible tables, citation/linkouts, copy controls,
  and explicit source-versus-derived language
- bounded summary API plus paginated detail so pages do not ship the evidence
  lake by default

Success criteria:

- visitors can determine which value Spacegate chose, why, and what credible
  alternatives exist
- collapsed-by-default presentation adds negligible initial System Page cost
- evidence display never implies that a selected model estimate is a direct
  measurement or that a conflict has disappeared

### M8.3e. Interactive Observation Labs (Later)

Goal:

- let visitors inspect real images, spectra, exoplanet atmosphere spectra,
  light curves, and related observations through scientifically grounded,
  accessible tools.

Dependencies:

- M8.3c observation-product indexes, calibration metadata, and lineage
- M8.3d public provenance patterns
- Concept Tag and explanatory presentation foundations

Deliverables:

- reusable observation-product viewer contract with attributed remote or cached
  products, checksums, calibration context, units, uncertainty, and citations
- first spectrum analyzer with pan/zoom, wavelength/unit modes, line overlays,
  radial-velocity/redshift context, comparison spectra, and element explanations
- light-curve/transit inspector and image/multi-wavelength viewer using the same
  product and provenance contract
- curated game-like learning missions such as identifying lines, locating a
  transit, or comparing model spectra, with caveats and explanations
- saved progress/scoring in presentation or community state only; visitor
  guesses never alter canonical science or accepted evidence

Success criteria:

- real observations remain distinguishable from models and annotations
- interactive tools are useful without requiring bulk products in hot databases
- gamification rewards conceptual understanding rather than false precision

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
  descendants. Castor now verifies inspectable A/B/C subsystem groups; `Castor
  AB` remains a stellar leaf inside the A group rather than a parallel subsystem.
  This creates no new science-layer stars or orbit solutions.
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

### M8.5. Time and Rim-Ready Rendering

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

July 15, 2026 public deep-map observation exposed a promotion gap: build
`20260715T015659Z_e392a11_side_rebuild` contains no prebuilt simulation-scene
artifacts, while an older local build contains 1,001. Cold, ordinary singleton
scenes measured approximately 1.3-2.2 seconds each on Photon, and map Peek was
also issuing duplicate detail and search enrichment requests. A cold profile
executed about 70 DuckDB statements in 1.23 seconds; public-system assembly used
1.06 seconds, including about 0.50 seconds in hierarchy assembly/reconciliation
for a lone star. Peek now reuses
the simulation-scene body/system payload for source tooltips, defers coolness
enrichment until tooltip intent, and debounces scene loads by 150 ms so rapid
selection does not launch every transient request. The initial response proposed
making priority-scene materialization a promoted-build gate. Subsequent cache
work showed that this would conflate performance warming with scientific
correctness: uncached scenes use the same public contract and persist into a
bounded build-keyed runtime cache. Promotion may therefore omit scene warming,
while deployment review must still account for the expected cold-request CPU
profile.

July 15 follow-up implements the first server-side scale correction. Dynamic
scenes now write compatible build-keyed compressed runtime artifacts, same-scene
cold requests are coalesced, and side builds can run priority scene
materialization before promotion. A Castor smoke test measured about 1.19 s
cold versus 16 ms from the generated artifact, with one assembly across two
simultaneous requests. Map tile schema v3 also carries a packed repeated-class
sequence for `Off`/`Primary`/`All` label badges without object-detail requests.
Component badge values remain governed by the general ARM evidence precedence;
no system-specific classification overrides belong in the build path.

July 16 follow-up versions this semantic boundary as
`simulation_scene_artifact_v2`. The API rejects v1 prebuilt scenes rather than
serving embedded stale classifications or names; a side-artifact build may
rematerialize its bounded priority set before deployment or warm the compatible
runtime cache afterward.
Candidate verification then exposed that the side builder copied v1 scenes and
the materializer counted any existing filename as reusable. Reuse now requires
both the current materializer version and the exact target build ID, preventing
science-changing ARM rebuilds from inheriting stale scene payloads.

July 16 stellar-leaf follow-up replaces the remaining surface-specific badge
assembly with `stellar_leaf_display_classification_v1`, a deterministic ARM
projection containing exactly one row per eligible canonical-hierarchy stellar
leaf. Map tile v4, system detail, hierarchy, OBJECTS, Peek, and simulation-scene
v3 consume that same projection. Aggregate nodes and inferred nonstellar
endpoints are excluded; repeated and unknown leaf classes remain visible; and
conflicting evidence stays auditable on the selected row. The verifier covers
all projected leaves plus HD 110067, HD 79107, Gl 161.1, HD 18134, and Castor as
general regression targets rather than system-specific ingestion branches.
Tile v4 also adds a bounded six-bit confirmed-planet presentation mask, while
ambiguous physical/environment cases remain unbadged.

Verified local checkpoint `20260716T1905Z_ad13e39_side` passes strict build,
exact tile membership, all-leaf projection, and 1,000/1,000 priority-scene v3
gates. Live Photon API and browser checks agree on `GKMM`, `FKMM`, `FKMMU`,
`MUUUU`, and `AAMMMM` across the reported systems. `Gl 161.1` is the first
exact search result with HIP 19335, HD 25998, and Gaia
225668203191521280. Planet masks are deployed only where composition and
environment evidence are sufficient; a general orbital-distance/host-
luminosity fallback for Solar System rows remains explicitly pending.

July 16 presentation/runtime follow-up adds `Warm Simulation Scenes` to the
allowlisted Admin background-action catalog. The action targets only
`cache/simulation_scenes/<build_id>/`, never `served/current` or immutable
`out/<build_id>` contents, and is bounded to 10,000 priority systems. This makes
scene warming an explicit post-promotion CPU-versus-latency decision while
retaining on-demand assembly as the correctness path. Map labels now place
planet badges to the right of system names with distinct ringed styling;
neighbor lines carry distance-only labels while their endpoints use normal
system labels; and camera-local Cool Stars Nearby recommendations update on a
2-ly quantized position to avoid per-frame full-catalog sorting.

July 16 object-badge and membership follow-up removes the remaining compact
card cap: Star Search/map result cards and System Hero now render every ordered
projected stellar leaf plus every confirmed CORE planet, retaining object keys
for future object-detail navigation. Search result responses expose the same
batch object-badge contract, and the scrolling map result header keeps its
Close action visible. Simulation-scene artifact v4 treats the canonical
stellar-leaf projection as membership authority and joins it through unique
canonical/evidence aliases, fixing HD 57041's white-dwarf display without a
system override. The canonical ingest also rejects MSC component rows grossly
displaced from their claimed WDS field and bridges exact authoritative
Gliese/GJ catalog roots or member-name roots into an existing WDS system only
under distance, sky, and unique-best-match bounds. The first regression target
is Struve 2398: its real Gaia A/B pair must become one system while a
misassociated V1298 Aql MSC surrogate is excluded.

The July 17 verified checkpoint is canonical build
`20260717T0035Z_868b4d9_canonical` and deployable public side artifact
`20260717T0057Z_868b4d9_side`. The public artifact contains 5,869,091 systems,
5,874,636 CORE stars, and 6,311 planets; all 5,879,140 projected hierarchy
leaves are accounted for exactly once. Tile-v4 verification passes at
100/250/500/1,000 ly with zero missing or extra systems, and 1,000 priority
simulation scenes were generated under `simulation_scene_artifact_v4` with
zero failures.

The general runtime correction was deployed to antiproton as a code-only
checkpoint at 2026-07-15 18:33 UTC. The deployment deliberately retained served
science build `20260715T015659Z_e392a11_side_rebuild`; it did not rebuild or
promote CORE, ARM, DISC, hierarchy, or map-tile artifacts. Public health/auth,
API integration, known-system/search goldens, and the desktop map
selection-to-Peek Playwright flow passed after container replacement. Priority
scene materialization is now available as optional pre-promotion artifact work
or post-promotion Admin cache warming; a cheaper singleton contract, cache
telemetry, and explicit concurrency budgets remain follow-up work rather than
blockers for the Concept Tag/AAA path.

The initial Photon review artifact was withdrawn after audit found that it
included four manually curated Castor component classifications. Spacegate does
not accept system-specific build overrides as a substitute for the planned AAA
adjudication path. The general simulation-cache and tile-v3 changes remain, but
the review checkpoint must be regenerated without those rows before promotion.
The rollback also removes an older literal Castor-name branch from ARM; ARM now
uses the canonical CORE system name while public aliases remain the API naming
layer. It also removes the verifier-specific `CC` endpoint exemption. A clean
catalog-only ARM rebuild exactly matched the pre-change Castor classifications
at that checkpoint. Subsequent pinned SB9 ingest supplies source `dM1e` spectra
for AB/BB, which normalize to M after the general case-aware parser correction;
CA/CB retain MSC M spectra. CC is a 0.05-solar-mass brown-dwarf candidate
without an accepted stellar classification. The audit also found that canonical
hierarchy currently calls all seven endpoints inferred stars while ARM types CC
as substellar; endpoint typing must be reconciled generically. Castor-specific
build-report counters were removed, and the dedicated multiplicity verifier now
scopes component-label checks by stable WDS identity instead of display-name
matching.

The audit also localized the TESS search mismatch: post-slice ARM rebuilds are
re-adjudicating identities against the pruned public core, allowing 32 formerly
ambiguous hosts and one removed-host rematch to become accepted. TESS identity
must instead be adjudicated once in full canonical context and projected into
the public slice.

Resolved July 15, 2026: side/public builds now project the full-canonical TESS
partition and fail verification if any projected identity or TOI host binding
differs. The deterministic full candidate and its 1,000-light-year public slice
retain canonical planet counts and do not re-adjudicate against trimmed rows.

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

Checkpoint note, July 16, 2026: side build
`20260716T154843Z_622f336_side_rebuild` regenerated all 1,000 bounded priority
simulation scenes under the v2 materializer contract and passed strict local
verification before promotion. Browser gates preserve Castor as a complex-system
regression target while asserting parser and evidence-precedence invariants
rather than a catalog-version-specific count of `dM1e` source rows.

Checkpoint note, July 17, 2026: public verification of the initial badge-v4
deployment found that canonical hierarchy omitted an unmatched terminal
single-letter MSC star even though ARM retained it. The replacement hierarchy
uses a general terminal/source-tree/WDS-pair/resolved-sibling gate and contains
no object-specific transform branch. Post-deployment verification then found
that the first rule could double-count an unmatched endpoint when canonical
leaves already represented the source tree's full capacity. The replacement
rule accepts the complete candidate set only within the source terminal-leaf
deficit: full canonical hierarchy accepts 658 WDS-supported candidates and
suppresses 61 with explicit reasons. Replacement side build
`20260717T0614Z_f452835_side` keeps Nu Sco at seven display leaves, Struve 2398
at two, retains the prior public CORE inventory exactly, passes strict and
exact-tile verification, and regenerates 1,000/1,000 scene-v4 artifacts. It was
deployed to antiproton on July 17 after archive checksum agreement; public API,
known-system, targeted identity, and map browser verification pass, with the
superseded `20260717T0336Z_8bee500_side` retained as the immediate rollback.

### System Narration Foundation v1

System pages now have a reusable narration block contract instead of one-off
page copy. `/api/v1/systems/{system_id}` emits DISC-scoped
`narrative_blocks` for What You're Looking At, Why This System Matters,
Infrared View, What We Know, What Remains Uncertain, and Further Exploration.
The current blocks are deterministic fallback prose generated from served
core/ARM/DISC evidence; reviewed AAA narration can supersede them later only
through explicit publication/review state and evidence inputs.

The first pass also connects WISE/AllWISE infrared imagery to layperson-facing
explanation and throttles System Simulation when scrolled out of view, reducing
GPU/CPU use while readers move through narrative, hierarchy, and evidence
sections.

## Governance Rule

No milestone in M6+ should compromise M1-M5 scientific integrity gates.

If there is conflict:

1. protect core correctness
2. keep derived content explicitly labeled
3. delay feature launch rather than blur canonical truth boundaries
