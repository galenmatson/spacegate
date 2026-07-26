# Public Read Architecture v2

Status: complete and locally accepted on
`feature/public-read-architecture-v2`; public deployment was not performed

This document defines the public consumer boundary introduced after the Evidence
Lake v2 promotion. It does not change scientific authority. CORE, ARM, DISC, and
RIM remain the authoritative layer boundaries; the public-read artifacts are
immutable, build-keyed projections of already selected science.

## Decision

Spacegate will adapt its existing public route and browser contracts while
replacing the scan-heavy implementations behind them.

The existing FastAPI routes, response shapes, URL handoffs, and React consumers
already encode working public workflows. FastAPI overhead was not material in the
M8.3d measurements. Rewriting those surfaces would create broad UX risk without
addressing the measured bottleneck.

The query and assembly internals do require a bounded rewrite:

- SQLite FTS5 and indexed exact tables serve interactive identity and search.
- Compact relational system, star, planet, alias, and selected-fact projections
  serve summaries and ordinary singleton details.
- Stored hierarchy bundles serve nontrivial canonical systems.
- Singleton scene seeds serve ordinary one-star/no-planet previews.
- Build-keyed compressed full scenes serve multistar systems, planet hosts, and
  policy-selected priority systems.
- DuckDB remains the compiler, bounded evidence-query, diagnostic, and temporary
  compatibility engine. It is not the normal camera/search/detail request path.

This is a logical module boundary, not a new network service. Distributed
databases, worker clusters, NFS runtime queries, and LAN runtime dependencies are
outside M8.3e.

## Measured Reason

The retained July 17 build and the Evidence Lake build both lack indexes on the
legacy public search-term table. Warm primitive timings established that the
architecture debt predates Evidence Lake:

| Pinned control | Aggregate p95 | Throughput |
| --- | ---: | ---: |
| Retained July 17 legacy consumer | 735.6 ms | 1.62 requests/s |
| Evidence Lake legacy consumer | 559.9 ms | 2.34 requests/s |
| Evidence Lake Public Read v2 | 15.6 ms | 131.19 requests/s |

Evidence Lake increased the search-term inventory from 6.69 million to 12.77
million. The short retained controls show host/cache variance strongly enough
that they must not be interpreted as a release-to-release scientific cost
ratio. They do establish that both retained scientific builds pay the same
hundreds-of-milliseconds legacy consumer cost. Public Read v2 changes the
architecture and is roughly 36 times lower latency than the matched
pre-projection Evidence Lake control. M8.3d also showed that warm traffic was
CPU-bound with no material disk I/O or I/O pressure. The durable correction is
indexed, build-specific read data, not a larger DuckDB connection pool.

## Current Consumer Inventory

| Surface | Public route or artifact | Required data |
| --- | --- | --- |
| Star Search and Search Cards | `GET /api/v1/systems/search` | exact identity, aliases, object focus, position, distance, counts, temperature range, spectral mask and badges, planet categories and badges, coolness, preview tier, sort/cursor state |
| Map search and handoff | `GET /api/v1/systems/search`; immutable map tiles | same compact system identity and facets; stable `system_id`/object key; tile position remains separate |
| Map sidebar, Recents, Cool Stars | search result/summary objects | name, position, distance, coolness, counts, stellar and planet badges |
| Peek and Explorer initialization | summary plus simulation contract | identity, display name, badges, counts, selected preview policy |
| System Page hero and objects | `GET /api/v1/systems/{system_id}` | system summary, selected stellar/planet values, aliases, provenance states, hierarchy, deterministic narrative inputs |
| Hierarchy tree | system detail/hierarchy bundle | accepted canonical nodes/edges, leaf classifications, quick facts, selected relation/orbit presentation inputs |
| Full simulation | `GET /api/v1/systems/{system_id}/simulation-scene` | stable bodies, selected physical fields and lineage, canonical hierarchy, selected coherent orbits, render policy and diagnostics |
| Singleton preview | singleton scene seed | one system/star identity, selected class, temperature, luminosity, radius, mass, fact IDs and status, render/HZ policy versions |
| Evidence inspection | bounded ARM endpoints/admin diagnostics | alternatives, conflicts, parameter sets, source rows; intentionally excluded from hot summaries |
| Extended objects and infrared | existing bounded routes | separate extended-object and observation-evidence domains; not folded into stellar Search v2 |

All browser API calls remain centralized in `srv/web/src/api.js`. Map tile
selection and static delivery remain independent of Search v2.

## Scientific Field Boundary

The read compiler does not rank sources or derive new scientific values.

- Identity and containment come from permanent CORE identity and accepted
  canonical hierarchy.
- Stellar values and display classifications come from shared selected-fact
  projections in ARM.
- Planet lifecycle and selected planet values come from canonical CORE/ARM
  projections.
- Coolness and public presentation fields come from the active DISC profile.
- Search aliases preserve source, release, priority, target scope, and object
  focus.
- Every projected selected value retains its selected-fact ID or derivation
  status needed to reach the bounded evidence API.

Browser code may turn selected luminosity and a pinned HZ policy into inexpensive
geometry. It may not select among evidence records, infer a missing luminosity,
or silently substitute presentation priors.

## Duplicate Consumer Logic Audit

| Logic | Disposition |
| --- | --- |
| Legacy API `_search_preview_policy` count/class/coolness heuristic | Retained only inside the instrumented DuckDB compatibility path |
| Browser lightweight-preview count/class/coolness heuristic | Removed; projection consumers require compiler-assigned `scene_representation`/preview policy |
| Stellar class text parsing in badge components | Presentation normalization and compatibility only; projected object badges and selected leaf classifications win |
| System/public naming preference | Shared display-name policy consumes projected alias kind, priority, scope, and style; it does not create identity |
| DISC narrative lookup | Full bundles carry existing narrative payloads; projected singletons generate deterministic blocks from projected inputs without probing DuckDB |
| Simulation/admin payload narration | Renderer and diagnostics builders explicitly skip unused narrative assembly; immutable DISC table-existence probes are fingerprint-cached for bundle work |
| Map representative class and label priority | Remain immutable tile presentation fields compiled from the same selected classifications and DISC policy |
| Planet category filtering and badges | Consume compiled category flags/classes; browser code only decodes flags and renders icons |
| HZ and condensation-line geometry | Intentionally client-side from selected luminosity/temperature plus pinned model versions; no evidence selection occurs |
| Orbit position, scale compression, and equal-mass visual fallback | Renderer geometry only; source/derived/assumed state remains explicit and no canonical orbit is created |
| Canonical hierarchy and selected group-pair orbits | Compiler/bundle output only; missing required bundles fail visibly |

This audit distinguishes presentation math from scientific selection. The
former may remain in the renderer when it is versioned and inspectable; the
latter belongs in Evidence Lake selection and the immutable read compiler.
If a future build introduces reviewed singleton narrative rows, the read
compiler must project them explicitly before the continuous DuckDB lookup can
remain retired.

## Artifact Contract

The first implementation uses one SQLite database per immutable scientific build:

`derived/public_read/<build_id>/public_read.sqlite`

Its manifest records:

- build, compiler, schema, search, seed, hierarchy, render, and HZ policy versions;
- source artifact hashes and byte sizes;
- table counts, collision and omission accounting, and logical table hashes;
- SQLite version, final artifact hash, byte size, and phase timings;
- exact identity and quarantine coverage;
- deterministic rebuild comparison.

The database contains separate tables for:

- `systems`: compact facets, positions, counts, DISC values, and representation policy;
- `stars`: public selected values, classifications, status, and selected-fact lineage;
- `stellar_badge_overlays`: a compact, separately versioned exceptional
  projection for systems whose selected hierarchy leaves differ from the
  canonical root-star rows; 16,167 rows cover 5,312 systems without duplicating
  the remaining inventory;
- `planets`: public confirmed-planet summary values, lifecycle state, and a
  compact per-quantity selected-fact lineage document containing uncertainty
  bounds and fact IDs for every populated orbital, size, mass, temperature,
  and insolation scalar;
- `aliases`: scoped source aliases;
- `search_terms`: every accepted public search term with focus identity;
- `exact_identifiers`: all 6,669,279 accepted permanent identifiers with
  namespace, object focus, binding, source, release, and evidence lineage;
- `search_terms_fts`: trigram candidate index; edit distance is computed only over
  a bounded candidate set;
- `identifier_outcomes`: accepted, missing, excluded, ambiguous, deferred, and
  quarantined exact-identifier outcomes, including explicit TIC/TOI semantics;
- `identifier_quarantine`: all 81,043 current reviewed quarantine records;
- `singleton_scene_seeds`: an indexed deterministic view over 5,861,345 eligible
  `systems` and `stars`, avoiding a second physical copy;
- `hierarchy_bundles`: 14,145 compressed, versioned nontrivial
  hierarchy/detail payloads.

The parity-complete candidate is 16,455,413,760 bytes with SHA-256
`0748a315ece80813c3349d4e8cc3495fbd0ffeb67745ba2aa3c225acc60e621f`
and accounts for 5,869,091
systems, 5,874,636 stars, 2,826 planets, 1,026,480 aliases, 12,768,410 search
terms, 6,669,279 accepted identifiers, 54,237 explicit identifier outcomes, and
81,043 quarantine rows. Its versioned full-scene policy selects 7,724 systems.
All selected artifact-v7 scenes materialized without failure in 2,812.852
seconds using 12 isolated workers. Their 80,415,323 compressed bytes freeze
into one 80,752,521-byte deterministic archive; a second freeze reproduced
SHA-256
`519ac2c7951a791bdd2b9cae2b7142475a42c706348e8bb14d2c8dedb5aeba9c`.
Full validation of the warmed payloads took 3.831 seconds.

The artifact is opened read-only and immutable. Runtime validation requires exact
build, projection, search, and stellar-badge-overlay schema identity. Missing
artifacts and corrupt, mismatched, sample, or incompatible artifacts fail
visibly. The legacy path is available only with the explicit
`SPACEGATE_PUBLIC_READ_COMPATIBILITY_FALLBACK` diagnostic setting; it is never
the default production response to an artifact failure.

`scripts/profile_public_read_plans.py` records the SQLite query plan for exact
terms and identifiers, summary/object reads, hierarchy bundles, singleton seeds,
trigram candidates, filtered search, and unfiltered coolness ordering. M8.3e
specifically indexes the null-safe coolness sort expression; the former
`(coolness_rank, system_id)` index did not match the public ordering and caused a
5.87-million-row scan and temporary sort.

## Search v2 Semantics

Search uses the following bounded order:

1. Exact identifier or exact normalized name through a B-tree.
2. Prefix range lookup through the same ordered index.
3. Substring/token candidates through FTS5 trigram indexes.
4. Typo candidates through trigram overlap followed by bounded edit distance.
5. Compact facets and stable sort keys applied to candidate systems.

Levenshtein distance is never evaluated across the complete term inventory.
Exact TIC/TOI outcomes are checked before fuzzy retrieval. An excluded,
ambiguous, deferred, missing, or quarantined identifier produces an explicit
exact-no-match result and cannot leak into unrelated fuzzy results.

Stable selection uses `system_id`, stable object keys, and optional focused object
identity. Array position is never an identity.

## Simulation Tiers

### Singleton seed

Eligible one-star/no-planet systems are represented by an indexed seed rather
than a dynamically assembled full ARM diagnostic response. The seed contains
selected values and lineage only. Ordinary geometry is deterministic and cheap.

### Full scene

The full scene contract remains authoritative for:

- every multistar system;
- every confirmed planet host;
- policy-selected complex or rare compact/exotic systems;
- policy-selected high-coolness systems;
- general regression/golden classes.

Selection is based on versioned properties, never a named-system production
branch. Full scene warming is a resumable admin job. A verified warmed set can be
frozen into an immutable build-keyed artifact without changing CORE, ARM, or DISC.

The 170,351 ordinary one-object compact systems, primarily white dwarfs, retain
their selected compact-object classification and fact lineage in singleton
seeds. They do not require one redundant full-scene file each. Rare compact
systems that need specialized bodies or hierarchy remain eligible for the full
policy tier.

Verbose readiness and reconciliation diagnostics may be lazy-loaded separately,
but no renderer-critical body, hierarchy, orbit, or scientific field is removed.

## Adapt Versus Rewrite

| Surface | Decision | Reason |
| --- | --- | --- |
| FastAPI application | Adapt | Framework overhead is not the measured bottleneck; preserve routes and auth/admin integration |
| Search query implementation | Rewrite internally | Full scans and inventory-wide edit distance do not scale |
| System summary/detail assembly | Rewrite internally | Repeated CORE/ARM/DISC scans duplicate selected presentation state |
| Hierarchy projection | Adapt compiler, add bundle | Existing canonical hierarchy semantics are hard-won; materialize its accepted output |
| Simulation compiler | Adapt and split tiers | Preserve fidelity; remove singleton diagnostic assembly from hot requests |
| Map tile transport | Keep | Already immutable, static, tiled, and outside DuckDB camera flight |
| React route structure and visuals | Adapt | Working UX; migrate contracts without redesign |
| Evidence/admin queries | Keep bounded DuckDB | Scientific inspection needs flexible relational access and is not a continuous public path |
| Extended objects/infrared | Defer bounded migration | Separate domains; not responsible for measured stellar search/scene saturation |

## Compatibility Window And Retirement

Compatibility fallback counters are exposed with health/runtime telemetry. The
normal production configuration disables fallback. The explicit diagnostic
legacy path can be removed entirely when:

1. all canonical systems, aliases, accepted terms, and identifier outcomes are
   accounted for;
2. search/filter/sort/object-focus parity passes;
3. all nontrivial hierarchy targets have verified bundles;
4. all full-scene policy targets have verified artifacts;
5. desktop/mobile browser and API goldens pass;
6. constrained cold/warm/burst/sustained/recovery capacity gates pass;
7. rollback to the prior immutable build is tested.

All seven criteria pass locally for the accepted M8.3e artifact and its retained
rollback. The diagnostic switch remains explicitly bounded through the first
public deployment and rollback verification; it is disabled in normal
configuration and recorded whenever used. It is not an artifact-loss fallback.

Deprecated databases remain retained evidence/build artifacts during the
compatibility window. They are never reopened as scientific authority.

## Migration Order

1. Build and deterministically verify Search v2 and compact summaries.
2. Route search reads through Search v2; retain instrumented cursor/compatibility
   fallback only until parity closes.
3. Route singleton summary/detail and preview reads through the compact tables.
4. Materialize and verify all required hierarchy bundles and full scenes.
5. Move Peek, Explorer, and System Page onto the shared contracts.
6. Repeat the M8.3d capacity campaign.
7. Freeze deployment artifacts, document warming and rollback, and retire or
   explicitly bound every remaining fallback.

`scripts/run_public_read_capacity_campaign.py` executes the checked-in campaign
and workload manifests, writes one immutable report directory, applies the
pinned SLO checker to every profile, and runs the configured concurrency
staircase. The runner expects the normal Photon and isolated constrained stacks
to be healthy; it does not change host quotas, deploy, or contact another host.

## Accepted Capacity Gate

The accepted run is `final_20260725_v5`. It ran from 22:16:27 to 22:45:30 UTC
under campaign SHA-256
`d6a1e87a4ee3e38ed8212c7ee75c6e0234e69e7de67ef28ab2114103a67f7ce8`
and workload SHA-256
`6165862d5672ba47e3b5f00b80800bb24399fd179839dad0db5d0ef83b1eff57`.
All 17 profiles and every c1/c2/c4/c6/c8/c12 staircase step passed with zero
errors, timeouts, OOM events, queue-delay failures, or scientific fallbacks.

| Accepted constrained profile | p95 | Throughput | Peak application memory |
| --- | ---: | ---: | ---: |
| Exact identifier/name, c1 | 8.5 ms | 190.1 rps | 1.50 GiB |
| Fuzzy/filtered search, c1 | 73.3 ms | 28.7 rps | 1.50 GiB |
| Summary/hierarchy, c1 | 6.4 ms | 226.7 rps | 1.50 GiB |
| Singleton/prebuilt fast scenes, c1 | 7.1 ms | 205.0 rps | 1.50 GiB |
| Mixed public workload, c12 | 1.647 s | 72.4 rps | 1.55 GiB |
| Public-read endpoints, c12 | 1.990 s | 53.6 rps | 1.56 GiB |
| Prebuilt scenes, c12 | 127.4 ms | 122.5 rps | 1.52 GiB |
| Sustained mixed, c12, 300 s | 522.6 ms | 73.2 rps | 1.54 GiB |
| Static UI/manifests/tiles, c24 | 111.5 ms | 531.6 rps | 1.56 GiB |

Target-bounded file-page eviction provides the cold artifact result. Application
cold scene runs restart only the isolated capacity API container and record that
method. Five different cold complex scenes produced five misses at 8.315 s p95
and 4.33 GiB peak. Twelve same-key cold requests produced exactly one miss and
eleven coalesced responses at 5.037 s p95; the warm repeat produced twelve hits
at 132.7 ms. No global host cache drop occurred.

The staircase passed through c12 at 1.893 s p95 and 66.4 rps, then recovered at
c1 to 36.3 ms and 126.9 rps. The five-minute idle trace remained near 1.5 GiB
with no pressure or growth.

The runtime decision is **conditional go** for the modeled 6-vCPU/12-GiB host.
The exact transfer payload is 33,588,971,005 bytes. After the separately
reviewed remote cleanup from M8.3d, streamed extraction retains
19,480,916,087 bytes of reserve; staging both archive and extracted artifacts
would retain only 2,428,111,363 bytes and is prohibited. At 85% payload
efficiency the transfer is approximately 8 h 47 m at 10 Mbps, 4 h 23 m at
20 Mbps, 1 h 45 m at 50 Mbps, 53 m at 100 Mbps, or 21 m at 250 Mbps.

The machine decision is:

`$SPACEGATE_STATE_DIR/reports/runtime_capacity_gate/public_read_v2/final_20260725_v5/public_read_capacity_gate.json`

Build timings and derived-artifact optimization priorities are in
`docs/PUBLIC_READ_BUILD_PERFORMANCE_2026-07-25.md`.

The July 26 release checkpoint migrates the Vite SPA from React Router v6 to
v7.18.1, outside the open-redirect/XSS range. npm reports the newer high-severity
RSC-action CSRF advisory for v7.18.1; Spacegate ships Declarative Mode with
`BrowserRouter` and no RSC, data routers, loaders, or actions, so the vulnerable
code path is absent. The fully patched v8.3 release is listed upstream but is
not yet available from npm. This bounded mode-based acceptance is explicit,
not represented as a clean audit. Production build and tile tests pass; full
route/query/focus/map handoff desktop/mobile parity runs again against rebuilt
release containers before public activation.
