# Public Read Architecture v2

Status: implementation in progress on `feature/public-read-architecture-v2`

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
- `planets`: public confirmed-planet summary values and lifecycle state;
- `aliases`: scoped source aliases;
- `search_terms`: every accepted public search term with focus identity;
- `exact_identifiers`: all 6,669,279 accepted permanent identifiers with
  namespace, object focus, binding, source, release, and evidence lineage;
- `search_terms_fts`: trigram candidate index; edit distance is computed only over
  a bounded candidate set;
- `identifier_outcomes`: accepted, missing, excluded, ambiguous, deferred, and
  quarantined exact-identifier outcomes, including explicit TIC/TOI semantics;
- `identifier_quarantine`: all 81,043 current reviewed quarantine records;
- `singleton_scene_seeds`: an indexed deterministic view over eligible
  `systems` and `stars`, avoiding a second 5.86-million-row physical copy;
- `hierarchy_bundles`: compressed, versioned nontrivial hierarchy/detail payloads.

The artifact is opened read-only and immutable. Runtime validation requires exact
build and schema identity. Missing artifacts may use an instrumented compatibility
path during the migration window. Corrupt, mismatched, sample, or incompatible
artifacts fail visibly.

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
legacy path can be retired only when:

1. all canonical systems, aliases, accepted terms, and identifier outcomes are
   accounted for;
2. search/filter/sort/object-focus parity passes;
3. all nontrivial hierarchy targets have verified bundles;
4. all full-scene policy targets have verified artifacts;
5. desktop/mobile browser and API goldens pass;
6. constrained cold/warm/burst/sustained/recovery capacity gates pass;
7. rollback to the prior immutable build is tested.

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
