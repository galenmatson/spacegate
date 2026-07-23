# E6 Shadow Build Performance - 2026-07-22

## Scope

This report began with the first accepted Evidence Lake v2 E6 shadow
foundation, `e6_994a6301c335ac385f5dc052_shadow`. Historical checkpoint
measurements remain below. The current measured candidate is
`e6_cfcdf2d9add2cd7e2b96af68_shadow`; its corrected public slice, map tiles,
and downstream verification measurements are appended as they finish. The
production-equivalent browser rows now pass; E6/E7 does not close until the
remaining scientific-delta review, promotion, and rollback rows are complete.

The public-slice builder now emits
`slice_build_performance_report.json` with named core selection,
materialization, checkpoint/vacuum, Parquet export, row-accounting, ARM,
hierarchy, DISC, verification, and atomic-promotion phases. Each phase records
wall and process CPU time, peak RSS, process I/O block deltas, and durable bytes
where the phase creates an artifact. The first E6 public-slice run will establish
the baseline; no build-path optimization is accepted before that measurement.
The map-tile report contract is likewise upgraded to v2 and records setup plus
an independent wall/CPU/RSS/I/O/output-byte row for each 100, 250, 500, and
1,000-ly radius, followed by index publication. This separates repeated source
scans and radius-specific encoding costs before deciding whether shared spatial
intermediates are worth their added complexity.
Simulation-scene materialization now records setup/module loading, system
selection, scene generation or reuse, and runtime-cache pruning independently,
with generated/reused/failed counts and output bytes attached to the generation
phase. This makes cache warmness explicit instead of comparing a reused run
against a cold generation run as if they were equivalent.
Phase `peak_rss_kib` is the process high-water mark observed at that boundary,
not an independently sampled per-phase maximum; process I/O values are true
phase deltas from `getrusage`. CPU rows measure the instrumented Python process
and its DuckDB worker threads. They exclude the child process used by the
1.06-second public-slice verification phase; that row's wall time is
authoritative and its displayed CPU time is intentionally not interpreted.

The E6/E7 acceptance report must retain a per-step breakdown for selected-fact
consumers, DISC, public slice, map tiles, simulation scenes, API/search and UX
verification, promotion, and rollback. Each row must include wall and CPU time,
peak memory, measured I/O where the runner exposes it, and output bytes. Slow
steps will be profiled and optimized only from this evidence; accepted changes
must include a before/after comparison.

Photon profile:

- 12 DuckDB threads
- 48 GB DuckDB memory limit
- `/mnt/space/spacegate/e6-shadow-spill` for bounded external spill
- stability reference `20260717T0614Z_f452835_side`
- selected facts `0a57f778ce13de1c2c800103`

## Foundation Result

- wall time: 128.0 seconds
- CPU time: 745.6 seconds
- peak RSS: 35.1 GiB
- external spill: none
- durable shadow size: approximately 16 GiB
- independent audit: 35.7 wall seconds, 7.6 GiB peak RSS
- isolated compile, audit, and logical-hash reproduction: 247.7 wall seconds

The output preserves 5,869,091 systems, 5,874,636 stars, 6,311 planets, and the
complete canonical hierarchy. The independent audit and clean reproduction
pass. CORE and ARM DuckDB files are not byte-identical across clean runs because
physical block layout is runtime-dependent; fifteen generated or mutated table
multisets match order-independent cryptographic logical hashes. Copied evidence
tables remain covered by immutable source database hashes and exact row counts.

## Phase Timings

| Phase | Wall s | CPU s | Observation |
|---|---:|---:|---|
| Verify seven E5 artifacts | 20.68 | 20.68 | Reads pinned database hashes |
| Verify stability products | 4.67 | 4.66 | Hashes four reference databases |
| Copy stability products | 2.58 | 2.58 | Reflink/copy behavior is filesystem-dependent |
| Selected quantity contract | 0.15 | 1.63 | Accounts 69 stellar and 16 planet quantities |
| Copy E5 evidence projections | 24.08 | 40.01 | Largest wall-time phase |
| Stellar astrometry projection | 20.99 | 208.61 | Largest CPU phase |
| Stellar physics projection | 11.19 | 108.23 | 4.66 million projected rows |
| Stellar photometry projection | 6.77 | 66.01 | 5.86 million projected rows |
| Stellar variability projection | 13.51 | 146.77 | Includes typed Boolean membership values |
| Stellar classification projection | 1.53 | 13.37 | 321,737 projected rows |
| Planet projection | 0.13 | 0.69 | 6,296 selected planets |
| Apply selected CORE values | 10.39 | 109.37 | Eighteen explicit scalar mappings |
| Official aliases | 0.39 | 3.69 | Adds 65 non-primary WGSN aliases |
| System temperature aggregates | 0.61 | 5.80 | Refresh after temperature selection |
| Metadata updates | 0.18 | 0.35 | CORE, ARM, and DISC total |
| Hash shadow products | 8.45 | 8.45 | Physical hashes retained as integrity metadata |

Phase wall times total less than process wall time because connection setup,
hierarchy metadata, inventory checks, checkpoints, and manifest promotion are
currently outside named timers. Add timers for those gaps when downstream
phases are integrated.

## Optimization Assessment

No E6 foundation phase currently justifies a more complex architecture. The
five domain-separated stellar aggregations complete in 54 seconds total and
avoid a very wide, high-memory single aggregation. The current design preserves
typed domain boundaries and stays below the memory limit without spill.

Potential improvements, in priority order:

1. Instrument downstream DISC, public-slice, tile, scene, API/search, and UX
   verification before changing the foundation compiler.
2. Add explicit timers around DuckDB connection/checkpoint work and manifest
   promotion so phase totals reconcile with process wall time.
3. Consider reflink-aware product copies only if the target filesystem and
   immutable-promotion contract can prove isolation.
4. Cache no input hash across invocations unless an integrity-preserving,
   fail-closed immutable-file attestation is implemented. The current 25-second
   verification cost is acceptable.
5. Run full logical hashing at stable checkpoints, not every development build;
   it adds roughly 85 seconds to reproduction and tests scientific identity
   more accurately than DuckDB container-byte equality.

The dominant end-to-end compiler cost remains E5, especially Gaia direct fact
materialization, deterministic exports, Bailer-Jones projection, immutable
input verification, and global selection. E6 optimization should not distract
from those measured E5 targets unless downstream regeneration changes this
ranking.

## Integrated Selected-Consumer Candidate

Policy `2026-07-22.e6-shadow.3` and compiler v2 move the shared selected-star
consumer and complete hierarchy-leaf classification into the immutable build
boundary. The earlier v2 output was useful for scientific diagnosis, but its
post-build materialization made its recorded product hashes stale; it is not a
promotion or reproduction candidate.

Integrated candidate `e6_2da376053461c8220bee06ad_shadow` completes in 166.11
wall seconds and 1,062.95 CPU-seconds with 34.15 GiB peak RSS, no swap, and no
external spill. It writes approximately 17.8 GiB at the filesystem accounting
layer. Its independent integrity, inventory, hierarchy, selected-value,
consumer-lineage, leaf-lineage, and lifecycle audit passes in 36.78 wall
seconds with 8.19 GiB peak RSS.

The added immutable phases are:

| Phase | Wall s | CPU s | Observation |
|---|---:|---:|---|
| Shared selected consumers | 9.61 | 167.69 | 5,874,636 parameter/classification subjects |
| Classification candidates | 6.86 | 129.08 | Dominant consumer subphase |
| Classification indexes | 1.43 | 11.48 | Stable star/system lookup indexes |
| Consumer verification | 0.74 | 17.77 | Inventory, class, and lineage gates |
| Stellar hierarchy leaves | 24.01 | 149.63 | 5,879,796 exact terminal leaves |
| Final product hashing | 9.38 | 9.38 | Four immutable product files |

The integrated compile is only 38.1 seconds slower than the original
foundation while adding both downstream projections and their internal gates.
The A/B auditor takes 7.92 seconds and reports 338,858 classification changes,
930 unknown-to-known transitions, zero known-to-unknown transitions, and exact
source attribution for every residual legacy parameter tail.

Clean isolated compile, expanded independent audit, and logical reproduction
complete in 311.37 process wall seconds (309.72 seconds measured inside the
runner), peaking at 34.09 GiB RSS. All eighteen generated or mutated table
multisets match cryptographic logical hashes, including the selected consumer
supplement, canonical-star display classification, and terminal-leaf
classification. DuckDB container bytes again differ, as expected; build
identity, scientific content, inventories, compiler reports, and product sets
all match. The reproduction work tree on `/mnt/space` is removed on completion.

## Selected Consumer Checkpoint

The first full selected-consumer run on USB scratch established the shared
stellar-parameter compatibility view and centralized display classification.
A warm-cache rerun took 8.56 wall seconds. Classification materialization took
5.93 seconds, indexes 1.40, verification 0.78, and the 8,041-row non-astrometric
subject supplement 0.37. Peak RSS was 10.72 GiB. The supplement avoids either
dropping non-Gaia subjects or duplicating the complete 5.87-million-row identity
spine.

The hierarchy-leaf projection then took 27.38 wall seconds, 137.64 CPU-seconds,
and 18.75 GiB peak RSS. Its central classification table occupies approximately
325.5 MiB of DuckDB data blocks and the complete leaf table approximately 620.0
MiB, excluding indexes and reusable/free database blocks. It projects 5,879,796
leaves with zero duplicate keys or invalid rows.

The scientific A/B changes 338,820 classifications, fills 929 prior unknowns,
and loses zero prior known classifications. Exact release-scoped MSC evidence
now supplies 5,683 component spectral classifications and 8,314 component-mass
priors with evidence IDs. The large remaining delta is chiefly the intentional
replacement of legacy Gaia temperature/color-generated class letters by the
versioned selected-temperature and selected-color policy; it remains an E6
scientific-review item.

The machine A/B auditor itself takes 4.40 wall seconds and 3.22 GiB peak RSS.
It exposed a policy blocker rather than a performance blocker: the selected
projection adds millions of physical values but loses 62 legacy temperatures,
686 masses, 193 radii, and 195 luminosities. All but one of those losses are
NASA Exoplanet Archive host-star values already preserved in E4; E5 currently
selects NASA planet quantities but omitted a host-star selection program. The
1,160 lost distances were initially described as inverse-parallax values. The
source audit corrects that: they are Gaia DR3 GSP-Phot posterior model distances
preserved in E4. Policy v14 selects them as distinct source-model facts after
Bailer-Jones posteriors rather than silently deriving reciprocal parallax.
Neither tail is hidden by the compatibility view.

## E5 Clean Reproduction and Distance Preflight

The isolated USB-scratch reproduction of E5 candidate
`16708b8ed193aeae9b2ab995` completes in 24:49.54, runs all 103 phases, matches
logical hash `d7e38431f403844a4a0736201a61200a2ab95070b9192c0b24be83cfd6f01208`
with no differing report sections, and removes scratch. Warm-phase costs remain
concentrated in Gaia candidate insertion (488.79 seconds), immutable input
verification (159.15), selected-fact and decision export (175.64), artifact
hashing (113.22), global selection (92.01), and Bailer-Jones processing
(166.27 combined).

The GSP-Phot preflight takes 4.84 seconds of measured query phases. It accounts
6,955,056 unique valid evidence rows, 1,982,472 unique accepted bindings, and
recovers all 1,160 legacy missing distances. The tail's parallax S/N is
2.07-7.39, so reciprocal parallax is not an acceptable optimization or fallback.

## NASA Host Policy Preflight

Before paying for another full E5 compile, the star-scoped NASA host program is
compiled alone by `scripts/verify_e5_nasa_host_selection.py`. The hash-verified
run reads and attests the 4.50-GB E4 NASA shard, resolves host identities, inserts
candidates, performs coherent parameter-set selection, and audits lineage and
scope and compares the resulting authority decisions with accepted policy v12
in 7.60 wall seconds and 20.38 CPU-seconds with 3.80-GiB peak RSS. Input hashing
accounts for 3.62 seconds; reference authority-impact composition adds 1.19
seconds.

The preflight accounts for 132,578 eligible host records as 27,945 accepted,
104,628 missing, and five ambiguous. It selects 12,210 facts for 4,308 coherent
host parameter sets: 2,124 temperatures, 2,013 masses, 1,895 surface gravities,
1,822 radii, 1,636 metallicities, 929 ages, 896 densities, and 895 log solar
luminosities. Exactly 10,830 facts come from NASA default reference-specific
rows and 1,380 from the reference-specific stellar-host table. Cross-object
facts, duplicate selected quantities, missing binding lineage, and missing
evidence lineage are all zero.

The authority-impact gate reports 99 atmosphere and 248 fundamental fills,
195 atmosphere and 1,814 fundamental replacements, 1,952 reference wins, and
zero authority ties. It predicts exactly 6,320 displaced primary Gaia AP facts
and 415 displaced supplementary AP facts before the global compiler runs.

This preflight is now the required fast gate for NASA host-policy changes. It
does not replace the complete E5 compile because the global compiler must still
prove competition against all other stellar authorities and deterministic
partition exports. Its machine report is
`state/reports/evidence_lake_v2/e5_nasa_host_selection_verification.json`.

## Corrected E6 v6 Integrated Checkpoint

Policy `2026-07-22.e6-shadow.6` compiles selected-fact v15 and component-scope
v9 into unserved shadow `e6_cfcdf2d9add2cd7e2b96af68_shadow`. The component
policy preserves case-significant MSC identities and removes 238 `AB`/`Ab`
class collision groups without a named-system rule. The 3:29.60 compile peaks
at 36.37 GiB RSS, uses no swap or external spill, and records 21 named phases.
Independent audit passes all 194 checks in 42.37 seconds. Clean isolated
compile, audit, and logical reproduction completes in 5:49.34, with all product
and report comparisons matching.

| Shadow phase | Wall s | CPU s | Peak RSS GiB | Observation |
|---|---:|---:|---:|---|
| Coolness rescore | 30.45 | 100.95 | 36.37 | Complete 5.87-million-system DISC rescore |
| Verify seven E5 artifacts | 29.40 | 23.04 | 0.07 | Immutable input-byte attestation |
| Stellar hierarchy leaves | 26.56 | 154.47 | 36.37 | 5,879,796 exact terminal leaves |
| Copy E5 evidence projections | 25.31 | 38.09 | 3.74 | Domain evidence copied into ARM |
| Stellar astrometry projection | 22.66 | 216.75 | 25.62 | 5,866,595 projected subjects |
| Stellar physics projection | 14.05 | 129.04 | 36.37 | 4,664,686 projected subjects |
| Stellar variability projection | 13.78 | 146.96 | 36.37 | Source-native selected facts |
| Apply selected CORE facts | 9.76 | 101.74 | 36.37 | Explicit scalar mappings only |
| Shared selected consumers | 9.61 | 168.18 | 36.37 | Central parameter/classification contract |
| Hash final products | 9.49 | 9.49 | 36.37 | Four immutable build products |

The corrected public slice contains all 5,869,091 systems: the radius policy
trims zero rows. It takes 4:43.42 externally and 283.21 seconds in measured
phases, peaks at 32.27 GiB RSS, and writes approximately 19.9 GiB of databases
and Parquet. Integrity verification reports zero missing, dangling, duplicate,
or mis-scoped rows.

| Public-slice phase | Wall s | CPU s | Peak RSS GiB | Share of measured wall |
|---|---:|---:|---:|---:|
| ARM slice | 194.89 | 797.33 | 32.27 | 68.8% |
| CORE materialization | 52.43 | 120.49 | 10.91 | 18.5% |
| Canonical hierarchy slice | 17.63 | 81.13 | 32.27 | 6.2% |
| DISC slice | 8.52 | 49.81 | 32.27 | 3.0% |
| CORE Parquet export | 7.57 | 121.41 | 25.63 | 2.7% |
| Other measured phases | 2.15 | 10.25 | 32.27 | 0.8% |

Map generation takes 4:38.38, peaks at 12.28 GiB RSS, and produces 481.7 MB
of compressed content-addressed artifacts. Every exact radius passes coverage,
checksum, display-name, representative, and badge verification with zero
missing or extra systems.

| Radius | Systems | Wall s | CPU s | Output MB |
|---:|---:|---:|---:|---:|
| 100 ly | 10,240 | 5.91 | 152.12 | 0.93 |
| 250 ly | 230,183 | 12.62 | 160.37 | 13.44 |
| 500 ly | 2,332,007 | 73.84 | 226.53 | 130.37 |
| 1,000 ly | 5,869,091 | 185.48 | 364.16 | 336.91 |

An isolated USB-backed rebuild with identical 100/250/500/1,000-ly publication
inputs completes in 4:38.14 and matches all four manifest SHA-256 values
exactly. The machine comparison passes and its 461-MiB scratch tree is removed.
An initial diagnostic used a 100/250-only publication flag: all tile bytes still
matched, while the expected `public_enabled` field changed the 500/1,000
manifest hashes. That parameter-mismatch report is retained separately and is
not treated as a determinism failure or as the accepted reproduction.

The cold 1,000-system simulation-scene cache takes 22:31.97 externally and
1,351.69 seconds in instrumented phases, peaks at 2.72 GiB RSS, and consumes
18,377.97 CPU-seconds (13.6 cores on average). All 1,000 scenes generate with
zero failures or incompatible artifacts. The compressed output is only
10,657,542 bytes, confirming that scene assembly rather than storage or gzip is
the bottleneck.

| Scene-cache phase | Wall s | CPU s | Result |
|---|---:|---:|---|
| Module/setup | 0.52 | 2.54 | API scene builder loaded |
| Select priority systems | 0.71 | 13.59 | 1,000 systems |
| Cold scene materialization | 1,350.46 | 18,361.83 | 1,000 generated, zero failed |
| Complete warm rerun | 1.65 | 16.65 | 1,000 reused, zero generated |

The externally observed warm run is 1.86 seconds. Cold and warm machine reports
are retained separately as `e6_shadow_v6_simulation_scenes_cold.json` and
`e6_shadow_v6_simulation_scenes_warm.json`; the ordinary report path may be
overwritten by later cache operations and is not the historical record.

Focused compiler/scope/coolness/public/tile/scene/API-consumer tests pass 29/29
in 2.26 seconds. Direct alias materialization verification passes 1,026,545
aliases, 775 proper names, and 5,179 expanded Bayer terms in 0.8 seconds. An
unpromoted local API process capped at eight DuckDB threads and 8 GB passes the
complete integration contract in 40.42 seconds and the strict twelve-system
search/detail/hierarchy/simulation benchmark in 37.39 seconds. The strict set
includes Castor, Nu Scorpii, Alpha Centauri, Sirius, Sol, TRAPPIST-1, and source
identifier search; it emits no stale-slice or preview warnings.

| Verification step | Wall s | Result |
|---|---:|---|
| Focused pytest suite | 2.26 | 29 passed |
| Alias/search materialization | 0.80 | Pass |
| API integration contract | 40.42 | Pass |
| Strict known-system API benchmark | 37.39 | Pass, no warnings |

## Production Browser Acceptance

The unpromoted public candidate passes the complete tiled-map Playwright suite
through a production Vite build served behind the same-origin nginx topology
used by Spacegate. Twelve desktop/mobile/4K cases pass, with four intentional
mobile skips. Coverage includes exact 100/250-ly loading, progressive
500/1,000-ly loading, canvas-pixel and screenshot checks, search handoff,
flight, exact-density behavior, and the 4K Bright style. The sole initial test
failure was a stale golden expecting label strategy v2; the candidate correctly
advertises the shared hierarchy-leaf v3 policy, and the updated golden passes.

The production performance matrix passes all 312 fixed acceptance checks. No
budget was raised. The checks cover eligible-system counts, rendered-point
bounds, tile completion and duplication, network failures and bytes, usable
and settle time, median/p95 frame time, long tasks, post-GC JavaScript heap,
search/selection latency, camera detail, and device density policy.

| Scenario/profile | Heap MB | Settle s | p95 frame ms | Requests | Encoded MB | Points |
|---|---:|---:|---:|---:|---:|---:|
| 500 cold / desktop | 116 | 2.61 | 33.4 | 110 | 9.84 | 22,176 |
| 500 cold / mobile | 86 | 1.91 | 16.8 | 106 | 6.78 | 19,951 |
| 500 cold / Photon | 103 | 2.97 | 50.0 | 110 | 9.89 | 22,176 |
| 1,000 cold / desktop | 342 | 10.92 | 50.1 | 512 | 18.79 | 100,158 |
| 1,000 cold / mobile | 254 | 6.71 | 50.1 | 508 | 15.68 | 97,933 |
| 1,000 cold / Photon | 269 | 13.11 | 66.7 | 512 | 18.79 | 100,158 |
| 1,000 warm / desktop | 225 | 10.43 | 50.1 | 512 | 0.31 | 100,158 |
| 1,000 warm / mobile | 462 | 5.81 | 50.1 | 508 | 0.31 | 97,933 |
| 1,000 warm / Photon | 254 | 13.09 | 66.7 | 512 | 0.31 | 100,158 |
| 1,000 rapid / desktop | 286 | 11.08 | 66.8 | 512 | 18.79 | 100,158 |
| 1,000 rapid / mobile | 239 | 6.27 | 50.1 | 508 | 15.68 | 97,933 |
| 1,000 rapid / Photon | 304 | 13.37 | 66.7 | 512 | 18.79 | 100,158 |

An earlier Vite development-server run missed two heap budgets: 364 MB against
the 320-MB 500-ly desktop limit and 747 MB against the 650-MB 1,000-ly warm
desktop limit. Equivalent production assets reduced those measurements to 116
MB and 225 MB. The development reports remain retained as diagnostics, but
development-runtime heap is not used to weaken or redefine the production
acceptance budget. Machine evidence is retained in
`e6_shadow_v6_deep_map_production_performance_acceptance.json` and the four
`e6-v6-prod-*` benchmark directories.

## Ranked Optimization Program

No optimization is accepted without producing equivalent scientific hashes,
coverage/accounting reports, and a before/after timing row. The measured order
of work is:

1. **E5 program-level intermediates.** The approximately 30-minute compiler is
   still the dominant build step. Content-addressed source/program outputs,
   reusable release-scoped identity outcomes, and a Parquet-first direct-scalar
   lane can prevent a one-field policy change from recompiling every Gaia fact.
   Separate the source-disposition closure ledger from the selected-scalar
   artifact identity: replacing a component-only projection should update the
   E6 composition manifest without forcing an unrelated 123-million-fact
   scalar compile. The current v15 ledger therefore remains an immutable input
   naming component v8 while E6 v6 explicitly composes its audited v9 successor.
   Previously tested binding-cache and partition-export shortcuts remain
   rejected because they were slower or nondeterministic.
2. **Public-slice identity fast path.** This build retains every source system,
   yet spends 194.89 seconds rebuilding ARM and 52.43 seconds rebuilding CORE.
   Add a fail-closed identity proof and immutable artifact-copy/reuse path, then
   patch only build metadata and independently compare logical hashes and row
   accounting. Hardlinks are prohibited because metadata mutation would alter
   the shadow; `/data` does not support reflinks, so physical copy cost remains.
3. **Map name projection and bounded encoding parallelism.** The 1,000-ly phase
   is 66.7% of tile wall time and becomes mostly serial after its initial
   parallel query. Precompute the public display-name projection, eliminate
   per-tile alias queries, and use a bounded deterministic worker pool for
   encoding, gzip, and writes. Scan the 1,000-ly population once and reuse fully
   interior content-addressed tiles across radii; radius-boundary tiles remain
   distinct.
4. **Preserve verification, profile moderate E6 phases.** E6 input attestation,
   coolness, and leaf materialization each cost 26-30 seconds but are not large
   enough to justify weakening integrity or domain separation. Optimize them
   only after the first three items and only with fail-closed attestations.
5. **Remove broad scene prewarming from the critical build path.** Cold scene
   generation costs 22.5 minutes while warm validation costs 1.86 seconds and
   the complete cache is only 10.7 MB. Keep bounded representative scene
   goldens in build acceptance, then use the existing admin/runtime-cache action
   to populate popular scenes incrementally after local promotion. Give that
   admin materializer a persistent batch query context or reusable scene-input
   projection so it does not reconstruct CORE/ARM/DISC API state 1,000 times.
   Do not simply add concurrent scene workers: the current sequential outer
   loop already averages 13.6 CPU cores through DuckDB, so unbounded workers
   would increase contention without addressing repeated assembly.

## Compact Identity Integration

E5 compact build `f0d7273f65371efeda365611` compiles in 1.74 seconds with a
454-MiB peak RSS and reproduces the same build ID and ordered Parquet hashes on
a second run. Its independent 34-check verifier takes 0.07 seconds. The broader
scope audit takes 6.31 seconds and peaks at 8.26 GiB because it traverses the
current canonical identity graph.

E6 v7 shadow `e6_95e7af54d69f3d9602d81e5b_shadow` integrates six compact
projection tables in 3:35.37, compared with 3:29.60 for v6. Peak RSS is 36.43
GiB with no swap or external spill. Named phases account for 213.16 seconds.
The +5.41-second named-phase delta is localized to immutable input verification
(+3.86 seconds) and evidence projection copying (+1.26 seconds); all scientific
wide projections, selected consumers, hierarchy leaves, and coolness rescoring
remain within 0.28 seconds of v6. The strengthened independent E6 audit passes
in 47.89 seconds and now verifies actual row counts for every copied projection
table, not only registry hashes.

Clean isolated E6 v7 compile/audit/logical-hash reproduction passes in 5:51.12,
only 1.78 seconds slower than v6, with a 36.53-GiB peak and no swap. The v7
public slice passes in 4:37.57, 5.85 seconds faster than v6, with exact inventory
parity. Four-radius tiles build in 4:38.87 and verify in 17.29 seconds. The
1,000-ly phase consumes 185.83 seconds, 66.7% of tile wall time, confirming it
as the bounded-encoding optimization target.

The critical-path scene gate now uses 24 deterministic high-priority systems:
cold generation takes 34.42 seconds with zero failures and warm reuse takes
1.14 seconds. Broad 1,000-scene population remains available through the Admin
materializer after promotion. Alias verification takes 0.74 seconds, complete
API integration 41.11 seconds, and the strict twelve-system benchmark 39.68
seconds. Parsed v7 tile `counts` and complete `tiles` arrays equal v6 at every
radius, so the accepted v6 production-browser screenshots and 312 performance
checks remain directly applicable; no frontend or renderer code changed.

## Permanent Identity Seed

E7 identity seed `5c878083872c738415971864` exports the permanent hierarchy
contract without copying any scientific scalar from the stability databases.
It contains 11,759,440 hierarchy nodes and 5,886,947 relationships in 402.2 MiB
of compressed Parquet. The production compile takes 30.48 wall seconds, 53.26
CPU-seconds, peaks at 5.42 GiB RSS, and writes 402.2 MiB. An isolated USB-backed
reproduction takes 30.63 seconds, matches both Parquet hashes and byte sizes,
and removes its scratch tree.

## E7 Build-Time Closeout Requirement

The final E7 checkpoint will publish one machine-readable and human-readable
critical-path report rather than only a total build duration. It must retain
wall and CPU time, process peak RSS, durable input/output bytes, cache state,
and named phase timings for each authoritative compiler, verifier, shadow
build, local promotion, rollback, and re-promotion step. Accepted and rejected
performance changes must include comparable before/after measurements and may
not weaken content hashing, scientific accounting, or deterministic output.

The first clean system-placement baseline is build
`9ccc087defca7aebc5b77d6a`: 103.10 wall seconds, 297.89 CPU-seconds, 26.16 GiB
peak RSS, and 1.48 GiB of durable Parquet. Its measured wall-time leaders are
winner selection (36.46 seconds), selected-star extraction (26.18 seconds),
immutable-input attestation (22.72 seconds), and deterministic Parquet export
(12.89 seconds). These four phases are the first E7 optimization targets; the
baseline artifact remains retained for exact scientific and performance
comparison.

The optimized compiler reads the parent compiler's deterministic
per-quantity Parquet products, attests each product against that parent
manifest, and evaluates precedence through reusable anti-join views rather
than writing or sorting a full intermediate winner table. Production build
provisional v3 build `4ec5b0e7f9f0aca4470cbe11` took 61.08 wall seconds and
peaked at 17.19 GiB RSS with byte-identical products. Final lineage review then
found the SBX fallback rows carried a provisional release label and uniform
J2016 epoch. V4 build `22e9a59dd02484454a629df7` joins the registered SBX
release and preserves the source position epoch. It takes 63.24 seconds and
peaks at 17.42 GiB: a 38.7% wall-time reduction and 33.4% peak-memory reduction
from baseline.

An intermediate materialized-winner experiment took 84.91 seconds at 18.95
GiB and was rejected. The isolated accepted reproduction plus independent
audit takes 71.18 seconds, matches the build identity, policy/compiler/input
attestations, source counts, verification, byte sizes, and both product hashes,
then removes scratch. Shared-host filesystem cache state was warm for the final
and reproduction runs and is recorded as such; the eventual E7 aggregate must
not present these as cold-cache timings.

The v3-to-v4 scientific A/B reports zero changes to geometry, representative
objects, winner sources, evidence IDs, or derivation JSON. Eight of the ten SBX
fallbacks change epoch metadata from J2016 to their actual J1991.25 or J2000
position epoch; the other two were already J2016. All ten replace the invented
`sbx_v2026_07_21` label with registered release
`sbx_tap_full_rolling_snapshot_v1`.

The E7 stability-table accounting pass verifies four cached database checksums
and inventories 74 table schemas/counts in 4.76 seconds at 83.5 MiB peak RSS.
Permanent identity vocabulary compilation takes 5.24 seconds at 2.06 GiB peak
RSS and writes 25.7 MiB; isolated compile plus audit takes 5.66 seconds. These
steps remain explicit rows in the final E7 critical path even though neither is
a material runtime bottleneck.

Clean identity/search foundation build `9c2d08086275ead386f71bf7` takes 68.23
wall seconds, 17.27 GiB peak RSS, and writes about 7.8 GiB across canonical
Parquet plus regenerable DuckDB query databases. The largest phases are index
construction (14.9 seconds), Parquet export (12.5), search materialization
(9.9), and hierarchy materialization (9.8). Isolated compile plus verification
takes 74.63 seconds. Preserving insertion order adds roughly 6-8 seconds but
makes all eight canonical Parquet outputs byte-identical; this is accepted.
DuckDB container byte hashes remain intentionally non-gating because internal
page layout is not a stable serialization, while independent logical-table
verification remains mandatory. This step is not responsible for the
hour-scale end-to-end build.

Clean selected-science build `35eb29fa3b2a3ac518f5303a` takes 190.81 seconds
and peaks at 37.45 GiB RSS with no swap. Its largest named phases are canonical
Parquet export (37.65 seconds), product hashing (32.77), immutable selected-
artifact verification (23.5), stellar astrometry pivot (about 20), domain copy
(13.6), stellar physics pivot (13.3), and variability pivot (12.1). An isolated
shared-cache rebuild takes 165.23 seconds; its product hash falls to 9.35
seconds while input attestation rises to 34.49 seconds. The net difference is
cache/I/O state, not an accepted code optimization. All canonical Parquet
hashes reproduce, the query database passes logical verification, and scratch
is removed.

Targeted WISE source refresh is now measured separately from the normal build.
The cold four-worker acquisition of 1,000 exact IRSA responses took 11:40.11,
peaked at 7.15 GiB RSS, and used 126.8 CPU-seconds. CatWISE took 383.4 seconds;
AllWISE took 310.9 seconds. A failed predecessor pass took 6:10.52 and exposed
seven IRSA density-limit responses plus executor failure handling that waited
for scheduled work. The accepted collector preserves those source errors,
uses deterministic 10/3-arcsec fallbacks, records all member failures, and
resumes completed responses.

The first warm verification still recomputed the clean target universe: 5.77
wall seconds, 102.9 CPU-seconds, and 7.06 GiB peak RSS. Reusing the pinned
target-set artifact when policy and clean input manifests match reduces that to
0.32 seconds, 2.41 CPU-seconds, and 91 MiB peak RSS, an 18x wall, 43x CPU, and
79x memory improvement without weakening input attestation. Explicit
`--rebuild-target-set` retains the full audit path.

Raw snapshot materialization takes 0.30 seconds; source-native typed cooking
takes 9.67 seconds at 221 MiB RSS; clean raw-to-typed reproduction takes 9.54
seconds. Clean WISE build `ec8e218402c3a4a3b55b2811` takes 3.10 seconds at
628 MiB RSS, and isolated compile plus independent verification takes 3.28
seconds with byte-identical Parquet. Consequently WISE network acquisition is
a release-refresh stage, while normal builds consume its immutable snapshot in
seconds.

Permanent extended-object identity seed `555fa1890943b97dd0e4ef3d` takes 2.31
wall seconds, 2.36 CPU-seconds, and 116 MiB peak RSS to write five canonical
Parquet products. Its isolated reproduction plus independent audit takes 2.41
seconds and 121 MiB, with exact hashes and scratch removal. This migration-only
step is retained in the final critical path but is not a meaningful bottleneck.

The first clean extended-object compiler used row-wise DuckDB insertion for
18,110 normalized geometry candidates. It took 36.77 wall seconds, 28.09
CPU-seconds, 352 MiB peak RSS, and caused about 188,000 voluntary context
switches. That artifact (`c7c3a96578e8341ec83d6b05`) is rejected and retained
only as comparison evidence. One Arrow batch produces byte-identical geometry
candidate and selected-geometry Parquet in 9.71 seconds, 9.86 CPU-seconds, and
361 MiB for accepted build `a4b521d1e1de52e14afac0da`: 73.6% lower wall time
and 64.9% lower CPU. Isolated compile plus independent audit takes 9.72 seconds.

The supplementary Cantat-Gaudin E4 compiler processes 236,317 source rows,
57 fields, 2,017 cluster contexts, and 234,128 membership claims in 21.32 wall
seconds and 26.27 CPU-seconds with 0.96 GiB peak RSS. The immutable DuckDB is
358.5 MiB. A clean reproduction takes 21.34 wall seconds, 26.59 CPU-seconds,
0.95 GiB peak RSS, and matches the logical hash. Both runs use `/mnt/space` for
artifact or scratch storage because `/data` is below its acquisition floor.

Clean multi-release cluster selection takes 9.43 wall seconds, 15.56
CPU-seconds, and 2.78 GiB peak RSS for 2,482 cluster outcomes, 2,933 context
rows, and 285,145 membership outcomes. Named phases are 1.23 seconds for
Hunt/Reffert, 3.07 seconds for Cantat-Gaudin, and 1.95 seconds for Parquet
export. Clean reproduction takes 9.43 seconds and matches all four products.

Clean extended-object v2 build `95f5f1ff8f2ddee405b39104` integrates those
selected cluster contexts in 9.91 wall seconds, 10.10 CPU-seconds, and 374 MiB
peak RSS. It writes 32.8 MiB of durable artifacts. Input geometry normalization
and candidate batching consume 8.88 seconds (89.7% of wall time); selection and
materialization take 0.71 seconds, Parquet export 0.24 seconds, and product
hashing 0.03 seconds. Exact reproduction takes about 9.9 seconds. The Python/Astropy
normalization phase is the local optimization target, but the end-to-end E7
report must rank hour-scale stages before further work is justified here.

Clean extended-object v3 build `c203e4f451890660ec02086a` adds permanent-HD
endpoint resolution and selected-system placement joins. It takes 10.75 wall
seconds, 11.56 CPU-seconds, and 942 MiB peak RSS, writing 34.6 MiB. Input hashing
adds 0.51 seconds, geometry/claim batching 8.88 seconds, selection 1.04 seconds,
export 0.24 seconds, and hashing 0.03 seconds. Exact reproduction takes 10.83
seconds. The join raises peak RSS because it scans the permanent vocabulary and
placement Parquet, but adds under one second to wall time; it is not a full-build
critical-path target. Two failed attempts are recorded separately: one SQL parse
failure at 10.00 seconds and one invariant failure at 10.95 seconds. Both removed
their incomplete staging data.

## E7 Timed Pipeline Checkpoint

The E7 timing harness is checked in as
`config/evidence_lake/e7_timed_pipeline.json` and
`scripts/run_e7_timed_pipeline.py`. It treats compiler reuse and verifier work
as separate rows, validates every reported build ID against the accepted pin,
and preserves per-stage GNU-time, stdout, and stderr logs under
`/mnt/space/spacegate/e7-build-runs`. Atomic machine summaries live under
`/data/spacegate/state/reports/evidence_lake_v2/e7_build_runs`.

The first verification-only pass after implementing the runner is a
storage-reading baseline, not a controlled cold-cache benchmark:

| Stage | Wall s | CPU s | Peak RSS | Filesystem input blocks |
|---|---:|---:|---:|---:|
| Clean science verification | 29.95 | 23.03 | 571 MiB | 29,014,128 |
| Clean foundation verification | 14.76 | 40.25 | 2,201 MiB | 13,558,440 |
| Clean clusters verification | 0.34 | 2.42 | 158 MiB | 0 |
| Clean extended-object verification | 0.16 | 0.21 | 90 MiB | 0 |
| Clean WISE verification | 0.12 | 0.14 | 77 MiB | 0 |
| Completion preflight and closeout | 0.10 | 0.07 | 23 MiB | 0 |
| **Measured total** | **45.42** | **66.12** | **2,201 MiB max** | **42,572,568** |

An immediate hot-cache pass took 16.54 seconds and reported zero filesystem
input blocks. Clean science took 10.03 seconds and clean foundation 5.79; all
other stages remained below 0.34 seconds. The hot pass is 28.9 seconds faster,
but no optimization is claimed because the commands and scientific artifacts
are identical and only cache state changed.

The accepted clean artifact products represented by this verification total
approximately 25.2 GiB before filesystem allocation overhead. The runner does
not yet close the end-to-end gate: full compiler execution, the clean runtime
composer, shadow/public products, local promotion, container restart and smoke,
rollback, and re-promotion still require measured rows. Existing measurements
continue to rank E5 selected facts (about 24.8 minutes), clean selected science
(190.81 seconds), clean foundation (68.23), and selected system placement
(63.24) ahead of the small clean-domain compilers. Optimization work should
therefore begin with E5 reusable intermediates and redundant immutable export
and hashing passes, subject to unchanged logical hashes and coverage.

## E7 Clean ARM Phase Report

Clean ARM v3 build `34069ba67abe3b4331c26adc` records each compiler phase in
its immutable manifest. It completes in 151.40 wall seconds, produces a
13,707,784,192-byte DuckDB, and peaks at 45.1 GiB RSS. Independent verification
takes 8.13 seconds. Isolated compile, logical-table comparison, independent
audit, and scratch removal take 174.13 seconds total; the reproduced compile is
150.08 seconds and all logical signatures match.

| Phase group | Wall s | Share of compile |
|---|---:|---:|
| Seven multi-million-row selected-science copies | 95.73 | 63.2% |
| Runtime index construction | 17.60 | 11.6% |
| CORE/science/hierarchy product checksum verification | 10.30 | 6.8% |
| Component graph and leaf-classification materialization | 15.38 | 10.2% |
| Final 13.7-GB database hashing | 6.93 | 4.6% |
| Stellar-orbit evidence copies and runtime projection | 0.86 | 0.6% |
| Remaining setup, Solar, WISE, verification, checkpoint, and small tables | 4.60 | 3.0% |

The same compiler before stellar-orbit integration took 148.73 seconds. The
2.67-second difference is within normal table-copy/index variation; the new
orbit work measures below one second and is not a regression.

Optimization order:

1. Split immutable selected-science storage from the runtime ARM projection so
   a new ARM graph build does not rewrite roughly 11 GiB of unchanged selected
   facts. The runtime can attach a manifest-pinned read-only science artifact,
   while deployment packages both files atomically. This is the highest-value
   change but requires explicit multi-artifact runtime and rollback contracts.
2. If a self-contained ARM remains mandatory, add a content-addressed reusable
   base ARM layer and compile graph/orbit deltas separately. Do not use DuckDB
   file copying or mutable in-place updates as a substitute for immutable build
   identity.
3. Audit runtime queries before rebuilding the large unique indexes. Attaching
   the already indexed science artifact could eliminate most of the 17.6-second
   index phase; any removed index needs query-plan and latency evidence.
4. Retain full checksum verification for clean builds. A trusted local
   attestation cache could avoid about ten seconds on iterative builds only if
   it keys inode/size/mtime plus immutable manifest identity and periodically
   rehashes; promotion and reproduction must still perform full hashes.
5. Leave stellar-orbit compilation alone. Its sub-second cost and independent
   reproducibility make optimization scientifically pointless.

No optimization is implemented at this checkpoint. The report establishes the
baseline and prevents speed work from weakening provenance, isolation, or
logical reproduction gates.

Clean TESS runtime build `ab880f46a111428e8021e47e` is not a critical-path
target: compile takes 1.45 seconds at 1.63 GiB peak RSS, independent audit 0.29
seconds, and isolated byte-exact reproduction 1.68 seconds total. Its four
Parquet products should remain independently reusable rather than being folded
back into a monolithic selection pass.

ARM v4 `e3e82312eaa3cab931e9e756` compiles in 151.73 seconds, versus 151.40
seconds for v3. TESS input verification is under 0.01 seconds and copying all
four compatibility tables takes 0.31 seconds. The result confirms that TESS is
not responsible for the slow ARM build; unchanged multi-million-row selected
science copies remain the optimization target.
Isolated v4 compilation takes 154.01 seconds; full rebuild, logical-table
comparison, independent audit, and scratch removal take 178.83 seconds.

The first fail-closed run exposed 5,092 reused source edge IDs containing 6,936
collision rows. The full relationship tuples were all unique. The accepted seed
therefore assigns deterministic sequential edge IDs from the complete ordered
relationship identity and preserves the old numeric ID only as migration
lineage. Verification reports zero duplicate nodes, output edge IDs,
relationships, missing endpoints, or canonical objects without hierarchy
nodes. This correction belongs in the identity migration boundary rather than
being propagated into a new authoritative compiler.
