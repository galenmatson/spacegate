# E7 End-to-End Build Performance - 2026-07-22

## Status

This report is provisional until the clean E7 compiler, independent verification,
shadow consumer build, local promotion, rollback, and re-promotion all complete.
Every executed step must retain machine-readable phase timings plus GNU `time -v`
evidence. The final report will rank the critical path and compare accepted and
rejected optimization attempts.

The report must account for:

- wall and CPU time;
- peak resident memory and swap behavior;
- filesystem input/output and durable artifact bytes;
- DuckDB staging and spill peaks;
- cold, warm, or reused input/cache state;
- named phase totals and uninstrumented process overhead; and
- scientific, scope, lineage, row-accounting, and deterministic-hash gates.

An optimization is acceptable only when the same scientific and reproducibility
gates pass. Reusing or weakening a stability-database path is not a build-time
optimization.

## Operator Checkpoint - 2026-07-23

The current end-to-end build is operationally too slow. E7 closeout must turn
the retained timing evidence into an actionable report rather than treating
instrumentation alone as completion. The report must:

- reconcile every named phase with total process and end-to-end wall time;
- separate compilation, queries, sorting, copying, serialization, hashing,
  verification, and uninstrumented overhead;
- identify whether each measurement was cold, warm, cached, or reused;
- rank bottlenecks by measured wall time, peak memory, I/O, and durable bytes;
- estimate the expected saving, implementation cost, and scientific or
  operational risk of each proposed optimization; and
- rerun the accepted path after changes so before/after results use equivalent
  inputs, gates, and cache conditions.

Local promotion correctness remains the immediate E7 gate. Performance work may
follow the local promotion/rollback drill, but the report and optimization
backlog are required before the slow-build issue can be considered resolved.

## Incremental Planet-Derivation Shard

The completion audit found that E5 policy v17 declared Kepler semimajor-axis,
inverse-square insolation, and equilibrium-temperature derivations, but the
monolithic selected-fact compiler implemented only stellar luminosity.
Rebuilding the approximately 39.1-GB selected-fact DuckDB and its roughly
71-GB complete artifact family for 1,374 planet facts would repeat the dominant
25-minute E5 path for a small, independent scientific program.

Focused build `1dfc750b79b88018983ded82` instead pins the exact selected-fact
and clean-foundation manifests and emits a 1.1-MB DuckDB plus 316 KB of
canonical Parquet. It derives 461 semimajor axes, 654 insolations, and 259
equilibrium temperatures. Every applicable target is resolved, direct selected
facts are never overridden, and every output has input-fact and algorithm
lineage.

| Phase | Wall s | CPU s | Input blocks |
|---|---:|---:|---:|
| Validate pinned inputs | 0.001 | 0.001 | 0 |
| Kepler semimajor axes | 1.754 | 7.377 | 495,728 |
| Insolation | 0.852 | 3.323 | 5,848 |
| Equilibrium temperature | 0.013 | 0.024 | 0 |
| Applicability and lineage audit | 1.613 | 6.363 | 8,552 |
| Export and checkpoint | 0.047 | 0.051 | 0 |
| **Total** | **4.412** | **17.359** | **510,128** |

Peak RSS is 2,480,616 KiB with no swap. A clean second compile reproduces both
Parquet files byte-for-byte and both DuckDB tables logically; DuckDB page bytes
remain correctly classified as nondeterministic query serialization. The
largest cost is scanning the upstream selected-fact database, not calculation.

This is the first accepted domain-sharding optimization. The next clean-science
compiler overlays the shard only for planet projection and carries selected
luminosity status/basis into runtime consumers. A downstream CORE/ARM/DISC/
public rebuild remains necessary before promotion, but the 25-minute monolithic
E5 compiler no longer sits on that iteration path.

Corrected modular classification build `3f645ac3de3323637ded93d5` provides the
clearest current I/O profile. It passes independent verification and isolated
reproduction with 60 new source-native classification evidence rows:

| Phase | Wall | Peak RSS after phase | Result |
|---|---:|---:|---|
| Verify immutable input bytes | 157.76s | 0.07 GiB | pass |
| Select Gaia DSC white dwarfs | 9.56s | 20.89 GiB | pass |
| Select source-native UltracoolSheet classes | 0.05s | 20.89 GiB | pass |
| Index, export, checkpoint, and product hash | 0.88s | 20.89 GiB | pass |
| Total | 169.20s | 20.89 GiB | pass |

Input hashing consumes 93.2% of wall time. The first optimization experiment
should cache a content-addressed byte attestation keyed by immutable file
identity and revalidate it against filesystem metadata during iteration, while
retaining a full cold hash for clean reproduction and promotion. This is a
performance optimization only if changed files cannot reuse the attestation.

## Preliminary Runtime ARM Measurements

The first clean runtime ARM graph implementation used one broad hierarchy join
against the system, star, and planet inventories. It was terminated after 16:05
without completing the component-graph phase. GNU `time -v` recorded 13,808.70
CPU-seconds, 1,433% average CPU, 23,133,316 KiB peak RSS, and 17,245,624 output
blocks. The partial database contained only the previously copied selected
consumer surfaces, confirming that the graph join was the bottleneck.

The graph was rewritten as type-specific system, star, and planet branches plus
a bounded hierarchy propagation for noncanonical nodes. A subsequent run reached
internal verification in approximately 2:08. That run was not accepted because
verification found 111 duplicate component bindings and 15 canonical planets
without selected parameter rows. The duplicate binding was traced to a nonunique
WDS identifier join and replaced with hierarchy-key propagation. The planet delta
is now reported as explicit selection coverage rather than mistaken for inventory
loss.

A corrected build reached and passed internal verification in 2:35.35 external
wall time. Internal named phases accounted 155.17 seconds and 437.88 CPU-seconds;
peak RSS was 47,273,908 KiB with no swap. The 13,698,871,296-byte database
contains exactly 11,759,440 component entities and 5,886,947 hierarchy edges.
It reports zero missing, duplicate, orphaned, ambiguously owned, or unbound graph
members. The 15 canonical planets without selected parameter rows remain explicit
coverage accounting rather than invented facts.

The broad `selected_consumer_surfaces` phase consumed 97.16 seconds (62.6% of
internal wall time), followed by indexes at 20.29 seconds, component graph at
9.19 seconds, database hashing at 7.16 seconds, and leaf classification at 7.24
seconds. Because the broad copy phase is not actionable, the compiler now times
each selected-science and WISE table independently.

Instrumented build `60435b9d8c85c94b9018ee36` passed in 2:35.95 external wall
time. Its non-overlapping phase sum is 152.57 seconds against 155.77 seconds of
internal process wall time, leaving 3.20 seconds of connection close, manifest,
publication, and timer overhead outside named phases. The six dominant table
copies were:

| Selected surface | Wall | Process share |
|---|---:|---:|
| stellar astrometry | 25.15s | 16.14% |
| stellar parameters | 18.66s | 11.98% |
| stellar physics | 17.46s | 11.21% |
| stellar variability | 15.33s | 9.84% |
| stellar photometry | 10.39s | 6.67% |
| stellar display classifications | 10.21s | 6.56% |

All seven WISE table copies together took less than 0.1 seconds. Index creation
took 19.94 seconds, component graph creation 8.94 seconds, leaf classification
7.65 seconds, final hashing 6.91 seconds, and immutable science-input byte
verification 5.41 seconds. The build used 429.07 CPU-seconds and peaked at
47,533,112 KiB RSS without swap. The independent verifier and clean reproduction
must still be timed separately.

Repository consumer search shows that current API and coolness paths directly
use the selected stellar-parameter and display-classification compatibility
surfaces. The wider astrometry, physics, variability, and photometry projections
are primarily compiler inputs or future evidence consumers. Splitting scientific
evidence from the self-contained deployable runtime could avoid duplicated bytes
and copy time, but it would alter deployment and failure boundaries. That is an
architectural optimization candidate, not an opportunistic E7 deletion.

The independent artifact audit completes in 8.01 seconds, uses 30.99 CPU-seconds,
and peaks at 2,073,572 KiB RSS. The clean isolated reproduction completes in
3:00.75 total: 155.91 seconds for recompilation and approximately 24.84 seconds
for reference/rebuilt logical scans, independent verification, report handling,
and scratch cleanup. It peaks at 47,448,364 KiB RSS, matches all 19 table schemas,
counts, and logical hashes, and removes the temporary artifact.

Permanent Solar identity compilation adds 4.22 seconds external wall time and
peaks at 282,888 KiB RSS. Its independent audit takes 4.09 seconds, and isolated
compile/audit/reproduction takes 8.23 seconds with a 279,780 KiB peak. These are
separate named stages in the eventual E7 critical path; they replace an implicit
stability-ARM identity lookup rather than adding optional duplicate work.

Selected Solar runtime compilation takes 0.15 seconds external wall time and
peaks at 92,484 KiB RSS. Independent verification and isolated byte-exact
compile/audit/reproduction each remain below 0.2 seconds. The stage is retained
separately because its policy distinction between periodic, hyperbolic, and
reference-origin solutions is scientifically material even though its runtime is
negligible.

Cross-source stellar-orbit selection build `80099350ba26465252053885` takes
0.86 seconds and peaks at 620,504 KiB RSS. Independent audit takes 0.35 seconds;
compile, audit, byte-exact product comparison, report handling, and scratch
cleanup take 1.18 seconds. Keeping authority selection in this compact artifact
allows policy iteration without paying the 2.5-minute ARM assembly cost.

Stellar-orbit endpoint bridge `5d9e530b307ad869142dcdaf` compiles in 1.52
seconds. Independent audit takes 1.31 seconds, while clean rebuild, audit,
byte-exact comparison, report handling, and scratch cleanup take 2.83 seconds
with a 611,140 KiB peak. Like orbit selection, endpoint-policy iteration is
therefore isolated from the large ARM assembly.

Clean Solar ARM v2 build `376285dd79d73a52972d74fd` completes in 148.73 seconds
of internal wall time and 2:28.91 under GNU `time -v`. It uses 430.19 CPU-seconds,
peaks at 47,150,292 KiB RSS without swap, and writes a 13,716,172,800-byte
DuckDB. The named phases total 145.41 seconds; connection close, publication,
manifest writing, and timer overhead account for the remaining 3.31 seconds.
The independent audit takes 8.15 seconds.

The six dominant selected-stellar copies still consume 92.89 seconds; all
selected-science copies consume 93.43 seconds. Component graph creation takes
8.31 seconds, leaf classification 6.95 seconds, indexes 18.03 seconds, input
product byte verification 10.31 seconds, and final database hashing 6.93 seconds.
All Solar copies, graph extension, and compatibility projections together take
0.48 seconds. This establishes that Solar compilation is not responsible for the
slow build.

The first Solar ARM attempt failed closed after 2:02.01 external wall time and a
39,006,908 KiB peak because the runtime projection referenced an unselected
source-native key instead of the release-scoped `source_record_id`. No artifact
was published and staging was removed. A lightweight schema smoke test was added
before the second full run; it validates the selected contracts, endpoint
resolution, compatibility column names, and period-null hyperbolic rows without
recopying the multi-million-row science surfaces.

The accepted isolated rebuild takes 149.18 seconds. Rebuild, logical signatures
for all 31 tables, independent verification, report handling, and scratch cleanup
take 2:52.34 internally and 2:52.54 under GNU `time -v`, peaking at 46,971,956
KiB RSS. Every schema, row count, verification section, and logical hash matches;
there are no differing tables.

## Clean DISC Phase Report

Clean DISC build `62c9d909371eef4dfd8b63c9` completes in 39.91 internal wall
seconds and 40.09 seconds under GNU `time -v`. It uses 125.29 CPU-seconds,
peaks at 10,088,360 KiB RSS without swap, and writes a 383,528,960-byte DuckDB
plus a 382,894,682-byte canonical Parquet. Independent verification takes 1.74
internal wall seconds and 1.86 seconds externally.

| Phase | Wall s | Compile share |
|---|---:|---:|
| Shared coolness scoring | 26.74 | 67.0% |
| Verify 7.72-GB CORE checksum | 3.96 | 10.0% |
| Verify 13.76-GB ARM checksum | 7.17 | 18.0% |
| Canonical Parquet export | 1.00 | 2.5% |
| Internal verification | 0.56 | 1.4% |
| Metadata and product hashing | 0.47 | 1.2% |

The named phase total is 39.91 seconds after rounding, so no meaningful work is
hidden outside the timers. An isolated rebuild takes 40.08 seconds, matches the
build identity, policy/input/profile lineage, verification results, canonical
Parquet size, and SHA-256 exactly, then removes scratch.

The 11.14 seconds spent rehashing immutable upstream databases is 27.9% of this
compiler but should not be removed from clean or promotion runs. A future local
attestation cache may reduce iterative developer builds only if it remains
content-addressed, periodically rehashes, and cannot satisfy reproduction or
promotion gates. Scoring remains the larger DISC-local target; optimize it only
behind identical ordered-Parquet and ranking checks. DISC is not the overall
critical path: ARM takes 151.73 seconds, clean selected science 190.81 seconds,
and selected facts approximately 25 minutes.

## Clean Runtime Bundle and Public Slice

Clean runtime bundle `fcd4eed36b84cf7a0cba67f3` composes the manifest-pinned
CORE, hierarchy, ARM, and DISC products as verified relative links. Its first
implementation re-read all 23.7 GB of linked products after already hashing the
source targets and took 24.61 seconds. The accepted implementation verifies that
each link resolves to the exact already-attested target with the registered byte
size instead of hashing the same bytes twice. It takes 12.31 internal seconds
and 12.35 seconds externally, peaks at 38,204 KiB RSS, and retains full SHA-256
verification of every source product. This is a 49.8% wall-time reduction with
no change to scientific or provenance gates.

Public-slice build `e7_fcd4eed36b84cf7a0cba67f3_public` materializes the clean
runtime candidate in 278.73 internal seconds and 4:38.96 externally. It uses
948.98 CPU-seconds, peaks at 32,650,664 KiB RSS without swap, and writes a
23,623,374,172-byte directory. The no-trim 1,000-ly policy preserves all
5,869,091 systems, 5,874,636 stars, 6,311 public planet rows, 1,026,480 aliases,
and 12,768,410 search terms. Derived verification and exact TESS compatibility
projection pass.

| Public-slice phase | Wall s | Build share |
|---|---:|---:|
| ARM materialization | 179.66 | 64.5% |
| CORE materialization | 62.77 | 22.5% |
| Canonical hierarchy materialization | 17.71 | 6.4% |
| CORE Parquet export | 8.75 | 3.1% |
| DISC materialization | 8.54 | 3.1% |
| All other measured work | 1.29 | 0.5% |

The comparable E6 public slice took 277.37 seconds, so the clean path has not
regressed materially. The cost is nevertheless mostly redundant copying: the
source bundle already contains deployable, content-addressed databases, but the
current public-slice contract creates another self-contained generation. The
best optimization target is therefore a manifest-addressed runtime layout or
block-reusing materializer, not lower scientific coverage, skipped checksums,
or weaker atomicity. Any prototype must preserve self-contained deployment,
visible missing-artifact failure, rollback, and exact row-accounting.

The isolated API integration gate against this unpromoted slice takes 16.42
seconds and passes after adapting the WISE motion schema and hierarchy leaf
overlay to the clean selected-fact contracts. API latency, rather than compiler
work, dominates that wall time; client-process CPU is 0.16 seconds and peak RSS
is 37,588 KiB. Map-tile and simulation-scene measurements remain open.

## Selected-Fact Compiler Phase Profile

The first fully instrumented selected-fact v17 run used a 48-GB DuckDB memory
limit, 16 threads, and USB-SSD spill storage. It failed closed at the final
source-accounting gate because the new SIMBAD policy initially equated accepted
source-record bindings with selected canonical facts. The run published no
artifact and removed its 39.2-GB staging database and 69.2-GB spill allocation.
It remains a valid performance measurement of every expensive compiler phase.

GNU `time -v` measured 28:35.67 wall time, 7,038.26 CPU-seconds, 60,050,964 KiB
peak RSS, no swap, approximately 580.8 GB of filesystem input, and 320.5 GB of
filesystem output. Named phases account for 1,703.95 of 1,715.67 wall seconds.
The largest completed phases were:

| Selected-fact phase | Wall s | CPU s |
|---|---:|---:|
| Gaia source candidate insertion | 793.93 | 3,119.14 |
| Global parameter-set selection | 294.68 | 861.42 |
| Immutable E4 input verification | 158.88 | 252.07 |
| Bailer-Jones binding | 83.23 | 442.38 |
| Bailer-Jones candidate insertion | 87.92 | 462.42 |
| Gaia AP supplement candidate insertion | 54.43 | 355.34 |
| Gaia AP candidate insertion | 47.38 | 330.10 |
| Gaia source binding | 35.65 | 376.73 |

The Gaia candidate insertion alone consumed 46.3% of process wall time and the
global selection over 123,224,517 source-selected facts consumed another 17.2%.
The process averaged only 410% CPU despite 16 configured threads and allocated
up to 69.2 GB of spill, so increasing thread count or memory without changing
the query plan is not an accepted optimization. The first prototype should
profile Gaia candidate construction independently, reduce repeated scans and
wide temporary materialization, and compare logical hashes before and after.

The failed gate exposed a useful accounting distinction rather than a science
error: 324,277 SIMBAD source records bind uniquely, while duplicate records for
the same canonical target collapse to 324,062 winning spectral-classification
facts. Both counts remain pinned independently. The corrected rerun must produce
the same binding outcomes and expensive-phase row counts before it can be
accepted.

The corrected build `5d9ec188dc2aab4c19439b89` passed all compiler integrity
gates and an independent artifact audit. Direct artifact queries confirm 324,277
accepted SIMBAD records, 324,062 distinct accepted targets, and 2,693 distinct
targets recovered through the no-Gaia HIP/HD fallback. Sirius A now carries the
selected `A0mA1Va` SIMBAD classification and literature reference through that
general release-scoped policy.

The accepted compiler run took 41:59.62 under GNU `time -v`, used 9,073.21
CPU-seconds, peaked at 55,692,780 KiB RSS without swap, read approximately
692.98 GB from the filesystem, and wrote approximately 355.36 GB. Its 107 named
phases account for 2,511.05 of 2,519.62 wall seconds. It produced 123,291,351
selected facts, 43,063,349 selection decisions, a 39,127,101,440-byte DuckDB,
and 71 GB of immutable database, Parquet, and manifest products.

| Accepted selected-fact phase | Wall s | Build share |
|---|---:|---:|
| Gaia source candidate insertion | 888.12 | 35.2% |
| Global parameter-set selection | 366.75 | 14.6% |
| Selected-fact Parquet exports | 312.29 | 12.4% |
| Artifact hashing | 204.22 | 8.1% |
| Immutable E4 input verification | 157.98 | 6.3% |
| Bailer-Jones binding and insertion | 175.35 | 7.0% |
| Selection-decision exports | 40.19 | 1.6% |

The identical accepted configuration was 13:23.95 slower than the failed run;
the failed run stopped before exports and hashing, so only like-for-like phase
comparisons are meaningful.
Gaia candidate CPU time changed by less than 1%, but its wall time increased by
94.19 seconds; global selection increased by 72.07 seconds. This confirms
substantial external-I/O sensitivity. The independent audit took 31.26 seconds,
252.07 CPU-seconds, and 32,264,892 KiB peak RSS. Clean reproduction timing and
hash comparison pass: the isolated run takes 42:13.96, uses 9,178.46
CPU-seconds, peaks at 54,367,020 KiB RSS without swap, matches the logical hash
`2c7e2b27a98305993437181953aeaab58d2d0858627b6e4f47f6ac04073c4c1c`
and every partition hash, and removes its scratch tree. Its Gaia candidate
insertion takes 920.22 seconds, global selection 340.54, selected-fact export
299.58, and hashing 165.35; this third profile confirms which costs are stable
CPU work and which fluctuate with storage state.

## Current Optimization Candidates

### Corrected Classification Cascade - 2026-07-23

The classification A/B exposed missing release-scoped white-dwarf and multiple-
component selection paths, so the clean science, runtime CORE, and runtime ARM
stages were rerun under corrected general policies. These are required scientific
rebuilds, not performance experiments, and their accepted and intermediate
timings remain part of the end-to-end cost record.

| Step | Build | Wall | Peak RSS | Result |
|---|---|---:|---:|---|
| Clean science compile | `90c218f01cbbb1aececbfd56` | 3:48.63 | 37.5 GiB | pass |
| Clean science independent audit | same | 11.92s | not separately sampled | pass |
| Clean science isolated reproduction | same | 4:02.97 | 37.3 GiB | pass |
| Runtime CORE compile | `1a3e15f2620f877881988bdc` | 1:42.79 | 27.0 GiB | pass |
| Runtime CORE independent audit | same | 7.7s | not separately sampled | pass |
| Runtime CORE isolated reproduction | same | 1:34.70 | 27.3 GiB | pass |
| Runtime ARM intermediate compile | `87080fbb6f2764743e3676ca` | 2:35.13 | 46.4 GiB | superseded after A/B |
| Runtime ARM accepted compile | `63cd3372d4bc8d32841dd08c` | 2:42.76 | 45.8 GiB | pass |
| Runtime ARM independent audit | same | 8.12s | not separately sampled | pass |
| Runtime ARM isolated reproduction | same | 2:58.53 | 46.4 GiB | pass |

No run swapped. Machine reports and GNU resource logs are retained under
`/data/spacegate/reports/evidence_lake_v2/e7_clean_science_v4`,
`e7_clean_runtime_core_v4`, `e7_clean_runtime_arm_v5`, and
`e7_clean_runtime_arm_v6`.

The accepted clean-science compiler accounts 228.16 internal seconds. Immutable
input verification takes 54.68 seconds, product hashing 43.71, canonical Parquet
export 37.30, and all scientific materialization and indexing 92.47. Integrity
and export work therefore consumes 59.5% of this stage. The isolated rebuild
reproduces every canonical Parquet hash exactly.

The corrected CORE compiler accounts 102.61 internal seconds. Index creation is
the largest single phase at 34.72 seconds in reproduction; stellar projection
takes 9.81, Parquet export 10.59, identity projection 7.15, and selected-science
verification 5.30. The accepted hierarchy is logically identical on rebuild and
all byte-exact products match.

The accepted ARM compiler accounts 162.54 internal seconds. Reproduction shows
component graph construction at 10.57 seconds and the complete release-scoped
leaf classification stage at 7.87 seconds. The broad selected-science copies and
indexes still dominate. The intermediate build is retained because its A/B
review reduced 5,938 old known-to-UNKNOWN leaf regressions to 1,549; the accepted
policy then reduced the tail to 372 while preserving genuine case-sensitive
component ambiguity rather than guessing.

The next optimization work is ordered by measured return:

1. Split clean science into content-addressed domain products. A classification-
   only policy change currently rebuilds unchanged astrometry, physics,
   photometry, and variability projections and then re-exports and re-hashes all
   of them.
2. Make runtime CORE and ARM manifest assemblies reference immutable scientific
   shards, or reuse unchanged physical blocks. This removes repeated broad table
   copies while preserving one atomic release identity, visible missing-artifact
   failure, and rollback.
3. Measure every CORE/ARM index against actual API, build, audit, and promotion
   query plans. Build only proven runtime indexes before promotion; keep audit-
   only indexes out of the deployable artifact when a logically identical scan
   is cheaper.
4. Add a content-addressed local attestation cache for iterative builds only.
   Clean reproduction and promotion must continue to byte-hash all pinned inputs
   and products.

### Modular Gaia Classification Correction - 2026-07-23

The Gaia DSC white-dwarf correction is the first classification source compiled
as an independently regenerable selected-science product. It demonstrates the
intended domain-shard architecture, but the current clean science, CORE, and ARM
assemblers still recopy all broad projections after the small shard changes.

| Step | Build | Wall | Peak RSS | Result |
|---|---|---:|---:|---|
| Gaia DSC selected classification compile | `8cd8c0805875c87fb4afeb4e` | 2:49.32 | 20.9 GiB | pass |
| Gaia DSC isolated reproduction | same | 2:50.19 | 20.9 GiB | byte/logical match |
| Clean science compile | `ba4ac952ef7fc86f1d3150d2` | 3:22.98 | 37.1 GiB | pass |
| Clean science independent audit | same | 10.23s | not separately sampled | pass |
| Clean science isolated reproduction | same | 3:24.63 | 36.8 GiB | canonical Parquet match |
| Runtime CORE compile | `9d66ffa81a03a714881be2f3` | 1:43.03 | 27.0 GiB | pass |
| Runtime CORE independent audit | same | 7.52s | not separately sampled | pass |
| Runtime CORE isolated reproduction | same | 1:31.98 | 27.1 GiB | no differing files |
| Runtime ARM compile | `c2eda7f868ff8ba2b747d717` | 2:33.01 | 46.2 GiB | pass |
| Runtime ARM independent audit | same | 8.08s | not separately sampled | pass |
| Runtime ARM isolated reproduction | same | 3:07.57 | 45.9 GiB | no differing logical tables |
| Runtime classification A/B | same | 1.64s | 2.4 GiB | pass |

The first Gaia-only modular classifier spent 157.23 of 169.13 internal seconds
hashing the 179.4-GB pinned Gaia AP database; selection itself took 10.06
seconds. The final source-native compile below shows the same architecture:
157.76 of 169.20 seconds goes to immutable-input hashing while the Gaia and
UltracoolSheet scientific passes take 9.56 and 0.05 seconds. This is
the clearest case for a local content-addressed input-attestation cache during
iteration. Full input hashing remains mandatory for clean reproduction,
promotion, and periodic integrity scrubs.

Clean science v5 spends 56.98 seconds verifying inputs, 38.30 exporting
canonical Parquet, and 13.27 hashing products. The four broad selected-star
projections consume another 55.89 seconds even though their content is unchanged
by the classifier shard. CORE then spends 34.38 seconds rebuilding indexes,
19.18 verifying science, and 10.28 re-exporting Parquet. ARM recopies unchanged
selected-science tables for 94.96 seconds; its actual component graph and leaf
classification work takes only 15.96 seconds. These measurements make immutable
table reuse and manifest assembly higher-value than optimizing classification
SQL.

Machine reports and GNU resource logs are retained under
`/data/spacegate/reports/evidence_lake_v2/e7_selected_stellar_classifications`,
`e7_clean_science_v5`, `e7_clean_runtime_core_v5`, and
`e7_clean_runtime_arm_v7`.

### Final Source-Native Correction, Runtime, Public Slice, and Map - 2026-07-23

The source-native UltracoolSheet correction feeds clean science
`2d084ee3c5939878259793bb`, CORE `21971e59527ccf5c729b7cab`, ARM
`45d996e094020fa52d8a3f82`, DISC `d43e93eeb9c09c9e7445c9d6`, runtime bundle
`73349c253a411945c246d459`, and deployment-shaped public candidate
`e7_73349c253a411945c246d459_public`. The public candidate is stored on the
internal `/data` volume in its intended production topology. It is not promoted
or served.

| Step | Wall | Peak RSS | Durable output | Result |
|---|---:|---:|---:|---|
| Modular classification compile | 2:49.20 | 20.9 GiB | 4 Parquet plus database | pass |
| Clean science compile | 3:53.27 | 37.0 GiB | 18 GiB allocated | pass |
| Clean science independent audit / reproduction | 10.18s / 3:33.02 | 37.0 GiB repro | scratch removed | pass / exact Parquet |
| Runtime CORE compile | 1:37.01 | 26.9 GiB | 12 GiB allocated | pass |
| Runtime CORE independent audit / reproduction | 7.8s / 1:36.37 | 27.3 GiB repro | scratch removed | pass / exact logical output |
| Runtime ARM compile | 2:37.10 | 45.8 GiB | 13 GiB allocated | pass |
| Runtime ARM independent audit / reproduction | 8.15s / 2:50.48 | 46.4 GiB repro | scratch removed | pass / exact logical output |
| Runtime classification A/B | 1.68s | 2.4 GiB | report only | 192 scoped deferrals, zero unaccounted |
| Clean DISC compile / isolated compile | 47.68s / 39.59s | 9.6 GiB | 384 MB database plus Parquet | byte-identical Parquet |
| Runtime bundle composition | 13.79s | 0.04 GiB | bounded immutable links | pass |
| Deployment-shaped public slice | 4:15.98 | 31.0 GiB | 23 GiB allocated | pass |
| Generic public-build verification | 28.68s | 4.2 GiB | reports only | pass |
| Four-radius map-tile build | 4:05.21 | 11.1 GiB | 413 MiB | pass |
| Four-radius map verification | 19.62s | 7.8 GiB | reports only | pass |
| Four-radius isolated map reproduction | 4:05.05 | 11.0 GiB | 413 MiB retained candidate | exact match |
| Deterministic map comparison | 0.68s | 0.04 GiB | report only | pass |
| Bounded 24-scene cold generation | 20.77s | 2.4 GiB | 282,262 bytes | pass |
| Bounded 24-scene warm reuse | 1.17s | 2.4 GiB | no new scenes | pass |
| API integration contract | 13.09s | client 37 MiB | reports only | pass |
| Strict search/detail/scene API benchmark | 20.82s | client 35 MiB | reports only | pass |
| Tiled-map desktop/mobile Playwright | 1:29.78 | runner 274 MiB | reports/screenshots | 12 pass, 4 intended skips |

The public candidate retains 5,869,091 systems, 5,874,636 stars, 6,311 public
planet rows, 1,026,480 aliases, and 12,768,410 search terms with zero trimming.
The generic build verifier was updated only for versioned clean contracts: named
stellar, multiplicity, and extended-object examples are diagnostics, while
identity, membership, lineage, scope, and accounting invariants remain gates.

The map builder exposes a clear nested-radius cost curve:

| Radius | Systems | Exact tiles | Wall | Share of map wall | Compressed exact bytes |
|---|---:|---:|---:|---:|---:|
| 100 ly | 10,209 | 8 | 5.57s | 2.3% | 633,894 |
| 250 ly | 206,913 | 64 | 11.15s | 4.6% | 11,797,103 |
| 500 ly | 1,820,142 | 450 | 58.38s | 23.9% | 101,344,253 |
| 1,000 ly | 5,319,825 | 2,574 | 169.52s | 69.3% | 297,224,289 |

All four exact memberships pass with zero missing, extra, duplicate, public-name,
representative-class, or leaf-badge mismatches. The first map optimization should
compile the 1,000-ly selected rows once and partition nested radii from that
materialization, then measure deterministic parallel tile compression. The
current implementation repeats the full joined selection query four times.
Compression work is CPU-parallel enough to consume 812 CPU-seconds in 245 wall
seconds, so additional concurrency must be benchmarked rather than assumed.

The first isolated API run found a build-identity cache violation: a candidate
could read the served build's precomputed scene because compatibility checked
the scene contract version but not the embedded build identity. The corrected
cache requires both top-level and materialization build IDs to match, writes the
same contract into runtime-cache artifacts, and then passes strict preview with
seven Castor hierarchy leaves and seven render bodies. This is a correctness and
cache-isolation fix, not a named-system exception.

Machine reports and GNU resource logs are retained under
`/data/spacegate/reports/evidence_lake_v2/e7_selected_stellar_classifications_v2`,
`e7_clean_science_v6`, `e7_clean_runtime_core_v8`,
`e7_clean_runtime_arm_v9`, `e7_clean_runtime_disc_v4`,
`e7_clean_runtime_bundle_v4`, `e7_public_v4`, `e7_clean_map_tiles_v4`,
`e7_scene_v4`, `e7_api_v4`, and `e7_browser_v4`.
The isolated map tree is retained as an exact 413-MiB retention candidate until
the cleanup ledger authorizes removal. The browser run checks nonblank canvas
pixels at every radius on desktop and mobile, 4K Bright rendering, exact-density
stress, flight refinement, and search handoff. Local promotion, rollback, and
re-promotion remain required before this report is final.

These experiments must compare logical table signatures, canonical Parquet
hashes, independent verification, API latency, and rollback behavior. More
threads are not the default remedy: the selected-fact compiler remains I/O- and
spill-sensitive, and the accepted ARM run averaged only 273% CPU while writing
about 12.9 GiB through the filesystem.

### Planet-Derivation Corrected Candidate - 2026-07-23

The focused planet-derivation shard required one downstream rebuild to prove
that its 1,374 facts reached every shared consumer. The corrected chain is clean
science `3b459ede84873e3adfeeec43`, CORE
`85e89d41dd1258e2f3015a7a`, ARM `abf25120b71bc71689790959`,
DISC `30280c152441804f449d86b3`, bundle
`39b7386d4524ce5b1ff2729f`, and unpromoted public candidate
`e7_39b7386d4524ce5b1ff2729f_public`.

| Corrected stage | Wall | Peak RSS | Filesystem output | Result |
|---|---:|---:|---:|---|
| Planet-derivation shard | 4.41s | 2.4 GiB | 1.4 MiB | pass |
| Clean science compile | 3:24.00 | 37.4 GiB | 17.5 GiB | pass |
| Clean science independent audit | 10.19s | 2.1 GiB | report only | pass |
| Runtime CORE compile | 1:18.26 | 27.3 GiB | 11.7 GiB | pass |
| Runtime CORE independent audit | 7.69s | 2.1 GiB | report only | pass |
| Runtime ARM compile | 2:38.09 | 46.4 GiB | 12.9 GiB | pass |
| Runtime ARM independent audit | 8.04s | 2.1 GiB | report only | pass |
| Runtime classification A/B | 1.6s | 2.4 GiB | report only | pass |
| Runtime DISC compile | 36.61s | 10.1 GiB | 737 MiB | pass |
| Runtime DISC independent audit | 1.76s | 2.1 GiB | report only | pass |
| Runtime bundle composition | 12.03s | 0.04 GiB | manifest only | pass |
| Self-contained public materialization | 4:20.95 | 31.1 GiB | 22.4 GiB | pass |
| Four-radius map tiles | 4:05.24 | 11.0 GiB | 412 MiB | pass |
| Four-radius map verification | 19.6s | 7.8 GiB | report only | pass |
| Planet derivation cutover A/B | 0.22s | 0.2 GiB | report only | pass |
| Bounded 24-scene cold / warm | 22.36s / 1.17s | 2.4 GiB | 283 KiB | pass |
| API integration / strict known systems | 19.29s / 31.30s | client 37 MiB | reports only | pass |
| Clean science isolated reproduction | 3:19.97 | 39.9 GiB | scratch removed | exact |
| Runtime CORE isolated reproduction | 1:19.44 | 27.4 GiB | scratch removed | exact/logical |
| Runtime ARM isolated reproduction | 2:32.87 | 46.0 GiB | scratch removed | logical exact |
| Runtime DISC isolated reproduction | 39.71s | 9.5 GiB | scratch removed | exact Parquet |
| Four-radius isolated map reproduction | 4:04.48 | 11.2 GiB | scratch removed | 3,686 payloads exact |
| Corrected mounted desktop/mobile Playwright | 1:31.08 | runner 272 MiB | reports/screenshots | 12 pass, 4 intended skips |

The compiler-only path from the focused shard through the deployable public
slice takes 12:34.35. Including map tiles takes 16:39.59. Verifiers and bounded
runtime gates add about 1:43 without map reproduction or browser tests. All
measurements used GNU `time -v`; compiler reports also retain named internal
phases.

The cutover A/B proves that inventory and all existing planet values are
unchanged. Exactly 461 semimajor axes, 654 insolations, and 259 equilibrium
temperatures appear with shard lineage. The only category changes are 89 new
insolation classes and 56 new orbit classes where the reference was null.
CORE, ARM, and public selected values agree exactly. Selected stellar
luminosity carries 1,983,744 source and 66,834 derived statuses with no missing
status or lineage mismatch.

This rebuild also exposed an invalid storage assumption: the DISC and bundle
assemblers resolved sibling paths relative to their output root. That works only
while every family is on one filesystem. They now resolve bounded,
release-scoped paths through the state artifact registry, so `/data` and
`/mnt/space` placement can change independently without weakening manifest
identity.

The measured optimization order is:

1. Make clean science a manifest of immutable domain shards. Its scientific
   planet overlay takes 0.31 seconds, while unchanged input verification,
   Parquet export, and product hashing take 111.46 seconds and unchanged stellar
   projections consume most of the remainder.
2. Stop physically recopying unchanged selected-science tables into both CORE
   and ARM. CORE spends 29.58 seconds on indexes; ARM spends 95.23 seconds on
   its six largest selected-science copies before graph work.
3. Replace public materialization with a self-contained content-addressed
   assembly or reflink/block-reusing materializer. The current stage spends
   4:21 and writes another 22.4 GiB even though its inputs are already immutable
   deployable databases.
4. Build the 1,000-ly selected map rowset once, then partition the nested radii
   and benchmark deterministic parallel compression. The current map compiler
   repeats the joined selection for four radii.
5. Add metadata-backed local hash attestations only for iterative runs. Clean
   reproduction and promotion continue to perform full byte hashing.

Adding threads is not the first intervention. These stages repeatedly scan,
copy, export, and hash the same tens of gigabytes; several already peak between
27 and 46 GiB and do not saturate Photon's CPUs. The durable speedup is immutable
shard reuse with the same scientific, lineage, atomicity, and rollback gates.

## Measured Optimization Backlog

The targets below are validation thresholds, not claimed results. Each change
must be benchmarked against an equivalent accepted input set and cache state.

| Rank | Measured opportunity | First implementation | Initial validation target | Principal constraint |
| --- | ---: | --- | ---: | --- |
| 1 | Public materialization: 260.95s and 22.4 GiB written | Content-addressed assembly or verified reflink/block reuse | Remove at least 50% of stage wall time and physical writes | Candidate must remain self-contained, checksum-verifiable, atomically promotable, and independently rollbackable |
| 2 | Clean science: 199.97s reproduction; 111.46s in unchanged verification/export/hash work | Manifest of immutable domain shards with one final projection | Remove at least 60s when only one bounded domain changes | Clean reproduction and promotion still perform full content verification |
| 3 | ARM: 152.87s reproduction; 95.23s copying six selected-science tables | Reference or block-reuse immutable selected-science tables | Remove at least 60s without increasing API latency | Runtime failure must remain visible and deployable products must not acquire hidden host-path dependencies |
| 4 | Four map radii: 245.24s | Select the 1,000-ly rowset once, partition nested radii, then test bounded encoding workers | Instrument query/partition/encode/compress phases before setting a savings gate | All 3,686 payloads and normalized manifests must reproduce exactly |
| 5 | CORE: 79.44s reproduction; 29.58s index construction | Measure every index against API/search/build query plans | Remove or defer only indexes with no accepted consumer | No unbounded request-time scan or search regression |

These changes should be implemented separately so their scientific and
performance effects remain attributable. Combining several structural changes
into the accepted E7 cutover candidate would invalidate the completed A/B and
reproduction review for little operational benefit.

## Recovery Verification - 2026-07-23

Verification run `20260723T091436Z_2034145` passed the current pinned artifact
graph in 50.63 seconds pipeline wall time. Executed verifiers used 141.63 CPU
seconds, peaked at 7,200,552 KiB RSS, read 29,798,416 filesystem blocks, and
wrote 480 blocks. Compiler rows were explicitly recorded as attested reuse;
none was reported as a zero-cost build.

The run was made while documentation changes were uncommitted, so it is a
recovery-command validation rather than promotion-grade repository evidence.
Its machine report is:

`/data/spacegate/state/reports/evidence_lake_v2/e7_build_runs/20260723T091436Z_2034145.json`

At the checkpoint, `/data` had 23 GiB free and `/mnt/space` had 19 GiB free.
That is insufficient headroom for another persistent 64.5-GiB downstream
cascade. No full compiler run should begin until retention review establishes
safe scratch/output placement or reclaims specifically reviewed superseded
artifacts.

1. Preserve the type-partitioned component graph and compare its exact logical
   output with the canonical hierarchy rather than returning to a multi-inventory
   join.
2. Prototype a deployable immutable selected-science shard referenced by ARM,
   or content-addressed assembly that reuses unchanged physical table blocks.
   Recopying unchanged selected science is the measured 93.43-second critical
   path; views are acceptable only if deployment and visible-failure guarantees
   remain self-contained.
3. Profile stellar leaf candidate ranking by evidence source and avoid sorting
   rows that cannot compete, while preserving the shared classification policy.
4. Profile the 18.03-second index phase against runtime and verification query
   plans. Defer or omit only indexes proven unused before atomic promotion.
5. Avoid rebuilding content-addressed phases whose full input and relevant policy
   hashes are unchanged, but continue byte-verifying every referenced artifact.
6. Keep the final database hash and clean reproduction. Test parallel or
   write-time hashing only behind identical logical and deterministic product
   gates.
7. Prototype a manifest-addressed public runtime generation that reuses the
   clean CORE, hierarchy, ARM, and DISC files. Compare it against the current
   278.73-second, 23.62-GB materialization while preserving atomic promotion and
   rollback as one build identity.

## Local Promotion Drill - 2026-07-23

Operator acceptance and the local production-topology drill close the remaining
E7 timing rows.

| Step | Wall | Peak runner RSS | Result |
|---|---:|---:|---|
| Candidate atomic pointer promotion | 0.36s | 60 MiB | pass |
| Cold image rebuild and service recreation after BuildKit prune | 2:42.34 | 69 MiB | pass |
| Candidate required strict verification | 30.55s | 4.29 GiB | pass |
| Candidate API integration | 17.37s | 35 MiB | pass |
| Candidate current known-system verification | 19.11s | 36 MiB | pass |
| Candidate focused desktop/mobile browser smoke | 40.25s | 230 MiB | 5 pass, 5 intended skips |
| Stability atomic rollback | 0.36s | 61 MiB | pass |
| Stability service recreation | 6.83s | 30 MiB | pass |
| Stability revision-pinned strict verification | 22.55s | 4.26 GiB | pass |
| Stability API integration | 36.23s | 35 MiB | pass |
| Candidate atomic re-promotion | 0.35s | 59 MiB | pass |
| Candidate service recreation | 6.76s | 30 MiB | pass |
| Re-promoted required strict verification | 30.33s | 4.21 GiB | pass |
| Re-promoted API integration / known systems | 17.45s / 18.96s | 36 / 36 MiB | pass |
| Re-promoted focused browser smoke | 17.60s | 228 MiB | 2 pass, 2 intended skips |

The container rebuild time is a cold post-prune dependency/image result, not a
database startup cost; reusing the built images reduces service recreation to
about 6.8 seconds. Pointer changes themselves are sub-second.

The drill exposed three compatibility debts without invalidating cutover:

- the opt-in multiplicity suite still queries deprecated MSC/orbit surfaces and
  fixed named-system row counts;
- the current leaf-classification verifier cannot validate the older rollback
  artifact because it expects 147 post-stability rows, while the verifier pinned
  to the rollback revision passes;
- the historical known-system verifier contains a Castor raw spectral-string
  assertion (`M1_Ve` versus equivalent `dM1e`) and is not a suitable rollback
  health gate.

These are inputs to M8.3e consumer review. No named-object transform was added.
Machine timings and checks are in
`e7_cutover_drill_2026-07-23/cutover_report.json`.

## Post-Cutover Hierarchical-Orbit Repair - 2026-07-24

The repair deliberately rebuilds only the endpoint bridge, ARM, runtime bundle,
public slice, map artifacts, and selected scenes. CORE, canonical hierarchy,
selected science, DISC scoring, and source acquisition remain unchanged.

| Step | Wall | Peak RSS | Durable result |
|---|---:|---:|---|
| Initial v4 bridge diagnostic | 82.22s | not externally sampled | correct but rejected O(n²) implementation |
| Indexed v4 bridge compile | 31.34s | 0.87 GiB | 3 Parquet products, 6.2 MiB |
| Indexed bridge reproduction and audit | 33.56s | 0.86 GiB | byte-exact, scratch removed |
| Final ARM compile | 164.29s | 46.4 GiB | 13.78-GB DuckDB |
| Final ARM independent audit | 8.24s | bounded verifier | all graph/scope checks pass |
| Final runtime bundle | 12.21s | 37 MiB | verified links, no product copy |
| Full 1,000-ly public materialization | 270.20s | 31.2 GiB | 23-GB exact-inventory public build |
| Four-radius map materialization | 256.44s | 11.0 GiB | 3,686 exact/sample tile artifacts |
| Nine repaired scene artifacts | 9.58s | 1.67 GiB | scene artifact contract v5 |
| Repaired candidate strict verification | about 33s | verifier-bounded | pass |
| Candidate service image rebuild/recreation | about 2s | warm BuildKit cache | pass |
| Prior-E7 rollback service recreation | 6.9s | bounded | health and API pass |
| Repaired-candidate re-promotion recreation | 6.9s | bounded | health, API, and wide-system pass |

Replacing the global source-label scan with a release/system-local index cuts
the new bridge by about 62%. The remaining increase over the old leaf-only
bridge pays for recursive group resolution, 10,146 membership rows, and 9,775
simulation-ready relation decisions. Profile the recursive hierarchy query and
Parquet export separately before further optimization; do not weaken exact
scope, collision, overlap, or deterministic-hash gates.

One public build was interrupted after the compiler-time issue was found and
its manifestless `.tmp` tree was retired through the standard state-retention
dry-run. A later complete candidate was rejected because the CLI default
applied the optional 500-ly distant-single trim. It was never served and was
retired by exact candidate hash before rebuilding with the full 1,000-ly
inventory. These costs are retained as failed/rejected iteration evidence.

The accepted runtime bundle is `24cb15211f430a37f199f462`; the locally served
public projection is `e7_24cb15211f430a37f199f462_full_public`. Its map phase
accounts exactly 10,209, 206,913, 1,820,142, and 5,319,825 systems at
100/250/500/1,000 ly. Local promotion, rollback to the preceding E7 public
build, and re-promotion all pass. No public-network transfer or antiproton
deployment is included in these timings.

## Required Final Sections

- complete step inventory and timing table;
- critical-path and resource ranking;
- before/after optimization comparisons;
- rejected experiments and scientific constraints;
- local promotion, rollback, and re-promotion timings; and
- machine-report paths and exact accepted build identities.
