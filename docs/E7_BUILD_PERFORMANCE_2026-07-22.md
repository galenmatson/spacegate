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

## Current Optimization Candidates

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

## Required Final Sections

- complete step inventory and timing table;
- critical-path and resource ranking;
- before/after optimization comparisons;
- rejected experiments and scientific constraints;
- local promotion, rollback, and re-promotion timings; and
- machine-report paths and exact accepted build identities.
