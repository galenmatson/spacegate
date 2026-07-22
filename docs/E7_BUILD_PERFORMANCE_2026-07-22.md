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

## Current Optimization Candidates

1. Preserve the type-partitioned component graph and compare its exact logical
   output with the canonical hierarchy rather than returning to a multi-inventory
   join.
2. Measure whether copying complete selected-science and WISE consumer tables is
   I/O-bound and whether immutable views or a smaller runtime projection can meet
   the same consumer contract.
3. Profile stellar leaf candidate ranking by evidence source and avoid sorting
   rows that cannot compete, while preserving the shared classification policy.
4. Measure index creation independently; omit only indexes proven unused by the
   runtime and verification query plans.
5. Avoid rebuilding content-addressed phases whose full input and relevant policy
   hashes are unchanged, but continue byte-verifying every referenced artifact.
6. Keep the final database hash and clean reproduction. Test parallel or
   write-time hashing only behind identical logical and deterministic product
   gates.

## Required Final Sections

- complete step inventory and timing table;
- critical-path and resource ranking;
- before/after optimization comparisons;
- rejected experiments and scientific constraints;
- local promotion, rollback, and re-promotion timings; and
- machine-report paths and exact accepted build identities.
