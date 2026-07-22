# E5 Selected-Fact Build Performance Report

Date: 2026-07-22
Host: Photon
Accepted build: `0a57f778ce13de1c2c800103`
Policy/compiler: `2026-07-22.e5-selection.12` / `selected_fact_compiler_v11`

## Result

The shared-host 8-thread/32-GB build passed compilation and independent audit,
then reproduced from clean USB-backed scratch with identical report sections
and logical Parquet hash
`6ccec12397bbe7d64878c52ead6a06ffca52d686e75020b8fb08831e58c69628`.
The artifact contains 94,414,212 exhaustive binding outcomes, 164,425 model
preselections, 41,078,490 coherent-set decisions, 121,304,924 selected facts,
and 65,104 derivations. All 99 measured phases passed.

The accepted compile took 1,577.317 measured phase seconds and 1,583.43
external wall seconds (26:23). Clean reproduction took 1,734.421 measured
phase seconds and 1,744.31 external wall seconds (29:04). The accepted run used
7,033.775 CPU-seconds and peaked at 35.25 GiB process RSS, 69.00 GiB staging
allocation, and 148.59 GiB spill allocation. Reproduction used 7,137.650
CPU-seconds and peaked at 36.19 GiB RSS, 69.02 GiB staging, and 149.79 GiB
spill.

## Phase Families

Times are wall seconds. The raw timing reports retain every individual phase,
source, CPU measurement, row count, memory high-water mark, and spill sample.

| Phase family | Accepted | Reproduction | Accepted share |
| --- | ---: | ---: | ---: |
| Source candidate insertion | 793.392 | 815.678 | 50.30% |
| Source binding | 216.819 | 225.377 | 13.75% |
| Deterministic exports | 204.172 | 229.851 | 12.94% |
| Immutable E4 input verification | 158.633 | 159.480 | 10.06% |
| Global parameter-set selection | 116.966 | 129.845 | 7.42% |
| Artifact finalization and hashing | 57.553 | 114.836 | 3.65% |
| Integrity checks | 20.782 | 41.880 | 1.32% |
| Preflight | 4.509 | 6.755 | 0.29% |
| Derivations | 1.645 | 7.121 | 0.10% |
| Source preselection | 1.337 | 1.393 | 0.09% |
| Source accounting | 0.681 | 1.408 | 0.04% |
| Summary accounting | 0.417 | 0.334 | 0.03% |
| Source preparation | 0.388 | 0.409 | 0.03% |
| Schema/input attachment | 0.014 | 0.042 | <0.01% |
| Database open | 0.008 | 0.012 | <0.01% |

The reproduction's larger export and final-hash cost is consistent with its
artifact being written and read from USB scratch. Scientific compilation varied
by roughly 0-4% for the dominant sources and produced identical content.

## Per-Source Work

Each total includes prepare, exhaustive binding, preselection, and candidate
materialization for the accepted run.

| Source | Wall seconds | CPU seconds | Build share |
| --- | ---: | ---: | ---: |
| Gaia DR3 source | 587.457 | 2,701.176 | 37.24% |
| Bailer-Jones EDR3 distances | 199.640 | 829.424 | 12.66% |
| Gaia DR3 astrophysical parameters | 85.548 | 466.667 | 5.42% |
| Gaia DR3 supplementary AP | 67.803 | 344.219 | 4.30% |
| Gaia DR3 variability | 23.198 | 114.220 | 1.47% |
| VSX | 12.780 | 67.163 | 0.81% |
| LAMOST DR11 | 12.504 | 68.006 | 0.79% |
| APOGEE DR17 | 5.457 | 12.607 | 0.35% |
| SIMBAD | 5.284 | 25.398 | 0.34% |
| Gaia EDR3 white dwarfs | 5.174 | 18.091 | 0.33% |
| NASA Planetary Systems | 3.891 | 9.385 | 0.25% |
| GALAH DR4 | 2.880 | 5.836 | 0.18% |
| UltracoolSheet | 0.194 | 0.836 | 0.01% |
| IAU WGSN | 0.125 | 0.695 | 0.01% |

## Ranked Optimization Work

1. **Gaia direct-fact representation (543.136 seconds).** Profile encoding and
   compare a Parquet-first or single-durable-representation compiler. The
   current build writes 89,068,940 Gaia source facts into the inspection
   database and later writes deterministic Parquet partitions. Removing one of
   those full representations during compilation has the largest plausible
   payoff, but the inspection and immutable-artifact contracts must remain.
2. **Deterministic export (204.172 seconds).** Test one-pass partitioned export
   only in disposable scratch with exact filename, row-order, row-count, and
   hash gates. Unordered partitioning was faster but nondeterministic; globally
   ordered and concurrent stable writers were measured and rejected for spill,
   incomplete throughput, or DuckDB temporary-directory conflicts.
3. **Bailer-Jones binding and projection (199.640 seconds).** Profile the
   accepted-versus-missing joins and test deterministic namespace buckets. All
   17,310,560 outcomes must remain explicit; an optimization may compact the
   execution path, not discard the unresolved tail.
4. **Immutable input verification (158.633 seconds warm; 358.4 seconds in the
   separate cold release-set check).** The current four-worker byte verification
   is real integrity work over hundreds of gigabytes. Test worker counts and a
   content-addressed unchanged-member attestation, but do not replace checksums
   with path, size, or mtime trust. Filesystem-level verified immutability would
   be a prerequisite for safely avoiding repeated full reads across processes.
5. **Global coherent-set selection (116.966 seconds).** Profile selection by
   quantity group and determine whether content-addressed per-group decisions
   can be composed without changing cross-source authority or winner/runner-up
   semantics.
6. **Artifact hashing (57.526 seconds).** Evaluate hashing each deterministic
   output while it is written. The final artifact must still expose independent
   hashes and permit a later verifier to reread the bytes.

The previously tested accepted-binding cache is not a candidate: it increased
Gaia insertion from 540.0 to 661.5 seconds and binding from 44.0 to 48.0
seconds. The isolated 12-thread/48-GB profile improved the earlier full build by
only 7.6% while consuming 6.5% more CPU and about 56 GiB RSS. The 8-thread/32-GB
profile remains the normal setting on the shared Photon host.

## Integrity and Retention

The first current-release intermediate exposed an over-strict external audit:
one alpha-abundance set had no optional `logchisq`, but it was the only
same-authority candidate. Compiler v11 and the independent auditor now require
a declared quality score only when a same-authority competitor exists. They
also verify the exact policy version and policy-file hash. The accepted build
passes these gates with zero failures.

The rejected intermediate `c27804da6fe9e6ada61184b0` was removed only after
the policy-v12 compile, independent audit, and clean reproduction passed. The
fail-closed retention run used candidate-set hash
`85b3c10f7c0853e994d27e9f59ad51762efb2a48a1d8b57ebe82570c7a295279`
and reclaimed 74,069,770,240 allocated bytes. Its audit, timing, performance,
compile, and retention reports remain.

## Machine Reports

- `/data/spacegate/state/reports/evidence_lake_v2/e5_selected_fact_policy_v12_compile.json`
- `/data/spacegate/state/reports/evidence_lake_v2/e5_selected_fact_policy_v12_compile_timing.json`
- `/data/spacegate/state/reports/evidence_lake_v2/e5_selected_fact_policy_v12_performance_analysis.json`
- `/data/spacegate/state/reports/evidence_lake_v2/e5_selected_fact_policy_v12_artifact_audit.json`
- `/data/spacegate/state/reports/evidence_lake_v2/e5_selected_fact_policy_v12_reproduction.json`
- `/data/spacegate/state/reports/evidence_lake_v2/e5_selected_fact_policy_v12_reproduction_timing.json`
- `/data/spacegate/state/reports/evidence_lake_v2/e5_selected_fact_policy_v12_reproduction_performance_analysis.json`
- `/data/spacegate/state/reports/evidence_lake_v2/e5_selected_fact_policy_v11_retention_applied.json`
