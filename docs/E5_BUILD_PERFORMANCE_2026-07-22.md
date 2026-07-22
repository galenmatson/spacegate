# E5 Selected-Fact Build Performance - 2026-07-22

## Scope

This report measures the complete Evidence Lake v2 E5 selected-fact compiler on
Photon. It covers policy v13 (NASA host evidence) and policy v14 (the same
selection plus official Gaia GSP-Phot posterior model distance). Compiler phase
reports are machine-readable under
`/data/spacegate/state/reports/evidence_lake_v2/`.

The v14 reference artifact is USB-backed at
`/mnt/space/spacegate/e5-selection-v14/929bf92b4c5dbd5aef7e5972`.
The USB device holds staging and spill because the build can allocate more than
230 GB temporarily. Accepted science inputs remain immutable and checksum-
verified; this placement is not permission to weaken retention or provenance.

Policy v15 adds no named-object handling. It corrects the general
UltracoolSheet astrometric-proxy scope contract and compiles against release set
`51b08e537e768acf63e554e1`. Its full selected-fact timing is appended after the
instrumented build and clean reproduction complete.

Policy v16 adds the official IAU 2015 Resolution B3 solar effective-temperature
evidence and fixes a general ranked-EAV contract defect: evidence with a
non-null component scope now joins its accepted parameter-set binding instead
of being silently filtered by the source-record-only candidate path. The fix is
covered by a synthetic scoped-evidence regression and a real-artifact preflight;
it contains no Sun- or name-specific selection branch.

The first instrumented v15 attempt stopped cleanly at source accounting after
23:39.39. Its predicted 4,830 UltracoolSheet winners omitted 13 object-owned
facts that had previously lost invalid same-Gaia quantity competition to a
primary-proxy row. A source-native collision audit accounts 27 object/proxy
quantity collisions and confirms that v14 selected the proxy in exactly 13.
The corrected exact gate is therefore 4,843 accepted bindings and 4,843
selected facts; the gate was not weakened to a minimum.

## Measured Runs

| Run | Result | Phase wall | External wall | Facts | Decisions | Peak RSS | Peak staging | Peak spill |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| v13 reference compile | pass | 1,733.73s | about 29:00 | 121,306,839 | 41,078,837 | 36.0 GiB | 69.0 GiB | 149.7 GiB |
| v13 clean reproduction | pass | 1,477.17s | 24:49.54 | 121,306,839 | 41,078,837 | 55.8 GiB | measured | measured |
| v14 reference compile | pass | 1,822.11s | 30:28.86 | 123,289,311 | 43,061,309 | 35.5 GiB | 70.5 GiB | 150.2 GiB |
| v14 clean reproduction | pass | 1,783.93s | 29:54.48 | 123,289,311 | 43,061,309 | 35.3 GiB | 70.4 GiB | 149.1 GiB |
| v15 reference compile | pass | 1,792.16s | 29:58.87 | 123,288,872 | 43,060,870 | 35.4 GiB | 70.6 GiB | 151.2 GiB |
| v15 clean reproduction | pass | measured | 28:58.22 | 123,288,872 | 43,060,870 | 34.9 GiB | measured | measured |
| v16 first compile | fail safely at source accounting | about 1,160s | 19:19.55 | not promoted | not promoted | 55.8 GiB | incomplete | about 120 GiB observed |
| v16 reference compile | pass | 1,499.50s | 25:07.51 | 123,288,873 | 43,060,871 | 54.6 GiB | 70.5 GiB | 119.1 GiB |
| v16 reproduction attempt 1 | fail exact Parquet hashes | 1,919.14s | 32:13.48 | 123,288,873 | 43,060,871 | 52.8 GiB | measured | measured |
| v16 diagnostic reproduction | pass | 2,342.42s | 39:16.47 | 123,288,873 | 43,060,871 | 51.5 GiB | measured | measured |

The compiler accounts all 103 phases. The v14 independent artifact audit takes
59.77 seconds, peaks at 30.4 GiB RSS, and reports zero schema, scope, lineage,
authority, duplication, or completeness failures.

The v14 clean run matches build identity
`929bf92b4c5dbd5aef7e5972`, logical content hash
`af1155454dc91f8d653735e81ae8c153cdb5c7454e93ea4ab69301ea59d4be1f`,
and every compared report section. Its isolated work tree is removed after the
gate. No candidate is promoted to the accepted `current` pointer by either run.

V15 build `fa4aaed18aebcffb8632d978` passes the independent artifact audit with
logical content hash
`1b4fd75c00f9a21deb69e0c2136c9c39f7b25bb082b3bd378c260487d417685e`.
It removes 452 invalid primary-proxy winners and restores 13 object-owned facts
that had lost same-target competition, for a net reduction of exactly 439
facts and 439 decisions relative to v14. Clean reproduction passed in 28:58.22,
matched all 103 compared phase/report sections and the logical hash, and removed
its scratch tree; v15 is now an accepted unserved E5 checkpoint.

The first v16 attempt proved the new source binding was accepted but stopped at
the exact selected-fact floor because the general ranked-EAV insertion path
hard-coded source-record subjects while `create_binding` correctly emitted a
parameter-set subject for scoped evidence. No artifact was promoted. The
corrected v16 build `33006bde9bedd1fb365238b5` passes all 107 instrumented
phases and the independent artifact audit with logical content hash
`dd7ec911f0eeee4f6f7c98b1a92e7193b1134e4a2cd0b953f1c6663bd553e5c7`.
It adds exactly one selected fact and one decision relative to v15: the Sun's
published `5772.0 +/- 0.8 K` current best estimate. Exact nominal IAU conversion
constants remain evidence-only and are not misrepresented as measurements.

The v16 process used 8,717 CPU-seconds at 578% average CPU, peaked at 57,230,224
KiB RSS, read 1,395,652,504 filesystem blocks, wrote 729,737,192 blocks, and did
not swap. The summed phase wall time is 1,499.50 seconds versus 1,507.51 seconds
external wall time, leaving 8.01 seconds (0.53%) of process startup, teardown,
and timer/report overhead explicitly outside named phases.

The first clean v16 reproduction matched build identity, counts, and every
scientific integrity section but produced different Parquet bytes. Its old
verifier removed failed scratch before listing changed files, so the exact
partition set could not be recovered. The verifier now retains failed scratch,
persists the reproduced compile report, and emits expected/actual per-file
sizes and hashes. A second clean diagnostic reproduction matched every Parquet
hash and the reference logical hash exactly, then removed its scratch tree.
The passing gate is retained, but the intermittent first failure is not called
resolved; future reproduction failures will now be directly diagnosable.

The v15 top phases remain Gaia source direct materialization (554.06 seconds),
selected-fact export (230.09), global selection (165.21), immutable E4 byte
verification (158.17), artifact hashing (111.36), Bailer-Jones insertion
(110.37), and Bailer-Jones binding (92.77). The ranking confirms that the scope
fix introduced no new performance bottleneck.

## V16 Ranked Cost

| Rank | Phase | Wall | Share |
|---|---|---:|---:|
| 1 | Gaia source authoritative-direct materialization | 445.62s | 29.72% |
| 2 | Selected-fact Parquet export | 176.34s | 11.76% |
| 3 | Immutable E4 byte verification | 158.35s | 10.56% |
| 4 | Global parameter-set selection | 144.38s | 9.63% |
| 5 | Artifact hashing | 123.03s | 8.21% |
| 6 | Bailer-Jones candidate insertion | 83.14s | 5.54% |
| 7 | Bailer-Jones identity binding | 76.41s | 5.10% |
| 8 | Gaia supplementary AP candidate insertion | 41.98s | 2.80% |
| 9 | Gaia source identity binding | 31.96s | 2.13% |
| 10 | Gaia AP candidate insertion | 31.76s | 2.12% |

All 107 phase rows, including every source prepare/bind/preselect/insert step,
every integrity check, export, hash, and promotion operation, are retained in
`e5_selected_fact_v16_compile_timing.json`. The successful build allocated at
most 127,927,771,136 spill bytes and 75,656,151,040 staging bytes. These are
allocated-byte peaks and can exceed the final 71-GiB artifact's disk usage.

## V14 Ranked Cost

| Rank | Phase | Wall | Share |
|---|---|---:|---:|
| 1 | Gaia source authoritative-direct materialization | 558.02s | 30.63% |
| 2 | Selected-fact Parquet export | 221.21s | 12.14% |
| 3 | Global parameter-set selection | 186.02s | 10.21% |
| 4 | Immutable E4 byte verification | 158.88s | 8.72% |
| 5 | Artifact hashing | 119.73s | 6.57% |
| 6 | Bailer-Jones candidate insertion | 110.68s | 6.07% |
| 7 | Bailer-Jones identity binding | 93.52s | 5.13% |
| 8 | Gaia AP candidate insertion | 58.98s | 3.24% |
| 9 | Gaia supplementary AP candidate insertion | 51.76s | 2.84% |
| 10 | Gaia source identity binding | 47.05s | 2.58% |

V14 adds 1,982,472 facts, a 1.63% fact increase, while phase time increases by
88.38 seconds, or 5.10%. Global selection grows disproportionately from 118.65
to 186.02 seconds in the reference and 141.12 seconds in clean reproduction.
An independent source-record scalar is therefore paying the cost of the general
coherent parameter-set competition, although the two-run spread means the
reference value is not a precise standalone benchmark.

## Optimization Order

1. **Immutable program-level intermediates.** Compile each source/object/scope
   program into a content-addressed binding and candidate shard keyed by its E4
   artifact, relevant policy subsection, identity graph, canonical reference,
   and compiler hash. A one-field Gaia policy addition should not rebuild
   unchanged Gaia source, Bailer-Jones, spectroscopy, naming, and variability
   programs.
2. **Separate direct scalars from coherent parameter sets.** Source-record
   scalar evidence such as GSP-Phot distance needs authority competition and
   exact lineage, but not parameter-set preselection. Feed it through a typed
   direct-scalar lane and reserve the global coherent selector for quantities
   that can actually conflict as parameter sets.
3. **Use one durable representation for authoritative-direct facts.** The Gaia
   source path currently writes about 89 million facts into DuckDB and later
   exports them again. Test a deterministic Parquet-first path that remains
   queryable during final conflict checks, rather than paying for two complete
   materializations.
4. **Export once and hash while writing.** Measure one-pass stable partitioned
   export and incremental content hashing. Preserve filenames, ordering,
   compression, row accounting, and logical hashes before accepting any change.
5. **Reuse release-scoped identity outcomes.** Binding results depend on the
   source release, identity graph, component policy, and canonical reference,
   not on unrelated scientific quantity policies. Promote them to immutable
   reusable compiler inputs without losing accepted, missing, ambiguous,
   excluded, or quarantined accounting.
6. **Keep byte verification authoritative.** Four workers verify 382.7 GB in
   about 159 seconds. Test worker counts and storage placement, but do not
   replace byte hashes with size, mtime, or manifest trust. Filesystem-level
   immutability or verity may eventually support a durable verification cache.

V16 sharpens that order. Gaia materialization reached the 48-GB DuckDB limit
and generated roughly 119 GiB of allocated spill, while using only 6.5 cores on
average during its 445.62-second phase. The next experiment should first reduce
the candidate/direct-fact row width and materialization count, then compare
spill bytes and logical hashes. Export plus final hashing costs 299.38 seconds;
stream-integrated hashing or one durable fact representation is worth testing,
but only behind exact partition and lineage comparisons. Raising thread or
memory limits alone is not supported by these measurements.

## Release Composition Cost

Composing release set `51b08e537e768acf63e554e1` with full checksum
verification reads 448,814,563,328 database bytes and takes 6:17.82 wall time
at 72% of one CPU core. This is scientifically authoritative but largely
single-threaded. A content-addressed unchanged-member attestation or bounded
parallel hashing pass is a valid optimization target only if it still verifies
every changed shard byte and fails closed on metadata or inode changes.

## Identity Churn

Selected fact and decision IDs currently include the global policy version.
Consequently, a one-field policy addition changes IDs for otherwise identical
winners and prevents safe program-level reuse. A future compiler contract should
derive decision identity from the relevant policy-rule hash while retaining the
global policy version as artifact lineage. That migration requires an explicit
schema/version boundary and A/B proof; it must not be changed opportunistically
inside the current E6 cutover.

## Rejected Experiments

- A one-time accepted-binding cache increased Gaia source binding from about
  44.0 to 48.0 seconds and direct insertion from about 540.0 to 661.5 seconds.
  It is not an optimization candidate in its measured form.
- Fast one-pass partitioned export produced nondeterministic output bytes.
  Globally ordered export spilled excessively, and concurrent stable writers
  contended on DuckDB temporary-directory state. The stable sequential export
  remains the accepted implementation until a replacement passes exact hashes.
- A 12-thread/48-GB profile improved the earlier complete build by only 7.6%
  while consuming 6.5% more CPU and peaking near 56 GiB RSS. The shared-host
  default remains eight threads and a 32-GB DuckDB limit.

## Machine Reports

- `e5_selected_fact_v14_compile.json`
- `e5_selected_fact_v14_compile_timing.json`
- `e5_selected_fact_v14_performance_analysis.json`
- `e5_selected_fact_v14_audit.json`
- `e5_selected_fact_v14_reproduction.json`
- `e5_selected_fact_v14_reproduction_performance.json`
- `e4_ultracoolsheet_v2_scope_audit.json` (includes all 27 object/proxy Gaia
  classification collision groups used to explain the v15 count delta)
- `e5_selected_fact_v15_compile.json`
- `e5_selected_fact_v15_compile_timing.json`
- `e5_selected_fact_v15_performance_analysis.json`
- `e5_selected_fact_v15_audit.json`
- `e5_selected_fact_v15_reproduction.json`
- `e5_selected_fact_v15_reproduction_performance.json`
- `e5_selected_fact_v16_attempt1_source_accounting_fail.time`
- `e5_selected_fact_v16_compile.json`
- `e5_selected_fact_v16_compile.time`
- `e5_selected_fact_v16_compile_timing.json`
- `e5_selected_fact_v16_performance_analysis.json`
- `e5_selected_fact_v16_audit.json`
- `e5_selected_fact_v16_audit.time`
- `e5_selected_fact_v16_reproduction.json` (written by the clean reproduction)
- `e5_selected_fact_v16_reproduction.time` (written by the clean reproduction)
- `e5_selected_fact_v16_reproduction_performance.json` (written by the clean reproduction)
- `e5_selected_fact_v16_reproduction_attempt1_parquet_hash_fail.json`
- `e5_selected_fact_v16_reproduction_attempt1_parquet_hash_fail.time`
- `e5_selected_fact_v16_reproduction_attempt1_parquet_hash_fail_performance.json`

All paths above are relative to
`/data/spacegate/state/reports/evidence_lake_v2/`. The compiler log and GNU
`time -v` reports are retained alongside them so wall time, CPU, RSS, page
faults, filesystem I/O, and exit status remain independently inspectable.

## Acceptance Constraints

Performance changes are acceptable only if they preserve exact source and
component scope, evidence and binding lineage, authority ordering, coherent-set
selection, missing/ambiguous/excluded accounting, deterministic partition
hashes, lower-authority rejection, and clean reproduction. A faster compiler
that weakens any of those properties is a regression.
