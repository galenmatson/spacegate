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

## Measured Runs

| Run | Result | Phase wall | External wall | Facts | Decisions | Peak RSS | Peak staging | Peak spill |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| v13 reference compile | pass | 1,733.73s | about 29:00 | 121,306,839 | 41,078,837 | 36.0 GiB | 69.0 GiB | 149.7 GiB |
| v13 clean reproduction | pass | 1,477.17s | 24:49.54 | 121,306,839 | 41,078,837 | 55.8 GiB | measured | measured |
| v14 reference compile | pass | 1,822.11s | 30:28.86 | 123,289,311 | 43,061,309 | 35.5 GiB | 70.5 GiB | 150.2 GiB |
| v14 clean reproduction | pass | 1,783.93s | 29:54.48 | 123,289,311 | 43,061,309 | 35.3 GiB | 70.4 GiB | 149.1 GiB |

The compiler accounts all 103 phases. The v14 independent artifact audit takes
59.77 seconds, peaks at 30.4 GiB RSS, and reports zero schema, scope, lineage,
authority, duplication, or completeness failures.

The v14 clean run matches build identity
`929bf92b4c5dbd5aef7e5972`, logical content hash
`af1155454dc91f8d653735e81ae8c153cdb5c7454e93ea4ab69301ea59d4be1f`,
and every compared report section. Its isolated work tree is removed after the
gate. No candidate is promoted to the accepted `current` pointer by either run.

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
