# Public Read v2 Build Performance

Date: 2026-07-25

Build: `e7_24cb15211f430a37f199f462_full_public`

This report separates the scientific Evidence Lake build from the derived
public-read work introduced by M8.3e. Public Read v2 does not add time to the
scientific compiler's critical path unless an operator chooses to build the
deployment projections immediately afterward. Hierarchy bundles and simulation
scenes are resumable, build-keyed admin products.

## End-to-End Steps

| Step | Measured wall time | Output | Notes |
| --- | ---: | ---: | --- |
| Public Read base compile | 584.767 s | 16,455,413,760-byte SQLite database | Includes base verification and indexes; later search-parity staging adds the selected-leaf overlay and final representation policy |
| Active hierarchy-bundle warming | 602.317 s | 14,145 compressed bundles | Zero missing or invalid bundles |
| Independent hierarchy reproduction | 615.361 s | Same 14,145 bundles | Reproduction timing; payload hashes pass |
| Full scene v7 warming | 2,812.852 s | 7,724 scenes / 80,415,323 compressed bytes | 12 isolated workers, zero failures, zero reuse |
| Scene validation and deterministic freeze | 7.044 s | 80,752,521-byte archive | Validation 3.831 s; archive creation 1.781 s; second freeze reproduced SHA-256 exactly |
| Desktop/mobile Playwright gate | 225.008 s | 62 passed / 48 intentional skips | Zero unexpected or flaky tests |
| Final constrained capacity campaign | 1,743 s | 17 profiles plus c1-c12 and recovery | All passed; includes cold, warm, burst, sustained, idle, scene miss/coalescing/hit, and staircase profiles |

The full-scene warm is the dominant derived-artifact step. It is not required
for ordinary singleton systems and is not part of CORE, ARM, or DISC
compilation. A deployment may resume this job, freeze the completed set, and
transfer only the verified archive.

## Base Projection Phases

The base compiler recorded these phases before search-parity staging:

| Phase | Rows | Wall time |
| --- | ---: | ---: |
| Planet facets | 2,019 | 0.011 s |
| Representation policies | 190,352 | 0.462 s |
| Systems | 5,869,091 | 47.353 s |
| Stars | 5,874,636 | 41.710 s |
| Planets | 2,826 | 0.021 s |
| Aliases and search terms | 13,794,890 | 31.907 s |
| Exact identifiers | 6,669,279 | 26.951 s |
| Identifier outcomes | 54,237 | 0.557 s |
| Identifier quarantine | 81,043 | 0.312 s |
| Singleton seed view | 5,862,333 at base stage | 19.912 s |
| Indexes and FTS5 | n/a | 172.196 s |
| Base verification | n/a | 95.216 s |

The final search-parity policy changes the singleton count to 5,861,345 by
moving every selected multistar and planet-host system into the full-scene
tier. The final manifest and independent verifier agree on that count.

## Resource Measurement

The 12-worker scene warm was observed at roughly 27 GiB aggregate resident
memory on Photon with ample available memory. The current materializer report
records only the parent process's 527 MiB peak and does not aggregate forkserver
worker CPU or RSS (`max_child_peak_rss_kib=0`). That is a measurement gap, not
evidence that the job uses only 527 MiB.

The public host does not perform this warm. It serves the 80.75 MB frozen
archive through bounded caches. Runtime memory and CPU are measured separately
by the M8.3e constrained capacity campaign.

The accepted capacity campaign is `final_20260725_v5`. Its constrained c12
mixed run reached 72.4 requests/s at 1.647-s p95; the five-minute sustained run
reached 73.2 requests/s at 522.6-ms p95 with zero errors. Five independent cold
complex scenes peaked at 4.33 GiB; all ordinary, prebuilt, mixed, sustained, and
idle profiles stayed near 1.5-1.6 GiB. The result is a conditional deployment
go based on disk staging, not a runtime-capacity failure.

## Optimization Priorities

1. **Keep warming off the compiler critical path.** This is already the largest
   practical improvement. Scientific promotion can complete before optional
   scene coverage is warmed.
2. **Reuse only semantically compatible scenes.** Ordinary reruns may reuse
   matching build and materializer versions. The v7 run correctly forced all
   7,724 scenes because classification and subsystem semantics changed.
3. **Instrument worker resources correctly.** Aggregate per-worker CPU, RSS,
   read bytes, and per-scene latency before changing worker count. The current
   parent-only resource figures cannot support worker-sizing decisions.
4. **Profile scene database access.** Each isolated worker owns correct
   read-only database attachments, but repeated per-system queries dominate the
   46.9-minute warm. A compiler-produced, policy-targeted input shard could
   reduce repeated joins without introducing distributed workers or weakening
   provenance.
5. **Batch hierarchy extraction.** The ten-minute hierarchy warm should be
   profiled for repeated attachment, decompression, and JSON assembly work.
   Preserve the existing accepted hierarchy semantics; optimize how its output
   is read and serialized.
6. **Treat FTS construction as a deliberate full-build phase.** Indexes and
   FTS5 consume 172.2 seconds, the largest base-projection phase, but replace
   continuous public scans with indexed millisecond reads. Rebuilding them is a
   sound trade unless incremental correctness can be proven.
7. **Do not weaken verification to save minutes.** Base verification costs
   95.2 seconds, and full artifact hashing/integrity scans are also material.
   They protect identity coverage, selected-fact lineage, and deterministic
   deployment artifacts.

The next optimization pass should first repair aggregate worker telemetry and
capture per-query/per-scene timing. Without that evidence, changing process
count or introducing more architecture would be guesswork.
