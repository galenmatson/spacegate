# Runtime Capacity and Deployment Gate

This document records M8.3d for promoted Evidence Lake public build
`e7_24cb15211f430a37f199f462_full_public`. It is a deployment decision, not a
reason to remove scientific content.

> **Superseded runtime decision:** M8.3e Public Read v2 replaces the scan-heavy
> paths measured below. Its accepted July 25 campaign passes all runtime SLOs
> and changes the decision to **conditional go**, subject to reviewed remote
> cleanup and streamed extraction. See
> `docs/PUBLIC_READ_ARCHITECTURE.md`. The M8.3d measurements remain here as the
> reproducible pre-change baseline.

## Decision

**No-go for deploying the complete Evidence Lake runtime to the existing
6-vCPU/12-GiB antiproton VPS with the current search/detail projection.**

The immutable map path, static UI, prebuilt simulation scenes, cache hits, and
same-key scene coalescing are suitable for constrained service. The dynamic
search and system-detail path is not. It misses the provisional 3,000-ms mixed
p95 gate at concurrency one, saturates near 3 requests/s at concurrency six,
and accumulates CPU queueing as concurrency rises. Unconstrained Photon also
misses the mixed latency gate, which shows that the primary defect is the
5.87-million-system runtime query shape rather than RAM alone.

M8.3e must add a stable, immutable, indexed search projection and bounded detail
summaries, then rerun this exact campaign. A larger host or separated API
compute remains a valid fallback if that projection still misses the budgets.

No antiproton deployment or load test occurred. The only antiproton operation
was a read-only disk/build inventory. Proton was not used or mutated. No
identifier, evidence, hierarchy, object, or scientific field was removed.

## Correctness

The preflight passes:

- accepted `TIC 307210830` and `TOI-700` resolve through exact identifier
  semantics
- deferred `TIC 150320610` and `TOI-6725.01` return explicit exact-no-match
  evidence rather than unrelated fuzzy results
- ambiguous `TIC 101462`, malformed forms, ordinary names, and ordinary fuzzy
  searches have API and desktop/mobile browser coverage
- four TIC/TOI browser tests pass with zero unexpected results
- the API integration suite passes against the optimized isolated stack
- Alpha Centauri, Tegmine, Fomalhaut, Xi Scorpii, Epsilon Indi, Sirius, Castor,
  Nu Scorpii, and 16 Cyg pass the fresh wide-orbit runtime verifier
- nested `simulation_tree_v1` and source-backed `group_pair` behavior remain
  generic; Xi Scorpii and Nu Scorpii retain one known diagnostic-only source
  endpoint each rather than receiving one-off production fixes

## Method

The pinned workload is
`config/runtime_capacity/workload_e7_24cb15211f430a37.json`. The isolated
Compose overlay assigns 5.5 CPU and 8.5 GiB to the API, 0.5 CPU and 512 MiB to
nginx, and leaves 3 GiB of the modeled 12-GiB host for the OS and other
services. Photon quotas model resource quantity, not the per-core speed of the
OVH virtual CPU.

Each report records the build and workload hashes, seed, endpoints, stable
system IDs, tile hashes, cache state, concurrency, duration, cgroup limits,
configuration, request rows, latency/queue percentiles, throughput, errors,
timeouts, RSS, cgroup memory, page cache, CPU, throttling, PSI, I/O, scene-cache
outcomes, and pool behavior.

Cold-cache measurement used target-bounded `POSIX_FADV_DONTNEED` on the pinned
database and artifact files and recorded `fincore` residency before and after.
It never used a global Photon cache drop. The cold single-user run evicted
3,406,565,376 resident bytes, read 5.5 GB from storage, completed without error,
and peaked at 4.17 GiB.

## Capacity Results

| Workload | Result |
| --- | --- |
| Unconstrained Photon mixed, c12 | 7.83 rps, p95 4.821 s, 0 errors, 5.44 GiB peak |
| Constrained baseline mixed, c12 | 2.70 rps, p95 11.474 s, 0 errors, 5.42 GiB peak |
| Optimized constrained mixed, c12 | 3.23 rps, p95 8.198 s, 0 errors, 2.88 GiB peak |
| Five diverse dynamic scene misses | p95 5.729 s, 2.53 GiB peak |
| Prebuilt scene hits | 224.5 rps, p95 72 ms |
| Repeated runtime scene-cache hits | 89.5 rps, p95 122 ms |
| Static UI/manifests/tiles | 533.3 rps, p95 111 ms |
| Sustained mixed, c2, 301 s | 1.249 rps, p95 5.569 s, 0 errors, 2.85 GiB peak |
| Idle observation, 300 s | 2.83 GiB steady, 1.69 CPU-s, no I/O, pressure, or growth |
| Open-loop burst, nominal 3 rps | 2.79 rps achieved, p95 7.106 s, queue p95 708 ms |

The raw saturation staircase produced:

| Concurrency | Throughput | p95 |
| ---: | ---: | ---: |
| 1 | 0.77 rps | 3.561 s |
| 2 | 1.24 rps | 4.582 s |
| 4 | 2.02 rps | 5.597 s |
| 6 | 3.06 rps | 5.997 s |
| 8 | 3.04 rps | 6.335 s |
| 12 | 2.96 rps | 7.964 s |

Recovery at concurrency one completed without errors or memory pressure. The
throughput plateau and rising latency after concurrency six define saturation;
they are not a claimed supported-user count. Under the 3,000-ms mixed p95 gate,
the supported dynamic mixed concurrency is currently zero.

## Low-Risk Optimizations

The API now supports an opt-in, build-aware pool of exclusive read-only DuckDB
connections. Pool entries retain side-database attachments, use the immutable
database fingerprint, close on served-build change, bound checkout wait, and
publish non-sensitive runtime counters through health. Pool size six with one
DuckDB thread per connection is the measured constrained configuration.

Against the four-thread/open-per-request baseline, the pool:

- increased mixed throughput by 19.5%
- reduced mixed p95 by 28.6%
- reduced peak service memory by 46.8%
- reduced the diverse dynamic-scene peak from 7.02 GB to 2.72 GB
- completed five-minute sustained and idle runs without OOM, memory pressure,
  checkout timeout, or idle growth

Health formerly checked out a DuckDB connection for every probe and queued
behind user requests. It now caches the verified build id by immutable database
fingerprint. Under the same c12 mix, health p95 fell from 3,705.8 ms to 3.4 ms
while total throughput changed only from 3.231 to 3.237 rps.

The pool is disabled by default in ordinary Compose. Future constrained
deployment should explicitly set:

```text
SPACEGATE_API_DUCKDB_MEMORY_LIMIT=5GB
SPACEGATE_API_DUCKDB_THREADS=1
SPACEGATE_API_DB_POOL_SIZE=6
SPACEGATE_API_DB_ACQUIRE_TIMEOUT_SECONDS=30
```

These changes improve bounds and resilience; they do not make the current
search/detail contract deployable.

## Browser and Map

The 1,000-ly cold-browser flight rendered approximately 95,000-97,000 points
with zero failed network requests or tile failures on desktop, mobile, and the
Photon-high profile. Usable controls arrived in 1.21-1.51 seconds. Search
results took 4.08-4.57 seconds, consistent with the server bottleneck.
Selection took 380-439 ms. JavaScript heap was 254-342 MB. Desktop and mobile
Playwright screenshots and canvas-pixel checks pass.

This validates the existing split: immutable nginx-delivered tiles scale; the
continuous camera path should remain outside DuckDB.

## Scene Architecture

Prebuilt scenes and runtime cache hits are inexpensive. A synchronized
12-request same-key miss produced one materialization and eleven coalesced
responses. Subsequent hits returned at p95 122 ms. Five diverse cold scene
misses still took p95 5.729 seconds.

Keep the current coalescer and bounded cache. Increase prebuilt coverage by a
versioned popularity/priority tier. If public traffic makes diverse scene
generation material, move misses to an asynchronous CPU work queue and return
an explicit pending response rather than multiplying synchronous API workers.
Do not reduce scene science or hierarchy to fit the VPS.

## Transfer and Disk

The exact candidate has:

- 3,712 files
- 24,197,303,766 logical bytes
- 24,205,422,592 allocated bytes on Photon
- a verified maximum-compression archive of 17,052,804,724 bytes
- archive SHA-256
  `c2954d6a1b641347968f56cb0753ea1d2ef7b4625d6f830fb78cede4462642e9`

At 85% payload efficiency, the archive alone takes about 4 h 28 m at 10 Mbps,
2 h 14 m at 20 Mbps, 53 m 30 s at 50 Mbps, 26 m 45 s at 100 Mbps, or
10 m 42 s at 250 Mbps. Build reports and metadata add less than 64 KiB.

The July 24 19:10 UTC read-only antiproton inventory found 32,639,197,184 bytes
available. It retains the current 12,897,808,384-byte extracted build, a
12,707,106,816-byte standby build, two 7.39-7.49-GB archives, and a
7,167,766,528-byte cache.

The candidate cannot be staged safely without cleanup. A separately reviewed
pretransfer retirement of the superseded standby and both old compressed
archives would reclaim 27,583,307,776 bytes while retaining the current
extracted build as rollback. Staging the new archive plus extracted candidate
would then leave 18,964,277,644 bytes at peak. Retiring the new archive only
after verified extraction would restore 36,017,082,368 available bytes.
No cleanup was performed.

Do not begin transfer until both the query architecture gate and a fresh
pretransfer disk inventory pass. Maintain at least 15 GiB available at all
times and preserve the current extracted build through public verification.

## Operational Gate

For the next candidate:

- mixed p95 must be at most 3,000 ms at a nonzero concurrency
- error rate must remain below 1%
- queue-delay p95 must remain below 1,000 ms
- aggregate service memory must remain below 8 GiB; alert at 6 GiB API RSS
- no OOM, timeout, sustained latency collapse, corrupt/missing artifact, or
  final-health failure is acceptable
- disk availability must remain above 15 GiB through staging and rollback
- static map delivery, scene hits/coalescing, TIC/TOI exact semantics, nested
  orbits, and desktop/mobile browser checks must remain at parity

An upgrade or service separation is triggered if the indexed projection still
cannot support the gate on the modeled envelope, if observed demand approaches
70% of measured saturation for a sustained interval, or if CPU queueing causes
the 3,000-ms p95 threshold to alert.

## Evidence

Machine reports are under:

```text
/data/spacegate/state/reports/runtime_capacity_gate/
  e7_24cb15211f430a37f199f462_full_public/
```

The review entrypoint is `runtime_capacity_gate_summary.json`. It hashes the
individual cold, warm, control, optimization, idle, burst, sustained,
staircase, scene, browser, and correctness reports. The exact archive is
retained locally at:

```text
/data/spacegate/dl/db/e7_24cb15211f430a37f199f462_full_public.7z
```

It is not the local `dl/current` target and was not transferred.

The host npm warning about unknown `globalignorefile` comes from npm 11's
system-level `/usr/local/lib/node_modules/npm/npmrc`, not this repository or the
Docker npm 10 build. Do not patch the project for it; remove or update that
system npm setting when npm 12 is adopted.
