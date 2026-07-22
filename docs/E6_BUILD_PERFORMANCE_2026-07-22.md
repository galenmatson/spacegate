# E6 Shadow Build Performance - 2026-07-22

## Scope

This report measures the first accepted Evidence Lake v2 E6 shadow foundation,
`e6_994a6301c335ac385f5dc052_shadow`. It does not yet include DISC rescore,
public-slice, map-tile, simulation-scene, API/search, or browser verification.
Those phases must be appended before E6 closes.

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
