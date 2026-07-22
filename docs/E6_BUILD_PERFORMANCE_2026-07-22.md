# E6 Shadow Build Performance - 2026-07-22

## Scope

This report measures the first accepted Evidence Lake v2 E6 shadow foundation,
`e6_994a6301c335ac385f5dc052_shadow`. It does not yet include DISC rescore,
public-slice, map-tile, simulation-scene, API/search, or browser verification.
Those phases must be appended before E6 closes.

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
1,160 lost distances are legacy Gaia inverse-parallax values without a selected
posterior estimate and require an explicit distance policy rather than silent
fallback. Neither tail is hidden by the compatibility view.

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
