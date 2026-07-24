# Full-Build Storage Requirements

Spacegate can be installed from a prebuilt public dataset without reproducing
the scientific compiler. A source build of the complete Evidence Lake is a
different storage class. Operators must decide which workflow they intend to
run before acquiring data.

## July 2026 Measured Reference

The accepted Evidence Lake v2 build on Photon provides the current planning
baseline. These are allocated bytes from immutable artifacts or measured peak
compiler reports, not estimates based on source download sizes:

| Build area | Allocated bytes | Approximate size |
| --- | ---: | ---: |
| Pinned raw Evidence Lake inputs | 48,548,913,152 | 45.2 GiB |
| Source-native typed lake | 56,499,077,120 | 52.6 GiB |
| Accepted E4 evidence release set | 449,199,915,008 | 418.4 GiB |
| Accepted E5 selected-fact generation | 74,092,294,144 | 69.0 GiB |
| Current full public generation | 24,205,422,592 | 22.5 GiB |
| **Retained accepted chain subtotal** | **652,545,622,016** | **about 608 GiB** |
| E5 clean-reproduction peak spill | 160,832,151,552 | 149.8 GiB |

The accepted-chain subtotal excludes permanent identity products, intermediate
CORE/ARM/DISC compiler generations, reports, published downloads, an immediate
rollback, filesystem reserve, and the temporary destination required for
atomic replacement. The E5 staging size is the candidate artifact itself;
while replacing an existing generation, both old and new generations may be
present.

The July 24 storage audit measured approximately 1.49 TB used on the Photon
internal data filesystem and 742 GB used on its bulk filesystem before the
latest archive retirement. That footprint included protected historical
generations and research material and is not the clean-build minimum. It is a
useful upper operational observation: generous reproducibility retention can
readily exceed 2 TB across tiers.

## Capacity Tiers

Use **usable filesystem capacity**, after formatting and reservations, rather
than the marketed drive size.

- **Prebuilt public runtime:** does not require the Evidence Lake compiler.
  Size the host from the published bundle, map tiles, one rollback, caches, and
  database runtime measurements for that release.
- **Constrained clean scientific build:** reserve at least **1.25 TB usable**.
  This assumes one pinned release, external or promptly retired scratch, no
  generous historical retention, and careful phase-by-phase preflight. It is a
  lower operational bound, not a comfortable configuration.
- **Recommended full E0-E7 builder:** provide at least **2 TB usable fast local
  storage**. This permits the accepted chain, measured spill, atomic candidate
  replacement, one rollback, reports, and reasonable failure headroom.
- **Builder with reproducibility history and observation caches:** provide
  **3 TB or more total usable capacity**, preferably split between a fast
  internal active tier and a separately failure-contained cold/archive tier.

A nominal 1 TB drive is not sufficient for a full current Evidence Lake build.
The compiler must fail preflight rather than depend on optimistic cleanup while
a build is running.

## Growth Preflight

Before a full build:

1. Refresh the Evidence Lake storage audit.
2. Sum retained immutable inputs, the largest expected new generation, the
   measured phase spill/staging peak, atomic replacement overlap, and the
   configured free-space floor.
3. Confirm that scratch and output filesystems have independent headroom where
   they are separate.
4. Identify the exact rollback and published artifacts that retention must
   protect.
5. Do not count a cold NFS archive as local compiler scratch or database
   working space.

The current measured E5 scratch floor is 160,832,151,552 bytes plus operating
headroom. This is a historical lower bound for the pinned July 2026 release,
not a permanent maximum. Source releases, enabled adapters, retained products,
and compiler algorithms can increase it. Every major build report must publish
per-phase wall time, CPU time, peak RSS, durable output, and peak spill so this
document can be revised from measurements.

Measurement sources on Photon:

- `/data/spacegate/state/reports/evidence_lake_v2/e0_storage_audit_2026-07-24_post_cleanup.json`
- `/data/spacegate/state/reports/evidence_lake_v2/e5_selected_fact_policy_v12_compile_timing.json`
- `/data/spacegate/state/reports/evidence_lake_v2/e5_selected_fact_policy_v12_reproduction_timing.json`
- the accepted E4 release-set manifest and its 36 pinned artifact manifests
- allocated-byte scans of the pinned raw, typed, selected-fact, and served
  generation roots named in the accepted E7 build reports

## Storage Roles

- Fast internal NVMe: active raw/typed inputs, compiler databases, current
  candidates, hot reports, and served runtime artifacts.
- Fast local bulk storage: disposable compiler spill and large active research
  products when its reliability is acceptable for regenerable data.
- Cold/archive storage: verified immutable superseded generations and
  re-fetchable research material. Archive transfer requires content manifests,
  destination verification, atomic publication, and a separate reviewed local
  retirement.

See `docs/RETENTION.md` for the authoritative protection and retirement rules.
