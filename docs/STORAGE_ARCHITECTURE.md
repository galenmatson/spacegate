# Storage Architecture

Spacegate separates active scientific work, hot runtime state, regenerable bulk
data, and cold archives. A path is not interchangeable merely because it has
free space. Database and compiler workloads depend heavily on latency and small
I/O, while immutable archives primarily need capacity and sequential throughput.

## Photon Storage Tiers

| Tier | Path | Device | Intended role |
| --- | --- | --- | --- |
| System | `/` | PM9A3 enterprise NVMe logical volume | Repository, virtual environments, host software, and small operational files |
| Hot state | `/data/spacegate` | PM9A3 enterprise NVMe logical volume | Raw and typed evidence, active state, served builds, reports, and runtime databases |
| Active bulk | `/space/spacegate` | Samsung 990 EVO Plus 4 TB internal NVMe | Compiler generations, spill, reproductions, observation caches, dossiers, and large active research products |
| Legacy bulk | `/mnt/space/spacegate` | Samsung T31 USB SSD | Migration source and optional bounded sequential secondary storage; not a default compiler or evidence path |
| Cold archive | `/mnt/proton/spacegate-archive/v1` | Proton NVMe over dedicated 2.5 GbE NFS | Verified immutable superseded generations only |

Host compiler scripts use `SPACEGATE_BULK_DIR`, defaulting to
`/space/spacegate`. Runtime containers receive the same path through Compose.
`SPACEGATE_COLD_ARCHIVE_DIR` may override the Proton archive root for explicit
archive operations. Neither variable changes scientific identity or build
lineage.

## August 10, 2026 Measurements

The bounded Photon benchmark used 4 GiB direct sequential transfers, 256 MiB
of 4 KiB direct I/O on local devices, a bounded 16 MiB 4 KiB test on NFS, and
10,000-file metadata operations. Results are operational comparisons, not
vendor specifications or a substitute for application benchmarks.

| Path | Sequential write | Sequential read | 4 KiB write | 4 KiB read | Create 10k files | Remove 10k files |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `/data` | 2,038 MiB/s | 3,724 MiB/s | 168 MiB/s | 203 MiB/s | 0.25 s | 0.10 s |
| `/space` | 2,885 MiB/s | 3,385 MiB/s | 203 MiB/s | 162 MiB/s | 0.25 s | 0.10 s |
| `/mnt/space` | 553 MiB/s | 824 MiB/s | 56 MiB/s | not retained | 0.25 s | 0.11 s |
| `/mnt/proton` | 198 MiB/s | 268 MiB/s | 2.13 MiB/s | 21.3 MiB/s | 18.24 s | 15.60 s |

The internal NVMe devices are effectively peers for Spacegate compilation.
`/space` has the larger capacity and the best measured sequential write rate,
so it is the active bulk tier. The USB SSD is useful for large sequential
copies but is a poor default for repeated compiler I/O. Proton nearly saturates
the direct network for large transfers but its metadata and small write cost
make it unsuitable for DuckDB, SQLite, Parquet compilation, cache churn, or
container runtime state.

Machine reports live under
`$SPACEGATE_STATE_DIR/reports/storage_housekeeping/20260810/`.

## Placement Rules

- Keep source evidence under `$SPACEGATE_STATE_DIR/raw` and source-native typed
  products under `$SPACEGATE_STATE_DIR/typed`. They are not ordinary cleanup
  candidates.
- Keep the served pointer, current public build, immediate rollback, published
  references, identity products, and release-set members on protected paths.
- Put large active, regenerable compiler products under
  `$SPACEGATE_BULK_DIR`.
- Put survey previews, source documents, dossiers, and observation-product
  caches under the bulk tier when their cache contract permits it. Preserve
  URLs, checksums, release identity, and lineage in durable state.
- Use Proton only through the manifest-first archive workflow. Never make an
  active runtime or compiler silently depend on NFS.
- Do not split a coherent database across devices to chase capacity. Move whole
  immutable generations or whole cache families with a manifest and atomic
  pointer change.

## Capacity and Cleanup Gates

Before a major build, record free bytes on `/`, `/data`, and `/space`; retained
generation sizes; estimated new output; peak spill; atomic replacement overlap;
and the required reserve. A full Evidence Lake build should start with at least
1 TiB of combined fast free capacity and should retain 200 GiB of operational
headroom across the active filesystems after the predicted peak. These are
conservative operational gates, not estimates of final artifact size.

Cleanup is fail closed:

1. Inventory a complete generation or explicitly named interrupted tree.
2. Prove it is not served, current, published, rollback, evidence-set referenced,
   linked, shared, or open by a process.
3. Emit an exact candidate hash and reclaimable byte count.
4. Review the dry run.
5. Apply only against the same candidate hash and retain the report.

Docker BuildKit cache is regenerable and may be pruned independently of images
and running containers. Raw evidence, typed releases, unique observation
products, and archive manifests are never generic age-based cleanup targets.

Photon's Docker 29 containerd image store needs separate accounting. Docker
reports `/data/docker` as `DockerRootDir`, but active snapshot layers resolve
through `/var/lib/containerd` and consume the root filesystem. At this
checkpoint the retained image set is about 34 GB, of which about 30 GB is the
active vLLM image and 3.45 GB is the browser-test image. Do not infer root usage
from `du` without privileges; use `docker system df` as well. Moving the active
containerd store is a host maintenance operation with service downtime, not a
Spacegate retention shortcut.
