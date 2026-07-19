# Retention Policy

This policy governs Spacegate build artifacts in `$SPACEGATE_STATE_DIR`.

## Scope

- `out/<build_id>/` immutable build artifacts (`core.duckdb`, `arm.duckdb`, parquet, disc outputs, `map_tiles/`)
- `reports/<build_id>/` per-build reports

Out of scope (never pruned by retention script):

- `raw/` catalog downloads
- `cooked/` normalized catalog exports
- `reports/manifests/` source manifests
- bounded runtime caches such as WISE image previews and simulation scenes; these have their own cache
  enforcement because they are mutable runtime artifacts, not immutable builds

Published bootstrap metadata may reference checksummed build reports. The
bootstrapper installs only bounded relative JSON paths under
`reports/<build_id>/`, verifies byte counts and SHA-256 digests, and stages the
directory before promotion. Published report directories follow the same
retention protection as local build reports; do not remove reports referenced
by the active `dl/current.json`.

Derived public-slice and side builds preserve verification lineage through
`derived_build_verification_report.json`. Retain the report with its build; it
contains the hashes and reported build IDs of the applicable upstream reports,
while strict verification recomputes its slice-native counts and integrity
checks against the derived database.

Simulation-scene runtime artifacts live under
`$SPACEGATE_STATE_DIR/cache/simulation_scenes/<build_id>/`. They are fully
regenerable presentation products and must never be treated as science inputs.
Each artifact carries a materializer contract version. The API must reject
older semantic versions after scene classification, naming, membership, or
evidence-precedence behavior changes; regenerate the bounded priority set with
the current materializer instead of treating stale presentation output as
compatible. Side-build materialization may reuse a scene only when both its
contract version and embedded target build ID match; a copied scene from the
source build must be regenerated even when its schema version is current.
The API opportunistically prunes oldest artifacts to a 2 GiB default cap;
operators may set `SPACEGATE_SIMULATION_SCENE_CACHE_LIMIT_BYTES`.
The Admin `Warm Simulation Scenes` action writes to this runtime-cache location,
including a regenerable `materialization_report.json`; it must never target the
served symlink or an immutable build directory after promotion. Deferred scene
warming does not make a build incomplete and is not promotion evidence.
Keep the served build's directory during normal operation; directories for
build IDs no longer retained in `out/` or referenced by `served/` may be pruned
as a separate cache cleanup after promotion verification.

## Default Policy

- Keep the currently served build (`served/current`) regardless of age.
- On Photon, keep newest 12 build directories in `out/`.
- On Photon, keep newest 24 per-build report directories in `reports/`.
- On smaller hosts, use at least newest 6 build directories and newest 12 report
  directories unless disk pressure requires tighter local overrides.
- Remove stale temporary ingest paths (`out/*.tmp`).
- Build/report directory detection accepts both dashed build IDs
  (`YYYY-MM-DDT...`) and compact build IDs (`YYYYMMDDT...`), with either
  minute-resolution (`HHMMZ`) or second-resolution (`HHMMSSZ`) timestamps.

## Scripted Cleanup

Use:

```bash
scripts/prune_state_retention.sh
```

This is dry-run by default. To apply:

```bash
scripts/prune_state_retention.sh --apply
```

Admin v2 exposes retention controls in the Builds workspace:

- **Retention Dry Run** runs this script without `--apply`, records an
  auditable job log, and shows the parsed candidate plan in the Builds page.
- **Retention Apply** is a guarded high-risk action. It requires a matching
  successful dry-run from the last 6 hours, an unchanged candidate hash, and the
  confirmation phrase. It deletes only the exact candidate directories from the
  checked plan. `raw/`, `cooked/`, and `served/current` are protected.
  After a matching dry-run succeeds, the Builds page updates the apply card with
  the dry-run job id, candidate count, estimated reclaimable space, and candidate
  hash.
- If a dry run finds zero candidates, that is a successful null result. Admin
  should report zero reclaimable bytes and leave Retention Apply disabled
  because there is nothing to delete.

Useful overrides:

```bash
scripts/prune_state_retention.sh --keep-builds 8 --keep-reports 16 --apply
scripts/prune_state_retention.sh --no-prune-tmp
```

Photon's current generous default:

```bash
scripts/prune_state_retention.sh --keep-builds 12 --keep-reports 24 --apply
```

Before applying retention, preserve every published, rollback, or otherwise
referenced build reported by the Evidence Lake storage audit. Supply the set
explicitly so an unserved but required checkpoint cannot be selected merely by
age:

```bash
jq -r '.build_references | keys[]' \
  /data/spacegate/state/reports/evidence_lake_v2/e0_storage_audit.json \
  > /data/spacegate/state/reports/evidence_lake_v2/protected_builds.txt

SPACEGATE_STATE_DIR=/data/spacegate/state \
  scripts/prune_state_retention.sh \
    --keep-builds 12 \
    --keep-reports 24 \
    --protect-file /data/spacegate/state/reports/evidence_lake_v2/protected_builds.txt
```

Review that dry run before adding `--apply`. The script always protects
`served/current`; `--protect-build` and `--protect-file` extend that set for
metadata references the retention script cannot safely infer on its own.

Run retention only after successful promotion and verification. Do not run it
during ingest or while diagnosing a failed build.

Evidence Lake v2 raw and typed snapshots are not `out/` retention candidates.
Keep the active raw snapshot and active parser-contract snapshot for every
registered source, plus any snapshot referenced by a build, report, publication,
rollback, or adjudication packet. Superseded parser outputs may be proposed for
retirement only after the replacement passes
`scripts/verify_evidence_lake_reproduction.py` and E7 records that no retained
lineage references them. Never prune individual Parquet files from inside a
typed snapshot. After E2's forward/reverse Gaia release evidence, the active
estate is 63 raw artifacts and 5,213,454,799 typed Parquet bytes across 27
source releases; older immutable parser checkpoints explain why the physical
typed directory is larger until E7 retirement.

Evidence Lake identity graphs under
`derived/evidence_lake_v2/identity/<graph_id>/` are immutable compiler
artifacts. Preserve the graph named by the adjacent atomic `current` pointer and
any graph referenced by a build, report, publication, rollback, or adjudication
packet. Never prune individual graph Parquet files or its DuckDB inspection
database independently. A superseded, unserved compiler iteration may be
removed only after the replacement passes
`scripts/verify_evidence_identity_reproduction.py` and the machine report no
longer references it. Clean reproduction graphs belong under `tmp/`; remove the
whole scratch graph only after the comparison report is durably written.

Scientific evidence compiler artifacts under
`derived/evidence_lake_v2/scientific_evidence/<build_id>/` are also immutable
units. Preserve any artifact named by a current pointer or referenced by a
shadow/served build, report, publication, rollback, or adjudication packet.
Never remove `scientific_evidence.duckdb` independently from its manifest. An
in-progress compiler checkpoint may be retired only as a whole after its
replacement has a durable logical-hash comparison; accepted E4/E5 inputs remain
protected until E7 records that no retained projection references them. Clean
reproduction uses `scripts/verify_scientific_evidence_reproduction.py`; its
scratch tree is removed only after comparison and the durable report is written.

The field-complete NASA checkpoint `cb82c09179afa740b02e2cdf` is approximately
4.2 GiB (`4,497,354,752` database bytes) and is protected as the current E4
reference. Earlier content-addressed experiments and interrupted hidden
temporary directories are cleanup candidates only through a retention dry-run;
do not remove the current reference, a reproduction input, or a rollback build.

The July 19 dry-run against `/data/spacegate/state` identified eight old build
trees and twelve old report trees with an estimated 148.86 GiB reclaimable. No
cleanup was applied: the candidates include recent canonical/public/side builds
and require served, rollback, publication, and reference confirmation first.
Evidence Lake raw/typed snapshots and the current E4 reference were not listed.

Side-artifact rebuilds created by `scripts/rebuild_side_artifacts.py` are normal
immutable `out/<build_id>/` artifacts once the `.tmp` directory is renamed into
place. Interrupted runs leave `out/<build_id>.tmp`, which is covered by the
default stale temporary ingest cleanup. The script intentionally copies only
known served artifacts and does not copy orphaned internal DuckDB temp
directories such as `core.duckdb.tmp`.
Presentation-only side builds should use `--preserve-arm` so adding immutable
map artifacts cannot change ARM evidence or expose core/ARM search-contract
drift. ARM regeneration remains the default for science-side changes.

### Tiled map artifacts

`map_tiles/` is part of the immutable served build. Content-addressed tile files
must not be pruned independently, even when the same hash occurs in another
build. The current index and radius manifests can reference any hash contained
in their own build directory. Retire tiles only by retiring the complete,
unserved `out/<build_id>/` through the normal retention dry run. Temporary tile
builds belong under `$SPACEGATE_STATE_DIR/tmp` and may be removed only after a
verified immutable build has been promoted and retained.

## Bulk Research Storage

Large research/document material should not live under repo paths or inside
`out/` build artifacts.

Photon default bulk root:

```bash
SPACEGATE_BULK_DIR=/mnt/space/spacegate
```

Use this root for:

- archived papers and source documents
- retrieved HTML/PDF/source pages
- OCR/intermediate text
- large object-dossier attachments
- reusable science-document caches
- one-off model/eval research bundles that are too large for git or reports

Recommended layout:

```text
/mnt/space/spacegate/
  research/
    sources/
    papers/
    ocr/
    dossiers/
    eval-runs/
  cache/
    source-documents/
    literature-indexes/
```

Container deployments should bind-mount `$SPACEGATE_BULK_DIR` into the API
container at the same path. If this mount is absent, Admin Runtime will report
the bulk root as missing from the container even when the host USB filesystem is
mounted correctly.

The USB-backed `/mnt/space` drive is large and fast but less trustworthy than
internal NVMe. Anything required for auditability must have durable metadata in
Spacegate state or generated databases:

- canonical URL
- source domain and trust tier
- retrieval timestamp
- content hash/checksum
- local cache path
- transform/prompt/model versions where applicable

If `/mnt/space` content is lost, Spacegate should be able to identify missing
attachments and re-fetch or mark dossiers stale from metadata.

### TESS source snapshots

Targeted TESS acquisition preserves raw inputs under
`$SPACEGATE_STATE_DIR/raw/tess_evidence/snapshots/<snapshot_id>/`. Snapshot IDs
are content-addressed across TOI, target-set, TIC, Gaia-neighborhood,
external-crossmatch, and targeted Gaia DR3 payloads. Multipart requests include
query-hash sidecars so interrupted-run resume cannot reuse a chunk for a
different target set.

- raw TESS snapshots are protected source evidence, not ordinary build
  retention candidates
- identical content reuses the same snapshot ID
- do not prune a snapshot referenced by a retained build manifest or source-
  delta history
- any future cleanup begins with a reference-counted dry run; directory age is
  not sufficient evidence for deletion

## Docker and Model Storage

Docker data and model caches are intentionally outside the Spacegate build
retention script:

- Docker data root: `/data/docker`
- model weights/caches: `/data/models`

Clean Docker image/container/build-cache slag with Docker-native tools after
checking active containers. Do not teach `scripts/prune_state_retention.sh` to
delete Docker or model data.

## Operational Notes

- Admin/API Docker jobs are expected to run as the host operator UID/GID via
  `scripts/compose_spacegate.sh`, with `SPACEGATE_UMASK=0002` by default. This
  prevents new generated state from becoming root-owned on the host.
- If older artifacts were created by root-owned container processes, normalize
  generated/admin state before cleanup:

```bash
scripts/normalize_state_permissions.sh
sudo scripts/normalize_state_permissions.sh --apply
```

- The permission normalizer is dry-run by default and avoids `raw/` and
  `cooked/`. It tightens the state root itself non-recursively, then repairs
  generated/admin subtrees.
- Production deploys grant a narrow ACL for the API runtime UID on served-build
  presentation artifacts before deploy-time presentation checks so uploaded
  build artifacts remain writable for coolness and snapshot outputs.
- Edge bootstrap from a local published `file://` archive reads the bounded
  download artifact in place instead of duplicating it under
  `cache/downloads/`. Published immutable builds with verified DISC outputs are
  promoted with `bootstrap_core_db.sh --skip-auto-score`; do not mutate them by
  re-running coolness scoring during activation.
- Run retention after successful promotion/verification, not during ingest.
- If large one-off caches (for example external catalog mirrors) are stored under the state root, move them outside `out/` and `reports/` so retention remains deterministic.
- Failed builds may be kept temporarily for diagnosis, but once the root cause is
  captured in a report or issue, remove them through the retention script rather
  than manual edits inside immutable build directories.

## Extended-Object Catalog Artifacts

Extended-object raw snapshots under `raw/extended_objects`, cooked CSVs under
`cooked/extended_objects`, and `reports/manifests/extended_objects_manifest.json`
are reproducibility inputs, not disposable build output. Normal build-retention
pruning must not remove them. Failed `out/<build_id>` and per-build reports use
the standard retention policy after diagnostic evidence has been captured.

## WISE Image Cache

WISE/IRSA image previews are runtime cache products, not build artifacts and
not repo files.

Default location:

```bash
$SPACEGATE_STATE_DIR/cache/wise_images
```

Default cap:

```bash
SPACEGATE_WISE_IMAGE_CACHE_LIMIT_BYTES=4294967296
```

Optional bulk-storage mode:

```bash
SPACEGATE_WISE_IMAGE_CACHE_PREFER_BULK=1
```

When bulk mode is enabled and `/mnt/space/spacegate` is mounted in the API
container, the default cache root becomes:

```text
/mnt/space/spacegate/cache/wise_images
```

Operators may set `SPACEGATE_WISE_IMAGE_CACHE_DIR` to an explicit path. A larger
cap, such as 20-50 GB, is reasonable on `/mnt/space/spacegate` if the mount is
healthy and the cache is treated as re-fetchable.

The API enforces the cap opportunistically when WISE metadata/previews are
requested. It removes oldest cached files first. Cache metadata must retain IRSA
source URLs, retrieval timestamp, bands, cutout size, and attribution so lost
previews can be regenerated or marked stale.
