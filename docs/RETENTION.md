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

The July 19 E4 wide-binary compiler work produced bounded checkpoint
`aaf262b1791d98ce3e9f96e7` (11,129,073,664 bytes) and several hidden temporary
scientific-evidence directories from failed or deliberately interrupted
diagnostic builds. Never delete those directories manually. General state
retention remains prohibited while E3 acquisition or E4 verification is active.
When a stopped compiler diagnostic itself creates material storage pressure, the
only allowed narrower exception is the explicit fail-closed command below. Its
interrupted-build mode accepts direct hidden children only and refuses
manifests, symlinks, shared files, and live file descriptors. A separate
independent-audit mode accepts a manifest-bearing 24-hex build only when the
external artifact audit has `status=fail`, identifies the same build and
database, contains at least one nonzero check, and the database still matches
the immutable manifest checksum. Both modes require a reviewed dry-run
candidate hash before whole-directory removal:

```bash
.venv/bin/python scripts/prune_evidence_lake_artifacts.py \
  --state-dir /data/spacegate/state \
  --candidate .<build-id>.<temporary-suffix> \
  --reason '<specific reproducible failure and replacement evidence>' \
  --report /data/spacegate/state/reports/evidence_lake_v2/e4_retention_dry_run.json

.venv/bin/python scripts/prune_evidence_lake_artifacts.py \
  --state-dir /data/spacegate/state \
  --candidate .<build-id>.<temporary-suffix> \
  --reason '<same reviewed reason>' \
  --expected-candidate-set-sha256 '<exact dry-run hash>' \
  --apply \
  --report /data/spacegate/state/reports/evidence_lake_v2/e4_retention_applied.json
```

For an immutable diagnostic that failed independent audit, substitute:

```bash
.venv/bin/python scripts/prune_evidence_lake_artifacts.py \
  --state-dir /data/spacegate/state \
  --failed-audit '<build-id>=/absolute/path/to/artifact_audit.json' \
  --reason '<specific audit failure and preserved reproduction inputs>' \
  --report /data/spacegate/state/reports/evidence_lake_v2/e4_failed_build_dry_run.json
```

Apply still requires the exact dry-run `candidate_set_sha256`. The durable
compiler and audit reports remain outside the retired artifact and preserve its
identity, metrics, checksum, and failure reason.

The minimum age is 60 minutes by default. Reducing it is an explicit operator
decision and does not weaken the no-live-process, no-manifest, or whole-artifact
gates. Accepted, served, published, rollback, and merely superseded artifacts,
source snapshots, typed snapshots, identity graphs, and individual files inside
an artifact remain outside this command's scope.

On July 19, the reviewed dry run
`e4_simbad_failed_artifact_retention_dry_run.json` identified exactly two closed,
manifest-less SIMBAD compiler diagnostics affected by the confirmed
199,495,267,914-row disjunctive citation-join plan. Candidate-set hash
`39ba9c0104621131a5c4b1673bda9a086f6689e7491ec6b7c85467e14fa0eece`
authorized the applied report
`e4_simbad_failed_artifact_retention_applied.json`, reclaiming
73,183,408,128 allocated bytes. Immutable raw and typed SIMBAD inputs were not
changed. Compiler v36 removed the disjunctive join but failed a later identity
audit; v37 retains bounded equality matching and explicitly quarantines failed
identifier normalizations.

The complete-envelope v37 retry then failed closed at the configured 16-GB
DuckDB limit while expanding bundled astrometry citations in one operation. Its
manifestless temporary had no live file descriptors and contained only the
compiler database. Under storage pressure, the explicit zero-age dry run
`e4_simbad_v37_failed_temporary_retention_dry_run.json` selected only that tree
with candidate hash
`0dc54bd5a607cff1da7f5315ea147a5075f6389c6cb28e29a2523215fca23204`.
The matching applied report reclaimed 36,925,886,464 allocated bytes. Raw/typed
inputs, failure logs, immutable v36, and the active v38 replacement remained
protected.

The v38 replacement then passed independent artifact audit and clean logical-
hash reproduction as build `fc5bd4e6398d72bde50ba6d5`. Only after those
reports were durable did
`e4_simbad_v36_failed_artifact_retention_dry_run.json` select immutable v36
build `07230826efefffce913a3569`, whose independent audit failed exactly 285
blank identifier claims. Candidate hash
`de47f05ca412b29f501f0eb1ee7e23b3be2327f7d5834de7f7114fe1f96af8f5`
authorized the matching applied report and reclaimed 42,799,505,408 allocated
bytes. The raw and typed snapshots, accepted v38 artifact, audit, reproduction,
compiler, and retention reports remain protected.

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

GCVS typed snapshot `ef540a47c43892e17ddc2bae` supersedes parser-v1 snapshot
`85d1e49a6ce11402c4d06a5f` for downstream work. The immutable raw snapshot is
unchanged, and the typed A/B plus clean-reproduction reports are durable. Keep
both typed snapshots until the combined E4/E5 input set has been verified; the
GCVS E4 adapter now passes, but this checkpoint authorizes no typed-snapshot
cleanup. Accepted scientific-evidence checkpoint `a6f6669d2bd48eac5d6204d2`
and its compile, artifact-audit, source/scope-audit, and clean-reproduction
reports are protected E4/E5 inputs. Provisional GCVS compiler artifacts may be
retired only as whole immutable units through the retention dry-run after this
accepted replacement and its reports are durable.
The source-specific GCVS scientific audit is an explicitly allowlisted failed-
artifact audit contract; arbitrary audit schemas cannot authorize cleanup. On
July 19, candidate-set hash
`d4d63bcd3f16cea22353667725c5e1ec2bb27c6a29854f1b05c2f07ebac21ca5`
authorized whole-artifact removal of independently failed provisional builds
`4d5c71f6e7e537f4c7d56693` and `41c15d3394bba2eef8d278a5`, reclaiming
1,855,528,960 allocated bytes. Their audit and retention reports remain; the
accepted checkpoint and every raw/typed input remain protected.

Large local compiles may set `SPACEGATE_E4_TEMP_DIRECTORY` to an operator-owned
scratch root such as `/mnt/space/spacegate/tmp/evidence_lake_v2/e4_spill`.
DuckDB spill there is disposable execution state, never an evidence artifact;
the compiler assigns a build-specific directory and removes it after closing the
database. A spill tree left by a crash must still be associated with a dead
process and explicit failed build before whole-tree cleanup. Never direct this
setting into raw, typed, served, rollback, or published artifact storage.

The field-complete NASA checkpoint `cb82c09179afa740b02e2cdf` is approximately
4.2 GiB (`4,497,354,752` database bytes) and is protected as the current E4
reference. Earlier content-addressed experiments and interrupted hidden
temporary directories are cleanup candidates only through a retention dry-run;
do not remove the current reference, a reproduction input, or a rollback build.
Complete-envelope SIMBAD checkpoint `fc5bd4e6398d72bde50ba6d5`, its immutable
raw/typed inputs, independent audit, and clean-reproduction report are protected
inputs to E4/E5.
Official WGSN checkpoint `0ff30b04008b93aafb3de66f`, raw snapshot
`ec563be8ca8038acd3cfe78e`, typed snapshot `437e2f27863efe5adc0423ab`,
and its compile, artifact-audit, scope-audit, and clean-reproduction reports are
also protected E4/E5 inputs.
Official GCVS checkpoint `a6f6669d2bd48eac5d6204d2`, raw and typed snapshot
`ef540a47c43892e17ddc2bae`, and its compile, artifact-audit, source/scope-audit,
and clean-reproduction reports are likewise protected E4/E5 inputs.
Hunt/Reffert checkpoint `7e66e0690aa962c837d43a86`, complete raw and typed
snapshot `cbfa7c6ec8c2e3bfbc226898`, and its compile, artifact-audit,
cluster/scope-audit, and clean-reproduction reports are protected E4/E5 inputs.
Extended-catalog checkpoint `54d1b0b6a841344c48327991`, complete raw and typed
snapshot `7753816661175edcb526c676`, and its compile, artifact-audit,
extended-scope-audit, and clean-reproduction reports are protected E4/E5 inputs.
MSC checkpoint `fc7e9dcabb0b27167c8f188c`, raw snapshot
`028096033a6805e740df3b66`, typed snapshot `d04364cc77130406a257dc89`,
and its compile, artifact-audit, MSC source/scope-audit, and clean-reproduction
reports are protected E4/E5 inputs. The MSC scientific audit is an explicitly
allowlisted failed-artifact audit contract; it does not authorize removal of
this accepted checkpoint or either source snapshot.
WDS checkpoint `ad98d4e369c5a0addc6477a0`, WDS raw snapshot
`9c84c16b098fc5339dbf0f98`, WDS typed snapshot
`d0c9fefb3acb5b2cab3d75f8`, CDS bridge raw snapshot
`d089e992715ba33e2c3c04cc`, bridge typed snapshot
`f9f48b3f691a6a38138ce5d7`, and its compile, artifact-audit, WDS source/scope-
audit, and clean-reproduction reports are protected E4/E5 inputs. The WDS
scientific audit is an explicitly allowlisted failed-artifact audit contract;
it does not authorize removal of this accepted checkpoint or source snapshots.

The E4 source checkpoints `aaf262b1791d98ce3e9f96e7` (bounded wide binary),
`fcbb6466bea0a7798ae8d2ed` (ORB6), and `b3a141c0caf953aa83c4e52b`
(DEBCat), `d08c5aa9af7dc8bcdbf0d6c3` (Green SNR), and
`255678b2daa6e8bf46e6dcd9` (TESS EB) remain protected inputs to the combined
E4/E5 build. White-dwarf checkpoint `486e4975af015d4e5f5a3c9b` and its
clean-reproduction report are likewise protected. ATNF checkpoint
`64c55c19a5a10a88877d4cd2`, its pinned raw/typed package, and its audit and
clean-reproduction reports are also protected. McGill magnetar checkpoint
`c599c951590451ace4248934`, its pinned `TabO1.csv` raw/typed snapshots, and its
audit and clean-reproduction reports are likewise protected. SB9 checkpoint
`72663823963198c8fcbbe569`, all four pinned source tables, and its audit and
clean-reproduction reports are protected. SBX checkpoint
`37ffa7255d026c8d930af6d4`, complete raw/typed snapshot
`ea236790d0501967b3c30466`, and its audit and clean-reproduction reports are
protected. The served legacy SBX projection remains protected as an E6 A/B
input. Earlier SBX scientific-evidence artifact
`c0b729ea1ec32ade4548a7b7` predates the accepted component-designation
normalization; `da07cc5c9fbf36faa314d98f` and
`3f24effe31db3258870e48cb` predate complete explicit registry coverage for the
preserved legacy projections. All are retention dry-run candidates,
not manual deletion targets. Earlier SB9 compiler artifacts
`5a216d394e0773f6175ff226` and `407c81af12127feb9cf61048` predate the accepted
endpoint-scope/cross-table-link contract and are retention dry-run candidates,
not manual deletion targets. Earlier
source-specific compiler iterations may be proposed only by a retention
dry-run after the replacement's clean-reproduction report is durable; never
manually remove a hidden interrupted compiler directory.

The white-dwarf adapter's first v21 attempt failed closed before promotion when
an unqualified source column collided with compiler lineage `source_id`. Its
hidden temporary artifact remains a retention dry-run candidate; the v22
replacement qualifies source expressions generally and reproduces cleanly.

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
