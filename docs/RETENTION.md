# Retention Policy

This policy governs Spacegate build artifacts in `$SPACEGATE_STATE_DIR`.

## Scope

- `out/<build_id>/` immutable build artifacts (`core.duckdb`, `arm.duckdb`, parquet, disc outputs)
- `reports/<build_id>/` per-build reports

Out of scope (never pruned by retention script):

- `raw/` catalog downloads
- `cooked/` normalized catalog exports
- `reports/manifests/` source manifests

## Default Policy

- Keep the currently served build (`served/current`) regardless of age.
- On Photon, keep newest 12 build directories in `out/`.
- On Photon, keep newest 24 per-build report directories in `reports/`.
- On smaller hosts, use at least newest 6 build directories and newest 12 report
  directories unless disk pressure requires tighter local overrides.
- Remove stale temporary ingest paths (`out/*.tmp`).
- Build/report directory detection accepts both dashed build IDs
  (`YYYY-MM-DDT...`) and compact build IDs (`YYYYMMDDT...`).

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

Useful overrides:

```bash
scripts/prune_state_retention.sh --keep-builds 8 --keep-reports 16 --apply
scripts/prune_state_retention.sh --no-prune-tmp
```

Photon's current generous default:

```bash
scripts/prune_state_retention.sh --keep-builds 12 --keep-reports 24 --apply
```

Run retention only after successful promotion and verification. Do not run it
during ingest or while diagnosing a failed build.

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

## Docker and Model Storage

Docker data and model caches are intentionally outside the Spacegate build
retention script:

- Docker data root: `/data/docker`
- model weights/caches: `/data/models`

Clean Docker image/container/build-cache slag with Docker-native tools after
checking active containers. Do not teach `scripts/prune_state_retention.sh` to
delete Docker or model data.

## Operational Notes

- If artifacts were created by root-owned container processes, run cleanup with appropriate permissions.
- Run retention after successful promotion/verification, not during ingest.
- If large one-off caches (for example external catalog mirrors) are stored under the state root, move them outside `out/` and `reports/` so retention remains deterministic.
- Failed builds may be kept temporarily for diagnosis, but once the root cause is
  captured in a report or issue, remove them through the retention script rather
  than manual edits inside immutable build directories.
