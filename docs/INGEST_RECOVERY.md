# Ingest Recovery + Runtime Tuning

## Authority Status

The commands in the legacy sections below reproduce and recover the retained
stability build. They are not the authoritative scientific refresh path for
Evidence Lake v2. In particular, do not use `scripts/refresh_core.sh`,
`scripts/ingest_core.py`, or `scripts/build_arm.py` to incorporate a new source
release after the Evidence Lake cutover.

The release-scoped path pins raw and typed releases, compiles domain evidence,
resolves identity and scope, selects facts through per-quantity policy, and
builds clean CORE/ARM/DISC/public projections. The E7 checkpoint is described
by:

- `docs/EVIDENCE_LAKE_V2.md`;
- `config/evidence_lake/e0_e7_acceptance.json`;
- `config/evidence_lake/e7_timed_pipeline.json`; and
- `scripts/run_e7_timed_pipeline.py`.

Use the timed runner's default `--mode verify` to recover context and validate
the pinned clean artifacts without recompiling them:

```bash
.venv/bin/python scripts/run_e7_timed_pipeline.py --mode verify
```

`--mode full` is an explicit, expensive clean compile. It writes immutable,
content-addressed artifacts and records per-stage timing; it does not promote,
deploy, mutate proton, or change `served/current`. Missing or corrupt artifacts
must fail visibly. Promotion remains a separate operator-approved action after
scientific review, retention preflight, and the local rollback drill.

## Legacy Differential Refresh Entry Point

Use `scripts/refresh_core.sh` only to reproduce or recover the retained
pre-Evidence-Lake stability path.

Default behavior:

1. run download + source-delta scan
2. run impacted-row planner
3. route automatically:
   - `planet_incremental_eligible`: selective cook + incremental planet ingest
   - otherwise: full cook + full ingest
4. promote + verify resulting build

Common flags:

- `--skip-download`: reuse existing manifests/delta report
- `--full`: force full rebuild path
- `--download-overwrite`: force source redownloads

Generated planning artifact:

- `reports/impacted_rows_plan.json`

## Identifier Collision Gates

`ingest_core.py` enforces identifier stewardship QC gates after table construction:

- `SPACEGATE_ATHYG_MERGE_AMBIGUOUS_LIMIT` (default `10000`)
- `SPACEGATE_ATHYG_MERGE_GAIA_COLLISION_MAX` (default `0`)
- `SPACEGATE_ATHYG_MERGE_HIP_COLLISION_MAX` (default `3000`)
- `SPACEGATE_ATHYG_MERGE_HD_COLLISION_MAX` (default `3000`)

Collision count means:
- the number of normalized IDs in a namespace that point to more than one target object (`count(distinct target_id) > 1`).
- This is a quality/stewardship guardrail, not a hard database constraint.

## Runtime Visibility

`scripts/ingest_core.sh` now emits periodic heartbeats while ingest runs:

- interval env: `SPACEGATE_INGEST_HEARTBEAT_S` (default `120`, minimum `15`)
- heartbeat message includes elapsed time and the most recent non-heartbeat stage line.

This provides live progress during long silent phases (`stars`, `alias tables`, etc.).

## Recovery Checkpoint Behavior

For Gaia-heavy ingest, `scripts/ingest_core.sh` defaults `SPACEGATE_KEEP_TMP=1` (if unset), so failed temporary builds are retained:

- temp path: `$SPACEGATE_STATE_DIR/out/<build_id>.tmp`
- reports path: `$SPACEGATE_STATE_DIR/reports/<build_id>/...`

This avoids losing expensive intermediate artifacts.

Incremental checkpoint behavior:

- planet/lifecycle-only refreshes clone the current build into `out/<build_id>.tmp`
- only `planets` and lifecycle side tables are rebuilt
- unchanged star/system artifacts remain inherited from parent build
- successful finalize promotes `out/<build_id>.tmp` to `out/<build_id>`

## Legacy Finalize From Temp Build

If ingest fails after core tables are already built (for example, QC gate tuning), recover without recomputing heavy stages:

```bash
scripts/finalize_ingest_tmp.sh --build-id <build_id>
```

The finalize helper:

1. Re-runs QC identifier gates with current env thresholds.
2. Writes Parquet exports from existing temp `core.duckdb`.
3. Builds `arm.duckdb`.
4. Promotes `<build_id>.tmp` to `<build_id>`.

## Legacy Resource Tuning Notes (Proton)

These measurements document the former Proton path. Do not mutate Proton while
performing Evidence Lake work. Photon compiler measurements and optimization
decisions belong in `docs/E7_BUILD_PERFORMANCE_2026-07-22.md`.

Useful env knobs before ingest:

- `SPACEGATE_DUCKDB_THREADS`
- `SPACEGATE_DUCKDB_MEMORY_LIMIT`

Observed stable setting on Proton for the Gaia-first core:

- `SPACEGATE_DUCKDB_THREADS=8`
- `SPACEGATE_DUCKDB_MEMORY_LIMIT=26GB`

If OOM appears during identifier merge or alias stage, reduce threads first, then adjust memory.

## Legacy Benchmark Matrix Runner

Use the matrix runner to measure ingest wall-clock and per-stage durations across thread/memory combinations:

```bash
scripts/benchmark_ingest_tuning.sh --threads 8,10,12 --memory 26GB,28GB,30GB
```

Outputs are written to:

- `$SPACEGATE_STATE_DIR/reports/benchmarks/<timestamp>/summary.csv`
- `$SPACEGATE_STATE_DIR/reports/benchmarks/<timestamp>/summary.md`
- per-run logs in the same directory

Notes:

- benchmark runs set `SPACEGATE_KEEP_TMP=1` for resumable recovery.
- benchmark script can auto-stop Folding@Home jobs before running (`--stop-fah 1`, default on).
