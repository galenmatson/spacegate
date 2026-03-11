# Ingest Recovery + Runtime Tuning

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

- temp path: `/data/spacegate/data/out/<build_id>.tmp`
- reports path: `/data/spacegate/data/reports/<build_id>/...`

This avoids losing expensive intermediate artifacts.

## Finalize From Temp Build

If ingest fails after core tables are already built (for example, QC gate tuning), recover without recomputing heavy stages:

```bash
scripts/finalize_ingest_tmp.sh --build-id <build_id>
```

The finalize helper:

1. Re-runs QC identifier gates with current env thresholds.
2. Writes Parquet exports from existing temp `core.duckdb`.
3. Builds `arm.duckdb`.
4. Promotes `<build_id>.tmp` to `<build_id>`.

## Resource Tuning Notes (Proton)

Useful env knobs before ingest:

- `SPACEGATE_DUCKDB_THREADS`
- `SPACEGATE_DUCKDB_MEMORY_LIMIT`

Observed stable setting on Proton for the Gaia-first core:

- `SPACEGATE_DUCKDB_THREADS=8`
- `SPACEGATE_DUCKDB_MEMORY_LIMIT=26GB`

If OOM appears during identifier merge or alias stage, reduce threads first, then adjust memory.

## Benchmark Matrix Runner

Use the matrix runner to measure ingest wall-clock and per-stage durations across thread/memory combinations:

```bash
scripts/benchmark_ingest_tuning.sh --threads 8,10,12 --memory 26GB,28GB,30GB
```

Outputs are written to:

- `/data/spacegate/data/reports/benchmarks/<timestamp>/summary.csv`
- `/data/spacegate/data/reports/benchmarks/<timestamp>/summary.md`
- per-run logs in the same directory

Notes:

- benchmark runs set `SPACEGATE_KEEP_TMP=1` for resumable recovery.
- benchmark script can auto-stop Folding@Home jobs before running (`--stop-fah 1`, default on).
