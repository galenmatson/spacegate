#!/usr/bin/env python3
"""Finalize a failed ingest temp build from post-QC artifacts.

This helper is intended for recovery when ingest failed after heavy table builds
but before final promotion. It re-runs QC gates, writes Parquet exports, builds
arm, and promotes `<build_id>.tmp` to `<build_id>`.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import duckdb

ATHYG_MERGE_AMBIGUOUS_DEFAULT_LIMIT = 10_000
ATHYG_MERGE_GAIA_COLLISION_MAX = 0
ATHYG_MERGE_HIP_COLLISION_MAX = 3_000
ATHYG_MERGE_HD_COLLISION_MAX = 3_000


def log(message: str) -> None:
    ts = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"{ts} {message}", flush=True)


def parse_nonnegative_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = int(str(raw).strip())
    except ValueError as exc:
        raise SystemExit(f"Invalid {name}: {raw!r} (expected nonnegative integer)") from exc
    if value < 0:
        raise SystemExit(f"Invalid {name}: {raw!r} (must be >= 0)")
    return value


def format_stage_totals(con: duckdb.DuckDBPyConnection) -> str:
    stars = con.execute("select count(*) from stars").fetchone()[0]
    systems = con.execute("select count(*) from systems").fetchone()[0]
    planets = con.execute("select count(*) from planets").fetchone()[0]
    aliases = con.execute("select count(*) from aliases").fetchone()[0]
    compact = con.execute("select count(*) from compact_objects").fetchone()[0]
    superstellar = con.execute("select count(*) from superstellar_objects").fetchone()[0]
    eclipsing = con.execute("select count(*) from eclipsing_binaries").fetchone()[0]
    return (
        f"stars={stars:,}, systems={systems:,}, planets={planets:,}, aliases={aliases:,}, "
        f"compact_objects={compact:,}, superstellar_objects={superstellar:,}, eclipsing_binaries={eclipsing:,}"
    )


def identifier_collision_counts(con: duckdb.DuckDBPyConnection) -> tuple[int, int, int, int]:
    identifier_quarantine_count = con.execute("select count(*) from identifier_quarantine").fetchone()[0]
    identifier_gaia_collision_count = con.execute(
        """
        select count(*)
        from (
          select id_value_norm
          from object_identifiers
          where namespace = 'gaia_dr3'
          group by id_value_norm
          having count(distinct target_id) > 1
        ) t
        """
    ).fetchone()[0]
    identifier_hip_collision_count = con.execute(
        """
        select count(*)
        from (
          select id_value_norm
          from object_identifiers
          where namespace = 'hip'
          group by id_value_norm
          having count(distinct target_id) > 1
        ) t
        """
    ).fetchone()[0]
    identifier_hd_collision_count = con.execute(
        """
        select count(*)
        from (
          select id_value_norm
          from object_identifiers
          where namespace = 'hd'
          group by id_value_norm
          having count(distinct target_id) > 1
        ) t
        """
    ).fetchone()[0]
    return (
        identifier_quarantine_count,
        identifier_gaia_collision_count,
        identifier_hip_collision_count,
        identifier_hd_collision_count,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--build-id", required=True)
    args = parser.parse_args()

    root = Path(args.root)
    state_dir = Path(os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR") or root / "data")
    build_id = args.build_id
    tmp_out_dir = state_dir / "out" / f"{build_id}.tmp"
    final_out_dir = state_dir / "out" / build_id
    db_path = tmp_out_dir / "core.duckdb"
    arm_db_path = tmp_out_dir / "arm.duckdb"
    parquet_dir = tmp_out_dir / "parquet"
    reports_dir = state_dir / "reports" / build_id

    if final_out_dir.exists():
        raise SystemExit(f"Final build already exists: {final_out_dir}")
    if not tmp_out_dir.exists():
        raise SystemExit(f"Missing temp build directory: {tmp_out_dir}")
    if not db_path.exists():
        raise SystemExit(f"Missing temp core db: {db_path}")

    parquet_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    ambiguous_limit = parse_nonnegative_int_env(
        "SPACEGATE_ATHYG_MERGE_AMBIGUOUS_LIMIT",
        ATHYG_MERGE_AMBIGUOUS_DEFAULT_LIMIT,
    )
    gaia_collision_max = parse_nonnegative_int_env(
        "SPACEGATE_ATHYG_MERGE_GAIA_COLLISION_MAX",
        ATHYG_MERGE_GAIA_COLLISION_MAX,
    )
    hip_collision_max = parse_nonnegative_int_env(
        "SPACEGATE_ATHYG_MERGE_HIP_COLLISION_MAX",
        ATHYG_MERGE_HIP_COLLISION_MAX,
    )
    hd_collision_max = parse_nonnegative_int_env(
        "SPACEGATE_ATHYG_MERGE_HD_COLLISION_MAX",
        ATHYG_MERGE_HD_COLLISION_MAX,
    )

    log(f"Finalize temp ingest start (build_id={build_id})")
    con = duckdb.connect(str(db_path))
    try:
        (
            identifier_quarantine_count,
            identifier_gaia_collision_count,
            identifier_hip_collision_count,
            identifier_hd_collision_count,
        ) = identifier_collision_counts(con)
        log(
            "QC checkpoint: "
            f"identifier_quarantine={identifier_quarantine_count:,} "
            f"gaia_collisions={identifier_gaia_collision_count:,} "
            f"hip_collisions={identifier_hip_collision_count:,} "
            f"hd_collisions={identifier_hd_collision_count:,}"
        )

        if identifier_quarantine_count > ambiguous_limit:
            raise SystemExit(
                "QC failed: identifier ambiguity gate exceeded. "
                f"quarantined={identifier_quarantine_count} limit={ambiguous_limit}. "
                f"See {reports_dir / 'identifier_report.json'}"
            )
        if identifier_gaia_collision_count > gaia_collision_max:
            raise SystemExit(
                "QC failed: Gaia identifier collision gate exceeded. "
                f"collisions={identifier_gaia_collision_count} limit={gaia_collision_max}. "
                f"See {reports_dir / 'identifier_report.json'}"
            )
        if identifier_hip_collision_count > hip_collision_max:
            raise SystemExit(
                "QC failed: HIP identifier collision gate exceeded. "
                f"collisions={identifier_hip_collision_count} limit={hip_collision_max}. "
                f"See {reports_dir / 'identifier_report.json'}"
            )
        if identifier_hd_collision_count > hd_collision_max:
            raise SystemExit(
                "QC failed: HD identifier collision gate exceeded. "
                f"collisions={identifier_hd_collision_count} limit={hd_collision_max}. "
                f"See {reports_dir / 'identifier_report.json'}"
            )

        parquet_started = time.monotonic()
        log("Writing Parquet exports (resume finalize)")
        con.execute(
            f"COPY (SELECT * FROM stars ORDER BY spatial_index) TO '{parquet_dir / 'stars.parquet'}' (FORMAT 'parquet')"
        )
        con.execute(
            f"COPY (SELECT * FROM systems ORDER BY spatial_index) TO '{parquet_dir / 'systems.parquet'}' (FORMAT 'parquet')"
        )
        con.execute(
            f"COPY (SELECT * FROM planets ORDER BY spatial_index) TO '{parquet_dir / 'planets.parquet'}' (FORMAT 'parquet')"
        )
        con.execute(
            f"COPY (SELECT * FROM aliases) TO '{parquet_dir / 'aliases.parquet'}' (FORMAT 'parquet')"
        )
        con.execute(
            f"COPY (SELECT * FROM object_identifiers) TO '{parquet_dir / 'object_identifiers.parquet'}' (FORMAT 'parquet')"
        )
        con.execute(
            f"COPY (SELECT * FROM identifier_quarantine) TO '{parquet_dir / 'identifier_quarantine.parquet'}' (FORMAT 'parquet')"
        )
        log(f"Parquet export complete in {time.monotonic() - parquet_started:.1f}s")

        def meta_value(key: str) -> str | None:
            row = con.execute("select value from build_metadata where key = ? limit 1", [key]).fetchone()
            if not row:
                return None
            value = row[0]
            return str(value) if value is not None else None

        ingested_at = meta_value("ingested_at") or dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        transform_version = meta_value("transform_version") or ""
    finally:
        con.close()

    arm_builder = root / "scripts" / "build_arm.py"
    if not arm_builder.exists():
        raise SystemExit(f"Missing arm builder script: {arm_builder}")

    arm_started = time.monotonic()
    log("Building arm database (resume finalize)")
    try:
        arm_proc = subprocess.run(
            [
                sys.executable,
                str(arm_builder),
                "--core-db",
                str(db_path),
                "--arm-db",
                str(arm_db_path),
                "--state-dir",
                str(state_dir),
                "--build-id",
                build_id,
                "--ingested-at",
                ingested_at,
                "--transform-version",
                transform_version,
                "--report-path",
                str(reports_dir / "arm_report.json"),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        err = (exc.stderr or exc.stdout or "").strip()
        raise SystemExit(f"Arm build failed: {err}") from exc

    arm_stdout = (arm_proc.stdout or "").strip()
    if arm_stdout:
        for line in arm_stdout.splitlines():
            log(f"arm: {line}")
    log(f"Arm build complete in {time.monotonic() - arm_started:.1f}s")

    if not arm_db_path.exists():
        raise SystemExit(f"Arm build failed: missing output {arm_db_path}")

    tmp_out_dir.rename(final_out_dir)
    log(f"Promoted build output to {final_out_dir}")
    final_con = duckdb.connect(str(final_out_dir / "core.duckdb"))
    try:
        totals = format_stage_totals(final_con)
    finally:
        final_con.close()
    log(f"Finalize temp ingest complete | {totals}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
