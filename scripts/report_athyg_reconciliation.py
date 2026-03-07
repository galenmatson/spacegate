#!/usr/bin/env python3
"""Report AT-HYG coverage and reconciliation status against the current core build."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path

import duckdb

PC_TO_LY = 3.26156
MORTON_MAX_ABS_LY = 1000.0
POS_MAX_DELTA_RA_DEG = 0.12
POS_MAX_DELTA_DEC_DEG = 0.12
POS_MAX_DELTA_DIST_LY = 1.0
POS_MAX_ANG_SEP_ARCSEC = 45.0
SKY_BIN_FACTOR = 4.0  # 0.25 degree bins
SKY_RA_BINS = int(360 * SKY_BIN_FACTOR)
SKY_DEC_BINS = int(180 * SKY_BIN_FACTOR)
SKY_DEC_BIN_MAX = SKY_DEC_BINS - 1


def log(message: str) -> None:
    timestamp = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"{timestamp} {message}", flush=True)


def fmt_count(value: int | None) -> str:
    if value is None:
        return "n/a"
    return f"{int(value):,}"


def stage_done(stage: str, started: float, *, totals: dict[str, int | None] | None = None) -> None:
    elapsed = time.monotonic() - started
    message = f"{stage} complete in {elapsed:.1f}s"
    if totals:
        ordered = ["athyg_rows", "direct_matches", "positional_matches", "unmatched_rows"]
        parts = [f"{key}={fmt_count(totals.get(key))}" for key in ordered if key in totals]
        if parts:
            message += " | totals: " + ", ".join(parts)
    log(message)


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    state_dir = Path(
        os.getenv(
            "SPACEGATE_STATE_DIR",
            os.getenv("SPACEGATE_DATA_DIR", str(root / "data")),
        )
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--core-db",
        default=str(state_dir / "served" / "current" / "core.duckdb"),
        help="Path to core.duckdb (default: served/current/core.duckdb)",
    )
    parser.add_argument(
        "--athyg-csv",
        default=str(state_dir / "cooked" / "athyg" / "athyg.csv.gz"),
        help="Path to cooked AT-HYG CSV (gzipped) file",
    )
    parser.add_argument(
        "--out",
        default="",
        help="Output report path (default: reports/<build_id>/athyg_reconciliation_report.json)",
    )
    return parser.parse_args()


def resolve_output_path(out_arg: str, state_dir: Path, build_id: str) -> Path:
    if out_arg:
        return Path(out_arg)
    return state_dir / "reports" / build_id / "athyg_reconciliation_report.json"


def main() -> int:
    args = parse_args()
    core_db = Path(args.core_db)
    athyg_csv = Path(args.athyg_csv)

    if not core_db.exists():
        raise SystemExit(f"Missing core DB: {core_db}")
    if not athyg_csv.exists():
        raise SystemExit(f"Missing AT-HYG cooked file: {athyg_csv}")

    log("AT-HYG reconciliation report begin")
    run_started = time.monotonic()

    con = duckdb.connect(str(core_db), read_only=True)
    duckdb_threads = os.getenv("SPACEGATE_DUCKDB_THREADS") or os.getenv("DUCKDB_THREADS")
    if duckdb_threads:
        con.execute(f"PRAGMA threads={int(duckdb_threads)}")
    build_id_row = con.execute(
        "select value from build_metadata where key = 'build_id' limit 1"
    ).fetchone()
    build_id = str(build_id_row[0]) if build_id_row and build_id_row[0] else "unknown_build"
    state_dir = core_db.parents[2] if core_db.parent.name == "current" else core_db.parent
    out_path = resolve_output_path(args.out, state_dir, build_id)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    totals: dict[str, int | None] = {}

    stage = time.monotonic()
    con.execute(
        f"""
        create temp table athyg_norm as
        select
          cast(nullif(id, '') as bigint) as source_pk,
          cast(nullif(gaia, '') as bigint) as gaia_id,
          cast(nullif(nullif(hip, ''), '0') as bigint) as hip_id,
          cast(nullif(nullif(hd, ''), '0') as bigint) as hd_id,
          nullif(proper, '') as proper_name,
          nullif(bayer, '') as bayer,
          nullif(flam, '') as flam,
          nullif(con, '') as constellation,
          case
            when cast(nullif(ra, '') as double) between 0.0 and 24.0 then cast(nullif(ra, '') as double) * 15.0
            else cast(nullif(ra, '') as double)
          end as ra_deg,
          cast(nullif(dec, '') as double) as dec_deg,
          cast(nullif(dist, '') as double) * {PC_TO_LY} as dist_ly,
          (
            nullif(proper, '') is not null
            or (nullif(bayer, '') is not null and nullif(con, '') is not null)
            or (nullif(flam, '') is not null and nullif(con, '') is not null)
          ) as has_human_name
        from read_csv_auto('{str(athyg_csv).replace("'", "''")}',
          compression='gzip',
          delim=',',
          quote='"',
          escape='"',
          header=true,
          strict_mode=false,
          null_padding=true,
          all_varchar=true
        )
        where cast(nullif(id, '') as bigint) is not null
        """
    )
    totals["athyg_rows"] = con.execute("select count(*) from athyg_norm").fetchone()[0]
    stage_done("Load AT-HYG snapshot", stage, totals=totals)

    stage = time.monotonic()
    con.execute(
        """
        create temp table athyg_gaia_ids as
        select distinct gaia_id
        from athyg_norm
        where gaia_id is not null
        """
    )
    con.execute(
        """
        create temp table athyg_hip_ids as
        select distinct hip_id
        from athyg_norm
        where hip_id is not null
        """
    )
    con.execute(
        """
        create temp table athyg_hd_ids as
        select distinct hd_id
        from athyg_norm
        where hd_id is not null
        """
    )
    log("Direct ID joins: prepared AT-HYG identifier sets")

    con.execute(
        """
        create temp table stars_by_gaia as
        select s.gaia_id, min(s.star_id) as star_id, min(s.system_id) as system_id
        from stars s
        join athyg_gaia_ids a using (gaia_id)
        group by s.gaia_id
        """
    )
    log("Direct ID joins: built Gaia match map")

    con.execute(
        """
        create temp table stars_by_hip as
        select s.hip_id, min(s.star_id) as star_id, min(s.system_id) as system_id
        from stars s
        join athyg_hip_ids a using (hip_id)
        group by s.hip_id
        """
    )
    log("Direct ID joins: built HIP match map")

    con.execute(
        """
        create temp table stars_by_hd as
        select s.hd_id, min(s.star_id) as star_id, min(s.system_id) as system_id
        from stars s
        join athyg_hd_ids a using (hd_id)
        group by s.hd_id
        """
    )
    log("Direct ID joins: built HD match map")

    con.execute(
        """
        create temp table athyg_direct as
        select
          a.source_pk,
          a.gaia_id,
          a.hip_id,
          a.hd_id,
          a.proper_name,
          a.bayer,
          a.flam,
          a.constellation,
          a.ra_deg,
          a.dec_deg,
          a.dist_ly,
          a.has_human_name,
          g.star_id as gaia_star_id,
          g.system_id as gaia_system_id,
          h.star_id as hip_star_id,
          h.system_id as hip_system_id,
          d.star_id as hd_star_id,
          d.system_id as hd_system_id
        from athyg_norm a
        left join stars_by_gaia g on a.gaia_id is not null and a.gaia_id = g.gaia_id
        left join stars_by_hip h on a.hip_id is not null and a.hip_id = h.hip_id
        left join stars_by_hd d on a.hd_id is not null and a.hd_id = d.hd_id
        """
    )
    direct_gaia = con.execute(
        "select count(*)::bigint from athyg_direct where gaia_star_id is not null"
    ).fetchone()[0]
    direct_hip = con.execute(
        "select count(*)::bigint from athyg_direct where gaia_star_id is null and hip_star_id is not null"
    ).fetchone()[0]
    direct_hd = con.execute(
        "select count(*)::bigint from athyg_direct where gaia_star_id is null and hip_star_id is null and hd_star_id is not null"
    ).fetchone()[0]
    totals["direct_matches"] = con.execute(
        """
        select count(*) from athyg_direct
        where gaia_star_id is not null or hip_star_id is not null or hd_star_id is not null
        """
    ).fetchone()[0]
    stage_done(
        "Direct ID joins",
        stage,
        totals=totals,
    )
    log(
        "Direct ID joins breakdown: "
        f"gaia={fmt_count(int(direct_gaia or 0))}, "
        f"hip_only={fmt_count(int(direct_hip or 0))}, "
        f"hd_only={fmt_count(int(direct_hd or 0))}"
    )

    stage = time.monotonic()
    con.execute(
        f"""
        create temp table athyg_positional_unresolved as
        select
          source_pk,
          ra_deg,
          dec_deg,
          dist_ly,
          cast(floor(ra_deg * {SKY_BIN_FACTOR}) as integer) as ra_bin,
          cast(floor((dec_deg + 90.0) * {SKY_BIN_FACTOR}) as integer) as dec_bin
        from athyg_direct
        where gaia_star_id is null
          and hip_star_id is null
          and hd_star_id is null
          and dist_ly is not null
          and dist_ly <= {MORTON_MAX_ABS_LY}
          and ra_deg is not null
          and dec_deg is not null
          and (has_human_name or hip_id is not null or hd_id is not null)
        """
    )
    unresolved_count = con.execute(
        "select count(*)::bigint from athyg_positional_unresolved"
    ).fetchone()[0]
    log(f"Positional fallback unresolved rows: {fmt_count(unresolved_count)}")

    con.execute(
        f"""
        create temp table stars_binned as
        select
          star_id,
          system_id,
          ra_deg,
          dec_deg,
          dist_ly,
          cast(floor(ra_deg * {SKY_BIN_FACTOR}) as integer) as ra_bin,
          cast(floor((dec_deg + 90.0) * {SKY_BIN_FACTOR}) as integer) as dec_bin
        from stars
        where ra_deg is not null
          and dec_deg is not null
          and dist_ly is not null
          and dist_ly <= {MORTON_MAX_ABS_LY}
        """
    )

    con.execute(
        f"""
        create temp table athyg_positional_candidates as
        with expanded as (
          select distinct
            u.source_pk,
            u.ra_deg,
            u.dec_deg,
            u.dist_ly,
            ((u.ra_bin + ro.delta + {SKY_RA_BINS}) % {SKY_RA_BINS}) as ra_bin_n,
            least(greatest(u.dec_bin + doff.delta, 0), {SKY_DEC_BIN_MAX}) as dec_bin_n
          from athyg_positional_unresolved u
          cross join (values (-1), (0), (1)) as ro(delta)
          cross join (values (-1), (0), (1)) as doff(delta)
        ), candidates as (
          select
            e.source_pk,
            s.star_id,
            s.system_id,
            abs(s.dist_ly - e.dist_ly) as dist_delta_ly,
            degrees(acos(
              least(
                1.0,
                greatest(
                  -1.0,
                  sin(radians(s.dec_deg)) * sin(radians(e.dec_deg)) +
                  cos(radians(s.dec_deg)) * cos(radians(e.dec_deg)) *
                    cos(radians(least(abs(s.ra_deg - e.ra_deg), 360.0 - abs(s.ra_deg - e.ra_deg))))
                )
              )
            )) * 3600.0 as ang_sep_arcsec
          from expanded e
          join stars_binned s
            on s.ra_bin = e.ra_bin_n
           and s.dec_bin = e.dec_bin_n
          where least(abs(s.ra_deg - e.ra_deg), 360.0 - abs(s.ra_deg - e.ra_deg)) <= {POS_MAX_DELTA_RA_DEG}
            and abs(s.dec_deg - e.dec_deg) <= {POS_MAX_DELTA_DEC_DEG}
            and abs(s.dist_ly - e.dist_ly) <= {POS_MAX_DELTA_DIST_LY}
        ), ranked as (
          select
            *,
            row_number() over (
              partition by source_pk
              order by dist_delta_ly asc, ang_sep_arcsec asc, star_id asc
            ) as rn,
            count(*) over (partition by source_pk) as candidate_count
          from candidates
          where ang_sep_arcsec <= {POS_MAX_ANG_SEP_ARCSEC}
        )
        select *
        from ranked
        where rn = 1
        """
    )
    totals["positional_matches"] = con.execute(
        "select count(*) from athyg_positional_candidates"
    ).fetchone()[0]
    stage_done("Positional fallback joins", stage, totals=totals)

    stage = time.monotonic()
    con.execute(
        f"""
        create temp table athyg_reconcile as
        select
          d.source_pk,
          d.gaia_id,
          d.hip_id,
          d.hd_id,
          d.proper_name,
          d.bayer,
          d.flam,
          d.constellation,
          d.ra_deg,
          d.dec_deg,
          d.dist_ly,
          d.has_human_name,
          coalesce(d.gaia_star_id, d.hip_star_id, d.hd_star_id, p.star_id) as matched_star_id,
          coalesce(d.gaia_system_id, d.hip_system_id, d.hd_system_id, p.system_id) as matched_system_id,
          p.dist_delta_ly as positional_dist_delta_ly,
          p.ang_sep_arcsec as positional_ang_sep_arcsec,
          p.candidate_count as positional_candidate_count,
          case
            when d.gaia_star_id is not null then 'direct_gaia'
            when d.hip_star_id is not null then 'direct_hip'
            when d.hd_star_id is not null then 'direct_hd'
            when p.star_id is not null and d.has_human_name then 'positional_named'
            when p.star_id is not null then 'positional_numeric'
            when d.dist_ly is not null and d.dist_ly > {MORTON_MAX_ABS_LY} then 'outside_core_distance'
            else 'unmatched'
          end as reconcile_status
        from athyg_direct d
        left join athyg_positional_candidates p using (source_pk)
        """
    )
    totals["unmatched_rows"] = con.execute(
        "select count(*) from athyg_reconcile where reconcile_status = 'unmatched'"
    ).fetchone()[0]
    stage_done("Build reconciliation status table", stage, totals=totals)

    status_counts = con.execute(
        """
        select reconcile_status, count(*)::bigint as count
        from athyg_reconcile
        group by 1
        order by count desc, reconcile_status asc
        """
    ).fetchall()

    id_field_counts = con.execute(
        """
        select
          count(*)::bigint as total_rows,
          sum(case when gaia_id is not null then 1 else 0 end)::bigint as rows_with_gaia_id,
          sum(case when hip_id is not null then 1 else 0 end)::bigint as rows_with_hip_id,
          sum(case when hd_id is not null then 1 else 0 end)::bigint as rows_with_hd_id,
          sum(case when has_human_name then 1 else 0 end)::bigint as rows_with_human_name
        from athyg_reconcile
        """
    ).fetchone()

    unmatched_samples = con.execute(
        """
        select
          source_pk,
          gaia_id,
          hip_id,
          hd_id,
          proper_name,
          case when bayer is not null and constellation is not null then bayer || ' ' || constellation else null end as bayer_name,
          case when flam is not null and constellation is not null then flam || ' ' || constellation else null end as flam_name,
          ra_deg,
          dec_deg,
          dist_ly
        from athyg_reconcile
        where reconcile_status = 'unmatched'
        order by
          case when has_human_name then 0 else 1 end,
          case when hip_id is not null then 0 else 1 end,
          case when hd_id is not null then 0 else 1 end,
          dist_ly asc nulls last,
          source_pk asc
        limit 50
        """
    ).fetchall()

    positional_ambiguous_count = con.execute(
        """
        select count(*)::bigint
        from athyg_reconcile
        where reconcile_status like 'positional_%'
          and coalesce(positional_candidate_count, 0) > 1
        """
    ).fetchone()[0]

    report = {
        "generated_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "core_build_id": build_id,
        "paths": {
            "core_db": str(core_db),
            "athyg_csv": str(athyg_csv),
            "report_path": str(out_path),
        },
        "reconcile_parameters": {
            "max_core_distance_ly": MORTON_MAX_ABS_LY,
            "positional_max_delta_ra_deg": POS_MAX_DELTA_RA_DEG,
            "positional_max_delta_dec_deg": POS_MAX_DELTA_DEC_DEG,
            "positional_max_delta_dist_ly": POS_MAX_DELTA_DIST_LY,
            "positional_max_ang_sep_arcsec": POS_MAX_ANG_SEP_ARCSEC,
        },
        "totals": {
            "athyg_rows": int(id_field_counts[0]),
            "athyg_rows_with_gaia_id": int(id_field_counts[1]),
            "athyg_rows_with_hip_id": int(id_field_counts[2]),
            "athyg_rows_with_hd_id": int(id_field_counts[3]),
            "athyg_rows_with_human_name": int(id_field_counts[4]),
            "direct_matches": int(totals.get("direct_matches") or 0),
            "positional_matches": int(totals.get("positional_matches") or 0),
            "unmatched_rows": int(totals.get("unmatched_rows") or 0),
            "positional_ambiguous_rows": int(positional_ambiguous_count or 0),
        },
        "status_counts": [
            {"status": str(status), "count": int(count)}
            for status, count in status_counts
        ],
        "samples": {
            "unmatched_top50": [
                {
                    "source_pk": row[0],
                    "gaia_id": row[1],
                    "hip_id": row[2],
                    "hd_id": row[3],
                    "proper_name": row[4],
                    "bayer_name": row[5],
                    "flamsteed_name": row[6],
                    "ra_deg": row[7],
                    "dec_deg": row[8],
                    "dist_ly": row[9],
                }
                for row in unmatched_samples
            ]
        },
        "notes": [
            "direct_* statuses are exact ID joins into current core stars.",
            "positional_* statuses are deterministic fallback matches for unresolved rows under tight positional and distance gates.",
            "outside_core_distance means AT-HYG row is beyond current core distance envelope and is expected to be absent.",
        ],
    }

    out_path.write_text(json.dumps(report, indent=2))
    con.close()

    total_elapsed = time.monotonic() - run_started
    log(f"Wrote reconciliation report: {out_path}")
    log(
        "AT-HYG reconciliation report complete "
        f"in {total_elapsed:.1f}s | totals: "
        f"athyg_rows={fmt_count(report['totals']['athyg_rows'])}, "
        f"direct_matches={fmt_count(report['totals']['direct_matches'])}, "
        f"positional_matches={fmt_count(report['totals']['positional_matches'])}, "
        f"unmatched={fmt_count(report['totals']['unmatched_rows'])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
