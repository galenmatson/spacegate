#!/usr/bin/env python3
import argparse
import datetime as dt
import json
from pathlib import Path

import duckdb

from catalog_eval import default_state_dir, init_env, sql_quote


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def source_select_sql(name: str, path: Path) -> str:
    quoted_path = sql_quote(str(path))
    if name == "wds":
        return f"""
            select
              'wds' as source_catalog,
              coalesce(wds_id || ':' || component, wds_id) as source_key,
              null::bigint as hip_id,
              null::bigint as hd_id,
              nullif(wds_id, '') as wds_id,
              try_cast(nullif(ra_deg, '') as double) as ra_deg_j2000,
              try_cast(nullif(dec_deg, '') as double) as dec_deg_j2000,
              try_cast(nullif(pm_primary_ra, '') as double) as pm_ra_mas_yr,
              try_cast(nullif(pm_primary_dec, '') as double) as pm_dec_mas_yr,
              try_cast(nullif(mag_primary, '') as double) as vmag,
              nullif(component, '') as component,
              nullif(discoverer, '') as source_label
            from read_csv_auto(
              {quoted_path},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """
    if name == "msc":
        return f"""
            select
              'msc' as source_catalog,
              coalesce(wds_id || ':' || component, wds_id) as source_key,
              try_cast(nullif(hip_id, '') as bigint) as hip_id,
              try_cast(nullif(hd_id, '') as bigint) as hd_id,
              nullif(wds_id, '') as wds_id,
              try_cast(nullif(ra_deg, '') as double) as ra_deg_j2000,
              try_cast(nullif(dec_deg, '') as double) as dec_deg_j2000,
              try_cast(nullif(pm_ra_mas_yr, '') as double) as pm_ra_mas_yr,
              try_cast(nullif(pm_dec_mas_yr, '') as double) as pm_dec_mas_yr,
              try_cast(nullif(vmag, '') as double) as vmag,
              nullif(component, '') as component,
              nullif(other_identifiers, '') as source_label
            from read_csv_auto(
              {quoted_path},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """
    if name == "orb6":
        return f"""
            select
              'orb6' as source_catalog,
              coalesce(wds_id, discoverer) as source_key,
              try_cast(nullif(hip_id, '') as bigint) as hip_id,
              try_cast(nullif(hd_id, '') as bigint) as hd_id,
              nullif(wds_id, '') as wds_id,
              try_cast(nullif(ra_deg, '') as double) as ra_deg_j2000,
              try_cast(nullif(dec_deg, '') as double) as dec_deg_j2000,
              null::double as pm_ra_mas_yr,
              null::double as pm_dec_mas_yr,
              try_cast(nullif(mag_primary, '') as double) as vmag,
              null::varchar as component,
              nullif(discoverer, '') as source_label
            from read_csv_auto(
              {quoted_path},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """
    raise ValueError(f"Unsupported source catalog: {name}")


def build_source_table(con: duckdb.DuckDBPyConnection, name: str, path: Path) -> None:
    con.execute(f"create or replace temp table source_rows as {source_select_sql(name, path)}")
    con.execute(
        """
        create or replace temp table source_prepared as
        select
          source_catalog,
          source_key,
          hip_id,
          hd_id,
          wds_id,
          component,
          source_label,
          ra_deg_j2000,
          dec_deg_j2000,
          pm_ra_mas_yr,
          pm_dec_mas_yr,
          vmag,
          case
            when ra_deg_j2000 is null then null
            when pm_ra_mas_yr is null or dec_deg_j2000 is null then ra_deg_j2000
            when abs(cos(radians(dec_deg_j2000))) < 0.01 then ra_deg_j2000
            else ra_deg_j2000 + ((pm_ra_mas_yr / 1000.0) / 3600.0) * 16.0 / cos(radians(dec_deg_j2000))
          end as ra_deg_norm,
          case
            when dec_deg_j2000 is null then null
            when pm_dec_mas_yr is null then dec_deg_j2000
            else dec_deg_j2000 + ((pm_dec_mas_yr / 1000.0) / 3600.0) * 16.0
          end as dec_deg_norm,
          floor(coalesce(
            case
              when ra_deg_j2000 is null then null
              when pm_ra_mas_yr is null or dec_deg_j2000 is null then ra_deg_j2000
              when abs(cos(radians(dec_deg_j2000))) < 0.01 then ra_deg_j2000
              else ra_deg_j2000 + ((pm_ra_mas_yr / 1000.0) / 3600.0) * 16.0 / cos(radians(dec_deg_j2000))
            end,
            ra_deg_j2000
          ) * 20.0) as ra_bin,
          floor((coalesce(
            case
              when dec_deg_j2000 is null then null
              when pm_dec_mas_yr is null then dec_deg_j2000
              else dec_deg_j2000 + ((pm_dec_mas_yr / 1000.0) / 3600.0) * 16.0
            end,
            dec_deg_j2000
          ) + 90.0) * 20.0) as dec_bin
        from source_rows
        """
    )


def prepare_core(con: duckdb.DuckDBPyConnection, core_db_path: Path) -> None:
    con.execute(f"attach {sql_quote(str(core_db_path))} as core (read_only)")
    con.execute(
        """
        create or replace temp table core_star_match as
        select
          star_id,
          star_name,
          gaia_id,
          hip_id,
          hd_id,
          ra_deg,
          dec_deg,
          pm_ra_mas_yr,
          pm_dec_mas_yr,
          vmag,
          floor(ra_deg * 20.0) as ra_bin,
          floor((dec_deg + 90.0) * 20.0) as dec_bin
        from core.stars
        where ra_deg is not null and dec_deg is not null
        """
    )


def build_match_table(con: duckdb.DuckDBPyConnection, max_sep_arcsec: float) -> None:
    con.execute(
        """
        create or replace temp table id_matches as
        with candidates as (
          select
            src.source_key,
            src.source_catalog,
            src.wds_id,
            src.component,
            src.source_label,
            src.hip_id,
            src.hd_id,
            src.ra_deg_norm,
            src.dec_deg_norm,
            src.pm_ra_mas_yr,
            src.pm_dec_mas_yr,
            src.vmag,
            core.star_id,
            core.star_name,
            core.ra_deg as core_ra_deg,
            core.dec_deg as core_dec_deg,
            core.pm_ra_mas_yr as core_pm_ra_mas_yr,
            core.pm_dec_mas_yr as core_pm_dec_mas_yr,
            core.vmag as core_vmag,
            case
              when src.hip_id is not null and src.hip_id = core.hip_id then 'hip_id'
              else 'hd_id'
            end as match_basis,
            100 as confidence_score,
            row_number() over (
              partition by src.source_key
              order by
                case
                  when src.hip_id is not null and src.hip_id = core.hip_id then 0
                  else 1
                end,
                case
                  when src.ra_deg_norm is null or src.dec_deg_norm is null then 1.0e18
                  else degrees(acos(least(
                    1.0,
                    greatest(
                      -1.0,
                      sin(radians(src.dec_deg_norm)) * sin(radians(core.dec_deg)) +
                      cos(radians(src.dec_deg_norm)) * cos(radians(core.dec_deg)) *
                      cos(radians(src.ra_deg_norm - core.ra_deg))
                    )
                  ))) * 3600.0
                end asc
            ) as match_rank
          from source_prepared src
          join core_star_match core
            on (src.hip_id is not null and src.hip_id = core.hip_id)
            or (src.hd_id is not null and src.hd_id = core.hd_id)
        )
        select *
        from candidates
        where match_rank = 1
        """
    )
    con.execute(
        f"""
        create or replace temp table coord_candidates as
        with candidates as (
          select
            src.source_key,
            src.source_catalog,
            src.wds_id,
            src.component,
            src.source_label,
            src.hip_id,
            src.hd_id,
            src.ra_deg_norm,
            src.dec_deg_norm,
            src.pm_ra_mas_yr,
            src.pm_dec_mas_yr,
            src.vmag,
            core.star_id,
            core.star_name,
            core.ra_deg as core_ra_deg,
            core.dec_deg as core_dec_deg,
            core.pm_ra_mas_yr as core_pm_ra_mas_yr,
            core.pm_dec_mas_yr as core_pm_dec_mas_yr,
            core.vmag as core_vmag,
            degrees(acos(least(
              1.0,
              greatest(
                -1.0,
                sin(radians(src.dec_deg_norm)) * sin(radians(core.dec_deg)) +
                cos(radians(src.dec_deg_norm)) * cos(radians(core.dec_deg)) *
                cos(radians(src.ra_deg_norm - core.ra_deg))
              )
            ))) * 3600.0 as sep_arcsec,
            case
              when src.vmag is null or core.vmag is null then null
              else abs(src.vmag - core.vmag)
            end as vmag_diff,
            case
              when src.pm_ra_mas_yr is null or src.pm_dec_mas_yr is null then null
              when core.pm_ra_mas_yr is null or core.pm_dec_mas_yr is null then null
              else sqrt(
                pow(src.pm_ra_mas_yr - core.pm_ra_mas_yr, 2) +
                pow(src.pm_dec_mas_yr - core.pm_dec_mas_yr, 2)
              )
            end as pm_diff,
            row_number() over (
              partition by src.source_key
              order by
                degrees(acos(least(
                  1.0,
                  greatest(
                    -1.0,
                    sin(radians(src.dec_deg_norm)) * sin(radians(core.dec_deg)) +
                    cos(radians(src.dec_deg_norm)) * cos(radians(core.dec_deg)) *
                    cos(radians(src.ra_deg_norm - core.ra_deg))
                  )
                ))) * 3600.0 asc,
                abs(coalesce(src.vmag, core.vmag, 0.0) - coalesce(core.vmag, src.vmag, 0.0)) asc
            ) as candidate_rank
          from source_prepared src
          join core_star_match core
            on src.ra_deg_norm is not null
           and src.dec_deg_norm is not null
           and core.ra_bin between src.ra_bin - 1 and src.ra_bin + 1
           and core.dec_bin between src.dec_bin - 1 and src.dec_bin + 1
          left join id_matches ids using (source_key)
          where ids.source_key is null
        )
        select *
        from candidates
        where candidate_rank = 1 and sep_arcsec <= {max_sep_arcsec}
        """
    )
    con.execute(
        """
        create or replace temp table coord_matches as
        select
          source_key,
          source_catalog,
          wds_id,
          component,
          source_label,
          hip_id,
          hd_id,
          ra_deg_norm,
          dec_deg_norm,
          pm_ra_mas_yr,
          pm_dec_mas_yr,
          vmag,
          star_id,
          star_name,
          core_ra_deg,
          core_dec_deg,
          core_pm_ra_mas_yr,
          core_pm_dec_mas_yr,
          core_vmag,
          'coordinate' as match_basis,
          (
            case
              when sep_arcsec <= 2.0 then 70
              when sep_arcsec <= 5.0 then 55
              when sep_arcsec <= 15.0 then 35
              else 15
            end
            + case
                when vmag_diff is null then 0
                when vmag_diff <= 0.5 then 20
                when vmag_diff <= 1.5 then 10
                else 0
              end
            + case
                when pm_diff is null then 0
                when pm_diff <= 5.0 then 15
                when pm_diff <= 20.0 then 8
                else 0
              end
          ) as confidence_score,
          sep_arcsec,
          vmag_diff,
          pm_diff
        from coord_candidates
        """
    )
    con.execute(
        """
        create or replace temp table all_matches as
        select
          source_key,
          source_catalog,
          wds_id,
          component,
          source_label,
          hip_id,
          hd_id,
          ra_deg_norm,
          dec_deg_norm,
          pm_ra_mas_yr,
          pm_dec_mas_yr,
          vmag,
          star_id,
          star_name,
          core_ra_deg,
          core_dec_deg,
          core_pm_ra_mas_yr,
          core_pm_dec_mas_yr,
          core_vmag,
          match_basis,
          confidence_score,
          case
            when match_basis <> 'coordinate' then
              degrees(acos(least(
                1.0,
                greatest(
                  -1.0,
                  sin(radians(ra_deg_norm * 0 + dec_deg_norm)) * sin(radians(core_dec_deg)) +
                  cos(radians(dec_deg_norm)) * cos(radians(core_dec_deg)) *
                  cos(radians(ra_deg_norm - core_ra_deg))
                )
              ))) * 3600.0
            else sep_arcsec
          end as sep_arcsec,
          case
            when match_basis <> 'coordinate' and vmag is not null and core_vmag is not null then abs(vmag - core_vmag)
            else vmag_diff
          end as vmag_diff,
          case
            when match_basis <> 'coordinate'
             and pm_ra_mas_yr is not null and pm_dec_mas_yr is not null
             and core_pm_ra_mas_yr is not null and core_pm_dec_mas_yr is not null
            then sqrt(
              pow(pm_ra_mas_yr - core_pm_ra_mas_yr, 2) +
              pow(pm_dec_mas_yr - core_pm_dec_mas_yr, 2)
            )
            else pm_diff
          end as pm_diff
        from coord_matches
        union all
        select
          source_key,
          source_catalog,
          wds_id,
          component,
          source_label,
          hip_id,
          hd_id,
          ra_deg_norm,
          dec_deg_norm,
          pm_ra_mas_yr,
          pm_dec_mas_yr,
          vmag,
          star_id,
          star_name,
          core_ra_deg,
          core_dec_deg,
          core_pm_ra_mas_yr,
          core_pm_dec_mas_yr,
          core_vmag,
          match_basis,
          confidence_score,
          degrees(acos(least(
            1.0,
            greatest(
              -1.0,
              sin(radians(dec_deg_norm)) * sin(radians(core_dec_deg)) +
              cos(radians(dec_deg_norm)) * cos(radians(core_dec_deg)) *
              cos(radians(ra_deg_norm - core_ra_deg))
            )
          ))) * 3600.0 as sep_arcsec,
          case
            when vmag is null or core_vmag is null then null
            else abs(vmag - core_vmag)
          end as vmag_diff,
          case
            when pm_ra_mas_yr is null or pm_dec_mas_yr is null then null
            when core_pm_ra_mas_yr is null or core_pm_dec_mas_yr is null then null
            else sqrt(
              pow(pm_ra_mas_yr - core_pm_ra_mas_yr, 2) +
              pow(pm_dec_mas_yr - core_pm_dec_mas_yr, 2)
            )
          end as pm_diff
        from id_matches
        """
    )
    con.execute(
        """
        create or replace temp table all_matches_scored as
        select
          *,
          case
            when match_basis <> 'coordinate' then 'exact_id'
            when confidence_score >= 80 then 'high'
            when confidence_score >= 55 then 'medium'
            when confidence_score >= 30 then 'low'
            else 'reject'
          end as confidence_tier
        from all_matches
        """
    )


def source_paths(state_dir: Path) -> dict[str, Path]:
    return {
        "wds": state_dir / "cooked" / "wds" / "wds_summary.csv",
        "msc": state_dir / "cooked" / "msc" / "msc_components.csv",
        "orb6": state_dir / "cooked" / "orb6" / "orb6_orbits.csv",
    }


def run_one_source(
    con: duckdb.DuckDBPyConnection,
    source_name: str,
    path: Path,
    output_dir: Path,
    max_sep_arcsec: float,
) -> dict:
    build_source_table(con, source_name, path)
    build_match_table(con, max_sep_arcsec)
    matches_path = output_dir / f"{source_name}_matches.csv"
    con.execute(
        f"""
        copy (
          select
            source_key,
            source_catalog,
            wds_id,
            component,
            source_label,
            hip_id,
            hd_id,
            star_id,
            star_name,
            match_basis,
            confidence_tier,
            confidence_score,
            round(sep_arcsec, 3) as sep_arcsec,
            round(vmag_diff, 3) as vmag_diff,
            round(pm_diff, 3) as pm_diff
          from all_matches_scored
          where confidence_tier <> 'reject'
          order by
            case confidence_tier
              when 'exact_id' then 0
              when 'high' then 1
              when 'medium' then 2
              else 3
            end,
            confidence_score desc,
            sep_arcsec asc nulls last
        ) to {sql_quote(str(matches_path))} (header, delimiter ',')
        """
    )

    coverage = con.execute("select count(*) from source_prepared").fetchone()[0]
    matched = con.execute(
        "select count(*) from all_matches_scored where confidence_tier <> 'reject'"
    ).fetchone()[0]
    tier_rows = con.execute(
        """
        select confidence_tier, count(*) as row_count
        from all_matches_scored
        where confidence_tier <> 'reject'
        group by confidence_tier
        order by confidence_tier
        """
    ).fetchall()
    tier_counts = {row[0]: int(row[1]) for row in tier_rows}
    median_sep = con.execute(
        """
        select median(sep_arcsec)
        from all_matches_scored
        where confidence_tier <> 'reject' and sep_arcsec is not null
        """
    ).fetchone()[0]
    best_examples = con.execute(
        """
        select
          source_key,
          star_name,
          confidence_tier,
          round(sep_arcsec, 3) as sep_arcsec,
          round(vmag_diff, 3) as vmag_diff,
          round(pm_diff, 3) as pm_diff
        from all_matches_scored
        where confidence_tier <> 'reject'
        order by
          case confidence_tier
            when 'exact_id' then 0
            when 'high' then 1
            when 'medium' then 2
            else 3
          end,
          confidence_score desc,
          sep_arcsec asc nulls last
        limit 10
        """
    ).fetchall()
    return {
        "source_catalog": source_name,
        "source_path": str(path),
        "rows_scanned": int(coverage),
        "matched_rows": int(matched),
        "median_sep_arcsec": None if median_sep is None else round(float(median_sep), 3),
        "tier_counts": tier_counts,
        "matches_path": str(matches_path),
        "examples": [
            {
                "source_key": row[0],
                "star_name": row[1],
                "confidence_tier": row[2],
                "sep_arcsec": row[3],
                "vmag_diff": row[4],
                "pm_diff": row[5],
            }
            for row in best_examples
        ],
    }


def render_markdown(run_id: str, reports: list[dict], max_sep_arcsec: float) -> str:
    lines = [
        "# Multiplicity Crossmatch Summary",
        "",
        f"- Run ID: `{run_id}`",
        f"- Maximum coordinate match radius: `{max_sep_arcsec}` arcsec",
        "",
    ]
    for report in reports:
        lines.extend(
            [
                f"## {report['source_catalog']}",
                "",
                f"- Source path: `{report['source_path']}`",
                f"- Rows scanned: `{report['rows_scanned']}`",
                f"- Accepted matches: `{report['matched_rows']}`",
                f"- Median separation: `{report['median_sep_arcsec']}` arcsec",
                f"- Match CSV: `{report['matches_path']}`",
                "",
                "### Confidence Tiers",
                "",
            ]
        )
        for tier, count in sorted(report["tier_counts"].items()):
            lines.append(f"- `{tier}`: {count}")
        if report["examples"]:
            lines.extend(["", "### Example Matches", ""])
            for example in report["examples"]:
                lines.append(
                    f"- `{example['source_key']}` -> `{example['star_name']}` "
                    f"({example['confidence_tier']}, sep={example['sep_arcsec']} arcsec)"
                )
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prototype confidence-scored crossmatches for multiplicity catalogs."
    )
    parser.add_argument(
        "--catalog",
        action="append",
        choices=["wds", "msc", "orb6"],
        help="Source catalog to crossmatch. Defaults to all supported sources.",
    )
    parser.add_argument(
        "--max-sep-arcsec",
        type=float,
        default=60.0,
        help="Maximum candidate separation for coordinate-led matches (default: 60 arcsec).",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    init_env(root)
    state_dir = default_state_dir(root)
    core_db_path = state_dir / "served" / "current" / "core.duckdb"
    if not core_db_path.exists():
        raise SystemExit(f"Missing core database: {core_db_path}")

    run_id = utc_now().strftime("%Y-%m-%dT%H%M%SZ")
    output_dir = state_dir / "reports" / "multiplicity_crossmatch" / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    prepare_core(con, core_db_path)
    reports = []
    selected = args.catalog or ["wds", "msc", "orb6"]
    for name in selected:
        path = source_paths(state_dir)[name]
        if not path.exists():
            raise SystemExit(f"Missing source file for {name}: {path}")
        reports.append(run_one_source(con, name, path, output_dir, args.max_sep_arcsec))
        for table_name in (
            "source_rows",
            "source_prepared",
            "id_matches",
            "coord_candidates",
            "coord_matches",
            "all_matches",
            "all_matches_scored",
        ):
            con.execute(f"drop table if exists {table_name}")

    summary = {"run_id": run_id, "reports": reports, "max_sep_arcsec": args.max_sep_arcsec}
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    (output_dir / "summary.md").write_text(
        render_markdown(run_id, reports, args.max_sep_arcsec)
    )
    print(str(output_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
