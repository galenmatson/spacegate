#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_token_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")


def state_dir(root: Path) -> Path:
    configured = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if configured:
        return Path(configured)
    shared_state = Path("/data/spacegate/data")
    if shared_state.exists():
        return shared_state
    return root / "data"


def resolve_build_dir(state: Path, build_id: str | None) -> tuple[str, Path]:
    out_dir = state / "out"
    if build_id:
        build_dir = out_dir / build_id
        if not build_dir.is_dir():
            raise SystemExit(f"Build directory not found: {build_dir}")
        return build_id, build_dir
    served = state / "served" / "current"
    if not served.exists():
        raise SystemExit("No served/current build found and no build_id was provided.")
    build_dir = served.resolve(strict=True)
    return build_dir.name, build_dir


def sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def symlink_or_copy(src: Path, dst: Path) -> None:
    if dst.exists() or dst.is_symlink():
        if dst.is_dir() and not dst.is_symlink():
            shutil.rmtree(dst)
        else:
            dst.unlink()
    try:
        dst.symlink_to(src)
    except Exception:
        if src.is_dir():
            shutil.copytree(src, dst)
        else:
            shutil.copy2(src, dst)


def build_slice(
    *,
    root: Path,
    state: Path,
    source_build_id: str,
    source_build_dir: Path,
    slice_build_id: str,
    max_distance_ly: float,
    min_parallax_over_error: float,
    trim_beyond_ly: float,
    trim_spectral: list[str],
) -> dict[str, object]:
    source_core = source_build_dir / "core.duckdb"
    if not source_core.exists():
        raise SystemExit(f"Missing source core DB: {source_core}")

    out_dir = state / "out"
    tmp_dir = out_dir / f"{slice_build_id}.tmp"
    final_dir = out_dir / slice_build_id
    reports_dir = state / "reports" / slice_build_id
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    if final_dir.exists():
        raise SystemExit(f"Target build already exists: {final_dir}")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    (tmp_dir / "ingest_v2").mkdir(parents=True, exist_ok=True)

    core_dst = tmp_dir / "core.duckdb"
    shutil.copy2(source_core, core_dst)

    for rel_path in [
        Path("arm.duckdb"),
        Path("rich.duckdb"),
        Path("rich"),
        Path("parquet"),
        Path("ingest_v2") / "canonical_hierarchy.duckdb",
    ]:
        src = source_build_dir / rel_path
        if src.exists():
            symlink_or_copy(src, tmp_dir / rel_path)

    con = duckdb.connect(str(core_dst))
    try:
        trim_spectral_sql = ", ".join(sql_literal(token.upper()) for token in trim_spectral)
        con.execute(
            f"""
            create temp table slice_trim_systems as
            with alias_named_systems as (
              select distinct system_id
              from aliases
              where system_id is not null
                and alias_kind in (
                  'proper_name', 'member_proper_name',
                  'bayer_name', 'member_bayer_name',
                  'flamsteed_name', 'member_flamsteed_name'
                )
            ), text_named_systems as (
              select system_id
              from systems
              where system_name_norm is not null
                and trim(system_name_norm) <> ''
                and system_name_norm not like 'gaia dr3 %'
                and system_name_norm not like 'gaia %'
                and system_name_norm not like 'hd %'
                and system_name_norm not like 'hip %'
                and system_name_norm not like 'hr %'
                and system_name_norm not like 'tyc %'
                and system_name_norm not like 'hyg %'
                and system_name_norm not like 'wds %'
                and system_name_norm not like 'gl %'
                and system_name_norm not like 'gj %'
              union
              select system_id
              from stars
              where system_id is not null
                and star_name_norm is not null
                and trim(star_name_norm) <> ''
                and star_name_norm not like 'gaia dr3 %'
                and star_name_norm not like 'gaia %'
                and star_name_norm not like 'hd %'
                and star_name_norm not like 'hip %'
                and star_name_norm not like 'hr %'
                and star_name_norm not like 'tyc %'
                and star_name_norm not like 'hyg %'
                and star_name_norm not like 'wds %'
                and star_name_norm not like 'gl %'
                and star_name_norm not like 'gj %'
            ), named_systems as (
              select system_id from alias_named_systems
              union
              select system_id from text_named_systems
            )
            select s.system_id
            from systems s
            left join named_systems ns using (system_id)
            where coalesce(s.dist_ly, 0) <= {max_distance_ly}
              and coalesce(s.star_count, 0) = 1
              and coalesce(s.planet_count, 0) = 0
              and coalesce(s.dist_ly, 0) > {trim_beyond_ly}
              and ns.system_id is null
              and exists (
                select 1
                from stars st
                where st.system_id = s.system_id
                  and (
                    case
                      when upper(coalesce(st.spectral_type_raw, '')) like 'D%' or coalesce(st.object_type, '') = 'white_dwarf'
                        then 'D'
                      when st.spectral_class in ('O', 'B', 'A', 'F', 'G', 'K', 'M', 'L', 'T', 'Y', 'D')
                        then st.spectral_class
                      else 'UNKNOWN'
                    end
                  ) in ({trim_spectral_sql})
              )
            """
        )
        con.execute(
            """
            create temp table slice_trim_stars as
            select star_id, system_id
            from stars
            where system_id in (select system_id from slice_trim_systems)
            """
        )
        con.execute(
            """
            create temp table slice_trim_planets as
            select planet_id, system_id
            from planets
            where system_id in (select system_id from slice_trim_systems)
            """
        )

        counts_before = con.execute(
            """
            select
              (select count(*) from systems),
              (select count(*) from stars),
              (select count(*) from planets),
              (select count(*) from aliases),
              (select count(*) from system_search_terms)
            """
        ).fetchone()
        trim_counts = con.execute(
            """
            select
              (select count(*) from slice_trim_systems),
              (select count(*) from slice_trim_stars),
              (select count(*) from slice_trim_planets),
              (select count(*) from aliases where system_id in (select system_id from slice_trim_systems)),
              (select count(*) from system_search_terms where system_id in (select system_id from slice_trim_systems))
            """
        ).fetchone()

        con.execute(
            """
            delete from object_identifiers
            where (target_type = 'star' and target_id in (select star_id from slice_trim_stars))
               or (target_type = 'system' and target_id in (select system_id from slice_trim_systems))
               or (target_type = 'planet' and target_id in (select planet_id from slice_trim_planets))
            """
        )
        for table_name in [
            "compact_objects",
            "eclipsing_binaries",
            "open_cluster_memberships",
        ]:
            exists = con.execute(
                """
                select 1
                from information_schema.tables
                where table_schema='main' and table_name=?
                """,
                [table_name],
            ).fetchone()
            if not exists:
                continue
            cols = {
                row[0]
                for row in con.execute(
                    """
                    select column_name
                    from information_schema.columns
                    where table_schema='main' and table_name=?
                    """,
                    [table_name],
                ).fetchall()
            }
            clauses: list[str] = []
            if "system_id" in cols:
                clauses.append("system_id in (select system_id from slice_trim_systems)")
            if "star_id" in cols:
                clauses.append("star_id in (select star_id from slice_trim_stars)")
            if clauses:
                con.execute(f"delete from {table_name} where {' or '.join(clauses)}")

        con.execute(
            """
            delete from aliases
            where system_id in (select system_id from slice_trim_systems)
               or star_id in (select star_id from slice_trim_stars)
               or (target_type = 'system' and target_id in (select system_id from slice_trim_systems))
               or (target_type = 'star' and target_id in (select star_id from slice_trim_stars))
               or (target_type = 'planet' and target_id in (select planet_id from slice_trim_planets))
            """
        )
        con.execute("delete from system_search_terms where system_id in (select system_id from slice_trim_systems)")
        con.execute("delete from planets where system_id in (select system_id from slice_trim_systems)")
        con.execute("delete from stars where system_id in (select system_id from slice_trim_systems)")
        con.execute("delete from systems where system_id in (select system_id from slice_trim_systems)")

        con.execute(
            """
            delete from build_metadata
            where key in (
              'build_id',
              'slice_profile_id',
              'slice_profile_version',
              'slice_max_distance_ly',
              'slice_min_parallax_over_error',
              'slice_distant_single_trim_beyond_ly',
              'slice_distant_single_trim_spectral',
              'slice_distant_single_trim_require_planetless',
              'slice_distant_single_trim_require_unnamed',
              'preview_source_build_id'
            )
            """
        )
        con.executemany(
            "insert into build_metadata values (?, ?)",
            [
                ("build_id", slice_build_id),
                ("preview_source_build_id", source_build_id),
                ("slice_profile_id", "core.public"),
                ("slice_profile_version", "v3"),
                ("slice_max_distance_ly", str(max_distance_ly)),
                ("slice_min_parallax_over_error", str(min_parallax_over_error)),
                ("slice_distant_single_trim_beyond_ly", str(trim_beyond_ly)),
                ("slice_distant_single_trim_spectral", ",".join(trim_spectral)),
                ("slice_distant_single_trim_require_planetless", "1"),
                ("slice_distant_single_trim_require_unnamed", "1"),
            ],
        )
        con.execute("checkpoint")
        con.execute("vacuum")

        counts_after = con.execute(
            """
            select
              (select count(*) from systems),
              (select count(*) from stars),
              (select count(*) from planets),
              (select count(*) from aliases),
              (select count(*) from system_search_terms)
            """
        ).fetchone()
    finally:
        con.close()

    report = {
        "generated_at": utc_now(),
        "source_build_id": source_build_id,
        "slice_build_id": slice_build_id,
        "slice_profile_id": "core.public",
        "slice_profile_version": "v3",
        "slice_policy": {
            "max_distance_ly": max_distance_ly,
            "min_parallax_over_error": min_parallax_over_error,
            "distant_single_trim_beyond_ly": trim_beyond_ly,
            "distant_single_trim_spectral_classes": trim_spectral,
            "distant_single_trim_require_planetless": True,
            "distant_single_trim_require_unnamed": True,
        },
        "counts_before": {
            "systems": int(counts_before[0]),
            "stars": int(counts_before[1]),
            "planets": int(counts_before[2]),
            "aliases": int(counts_before[3]),
            "system_search_terms": int(counts_before[4]),
        },
        "trim_counts": {
            "systems": int(trim_counts[0]),
            "stars": int(trim_counts[1]),
            "planets": int(trim_counts[2]),
            "aliases": int(trim_counts[3]),
            "system_search_terms": int(trim_counts[4]),
        },
        "counts_after": {
            "systems": int(counts_after[0]),
            "stars": int(counts_after[1]),
            "planets": int(counts_after[2]),
            "aliases": int(counts_after[3]),
            "system_search_terms": int(counts_after[4]),
        },
        "core_db_bytes": core_dst.stat().st_size,
    }

    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / "slice_policy_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_dir.rename(final_dir)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a v2-preview public slice from an existing canonical preview build.")
    parser.add_argument("--build-id", help="Source build id. Defaults to served/current.", default="")
    parser.add_argument("--slice-build-id", help="Explicit output build id.", default="")
    parser.add_argument("--max-distance-ly", type=float, default=1000.0)
    parser.add_argument("--min-parallax-over-error", type=float, default=5.0)
    parser.add_argument("--trim-beyond-ly", type=float, default=500.0)
    parser.add_argument("--trim-spectral", default="M,L,UNKNOWN")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[2]
    state = state_dir(root)
    source_build_id, source_build_dir = resolve_build_dir(state, args.build_id or None)
    slice_build_id = (
        str(args.slice_build_id).strip()
        if args.slice_build_id
        else f"{build_token_now()}_core.public.v3"
    )
    trim_spectral = [token.strip().upper() for token in str(args.trim_spectral).split(",") if token.strip()]
    payload = build_slice(
        root=root,
        state=state,
        source_build_id=source_build_id,
        source_build_dir=source_build_dir,
        slice_build_id=slice_build_id,
        max_distance_ly=float(args.max_distance_ly),
        min_parallax_over_error=float(args.min_parallax_over_error),
        trim_beyond_ly=float(args.trim_beyond_ly),
        trim_spectral=trim_spectral,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
