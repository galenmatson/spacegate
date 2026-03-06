#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path

import duckdb


def log(msg: str) -> None:
    now = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{now} {msg}")


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def path_sql(path: Path) -> str:
    return sql_literal(str(path.resolve()))


def read_build_metadata(con: duckdb.DuckDBPyConnection, schema_name: str) -> dict[str, str]:
    rows = con.execute(f"select key, value from {schema_name}.build_metadata").fetchall()
    out: dict[str, str] = {}
    for key, value in rows:
        out[str(key)] = "" if value is None else str(value)
    return out


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--galaxy-build-id", required=True)
    parser.add_argument("--core-build-id", required=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--no-parquet", action="store_true")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    state_dir = Path(args.state_dir or os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR") or (root / "data")).resolve()
    out_dir = state_dir / "out"
    reports_dir = state_dir / "reports" / args.core_build_id

    galaxy_build_dir = out_dir / args.galaxy_build_id
    core_build_dir = out_dir / args.core_build_id
    if not galaxy_build_dir.is_dir():
        raise SystemExit(f"Galaxy build directory not found: {galaxy_build_dir}")
    if not core_build_dir.is_dir():
        raise SystemExit(f"Core build directory not found: {core_build_dir}")

    galaxy_db_path = galaxy_build_dir / "galaxy.duckdb"
    if not galaxy_db_path.exists():
        galaxy_db_path = galaxy_build_dir / "core.duckdb"
    core_db_path = core_build_dir / "core.duckdb"
    if not galaxy_db_path.is_file():
        raise SystemExit(f"Galaxy DB not found: {galaxy_db_path}")
    if not core_db_path.is_file():
        raise SystemExit(f"Core DB not found: {core_db_path}")

    halo_db_path = core_build_dir / "halo.duckdb"
    halo_tmp_path = core_build_dir / "halo.duckdb.tmp"
    if halo_db_path.exists() and not args.force:
        raise SystemExit(f"Halo DB already exists: {halo_db_path} (use --force to replace)")
    if halo_tmp_path.exists():
        halo_tmp_path.unlink()

    log(
        f"Building halo complement: galaxy={args.galaxy_build_id} core={args.core_build_id} "
        f"output={halo_db_path}"
    )
    con = duckdb.connect(str(halo_tmp_path))
    con.execute(f"ATTACH {path_sql(galaxy_db_path)} AS galaxy (READ_ONLY)")
    con.execute(f"ATTACH {path_sql(core_db_path)} AS core (READ_ONLY)")

    core_meta = read_build_metadata(con, "core")
    coord_epoch = core_meta.get("coordinate_epoch", "J2016.0")
    coord_frame = core_meta.get("coordinate_frame", "ICRS")
    profile_id = core_meta.get("slice_profile_id", "")
    profile_version = core_meta.get("slice_profile_version", "")
    transform_version = core_meta.get("git_sha", "")
    generated_at = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    con.execute(
        """
        create temp table halo_stars as
        select g.*
        from galaxy.stars g
        left join core.stars c on c.stable_object_key = g.stable_object_key
        where c.stable_object_key is null
        """
    )
    con.execute(
        """
        create temp table halo_system_ids as
        select distinct system_id
        from halo_stars
        where system_id is not null
        """
    )
    con.execute(
        """
        create temp table halo_star_ids as
        select distinct star_id
        from halo_stars
        where star_id is not null
        """
    )

    con.execute("create table stars as select * from halo_stars")
    con.execute(
        """
        create table systems as
        select s.*
        from galaxy.systems s
        join halo_system_ids hs on hs.system_id = s.system_id
        """
    )
    con.execute(
        """
        create table planets as
        select p.*
        from galaxy.planets p
        where p.star_id in (select star_id from halo_star_ids)
           or p.system_id in (select system_id from halo_system_ids)
        """
    )

    con.execute("create table build_metadata as select * from core.build_metadata")
    con.execute(
        """
        delete from build_metadata
        where key in (
          'build_layer',
          'source_galaxy_build_id',
          'source_core_build_id',
          'source_core_slice_profile_id',
          'source_core_slice_profile_version',
          'generated_at',
          'git_sha',
          'coordinate_epoch',
          'coordinate_frame',
          'slice_profile_id',
          'slice_profile_version'
        )
        """
    )
    con.execute(
        """
        insert into build_metadata values
          ('build_layer', 'halo'),
          ('source_galaxy_build_id', ?),
          ('source_core_build_id', ?),
          ('source_core_slice_profile_id', ?),
          ('source_core_slice_profile_version', ?),
          ('slice_profile_id', 'halo.complement'),
          ('slice_profile_version', 'v1'),
          ('coordinate_epoch', ?),
          ('coordinate_frame', ?),
          ('generated_at', ?),
          ('git_sha', ?)
        """,
        [
            args.galaxy_build_id,
            args.core_build_id,
            profile_id,
            profile_version,
            coord_epoch,
            coord_frame,
            generated_at,
            transform_version,
        ],
    )

    galaxy_star_count = int(con.execute("select count(*) from galaxy.stars").fetchone()[0])
    core_star_count = int(con.execute("select count(*) from core.stars").fetchone()[0])
    halo_star_count = int(con.execute("select count(*) from stars").fetchone()[0])
    halo_system_count = int(con.execute("select count(*) from systems").fetchone()[0])
    halo_planet_count = int(con.execute("select count(*) from planets").fetchone()[0])
    overlap_star_count = int(
        con.execute(
            """
            select count(*)
            from stars h
            join core.stars c on c.stable_object_key = h.stable_object_key
            """
        ).fetchone()[0]
    )

    if not args.no_parquet:
        halo_parquet_dir = core_build_dir / "parquet_halo"
        halo_parquet_dir.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"COPY (SELECT * FROM stars ORDER BY spatial_index) TO {path_sql(halo_parquet_dir / 'stars.parquet')} (FORMAT PARQUET)"
        )
        con.execute(
            f"COPY (SELECT * FROM systems ORDER BY spatial_index) TO {path_sql(halo_parquet_dir / 'systems.parquet')} (FORMAT PARQUET)"
        )
        con.execute(
            f"COPY (SELECT * FROM planets ORDER BY spatial_index) TO {path_sql(halo_parquet_dir / 'planets.parquet')} (FORMAT PARQUET)"
        )

    con.close()
    if halo_db_path.exists():
        halo_db_path.unlink()
    halo_tmp_path.rename(halo_db_path)

    galaxy_link = core_build_dir / "galaxy.duckdb"
    rel_target = os.path.relpath(galaxy_db_path, core_build_dir)
    if galaxy_link.exists() or galaxy_link.is_symlink():
        galaxy_link.unlink()
    galaxy_link.symlink_to(rel_target)

    halo_report = {
        "build_id": args.core_build_id,
        "build_layer": "halo",
        "generated_at_utc": generated_at,
        "source_galaxy_build_id": args.galaxy_build_id,
        "source_core_build_id": args.core_build_id,
        "source_core_slice_profile_id": profile_id,
        "source_core_slice_profile_version": profile_version,
        "counts": {
            "galaxy_stars": galaxy_star_count,
            "core_stars": core_star_count,
            "halo_stars": halo_star_count,
            "halo_systems": halo_system_count,
            "halo_planets": halo_planet_count,
            "halo_star_core_overlap": overlap_star_count,
        },
        "paths": {
            "galaxy_db": str(galaxy_db_path),
            "core_db": str(core_db_path),
            "halo_db": str(halo_db_path),
            "galaxy_link": str(galaxy_link),
            "halo_parquet_dir": str(core_build_dir / "parquet_halo"),
        },
        "notes": [
            "Halo is computed from stable_object_key anti-join at star level.",
            "Halo systems/planets are kept self-consistent for retained halo stars.",
            "System rows may overlap with core when mixed-inclusion systems exist.",
        ],
    }
    write_json(reports_dir / "halo_report.json", halo_report)

    log(
        f"Halo build complete: stars={halo_star_count:,} systems={halo_system_count:,} "
        f"planets={halo_planet_count:,} overlap={overlap_star_count:,}"
    )
    log(f"Halo report: {reports_dir / 'halo_report.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
