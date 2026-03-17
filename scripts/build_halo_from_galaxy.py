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


def has_table(con: duckdb.DuckDBPyConnection, *, alias: str, table_name: str) -> bool:
    row = con.execute(
        """
        select 1
        from information_schema.tables
        where table_catalog = ?
          and table_schema = 'main'
          and table_name = ?
        limit 1
        """,
        [alias, table_name],
    ).fetchone()
    return bool(row)


def has_local_table(con: duckdb.DuckDBPyConnection, *, table_name: str) -> bool:
    row = con.execute(
        """
        select 1
        from information_schema.tables
        where table_schema = 'main'
          and table_name = ?
        limit 1
        """,
        [table_name],
    ).fetchone()
    return bool(row)


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
    galaxy_arm_db_path = galaxy_build_dir / "arm.duckdb"
    core_arm_db_path = core_build_dir / "arm.duckdb"
    arm_projection_enabled = False
    arm_projection_notes: list[str] = []
    if galaxy_arm_db_path.is_file():
        con.execute(f"ATTACH {path_sql(galaxy_arm_db_path)} AS galaxy_arm (READ_ONLY)")
        arm_projection_enabled = True
        arm_projection_notes.append("Attached galaxy arm overlay for halo projection.")
    else:
        arm_projection_notes.append(f"Galaxy arm DB missing ({galaxy_arm_db_path}); skipping arm halo projection.")
    if core_arm_db_path.is_file():
        con.execute(f"ATTACH {path_sql(core_arm_db_path)} AS core_arm (READ_ONLY)")
        arm_projection_notes.append("Attached core arm overlay for overlap diagnostics.")
    else:
        arm_projection_notes.append("Core arm DB missing; overlap diagnostics for arm projection are unavailable.")

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

    arm_component_count = 0
    arm_hierarchy_count = 0
    arm_orbit_count = 0
    arm_solution_count = 0
    arm_barycenter_count = 0
    arm_animation_count = 0
    arm_small_body_count = 0
    arm_artificial_count = 0
    arm_overlap_component_count = 0
    if arm_projection_enabled and has_table(con, alias="galaxy_arm", table_name="component_entities"):
        log("Projecting halo arm overlay tables")
        con.execute(
            """
            create temp table halo_component_seed as
            select 'comp:star:' || stable_object_key as stable_component_key
            from stars
            union all
            select 'comp:system:' || stable_object_key as stable_component_key
            from systems
            union all
            select 'comp:planet:' || stable_object_key as stable_component_key
            from planets
            """
        )
        if has_table(con, alias="galaxy_arm", table_name="system_hierarchy_edges") and has_table(
            con, alias="galaxy_arm", table_name="orbit_edges"
        ):
            con.execute(
                """
                create temp table halo_component_keys as
                with direct_keys as (
                  select distinct ge.stable_component_key
                  from galaxy_arm.component_entities ge
                  join halo_component_seed hs on hs.stable_component_key = ge.stable_component_key
                ), hierarchy_neighbors as (
                  select distinct h.parent_component_key as stable_component_key
                  from galaxy_arm.system_hierarchy_edges h
                  where h.parent_component_key in (select stable_component_key from halo_component_seed)
                     or h.child_component_key in (select stable_component_key from halo_component_seed)
                  union
                  select distinct h.child_component_key as stable_component_key
                  from galaxy_arm.system_hierarchy_edges h
                  where h.parent_component_key in (select stable_component_key from halo_component_seed)
                     or h.child_component_key in (select stable_component_key from halo_component_seed)
                ), orbit_neighbors as (
                  select distinct o.host_component_key as stable_component_key
                  from galaxy_arm.orbit_edges o
                  where o.host_component_key in (select stable_component_key from halo_component_seed)
                     or o.primary_component_key in (select stable_component_key from halo_component_seed)
                     or o.secondary_component_key in (select stable_component_key from halo_component_seed)
                  union
                  select distinct o.primary_component_key as stable_component_key
                  from galaxy_arm.orbit_edges o
                  where o.host_component_key in (select stable_component_key from halo_component_seed)
                     or o.primary_component_key in (select stable_component_key from halo_component_seed)
                     or o.secondary_component_key in (select stable_component_key from halo_component_seed)
                  union
                  select distinct o.secondary_component_key as stable_component_key
                  from galaxy_arm.orbit_edges o
                  where o.host_component_key in (select stable_component_key from halo_component_seed)
                     or o.primary_component_key in (select stable_component_key from halo_component_seed)
                     or o.secondary_component_key in (select stable_component_key from halo_component_seed)
                )
                select distinct stable_component_key
                from (
                  select stable_component_key from direct_keys
                  union all
                  select stable_component_key from hierarchy_neighbors
                  union all
                  select stable_component_key from orbit_neighbors
                ) u
                where stable_component_key is not null
                  and trim(stable_component_key) <> ''
                """
            )
        else:
            con.execute(
                """
                create temp table halo_component_keys as
                select distinct ge.stable_component_key
                from galaxy_arm.component_entities ge
                join halo_component_seed hs on hs.stable_component_key = ge.stable_component_key
                """
            )
            arm_projection_notes.append(
                "Galaxy arm missing hierarchy/orbit edge tables; projected only direct component-entity keys."
            )

        con.execute(
            """
            create table component_entities as
            select ce.*
            from galaxy_arm.component_entities ce
            join halo_component_keys hk on hk.stable_component_key = ce.stable_component_key
            """
        )
        arm_component_count = int(con.execute("select count(*) from component_entities").fetchone()[0] or 0)

        if has_table(con, alias="galaxy_arm", table_name="system_hierarchy_edges"):
            con.execute(
                """
                create table system_hierarchy_edges as
                select h.*
                from galaxy_arm.system_hierarchy_edges h
                where h.parent_component_key in (select stable_component_key from halo_component_keys)
                  and h.child_component_key in (select stable_component_key from halo_component_keys)
                """
            )
            arm_hierarchy_count = int(
                con.execute("select count(*) from system_hierarchy_edges").fetchone()[0] or 0
            )

        if has_table(con, alias="galaxy_arm", table_name="orbit_edges"):
            con.execute(
                """
                create table orbit_edges as
                select o.*
                from galaxy_arm.orbit_edges o
                where o.host_component_key in (select stable_component_key from halo_component_keys)
                  and o.primary_component_key in (select stable_component_key from halo_component_keys)
                  and o.secondary_component_key in (select stable_component_key from halo_component_keys)
                """
            )
            arm_orbit_count = int(con.execute("select count(*) from orbit_edges").fetchone()[0] or 0)

        if has_table(con, alias="galaxy_arm", table_name="orbital_solutions") and has_local_table(
            con, table_name="orbit_edges"
        ):
            con.execute(
                """
                create table orbital_solutions as
                select s.*
                from galaxy_arm.orbital_solutions s
                join orbit_edges o on o.orbit_edge_id = s.orbit_edge_id
                """
            )
            arm_solution_count = int(
                con.execute("select count(*) from orbital_solutions").fetchone()[0] or 0
            )

        if has_table(con, alias="galaxy_arm", table_name="barycenters") and has_local_table(
            con, table_name="orbit_edges"
        ):
            con.execute(
                """
                create table barycenters as
                select b.*
                from galaxy_arm.barycenters b
                where b.barycenter_key in (
                  select distinct barycenter_key
                  from orbit_edges
                  where barycenter_key is not null
                )
                """
            )
            arm_barycenter_count = int(con.execute("select count(*) from barycenters").fetchone()[0] or 0)

        if has_table(con, alias="galaxy_arm", table_name="animation_readiness"):
            if has_local_table(con, table_name="orbit_edges"):
                con.execute(
                    """
                    create table animation_readiness as
                    select a.*
                    from galaxy_arm.animation_readiness a
                    where a.component_key in (select stable_component_key from halo_component_keys)
                       or a.orbit_edge_id in (select orbit_edge_id from orbit_edges)
                    """
                )
            else:
                con.execute(
                    """
                    create table animation_readiness as
                    select a.*
                    from galaxy_arm.animation_readiness a
                    where a.component_key in (select stable_component_key from halo_component_keys)
                    """
                )
            arm_animation_count = int(
                con.execute("select count(*) from animation_readiness").fetchone()[0] or 0
            )

        if has_table(con, alias="galaxy_arm", table_name="sol_small_body_objects"):
            con.execute(
                """
                create table sol_small_body_objects as
                select s.*
                from galaxy_arm.sol_small_body_objects s
                where s.stable_component_key in (select stable_component_key from halo_component_keys)
                   or s.host_component_key in (select stable_component_key from halo_component_keys)
                   or s.primary_component_key in (select stable_component_key from halo_component_keys)
                   or s.secondary_component_key in (select stable_component_key from halo_component_keys)
                """
            )
            arm_small_body_count = int(
                con.execute("select count(*) from sol_small_body_objects").fetchone()[0] or 0
            )

        if has_table(con, alias="galaxy_arm", table_name="sol_artificial_objects"):
            con.execute(
                """
                create table sol_artificial_objects as
                select s.*
                from galaxy_arm.sol_artificial_objects s
                where s.stable_component_key in (select stable_component_key from halo_component_keys)
                   or s.host_component_key in (select stable_component_key from halo_component_keys)
                   or s.primary_component_key in (select stable_component_key from halo_component_keys)
                   or s.secondary_component_key in (select stable_component_key from halo_component_keys)
                """
            )
            arm_artificial_count = int(
                con.execute("select count(*) from sol_artificial_objects").fetchone()[0] or 0
            )

        if has_table(con, alias="core_arm", table_name="component_entities") and has_local_table(
            con, table_name="component_entities"
        ):
            arm_overlap_component_count = int(
                con.execute(
                    """
                    select count(*)
                    from component_entities h
                    join core_arm.component_entities c
                      on c.stable_component_key = h.stable_component_key
                    """
                ).fetchone()[0]
                or 0
            )
    elif arm_projection_enabled:
        arm_projection_notes.append("Galaxy arm overlay is missing component_entities table; skipped.")

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
        halo_arm_parquet_dir = halo_parquet_dir / "arm"
        if has_local_table(con, table_name="component_entities"):
            halo_arm_parquet_dir.mkdir(parents=True, exist_ok=True)
            con.execute(
                f"COPY (SELECT * FROM component_entities ORDER BY stable_component_key) TO {path_sql(halo_arm_parquet_dir / 'component_entities.parquet')} (FORMAT PARQUET)"
            )
        if has_local_table(con, table_name="system_hierarchy_edges"):
            halo_arm_parquet_dir.mkdir(parents=True, exist_ok=True)
            con.execute(
                f"COPY (SELECT * FROM system_hierarchy_edges ORDER BY hierarchy_edge_id) TO {path_sql(halo_arm_parquet_dir / 'system_hierarchy_edges.parquet')} (FORMAT PARQUET)"
            )
        if has_local_table(con, table_name="orbit_edges"):
            halo_arm_parquet_dir.mkdir(parents=True, exist_ok=True)
            con.execute(
                f"COPY (SELECT * FROM orbit_edges ORDER BY orbit_edge_id) TO {path_sql(halo_arm_parquet_dir / 'orbit_edges.parquet')} (FORMAT PARQUET)"
            )
        if has_local_table(con, table_name="orbital_solutions"):
            halo_arm_parquet_dir.mkdir(parents=True, exist_ok=True)
            con.execute(
                f"COPY (SELECT * FROM orbital_solutions ORDER BY orbital_solution_id) TO {path_sql(halo_arm_parquet_dir / 'orbital_solutions.parquet')} (FORMAT PARQUET)"
            )
        if has_local_table(con, table_name="barycenters"):
            halo_arm_parquet_dir.mkdir(parents=True, exist_ok=True)
            con.execute(
                f"COPY (SELECT * FROM barycenters ORDER BY barycenter_id) TO {path_sql(halo_arm_parquet_dir / 'barycenters.parquet')} (FORMAT PARQUET)"
            )
        if has_local_table(con, table_name="animation_readiness"):
            halo_arm_parquet_dir.mkdir(parents=True, exist_ok=True)
            con.execute(
                f"COPY (SELECT * FROM animation_readiness ORDER BY animation_readiness_id) TO {path_sql(halo_arm_parquet_dir / 'animation_readiness.parquet')} (FORMAT PARQUET)"
            )
        if has_local_table(con, table_name="sol_small_body_objects"):
            halo_arm_parquet_dir.mkdir(parents=True, exist_ok=True)
            con.execute(
                f"COPY (SELECT * FROM sol_small_body_objects ORDER BY sol_small_body_id) TO {path_sql(halo_arm_parquet_dir / 'sol_small_body_objects.parquet')} (FORMAT PARQUET)"
            )
        if has_local_table(con, table_name="sol_artificial_objects"):
            halo_arm_parquet_dir.mkdir(parents=True, exist_ok=True)
            con.execute(
                f"COPY (SELECT * FROM sol_artificial_objects ORDER BY sol_artificial_id) TO {path_sql(halo_arm_parquet_dir / 'sol_artificial_objects.parquet')} (FORMAT PARQUET)"
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
            "halo_arm_components": arm_component_count,
            "halo_arm_hierarchy_edges": arm_hierarchy_count,
            "halo_arm_orbit_edges": arm_orbit_count,
            "halo_arm_orbital_solutions": arm_solution_count,
            "halo_arm_barycenters": arm_barycenter_count,
            "halo_arm_animation_readiness": arm_animation_count,
            "halo_arm_sol_small_bodies": arm_small_body_count,
            "halo_arm_sol_artificial": arm_artificial_count,
            "halo_arm_component_core_overlap": arm_overlap_component_count,
        },
        "paths": {
            "galaxy_db": str(galaxy_db_path),
            "core_db": str(core_db_path),
            "halo_db": str(halo_db_path),
            "galaxy_link": str(galaxy_link),
            "halo_parquet_dir": str(core_build_dir / "parquet_halo"),
            "galaxy_arm_db": str(galaxy_arm_db_path),
            "core_arm_db": str(core_arm_db_path),
        },
        "notes": [
            "Halo is computed from stable_object_key anti-join at star level.",
            "Halo systems/planets are kept self-consistent for retained halo stars.",
            "System rows may overlap with core when mixed-inclusion systems exist.",
            *arm_projection_notes,
        ],
    }
    write_json(reports_dir / "halo_report.json", halo_report)

    log(
        f"Halo build complete: stars={halo_star_count:,} systems={halo_system_count:,} "
        f"planets={halo_planet_count:,} overlap={overlap_star_count:,} "
        f"arm_components={arm_component_count:,} arm_orbits={arm_orbit_count:,}"
    )
    log(f"Halo report: {reports_dir / 'halo_report.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
