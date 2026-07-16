#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

TESS_CANONICAL_PROJECTION_VERSION = "tess_canonical_arm_projection_v1"
TESS_PROJECTION_TABLES = {
    "tess_target_identity",
    "tess_missing_object_audit",
    "toi_current_evidence",
    "toi_disposition_history",
}


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


def table_exists(con: duckdb.DuckDBPyConnection, alias: str, table_name: str) -> bool:
    return bool(
        con.execute(
            """
            select 1
            from information_schema.tables
            where table_catalog = ? and table_schema = 'main' and table_name = ?
            limit 1
            """,
            [alias, table_name],
        ).fetchone()
    )


def table_columns(con: duckdb.DuckDBPyConnection, alias: str, table_name: str) -> set[str]:
    rows = con.execute(f"describe {alias}.{table_name}").fetchall()
    return {str(row[0]) for row in rows}


def count_table(con: duckdb.DuckDBPyConnection, table_name: str, alias: str = "main") -> int:
    return int(con.execute(f"select count(*)::bigint from {alias}.{table_name}").fetchone()[0] or 0)


def verify_sliced_tess_projection(con: duckdb.DuckDBPyConnection) -> dict[str, dict[str, int]]:
    comparisons = {
        "tess_target_identity": (
            "select * exclude(tess_identity_id, ingested_at) from tess_target_identity",
            """
            select * exclude(tess_identity_id, ingested_at)
            from src.tess_target_identity t
            where t.resolution_status <> 'accepted'
               or (
                 t.star_id in (select star_id from core.stars)
                 and t.system_id in (select system_id from core.systems)
               )
            """,
        ),
        "tess_missing_object_audit": (
            "select * exclude(audit_id) from tess_missing_object_audit",
            "select * exclude(audit_id) from src.tess_missing_object_audit",
        ),
        "toi_current_evidence": (
            "select * exclude(toi_evidence_id, ingested_at) from toi_current_evidence",
            """
            select * exclude(toi_evidence_id, ingested_at)
            from src.toi_current_evidence t
            where (t.system_id is null or t.system_id in (select system_id from core.systems))
              and (t.star_id is null or t.star_id in (select star_id from core.stars))
              and (t.planet_id is null or t.planet_id in (select planet_id from core.planets))
            """,
        ),
        "toi_disposition_history": (
            "select * exclude(history_id, ingested_at) from toi_disposition_history",
            "select * exclude(history_id, ingested_at) from src.toi_disposition_history",
        ),
    }
    report: dict[str, dict[str, int]] = {}
    failures: list[str] = []
    for table_name, (actual_sql, expected_sql) in comparisons.items():
        unexpected = int(
            con.execute(f"select count(*) from ({actual_sql} except {expected_sql})").fetchone()[0]
        )
        missing = int(
            con.execute(f"select count(*) from ({expected_sql} except {actual_sql})").fetchone()[0]
        )
        report[table_name] = {"unexpected": unexpected, "missing": missing}
        if unexpected or missing:
            failures.append(f"{table_name}: unexpected={unexpected}, missing={missing}")
    if failures:
        raise RuntimeError("Canonical TESS slice projection mismatch: " + "; ".join(failures))
    return report


def mark_tess_projection_metadata(
    con: duckdb.DuckDBPyConnection,
    *,
    source_arm: Path,
    source_build_id: str,
) -> None:
    source_metadata = {
        str(key): "" if value is None else str(value)
        for key, value in con.execute("select key, value from src.build_metadata").fetchall()
    }
    canonical_arm = source_arm.resolve()
    if source_metadata.get("arm_tess_identity_mode") == "canonical_projection":
        inherited_source = source_metadata.get("arm_tess_identity_source_arm", "").strip()
        if not inherited_source:
            raise RuntimeError("Source TESS projection metadata has no canonical ARM path")
        canonical_arm = Path(inherited_source)
    if not canonical_arm.is_file():
        raise RuntimeError(f"Canonical TESS projection source is missing: {canonical_arm}")

    con.execute(
        """
        delete from build_metadata
        where key in (
          'arm_tess_identity_mode',
          'arm_tess_identity_projection_version',
          'arm_tess_identity_source_arm',
          'arm_tess_identity_source_build_id'
        )
        """
    )
    con.executemany(
        "insert into build_metadata values (?, ?)",
        [
            ("arm_tess_identity_mode", "canonical_projection"),
            ("arm_tess_identity_projection_version", TESS_CANONICAL_PROJECTION_VERSION),
            ("arm_tess_identity_source_arm", str(canonical_arm)),
            ("arm_tess_identity_source_build_id", source_build_id),
        ],
    )


def emit_build_metadata(
    con: duckdb.DuckDBPyConnection,
    *,
    source_alias: str,
    slice_build_id: str,
    source_build_id: str,
    artifact_kind: str,
) -> None:
    if table_exists(con, source_alias, "build_metadata"):
        con.execute(f"create table build_metadata as select * from {source_alias}.build_metadata")
        columns = table_columns(con, "main", "build_metadata")
        if not {"key", "value"}.issubset(columns):
            return
        con.execute(
            """
            delete from build_metadata
            where key in (
              'build_id',
              'bootstrap_source_build_id',
              'slice_side_artifacts_sliced',
              'slice_side_artifact_kind'
            )
            """
        )
    else:
        con.execute("create table build_metadata(key varchar, value varchar)")
    con.executemany(
        "insert into build_metadata values (?, ?)",
        [
            ("build_id", slice_build_id),
            ("bootstrap_source_build_id", source_build_id),
            ("slice_side_artifacts_sliced", "1"),
            ("slice_side_artifact_kind", artifact_kind),
        ],
    )


def generic_core_retention_predicate(columns: set[str], *, component_column: str = "stable_component_key") -> str:
    predicates: list[str] = []
    if "wds_id" in columns:
        predicates.append("(wds_id is null or wds_id in (select wds_id from retained_wds_ids))")
    if "system_id" in columns:
        predicates.append("(system_id is null or system_id in (select system_id from core.systems))")
    if "star_id" in columns:
        predicates.append("(star_id is null or star_id in (select star_id from core.stars))")
    if "planet_id" in columns:
        predicates.append("(planet_id is null or planet_id in (select planet_id from core.planets))")
    if "stable_object_key" in columns:
        predicates.append(
            "(stable_object_key is null or stable_object_key in (select stable_object_key from retained_stable_object_keys))"
        )
    if component_column in columns:
        predicates.append(
            f"({component_column} is null or {component_column} in (select stable_component_key from retained_component_keys))"
        )
    if "orbit_edge_id" in columns:
        predicates.append("(orbit_edge_id is null or orbit_edge_id in (select orbit_edge_id from retained_orbit_edge_ids))")
    for col in ("host_component_key", "primary_component_key", "secondary_component_key"):
        if col in columns:
            predicates.append(f"({col} is null or {col} in (select stable_component_key from retained_component_keys))")
    return " and ".join(predicates) if predicates else "true"


def build_sliced_arm(
    *,
    source_build_dir: Path,
    tmp_dir: Path,
    core_dst: Path,
    source_build_id: str,
    slice_build_id: str,
) -> dict[str, object] | None:
    source_arm = source_build_dir / "arm.duckdb"
    if not source_arm.exists():
        return None

    arm_dst = tmp_dir / "arm.duckdb"
    con = duckdb.connect(str(arm_dst))
    table_report: dict[str, dict[str, int]] = {}
    try:
        con.execute(f"attach {sql_literal(str(source_arm))} as src (read_only)")
        con.execute(f"attach {sql_literal(str(core_dst))} as core (read_only)")
        emit_build_metadata(
            con,
            source_alias="src",
            slice_build_id=slice_build_id,
            source_build_id=source_build_id,
            artifact_kind="arm",
        )

        con.execute(
            """
            create temp table retained_stable_object_keys as
            select stable_object_key from core.systems where stable_object_key is not null
            union
            select stable_object_key from core.stars where stable_object_key is not null
            union
            select stable_object_key from core.planets where stable_object_key is not null
            """
        )
        con.execute(
            """
            create temp table retained_wds_ids as
            select distinct wds_id
            from core.systems
            where wds_id is not null
            """
        )
        con.execute(
            """
            create temp table retained_sol_component_keys as
            select stable_component_key
            from src.sol_small_body_objects
            where stable_component_key is not null
            union
            select host_component_key
            from src.sol_small_body_objects
            where host_component_key is not null
            union
            select primary_component_key
            from src.sol_small_body_objects
            where primary_component_key is not null
            union
            select secondary_component_key
            from src.sol_small_body_objects
            where secondary_component_key is not null
            union
            select stable_component_key
            from src.sol_artificial_objects
            where stable_component_key is not null
            union
            select host_component_key
            from src.sol_artificial_objects
            where host_component_key is not null
            union
            select primary_component_key
            from src.sol_artificial_objects
            where primary_component_key is not null
            union
            select secondary_component_key
            from src.sol_artificial_objects
            where secondary_component_key is not null
            """
        )
        con.execute(
            """
            create temp table retained_component_keys as
            select stable_component_key
            from (
              select ce.stable_component_key
              from src.component_entities ce
              where ce.stable_component_key is not null
                and (
                  (ce.core_object_type = 'system' and ce.core_object_id in (select system_id from core.systems))
                  or (ce.core_object_type = 'star' and ce.core_object_id in (select star_id from core.stars))
                  or (ce.core_object_type in ('planet', 'subplanet') and ce.core_object_id in (select planet_id from core.planets))
                  or ce.stable_component_key in (select stable_object_key from retained_stable_object_keys)
                )
              union
              select stable_component_key
              from retained_sol_component_keys
              union
              select ce.stable_component_key
              from src.component_entities ce
              where ce.stable_component_key in (select stable_component_key from retained_sol_component_keys)
              union
              select ce.stable_component_key
              from src.component_entities ce
              join retained_wds_ids w
                on split_part(split_part(ce.stable_component_key, 'wds:', 2), ':', 1) = w.wds_id
              where ce.stable_component_key is not null
                and ce.source_catalog = 'msc'
            )
            """
        )
        for _ in range(8):
            con.execute(
                """
                create or replace temp table new_retained_component_keys as
                select distinct h.child_component_key as stable_component_key
                from src.system_hierarchy_edges h
                join retained_component_keys parent
                  on parent.stable_component_key = h.parent_component_key
                left join retained_component_keys existing
                  on existing.stable_component_key = h.child_component_key
                where h.child_component_key is not null
                  and existing.stable_component_key is null
                """
            )
            new_count = count_table(con, "new_retained_component_keys")
            if new_count == 0:
                break
            con.execute(
                """
                insert into retained_component_keys
                select stable_component_key
                from new_retained_component_keys
                """
            )
        con.execute(
            """
            create temp table retained_orbit_edge_ids as
            select oe.orbit_edge_id
            from src.orbit_edges oe
            where oe.orbit_edge_id is not null
              and (oe.host_component_key is null or oe.host_component_key in (select stable_component_key from retained_component_keys))
              and (oe.primary_component_key is null or oe.primary_component_key in (select stable_component_key from retained_component_keys))
              and (oe.secondary_component_key is null or oe.secondary_component_key in (select stable_component_key from retained_component_keys))
            """
        )
        con.execute(
            """
            create temp table retained_barycenter_keys as
            select distinct barycenter_key
            from src.orbit_edges
            where barycenter_key is not null
              and orbit_edge_id in (select orbit_edge_id from retained_orbit_edge_ids)
            """
        )

        table_names = [
            row[0]
            for row in con.execute(
                """
                select table_name
                from information_schema.tables
                where table_catalog = 'src' and table_schema = 'main'
                order by table_name
                """
            ).fetchall()
        ]
        predicates = {
            "build_metadata": None,
            "component_entities": "stable_component_key in (select stable_component_key from retained_component_keys)",
            "system_hierarchy_edges": (
                "parent_component_key in (select stable_component_key from retained_component_keys) "
                "and child_component_key in (select stable_component_key from retained_component_keys)"
            ),
            "orbit_edges": "orbit_edge_id in (select orbit_edge_id from retained_orbit_edge_ids)",
            "orbital_solutions": "orbit_edge_id in (select orbit_edge_id from retained_orbit_edge_ids)",
            "barycenters": "true",
            "animation_readiness": (
                "(component_key is not null and component_key in (select stable_component_key from retained_component_keys)) "
                "or (orbit_edge_id is not null and orbit_edge_id in (select orbit_edge_id from retained_orbit_edge_ids))"
            ),
            "msc_system_details": (
                "wds_id in (select wds_id from retained_wds_ids) "
                "or coalesce(parent_component_key, '') in (select stable_component_key from retained_component_keys) "
                "or coalesce(primary_component_key, '') in (select stable_component_key from retained_component_keys) "
                "or coalesce(secondary_component_key, '') in (select stable_component_key from retained_component_keys)"
            ),
            "msc_orbit_details": (
                "wds_id in (select wds_id from retained_wds_ids) "
                "or coalesce(host_component_key, '') in (select stable_component_key from retained_component_keys) "
                "or coalesce(primary_component_key, '') in (select stable_component_key from retained_component_keys) "
                "or coalesce(secondary_component_key, '') in (select stable_component_key from retained_component_keys)"
            ),
            "msc_orbit_reconciliation": "wds_id in (select wds_id from retained_wds_ids)",
            "wds_component_observations": "wds_id in (select wds_id from retained_wds_ids)",
            "wds_pair_evidence": "wds_id in (select wds_id from retained_wds_ids)",
            "infrared_source_matches": (
                "system_id in (select system_id from core.systems) and ("
                "(lower(target_type) = 'star' and target_id in (select star_id from core.stars)) or "
                "(lower(target_type) = 'system' and target_id in (select system_id from core.systems))"
                ")"
            ),
            "sol_small_body_objects": "true",
            "sol_artificial_objects": "true",
        }

        for table_name in table_names:
            if table_name == "build_metadata":
                table_report[table_name] = {"before": count_table(con, table_name, alias="src"), "after": count_table(con, table_name)}
                continue
            columns = table_columns(con, "src", table_name)
            predicate = predicates.get(table_name)
            if predicate is None:
                predicate = generic_core_retention_predicate(columns)
            con.execute(f"create table {table_name} as select * from src.{table_name} where {predicate}")
            table_report[table_name] = {
                "before": count_table(con, table_name, alias="src"),
                "after": count_table(con, table_name),
            }
        if TESS_PROJECTION_TABLES.issubset(table_names):
            table_report["tess_canonical_projection"] = verify_sliced_tess_projection(con)
            mark_tess_projection_metadata(
                con,
                source_arm=source_arm,
                source_build_id=source_build_id,
            )
        con.execute("checkpoint")
        con.execute("vacuum")
    finally:
        con.close()

    return {
        "db_bytes": arm_dst.stat().st_size,
        "tables": table_report,
    }


def build_sliced_canonical_hierarchy(
    *,
    source_build_dir: Path,
    tmp_dir: Path,
    core_dst: Path,
    source_build_id: str,
    slice_build_id: str,
) -> dict[str, object] | None:
    source_hierarchy = source_build_dir / "canonical_hierarchy.duckdb"
    if not source_hierarchy.exists():
        return None

    hierarchy_dst = tmp_dir / "canonical_hierarchy.duckdb"
    con = duckdb.connect(str(hierarchy_dst))
    try:
        con.execute(f"attach {sql_literal(str(source_hierarchy))} as src (read_only)")
        con.execute(f"attach {sql_literal(str(core_dst))} as core (read_only)")
        emit_build_metadata(
            con,
            source_alias="src",
            slice_build_id=slice_build_id,
            source_build_id=source_build_id,
            artifact_kind="canonical_hierarchy",
        )
        con.execute(
            """
            create temp table retained_hierarchy_node_keys as
            select distinct hn.hierarchy_node_key
            from src.hierarchy_nodes hn
            where hn.hierarchy_node_key is not null
              and (
                hn.hierarchy_node_key in (select stable_object_key from core.systems)
                or hn.canonical_key in (select stable_object_key from core.systems)
                or hn.canonical_key in (select stable_object_key from core.stars)
                or hn.canonical_key in (select stable_object_key from core.planets)
              )
            """
        )
        for _ in range(8):
            con.execute(
                """
                create or replace temp table new_retained_hierarchy_node_keys as
                select distinct e.child_node_key as hierarchy_node_key
                from src.hierarchy_edges e
                join retained_hierarchy_node_keys parent
                  on parent.hierarchy_node_key = e.parent_node_key
                left join retained_hierarchy_node_keys existing
                  on existing.hierarchy_node_key = e.child_node_key
                where e.child_node_key is not null
                  and existing.hierarchy_node_key is null
                """
            )
            new_count = count_table(con, "new_retained_hierarchy_node_keys")
            if new_count == 0:
                break
            con.execute(
                """
                insert into retained_hierarchy_node_keys
                select hierarchy_node_key
                from new_retained_hierarchy_node_keys
                """
            )
        nodes_before = count_table(con, "hierarchy_nodes", alias="src")
        edges_before = count_table(con, "hierarchy_edges", alias="src")
        con.execute(
            """
            create table hierarchy_nodes as
            select *
            from src.hierarchy_nodes
            where hierarchy_node_key in (select hierarchy_node_key from retained_hierarchy_node_keys)
            """
        )
        con.execute(
            """
            create table hierarchy_edges as
            select *
            from src.hierarchy_edges
            where parent_node_key in (select hierarchy_node_key from retained_hierarchy_node_keys)
              and child_node_key in (select hierarchy_node_key from retained_hierarchy_node_keys)
            """
        )
        con.execute("checkpoint")
        con.execute("vacuum")
        return {
            "db_bytes": hierarchy_dst.stat().st_size,
            "tables": {
                "hierarchy_nodes": {"before": nodes_before, "after": count_table(con, "hierarchy_nodes")},
                "hierarchy_edges": {"before": edges_before, "after": count_table(con, "hierarchy_edges")},
            },
        }
    finally:
        con.close()


def build_sliced_disc(
    *,
    source_build_dir: Path,
    tmp_dir: Path,
    core_dst: Path,
    source_build_id: str,
    slice_build_id: str,
) -> dict[str, object] | None:
    source_disc = source_build_dir / "disc.duckdb"
    if not source_disc.exists():
        return None

    disc_dst = tmp_dir / "disc.duckdb"
    disc_parquet_dir = tmp_dir / "disc"
    disc_parquet_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(disc_dst))
    table_report: dict[str, dict[str, int]] = {}
    try:
        con.execute(f"attach {sql_literal(str(source_disc))} as src (read_only)")
        con.execute(f"attach {sql_literal(str(core_dst))} as core (read_only)")
        con.execute(
            """
            create temp table retained_stable_object_keys as
            select stable_object_key from core.systems where stable_object_key is not null
            union
            select stable_object_key from core.stars where stable_object_key is not null
            union
            select stable_object_key from core.planets where stable_object_key is not null
            """
        )
        table_names = [
            row[0]
            for row in con.execute(
                """
                select table_name
                from information_schema.tables
                where table_catalog = 'src' and table_schema = 'main'
                order by table_name
                """
            ).fetchall()
        ]
        for table_name in table_names:
            if table_name == "build_metadata":
                emit_build_metadata(
                    con,
                    source_alias="src",
                    slice_build_id=slice_build_id,
                    source_build_id=source_build_id,
                    artifact_kind="disc",
                )
                table_report[table_name] = {"before": count_table(con, table_name, alias="src"), "after": count_table(con, table_name)}
                continue
            columns = table_columns(con, "src", table_name)
            predicates: list[str] = []
            if "system_id" in columns:
                predicates.append("(system_id is null or system_id in (select system_id from core.systems))")
            if "star_id" in columns:
                predicates.append("(star_id is null or star_id in (select star_id from core.stars))")
            if "planet_id" in columns:
                predicates.append("(planet_id is null or planet_id in (select planet_id from core.planets))")
            if "stable_object_key" in columns:
                predicates.append(
                    "(stable_object_key is null or stable_object_key in (select stable_object_key from retained_stable_object_keys))"
                )
            predicate = " and ".join(predicates) if predicates else "true"
            con.execute(f"create table {table_name} as select * from src.{table_name} where {predicate}")
            table_report[table_name] = {
                "before": count_table(con, table_name, alias="src"),
                "after": count_table(con, table_name),
            }
            con.execute(
                f"copy (select * from {table_name}) to {sql_literal(str(disc_parquet_dir / f'{table_name}.parquet'))} (format parquet)"
            )
        if "build_metadata" not in table_names:
            con.execute("create table build_metadata(key varchar, value varchar)")
            con.executemany(
                "insert into build_metadata values (?, ?)",
                [
                    ("build_id", slice_build_id),
                    ("bootstrap_source_build_id", source_build_id),
                    ("slice_side_artifacts_sliced", "1"),
                    ("slice_side_artifact_kind", "disc"),
                ],
            )
        con.execute("checkpoint")
        con.execute("vacuum")
    finally:
        con.close()

    return {
        "db_bytes": disc_dst.stat().st_size,
        "parquet_bytes": sum(path.stat().st_size for path in disc_parquet_dir.glob("*.parquet")),
        "tables": table_report,
    }


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
    (tmp_dir / "ingest").mkdir(parents=True, exist_ok=True)
    (tmp_dir / "parquet").mkdir(parents=True, exist_ok=True)

    core_dst = tmp_dir / "core.duckdb"
    con = duckdb.connect(str(core_dst))
    try:
        con.execute(f"ATTACH {sql_literal(str(source_core))} AS src (READ_ONLY)")
        trim_spectral_sql = ", ".join(sql_literal(token.upper()) for token in trim_spectral)
        con.execute(
            f"""
            create temp table slice_trim_systems as
            with alias_named_systems as (
              select distinct system_id
              from src.aliases
              where system_id is not null
                and alias_kind in (
                  'proper_name', 'member_proper_name',
                  'bayer_name', 'member_bayer_name',
                  'flamsteed_name', 'member_flamsteed_name'
                )
            ), text_named_systems as (
              select system_id
              from src.systems
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
              from src.stars
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
            from src.systems s
            left join named_systems ns using (system_id)
            where coalesce(s.dist_ly, 0) <= {max_distance_ly}
              and coalesce(s.star_count, 0) = 1
              and coalesce(s.planet_count, 0) = 0
              and coalesce(s.dist_ly, 0) > {trim_beyond_ly}
              and ns.system_id is null
              and exists (
                select 1
                from src.stars st
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
            from src.stars
            where system_id in (select system_id from slice_trim_systems)
            """
        )
        con.execute(
            """
            create temp table slice_trim_planets as
            select planet_id, system_id
            from src.planets
            where system_id in (select system_id from slice_trim_systems)
            """
        )

        counts_before = con.execute(
            """
            select
              (select count(*) from src.systems),
              (select count(*) from src.stars),
              (select count(*) from src.planets),
              (select count(*) from src.aliases),
              (select count(*) from src.system_search_terms)
            """
        ).fetchone()
        trim_counts = con.execute(
            """
            select
              (select count(*) from slice_trim_systems),
              (select count(*) from slice_trim_stars),
              (select count(*) from slice_trim_planets),
              (select count(*) from src.aliases where system_id in (select system_id from slice_trim_systems)),
              (select count(*) from src.system_search_terms where system_id in (select system_id from slice_trim_systems))
            """
        ).fetchone()

        con.execute("create table build_metadata as select * from src.build_metadata")
        con.execute(
            """
            delete from build_metadata
            where key in (
              'build_id',
              'bootstrap_source_build_id',
              'slice_profile_id',
              'slice_profile_version',
              'slice_max_distance_ly',
              'slice_min_parallax_over_error',
              'slice_distant_single_trim_beyond_ly',
              'slice_distant_single_trim_spectral',
              'slice_distant_single_trim_require_planetless',
              'slice_distant_single_trim_require_unnamed'
            )
            """
        )
        con.execute(
            """
            create table systems as
            select *
            from src.systems
            where system_id not in (select system_id from slice_trim_systems)
            """
        )
        con.execute(
            """
            create table stars as
            select *
            from src.stars
            where system_id is null
               or system_id not in (select system_id from slice_trim_systems)
            """
        )
        con.execute(
            """
            create table planets as
            select *
            from src.planets
            where system_id is null
               or system_id not in (select system_id from slice_trim_systems)
            """
        )
        con.execute(
            """
            create table aliases as
            select *
            from src.aliases
            where (system_id is null or system_id not in (select system_id from slice_trim_systems))
              and coalesce(star_id, -1) not in (select star_id from slice_trim_stars)
              and not (target_type = 'system' and target_id in (select system_id from slice_trim_systems))
              and not (target_type = 'star' and target_id in (select star_id from slice_trim_stars))
              and not (target_type = 'planet' and target_id in (select planet_id from slice_trim_planets))
            """
        )
        con.execute(
            """
            create table system_search_terms as
            select *
            from src.system_search_terms
            where system_id is null
               or system_id not in (select system_id from slice_trim_systems)
            """
        )
        con.execute(
            """
            create table object_identifiers as
            select *
            from src.object_identifiers
            where not (target_type = 'star' and target_id in (select star_id from slice_trim_stars))
              and not (target_type = 'system' and target_id in (select system_id from slice_trim_systems))
              and not (target_type = 'planet' and target_id in (select planet_id from slice_trim_planets))
            """
        )
        con.execute("create table identifier_quarantine as select * from src.identifier_quarantine")
        con.execute(
            """
            create table compact_objects as
            select *
            from src.compact_objects
            where (system_id is null or system_id not in (select system_id from slice_trim_systems))
              and coalesce(star_id, -1) not in (select star_id from slice_trim_stars)
            """
        )
        con.execute(
            """
            create table eclipsing_binaries as
            select *
            from src.eclipsing_binaries
            where (system_id is null or system_id not in (select system_id from slice_trim_systems))
              and coalesce(star_id, -1) not in (select star_id from slice_trim_stars)
            """
        )
        con.execute(
            """
            create table open_cluster_memberships as
            select *
            from src.open_cluster_memberships
            where (system_id is null or system_id not in (select system_id from slice_trim_systems))
              and coalesce(star_id, -1) not in (select star_id from slice_trim_stars)
            """
        )
        con.execute("create table open_clusters as select * from src.open_clusters")
        con.execute(
            "create table planet_catalog_observations as select * from src.planet_catalog_observations"
        )
        con.execute(
            "create table planet_reclassification_audit as select * from src.planet_reclassification_audit"
        )
        con.execute(
            "create table planet_status_history as select * from src.planet_status_history"
        )
        con.execute("create table superstellar_objects as select * from src.superstellar_objects")
        for table_name in (
            "extended_objects",
            "extended_object_aliases",
            "extended_object_identifiers",
            "extended_object_search_terms",
            "extended_object_source_reconciliation",
            "extended_object_identity_quarantine",
        ):
            if table_exists(con, "src", table_name):
                con.execute(f"create table {table_name} as select * from src.{table_name}")

        con.executemany(
            "insert into build_metadata values (?, ?)",
            [
                ("build_id", slice_build_id),
                ("bootstrap_source_build_id", source_build_id),
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

        parquet_dir = tmp_dir / "parquet"
        con.execute(
            f"copy (select * from stars order by spatial_index) to {sql_literal(str(parquet_dir / 'stars.parquet'))} (format parquet)"
        )
        con.execute(
            f"copy (select * from systems order by spatial_index) to {sql_literal(str(parquet_dir / 'systems.parquet'))} (format parquet)"
        )
        con.execute(
            f"copy (select * from planets order by spatial_index) to {sql_literal(str(parquet_dir / 'planets.parquet'))} (format parquet)"
        )
        con.execute(
            f"copy (select * from aliases) to {sql_literal(str(parquet_dir / 'aliases.parquet'))} (format parquet)"
        )
        con.execute(
            f"copy (select * from system_search_terms) to {sql_literal(str(parquet_dir / 'system_search_terms.parquet'))} (format parquet)"
        )
        con.execute(
            f"copy (select * from object_identifiers) to {sql_literal(str(parquet_dir / 'object_identifiers.parquet'))} (format parquet)"
        )
        con.execute(
            f"copy (select * from identifier_quarantine) to {sql_literal(str(parquet_dir / 'identifier_quarantine.parquet'))} (format parquet)"
        )
        for table_name in (
            "extended_objects",
            "extended_object_aliases",
            "extended_object_identifiers",
            "extended_object_search_terms",
            "extended_object_source_reconciliation",
            "extended_object_identity_quarantine",
        ):
            if table_exists(con, "main", table_name):
                con.execute(
                    f"copy (select * from {table_name}) to {sql_literal(str(parquet_dir / f'{table_name}.parquet'))} (format parquet)"
                )

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

    side_artifacts = {
        "arm": build_sliced_arm(
            source_build_dir=source_build_dir,
            tmp_dir=tmp_dir,
            core_dst=core_dst,
            source_build_id=source_build_id,
            slice_build_id=slice_build_id,
        ),
        "canonical_hierarchy": build_sliced_canonical_hierarchy(
            source_build_dir=source_build_dir,
            tmp_dir=tmp_dir,
            core_dst=core_dst,
            source_build_id=source_build_id,
            slice_build_id=slice_build_id,
        ),
        "disc": build_sliced_disc(
            source_build_dir=source_build_dir,
            tmp_dir=tmp_dir,
            core_dst=core_dst,
            source_build_id=source_build_id,
            slice_build_id=slice_build_id,
        ),
    }

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
        "side_artifacts": side_artifacts,
    }

    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / "slice_policy_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    subprocess.check_call(
        [
            sys.executable,
            str(root / "scripts" / "derived_build_verification.py"),
            "emit",
            "--build-dir",
            str(tmp_dir),
            "--build-id",
            slice_build_id,
            "--source-build-id",
            source_build_id,
            "--upstream-reports-dir",
            str(state / "reports" / source_build_id),
            "--report",
            str(reports_dir / "derived_build_verification_report.json"),
        ],
        cwd=str(root),
    )
    tmp_dir.rename(final_dir)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a public core slice from an existing canonical build.")
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
