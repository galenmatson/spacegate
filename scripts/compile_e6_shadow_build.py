#!/usr/bin/env python3
"""Compile an immutable Evidence Lake v2 E6 shadow product build."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

import duckdb

import materialize_e6_selected_consumers as selected_consumers
import materialize_stellar_leaf_classifications as leaf_classifications


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e6_shadow_build.json"
DEFAULT_STATE = Path("/data/spacegate/state")
SAFE_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def stable_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def sql_identifier(value: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier: {value!r}")
    return f'"{value}"'


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def table_names(con: duckdb.DuckDBPyConnection, alias: str) -> list[str]:
    return [
        str(row[0])
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog=? AND table_schema='main' ORDER BY table_name",
            [alias],
        ).fetchall()
    ]


def manifest_file_entry(manifest: dict[str, Any], filename: str) -> dict[str, Any] | None:
    for container in (
        manifest.get("files"),
        manifest.get("deterministic_files"),
        (manifest.get("report") or {}).get("files"),
    ):
        if not isinstance(container, dict):
            continue
        for key, value in container.items():
            if Path(str(key)).name == filename and isinstance(value, dict):
                return value
    return None


class PhaseRecorder:
    def __init__(self) -> None:
        self.phases: list[dict[str, Any]] = []

    def run(self, phase: str, operation: Any) -> Any:
        started = time.monotonic()
        cpu_started = time.process_time()
        try:
            result = operation()
        except Exception:
            self.phases.append(
                {
                    "phase": phase,
                    "wall_seconds": round(time.monotonic() - started, 6),
                    "cpu_seconds": round(time.process_time() - cpu_started, 6),
                    "status": "fail",
                }
            )
            raise
        self.phases.append(
            {
                "phase": phase,
                "wall_seconds": round(time.monotonic() - started, 6),
                "cpu_seconds": round(time.process_time() - cpu_started, 6),
                "status": "pass",
            }
        )
        return result


def artifact_inputs(state: Path, policy: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in policy["selected_artifacts"]:
        family = str(item["family"])
        build_id = str(item["build_id"])
        artifact = state / "derived/evidence_lake_v2" / family / build_id
        manifest_path = artifact / "manifest.json"
        database = artifact / str(item["database"])
        if not manifest_path.is_file() or not database.is_file():
            raise ValueError(f"missing E5 artifact input: {family}:{build_id}")
        manifest = load_json(manifest_path)
        acceptance_mode = str(item["acceptance_mode"])
        if acceptance_mode == "report_status_pass":
            accepted = (manifest.get("report") or {}).get("status") == "pass"
        elif acceptance_mode == "top_level_status_pass":
            accepted = manifest.get("status") == "pass"
        elif acceptance_mode == "legacy_zero_verification":
            verification = manifest.get("verification")
            accepted = bool(verification) and all(
                isinstance(value, (int, float)) and value == 0
                for value in verification.values()
            )
        else:
            raise ValueError(f"unsupported E5 acceptance mode: {acceptance_mode}")
        if not accepted:
            raise ValueError(f"E5 artifact is not passing: {family}:{build_id}")
        if str(manifest.get("build_id") or "") != build_id:
            raise ValueError(f"E5 artifact identity mismatch: {family}:{build_id}")
        database_sha = file_sha256(database)
        declared = manifest_file_entry(manifest, database.name)
        if declared and declared.get("sha256") != database_sha:
            raise ValueError(f"E5 artifact database checksum mismatch: {family}:{build_id}")
        rows.append(
            {
                **item,
                "artifact_path": str(artifact.relative_to(state)),
                "manifest_path": str(manifest_path),
                "manifest_sha256": file_sha256(manifest_path),
                "database_path": str(database),
                "database_sha256": database_sha,
                "database_bytes": database.stat().st_size,
                "manifest": manifest,
            }
        )
    return rows


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.e6_shadow_build_policy.v1":
        raise ValueError("unsupported E6 shadow policy schema")
    families = [str(row.get("family") or "") for row in policy.get("selected_artifacts") or []]
    if len(families) != len(set(families)) or "selected_facts" not in families:
        raise ValueError("E6 selected artifact families are missing or duplicated")
    acceptance_modes = {
        "report_status_pass", "top_level_status_pass", "legacy_zero_verification"
    }
    for artifact in policy["selected_artifacts"]:
        if artifact.get("acceptance_mode") not in acceptance_modes:
            raise ValueError(f"unsupported E6 artifact acceptance mode: {artifact.get('family')}")
    table_names_seen: set[str] = set()
    star_quantities: set[str] = set()
    for group in policy.get("star_projection_groups") or []:
        table = str(group.get("table") or "")
        sql_identifier(table)
        if table in table_names_seen:
            raise ValueError(f"duplicate E6 projection table: {table}")
        table_names_seen.add(table)
        for quantity in group.get("quantities") or []:
            sql_identifier(str(quantity))
            if quantity in star_quantities:
                raise ValueError(f"star quantity appears in multiple projections: {quantity}")
            star_quantities.add(str(quantity))
    planet = policy.get("planet_projection") or {}
    sql_identifier(str(planet.get("table") or ""))
    planet_quantities = [str(value) for value in planet.get("quantities") or []]
    if len(planet_quantities) != len(set(planet_quantities)):
        raise ValueError("duplicate planet quantity projection")
    categorical = {str(value) for value in policy.get("categorical_quantities") or []}
    boolean = {str(value) for value in policy.get("boolean_quantities") or []}
    projected_quantities = star_quantities | set(planet_quantities)
    if not categorical.issubset(projected_quantities):
        raise ValueError("categorical E6 quantities are not projected")
    if not boolean.issubset(projected_quantities):
        raise ValueError("boolean E6 quantities are not projected")
    if categorical & boolean:
        raise ValueError("E6 quantity types overlap")
    mapped: set[tuple[str, str]] = set()
    for mapping in policy.get("core_scalar_updates") or []:
        object_type = str(mapping.get("object_type") or "")
        quantity = str(mapping["quantity"])
        if object_type not in {"star", "planet"}:
            raise ValueError("unsupported E6 core object mapping")
        sql_identifier(quantity)
        sql_identifier(str(mapping["column"]))
        if (object_type, quantity) in mapped:
            raise ValueError(f"duplicate E6 CORE mapping: {object_type}:{quantity}")
        mapped.add((object_type, quantity))
        projected = star_quantities if object_type == "star" else set(planet_quantities)
        if quantity not in projected:
            raise ValueError(f"E6 CORE mapping is not projected: {object_type}:{quantity}")
        if quantity in categorical or quantity in boolean:
            raise ValueError(f"categorical E6 CORE update is unsupported: {quantity}")
        if float(mapping.get("absolute_tolerance", -1)) < 0:
            raise ValueError("invalid E6 core update tolerance")


def quantity_contract_check(
    con: duckdb.DuckDBPyConnection, policy: dict[str, Any], selected_alias: str
) -> dict[str, Any]:
    expected_star = {
        str(quantity)
        for group in policy["star_projection_groups"]
        for quantity in group["quantities"]
    }
    expected_planet = {str(value) for value in policy["planet_projection"]["quantities"]}
    actual: dict[str, set[str]] = {}
    for object_type, quantity in con.execute(
        f"SELECT DISTINCT object_type,quantity_key FROM {sql_identifier(selected_alias)}.selected_facts"
    ).fetchall():
        actual.setdefault(str(object_type), set()).add(str(quantity))
    failures = {
        "missing_star_quantities": sorted(expected_star - actual.get("star", set())),
        "unexpected_star_quantities": sorted(actual.get("star", set()) - expected_star),
        "missing_planet_quantities": sorted(expected_planet - actual.get("planet", set())),
        "unexpected_planet_quantities": sorted(actual.get("planet", set()) - expected_planet),
        "unexpected_object_types": sorted(set(actual) - {"star", "planet"}),
    }
    if any(failures.values()):
        raise ValueError(f"E6 selected quantity contract failed: {failures}")
    return {
        "star_quantities": len(expected_star),
        "planet_quantities": len(expected_planet),
        "failures": failures,
    }


def projection_expressions(
    quantities: list[str], categorical: set[str], boolean: set[str]
) -> list[str]:
    expressions: list[str] = []
    for quantity in quantities:
        q = sql_identifier(quantity)
        condition = f"quantity_key={sql_literal(quantity)}"
        if quantity in categorical:
            expressions.append(f"MAX(value_raw) FILTER (WHERE {condition}) AS {q}")
        elif quantity in boolean:
            expressions.append(
                f"MAX(try_cast(value_raw AS BOOLEAN)) FILTER (WHERE {condition}) AS {q}"
            )
        else:
            expressions.extend(
                [
                    f"MAX(normalized_value) FILTER (WHERE {condition}) AS {q}",
                    f"MAX(value_lower) FILTER (WHERE {condition}) AS {sql_identifier(quantity + '_lower')}",
                    f"MAX(value_upper) FILTER (WHERE {condition}) AS {sql_identifier(quantity + '_upper')}",
                ]
            )
        expressions.append(
            f"MAX(selected_fact_id) FILTER (WHERE {condition}) AS {sql_identifier(quantity + '_fact_id')}"
        )
    return expressions


def create_wide_projection(
    con: duckdb.DuckDBPyConnection,
    *,
    output_table: str,
    object_type: str,
    quantities: list[str],
    categorical: set[str],
    boolean: set[str],
    selected_alias: str,
) -> int:
    core_table = "stars" if object_type == "star" else "planets"
    id_column = "star_id" if object_type == "star" else "planet_id"
    object_columns = (
        f"o.{id_column},o.system_id,o.stable_object_key"
        + (",o.star_id" if object_type == "planet" else "")
    )
    selected = ",\n          ".join(
        projection_expressions(quantities, categorical, boolean)
    )
    values = ",".join(sql_literal(value) for value in quantities)
    con.execute(
        f"""
        CREATE TABLE {sql_identifier(output_table)} AS
        SELECT {object_columns},
          {selected}
        FROM core.{sql_identifier(core_table)} o
        JOIN {sql_identifier(selected_alias)}.selected_facts f
          ON f.object_type={sql_literal(object_type)}
         AND f.stable_object_key=o.stable_object_key
         AND f.quantity_key IN ({values})
        GROUP BY {object_columns}
        ORDER BY o.{id_column}
        """
    )
    return int(con.execute(f"SELECT COUNT(*) FROM {sql_identifier(output_table)}").fetchone()[0])


def copy_projection_artifact_tables(
    con: duckdb.DuckDBPyConnection,
    *,
    inputs: list[dict[str, Any]],
    policy: dict[str, Any],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    compact = {str(value) for value in policy["selected_fact_compact_tables"]}
    copy_all = {str(value) for value in policy["copy_all_projection_tables_for_families"]}
    for item in inputs:
        alias = str(item["alias"])
        prefix = str(item["arm_prefix"])
        family = str(item["family"])
        for source_table in table_names(con, alias):
            if family == "selected_facts" and source_table not in compact:
                continue
            if family != "selected_facts" and family not in copy_all:
                continue
            output_table = f"{prefix}_{source_table}"
            sql_identifier(output_table)
            con.execute(
                f"CREATE TABLE {sql_identifier(output_table)} AS "
                f"SELECT * FROM {sql_identifier(alias)}.{sql_identifier(source_table)}"
            )
            counts[output_table] = int(
                con.execute(f"SELECT COUNT(*) FROM {sql_identifier(output_table)}").fetchone()[0]
            )
    return counts


def core_update_report(
    con: duckdb.DuckDBPyConnection,
    *,
    policy: dict[str, Any],
    projection_by_quantity: dict[tuple[str, str], str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for mapping in policy["core_scalar_updates"]:
        object_type = str(mapping["object_type"])
        quantity = str(mapping["quantity"])
        column = str(mapping["column"])
        tolerance = float(mapping["absolute_tolerance"])
        table = "stars" if object_type == "star" else "planets"
        id_column = "star_id" if object_type == "star" else "planet_id"
        projection = projection_by_quantity[(object_type, quantity)]
        before = con.execute(
            f"""
            SELECT COUNT(*)::BIGINT,
                   COUNT(*) FILTER (WHERE p.{sql_identifier(quantity)} IS NOT NULL)::BIGINT,
                   COUNT(*) FILTER (WHERE b.{sql_identifier(column)} IS NULL)::BIGINT,
                   COUNT(*) FILTER (WHERE b.{sql_identifier(column)} IS NULL
                                      AND p.{sql_identifier(quantity)} IS NOT NULL)::BIGINT,
                   COUNT(*) FILTER (WHERE b.{sql_identifier(column)} IS NOT NULL
                                      AND p.{sql_identifier(quantity)} IS NOT NULL
                                      AND abs(b.{sql_identifier(column)}-p.{sql_identifier(quantity)})>{tolerance})::BIGINT
            FROM base.{sql_identifier(table)} b
            JOIN {sql_identifier(projection)} p USING ({sql_identifier(id_column)})
            """
        ).fetchone()
        con.execute(
            f"""
            UPDATE core.{sql_identifier(table)} AS target
            SET {sql_identifier(column)}=p.{sql_identifier(quantity)}
            FROM {sql_identifier(projection)} p
            WHERE target.{sql_identifier(id_column)}=p.{sql_identifier(id_column)}
              AND p.{sql_identifier(quantity)} IS NOT NULL
            """
        )
        remaining = int(
            con.execute(
                f"""
                SELECT COUNT(*) FROM core.{sql_identifier(table)} target
                JOIN {sql_identifier(projection)} p USING ({sql_identifier(id_column)})
                WHERE p.{sql_identifier(quantity)} IS NOT NULL
                  AND abs(target.{sql_identifier(column)}-p.{sql_identifier(quantity)})>1e-12
                """
            ).fetchone()[0]
        )
        rows.append(
            {
                "object_type": object_type,
                "quantity": quantity,
                "core_column": column,
                "projection_rows": int(before[0]),
                "selected_value_rows": int(before[1]),
                "previously_null_rows": int(before[2]),
                "filled_rows": int(before[3]),
                "scientifically_changed_rows": int(before[4]),
                "absolute_tolerance": tolerance,
                "post_update_mismatches": remaining,
            }
        )
    return rows


def add_official_name_aliases(
    con: duckdb.DuckDBPyConnection,
    *,
    policy: dict[str, Any],
    classification_table: str,
) -> int:
    alias_policy = policy["official_name_alias_policy"]
    if not alias_policy.get("enabled"):
        return 0
    before = int(con.execute("SELECT COUNT(*) FROM core.aliases").fetchone()[0])
    con.execute(
        f"""
        INSERT INTO core.aliases BY NAME
        WITH candidates AS (
          SELECT s.star_id::BIGINT target_id,s.system_id::BIGINT system_id,
                 p.official_proper_name alias_raw,
                 trim(regexp_replace(regexp_replace(lower(p.official_proper_name),
                   '[^0-9a-z]+',' ','g'),'\\s+',' ','g')) alias_norm,
                 p.official_proper_name_fact_id fact_id
          FROM {sql_identifier(classification_table)} p
          JOIN core.stars s USING (star_id)
          WHERE nullif(trim(p.official_proper_name),'') IS NOT NULL
        ), new_rows AS (
          SELECT *,row_number() OVER (ORDER BY target_id,alias_norm,fact_id) rn
          FROM candidates c
          WHERE NOT EXISTS (
            SELECT 1 FROM core.aliases a
            WHERE a.target_type='star' AND a.target_id=c.target_id
              AND a.alias_norm=c.alias_norm
          )
        )
        SELECT (SELECT coalesce(max(alias_id),0) FROM core.aliases)+rn alias_id,
               'star' target_type,target_id,system_id,target_id star_id,
               alias_raw,alias_norm,{sql_literal(alias_policy['alias_kind'])} alias_kind,
               {int(alias_policy['alias_priority'])} alias_priority,
               {sql_literal(bool(alias_policy['is_primary']))} is_primary,
               'naming.iau_wgsn' source_catalog,'evidence_lake_v2' source_version,
               NULL::BIGINT source_pk
        FROM new_rows
        """
    )
    return int(con.execute("SELECT COUNT(*) FROM core.aliases").fetchone()[0]) - before


def set_key_value_metadata(
    database: Path, values: dict[str, str], *, table: str = "build_metadata"
) -> None:
    con = duckdb.connect(str(database))
    try:
        columns = {str(row[0]) for row in con.execute(f"DESCRIBE {sql_identifier(table)}").fetchall()}
        if not {"key", "value"}.issubset(columns):
            raise ValueError(f"metadata table is not key/value: {database}:{table}")
        for key, value in values.items():
            con.execute(f"DELETE FROM {sql_identifier(table)} WHERE key=?", [key])
            con.execute(f"INSERT INTO {sql_identifier(table)}(key,value) VALUES (?,?)", [key, value])
        con.execute("CHECKPOINT")
    finally:
        con.close()


def compile_shadow_build(
    *,
    state: Path,
    policy_path: Path,
    report_path: Path | None = None,
    memory_limit: str = "32GB",
    threads: int = 8,
    temp_directory: Path | None = None,
) -> dict[str, Any]:
    state = state.resolve()
    policy_path = policy_path.resolve()
    policy = load_json(policy_path)
    validate_policy(policy)
    recorder = PhaseRecorder()
    inputs = recorder.run("verify_e5_artifacts", lambda: artifact_inputs(state, policy))
    base_id = str(policy["stability_reference_build_id"])
    base_dir = state / "out" / base_id
    base_files = {
        name: base_dir / name
        for name in ["core.duckdb", "arm.duckdb", "canonical_hierarchy.duckdb", "disc.duckdb"]
    }
    if any(not path.is_file() for path in base_files.values()):
        raise ValueError(f"stability reference product is incomplete: {base_dir}")
    base_hashes = recorder.run(
        "verify_stability_reference",
        lambda: {name: file_sha256(path) for name, path in base_files.items()},
    )
    compiler_sha = file_sha256(Path(__file__).resolve())
    selected_consumer_compiler_sha = file_sha256(Path(selected_consumers.__file__).resolve())
    leaf_classification_compiler_sha = file_sha256(
        Path(leaf_classifications.__file__).resolve()
    )
    policy_sha = file_sha256(policy_path)
    build_inputs = {
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "selected_consumer_compiler_sha256": selected_consumer_compiler_sha,
        "leaf_classification_compiler_sha256": leaf_classification_compiler_sha,
        "stability_reference_build_id": base_id,
        "stability_reference_files": base_hashes,
        "selected_artifacts": {
            str(row["family"]): {
                "build_id": str(row["build_id"]),
                "manifest_sha256": str(row["manifest_sha256"]),
                "database_sha256": str(row["database_sha256"]),
            }
            for row in inputs
        },
        "duckdb_version": duckdb.__version__,
    }
    build_sha = stable_sha256(build_inputs)
    build_id = f"e6_{build_sha[:24]}_shadow"
    out_root = state / "out"
    final_dir = out_root / build_id
    if (final_dir / "manifest.json").is_file():
        manifest = load_json(final_dir / "manifest.json")
        if manifest.get("build_sha256") != build_sha:
            raise ValueError(f"E6 immutable build collision: {build_id}")
        if report_path:
            atomic_json(report_path, manifest["report"])
        return manifest["report"]

    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=out_root))
    temp_directory = (temp_directory or Path("/mnt/space/spacegate/e6-shadow-spill")).resolve()
    temp_directory.mkdir(parents=True, exist_ok=True)
    try:
        recorder.run(
            "copy_stability_products",
            lambda: [shutil.copy2(path, staging / name) for name, path in base_files.items()],
        )
        shadow_core = staging / "core.duckdb"
        shadow_arm = staging / "arm.duckdb"
        con = duckdb.connect(
            str(shadow_arm),
            config={
                "memory_limit": memory_limit,
                "threads": str(max(1, threads)),
                "temp_directory": str(temp_directory),
                "preserve_insertion_order": "false",
            },
        )
        try:
            con.execute(f"ATTACH {sql_literal(str(shadow_core))} AS core")
            con.execute(f"ATTACH {sql_literal(str(base_files['core.duckdb']))} AS base (READ_ONLY)")
            for index, row in enumerate(inputs):
                alias = f"e5_{index}_{row['family']}"
                alias = re.sub(r"[^a-z0-9_]", "_", alias.lower())
                row["alias"] = alias
                con.execute(
                    f"ATTACH {sql_literal(str(row['database_path']))} AS {sql_identifier(alias)} (READ_ONLY)"
                )
            selected = next(row for row in inputs if row["family"] == "selected_facts")
            selected_alias = str(selected["alias"])
            quantity_contract = recorder.run(
                "selected_quantity_contract",
                lambda: quantity_contract_check(con, policy, selected_alias),
            )
            copied_tables = recorder.run(
                "materialize_e5_evidence_projections",
                lambda: copy_projection_artifact_tables(con, inputs=inputs, policy=policy),
            )
            categorical = {str(value) for value in policy["categorical_quantities"]}
            boolean = {str(value) for value in policy["boolean_quantities"]}
            projection_counts: dict[str, int] = {}
            projection_by_quantity: dict[tuple[str, str], str] = {}
            for group in policy["star_projection_groups"]:
                table = str(group["table"])
                quantities = [str(value) for value in group["quantities"]]
                projection_counts[table] = recorder.run(
                    f"materialize_{table}",
                    lambda table=table, quantities=quantities: create_wide_projection(
                        con,
                        output_table=table,
                        object_type="star",
                        quantities=quantities,
                        categorical=categorical,
                        boolean=boolean,
                        selected_alias=selected_alias,
                    ),
                )
                for quantity in quantities:
                    projection_by_quantity[("star", quantity)] = table
            planet_table = str(policy["planet_projection"]["table"])
            planet_quantities = [str(value) for value in policy["planet_projection"]["quantities"]]
            projection_counts[planet_table] = recorder.run(
                f"materialize_{planet_table}",
                lambda: create_wide_projection(
                    con,
                    output_table=planet_table,
                    object_type="planet",
                    quantities=planet_quantities,
                    categorical=categorical,
                    boolean=boolean,
                    selected_alias=selected_alias,
                ),
            )
            for quantity in planet_quantities:
                projection_by_quantity[("planet", quantity)] = planet_table
            con.execute(
                """
                CREATE TABLE e6_evidence_artifact_registry(
                  family VARCHAR,build_id VARCHAR,artifact_path VARCHAR,
                  manifest_sha256 VARCHAR,database_sha256 VARCHAR,database_bytes BIGINT,
                  policy_version VARCHAR,compiler_version VARCHAR
                )
                """
            )
            con.executemany(
                "INSERT INTO e6_evidence_artifact_registry VALUES (?,?,?,?,?,?,?,?)",
                [
                    [
                        row["family"], row["build_id"], row["artifact_path"],
                        row["manifest_sha256"], row["database_sha256"], row["database_bytes"],
                        policy["policy_version"], policy["compiler_version"],
                    ]
                    for row in inputs
                ],
            )
            core_updates = recorder.run(
                "apply_core_selected_facts",
                lambda: core_update_report(
                    con, policy=policy, projection_by_quantity=projection_by_quantity
                ),
            )
            alias_additions = recorder.run(
                "materialize_official_name_aliases",
                lambda: add_official_name_aliases(
                    con,
                    policy=policy,
                    classification_table=next(
                        str(group["table"])
                        for group in policy["star_projection_groups"]
                        if "official_proper_name" in group["quantities"]
                    ),
                ),
            )
            recorder.run(
                "refresh_system_teff_aggregates",
                lambda: con.execute(
                    """
                    UPDATE core.systems target SET
                      star_teff_count=summary.star_teff_count,
                      min_star_teff_k=summary.min_star_teff_k,
                      max_star_teff_k=summary.max_star_teff_k
                    FROM (
                      SELECT system_id,count(teff_k)::BIGINT star_teff_count,
                             min(teff_k) min_star_teff_k,max(teff_k) max_star_teff_k
                      FROM core.stars GROUP BY system_id
                    ) summary
                    WHERE target.system_id=summary.system_id
                    """
                ),
            )
            base_inventory = {
                table: int(con.execute(f"SELECT COUNT(*) FROM base.{sql_identifier(table)}").fetchone()[0])
                for table in ["systems", "stars", "planets"]
            }
            shadow_inventory = {
                table: int(con.execute(f"SELECT COUNT(*) FROM core.{sql_identifier(table)}").fetchone()[0])
                for table in ["systems", "stars", "planets"]
            }
            inventory_delta = {
                table: shadow_inventory[table] - base_inventory[table]
                for table in base_inventory
            }
            if any(inventory_delta.values()):
                raise ValueError(f"E6 inventory mutation is forbidden: {inventory_delta}")
            if any(int(row["post_update_mismatches"]) for row in core_updates):
                raise ValueError("E6 selected CORE projection failed post-update verification")
            con.execute("CHECKPOINT")
        finally:
            con.close()

        selected_consumer_report = recorder.run(
            "materialize_selected_consumers",
            lambda: selected_consumers.materialize(
                core_db=shadow_core,
                arm_db=shadow_arm,
                build_id=build_id,
            ),
        )
        if selected_consumer_report.get("status") != "pass":
            raise ValueError("E6 selected consumer materialization failed")
        hierarchy = staging / "canonical_hierarchy.duckdb"
        stellar_leaf_report = recorder.run(
            "materialize_stellar_leaf_classifications",
            lambda: leaf_classifications.materialize(
                core_db=shadow_core,
                arm_db=shadow_arm,
                hierarchy_db=hierarchy,
                build_id=build_id,
            ),
        )
        if stellar_leaf_report.get("status") != "pass":
            raise ValueError("E6 stellar-leaf materialization failed")

        lineage_timestamp = max(
            str(row["manifest"].get("generated_at") or "") for row in inputs
        ) or "1970-01-01T00:00:00Z"
        metadata = {
            "build_id": build_id,
            "e6_shadow_build_id": build_id,
            "e6_shadow_policy_version": str(policy["policy_version"]),
            "e6_stability_reference_build_id": base_id,
            "e6_selected_fact_build_id": str(selected["build_id"]),
            "e6_product_status": "shadow_unpromoted",
        }
        recorder.run(
            "update_core_metadata",
            lambda: set_key_value_metadata(shadow_core, metadata),
        )
        recorder.run(
            "update_arm_metadata",
            lambda: set_key_value_metadata(shadow_arm, metadata),
        )
        hcon = duckdb.connect(str(hierarchy))
        try:
            hcon.execute("DELETE FROM build_metadata")
            hcon.execute(
                "INSERT INTO build_metadata VALUES (?,?,?,?)",
                [
                    build_id,
                    lineage_timestamp,
                    "e6_shadow_copy",
                    f"out/{base_id}/canonical_hierarchy.duckdb",
                ],
            )
            hcon.execute("CHECKPOINT")
        finally:
            hcon.close()
        recorder.run(
            "update_disc_metadata",
            lambda: set_key_value_metadata(
                staging / "disc.duckdb",
                {
                    **metadata,
                    "e6_disc_status": "carried_forward_pending_selected_fact_rescore",
                },
            ),
        )
        output_hashes = recorder.run(
            "hash_shadow_products",
            lambda: {
                name: {
                    "bytes": (staging / name).stat().st_size,
                    "sha256": file_sha256(staging / name),
                }
                for name in base_files
            },
        )
        report = {
            "schema_version": "spacegate.e6_shadow_build_report.v1",
            "status": "pass",
            "e6_acceptance_status": "in_progress",
            "build_id": build_id,
            "build_sha256": build_sha,
            "policy_version": policy["policy_version"],
            "compiler_version": policy["compiler_version"],
            "stability_reference_build_id": base_id,
            "selected_fact_build_id": selected["build_id"],
            "quantity_contract": quantity_contract,
            "projection_table_counts": {**copied_tables, **projection_counts},
            "core_updates": core_updates,
            "official_name_alias_additions": alias_additions,
            "selected_consumer_report": selected_consumer_report,
            "stellar_leaf_report": stellar_leaf_report,
            "inventory_before": base_inventory,
            "inventory_after": shadow_inventory,
            "inventory_delta": inventory_delta,
            "hierarchy_status": "byte_copy_with_shadow_metadata_only",
            "disc_status": "carried_forward_pending_selected_fact_rescore",
            "public_slice_status": "pending",
            "map_simulation_api_status": "pending",
            "promotion_status": "unpromoted",
            "product_files": output_hashes,
            "phases": recorder.phases,
        }
        manifest = {
            "schema_version": "spacegate.e6_shadow_build.v1",
            "build_id": build_id,
            "build_sha256": build_sha,
            "inputs": build_inputs,
            "report": report,
        }
        atomic_json(staging / "manifest.json", manifest)
        os.replace(staging, final_dir)
        if report_path:
            atomic_json(report_path, report)
        return report
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--memory-limit", default="32GB")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--temp-directory", type=Path)
    args = parser.parse_args()
    report = compile_shadow_build(
        state=args.state_dir,
        policy_path=args.policy,
        report_path=args.report,
        memory_limit=args.memory_limit,
        threads=args.threads,
        temp_directory=args.temp_directory,
    )
    print(
        f"E6 shadow build {report['build_id']} pass: "
        f"stars={report['inventory_after']['stars']} "
        f"planets={report['inventory_after']['planets']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
