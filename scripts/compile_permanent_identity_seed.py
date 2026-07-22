#!/usr/bin/env python3
"""Compile the one-time identity-only hierarchy seed for Evidence Lake E7."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_permanent_identity_seed.json"
DEFAULT_STATE = Path("/data/spacegate/state")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.permanent_identity_seed_policy.v1":
        raise ValueError("unsupported permanent identity seed policy schema")
    tables = policy.get("tables") or {}
    source_columns = policy.get("source_columns") or {}
    if set(tables) != {"hierarchy_nodes", "hierarchy_edges"}:
        raise ValueError("identity seed must contain exactly hierarchy_nodes and hierarchy_edges")
    if set(source_columns) != set(tables):
        raise ValueError("identity seed source-column contract is incomplete")
    prohibited = [str(value).lower() for value in policy.get("prohibited_scientific_column_tokens") or []]
    violations = sorted(
        f"{table}.{column}"
        for table, columns in tables.items()
        for column in columns
        if any(token in str(column).lower() for token in prohibited)
    )
    if violations:
        raise ValueError(f"scientific columns are prohibited in identity seed: {violations}")
    rules = policy.get("rules") or {}
    required = {
        "preserve_stable_spacegate_ids": True,
        "preserve_component_case": True,
        "allow_scientific_values": False,
        "allow_named_object_conditions": False,
        "future_builds_may_read_stability_databases": False,
        "seed_is_retained_permanent_identity_work": True,
    }
    mismatches = {key: rules.get(key) for key, expected in required.items() if rules.get(key) != expected}
    if mismatches or policy.get("scientific_authority") is not False:
        raise ValueError(f"unsafe permanent identity seed policy: {mismatches}")


def configure(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("SET threads=1")
    con.execute("SET preserve_insertion_order=true")
    memory = os.getenv("SPACEGATE_DUCKDB_MEMORY_LIMIT", "16GB")
    con.execute(f"SET memory_limit={sql_literal(memory)}")


def phase(started: float, cpu_started: float) -> dict[str, Any]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "wall_seconds": round(time.monotonic() - started, 6),
        "cpu_seconds": round(time.process_time() - cpu_started, 6),
        "peak_rss_kib": int(usage.ru_maxrss),
    }


def compile_seed(policy_path: Path, state_dir: Path, output_root: Path | None = None) -> dict[str, Any]:
    policy = load_object(policy_path)
    validate_policy(policy)
    identity_db = (
        state_dir / "derived/evidence_lake_v2/identity"
        / str(policy["identity_graph_id"]) / "identity_graph.duckdb"
    )
    hierarchy_db = (
        state_dir / "out" / str(policy["hierarchy_source_build_id"])
        / "canonical_hierarchy.duckdb"
    )
    for path in (identity_db, hierarchy_db):
        if not path.is_file():
            raise FileNotFoundError(path)

    timings: dict[str, Any] = {}
    started = time.monotonic()
    cpu_started = time.process_time()
    policy_sha = file_sha256(policy_path)
    identity_sha = file_sha256(identity_db)
    hierarchy_sha = file_sha256(hierarchy_db)
    timings["input_attestation"] = phase(started, cpu_started)
    seed_id = stable_hash({
        "policy_sha256": policy_sha,
        "identity_graph_sha256": identity_sha,
        "hierarchy_source_sha256": hierarchy_sha,
        "compiler_version": policy["compiler_version"],
    })[:24]
    root = output_root or state_dir / "derived/evidence_lake_v2/permanent_identity_seed"
    final_dir = root / seed_id
    temp_dir = root / f".{seed_id}.tmp"
    if final_dir.is_dir():
        manifest = load_object(final_dir / "manifest.json")
        if manifest.get("seed_id") != seed_id:
            raise ValueError(f"existing seed manifest mismatch: {final_dir}")
        return manifest
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True)

    con = duckdb.connect(str(hierarchy_db), read_only=True)
    configure(con)
    products: dict[str, Any] = {}
    try:
        actual_source_columns = {
            str(table): [str(row[1]) for row in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
            for table in policy["tables"]
        }
        if actual_source_columns != policy["source_columns"]:
            raise ValueError(
                f"hierarchy schema drift: expected={policy['source_columns']} "
                f"actual={actual_source_columns}"
            )
        for table, columns in policy["tables"].items():
            phase_started = time.monotonic()
            phase_cpu = time.process_time()
            output = temp_dir / f"{table}.parquet"
            order_key = "hierarchy_node_key" if table == "hierarchy_nodes" else "hierarchy_edge_id"
            if table == "hierarchy_edges":
                edge_order = (
                    "parent_node_key,child_node_key,edge_kind,"
                    "coalesce(member_role,''),source_basis,confidence_score,"
                    "supporting_edge_count,hierarchy_edge_id"
                )
                projection = (
                    f"row_number() OVER (ORDER BY {edge_order})::BIGINT AS hierarchy_edge_id,"
                    "hierarchy_edge_id AS source_hierarchy_edge_id,"
                    "parent_node_key,child_node_key,edge_kind,member_role,source_basis,"
                    "confidence_score,supporting_edge_count"
                )
                order_key = edge_order
            else:
                projection = ",".join(f'"{column}"' for column in columns)
            con.execute(
                f"COPY (SELECT {projection} FROM {table} ORDER BY {order_key}) "
                f"TO {sql_literal(str(output))} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)"
            )
            products[table] = {
                "path": output.name,
                "columns": columns,
                "row_count": int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]),
                "bytes": output.stat().st_size,
                "sha256": file_sha256(output),
            }
            timings[f"export_{table}"] = phase(phase_started, phase_cpu)
    finally:
        con.close()

    verify_started = time.monotonic()
    verify_cpu = time.process_time()
    check = duckdb.connect()
    try:
        check.execute(f"ATTACH {sql_literal(str(identity_db))} AS identity (READ_ONLY)")
        nodes_path = temp_dir / "hierarchy_nodes.parquet"
        edges_path = temp_dir / "hierarchy_edges.parquet"
        verification = {
            "duplicate_node_keys": int(check.execute(
                "SELECT COUNT(*) FROM (SELECT hierarchy_node_key FROM read_parquet(?) GROUP BY 1 HAVING COUNT(*)>1)",
                [str(nodes_path)],
            ).fetchone()[0]),
            "duplicate_edge_ids": int(check.execute(
                "SELECT COUNT(*) FROM (SELECT hierarchy_edge_id FROM read_parquet(?) GROUP BY 1 HAVING COUNT(*)>1)",
                [str(edges_path)],
            ).fetchone()[0]),
            "duplicate_edge_relationships": int(check.execute(
                "SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) n FROM read_parquet(?) GROUP BY parent_node_key,child_node_key,edge_kind,member_role,source_basis,confidence_score,supporting_edge_count HAVING COUNT(*)>1)",
                [str(edges_path)],
            ).fetchone()[0]),
            "edges_with_missing_parent": int(check.execute(
                "SELECT COUNT(*) FROM read_parquet(?) e LEFT JOIN read_parquet(?) n ON n.hierarchy_node_key=e.parent_node_key WHERE n.hierarchy_node_key IS NULL",
                [str(edges_path), str(nodes_path)],
            ).fetchone()[0]),
            "edges_with_missing_child": int(check.execute(
                "SELECT COUNT(*) FROM read_parquet(?) e LEFT JOIN read_parquet(?) n ON n.hierarchy_node_key=e.child_node_key WHERE n.hierarchy_node_key IS NULL",
                [str(edges_path), str(nodes_path)],
            ).fetchone()[0]),
            "canonical_objects_missing_nodes": int(check.execute(
                "SELECT COUNT(*) FROM identity.canonical_object_nodes o LEFT JOIN read_parquet(?) n ON n.hierarchy_node_key=o.stable_object_key WHERE n.hierarchy_node_key IS NULL",
                [str(nodes_path)],
            ).fetchone()[0]),
            "source_edge_id_collision_rows": int(check.execute(
                "SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) n FROM read_parquet(?) GROUP BY source_hierarchy_edge_id HAVING COUNT(*)>1)",
                [str(edges_path)],
            ).fetchone()[0]),
        }
    finally:
        check.close()
    failing_verification = {
        key: value for key, value in verification.items()
        if key != "source_edge_id_collision_rows" and value
    }
    if failing_verification:
        raise ValueError(f"permanent identity seed verification failed: {verification}")
    timings["verification"] = phase(verify_started, verify_cpu)

    manifest = {
        "schema_version": "spacegate.permanent_identity_seed_manifest.v1",
        "seed_id": seed_id,
        "status": "pass",
        "created_at": utc_now(),
        "policy_version": policy["policy_version"],
        "policy_sha256": policy_sha,
        "compiler_version": policy["compiler_version"],
        "migration_role": policy["migration_role"],
        "scientific_authority": False,
        "canonical_reference_build_id": policy["canonical_reference_build_id"],
        "identity_graph_id": policy["identity_graph_id"],
        "identity_graph_sha256": identity_sha,
        "hierarchy_source_build_id": policy["hierarchy_source_build_id"],
        "hierarchy_source_sha256": hierarchy_sha,
        "products": products,
        "verification": verification,
        "timings": timings,
    }
    (temp_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temp_dir, final_dir)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    manifest = compile_seed(
        args.policy.resolve(),
        args.state_dir.resolve(),
        args.output_root.resolve() if args.output_root else None,
    )
    rendered = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
