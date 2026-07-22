#!/usr/bin/env python3
"""Export the one-time permanent extended-object identity migration seed."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_extended_identity_seed.json"
DEFAULT_STATE = Path("/data/spacegate/state")


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sql_identifier(value: str) -> str:
    if not value or not value.replace("_", "a").isalnum() or not value[0].isalpha():
        raise ValueError(f"unsafe SQL identifier: {value!r}")
    return f'"{value}"'


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.extended_identity_seed_policy.v1":
        raise ValueError("unsupported extended identity seed policy")
    if policy.get("scientific_authority") is not False:
        raise ValueError("extended identity seed must not be scientific authority")
    prohibited = set(policy.get("prohibited_scientific_columns") or [])
    tables = policy.get("tables") or {}
    if not tables:
        raise ValueError("extended identity seed has no tables")
    for output_name, spec in tables.items():
        sql_identifier(str(output_name))
        sql_identifier(str(spec.get("source_table") or ""))
        columns = [str(value) for value in spec.get("columns") or []]
        order_by = [str(value) for value in spec.get("order_by") or []]
        if not columns or len(columns) != len(set(columns)):
            raise ValueError(f"invalid output columns: {output_name}")
        if not order_by or not set(order_by).issubset(columns):
            raise ValueError(f"invalid deterministic ordering: {output_name}")
        if set(columns) & prohibited:
            raise ValueError(f"scientific columns entered identity seed: {output_name}")
        for value in [*columns, *order_by]:
            sql_identifier(value)
    expected = {
        "future_clean_build_reads_migration_core": False,
        "preserve_numeric_object_ids": True,
        "preserve_reconciliation_outcomes": True,
        "scientific_geometry_or_distance_allowed": False,
        "every_alias_and_identifier_requires_identity_node": True,
    }
    if policy.get("rules") != expected:
        raise ValueError("unsafe extended identity seed rules")


def compile_seed(
    policy_path: Path,
    state_dir: Path,
    output_root: Path | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    cpu_started = time.process_time()
    policy = load_object(policy_path)
    validate_policy(policy)
    source = state_dir / "out" / policy["migration_source_build_id"] / "core.duckdb"
    if not source.is_file():
        raise FileNotFoundError(source)
    source_sha = file_hash(source)
    if source_sha != policy["migration_source_core_sha256"]:
        raise ValueError("migration CORE checksum mismatch")
    policy_sha = file_hash(policy_path)
    compiler_sha = file_hash(Path(__file__).resolve())
    seed_id = stable_hash(
        {
            "policy_sha256": policy_sha,
            "compiler_sha256": compiler_sha,
            "migration_source_core_sha256": source_sha,
        }
    )[:24]
    root = output_root or state_dir / "derived/evidence_lake_v2/extended_identity_seed"
    final = root / seed_id
    if (final / "manifest.json").is_file():
        manifest = load_object(final / "manifest.json")
        if manifest.get("seed_id") != seed_id:
            raise ValueError("extended identity seed collision")
        return manifest
    root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{seed_id}.", dir=root))
    con = duckdb.connect()
    products: dict[str, dict[str, Any]] = {}
    checks: dict[str, int] = {}
    try:
        con.execute(f"ATTACH {sql_literal(source)} AS migration (READ_ONLY)")
        for output_name, spec in policy["tables"].items():
            columns = ",".join(sql_identifier(str(value)) for value in spec["columns"])
            order_by = ",".join(sql_identifier(str(value)) for value in spec["order_by"])
            output = staging / f"{output_name}.parquet"
            con.execute(
                f"COPY (SELECT {columns} FROM migration.{sql_identifier(spec['source_table'])} "
                f"ORDER BY {order_by}) TO {sql_literal(output)} "
                "(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 122880)"
            )
            row_count = int(
                con.execute(
                    f"SELECT count(*) FROM migration.{sql_identifier(spec['source_table'])}"
                ).fetchone()[0]
            )
            products[output_name] = {
                "path": output.name,
                "row_count": row_count,
                "bytes": output.stat().st_size,
                "sha256": file_hash(output),
                "columns": list(spec["columns"]),
            }
        con.execute(
            f"CREATE VIEW nodes AS SELECT * FROM read_parquet({sql_literal(staging / 'extended_identity_nodes.parquet')})"
        )
        con.execute(
            f"CREATE VIEW aliases AS SELECT * FROM read_parquet({sql_literal(staging / 'extended_object_aliases.parquet')})"
        )
        con.execute(
            f"CREATE VIEW identifiers AS SELECT * FROM read_parquet({sql_literal(staging / 'extended_object_identifiers.parquet')})"
        )
        checks = {
            "duplicate_object_ids": int(con.execute(
                "SELECT count(*) FROM (SELECT extended_object_id FROM nodes GROUP BY 1 HAVING count(*)>1)"
            ).fetchone()[0]),
            "duplicate_stable_keys": int(con.execute(
                "SELECT count(*) FROM (SELECT stable_object_key FROM nodes GROUP BY 1 HAVING count(*)>1)"
            ).fetchone()[0]),
            "empty_stable_keys": int(con.execute(
                "SELECT count(*) FROM nodes WHERE nullif(trim(stable_object_key),'') IS NULL"
            ).fetchone()[0]),
            "orphan_aliases": int(con.execute(
                "SELECT count(*) FROM aliases a LEFT JOIN nodes n USING(extended_object_id) WHERE n.extended_object_id IS NULL"
            ).fetchone()[0]),
            "orphan_identifiers": int(con.execute(
                "SELECT count(*) FROM identifiers i LEFT JOIN nodes n USING(extended_object_id) WHERE n.extended_object_id IS NULL"
            ).fetchone()[0]),
        }
        if any(checks.values()):
            raise ValueError(f"extended identity seed verification failed: {checks}")
    finally:
        con.close()
    manifest = {
        "schema_version": "spacegate.extended_identity_seed_manifest.v1",
        "seed_id": seed_id,
        "status": "pass",
        "scientific_authority": False,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "policy_version": policy["policy_version"],
        "compiler_version": policy["compiler_version"],
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "migration_source_build_id": policy["migration_source_build_id"],
        "migration_source_core_sha256": source_sha,
        "products": products,
        "verification": checks,
        "timing": {
            "wall_seconds": round(time.monotonic() - started, 6),
            "cpu_seconds": round(time.process_time() - cpu_started, 6),
            "peak_rss_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
        },
    }
    (staging / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(staging, final)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = compile_seed(
        args.policy.resolve(), args.state_dir.resolve(),
        args.output_root.resolve() if args.output_root else None,
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
