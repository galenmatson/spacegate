#!/usr/bin/env python3
"""Account every table in the E7 stability reference before clean cutover."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONTRACT = ROOT / "config/evidence_lake/e7_stability_table_migration.json"
DEFAULT_STATE = Path("/data/spacegate/state")
ALLOWED_DISPOSITIONS = {
    "verified_artifact",
    "identity_seed_required",
    "clean_compiler_required",
    "clean_projection_blocker",
    "retire_empty",
}


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def validate_contract(contract: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if contract.get("schema_version") != "spacegate.e7_stability_table_migration.v1":
        errors.append("schema_version")
    rules = contract.get("rules") or {}
    required_rules = {
        "all_tables_must_be_owned_once": True,
        "stability_scientific_values_may_enter_clean_build": False,
        "identity_migration_seed_may_contain_scientific_scalars": False,
        "retire_empty_requires_zero_rows": True,
        "clean_compiler_must_not_open_stability_databases": True,
    }
    for key, expected in required_rules.items():
        if rules.get(key) is not expected:
            errors.append(f"rule:{key}")
    databases = contract.get("database_contracts") or []
    names = [str(item.get("database") or "") for item in databases]
    if names != ["core.duckdb", "arm.duckdb", "canonical_hierarchy.duckdb", "disc.duckdb"]:
        errors.append("database_order_or_inventory")
    for database in databases:
        tables: list[str] = []
        group_ids: list[str] = []
        for group in database.get("groups") or []:
            group_id = str(group.get("group_id") or "")
            group_ids.append(group_id)
            disposition = str(group.get("disposition") or "")
            if disposition not in ALLOWED_DISPOSITIONS:
                errors.append(f"disposition:{database.get('database')}:{group_id}")
            if not str(group.get("replacement") or "").strip():
                errors.append(f"replacement:{database.get('database')}:{group_id}")
            group_tables = [str(value) for value in group.get("tables") or []]
            if not group_tables:
                errors.append(f"empty_group:{database.get('database')}:{group_id}")
            tables.extend(group_tables)
        if len(group_ids) != len(set(group_ids)) or not all(group_ids):
            errors.append(f"group_ids:{database.get('database')}")
        if len(tables) != len(set(tables)) or not all(tables):
            errors.append(f"duplicate_or_empty_tables:{database.get('database')}")
    return sorted(errors)


def audit(contract_path: Path, state_dir: Path) -> dict[str, Any]:
    contract = load_object(contract_path)
    validation_errors = validate_contract(contract)
    stability_dir = state_dir / "out" / str(contract.get("stability_build_id") or "")
    database_reports: list[dict[str, Any]] = []
    unowned_tables: list[str] = []
    unexpected_tables: list[str] = []
    checksum_mismatches: list[str] = []
    retire_empty_nonzero: list[str] = []
    disposition_counts: dict[str, int] = {}

    for database_contract in contract.get("database_contracts") or []:
        database_name = str(database_contract["database"])
        path = stability_dir / database_name
        if not path.is_file():
            validation_errors.append(f"missing_database:{database_name}")
            continue
        actual_sha = file_sha256(path)
        if actual_sha != database_contract.get("sha256"):
            checksum_mismatches.append(database_name)
        con = duckdb.connect(str(path), read_only=True)
        try:
            actual_tables = {
                str(row[0])
                for row in con.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='main' ORDER BY table_name"
                ).fetchall()
            }
            ownership: dict[str, dict[str, str]] = {}
            for group in database_contract.get("groups") or []:
                for table in group["tables"]:
                    ownership[str(table)] = {
                        "group_id": str(group["group_id"]),
                        "disposition": str(group["disposition"]),
                        "replacement": str(group["replacement"]),
                    }
            declared_tables = set(ownership)
            unowned_tables.extend(
                f"{database_name}:{table}" for table in sorted(actual_tables - declared_tables)
            )
            unexpected_tables.extend(
                f"{database_name}:{table}" for table in sorted(declared_tables - actual_tables)
            )
            rows: list[dict[str, Any]] = []
            for table in sorted(actual_tables & declared_tables):
                row_count = int(con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
                columns = [
                    {
                        "name": str(row[0]),
                        "type": str(row[1]),
                        "nullable": str(row[2]),
                    }
                    for row in con.execute(f'DESCRIBE "{table}"').fetchall()
                ]
                owner = ownership[table]
                disposition_counts[owner["disposition"]] = (
                    disposition_counts.get(owner["disposition"], 0) + 1
                )
                if owner["disposition"] == "retire_empty" and row_count != 0:
                    retire_empty_nonzero.append(f"{database_name}:{table}:{row_count}")
                rows.append({
                    "table": table,
                    "row_count": row_count,
                    "schema_sha256": stable_hash(columns),
                    **owner,
                })
        finally:
            con.close()
        database_reports.append({
            "database": database_name,
            "bytes": path.stat().st_size,
            "sha256": actual_sha,
            "expected_sha256": database_contract.get("sha256"),
            "table_count": len(actual_tables),
            "tables": rows,
        })

    blockers = sorted(
        f"{database['database']}:{table['table']}"
        for database in database_reports
        for table in database["tables"]
        if table["disposition"] in {
            "identity_seed_required", "clean_compiler_required", "clean_projection_blocker"
        }
    )
    failures = {
        "contract_validation_errors": sorted(validation_errors),
        "checksum_mismatches": sorted(checksum_mismatches),
        "unowned_tables": sorted(unowned_tables),
        "unexpected_tables": sorted(unexpected_tables),
        "retire_empty_nonzero": sorted(retire_empty_nonzero),
    }
    failing = {key: value for key, value in failures.items() if value}
    return {
        "schema_version": "spacegate.e7_stability_table_migration_audit.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "pass" if not failing else "fail",
        "completion_status": "complete" if not failing and not blockers else "incomplete",
        "contract_version": contract.get("contract_version"),
        "contract_sha256": stable_hash(contract),
        "stability_build_id": contract.get("stability_build_id"),
        "database_count": len(database_reports),
        "table_count": sum(item["table_count"] for item in database_reports),
        "disposition_counts": dict(sorted(disposition_counts.items())),
        "failing_checks": failing,
        "open_replacement_count": len(blockers),
        "open_replacements": blockers,
        "databases": database_reports,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = audit(args.contract.resolve(), args.state_dir.resolve())
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    raise SystemExit(0 if report["status"] == "pass" else 1)


if __name__ == "__main__":
    main()
