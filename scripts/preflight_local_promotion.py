#!/usr/bin/env python3
"""Read-only preflight for a local Spacegate promotion and rollback drill."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_STATE = Path("/data/spacegate/state")
REQUIRED_FILES = (
    "core.duckdb",
    "arm.duckdb",
    "disc.duckdb",
    "canonical_hierarchy.duckdb",
    "map_tiles/index.json",
)


def bounded_build(state: Path, build_id: str) -> Path:
    if not build_id or Path(build_id).name != build_id or build_id in {".", ".."}:
        raise ValueError("build ID must be one bounded path component")
    out = (state / "out").resolve()
    candidate = (out / build_id).resolve()
    if candidate.parent != out:
        raise ValueError("build path escaped state/out")
    return candidate


def metadata(path: Path) -> dict[str, str]:
    con = duckdb.connect(str(path), read_only=True)
    try:
        tables = {
            str(row[0])
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
        if "build_metadata" not in tables:
            return {}
        return {
            str(key): str(value)
            for key, value in con.execute("SELECT key,value FROM build_metadata").fetchall()
        }
    finally:
        con.close()


def preflight(state: Path, candidate_id: str) -> dict[str, Any]:
    state = state.resolve()
    candidate = bounded_build(state, candidate_id)
    current_link = state / "served/current"
    current_target = current_link.resolve() if current_link.exists() else None
    checks: dict[str, Any] = {
        "candidate_directory": candidate.is_dir(),
        "candidate_not_current": current_target is None or candidate != current_target,
        "served_directory_writable": os.access(state / "served", os.W_OK),
    }
    for relative in REQUIRED_FILES:
        checks[f"required:{relative}"] = (candidate / relative).is_file()
    checks["parquet_present"] = any((candidate / "parquet").glob("*.parquet"))
    identities: dict[str, Any] = {}
    for database_name in ("core", "arm", "disc"):
        database = candidate / f"{database_name}.duckdb"
        if not database.is_file():
            continue
        values = metadata(database)
        identities[database_name] = values
        checks[f"{database_name}_build_identity"] = values.get("build_id") == candidate_id
    map_path = candidate / "map_tiles/index.json"
    map_identity = None
    if map_path.is_file():
        map_identity = json.loads(map_path.read_text(encoding="utf-8")).get("build_id")
        checks["map_build_identity"] = map_identity == candidate_id
    rollback = {
        "link": str(current_link),
        "target": str(current_target) if current_target else None,
        "exists": bool(current_target and current_target.is_dir()),
        "bounded_to_out": bool(
            current_target
            and current_target.parent == (state / "out").resolve()
        ),
    }
    checks["rollback_target_available"] = rollback["exists"]
    checks["rollback_target_bounded"] = rollback["bounded_to_out"]
    return {
        "schema_version": "spacegate.local_promotion_preflight.v1",
        "candidate_build_id": candidate_id,
        "candidate_path": str(candidate),
        "rollback": rollback,
        "database_metadata": identities,
        "map_build_id": map_identity,
        "checks": checks,
        "failing_checks": sorted(key for key, passed in checks.items() if not passed),
        "mutations_performed": False,
        "status": "pass" if all(checks.values()) else "fail",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--candidate-build-id", required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    result = preflight(args.state_dir, args.candidate_build_id)
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.report.with_name(f".{args.report.name}.{os.getpid()}.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, args.report)
    print(rendered, end="")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
