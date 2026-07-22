#!/usr/bin/env python3
"""Independently verify an E7 clean foundation build and its accounting."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_ARTIFACT_ROOT = Path("/mnt/space/spacegate/e7-clean-foundation")


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


def verify(build_dir: Path) -> dict[str, Any]:
    started = time.monotonic()
    manifest_path = build_dir / "manifest.json"
    manifest = load_object(manifest_path)
    failures: dict[str, Any] = {}
    if manifest.get("schema_version") != "spacegate.e7_clean_foundation_manifest.v1":
        failures["manifest_schema"] = manifest.get("schema_version")
    if manifest.get("status") != "pass":
        failures["manifest_status"] = manifest.get("status")
    if manifest.get("stability_databases_opened") != []:
        failures["stability_databases_opened"] = manifest.get("stability_databases_opened")
    if manifest.get("scientific_authority_from_identity_seed") is not False:
        failures["identity_seed_scientific_authority"] = manifest.get("scientific_authority_from_identity_seed")
    product_failures: dict[str, Any] = {}
    for relative, expected in sorted((manifest.get("products") or {}).items()):
        path = build_dir / relative
        if not path.is_file():
            product_failures[relative] = "missing"
            continue
        actual = {"bytes": path.stat().st_size, "sha256": file_sha256(path)}
        expected_bytes = {"bytes": expected.get("bytes"), "sha256": expected.get("sha256")}
        if actual != expected_bytes:
            product_failures[relative] = {"expected": expected, "actual": actual}
    if product_failures:
        failures["products"] = product_failures

    core = build_dir / "clean_core_foundation.duckdb"
    hierarchy = build_dir / "canonical_hierarchy.duckdb"
    actual_counts: dict[str, int] = {}
    checks: dict[str, int] = {}
    if core.is_file():
        con = duckdb.connect(str(core), read_only=True)
        try:
            tables = [str(row[0]) for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='main' AND table_type='BASE TABLE' ORDER BY 1"
            ).fetchall()]
            required = {
                "aliases", "build_metadata", "canonical_objects", "identifier_quarantine",
                "object_identifiers", "planets", "stars", "system_search_terms", "systems",
            }
            if set(tables) != required:
                failures["core_tables"] = {"expected": sorted(required), "actual": tables}
            for table in sorted(required - {"build_metadata"}):
                actual_counts[table] = int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            checks = {
                "canonical_system_delta": int(con.execute("SELECT (SELECT count(*) FROM systems)-(SELECT count(*) FROM canonical_objects WHERE object_type='system')").fetchone()[0]),
                "canonical_star_delta": int(con.execute("SELECT (SELECT count(*) FROM stars)-(SELECT count(*) FROM canonical_objects WHERE object_type='star')").fetchone()[0]),
                "canonical_planet_delta": int(con.execute("SELECT (SELECT count(*) FROM planets)-(SELECT count(*) FROM canonical_objects WHERE object_type='planet')").fetchone()[0]),
                "unplaced_systems": int(con.execute("SELECT count(*) FROM systems WHERE placement_source IS NULL").fetchone()[0]),
                "duplicate_object_keys": int(con.execute("SELECT count(*) FROM (SELECT stable_object_key FROM canonical_objects GROUP BY 1 HAVING count(*)>1)").fetchone()[0]),
                "duplicate_search_semantics": int(con.execute("SELECT count(*) FROM (SELECT system_id,target_type,target_id,term_norm FROM system_search_terms GROUP BY 1,2,3,4 HAVING count(*)>1)").fetchone()[0]),
                "orphan_alias_targets": int(con.execute("SELECT count(*) FROM aliases a LEFT JOIN canonical_objects o ON o.object_type=a.target_type AND o.canonical_id=a.target_id WHERE o.canonical_id IS NULL").fetchone()[0]),
                "orphan_identifier_targets": int(con.execute("SELECT count(*) FROM object_identifiers i LEFT JOIN canonical_objects o ON o.object_type=i.target_type AND o.canonical_id=i.target_id WHERE o.canonical_id IS NULL").fetchone()[0]),
                "metadata_stability_opened": int(con.execute("SELECT count(*) FROM build_metadata WHERE key='stability_database_opened' AND value<>'0'").fetchone()[0]),
            }
        finally:
            con.close()
    else:
        failures["core_database"] = "missing"
    expected_counts = manifest.get("counts") or {}
    if actual_counts != expected_counts:
        failures["row_counts"] = {"expected": expected_counts, "actual": actual_counts}
    nonzero = {key: value for key, value in checks.items() if value}
    if nonzero:
        failures["invariants"] = nonzero
    if hierarchy.is_file():
        con = duckdb.connect(str(hierarchy), read_only=True)
        try:
            hierarchy_counts = {
                "hierarchy_nodes": int(con.execute("SELECT count(*) FROM hierarchy_nodes").fetchone()[0]),
                "hierarchy_edges": int(con.execute("SELECT count(*) FROM hierarchy_edges").fetchone()[0]),
            }
            metadata_bad = int(con.execute(
                "SELECT count(*) FROM build_metadata WHERE key='stability_database_opened' AND value<>'0'"
            ).fetchone()[0])
            if metadata_bad:
                failures["hierarchy_metadata_stability_opened"] = metadata_bad
        finally:
            con.close()
    else:
        hierarchy_counts = {}
        failures["hierarchy_database"] = "missing"
    return {
        "schema_version": "spacegate.e7_clean_foundation_verification.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_id": manifest.get("build_id"),
        "status": "pass" if not failures else "fail",
        "build_dir": str(build_dir),
        "counts": actual_counts,
        "hierarchy_counts": hierarchy_counts,
        "checks": checks,
        "failing_checks": failures,
        "wall_seconds": round(time.monotonic() - started, 6),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = verify(args.artifact_root.resolve() / args.build_id)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    if report["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
