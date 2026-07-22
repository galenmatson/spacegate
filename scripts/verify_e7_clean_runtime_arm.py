#!/usr/bin/env python3
"""Independently verify an E7 clean runtime ARM artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ARTIFACT_ROOT = Path("/mnt/space/spacegate/e7-clean-runtime-arm")
SCIENCE_TABLES = {
    "selected_planet_parameters",
    "selected_stellar_astrometry",
    "selected_stellar_classification",
    "selected_stellar_display_classifications",
    "selected_stellar_parameters",
    "selected_stellar_photometry",
    "selected_stellar_physics",
    "selected_stellar_variability",
}
WISE_TABLES = {
    "allwise_sources",
    "catwise_sources",
    "infrared_candidate_queue",
    "infrared_motion_evidence",
    "infrared_photometry",
    "infrared_source_matches",
    "wise_sources",
}
RUNTIME_TABLES = {
    "build_metadata",
    "component_entities",
    "stellar_leaf_display_classifications",
    "system_hierarchy_edges",
}
EXPECTED_TABLES = SCIENCE_TABLES | WISE_TABLES | RUNTIME_TABLES
EXPECTED_VIEWS = {
    "e6_selected_planet_parameters",
    "e6_selected_stellar_display_classifications",
    "e6_selected_stellar_parameters",
}
REQUIRED_COLUMNS = {
    "component_entities": {
        "component_entity_id", "stable_component_key", "component_type",
        "core_object_type", "core_object_id", "display_name", "source_catalog",
        "source_version", "source_pk", "source_row_hash", "transform_version",
    },
    "system_hierarchy_edges": {
        "hierarchy_edge_id", "parent_component_key", "child_component_key",
        "edge_kind", "member_role", "confidence_score", "source_catalog",
        "source_version", "source_pk", "source_row_hash", "transform_version",
    },
    "stellar_leaf_display_classifications": {
        "stellar_leaf_classification_id", "system_id", "hierarchy_node_key",
        "leaf_component_key", "classification_value", "classification_status",
        "evidence_basis", "selected_fact_id", "source_catalog", "source_pk",
        "confidence_score", "has_classification_conflict", "projection_version",
    },
}
VALID_CLASSES = (
    "O", "B", "A", "F", "G", "K", "M", "L", "T", "Y", "WR", "WD",
    "NS", "PULSAR", "MAGNETAR", "BLACK HOLE", "UNKNOWN",
)


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


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0] or 0)


def verify(build_dir: Path) -> dict[str, Any]:
    started = time.monotonic()
    manifest = load_object(build_dir / "manifest.json")
    failures: dict[str, Any] = {}
    if manifest.get("schema_version") != "spacegate.e7_clean_runtime_arm_manifest.v1":
        failures["manifest_schema"] = manifest.get("schema_version")
    if manifest.get("status") != "pass":
        failures["manifest_status"] = manifest.get("status")
    if manifest.get("stability_databases_opened") != []:
        failures["stability_databases_opened"] = manifest.get("stability_databases_opened")

    product_failures: dict[str, Any] = {}
    for relative, expected in sorted((manifest.get("products") or {}).items()):
        path = build_dir / relative
        if not path.is_file():
            product_failures[relative] = "missing"
            continue
        actual = {"bytes": path.stat().st_size, "sha256": file_sha256(path)}
        expected_identity = {
            "bytes": expected.get("bytes"), "sha256": expected.get("sha256")
        }
        if actual != expected_identity:
            product_failures[relative] = {"expected": expected_identity, "actual": actual}
    if product_failures:
        failures["products"] = product_failures

    counts: dict[str, int] = {}
    checks: dict[str, int] = {}
    db = build_dir / "arm.duckdb"
    if not db.is_file():
        failures["database"] = "missing"
    else:
        con = duckdb.connect(str(db), read_only=True)
        try:
            rows = con.execute(
                "SELECT table_name,table_type FROM information_schema.tables "
                "WHERE table_schema='main'"
            ).fetchall()
            tables = {str(name) for name, kind in rows if kind == "BASE TABLE"}
            views = {str(name) for name, kind in rows if kind == "VIEW"}
            if tables != EXPECTED_TABLES:
                failures["table_set"] = {
                    "missing": sorted(EXPECTED_TABLES - tables),
                    "unexpected": sorted(tables - EXPECTED_TABLES),
                }
            if views != EXPECTED_VIEWS:
                failures["view_set"] = {
                    "missing": sorted(EXPECTED_VIEWS - views),
                    "unexpected": sorted(views - EXPECTED_VIEWS),
                }
            for table, required in REQUIRED_COLUMNS.items():
                columns = {str(row[0]) for row in con.execute(f"DESCRIBE {table}").fetchall()}
                missing = sorted(required - columns)
                if missing:
                    failures[f"{table}_columns"] = missing

            manifest_counts = (manifest.get("verification") or {}).get("counts") or {}
            counts = {
                table: scalar(con, f"SELECT count(*) FROM {table}")
                for table in sorted(EXPECTED_TABLES)
            }
            checks = {
                "duplicate_component_ids": scalar(con, "SELECT count(*) FROM (SELECT component_entity_id FROM component_entities GROUP BY 1 HAVING count(*)<>1)"),
                "duplicate_component_keys": scalar(con, "SELECT count(*) FROM (SELECT stable_component_key FROM component_entities GROUP BY 1 HAVING count(*)<>1)"),
                "duplicate_hierarchy_edges": scalar(con, "SELECT count(*) FROM (SELECT hierarchy_edge_id FROM system_hierarchy_edges GROUP BY 1 HAVING count(*)<>1)"),
                "duplicate_leaf_nodes": scalar(con, "SELECT count(*) FROM (SELECT hierarchy_node_key FROM stellar_leaf_display_classifications GROUP BY 1 HAVING count(*)<>1)"),
                "orphan_hierarchy_parents": scalar(con, "SELECT count(*) FROM system_hierarchy_edges e LEFT JOIN component_entities c ON c.stable_component_key=e.parent_component_key WHERE c.component_entity_id IS NULL"),
                "orphan_hierarchy_children": scalar(con, "SELECT count(*) FROM system_hierarchy_edges e LEFT JOIN component_entities c ON c.stable_component_key=e.child_component_key WHERE c.component_entity_id IS NULL"),
                "source_claim_containment_edges": scalar(con, "SELECT count(*) FROM system_hierarchy_edges WHERE source_catalog<>'canonical_hierarchy'"),
                "invalid_leaf_classes": scalar(con, f"SELECT count(*) FROM stellar_leaf_display_classifications WHERE classification_value NOT IN {VALID_CLASSES}"),
                "nonmissing_leaf_without_lineage": scalar(con, "SELECT count(*) FROM stellar_leaf_display_classifications WHERE classification_status<>'missing' AND (evidence_basis IS NULL OR source_catalog IS NULL OR source_pk IS NULL)"),
                "duplicate_selected_stars": scalar(con, "SELECT count(*) FROM (SELECT star_id FROM selected_stellar_parameters GROUP BY 1 HAVING count(*)<>1)"),
                "duplicate_selected_display_stars": scalar(con, "SELECT count(*) FROM (SELECT star_id FROM selected_stellar_display_classifications GROUP BY 1 HAVING count(*)<>1)"),
                "duplicate_selected_planets": scalar(con, "SELECT count(*) FROM (SELECT planet_id FROM selected_planet_parameters GROUP BY 1 HAVING count(*)<>1)"),
                "parameter_view_count_delta": abs(scalar(con, "SELECT count(*) FROM e6_selected_stellar_parameters") - counts.get("selected_stellar_parameters", -1)),
                "display_view_count_delta": abs(scalar(con, "SELECT count(*) FROM e6_selected_stellar_display_classifications") - counts.get("selected_stellar_display_classifications", -1)),
                "planet_view_count_delta": abs(scalar(con, "SELECT count(*) FROM e6_selected_planet_parameters") - counts.get("selected_planet_parameters", -1)),
                "metadata_stability_opened": scalar(con, "SELECT count(*) FROM build_metadata WHERE key='stability_database_opened' AND value<>'0'"),
                "manifest_count_delta": sum(
                    abs(counts.get(table, -1) - int(expected))
                    for table, expected in manifest_counts.items()
                ),
            }
        finally:
            con.close()

    nonzero = {key: value for key, value in checks.items() if value}
    if nonzero:
        failures["invariants"] = nonzero
    return {
        "schema_version": "spacegate.e7_clean_runtime_arm_verification.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_id": manifest.get("build_id"),
        "status": "pass" if not failures else "fail",
        "counts": counts,
        "checks": checks,
        "runtime_graph_status": manifest.get("verification", {}).get("runtime_graph_status"),
        "failing_checks": failures,
        "wall_seconds": round(time.monotonic() - started, 6),
    }


def main() -> int:
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
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
