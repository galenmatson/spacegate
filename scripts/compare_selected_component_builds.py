#!/usr/bin/env python3
"""Compare selected-component artifacts and explain every semantic change."""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


TABLE_SPECS = {
    "msc_relation_evidence_projection": (
        "relation_evidence_id",
        ("source_record_id", "left_source_component_key", "right_source_component_key", "canonical_system_stable_object_key", "projection_status"),
    ),
    "msc_component_parameter_set_bindings": (
        "parameter_set_id",
        ("source_record_id", "component_scope", "target_key", "canonical_system_stable_object_key", "binding_status"),
    ),
    "msc_stellar_parameter_projection": (
        "evidence_id",
        ("source_record_id", "component_scope", "quantity_key", "target_key", "canonical_system_stable_object_key", "projection_status"),
    ),
    "msc_classification_projection": (
        "evidence_id",
        ("source_record_id", "component_scope", "classification_normalized", "target_key", "canonical_system_stable_object_key", "projection_status"),
    ),
    "msc_photometry_projection": (
        "evidence_id",
        ("source_record_id", "quantity_key", "target_key", "canonical_system_stable_object_key", "projection_status"),
    ),
    "msc_astrometry_projection": (
        "evidence_id",
        ("source_record_id", "quantity_key", "target_key", "canonical_system_stable_object_key", "projection_status"),
    ),
    "msc_orbit_solution_bindings": (
        "orbit_evidence_id",
        ("source_record_id", "primary_source_component_key", "secondary_source_component_key", "canonical_system_stable_object_key", "binding_status"),
    ),
    "msc_orbital_solution_projection": (
        "evidence_id",
        ("source_record_id", "primary_source_component_key", "secondary_source_component_key", "canonical_system_stable_object_key", "projection_status"),
    ),
    "sb9_relation_bindings": (
        "sb9_relation_evidence_id",
        ("source_record_id", "sb9_sequence", "msc_relation_evidence_id", "canonical_system_stable_object_key", "binding_status"),
    ),
    "sb9_parameter_set_bindings": (
        "parameter_set_id",
        ("source_record_id", "component_scope", "target_key", "canonical_system_stable_object_key", "binding_status"),
    ),
    "sb9_stellar_parameter_projection": (
        "evidence_id",
        ("source_record_id", "component_scope", "quantity_key", "target_key", "canonical_system_stable_object_key", "projection_status"),
    ),
    "sb9_classification_projection": (
        "evidence_id",
        ("source_record_id", "component_scope", "classification_normalized", "target_key", "canonical_system_stable_object_key", "projection_status"),
    ),
    "sb9_orbital_solution_projection": (
        "evidence_id",
        ("source_record_id", "primary_source_component_key", "secondary_source_component_key", "canonical_system_stable_object_key", "projection_status"),
    ),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def load_artifact(root: Path) -> tuple[dict[str, Any], Path]:
    manifest_path = root / "manifest.json"
    database = root / "selected_components.duckdb"
    if not manifest_path.is_file() or not database.is_file():
        raise FileNotFoundError(f"incomplete selected-component artifact: {root}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") not in (None, "pass") or any(
        int(value or 0) != 0 for value in (manifest.get("verification") or {}).values()
    ):
        raise ValueError(f"unaccepted selected-component artifact: {root}")
    return manifest, database


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def status_counts(con: duckdb.DuckDBPyConnection, alias: str, table: str, field: str) -> dict[str, int]:
    return {
        str(status): int(count)
        for status, count in con.execute(
            f"SELECT coalesce(cast({field} AS VARCHAR),'NULL'),count(*) FROM {alias}.{table} GROUP BY 1 ORDER BY 1"
        ).fetchall()
    }


def compare_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    key: str,
    fields: tuple[str, ...],
) -> dict[str, Any]:
    status_field = next(field for field in fields if field.endswith("_status"))
    projection = ",".join(f"{side}.{field} AS {side}_{field}" for field in fields for side in ("b", "a"))
    changed = con.execute(
        f"""
        SELECT coalesce(b.{key},a.{key}) change_key,{projection}
        FROM before.{table} b FULL OUTER JOIN after.{table} a USING({key})
        WHERE ({','.join(f'b.{field}' for field in fields)})
              IS DISTINCT FROM ({','.join(f'a.{field}' for field in fields)})
        ORDER BY change_key
        """
    ).fetchall()
    columns = [description[0] for description in con.description]
    rows = [dict(zip(columns, row)) for row in changed]
    return {
        "row_count_before": int(con.execute(f"SELECT count(*) FROM before.{table}").fetchone()[0]),
        "row_count_after": int(con.execute(f"SELECT count(*) FROM after.{table}").fetchone()[0]),
        "status_counts_before": status_counts(con, "before", table, status_field),
        "status_counts_after": status_counts(con, "after", table, status_field),
        "semantic_change_count": len(rows),
        "changes": rows,
    }


def component_parts(key: str | None) -> tuple[str, str] | None:
    parts = str(key or "").split(":")
    if len(parts) < 2:
        return None
    label = parts[-1].strip()
    # MSC case is semantic: Ab is a stellar leaf, while AB is a subsystem.
    # The legacy hierarchy case-folded both into the same key, so only
    # case-valid terminal labels may support an inferred stellar leaf.
    if not re.fullmatch(r"[A-Z][a-z][0-9]*", label):
        return None
    return parts[-2].lower(), label.lower()


def hierarchy_impacts(
    con: duckdb.DuckDBPyConnection,
    hierarchy_path: Path | None,
    context_relations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if hierarchy_path is None:
        return []
    con.execute(f"ATTACH {sql_literal(hierarchy_path)} AS hierarchy (READ_ONLY)")
    accepted_after = {
        component_parts(key)
        for row in con.execute(
            "SELECT left_source_component_key,right_source_component_key FROM after.msc_relation_evidence_projection WHERE projection_status='accepted_relation_evidence'"
        ).fetchall()
        for key in row
    }
    candidates = {
        component_parts(key)
        for relation in context_relations
        for key in (relation.get("left_source_component_key"), relation.get("right_source_component_key"))
    }
    unsupported = {value for value in candidates if value and value not in accepted_after}
    impacts: list[dict[str, Any]] = []
    for node_key, display_name, wds_id, source_basis in con.execute(
        "SELECT hierarchy_node_key,display_name,wds_id,source_basis FROM hierarchy.hierarchy_nodes WHERE source_basis='msc_inferred_leaf' ORDER BY hierarchy_node_key"
    ).fetchall():
        suffix = str(node_key).rsplit(":", 1)[-1].lower()
        if (str(wds_id or "").lower(), suffix) in unsupported:
            impacts.append({
                "hierarchy_node_key": node_key,
                "display_name": display_name,
                "wds_id": wds_id,
                "source_basis": source_basis,
                "after_supporting_accepted_relation_count": 0,
            })
    return impacts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", required=True, type=Path)
    parser.add_argument("--after", required=True, type=Path)
    parser.add_argument("--hierarchy", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    before_manifest, before_db = load_artifact(args.before.resolve())
    after_manifest, after_db = load_artifact(args.after.resolve())
    con = duckdb.connect()
    con.execute(f"ATTACH {sql_literal(before_db)} AS before (READ_ONLY)")
    con.execute(f"ATTACH {sql_literal(after_db)} AS after (READ_ONLY)")
    tables = {
        table: compare_table(con, table, key, fields)
        for table, (key, fields) in TABLE_SPECS.items()
    }
    context_rows = con.execute(
        """
        SELECT source_record_id,left_source_component_key,right_source_component_key,
               canonical_system_stable_object_key,quality_json,projection_status
        FROM after.msc_relation_evidence_projection
        WHERE projection_status='context_only_planetary_relation_evidence'
        ORDER BY canonical_system_stable_object_key,source_record_id
        """
    ).fetchall()
    context_relations = [
        {
            "source_record_id": row[0],
            "left_source_component_key": row[1],
            "right_source_component_key": row[2],
            "canonical_system_stable_object_key": row[3],
            "quality": json.loads(row[4]) if isinstance(row[4], str) else row[4],
            "projection_status": row[5],
        }
        for row in context_rows
    ]
    context_source_records = {row["source_record_id"] for row in context_relations}
    context_orbit_source_records = {
        str(row[0])
        for row in con.execute(
            "SELECT source_record_id FROM after.msc_orbit_solution_bindings WHERE binding_status='context_only_planetary_relation'"
        ).fetchall()
    }
    expected_msc_source_records = context_source_records | context_orbit_source_records
    msc_changed_sources = {
        str(change.get("a_source_record_id") or change.get("b_source_record_id"))
        for table, comparison in tables.items()
        if table.startswith("msc_")
        for change in comparison["changes"]
    }
    msc_changed_sources.discard("None")
    source_reports_before = {item["source_id"]: item["observed"] for item in before_manifest.get("source_reports", [])}
    source_reports_after = {item["source_id"]: item["observed"] for item in after_manifest.get("source_reports", [])}
    unaffected_source_reports_equal = all(
        source_reports_before.get(source_id) == observed
        for source_id, observed in source_reports_after.items()
        if source_id not in {"multiplicity.msc", "multiplicity.sb9"}
    )
    row_counts_equal = all(
        comparison["row_count_before"] == comparison["row_count_after"]
        for comparison in tables.values()
    )
    unexpected_msc_sources = sorted(msc_changed_sources - expected_msc_source_records)
    impacts = hierarchy_impacts(con, args.hierarchy.resolve() if args.hierarchy else None, context_relations)
    checks = {
        "all_compared_table_row_counts_preserved": row_counts_equal,
        "all_msc_changes_trace_to_planetary_source_records": not unexpected_msc_sources,
        "unaffected_source_reports_unchanged": unaffected_source_reports_equal,
        "context_relations_exist": len(context_relations) > 0,
        "no_context_relation_remains_stellar_eligible": all(
            row[0] == 0
            for row in con.execute(
                """
                SELECT count(*) FROM after.msc_relation_evidence_projection
                WHERE source_record_id IN (SELECT source_record_id FROM after.msc_relation_evidence_projection WHERE projection_status='context_only_planetary_relation_evidence')
                  AND projection_status='accepted_relation_evidence'
                """
            ).fetchall()
        ),
    }
    report = {
        "schema_version": "spacegate.selected_component_scientific_ab.v1",
        "status": "pass" if all(checks.values()) else "fail",
        "generated_at_utc": utc_now(),
        "before": {"build_id": before_manifest.get("build_id"), "path": str(args.before.resolve())},
        "after": {"build_id": after_manifest.get("build_id"), "path": str(args.after.resolve())},
        "checks": checks,
        "unexpected_msc_source_records": unexpected_msc_sources,
        "context_relation_count": len(context_relations),
        "affected_system_count": len({row["canonical_system_stable_object_key"] for row in context_relations}),
        "context_relations": context_relations,
        "hierarchy_nodes_losing_selected_relation_support": impacts,
        "hierarchy_node_impact_count": len(impacts),
        "table_comparisons": tables,
    }
    atomic_json(args.output.resolve(), report)
    print(json.dumps({
        "status": report["status"],
        "context_relation_count": report["context_relation_count"],
        "affected_system_count": report["affected_system_count"],
        "hierarchy_node_impact_count": report["hierarchy_node_impact_count"],
        "unexpected_msc_source_records": unexpected_msc_sources,
    }, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
