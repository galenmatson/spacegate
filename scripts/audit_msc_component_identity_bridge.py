#!/usr/bin/env python3
"""Audit a runtime ARM candidate using the exact MSC component identity bridge."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any

import duckdb


INVENTORY_TABLES = (
    "component_entities",
    "system_hierarchy_edges",
    "orbit_edges",
    "orbital_solutions",
    "stellar_leaf_display_classifications",
    "msc_runtime_leaf_bindings",
)


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def rows_as_dicts(
    con: duckdb.DuckDBPyConnection, sql: str, parameters: list[Any] | None = None
) -> list[dict[str, Any]]:
    cursor = con.execute(sql, parameters or [])
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def audit(*, candidate_arm: Path, reference_arm: Path) -> dict[str, Any]:
    started = time.monotonic()
    con = duckdb.connect(str(candidate_arm), read_only=True)
    try:
        con.execute(
            f"ATTACH {sql_literal(reference_arm.resolve())} AS reference (READ_ONLY)"
        )
        con.execute(
            """
            CREATE TEMP VIEW classification_delta AS
            SELECT
              coalesce(candidate.hierarchy_node_key,baseline.hierarchy_node_key)
                AS hierarchy_node_key,
              baseline.classification_value AS reference_class,
              baseline.classification_status AS reference_status,
              baseline.evidence_basis AS reference_basis,
              baseline.selected_fact_id AS reference_fact_id,
              candidate.classification_value AS candidate_class,
              candidate.classification_status AS candidate_status,
              candidate.evidence_basis AS candidate_basis,
              candidate.selected_fact_id AS candidate_fact_id,
              candidate.source_catalog AS candidate_source_catalog,
              candidate.source_value AS candidate_source_value,
              candidate.system_id,
              candidate.display_name
            FROM reference.stellar_leaf_display_classifications baseline
            FULL OUTER JOIN stellar_leaf_display_classifications candidate
              USING(hierarchy_node_key)
            WHERE baseline.hierarchy_node_key IS NULL
               OR candidate.hierarchy_node_key IS NULL
               OR baseline.classification_value IS DISTINCT FROM candidate.classification_value
               OR baseline.classification_status IS DISTINCT FROM candidate.classification_status
               OR baseline.evidence_basis IS DISTINCT FROM candidate.evidence_basis
               OR baseline.selected_fact_id IS DISTINCT FROM candidate.selected_fact_id;

            CREATE TEMP VIEW binding_delta AS
            SELECT candidate.component_entity_id,candidate.wds_id_raw,
              candidate.component_label_raw,candidate.component_label_normalized,
              baseline.runtime_binding_status AS reference_status,
              baseline.runtime_binding_reason AS reference_reason,
              baseline.hierarchy_node_key AS reference_hierarchy_node_key,
              candidate.runtime_binding_status AS candidate_status,
              candidate.runtime_binding_reason AS candidate_reason,
              candidate.hierarchy_node_key AS candidate_hierarchy_node_key,
              candidate.runtime_component_key,candidate.source_component_key,
              candidate.runtime_identity_bridge_id,
              candidate.runtime_identity_bridge_build_id,
              candidate.runtime_identity_bridge_policy_version
            FROM reference.msc_runtime_leaf_bindings baseline
            FULL OUTER JOIN msc_runtime_leaf_bindings candidate
              USING(component_entity_id)
            WHERE baseline.component_entity_id IS NULL
               OR candidate.component_entity_id IS NULL
               OR baseline.runtime_binding_status IS DISTINCT FROM candidate.runtime_binding_status
               OR baseline.runtime_binding_reason IS DISTINCT FROM candidate.runtime_binding_reason
               OR baseline.hierarchy_node_key IS DISTINCT FROM candidate.hierarchy_node_key
               OR baseline.runtime_component_key IS DISTINCT FROM candidate.runtime_component_key;
            """
        )
        scalar = lambda sql: int(con.execute(sql).fetchone()[0] or 0)
        inventory = {
            table: {
                "reference": scalar(f"SELECT count(*) FROM reference.{table}"),
                "candidate": scalar(f"SELECT count(*) FROM {table}"),
            }
            for table in INVENTORY_TABLES
        }
        for values in inventory.values():
            values["delta"] = values["candidate"] - values["reference"]

        checks = {
            "inventory_count_delta": sum(abs(values["delta"]) for values in inventory.values()),
            "classification_changes_outside_exact_bridge": scalar(
                "SELECT count(*) FROM classification_delta d LEFT JOIN msc_runtime_leaf_bindings b "
                "ON b.hierarchy_node_key=d.hierarchy_node_key "
                "AND b.runtime_binding_reason='exact_release_scoped_leaf_identity_bridge' "
                "WHERE b.binding_id IS NULL"
            ),
            "known_to_unknown": scalar(
                "SELECT count(*) FROM classification_delta "
                "WHERE reference_class<>'UNKNOWN' AND candidate_class='UNKNOWN'"
            ),
            "known_to_different": scalar(
                "SELECT count(*) FROM classification_delta "
                "WHERE reference_class<>'UNKNOWN' AND candidate_class<>'UNKNOWN' "
                "AND reference_class<>candidate_class"
            ),
            "removed_or_added_leaf_rows": scalar(
                "SELECT count(*) FROM classification_delta "
                "WHERE reference_class IS NULL OR candidate_class IS NULL"
            ),
            "unexpected_binding_delta": scalar(
                "SELECT count(*) FROM binding_delta WHERE NOT ("
                "reference_status='ambiguous' "
                "AND reference_reason='case_significant_source_collision' "
                "AND candidate_status='accepted' "
                "AND candidate_reason='exact_release_scoped_leaf_identity_bridge')"
            ),
            "recovered_binding_without_lineage": scalar(
                "SELECT count(*) FROM binding_delta WHERE "
                "candidate_reason='exact_release_scoped_leaf_identity_bridge' AND ("
                "runtime_identity_bridge_id IS NULL "
                "OR runtime_identity_bridge_build_id IS NULL "
                "OR runtime_identity_bridge_policy_version IS NULL)"
            ),
            "recovered_binding_bridge_mismatch": scalar(
                "SELECT count(*) FROM binding_delta d "
                "LEFT JOIN stellar_orbit_endpoint_bindings bridge "
                "ON bridge.endpoint_binding_id=d.runtime_identity_bridge_id "
                "WHERE d.candidate_reason='exact_release_scoped_leaf_identity_bridge' "
                "AND (bridge.endpoint_binding_id IS NULL "
                "OR bridge.component_entity_id<>d.component_entity_id "
                "OR bridge.endpoint_kind<>'leaf' OR bridge.binding_status<>'accepted' "
                "OR bridge.hierarchy_node_key<>d.candidate_hierarchy_node_key)"
            ),
            "multiple_accepted_collision_bindings": scalar(
                "SELECT count(*) FROM (SELECT wds_id_raw,"
                "lower(component_label_normalized) FROM msc_runtime_leaf_bindings "
                "WHERE source_candidate_count>1 AND runtime_binding_status='accepted' "
                "GROUP BY 1,2 HAVING count(*)>1)"
            ),
            "canonical_containment_promotions": scalar(
                "SELECT count(*) FROM msc_runtime_leaf_bindings WHERE canonical_containment"
            ),
        }
        failing = {key: value for key, value in checks.items() if value}
        return {
            "schema_version": "spacegate.msc_component_identity_bridge_ab.v1",
            "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "status": "pass" if not failing else "fail",
            "candidate_arm": str(candidate_arm.resolve()),
            "reference_arm": str(reference_arm.resolve()),
            "policy": {
                "named_object_rules": False,
                "only_preexisting_casefold_collisions_may_change": True,
                "unique_accepted_leaf_bridge_required": True,
                "ambiguous_tail_remains_deferred": True,
            },
            "inventory": inventory,
            "counts": {
                "binding_deltas": scalar("SELECT count(*) FROM binding_delta"),
                "classification_deltas": scalar("SELECT count(*) FROM classification_delta"),
                "unknown_to_known": scalar(
                    "SELECT count(*) FROM classification_delta "
                    "WHERE reference_class='UNKNOWN' AND candidate_class<>'UNKNOWN'"
                ),
                "recovered_collision_groups": scalar(
                    "SELECT count(*) FROM msc_runtime_leaf_bindings WHERE "
                    "runtime_binding_reason='exact_release_scoped_leaf_identity_bridge'"
                ),
                "deferred_collision_groups": scalar(
                    "SELECT count(*) FROM (SELECT wds_id_raw,"
                    "lower(component_label_normalized) FROM msc_runtime_leaf_bindings "
                    "WHERE source_candidate_count>1 GROUP BY 1,2 HAVING count(*) "
                    "FILTER (WHERE runtime_binding_status='accepted')=0)"
                ),
            },
            "classification_deltas_by_basis": rows_as_dicts(
                con,
                "SELECT candidate_status,candidate_basis,candidate_source_catalog,"
                "candidate_class,count(*) AS rows FROM classification_delta "
                "GROUP BY ALL ORDER BY rows DESC,candidate_basis,candidate_class",
            ),
            "binding_deltas_by_status": rows_as_dicts(
                con,
                "SELECT reference_status,reference_reason,candidate_status,candidate_reason,"
                "count(*) AS rows FROM binding_delta GROUP BY ALL ORDER BY rows DESC",
            ),
            "classification_deltas": rows_as_dicts(
                con,
                "SELECT * FROM classification_delta ORDER BY hierarchy_node_key",
            ),
            "checks": checks,
            "failing_checks": failing,
            "wall_seconds": round(time.monotonic() - started, 6),
        }
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-arm", type=Path, required=True)
    parser.add_argument("--reference-arm", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = audit(
        candidate_arm=args.candidate_arm.resolve(),
        reference_arm=args.reference_arm.resolve(),
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
