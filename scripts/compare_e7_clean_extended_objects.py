#!/usr/bin/env python3
"""Account clean extended-object changes against the stability reference."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_STABILITY = "20260717T0614Z_f452835_side"
DEFAULT_CLEAN = "c203e4f451890660ec02086a"


def sql_literal(value: Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def compare(stability: Path, clean: Path) -> dict[str, Any]:
    con = duckdb.connect()
    try:
        con.execute(f"ATTACH {sql_literal(stability)} AS stability (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(clean)} AS clean (READ_ONLY)")
        metrics = {
            "stability_inventory": int(con.execute("SELECT count(*) FROM stability.extended_objects").fetchone()[0]),
            "clean_inventory": int(con.execute("SELECT count(*) FROM clean.extended_objects").fetchone()[0]),
            "missing_clean_identity": int(con.execute("SELECT count(*) FROM stability.extended_objects o LEFT JOIN clean.extended_objects n USING(extended_object_id) WHERE n.extended_object_id IS NULL").fetchone()[0]),
            "new_clean_identity": int(con.execute("SELECT count(*) FROM clean.extended_objects n LEFT JOIN stability.extended_objects o USING(extended_object_id) WHERE o.extended_object_id IS NULL").fetchone()[0]),
            "shared_stable_key_mismatch": int(con.execute("SELECT count(*) FROM stability.extended_objects o JOIN clean.extended_objects n USING(extended_object_id) WHERE o.stable_object_key<>n.stable_object_key").fetchone()[0]),
            "stability_geometry_rows": int(con.execute("SELECT count(*) FROM stability.extended_objects WHERE ra_deg IS NOT NULL AND dec_deg IS NOT NULL").fetchone()[0]),
            "clean_geometry_rows": int(con.execute("SELECT count(*) FROM clean.extended_objects WHERE ra_deg IS NOT NULL AND dec_deg IS NOT NULL").fetchone()[0]),
            "shared_geometry_coordinate_changes": int(con.execute("SELECT count(*) FROM stability.extended_objects o JOIN clean.extended_objects n USING(extended_object_id) WHERE o.ra_deg IS NOT NULL AND n.ra_deg IS NOT NULL AND (o.ra_deg<>n.ra_deg OR o.dec_deg<>n.dec_deg)").fetchone()[0]),
            "stability_only_geometry_rows": int(con.execute("SELECT count(*) FROM stability.extended_objects o JOIN clean.extended_objects n USING(extended_object_id) WHERE o.ra_deg IS NOT NULL AND n.ra_deg IS NULL").fetchone()[0]),
            "clean_only_geometry_rows": int(con.execute("SELECT count(*) FROM stability.extended_objects o JOIN clean.extended_objects n USING(extended_object_id) WHERE o.ra_deg IS NULL AND n.ra_deg IS NOT NULL").fetchone()[0]),
            "stability_distance_rows": int(con.execute("SELECT count(*) FROM stability.extended_objects WHERE dist_pc IS NOT NULL").fetchone()[0]),
            "clean_distance_rows": int(con.execute("SELECT count(*) FROM clean.extended_objects WHERE dist_pc IS NOT NULL").fetchone()[0]),
            "stability_only_distance_rows": int(con.execute("SELECT count(*) FROM stability.extended_objects o JOIN clean.extended_objects n USING(extended_object_id) WHERE o.dist_pc IS NOT NULL AND n.dist_pc IS NULL").fetchone()[0]),
            "selected_distance_candidates": int(con.execute("SELECT count(*) FROM clean.extended_object_distance_candidates").fetchone()[0]),
            "selected_distance_rows": int(con.execute("SELECT count(*) FROM clean.selected_extended_object_distance").fetchone()[0]),
            "selected_relation_distance_rows": int(con.execute("SELECT count(*) FROM clean.selected_extended_object_relation_distance WHERE selection_status='selected'").fetchone()[0]),
            "accepted_relation_bindings": int(con.execute("SELECT count(*) FROM clean.extended_object_relation_bindings WHERE binding_status='accepted'").fetchone()[0]),
            "geometry_changes_without_selected_cluster_evidence": int(con.execute(
                "SELECT count(*) FROM stability.extended_objects o JOIN clean.extended_objects n USING(extended_object_id) "
                "WHERE o.ra_deg IS NOT NULL AND n.ra_deg IS NOT NULL "
                "AND (o.ra_deg<>n.ra_deg OR o.dec_deg<>n.dec_deg) "
                "AND n.source_catalog NOT IN ('clusters.hunt_reffert_2024','clusters.cantat_gaudin_2020')"
            ).fetchone()[0]),
            "clean_distances_without_selected_evidence": int(con.execute(
                "SELECT count(*) FROM clean.extended_objects o WHERE o.dist_pc IS NOT NULL "
                "AND NOT EXISTS (SELECT 1 FROM clean.selected_extended_object_distance d "
                "WHERE d.extended_object_id=o.extended_object_id AND d.dist_pc=o.dist_pc)"
            ).fetchone()[0]),
        }
        geometry_tail = {
            str(row[0] or "missing"): int(row[1])
            for row in con.execute(
                "SELECT split_part(o.geometry_source_record_key,':',1),count(*) "
                "FROM stability.extended_objects o JOIN clean.extended_objects n USING(extended_object_id) "
                "WHERE o.ra_deg IS NOT NULL AND n.ra_deg IS NULL GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        distance_tail = {
            str(row[0] or "missing"): int(row[1])
            for row in con.execute(
                "SELECT o.distance_method,count(*) FROM stability.extended_objects o "
                "JOIN clean.extended_objects n USING(extended_object_id) "
                "WHERE o.dist_pc IS NOT NULL AND n.dist_pc IS NULL GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        checks = {
            "inventory_preserved": metrics["stability_inventory"] == metrics["clean_inventory"],
            "identity_preserved": metrics["missing_clean_identity"] == 0
            and metrics["new_clean_identity"] == 0
            and metrics["shared_stable_key_mismatch"] == 0,
            "geometry_changes_are_selected_cluster_evidence": metrics["geometry_changes_without_selected_cluster_evidence"] == 0,
            "no_unexplained_geometry_additions": metrics["clean_only_geometry_rows"] == 0,
            "geometry_inventory_complete": metrics["stability_only_geometry_rows"] == 0,
            "selected_distances_materialized": metrics["clean_distance_rows"] == metrics["selected_distance_rows"] == 1909,
            "selected_relation_distances_materialized": metrics["selected_relation_distance_rows"] == 59,
            "clean_distances_have_selected_evidence": metrics["clean_distances_without_selected_evidence"] == 0,
            "distance_tail_fully_accounted": sum(distance_tail.values()) == metrics["stability_only_distance_rows"],
            "distance_tail_is_invalid_scope_or_relation_deferred": distance_tail == {
                "associated_star_gaia_dr3_v1": 1,
                "cantat_gaudin_2020_cluster_distance": 19,
            },
        }
    finally:
        con.close()
    failing = sorted(name for name, passed in checks.items() if not passed)
    return {
        "schema_version": "spacegate.e7_clean_extended_objects_ab.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "pass" if not failing else "fail",
        "geometry_cutover_status": "selected_cluster_evidence_complete",
        "distance_cutover_status": "selected_cluster_and_relation_complete_one_identity_tail_deferred",
        "checks": checks, "failing_checks": failing, "metrics": metrics,
        "deferred_geometry_by_source": geometry_tail,
        "deferred_distance_by_method": distance_tail,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--stability-build-id", default=DEFAULT_STABILITY)
    parser.add_argument("--clean-build-id", default=DEFAULT_CLEAN)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = compare(
        args.state_dir / "out" / args.stability_build_id / "core.duckdb",
        Path("/mnt/space/spacegate/e7-clean-extended-objects") / args.clean_build_id / "clean_extended_objects.duckdb",
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    raise SystemExit(0 if report["status"] == "pass" else 1)


if __name__ == "__main__":
    main()
