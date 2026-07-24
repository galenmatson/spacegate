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
SOLAR_IDENTITY_TABLES = {
    "solar_component_aliases",
    "solar_component_identifiers",
    "solar_component_identities",
    "solar_relation_identity_outcomes",
}
SOLAR_RUNTIME_TABLES = {
    "selected_solar_orbital_solutions",
    "selected_solar_physical_parameters",
    "selected_solar_relation_bindings",
    "selected_solar_target_bindings",
}
STELLAR_ORBIT_TABLES = {
    "deferred_stellar_orbit_context",
    "selected_stellar_orbit_relations",
    "selected_stellar_orbit_solutions",
}
STELLAR_ORBIT_BRIDGE_TABLES = {
    "stellar_orbit_endpoint_bindings",
    "stellar_orbit_group_memberships",
    "stellar_orbit_relation_bindings",
}
TESS_RUNTIME_TABLES = {
    "tess_missing_object_audit",
    "tess_target_identity",
    "toi_current_evidence",
    "toi_disposition_history",
}
RUNTIME_TABLES = {
    "build_metadata",
    "component_entities",
    "orbital_solutions",
    "orbit_edges",
    "sol_artificial_objects",
    "sol_small_body_objects",
    "stellar_leaf_display_classifications",
    "msc_runtime_leaf_bindings",
    "system_hierarchy_edges",
}
EXPECTED_TABLES = (
    SCIENCE_TABLES | WISE_TABLES | SOLAR_IDENTITY_TABLES
    | SOLAR_RUNTIME_TABLES | STELLAR_ORBIT_TABLES
    | STELLAR_ORBIT_BRIDGE_TABLES | TESS_RUNTIME_TABLES | RUNTIME_TABLES
)
EXPECTED_VIEWS = {
    "e6_selected_planet_parameters",
    "e6_selected_stellar_display_classifications",
    "e6_selected_stellar_parameters",
}
REQUIRED_COLUMNS = {
    "msc_runtime_leaf_bindings": {
        "binding_id", "component_entity_id", "source_component_key",
        "wds_id_raw", "component_label_raw", "source_candidate_count",
        "runtime_candidate_count", "hierarchy_node_key", "runtime_component_key",
        "runtime_binding_status", "runtime_binding_reason", "canonical_containment",
    },
    "component_entities": {
        "component_entity_id", "stable_component_key", "component_type",
        "core_object_type", "core_object_id", "display_name", "source_catalog",
        "source_version", "source_pk", "source_row_hash", "transform_version",
    },
    "stellar_orbit_group_memberships": {
        "group_membership_id", "group_source_component_key",
        "group_runtime_component_key", "child_source_component_key",
        "child_runtime_component_key", "canonical_system_stable_object_key",
        "source_id", "release_id", "child_descendant_leaf_keys_json",
        "binding_status", "canonical_containment", "policy_version", "build_id",
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
    "orbit_edges": {
        "orbit_edge_id", "host_component_key", "primary_component_key",
        "secondary_component_key", "relation_kind", "preferred_solution_id",
        "confidence_score", "source_catalog", "source_pk", "source_row_hash",
    },
    "orbital_solutions": {
        "orbital_solution_id", "orbit_edge_id", "solution_source_catalog",
        "period_days", "semi_major_axis_au", "eccentricity", "inclination_deg",
        "normalization_method", "source_catalog", "source_pk", "source_row_hash",
    },
    "tess_target_identity": {
        "tess_identity_id", "tic_id", "resolution_status", "star_id", "system_id",
        "source_row_hash", "retrieval_checksum", "transform_version",
    },
    "tess_missing_object_audit": {
        "audit_id", "tic_id", "resolution_status", "gap_class", "source_row_hash",
    },
    "toi_current_evidence": {
        "toi_evidence_id", "source_key", "tic_id", "toi", "disposition",
        "star_id", "system_id", "planet_id", "orbital_period_days",
        "source_row_hash", "retrieval_checksum", "transform_version",
    },
    "toi_disposition_history": {
        "history_id", "source_key", "tic_id", "disposition", "effective_at",
        "source_row_hash", "first_observed_at", "last_observed_at",
        "retrieval_checksum", "transform_version",
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
    if manifest.get("schema_version") != "spacegate.e7_clean_runtime_arm_manifest.v6":
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
                "duplicate_msc_runtime_component_bindings": scalar(con, "SELECT count(*) FROM (SELECT component_entity_id FROM msc_runtime_leaf_bindings GROUP BY 1 HAVING count(*)<>1)"),
                "accepted_msc_runtime_bindings_without_leaf": scalar(con, "SELECT count(*) FROM msc_runtime_leaf_bindings WHERE runtime_binding_status='accepted' AND (hierarchy_node_key IS NULL OR runtime_component_key IS NULL OR runtime_system_stable_object_key IS NULL)"),
                "unaccepted_msc_runtime_bindings_with_leaf": scalar(con, "SELECT count(*) FROM msc_runtime_leaf_bindings WHERE runtime_binding_status<>'accepted' AND (hierarchy_node_key IS NOT NULL OR runtime_component_key IS NOT NULL)"),
                "msc_runtime_containment_promotions": scalar(con, "SELECT count(*) FROM msc_runtime_leaf_bindings WHERE canonical_containment"),
                "orphan_hierarchy_parents": scalar(con, "SELECT count(*) FROM system_hierarchy_edges e LEFT JOIN component_entities c ON c.stable_component_key=e.parent_component_key WHERE c.component_entity_id IS NULL"),
                "orphan_hierarchy_children": scalar(con, "SELECT count(*) FROM system_hierarchy_edges e LEFT JOIN component_entities c ON c.stable_component_key=e.child_component_key WHERE c.component_entity_id IS NULL"),
                "source_claim_containment_edges": scalar(con, "SELECT count(*) FROM system_hierarchy_edges WHERE source_catalog<>'canonical_hierarchy' AND edge_kind='contains'"),
                "invalid_leaf_classes": scalar(con, f"SELECT count(*) FROM stellar_leaf_display_classifications WHERE classification_value NOT IN {VALID_CLASSES}"),
                "nonmissing_leaf_without_lineage": scalar(con, "SELECT count(*) FROM stellar_leaf_display_classifications WHERE classification_status<>'missing' AND (evidence_basis IS NULL OR source_catalog IS NULL OR source_pk IS NULL)"),
                "duplicate_selected_stars": scalar(con, "SELECT count(*) FROM (SELECT star_id FROM selected_stellar_parameters GROUP BY 1 HAVING count(*)<>1)"),
                "duplicate_selected_display_stars": scalar(con, "SELECT count(*) FROM (SELECT star_id FROM selected_stellar_display_classifications GROUP BY 1 HAVING count(*)<>1)"),
                "duplicate_selected_planets": scalar(con, "SELECT count(*) FROM (SELECT planet_id FROM selected_planet_parameters GROUP BY 1 HAVING count(*)<>1)"),
                "parameter_view_count_delta": abs(scalar(con, "SELECT count(*) FROM e6_selected_stellar_parameters") - counts.get("selected_stellar_parameters", -1)),
                "display_view_count_delta": abs(scalar(con, "SELECT count(*) FROM e6_selected_stellar_display_classifications") - counts.get("selected_stellar_display_classifications", -1)),
                "planet_view_count_delta": abs(scalar(con, "SELECT count(*) FROM e6_selected_planet_parameters") - counts.get("selected_planet_parameters", -1)),
                "metadata_stability_opened": scalar(con, "SELECT count(*) FROM build_metadata WHERE key='stability_database_opened' AND value<>'0'"),
                "solar_canonical_containment_promotions": scalar(con, "SELECT (SELECT count(*) FROM solar_relation_identity_outcomes WHERE canonical_containment)+(SELECT count(*) FROM selected_solar_relation_bindings WHERE canonical_containment)+(SELECT count(*) FROM selected_solar_orbital_solutions WHERE canonical_containment)"),
                "solar_identity_projection_delta": abs(scalar(con, "SELECT count(*) FROM component_entities WHERE source_catalog IN ('sol_authority','sol_artificial')") - scalar(con, "SELECT count(*) FROM solar_component_identities WHERE core_object_id IS NULL")),
                "solar_orbit_projection_delta": abs(scalar(con, "SELECT count(*) FROM orbit_edges WHERE source_catalog IN ('sol_authority','sol_artificial')") - scalar(con, "SELECT count(*) FROM selected_solar_orbital_solutions WHERE runtime_eligible")),
                "solar_solution_projection_delta": abs(scalar(con, "SELECT count(*) FROM orbital_solutions WHERE source_catalog IN ('sol_authority','sol_artificial')") - scalar(con, "SELECT count(*) FROM selected_solar_orbital_solutions WHERE runtime_eligible")),
                "stellar_orbit_projection_delta": abs(scalar(con, "SELECT count(*) FROM orbit_edges WHERE transform_version='e7_selected_stellar_orbit_projection_v1'") - scalar(con, "SELECT count(*) FROM stellar_orbit_relation_bindings WHERE simulation_eligible")),
                "stellar_solution_projection_delta": abs(scalar(con, "SELECT count(*) FROM orbital_solutions WHERE transform_version='e7_selected_stellar_orbit_projection_v1'") - scalar(con, "SELECT count(*) FROM selected_stellar_orbit_solutions s JOIN stellar_orbit_relation_bindings b USING(relation_id) WHERE b.simulation_eligible")),
                "stellar_relation_period_solution_delta": abs(scalar(con, "SELECT count(*) FROM orbital_solutions WHERE transform_version='e7_source_relation_period_projection_v1'") - scalar(con, "SELECT count(*) FROM stellar_orbit_relation_bindings WHERE simulation_eligible AND preferred_simulation_solution_id IS NULL AND source_period_days>0")),
                "stellar_preferred_projection_delta": abs(scalar(con, "SELECT count(*) FROM orbit_edges WHERE transform_version='e7_selected_stellar_orbit_projection_v1' AND preferred_solution_id IS NOT NULL") - scalar(con, "SELECT count(*) FROM stellar_orbit_relation_bindings WHERE simulation_eligible")),
                "unresolved_stellar_runtime_edges": scalar(con, "SELECT count(*) FROM orbit_edges e JOIN stellar_orbit_relation_bindings b ON b.relation_evidence_id=e.source_pk WHERE NOT b.simulation_eligible"),
                "source_group_canonical_containment_promotions": scalar(con, "SELECT count(*) FROM stellar_orbit_group_memberships WHERE canonical_containment"),
                "nonphysical_stellar_preferences": scalar(con, "SELECT count(*) FROM orbit_edges e JOIN orbital_solutions s ON s.orbital_solution_id=e.preferred_solution_id WHERE e.transform_version='e7_selected_stellar_orbit_projection_v1' AND (s.period_days<=0 OR s.semi_major_axis_arcsec<=0 OR s.eccentricity<0 OR s.eccentricity>=1)"),
                "tess_candidate_or_negative_planet_links": scalar(con, "SELECT count(*) FROM toi_current_evidence WHERE disposition IN ('PC','APC','FP','FA') AND planet_id IS NOT NULL"),
                "tess_confirmed_link_delta": abs(scalar(con, "SELECT count(*) FROM toi_current_evidence WHERE disposition IN ('CP','KP') AND planet_id IS NOT NULL") - 824),
                "tess_target_partition_delta": abs(counts.get("tess_target_identity", -1) - 27930),
                "toi_inventory_delta": abs(counts.get("toi_current_evidence", -1) - 8064),
                "toi_history_delta": abs(counts.get("toi_disposition_history", -1) - 8064),
                "toi_history_orphans": scalar(con, "SELECT count(*) FROM toi_disposition_history h LEFT JOIN toi_current_evidence t USING(source_key) WHERE t.source_key IS NULL"),
                "orphan_orbit_hosts": scalar(con, "SELECT count(*) FROM orbit_edges e LEFT JOIN component_entities c ON c.stable_component_key=e.host_component_key WHERE c.component_entity_id IS NULL"),
                "orphan_orbit_primaries": scalar(con, "SELECT count(*) FROM orbit_edges e LEFT JOIN component_entities c ON c.stable_component_key=e.primary_component_key WHERE c.component_entity_id IS NULL"),
                "orphan_orbit_secondaries": scalar(con, "SELECT count(*) FROM orbit_edges e LEFT JOIN component_entities c ON c.stable_component_key=e.secondary_component_key WHERE c.component_entity_id IS NULL"),
                "orphan_orbital_solutions": scalar(con, "SELECT count(*) FROM orbital_solutions s LEFT JOIN orbit_edges e USING(orbit_edge_id) WHERE e.orbit_edge_id IS NULL"),
                "periodic_hyperbolic_solutions": scalar(con, "SELECT count(*) FROM selected_solar_orbital_solutions WHERE render_mode='hyperbolic_trajectory' AND periodic_renderable"),
                "small_body_count_delta": abs(counts.get("sol_small_body_objects", -1) - 35),
                "artificial_count_delta": abs(counts.get("sol_artificial_objects", -1) - 11),
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
        "schema_version": "spacegate.e7_clean_runtime_arm_verification.v6",
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
