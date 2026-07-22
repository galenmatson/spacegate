#!/usr/bin/env python3
"""Independently verify a clean E7 cluster evidence artifact."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_POLICY = Path(__file__).resolve().parents[1] / "config/evidence_lake/e7_clean_clusters.json"


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
    os.replace(temporary, path)


def scalar(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> int:
    return int(con.execute(sql, params or []).fetchone()[0] or 0)


def verify(artifact: Path, policy: dict[str, Any]) -> dict[str, Any]:
    manifest = read_json(artifact / "manifest.json")
    database = artifact / "clean_clusters.duckdb"
    con = duckdb.connect(str(database), read_only=True)
    checks = {
        "manifest_not_pass": int(manifest.get("status") != "pass"),
        "stability_database_opened": len(manifest.get("stability_databases_opened") or []),
        "containment_promotions": scalar(con, "SELECT count(*) FROM cluster_membership_projection WHERE canonical_containment_promotion"),
        "accepted_cluster_collision": scalar(con, "SELECT count(*) FROM (SELECT source_id,canonical_cluster_stable_object_key FROM cluster_identity_bindings WHERE binding_status='accepted' GROUP BY 1,2 HAVING count(*)<>1)"),
        "accepted_cluster_missing_target": scalar(con, "SELECT count(*) FROM cluster_identity_bindings WHERE binding_status='accepted' AND canonical_cluster_stable_object_key IS NULL"),
        "scope_conflict_with_target": scalar(con, "SELECT count(*) FROM cluster_identity_bindings WHERE binding_status='scope_conflict' AND canonical_cluster_stable_object_key IS NOT NULL"),
        "accepted_member_missing_target": scalar(con, "SELECT count(*) FROM cluster_membership_endpoint_bindings WHERE member_binding_status='accepted' AND (member_stable_object_key IS NULL OR member_system_stable_object_key IS NULL)"),
        "unaccepted_member_with_target": scalar(con, "SELECT count(*) FROM cluster_membership_endpoint_bindings WHERE member_binding_status<>'accepted' AND (member_stable_object_key IS NOT NULL OR member_system_stable_object_key IS NOT NULL)"),
        "invalid_membership_probability": scalar(con, "SELECT count(*) FROM cluster_membership_projection WHERE membership_probability IS NULL OR membership_probability<0 OR membership_probability>1"),
        "pleiades_fallback_not_accepted": abs(scalar(con, "SELECT count(*) FROM cluster_identity_bindings WHERE source_id='clusters.cantat_gaudin_2020' AND cluster_identity_raw='Melotte_22' AND binding_status='accepted' AND canonical_cluster_name='M 45'") - 1),
        "ambiguous_current_pleiades_claims": abs(scalar(con, "SELECT count(*) FROM cluster_identity_bindings WHERE source_id='clusters.hunt_reffert_2024' AND cluster_identity_raw IN ('2837','4423') AND binding_status='ambiguous'") - 2),
    }
    source_summaries: list[dict[str, Any]] = []
    for source in policy["sources"]:
        source_id = source["source_id"]
        cluster_counts = dict(con.execute(
            "SELECT binding_status,count(*) FROM cluster_identity_bindings WHERE source_id=? GROUP BY 1",
            [source_id],
        ).fetchall())
        member_counts = dict(con.execute(
            "SELECT member_binding_status,count(*) FROM cluster_membership_endpoint_bindings WHERE source_id=? GROUP BY 1",
            [source_id],
        ).fetchall())
        observed = {
            "cluster_bindings": sum(cluster_counts.values()),
            "clusters_accepted": cluster_counts.get("accepted", 0),
            "clusters_missing": cluster_counts.get("missing", 0),
            "clusters_ambiguous": cluster_counts.get("ambiguous", 0),
            "clusters_scope_conflict": cluster_counts.get("scope_conflict", 0),
            "cluster_evidence": scalar(con, "SELECT count(*) FROM cluster_evidence_projection WHERE source_id=?", [source_id]),
            "cluster_characterizations_eligible": scalar(con, "SELECT count(*) FROM cluster_evidence_projection WHERE source_id=? AND projection_status='eligible_for_quantity_selection'", [source_id]),
            "memberships": sum(member_counts.values()),
            "member_endpoints_accepted": member_counts.get("accepted", 0),
            "member_endpoints_missing": member_counts.get("missing", 0),
            "member_endpoints_ambiguous": member_counts.get("ambiguous", 0),
            "member_endpoints_excluded": member_counts.get("excluded", 0),
        }
        checks[f"{source_id}_acceptance_delta"] = sum(
            abs(observed[key] - int(expected))
            for key, expected in source["acceptance"].items()
        )
        source_summaries.append({"source_id": source_id, "observed": observed})
    con.close()
    failing = {name: count for name, count in checks.items() if count}
    return {
        "schema_version": "spacegate.e7_clean_clusters_verification.v1",
        "build_id": manifest["build_id"],
        "status": "pass" if not failing else "fail",
        "checks": checks,
        "failing_checks": failing,
        "source_summaries": source_summaries,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--report", type=Path, default=DEFAULT_STATE / "reports/evidence_lake_v2/e7_clean_clusters_verification.json")
    args = parser.parse_args()
    report = verify(args.artifact, read_json(args.policy))
    write_json(args.report, report)
    print(f"Clean cluster verification {report['status']}: {report['build_id']}")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
