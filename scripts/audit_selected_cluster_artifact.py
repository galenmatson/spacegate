#!/usr/bin/env python3
"""Independently audit an E5 selected-cluster evidence artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_cluster_policies.json"


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def audit(*, artifact: Path, policy_path: Path) -> dict[str, Any]:
    manifest = read_json(artifact / "manifest.json")
    policy = read_json(policy_path)
    policy_sha = hashlib.sha256(
        json.dumps(policy, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    failures: dict[str, int] = {
        "bad_manifest_schema": int(manifest.get("schema_version") != "spacegate.e5_selected_clusters.v1"),
        "build_id_path_mismatch": int(manifest.get("build_id") != artifact.name),
        "policy_sha256_mismatch": int(manifest.get("policy_sha256") != policy_sha),
        "identity_graph_mismatch": int(manifest.get("identity_graph_id") != policy.get("identity_graph_id")),
        "canonical_reference_mismatch": int(manifest.get("canonical_reference_build_id") != policy.get("canonical_reference_build_id")),
        "missing_compiler_lineage": int(not manifest.get("compiler_version") or not manifest.get("compiler_sha256")),
    }
    missing_files = 0
    bad_hashes = 0
    for name, metadata in manifest.get("deterministic_files", {}).items():
        path = artifact / name
        if not path.is_file():
            missing_files += 1
        elif path.stat().st_size != metadata.get("bytes") or sha256_file(path) != metadata.get("sha256"):
            bad_hashes += 1
    failures["missing_deterministic_files"] = missing_files
    failures["deterministic_file_hash_mismatches"] = bad_hashes

    con = duckdb.connect(str(artifact / "selected_clusters.duckdb"), read_only=True)
    checks = {
        "duplicate_cluster_binding_ids": "SELECT count(*)-count(DISTINCT cluster_binding_id) FROM cluster_identity_bindings",
        "duplicate_membership_binding_ids": "SELECT count(*)-count(DISTINCT membership_binding_id) FROM cluster_membership_endpoint_bindings",
        "accepted_clusters_without_targets": "SELECT count(*) FROM cluster_identity_bindings WHERE binding_status='accepted' AND (canonical_cluster_id IS NULL OR canonical_cluster_stable_object_key IS NULL)",
        "unaccepted_clusters_with_targets": "SELECT count(*) FROM cluster_identity_bindings WHERE binding_status<>'accepted' AND canonical_cluster_stable_object_key IS NOT NULL",
        "accepted_cluster_target_collisions": "SELECT count(*) FROM (SELECT canonical_cluster_stable_object_key FROM cluster_identity_bindings WHERE binding_status='accepted' GROUP BY 1 HAVING count(*)<>1)",
        "eligible_noncharacterizations": "SELECT count(*) FROM cluster_evidence_projection WHERE projection_status='eligible_for_quantity_selection' AND source_table<>'hunt_reffert_2024_clusters'",
        "eligible_unbound_clusters": "SELECT count(*) FROM cluster_evidence_projection WHERE projection_status='eligible_for_quantity_selection' AND canonical_cluster_stable_object_key IS NULL",
        "accepted_members_without_targets": "SELECT count(*) FROM cluster_membership_endpoint_bindings WHERE member_binding_status='accepted' AND (member_stable_object_key IS NULL OR member_system_stable_object_key IS NULL)",
        "unaccepted_members_with_targets": "SELECT count(*) FROM cluster_membership_endpoint_bindings WHERE member_binding_status<>'accepted' AND (member_stable_object_key IS NOT NULL OR member_system_stable_object_key IS NOT NULL)",
        "membership_probabilities_outside_unit_interval": "SELECT count(*) FROM cluster_membership_projection WHERE membership_probability IS NULL OR membership_probability<0 OR membership_probability>1",
        "bound_memberships_without_two_targets": "SELECT count(*) FROM cluster_membership_projection WHERE projection_status='probability_bearing_membership_evidence' AND (canonical_cluster_stable_object_key IS NULL OR member_stable_object_key IS NULL)",
        "canonical_containment_rows": "SELECT count(*) FROM cluster_membership_projection WHERE canonical_containment_promotion",
    }
    failures.update({name: int(con.execute(sql).fetchone()[0] or 0) for name, sql in checks.items()})
    source = policy["sources"][0]
    cluster_counts = dict(con.execute("SELECT binding_status,count(*) FROM cluster_identity_bindings GROUP BY 1").fetchall())
    member_counts = dict(con.execute("SELECT member_binding_status,count(*) FROM cluster_membership_endpoint_bindings GROUP BY 1").fetchall())
    combinations = dict(con.execute("SELECT cluster_binding_status || ':' || member_binding_status,count(*) FROM cluster_membership_endpoint_bindings GROUP BY 1").fetchall())
    observed = {
        "cluster_bindings": sum(cluster_counts.values()),
        "clusters_accepted": cluster_counts.get("accepted", 0),
        "clusters_missing": cluster_counts.get("missing", 0),
        "clusters_ambiguous": cluster_counts.get("ambiguous", 0),
        "cluster_evidence": int(con.execute("SELECT count(*) FROM cluster_evidence_projection").fetchone()[0]),
        "cluster_characterizations": int(con.execute("SELECT count(*) FROM cluster_evidence_projection WHERE source_table='hunt_reffert_2024_clusters'").fetchone()[0]),
        "cluster_characterizations_eligible": int(con.execute("SELECT count(*) FROM cluster_evidence_projection WHERE projection_status='eligible_for_quantity_selection'").fetchone()[0]),
        "crossmatch_context": int(con.execute("SELECT count(*) FROM cluster_evidence_projection WHERE source_table='hunt_reffert_2024_crossmatch' AND projection_status='identity_context_evidence'").fetchone()[0]),
        "memberships": int(con.execute("SELECT count(*) FROM cluster_membership_projection").fetchone()[0]),
        "member_endpoints_accepted": member_counts.get("accepted", 0),
        "member_endpoints_missing": member_counts.get("missing", 0),
        "member_endpoints_ambiguous": member_counts.get("ambiguous", 0),
        "memberships_both_accepted": combinations.get("accepted:accepted", 0),
        "memberships_cluster_accepted_member_missing": combinations.get("accepted:missing", 0),
        "memberships_cluster_ambiguous_member_accepted": combinations.get("ambiguous:accepted", 0),
        "memberships_cluster_ambiguous_member_missing": combinations.get("ambiguous:missing", 0),
        "memberships_cluster_missing_member_accepted": combinations.get("missing:accepted", 0),
        "memberships_both_missing": combinations.get("missing:missing", 0),
        "canonical_containment_rows": int(con.execute("SELECT count(*) FROM cluster_membership_projection WHERE canonical_containment_promotion").fetchone()[0]),
    }
    expected = {key.removeprefix("expected_"): int(value) for key, value in source["acceptance"].items()}
    failures["acceptance_mismatch"] = int(observed != expected)
    con.close()
    failing = {name: count for name, count in failures.items() if count}
    return {
        "schema_version": "spacegate.e5_selected_cluster_artifact_audit.v1",
        "artifact_path": str(artifact),
        "build_id": manifest.get("build_id"),
        "source_reports": [{"source_id": source["source_id"], "observed": observed, "expected": expected}],
        "checks": failures,
        "failing_checks": failing,
        "status": "pass" if not failing else "fail",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = audit(artifact=args.artifact, policy_path=args.policy)
    write_json(args.report, report)
    print(f"Selected cluster artifact {report['status']}: build={report['build_id']}")
    if report["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
