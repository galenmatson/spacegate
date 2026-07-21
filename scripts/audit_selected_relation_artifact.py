#!/usr/bin/env python3
"""Independently audit an E5 selected-relation evidence artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_relation_policies.json"


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
    database = artifact / "selected_relations.duckdb"
    failures: dict[str, int] = {}
    failures["bad_manifest_schema"] = int(manifest.get("schema_version") != "spacegate.e5_selected_relations.v1")
    failures["build_id_path_mismatch"] = int(manifest.get("build_id") != artifact.name)
    failures["policy_sha256_mismatch"] = int(manifest.get("policy_sha256") != policy_sha)
    failures["missing_compiler_lineage"] = int(
        not manifest.get("compiler_version") or not manifest.get("compiler_sha256")
    )
    hash_failures = 0
    missing_files = 0
    for name, metadata in manifest.get("deterministic_files", {}).items():
        path = artifact / name
        if not path.is_file():
            missing_files += 1
        elif sha256_file(path) != metadata.get("sha256") or path.stat().st_size != metadata.get("bytes"):
            hash_failures += 1
    failures["missing_deterministic_files"] = missing_files
    failures["deterministic_file_hash_mismatches"] = hash_failures

    con = duckdb.connect(str(database), read_only=True)
    checks = {
        "duplicate_endpoint_binding_ids": "SELECT COUNT(*)-COUNT(DISTINCT endpoint_binding_id) FROM relation_endpoint_bindings",
        "duplicate_projected_relation_ids": "SELECT COUNT(*)-COUNT(DISTINCT projected_relation_id) FROM relation_evidence_projection",
        "relations_without_two_endpoints": "SELECT COUNT(*) FROM (SELECT source_id,relation_evidence_id,COUNT(*) n FROM relation_endpoint_bindings GROUP BY 1,2 HAVING n<>2)",
        "accepted_endpoints_without_targets": "SELECT COUNT(*) FROM relation_endpoint_bindings WHERE binding_status='accepted' AND (stable_object_key IS NULL OR canonical_object_node_key IS NULL)",
        "unaccepted_endpoints_with_targets": "SELECT COUNT(*) FROM relation_endpoint_bindings WHERE binding_status<>'accepted' AND (stable_object_key IS NOT NULL OR canonical_object_node_key IS NOT NULL)",
        "fabricated_probabilities": "SELECT COUNT(*) FROM relation_evidence_projection WHERE probability IS NOT NULL",
        "high_confidence_wrong_polarity": "SELECT COUNT(*) FROM relation_evidence_projection WHERE projection_status='high_confidence_relation_evidence' AND evidence_polarity<>'candidate'",
        "high_confidence_above_threshold": "SELECT COUNT(*) FROM relation_evidence_projection WHERE projection_status='high_confidence_relation_evidence' AND (confidence_statistic_value IS NULL OR confidence_statistic_value>=high_confidence_threshold)",
        "negative_control_promotions": "SELECT COUNT(*) FROM relation_evidence_projection WHERE evidence_polarity='negative_control' AND projection_status NOT IN ('negative_control_evidence','unresolved_endpoint_evidence')",
        "resolved_self_relations": "SELECT COUNT(*) FROM relation_evidence_projection WHERE left_stable_object_key IS NOT NULL AND left_stable_object_key=right_stable_object_key",
    }
    failures.update({name: int(con.execute(sql).fetchone()[0] or 0) for name, sql in checks.items()})
    source_reports: list[dict[str, Any]] = []
    configured = {row["source_id"]: row for row in policy["sources"]}
    actual_sources = {row[0] for row in con.execute("SELECT DISTINCT source_id FROM relation_evidence_projection").fetchall()}
    failures["unexpected_sources"] = len(actual_sources - set(configured))
    failures["missing_sources"] = len(set(configured) - actual_sources)
    for source_id in sorted(actual_sources & set(configured)):
        projection_counts = dict(con.execute("SELECT projection_status,COUNT(*) FROM relation_evidence_projection WHERE source_id=? GROUP BY 1 ORDER BY 1", [source_id]).fetchall())
        binding_counts = dict(con.execute("SELECT binding_status,COUNT(*) FROM relation_endpoint_bindings WHERE source_id=? GROUP BY 1 ORDER BY 1", [source_id]).fetchall())
        observed = {
            "relation_claims": int(sum(projection_counts.values())),
            "endpoint_bindings": int(sum(binding_counts.values())),
            "both_endpoints_accepted": int(con.execute("SELECT COUNT(*) FROM relation_evidence_projection WHERE source_id=? AND left_stable_object_key IS NOT NULL AND right_stable_object_key IS NOT NULL", [source_id]).fetchone()[0]),
            "high_confidence_relations": int(projection_counts.get("high_confidence_relation_evidence", 0)),
            "negative_controls_with_bound_endpoints": int(projection_counts.get("negative_control_evidence", 0)),
        }
        expected = {key.removeprefix("expected_"): int(value) for key, value in configured[source_id]["acceptance"].items()}
        failures[f"acceptance_mismatch:{source_id}"] = int(observed != expected)
        source_reports.append({"source_id": source_id, "observed": observed, "expected": expected, "binding_outcomes": binding_counts, "projection_outcomes": projection_counts})
    con.close()
    failing = {name: count for name, count in failures.items() if count}
    return {
        "schema_version": "spacegate.e5_selected_relation_artifact_audit.v1",
        "artifact_path": str(artifact),
        "build_id": manifest.get("build_id"),
        "source_reports": source_reports,
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
    print(f"Selected relation artifact {report['status']}: build={report['build_id']}")
    if report["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
