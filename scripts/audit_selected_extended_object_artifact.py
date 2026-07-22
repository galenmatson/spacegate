#!/usr/bin/env python3
"""Independently audit an E5 selected extended-object artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_extended_object_policies.json"


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
        "bad_manifest_schema": int(manifest.get("schema_version") != "spacegate.e5_selected_extended_objects.v1"),
        "build_id_path_mismatch": int(manifest.get("build_id") != artifact.name),
        "policy_sha256_mismatch": int(manifest.get("policy_sha256") != policy_sha),
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

    con = duckdb.connect(str(artifact / "selected_extended_objects.duckdb"), read_only=True)
    checks = {
        "duplicate_binding_ids": "SELECT count(*)-count(DISTINCT binding_id) FROM extended_object_bindings",
        "duplicate_source_evidence_bindings": "SELECT count(*) FROM (SELECT source_id,evidence_id FROM extended_object_bindings GROUP BY 1,2 HAVING count(*)<>1)",
        "accepted_bindings_without_targets": "SELECT count(*) FROM extended_object_bindings WHERE binding_status='accepted' AND (canonical_extended_object_id IS NULL OR canonical_stable_object_key IS NULL)",
        "unaccepted_bindings_with_targets": "SELECT count(*) FROM extended_object_bindings WHERE binding_status<>'accepted' AND (canonical_extended_object_id IS NOT NULL OR canonical_stable_object_key IS NOT NULL)",
        "accepted_bindings_without_one_candidate": "SELECT count(*) FROM extended_object_bindings WHERE binding_status='accepted' AND canonical_candidate_count<>1",
        "source_rows_without_keys": "SELECT count(*) FROM extended_object_bindings WHERE source_record_key IS NULL OR source_record_key=''",
        "eligible_unbound_evidence": "SELECT count(*) FROM extended_object_evidence_projection WHERE projection_status='eligible_for_extended_quantity_selection' AND canonical_stable_object_key IS NULL",
        "stellar_fact_rows": "SELECT count(*) FROM extended_object_evidence_projection WHERE stellar_fact_projection",
    }
    failures.update({name: int(con.execute(sql).fetchone()[0] or 0) for name, sql in checks.items()})
    configured = {source["source_id"]: source for source in policy["sources"]}
    actual = {row[0] for row in con.execute("SELECT DISTINCT source_id FROM extended_object_bindings").fetchall()}
    failures["unexpected_sources"] = len(actual - set(configured))
    failures["missing_sources"] = len(set(configured) - actual)
    source_reports: list[dict[str, Any]] = []
    for source_id in sorted(actual & set(configured)):
        source = configured[source_id]
        bindings = dict(con.execute(
            "SELECT binding_status,count(*) FROM extended_object_bindings WHERE source_id=? GROUP BY 1",
            [source_id],
        ).fetchall())
        observed = {
            "bindings": sum(bindings.values()),
            "bindings_accepted": bindings.get("accepted", 0),
            "bindings_excluded": bindings.get("excluded", 0),
            "bindings_quarantined": bindings.get("quarantined", 0),
            "bindings_unresolved": bindings.get("unresolved", 0),
            "evidence": int(con.execute("SELECT count(*) FROM extended_object_evidence_projection WHERE source_id=?", [source_id]).fetchone()[0]),
            "evidence_eligible": int(con.execute("SELECT count(*) FROM extended_object_evidence_projection WHERE source_id=? AND projection_status='eligible_for_extended_quantity_selection'", [source_id]).fetchone()[0]),
            "canonical_candidate_ambiguities": int(con.execute("SELECT count(*) FROM extended_object_bindings WHERE source_id=? AND canonical_candidate_count>1", [source_id]).fetchone()[0]),
            "stellar_fact_rows": int(con.execute("SELECT count(*) FROM extended_object_evidence_projection WHERE source_id=? AND stellar_fact_projection", [source_id]).fetchone()[0]),
        }
        expected = {key.removeprefix("expected_"): int(value) for key, value in source["acceptance"].items()}
        failures[f"acceptance_mismatch:{source_id}"] = int(observed != expected)
        source_reports.append({"source_id": source_id, "observed": observed, "expected": expected})
    con.close()
    failing = {name: count for name, count in failures.items() if count}
    return {
        "schema_version": "spacegate.e5_selected_extended_object_artifact_audit.v1",
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
    print(f"Selected extended-object artifact {report['status']}: build={report['build_id']}")
    if report["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
