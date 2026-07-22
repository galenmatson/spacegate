#!/usr/bin/env python3
"""Independently audit an E5 supplemental planet and TESS evidence artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_planet_evidence_policies.json"
DEFAULT_STATE = Path("/data/spacegate/state")
EXPECTED_FILES = {
    "parquet/planet_source_bindings.parquet",
    "parquet/planet_lifecycle_projection.parquet",
    "parquet/planet_parameter_set_projection.parquet",
    "parquet/planet_parameter_projection.parquet",
    "parquet/planet_lifecycle_conflicts.parquet",
    "parquet/tess_target_bindings.parquet",
    "parquet/tess_candidate_projection.parquet",
    "parquet/tess_transit_projection.parquet",
    "parquet/tess_planet_parameter_projection.parquet",
}


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(canonical_json(value) + b"\n")
    os.replace(temporary, path)


def sql_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0] or 0)


def audit(*, artifact: Path, policy_path: Path) -> dict[str, Any]:
    manifest_path = artifact / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    checks: dict[str, int] = {
        "bad_manifest_schema": int(manifest.get("schema_version") != "spacegate.e5_selected_planet_evidence.v1"),
        "build_id_path_mismatch": int(manifest.get("build_id") != artifact.name),
        "policy_version_mismatch": int(manifest.get("policy_version") != policy.get("policy_version")),
        "policy_sha256_mismatch": int(manifest.get("policy_sha256") != hashlib.sha256(canonical_json(policy)).hexdigest()),
        "release_set_mismatch": int(manifest.get("evidence_release_set_id") != policy.get("evidence_release_set_id")),
        "identity_graph_mismatch": int(manifest.get("identity_graph_id") != policy.get("identity_graph_id")),
        "canonical_reference_mismatch": int(manifest.get("canonical_reference_build_id") != policy.get("canonical_reference_build_id")),
        "canonical_planet_count_changed": int(manifest.get("canonical_planet_count_before") != manifest.get("canonical_planet_count_after")),
    }
    deterministic = manifest.get("deterministic_files") or {}
    checks["missing_deterministic_files"] = len(EXPECTED_FILES - set(deterministic))
    checks["unexpected_deterministic_files"] = len(set(deterministic) - EXPECTED_FILES)
    hash_mismatches = 0
    for relative in EXPECTED_FILES:
        path = artifact / relative
        entry = deterministic.get(relative) or {}
        if not path.is_file() or path.stat().st_size != entry.get("bytes") or sha256_file(path) != entry.get("sha256"):
            hash_mismatches += 1
    checks["deterministic_file_hash_mismatches"] = hash_mismatches
    input_mismatches = 0
    for entry in (manifest.get("input_fingerprints") or {}).values():
        path = Path(str(entry.get("path") or ""))
        if not path.is_file() or path.stat().st_size != entry.get("bytes") or sha256_file(path) != entry.get("sha256"):
            input_mismatches += 1
    checks["input_fingerprint_mismatches"] = input_mismatches

    observed: dict[str, int] = {}
    if not any((checks["missing_deterministic_files"], checks["unexpected_deterministic_files"], hash_mismatches)):
        con = duckdb.connect()
        for relative in sorted(EXPECTED_FILES):
            name = Path(relative).stem
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet({sql_literal(artifact / relative)})")
        queries = {
            "duplicate_planet_bindings": "SELECT count(*)-count(DISTINCT binding_id) FROM planet_source_bindings",
            "duplicate_planet_source_objects": "SELECT count(*) FROM (SELECT source_id,source_object_key FROM planet_source_bindings GROUP BY 1,2 HAVING count(*)<>1)",
            "accepted_planets_without_one_candidate": "SELECT count(*) FROM planet_source_bindings WHERE binding_status='accepted' AND (canonical_candidate_count<>1 OR canonical_planet_key IS NULL)",
            "unaccepted_planets_with_canonical_keys": "SELECT count(*) FROM planet_source_bindings WHERE binding_status<>'accepted' AND canonical_planet_key IS NOT NULL",
            "lifecycle_without_binding_lineage": "SELECT count(*) FROM planet_lifecycle_projection WHERE binding_id IS NULL OR evidence_role IS NULL",
            "lifecycle_inventory_mutations": "SELECT count(*) FROM planet_lifecycle_projection WHERE canonical_inventory_mutation",
            "parameter_sets_without_binding_lineage": "SELECT count(*) FROM planet_parameter_set_projection WHERE binding_id IS NULL OR selection_status IS NULL",
            "parameter_facts_without_sets": "SELECT count(*) FROM planet_parameter_projection f WHERE NOT EXISTS (SELECT 1 FROM planet_parameter_set_projection s WHERE s.parameter_set_id=f.parameter_set_id AND s.source_id=f.source_id)",
            "hwc_measurement_authority_leak": "SELECT count(*) FROM planet_parameter_set_projection WHERE source_id='exoplanet_lifecycle.hwc' AND selection_status<>'evidence_only' AND selection_status<>'unresolved_object'",
            "supplemental_selected_winner_leak": "SELECT count(*) FROM planet_parameter_set_projection WHERE selection_status NOT IN ('evidence_only','fallback_candidate_pending_e6','unresolved_object')",
            "duplicate_tess_target_bindings": "SELECT count(*)-count(DISTINCT binding_id) FROM tess_target_bindings",
            "duplicate_tic_ids": "SELECT count(*) FROM (SELECT tic_id FROM tess_target_bindings GROUP BY 1 HAVING count(*)<>1)",
            "accepted_tess_without_one_candidate": "SELECT count(*) FROM tess_target_bindings WHERE binding_status='accepted' AND (canonical_candidate_count<>1 OR canonical_star_key IS NULL)",
            "unaccepted_tess_with_canonical_keys": "SELECT count(*) FROM tess_target_bindings WHERE binding_status<>'accepted' AND canonical_star_key IS NOT NULL",
            "candidate_without_host_outcome": "SELECT count(*) FROM tess_candidate_projection WHERE host_binding_id IS NULL OR host_binding_status IS NULL",
            "nonconfirmed_planet_link": "SELECT count(*) FROM tess_candidate_projection WHERE canonical_planet_key IS NOT NULL AND (disposition_normalized<>'CONFIRMED' OR evidence_polarity<>'positive' OR canonical_planet_candidate_count<>1)",
            "candidate_inventory_mutations": "SELECT count(*) FROM tess_candidate_projection WHERE canonical_inventory_mutation",
            "transit_without_candidate_lineage": "SELECT count(*) FROM tess_transit_projection WHERE toi_id IS NULL OR host_binding_status IS NULL",
            "tess_parameter_without_candidate_lineage": "SELECT count(*) FROM tess_planet_parameter_projection WHERE toi_id IS NULL OR host_binding_status IS NULL",
            "polarity_conflict_flag_mismatch": "SELECT count(*) FROM planet_lifecycle_conflicts WHERE has_polarity_conflict IS DISTINCT FROM (negative_count>0 AND positive_count+candidate_count>0)",
        }
        checks.update({name: scalar(con, query) for name, query in queries.items()})
        observed = {
            "canonical_inventory_mutations": scalar(con, "SELECT (SELECT count(*) FROM planet_lifecycle_projection WHERE canonical_inventory_mutation)+(SELECT count(*) FROM tess_candidate_projection WHERE canonical_inventory_mutation)"),
            "canonical_planets_reference": int(manifest.get("canonical_planet_count_before") or 0),
            "supplemental_planet_objects": scalar(con, "SELECT count(*) FROM planet_source_bindings"),
            "supplemental_planets_accepted": scalar(con, "SELECT count(*) FROM planet_source_bindings WHERE binding_status='accepted'"),
            "supplemental_planets_missing": scalar(con, "SELECT count(*) FROM planet_source_bindings WHERE binding_status='missing'"),
            "supplemental_planets_ambiguous": scalar(con, "SELECT count(*) FROM planet_source_bindings WHERE binding_status='ambiguous'"),
            "supplemental_lifecycle_rows": scalar(con, "SELECT count(*) FROM planet_lifecycle_projection"),
            "supplemental_parameter_sets": scalar(con, "SELECT count(*) FROM planet_parameter_set_projection"),
            "supplemental_parameter_facts": scalar(con, "SELECT count(*) FROM planet_parameter_projection"),
            "supplemental_conflict_rows": scalar(con, "SELECT count(*) FROM planet_lifecycle_conflicts WHERE has_polarity_conflict"),
            "tess_target_bindings": scalar(con, "SELECT count(*) FROM tess_target_bindings"),
            "tess_targets_accepted": scalar(con, "SELECT count(*) FROM tess_target_bindings WHERE binding_status='accepted'"),
            "tess_targets_missing": scalar(con, "SELECT count(*) FROM tess_target_bindings WHERE binding_status='missing'"),
            "tess_targets_excluded": scalar(con, "SELECT count(*) FROM tess_target_bindings WHERE binding_status='excluded'"),
            "tess_targets_ambiguous": scalar(con, "SELECT count(*) FROM tess_target_bindings WHERE binding_status='ambiguous'"),
            "tess_candidates": scalar(con, "SELECT count(*) FROM tess_candidate_projection"),
            "tess_confirmed": scalar(con, "SELECT count(*) FROM tess_candidate_projection WHERE disposition_normalized='CONFIRMED'"),
            "tess_confirmed_planet_links": scalar(con, "SELECT count(*) FROM tess_candidate_projection WHERE canonical_planet_key IS NOT NULL"),
            "tess_candidate_evidence": scalar(con, "SELECT count(*) FROM tess_candidate_projection WHERE evidence_polarity='candidate'"),
            "tess_negative_evidence": scalar(con, "SELECT count(*) FROM tess_candidate_projection WHERE evidence_polarity='negative'"),
            "tess_unclassified_evidence": scalar(con, "SELECT count(*) FROM tess_candidate_projection WHERE evidence_polarity IS NULL"),
            "tess_transit_facts": scalar(con, "SELECT count(*) FROM tess_transit_projection"),
            "tess_planet_parameter_facts": scalar(con, "SELECT count(*) FROM tess_planet_parameter_projection"),
        }
        for source, key in (
            ("exoplanet_lifecycle.exoplanet_eu", "eu"),
            ("exoplanet_lifecycle.hwc", "hwc"),
            ("exoplanet_lifecycle.open_exoplanet_catalogue", "oec"),
        ):
            for status in ("accepted", "missing", "ambiguous"):
                observed[f"{key}_planets_{status}"] = scalar(
                    con,
                    f"SELECT count(*) FROM planet_source_bindings WHERE source_id={sql_literal(source)} AND binding_status={sql_literal(status)}",
                )
        con.close()
    expected = {str(key): int(value) for key, value in (policy.get("acceptance") or {}).items()}
    checks["acceptance_mismatches"] = sum(int(observed.get(key) != value) for key, value in expected.items()) + len(set(observed) - set(expected))
    checks["manifest_observed_mismatch"] = int(manifest.get("observed") != observed)
    failing = {name: count for name, count in checks.items() if count}
    return {
        "schema_version": "spacegate.e5_selected_planet_evidence_artifact_audit.v1",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "artifact_path": str(artifact),
        "build_id": manifest.get("build_id"),
        "observed": observed,
        "expected": expected,
        "checks": checks,
        "failing_checks": failing,
        "status": "fail" if failing else "pass",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = audit(artifact=args.artifact, policy_path=args.policy)
    report_path = args.report or DEFAULT_STATE / "reports/evidence_lake_v2/e5_selected_planet_evidence_artifact_audit.json"
    write_json(report_path, report)
    print(f"Selected planet evidence artifact {report['status']}: build={report['build_id']}")
    if report["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
