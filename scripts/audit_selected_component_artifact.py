#!/usr/bin/env python3
"""Independently audit an E5 selected-component evidence artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_component_scope_policies.json"


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
        "bad_manifest_schema": int(manifest.get("schema_version") != "spacegate.e5_selected_components.v2"),
        "build_id_path_mismatch": int(manifest.get("build_id") != artifact.name),
        "policy_sha256_mismatch": int(manifest.get("policy_sha256") != policy_sha),
        "identity_graph_mismatch": int(manifest.get("identity_graph_id") != policy.get("identity_graph_id")),
        "canonical_reference_mismatch": int(
            manifest.get("canonical_reference_build_id") != policy.get("canonical_reference_build_id")
        ),
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

    con = duckdb.connect(str(artifact / "selected_components.duckdb"), read_only=True)
    checks = {
        "duplicate_msc_system_binding_ids": "SELECT count(*)-count(DISTINCT system_binding_id) FROM msc_system_bindings",
        "duplicate_msc_component_entity_ids": "SELECT count(*)-count(DISTINCT component_entity_id) FROM msc_component_entities",
        "duplicate_msc_relation_projection_ids": "SELECT count(*)-count(DISTINCT projected_relation_id) FROM msc_relation_evidence_projection",
        "duplicate_debcat_system_binding_ids": "SELECT count(*)-count(DISTINCT system_binding_id) FROM debcat_system_bindings",
        "duplicate_debcat_relation_binding_ids": "SELECT count(*)-count(DISTINCT relation_binding_id) FROM debcat_relation_bindings",
        "duplicate_parameter_set_binding_ids": "SELECT count(*)-count(DISTINCT parameter_set_binding_id) FROM debcat_parameter_set_bindings",
        "accepted_components_without_targets": "SELECT count(*) FROM msc_component_entities WHERE binding_status='accepted' AND (source_component_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "unaccepted_components_with_targets": "SELECT count(*) FROM msc_component_entities WHERE binding_status<>'accepted' AND source_component_key IS NOT NULL",
        "accepted_msc_relations_without_targets": "SELECT count(*) FROM msc_relation_evidence_projection WHERE projection_status='accepted_relation_evidence' AND (left_source_component_key IS NULL OR right_source_component_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "accepted_msc_self_relations": "SELECT count(*) FROM msc_relation_evidence_projection WHERE projection_status='accepted_relation_evidence' AND left_source_component_key=right_source_component_key",
        "invalid_self_relations_not_self": "SELECT count(*) FROM msc_relation_evidence_projection WHERE projection_status='invalid_self_relation_evidence' AND left_component_entity_id<>right_component_entity_id",
        "accepted_debcat_relations_without_targets": "SELECT count(*) FROM debcat_relation_bindings WHERE binding_status='accepted' AND (primary_source_component_key IS NULL OR secondary_source_component_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "unaccepted_debcat_relations_with_targets": "SELECT count(*) FROM debcat_relation_bindings WHERE binding_status<>'accepted' AND (primary_source_component_key IS NOT NULL OR secondary_source_component_key IS NOT NULL)",
        "eligible_parameter_rows_without_target": "SELECT count(*) FROM debcat_stellar_parameter_projection WHERE projection_status='eligible_for_quantity_selection' AND (evidence_id IS NULL OR parameter_set_id IS NULL OR target_key IS NULL)",
        "eligible_classification_rows_without_target": "SELECT count(*) FROM debcat_classification_projection WHERE projection_status='eligible_for_quantity_selection' AND target_key IS NULL",
        "eligible_photometry_rows_without_system": "SELECT count(*) FROM debcat_photometry_projection WHERE projection_status='eligible_for_quantity_selection' AND target_key IS NULL",
        "eligible_orbit_rows_without_relation": "SELECT count(*) FROM debcat_orbital_solution_projection WHERE projection_status='eligible_for_quantity_selection' AND relation_binding_id IS NULL",
        "duplicate_sb9_relation_binding_ids": "SELECT count(*)-count(DISTINCT relation_binding_id) FROM sb9_relation_bindings",
        "duplicate_sb9_parameter_set_binding_ids": "SELECT count(*)-count(DISTINCT parameter_set_binding_id) FROM sb9_parameter_set_bindings",
        "accepted_sb9_relations_without_targets": "SELECT count(*) FROM sb9_relation_bindings WHERE binding_status='accepted' AND (primary_source_component_key IS NULL OR secondary_source_component_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "unaccepted_sb9_relations_with_targets": "SELECT count(*) FROM sb9_relation_bindings WHERE binding_status<>'accepted' AND (primary_source_component_key IS NOT NULL OR secondary_source_component_key IS NOT NULL)",
        "eligible_sb9_parameters_without_target": "SELECT count(*) FROM sb9_stellar_parameter_projection WHERE projection_status='eligible_for_quantity_selection' AND target_key IS NULL",
        "eligible_sb9_classifications_without_target": "SELECT count(*) FROM sb9_classification_projection WHERE projection_status='eligible_for_quantity_selection' AND target_key IS NULL",
        "eligible_sb9_orbits_without_relation": "SELECT count(*) FROM sb9_orbital_solution_projection WHERE projection_status='eligible_for_quantity_selection' AND relation_binding_id IS NULL",
    }
    failures.update({name: int(con.execute(sql).fetchone()[0] or 0) for name, sql in checks.items()})

    msc_system = dict(con.execute("SELECT binding_status,count(*) FROM msc_system_bindings GROUP BY 1").fetchall())
    msc_identity = dict(con.execute("SELECT identity_graph_binding_status,count(*) FROM msc_system_bindings GROUP BY 1").fetchall())
    msc_components = dict(con.execute("SELECT binding_status,count(*) FROM msc_component_entities GROUP BY 1").fetchall())
    msc_relations = dict(con.execute("SELECT projection_status,count(*) FROM msc_relation_evidence_projection GROUP BY 1").fetchall())
    msc_observed = {
        "system_bindings": sum(msc_system.values()),
        "systems_accepted": msc_system.get("accepted", 0),
        "systems_missing": msc_system.get("missing", 0),
        "systems_ambiguous": msc_system.get("ambiguous", 0),
        "identity_graph_systems_accepted": msc_identity.get("accepted", 0),
        "identity_graph_systems_missing": msc_identity.get("missing", 0),
        "identity_graph_systems_ambiguous": msc_identity.get("ambiguous", 0),
        "component_entities": sum(msc_components.values()),
        "components_accepted": msc_components.get("accepted", 0),
        "components_missing": msc_components.get("missing", 0),
        "components_ambiguous": msc_components.get("ambiguous", 0),
        "relation_claims": sum(msc_relations.values()),
        "relations_accepted": msc_relations.get("accepted_relation_evidence", 0),
        "relations_unresolved": msc_relations.get("unresolved_endpoint_evidence", 0),
        "relations_invalid_self": msc_relations.get("invalid_self_relation_evidence", 0),
    }
    msc_expected = {key.removeprefix("expected_"): int(value) for key, value in policy["msc"]["acceptance"].items()}
    failures["msc_acceptance_mismatch"] = int(msc_observed != msc_expected)

    deb_system = dict(con.execute("SELECT binding_status,count(*) FROM debcat_system_bindings GROUP BY 1").fetchall())
    deb_relations = dict(con.execute("SELECT binding_status,count(*) FROM debcat_relation_bindings GROUP BY 1").fetchall())

    def eligible(table: str) -> int:
        return int(con.execute(
            f"SELECT count(*) FROM {table} WHERE projection_status='eligible_for_quantity_selection'"
        ).fetchone()[0])

    deb_observed = {
        "system_bindings": sum(deb_system.values()),
        "systems_accepted": deb_system.get("accepted", 0),
        "systems_missing": deb_system.get("missing", 0),
        "systems_ambiguous": deb_system.get("ambiguous", 0),
        "relation_bindings": sum(deb_relations.values()),
        "relations_accepted": deb_relations.get("accepted", 0),
        "relations_missing_system": deb_relations.get("missing_system", 0),
        "relations_no_period_match": deb_relations.get("no_period_match", 0),
        "relations_ambiguous": deb_relations.get("ambiguous_system", 0) + deb_relations.get("ambiguous_period_match", 0),
        "parameter_sets": int(con.execute("SELECT count(*) FROM debcat_parameter_set_bindings").fetchone()[0]),
        "parameter_sets_eligible": int(con.execute("SELECT count(*) FROM debcat_parameter_set_bindings WHERE binding_status='accepted'").fetchone()[0]),
        "parameter_evidence": int(con.execute("SELECT count(*) FROM debcat_stellar_parameter_projection").fetchone()[0]),
        "parameter_evidence_eligible": eligible("debcat_stellar_parameter_projection"),
        "classification_evidence": int(con.execute("SELECT count(*) FROM debcat_classification_projection").fetchone()[0]),
        "classification_evidence_eligible": eligible("debcat_classification_projection"),
        "photometry_evidence": int(con.execute("SELECT count(*) FROM debcat_photometry_projection").fetchone()[0]),
        "photometry_evidence_eligible": eligible("debcat_photometry_projection"),
        "orbital_solutions": int(con.execute("SELECT count(*) FROM debcat_orbital_solution_projection").fetchone()[0]),
        "orbital_solutions_eligible": eligible("debcat_orbital_solution_projection"),
    }
    deb_expected = {key.removeprefix("expected_"): int(value) for key, value in policy["debcat"]["acceptance"].items()}
    failures["debcat_acceptance_mismatch"] = int(deb_observed != deb_expected)

    sb9_relations = dict(con.execute("SELECT binding_status,count(*) FROM sb9_relation_bindings GROUP BY 1").fetchall())
    sb9_observed = {
        "relation_bindings": sum(sb9_relations.values()),
        "relations_accepted": sb9_relations.get("accepted", 0),
        "relations_missing_reference": sb9_relations.get("missing_reference", 0),
        "relations_ambiguous_reference": sb9_relations.get("ambiguous_reference", 0),
        "relations_unresolved_msc": sb9_relations.get("unresolved_msc_relation", 0),
        "parameter_sets": int(con.execute("SELECT count(*) FROM sb9_parameter_set_bindings").fetchone()[0]),
        "parameter_sets_eligible": int(con.execute("SELECT count(*) FROM sb9_parameter_set_bindings WHERE binding_status='accepted'").fetchone()[0]),
        "parameter_evidence": int(con.execute("SELECT count(*) FROM sb9_stellar_parameter_projection").fetchone()[0]),
        "parameter_evidence_eligible": eligible("sb9_stellar_parameter_projection"),
        "classification_evidence": int(con.execute("SELECT count(*) FROM sb9_classification_projection").fetchone()[0]),
        "classification_evidence_eligible": eligible("sb9_classification_projection"),
        "orbital_solutions": int(con.execute("SELECT count(*) FROM sb9_orbital_solution_projection").fetchone()[0]),
        "orbital_solutions_eligible": eligible("sb9_orbital_solution_projection"),
    }
    sb9_expected = {key.removeprefix("expected_"): int(value) for key, value in policy["sb9"]["acceptance"].items()}
    failures["sb9_acceptance_mismatch"] = int(sb9_observed != sb9_expected)
    con.close()
    failing = {name: count for name, count in failures.items() if count}
    return {
        "schema_version": "spacegate.e5_selected_component_artifact_audit.v1",
        "artifact_path": str(artifact),
        "build_id": manifest.get("build_id"),
        "source_reports": [
            {"source_id": policy["msc"]["source_id"], "observed": msc_observed, "expected": msc_expected},
            {"source_id": policy["debcat"]["source_id"], "observed": deb_observed, "expected": deb_expected},
            {"source_id": policy["sb9"]["source_id"], "observed": sb9_observed, "expected": sb9_expected},
        ],
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
    print(f"Selected component artifact {report['status']}: build={report['build_id']}")
    if report["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
