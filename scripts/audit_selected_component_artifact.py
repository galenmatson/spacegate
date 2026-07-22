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
        "bad_manifest_schema": int(manifest.get("schema_version") != "spacegate.e5_selected_components.v9"),
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
        "duplicate_accepted_msc_source_component_keys": "SELECT count(*)-count(DISTINCT source_component_key) FROM msc_component_entities WHERE binding_status='accepted'",
        "duplicate_msc_relation_projection_ids": "SELECT count(*)-count(DISTINCT projected_relation_id) FROM msc_relation_evidence_projection",
        "duplicate_debcat_system_binding_ids": "SELECT count(*)-count(DISTINCT system_binding_id) FROM debcat_system_bindings",
        "duplicate_debcat_relation_binding_ids": "SELECT count(*)-count(DISTINCT relation_binding_id) FROM debcat_relation_bindings",
        "duplicate_parameter_set_binding_ids": "SELECT count(*)-count(DISTINCT parameter_set_binding_id) FROM debcat_parameter_set_bindings",
        "accepted_components_without_targets": "SELECT count(*) FROM msc_component_entities WHERE binding_status='accepted' AND (source_component_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "unaccepted_components_with_targets": "SELECT count(*) FROM msc_component_entities WHERE binding_status<>'accepted' AND source_component_key IS NOT NULL",
        "accepted_msc_relations_without_targets": "SELECT count(*) FROM msc_relation_evidence_projection WHERE projection_status='accepted_relation_evidence' AND (left_source_component_key IS NULL OR right_source_component_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "accepted_msc_self_relations": "SELECT count(*) FROM msc_relation_evidence_projection WHERE projection_status='accepted_relation_evidence' AND left_source_component_key=right_source_component_key",
        "invalid_self_relations_not_self": "SELECT count(*) FROM msc_relation_evidence_projection WHERE projection_status='invalid_self_relation_evidence' AND left_component_entity_id<>right_component_entity_id",
        "duplicate_msc_parameter_set_binding_ids": "SELECT count(*)-count(DISTINCT parameter_set_binding_id) FROM msc_component_parameter_set_bindings",
        "duplicate_msc_orbit_binding_ids": "SELECT count(*)-count(DISTINCT orbit_binding_id) FROM msc_orbit_solution_bindings",
        "accepted_msc_parameter_sets_without_targets": "SELECT count(*) FROM msc_component_parameter_set_bindings WHERE binding_status='accepted' AND (component_entity_id IS NULL OR target_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "unaccepted_msc_parameter_sets_with_targets": "SELECT count(*) FROM msc_component_parameter_set_bindings WHERE binding_status<>'accepted' AND target_key IS NOT NULL",
        "eligible_msc_parameters_without_targets": "SELECT count(*) FROM msc_stellar_parameter_projection WHERE projection_status='eligible_for_quantity_selection' AND target_key IS NULL",
        "selectable_msc_relative_separations": "SELECT count(*) FROM msc_stellar_parameter_projection WHERE quantity_key='separation_from_main_component' AND projection_status='eligible_for_quantity_selection'",
        "eligible_msc_classifications_without_targets": "SELECT count(*) FROM msc_classification_projection WHERE projection_status='eligible_for_quantity_selection' AND target_key IS NULL",
        "eligible_msc_photometry_without_targets": "SELECT count(*) FROM msc_photometry_projection WHERE projection_status='eligible_for_quantity_selection' AND target_key IS NULL",
        "eligible_msc_astrometry_without_targets": "SELECT count(*) FROM msc_astrometry_projection WHERE projection_status='eligible_for_quantity_selection' AND target_key IS NULL",
        "accepted_msc_orbits_without_targets": "SELECT count(*) FROM msc_orbit_solution_bindings WHERE binding_status='accepted' AND (msc_projected_relation_id IS NULL OR primary_source_component_key IS NULL OR secondary_source_component_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "unaccepted_msc_orbits_with_targets": "SELECT count(*) FROM msc_orbit_solution_bindings WHERE binding_status<>'accepted' AND (primary_source_component_key IS NOT NULL OR secondary_source_component_key IS NOT NULL OR canonical_system_stable_object_key IS NOT NULL)",
        "eligible_msc_orbit_projections_without_bindings": "SELECT count(*) FROM msc_orbital_solution_projection WHERE projection_status='eligible_for_quantity_selection' AND orbit_binding_id IS NULL",
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
        "duplicate_orb6_relation_binding_ids": "SELECT count(*)-count(DISTINCT relation_binding_id) FROM orb6_relation_bindings",
        "accepted_orb6_relations_without_targets": "SELECT count(*) FROM orb6_relation_bindings WHERE binding_status='accepted' AND (wds_source_record_id IS NULL OR msc_projected_relation_id IS NULL OR primary_source_component_key IS NULL OR secondary_source_component_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "unaccepted_orb6_relations_with_targets": "SELECT count(*) FROM orb6_relation_bindings WHERE binding_status<>'accepted' AND (msc_projected_relation_id IS NOT NULL OR primary_source_component_key IS NOT NULL OR secondary_source_component_key IS NOT NULL OR canonical_system_stable_object_key IS NOT NULL)",
        "eligible_orb6_orbits_without_relation": "SELECT count(*) FROM orb6_orbital_solution_projection WHERE projection_status='eligible_for_quantity_selection' AND relation_binding_id IS NULL",
        "duplicate_sbx_system_binding_ids": "SELECT count(*)-count(DISTINCT system_binding_id) FROM sbx_system_bindings",
        "duplicate_sbx_component_entity_ids": "SELECT count(*)-count(DISTINCT component_entity_id) FROM sbx_component_entities",
        "duplicate_sbx_relation_projection_ids": "SELECT count(*)-count(DISTINCT projected_relation_id) FROM sbx_relation_evidence_projection",
        "accepted_sbx_components_without_targets": "SELECT count(*) FROM sbx_component_entities WHERE binding_status='accepted' AND (source_component_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "unaccepted_sbx_components_with_targets": "SELECT count(*) FROM sbx_component_entities WHERE binding_status<>'accepted' AND source_component_key IS NOT NULL",
        "accepted_sbx_binary_relations_without_targets": "SELECT count(*) FROM sbx_relation_evidence_projection WHERE projection_status='accepted_relation_evidence' AND (left_target_key IS NULL OR right_target_key IS NULL OR left_canonical_system_stable_object_key IS NULL OR right_canonical_system_stable_object_key IS NULL)",
        "unaccepted_sbx_relations_with_targets": "SELECT count(*) FROM sbx_relation_evidence_projection WHERE projection_status='unresolved_endpoint_evidence' AND (left_target_key IS NOT NULL OR right_target_key IS NOT NULL)",
        "duplicate_sbx_parameter_set_binding_ids": "SELECT count(*)-count(DISTINCT parameter_set_binding_id) FROM sbx_parameter_set_bindings",
        "eligible_sbx_parameters_without_target": "SELECT count(*) FROM sbx_stellar_parameter_projection WHERE projection_status='eligible_for_quantity_selection' AND target_key IS NULL",
        "eligible_sbx_classifications_without_target": "SELECT count(*) FROM sbx_classification_projection WHERE projection_status='eligible_for_quantity_selection' AND target_key IS NULL",
        "eligible_sbx_orbits_without_relation": "SELECT count(*) FROM sbx_orbital_solution_projection WHERE projection_status='eligible_for_quantity_selection' AND projected_relation_id IS NULL",
        "selectable_sbx_astrometry": "SELECT count(*) FROM sbx_astrometry_projection WHERE projection_status='eligible_for_quantity_selection'",
        "context_sbx_astrometry_without_system": "SELECT count(*) FROM sbx_astrometry_projection WHERE projection_status='context_only_evidence' AND target_key IS NULL",
        "duplicate_wds_relation_binding_ids": "SELECT count(*)-count(DISTINCT relation_binding_id) FROM wds_pair_relation_bindings",
        "accepted_wds_relations_without_targets": "SELECT count(*) FROM wds_pair_relation_bindings WHERE binding_status='accepted' AND (msc_projected_relation_id IS NULL OR primary_source_component_key IS NULL OR secondary_source_component_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "unaccepted_wds_relations_with_targets": "SELECT count(*) FROM wds_pair_relation_bindings WHERE binding_status<>'accepted' AND (msc_projected_relation_id IS NOT NULL OR primary_source_component_key IS NOT NULL OR secondary_source_component_key IS NOT NULL OR canonical_system_stable_object_key IS NOT NULL)",
        "accepted_wds_self_relations": "SELECT count(*) FROM wds_pair_relation_bindings WHERE binding_status='accepted' AND primary_source_component_key=secondary_source_component_key",
        "selectable_wds_classifications": "SELECT count(*) FROM wds_classification_projection WHERE projection_status='eligible_for_quantity_selection'",
        "selectable_wds_photometry": "SELECT count(*) FROM wds_photometry_projection WHERE projection_status='eligible_for_quantity_selection'",
        "selectable_wds_astrometry": "SELECT count(*) FROM wds_astrometry_projection WHERE projection_status='eligible_for_quantity_selection'",
        "context_wds_classifications_without_target": "SELECT count(*) FROM wds_classification_projection WHERE projection_status='context_only_evidence' AND target_key IS NULL",
        "context_wds_photometry_without_target": "SELECT count(*) FROM wds_photometry_projection WHERE projection_status='context_only_evidence' AND target_key IS NULL",
        "context_wds_astrometry_without_target": "SELECT count(*) FROM wds_astrometry_projection WHERE projection_status='context_only_evidence' AND target_key IS NULL",
        "duplicate_gaia_nss_solution_binding_ids": "SELECT count(*)-count(DISTINCT solution_binding_id) FROM gaia_nss_solution_bindings",
        "accepted_gaia_nss_solutions_without_targets": "SELECT count(*) FROM gaia_nss_solution_bindings WHERE binding_status='accepted' AND (canonical_stable_object_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "unaccepted_gaia_nss_solutions_with_targets": "SELECT count(*) FROM gaia_nss_solution_bindings WHERE binding_status<>'accepted' AND (canonical_stable_object_key IS NOT NULL OR canonical_system_stable_object_key IS NOT NULL)",
        "gaia_nss_solutions_not_requiring_adjudication": "SELECT count(*) FROM gaia_nss_solution_bindings WHERE relation_adjudication_required IS DISTINCT FROM true",
        "selectable_gaia_nss_solutions": "SELECT count(*) FROM gaia_nss_orbital_solution_projection WHERE projection_status='eligible_for_quantity_selection'",
        "context_gaia_nss_solutions_without_targets": "SELECT count(*) FROM gaia_nss_orbital_solution_projection WHERE projection_status='context_only_evidence' AND (target_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "gaia_nss_solutions_with_fabricated_relations": "SELECT count(*) FROM gaia_nss_orbital_solution_projection WHERE relation_claim_id IS NOT NULL",
        "duplicate_tess_eb_target_binding_ids": "SELECT count(*)-count(DISTINCT target_binding_id) FROM tess_eb_target_bindings",
        "accepted_tess_eb_targets_without_targets": "SELECT count(*) FROM tess_eb_target_bindings WHERE binding_status='accepted' AND (canonical_stable_object_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "unaccepted_tess_eb_targets_with_targets": "SELECT count(*) FROM tess_eb_target_bindings WHERE binding_status<>'accepted' AND (canonical_stable_object_key IS NOT NULL OR canonical_system_stable_object_key IS NOT NULL)",
        "tess_eb_targets_not_requiring_adjudication": "SELECT count(*) FROM tess_eb_target_bindings WHERE relation_adjudication_required IS DISTINCT FROM true",
        "selectable_tess_eb_variability": "SELECT count(*) FROM tess_eb_variability_projection WHERE projection_status='eligible_for_quantity_selection'",
        "selectable_tess_eb_parameters": "SELECT count(*) FROM tess_eb_stellar_parameter_projection WHERE projection_status='eligible_for_quantity_selection'",
        "selectable_tess_eb_photometry": "SELECT count(*) FROM tess_eb_photometry_projection WHERE projection_status='eligible_for_quantity_selection'",
        "selectable_tess_eb_astrometry": "SELECT count(*) FROM tess_eb_astrometry_projection WHERE projection_status='eligible_for_quantity_selection'",
        "selectable_tess_eb_orbits": "SELECT count(*) FROM tess_eb_orbital_solution_projection WHERE projection_status='eligible_for_quantity_selection'",
        "context_tess_eb_evidence_without_targets": "SELECT (SELECT count(*) FROM tess_eb_variability_projection WHERE projection_status='context_only_evidence' AND target_key IS NULL) + (SELECT count(*) FROM tess_eb_stellar_parameter_projection WHERE projection_status='context_only_evidence' AND target_key IS NULL) + (SELECT count(*) FROM tess_eb_photometry_projection WHERE projection_status='context_only_evidence' AND target_key IS NULL) + (SELECT count(*) FROM tess_eb_astrometry_projection WHERE projection_status='context_only_evidence' AND target_key IS NULL) + (SELECT count(*) FROM tess_eb_orbital_solution_projection WHERE projection_status='context_only_evidence' AND target_key IS NULL)",
        "tess_eb_orbits_with_fabricated_relations": "SELECT count(*) FROM tess_eb_orbital_solution_projection WHERE relation_claim_id IS NOT NULL",
    }
    failures.update({name: int(con.execute(sql).fetchone()[0] or 0) for name, sql in checks.items()})

    def eligible(table: str) -> int:
        return int(con.execute(
            f"SELECT count(*) FROM {table} WHERE projection_status='eligible_for_quantity_selection'"
        ).fetchone()[0])

    msc_system = dict(con.execute("SELECT binding_status,count(*) FROM msc_system_bindings GROUP BY 1").fetchall())
    msc_identity = dict(con.execute("SELECT identity_graph_binding_status,count(*) FROM msc_system_bindings GROUP BY 1").fetchall())
    msc_components = dict(con.execute("SELECT binding_status,count(*) FROM msc_component_entities GROUP BY 1").fetchall())
    msc_relations = dict(con.execute("SELECT projection_status,count(*) FROM msc_relation_evidence_projection GROUP BY 1").fetchall())
    msc_orbits = dict(con.execute("SELECT binding_status,count(*) FROM msc_orbit_solution_bindings GROUP BY 1").fetchall())
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
        "parameter_sets": int(con.execute("SELECT count(*) FROM msc_component_parameter_set_bindings").fetchone()[0]),
        "parameter_sets_bound": int(con.execute("SELECT count(*) FROM msc_component_parameter_set_bindings WHERE binding_status='accepted'").fetchone()[0]),
        "parameter_evidence": int(con.execute("SELECT count(*) FROM msc_stellar_parameter_projection").fetchone()[0]),
        "parameter_evidence_eligible": eligible("msc_stellar_parameter_projection"),
        "parameter_evidence_context_only": int(con.execute("SELECT count(*) FROM msc_stellar_parameter_projection WHERE projection_status='context_only_evidence'").fetchone()[0]),
        "classification_evidence": int(con.execute("SELECT count(*) FROM msc_classification_projection").fetchone()[0]),
        "classification_evidence_eligible": eligible("msc_classification_projection"),
        "photometry_evidence": int(con.execute("SELECT count(*) FROM msc_photometry_projection").fetchone()[0]),
        "photometry_evidence_eligible": eligible("msc_photometry_projection"),
        "astrometry_evidence": int(con.execute("SELECT count(*) FROM msc_astrometry_projection").fetchone()[0]),
        "astrometry_evidence_eligible": eligible("msc_astrometry_projection"),
        "orbital_solutions": int(con.execute("SELECT count(*) FROM msc_orbital_solution_projection").fetchone()[0]),
        "orbital_solutions_eligible": eligible("msc_orbital_solution_projection"),
        "orbits_unresolved_msc_relation": msc_orbits.get("unresolved_msc_relation", 0),
        "orbits_invalid_msc_relation": msc_orbits.get("invalid_msc_relation", 0),
        "orbits_missing_msc_relation": msc_orbits.get("missing_msc_relation", 0),
        "orbits_ambiguous_msc_relation": msc_orbits.get("ambiguous_msc_relation", 0),
        "orbits_unparsed_pair": msc_orbits.get("unparsed_pair", 0),
        "orbits_missing_pair_identity": msc_orbits.get("missing_pair_identity", 0),
    }
    msc_expected = {key.removeprefix("expected_"): int(value) for key, value in policy["msc"]["acceptance"].items()}
    failures["msc_acceptance_mismatch"] = int(msc_observed != msc_expected)

    deb_system = dict(con.execute("SELECT binding_status,count(*) FROM debcat_system_bindings GROUP BY 1").fetchall())
    deb_relations = dict(con.execute("SELECT binding_status,count(*) FROM debcat_relation_bindings GROUP BY 1").fetchall())

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

    orb6_relations = dict(con.execute("SELECT binding_status,count(*) FROM orb6_relation_bindings GROUP BY 1").fetchall())
    orb6_observed = {
        "relation_bindings": sum(orb6_relations.values()),
        "relations_accepted": orb6_relations.get("accepted", 0),
        "relations_missing_wds_pair": orb6_relations.get("missing_wds_pair", 0),
        "relations_ambiguous_wds_pair": orb6_relations.get("ambiguous_wds_pair", 0),
        "relations_unparsed_wds_pair": orb6_relations.get("unparsed_wds_pair", 0),
        "relations_missing_msc_relation": orb6_relations.get("missing_msc_relation", 0),
        "relations_ambiguous_msc_relation": orb6_relations.get("ambiguous_msc_relation", 0),
        "orbital_solutions": int(con.execute("SELECT count(*) FROM orb6_orbital_solution_projection").fetchone()[0]),
        "orbital_solutions_eligible": eligible("orb6_orbital_solution_projection"),
    }
    orb6_expected = {key.removeprefix("expected_"): int(value) for key, value in policy["orb6"]["acceptance"].items()}
    failures["orb6_acceptance_mismatch"] = int(orb6_observed != orb6_expected)

    sbx_systems = dict(con.execute("SELECT binding_status,count(*) FROM sbx_system_bindings GROUP BY 1").fetchall())
    sbx_components = dict(con.execute("SELECT binding_status,count(*) FROM sbx_component_entities GROUP BY 1").fetchall())
    sbx_relations = dict(con.execute(
        "SELECT relation_kind || ':' || projection_status,count(*) "
        "FROM sbx_relation_evidence_projection GROUP BY 1"
    ).fetchall())
    sbx_observed = {
        "system_bindings": sum(sbx_systems.values()),
        "systems_accepted": sbx_systems.get("accepted", 0),
        "systems_missing": sbx_systems.get("missing", 0),
        "systems_ambiguous": sbx_systems.get("ambiguous", 0),
        "component_entities": sum(sbx_components.values()),
        "components_accepted": sbx_components.get("accepted", 0),
        "components_missing": sbx_components.get("missing", 0),
        "components_ambiguous": sbx_components.get("ambiguous", 0),
        "binary_relations": sum(value for key, value in sbx_relations.items() if key.startswith("spectroscopic_binary:")),
        "binary_relations_accepted": sbx_relations.get("spectroscopic_binary:accepted_relation_evidence", 0),
        "binary_relations_unresolved": sbx_relations.get("spectroscopic_binary:unresolved_endpoint_evidence", 0),
        "hierarchy_relations": sum(value for key, value in sbx_relations.items() if key.startswith("hierarchical_parent:")),
        "hierarchy_relations_accepted": sbx_relations.get("hierarchical_parent:accepted_source_hierarchy_evidence", 0),
        "hierarchy_relations_unresolved": sbx_relations.get("hierarchical_parent:unresolved_endpoint_evidence", 0),
        "parameter_sets": int(con.execute("SELECT count(*) FROM sbx_parameter_set_bindings").fetchone()[0]),
        "parameter_sets_eligible": int(con.execute("SELECT count(*) FROM sbx_parameter_set_bindings WHERE binding_status='accepted'").fetchone()[0]),
        "parameter_evidence": int(con.execute("SELECT count(*) FROM sbx_stellar_parameter_projection").fetchone()[0]),
        "parameter_evidence_eligible": eligible("sbx_stellar_parameter_projection"),
        "classification_evidence": int(con.execute("SELECT count(*) FROM sbx_classification_projection").fetchone()[0]),
        "classification_evidence_eligible": eligible("sbx_classification_projection"),
        "orbital_solutions": int(con.execute("SELECT count(*) FROM sbx_orbital_solution_projection").fetchone()[0]),
        "orbital_solutions_eligible": eligible("sbx_orbital_solution_projection"),
        "astrometry_evidence": int(con.execute("SELECT count(*) FROM sbx_astrometry_projection").fetchone()[0]),
        "astrometry_context_only": int(con.execute("SELECT count(*) FROM sbx_astrometry_projection WHERE projection_status='context_only_evidence'").fetchone()[0]),
    }
    sbx_expected = {key.removeprefix("expected_"): int(value) for key, value in policy["sbx"]["acceptance"].items()}
    failures["sbx_acceptance_mismatch"] = int(sbx_observed != sbx_expected)

    wds_relations = dict(con.execute(
        "SELECT binding_status,count(*) FROM wds_pair_relation_bindings GROUP BY 1"
    ).fetchall())
    wds_observed = {
        "relation_bindings": sum(wds_relations.values()),
        "relations_accepted": wds_relations.get("accepted", 0),
        "relations_missing_msc": wds_relations.get("missing_msc_relation", 0),
        "relations_ambiguous_msc": wds_relations.get("ambiguous_msc_relation", 0),
        "relations_unparsed": wds_relations.get("unparsed_pair", 0),
        "classification_evidence": int(con.execute("SELECT count(*) FROM wds_classification_projection").fetchone()[0]),
        "classification_context_only": int(con.execute("SELECT count(*) FROM wds_classification_projection WHERE projection_status='context_only_evidence'").fetchone()[0]),
        "photometry_evidence": int(con.execute("SELECT count(*) FROM wds_photometry_projection").fetchone()[0]),
        "photometry_context_only": int(con.execute("SELECT count(*) FROM wds_photometry_projection WHERE projection_status='context_only_evidence'").fetchone()[0]),
        "astrometry_evidence": int(con.execute("SELECT count(*) FROM wds_astrometry_projection").fetchone()[0]),
        "astrometry_context_only": int(con.execute("SELECT count(*) FROM wds_astrometry_projection WHERE projection_status='context_only_evidence'").fetchone()[0]),
    }
    wds_expected = {key.removeprefix("expected_"): int(value) for key, value in policy["wds"]["acceptance"].items()}
    failures["wds_acceptance_mismatch"] = int(wds_observed != wds_expected)

    nss_bindings = dict(con.execute(
        "SELECT binding_status,count(*) FROM gaia_nss_solution_bindings GROUP BY 1"
    ).fetchall())
    nss_observed = {
        "solution_bindings": sum(nss_bindings.values()),
        "solutions_accepted": nss_bindings.get("accepted", 0),
        "solutions_missing_canonical": nss_bindings.get("missing_canonical_source", 0),
        "solutions_ambiguous_canonical": nss_bindings.get("ambiguous_canonical_source", 0),
        "solutions_missing_source_identifier": nss_bindings.get("missing_source_identifier", 0),
        "solutions_ambiguous_source_identifier": nss_bindings.get("ambiguous_source_identifier", 0),
        "canonical_sources_accepted": int(con.execute("SELECT count(DISTINCT gaia_dr3_source_id) FROM gaia_nss_solution_bindings WHERE binding_status='accepted'").fetchone()[0]),
        "canonical_sources_missing": int(con.execute("SELECT count(DISTINCT gaia_dr3_source_id) FROM gaia_nss_solution_bindings WHERE binding_status='missing_canonical_source'").fetchone()[0]),
        "orbital_solutions": int(con.execute("SELECT count(*) FROM gaia_nss_orbital_solution_projection").fetchone()[0]),
        "orbital_solutions_context_only": int(con.execute("SELECT count(*) FROM gaia_nss_orbital_solution_projection WHERE projection_status='context_only_evidence'").fetchone()[0]),
        "orbital_solutions_selectable": int(con.execute("SELECT count(*) FROM gaia_nss_orbital_solution_projection WHERE projection_status='eligible_for_quantity_selection'").fetchone()[0]),
        "solutions_without_relation_claim": int(con.execute("SELECT count(*) FROM gaia_nss_orbital_solution_projection WHERE relation_claim_id IS NULL").fetchone()[0]),
    }
    nss_expected = {key.removeprefix("expected_"): int(value) for key, value in policy["gaia_nss"]["acceptance"].items()}
    failures["gaia_nss_acceptance_mismatch"] = int(nss_observed != nss_expected)

    tess_bindings = dict(con.execute(
        "SELECT binding_status,count(*) FROM tess_eb_target_bindings GROUP BY 1"
    ).fetchall())
    tess_membership = dict(con.execute(
        "SELECT evidence_polarity || ':' || binding_status,count(*) "
        "FROM tess_eb_target_bindings GROUP BY 1"
    ).fetchall())
    context = lambda table: int(con.execute(
        f"SELECT count(*) FROM {table} WHERE projection_status='context_only_evidence'"
    ).fetchone()[0])
    tess_observed = {
        "target_bindings": sum(tess_bindings.values()),
        "targets_accepted": tess_bindings.get("accepted", 0),
        "targets_missing_canonical": tess_bindings.get("missing_canonical_target", 0),
        "targets_ambiguous_canonical": tess_bindings.get("ambiguous_canonical_target", 0),
        "targets_missing_source_identifier": tess_bindings.get("missing_source_identifier", 0),
        "targets_ambiguous_source_identifier": tess_bindings.get("ambiguous_source_identifier", 0),
        "positive_targets_accepted": tess_membership.get("positive:accepted", 0),
        "positive_targets_unresolved": sum(value for key, value in tess_membership.items() if key.startswith("positive:") and key != "positive:accepted"),
        "negative_targets_accepted": tess_membership.get("negative:accepted", 0),
        "negative_targets_unresolved": sum(value for key, value in tess_membership.items() if key.startswith("negative:") and key != "negative:accepted"),
        "variability_evidence": int(con.execute("SELECT count(*) FROM tess_eb_variability_projection").fetchone()[0]),
        "variability_context_only": context("tess_eb_variability_projection"),
        "parameter_sets": int(con.execute("SELECT count(*) FROM tess_eb_parameter_set_bindings").fetchone()[0]),
        "parameter_evidence": int(con.execute("SELECT count(*) FROM tess_eb_stellar_parameter_projection").fetchone()[0]),
        "parameter_evidence_context_only": context("tess_eb_stellar_parameter_projection"),
        "photometry_evidence": int(con.execute("SELECT count(*) FROM tess_eb_photometry_projection").fetchone()[0]),
        "photometry_context_only": context("tess_eb_photometry_projection"),
        "astrometry_evidence": int(con.execute("SELECT count(*) FROM tess_eb_astrometry_projection").fetchone()[0]),
        "astrometry_context_only": context("tess_eb_astrometry_projection"),
        "orbital_solutions": int(con.execute("SELECT count(*) FROM tess_eb_orbital_solution_projection").fetchone()[0]),
        "orbital_solutions_context_only": context("tess_eb_orbital_solution_projection"),
        "orbital_solutions_selectable": int(con.execute("SELECT count(*) FROM tess_eb_orbital_solution_projection WHERE projection_status='eligible_for_quantity_selection'").fetchone()[0]),
        "solutions_without_relation_claim": int(con.execute("SELECT count(*) FROM tess_eb_orbital_solution_projection WHERE relation_claim_id IS NULL").fetchone()[0]),
    }
    tess_expected = {key.removeprefix("expected_"): int(value) for key, value in policy["tess_eb"]["acceptance"].items()}
    failures["tess_eb_acceptance_mismatch"] = int(tess_observed != tess_expected)
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
            {"source_id": policy["orb6"]["source_id"], "observed": orb6_observed, "expected": orb6_expected},
            {"source_id": policy["sbx"]["source_id"], "observed": sbx_observed, "expected": sbx_expected},
            {"source_id": policy["wds"]["source_id"], "observed": wds_observed, "expected": wds_expected},
            {"source_id": policy["gaia_nss"]["source_id"], "observed": nss_observed, "expected": nss_expected},
            {"source_id": policy["tess_eb"]["source_id"], "observed": tess_observed, "expected": tess_expected},
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
