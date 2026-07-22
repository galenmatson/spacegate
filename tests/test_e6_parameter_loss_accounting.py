from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_e6_parameter_loss_accounting as auditor  # noqa: E402


def make_inputs(
    root: Path, *, composite_rank: int = 25, current_evidence_value: float = 0.8
) -> dict[str, Path]:
    candidate = root / "candidate.duckdb"
    reference = root / "reference.duckdb"
    selected = root / "selected.duckdb"
    evidence = root / "evidence.duckdb"
    policy = root / "policy.json"

    con = duckdb.connect(str(candidate))
    con.execute(
        "CREATE TABLE e6_selected_stellar_parameters("
        "star_id BIGINT,stable_object_key VARCHAR,teff_k DOUBLE,mass_msun DOUBLE,"
        "radius_rsun DOUBLE,luminosity_log10_lsun DOUBLE)"
    )
    con.execute("INSERT INTO e6_selected_stellar_parameters VALUES (1,'star:1',5000,NULL,1,0)")
    con.close()

    con = duckdb.connect(str(reference))
    con.execute(
        "CREATE TABLE stellar_parameters("
        "stellar_parameter_id BIGINT,star_id BIGINT,stable_object_key VARCHAR,"
        "source_catalog VARCHAR,parameter_source VARCHAR,teff_k DOUBLE,"
        "mass_msun DOUBLE,radius_rsun DOUBLE,luminosity_log10_lsun DOUBLE)"
    )
    con.execute(
        "INSERT INTO stellar_parameters VALUES "
        "(1,1,'star:1','nasa_exoplanet_archive','nasa_pscomppars_host',5000,0.8,1,0)"
    )
    con.close()

    con = duckdb.connect(str(selected))
    con.execute(
        "CREATE TABLE parameter_set_selection_decisions("
        "decision_id VARCHAR,object_type VARCHAR,stable_object_key VARCHAR,"
        "quantity_group VARCHAR,selected_source_id VARCHAR,"
        "selected_source_record_id VARCHAR,selected_parameter_set_id VARCHAR,"
        "authority_rank INTEGER)"
    )
    con.execute(
        "INSERT INTO parameter_set_selection_decisions VALUES "
        "('d','star','star:1','stellar_fundamental',?, 'selected-record','selected-set',8)",
        [auditor.NASA_SOURCE_ID],
    )
    con.execute(
        "CREATE TABLE selected_facts("
        "selected_fact_id VARCHAR,object_type VARCHAR,stable_object_key VARCHAR,"
        "quantity_key VARCHAR)"
    )
    con.execute(
        "CREATE TABLE evidence_object_bindings("
        "stable_object_key VARCHAR,source_id VARCHAR,object_type VARCHAR,"
        "binding_scope VARCHAR,binding_status VARCHAR,source_record_id VARCHAR)"
    )
    con.execute(
        "INSERT INTO evidence_object_bindings VALUES "
        "('star:1',?,'star','host','accepted','composite-record')",
        [auditor.NASA_SOURCE_ID],
    )
    con.close()

    con = duckdb.connect(str(evidence))
    con.execute(
        "CREATE TABLE source_records("
        "source_record_id VARCHAR,source_table VARCHAR,source_context_json JSON)"
    )
    con.execute("INSERT INTO source_records VALUES ('composite-record','nasa_pscomppars_v2','{}')")
    con.execute(
        "CREATE TABLE stellar_parameter_sets("
        "parameter_set_id VARCHAR,source_record_id VARCHAR)"
    )
    con.execute("INSERT INTO stellar_parameter_sets VALUES ('composite-set','composite-record')")
    con.execute(
        "CREATE TABLE stellar_parameter_evidence("
        "evidence_id VARCHAR,parameter_set_id VARCHAR,quantity_key VARCHAR,"
        "normalized_value DOUBLE)"
    )
    con.execute(
        "INSERT INTO stellar_parameter_evidence VALUES "
        "('e','composite-set','nasa_exoplanet_archive.st_mass',?)",
        [current_evidence_value],
    )
    con.close()

    policy.write_text(
        json.dumps(
            {
                "policy_version": "test",
                "selection_sources": [
                    {
                        "source_id": auditor.NASA_SOURCE_ID,
                        "object_type": "star",
                        "binding_scope": "host",
                        "quantity_groups": [
                            {
                                "group_key": "stellar_atmosphere",
                                "quantities": {
                                    "nasa_exoplanet_archive.st_teff": "teff_k"
                                },
                                "authorities": [
                                    {"source_table": "nasa_pscomppars_v2", "rank": 35}
                                ],
                            },
                            {
                                "group_key": "stellar_fundamental",
                                "quantities": {
                                    "nasa_exoplanet_archive.st_mass": "mass_msun",
                                    "nasa_exoplanet_archive.st_rad": "radius_rsun",
                                    "nasa_exoplanet_archive.st_lum": "luminosity_log10_lsun",
                                },
                                "authorities": [
                                    {
                                        "source_table": "nasa_pscomppars_v2",
                                        "rank": composite_rank,
                                    }
                                ],
                            },
                        ],
                    }
                ],
            }
        )
    )
    return {
        "candidate_arm": candidate,
        "reference_arm": reference,
        "selected_facts": selected,
        "nasa_evidence": evidence,
        "policy_path": policy,
    }


def test_accounts_lower_authority_coherent_alternative(tmp_path: Path) -> None:
    report = auditor.audit(**make_inputs(tmp_path), build_id="candidate")
    assert report["status"] == "pass"
    assert report["total_legacy_parameter_losses"] == 1
    assert report["by_quantity"]["mass_msun"]["losses"] == 1
    assert report["by_quantity"]["mass_msun"]["exact_current_release_value_matches"] == 1


def test_accounts_superseded_source_release_value(tmp_path: Path) -> None:
    report = auditor.audit(
        **make_inputs(tmp_path, current_evidence_value=0.9), build_id="candidate"
    )
    assert report["status"] == "pass"
    assert report["observations"]["superseded_source_release_values"] == 1
    assert report["failing_checks"] == {}


def test_rejects_higher_authority_unselected_alternative(tmp_path: Path) -> None:
    report = auditor.audit(
        **make_inputs(tmp_path, composite_rank=5), build_id="candidate"
    )
    assert report["status"] == "fail"
    assert report["checks"]["higher_authority_alternative_rows"] == 1
