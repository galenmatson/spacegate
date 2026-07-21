#!/usr/bin/env python3
"""Verify E5 white-dwarf applicability and coherent model selection."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

import compile_selected_facts as compiler


DEFAULT_REPORT = Path(
    "/data/spacegate/state/reports/evidence_lake_v2/"
    "e5_white_dwarf_selection_verification.json"
)
SOURCE_ID = "compact.gaia_edr3_white_dwarf"


def selected_source(policy: dict[str, Any]) -> dict[str, Any]:
    matches = [
        source
        for source in policy.get("selection_sources") or []
        if source.get("source_id") == SOURCE_ID
    ]
    if len(matches) != 1:
        raise ValueError(f"expected one {SOURCE_ID} policy, found {len(matches)}")
    source = dict(matches[0])
    source["_policy_version"] = str(policy["policy_version"])
    return source


def compile_projection(
    *, state_dir: Path, policy: dict[str, Any], release_manifest: dict[str, Any]
) -> dict[str, Any]:
    source = selected_source(policy)
    member = compiler.member_by_source(release_manifest)[SOURCE_ID]
    artifact = state_dir / str(member["artifact_path"])
    database = artifact / str(member["database"])
    manifest_path = artifact / "manifest.json"
    if compiler.file_sha256(manifest_path) != member["manifest_sha256"]:
        raise ValueError("white-dwarf E4 member manifest changed")
    if database.stat().st_size != int(member["database_bytes"]):
        raise ValueError("white-dwarf E4 database size changed")
    if compiler.file_sha256(database) != member["database_sha256"]:
        raise ValueError("white-dwarf E4 database checksum changed")

    identity_db = (
        state_dir / "derived/evidence_lake_v2/identity"
        / str(policy["identity_graph_id"]) / "identity_graph.duckdb"
    )
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET threads=1")
        compiler.create_schema(con)
        compiler.create_candidate_table(con)
        con.execute(
            f"ATTACH {compiler.sql_literal(str(identity_db))} AS identity (READ_ONLY)"
        )
        con.execute(
            f"ATTACH {compiler.sql_literal(str(database))} AS e4_source (READ_ONLY)"
        )
        release_id = str(member["release_ids"][SOURCE_ID])
        eligible, accepted = compiler.create_binding(
            con,
            source=source,
            source_alias="e4_source",
            member=member,
            release_id=release_id,
        )
        preselected = compiler.create_parameter_set_preselection(
            con,
            source=source,
            source_alias="e4_source",
            member=member,
            release_id=release_id,
        )
        compiler.insert_candidates(
            con,
            source=source,
            source_alias="e4_source",
            member=member,
            release_id=release_id,
        )
        compiler.select_parameter_sets(con, str(policy["policy_version"]))

        outcomes = {
            str(status): int(count)
            for status, count in con.execute(
                "SELECT binding_status,COUNT(*) FROM evidence_object_bindings "
                "GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        facts_by_quantity = {
            str(quantity): int(count)
            for quantity, count in con.execute(
                "SELECT quantity_key,COUNT(*) FROM selected_facts "
                "GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        winning_models = {
            str(model): int(count)
            for model, count in con.execute(
                "SELECT selected_model,COUNT(*) "
                "FROM source_parameter_set_preselections GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        checks = {
            "binding_subject_accounting": int(
                con.execute("SELECT COUNT(*) FROM evidence_object_bindings").fetchone()[0]
            ) - eligible,
            "accepted_binding_accounting": outcomes.get("accepted", 0) - accepted,
            "accepted_without_high_probability": int(
                con.execute(
                    "SELECT COUNT(*) FROM evidence_object_bindings b "
                    "JOIN e4_source.compact_object_evidence ce "
                    "ON ce.evidence_id=b.applicability_evidence_id "
                    "WHERE b.binding_status='accepted' AND NOT coalesce("
                    "CAST(json_extract_string(ce.parameter_set_raw,'$.Pwd') AS DOUBLE)>0.75,FALSE)"
                ).fetchone()[0]
            ),
            "excluded_high_probability": int(
                con.execute(
                    "SELECT COUNT(*) FROM evidence_object_bindings b "
                    "JOIN e4_source.compact_object_evidence ce "
                    "ON ce.evidence_id=b.applicability_evidence_id "
                    "WHERE b.binding_status='excluded' AND "
                    "CAST(json_extract_string(ce.parameter_set_raw,'$.Pwd') AS DOUBLE)>0.75"
                ).fetchone()[0]
            ),
            "bindings_without_applicability_lineage": int(
                con.execute(
                    "SELECT COUNT(*) FROM evidence_object_bindings "
                    "WHERE applicability_evidence_id IS NULL"
                ).fetchone()[0]
            ),
            "preselections_without_complete_model": int(
                con.execute(
                    "SELECT COUNT(*) FROM source_parameter_set_preselections "
                    "WHERE selected_completeness<>3 OR selected_order_value IS NULL"
                ).fetchone()[0]
            ),
            "preselections_not_minimum_published_chi_square": int(
                con.execute(
                    "WITH complete AS ("
                    "SELECT ps.source_record_id,ps.parameter_set_id,"
                    "COUNT(DISTINCT pe.quantity_key) FILTER(WHERE pe.quantity_key IN "
                    "('effective_temperature','log10_surface_gravity','mass') "
                    "AND pe.normalized_value IS NOT NULL) completeness,"
                    "MAX(pe.normalized_value) FILTER(WHERE pe.quantity_key='fit_chi_square') chisq "
                    "FROM e4_source.stellar_parameter_sets ps "
                    "JOIN e4_source.stellar_parameter_evidence pe "
                    "ON pe.parameter_set_id=ps.parameter_set_id GROUP BY 1,2), "
                    "minimums AS (SELECT source_record_id,MIN(chisq) minimum_chisq "
                    "FROM complete WHERE completeness=3 AND chisq IS NOT NULL GROUP BY 1) "
                    "SELECT COUNT(*) FROM source_parameter_set_preselections p "
                    "JOIN minimums m USING(source_record_id) "
                    "WHERE p.selected_order_value<>m.minimum_chisq"
                ).fetchone()[0]
            ),
            "selected_facts_outside_preselected_model": int(
                con.execute(
                    "SELECT COUNT(*) FROM selected_facts f WHERE NOT EXISTS ("
                    "SELECT 1 FROM source_parameter_set_preselections p "
                    "WHERE p.source_record_id=f.source_record_id "
                    "AND p.selected_parameter_set_id=f.parameter_set_id)"
                ).fetchone()[0]
            ),
            "selected_facts_without_applicability_lineage": int(
                con.execute(
                    "SELECT COUNT(*) FROM selected_facts f "
                    "JOIN source_parameter_set_preselections p "
                    "ON p.source_record_id=f.source_record_id "
                    "WHERE p.applicability_evidence_id IS NULL"
                ).fetchone()[0]
            ),
            "duplicate_selected_object_quantities": int(
                con.execute(
                    "SELECT COALESCE(SUM(n-1),0) FROM ("
                    "SELECT COUNT(*) n FROM selected_facts "
                    "GROUP BY stable_object_key,quantity_key HAVING COUNT(*)>1)"
                ).fetchone()[0]
            ),
        }
        selected_facts = int(
            con.execute("SELECT COUNT(*) FROM selected_facts").fetchone()[0]
        )
        logical_rows = {
            "bindings": con.execute(
                "SELECT binding_id,binding_status,coalesce(stable_object_key,''),"
                "applicability_evidence_id,binding_reason "
                "FROM evidence_object_bindings ORDER BY binding_id"
            ).fetchall(),
            "preselections": con.execute(
                "SELECT preselection_id,source_record_id,selected_parameter_set_id,"
                "selected_model,selected_order_value,runner_up_parameter_set_id,"
                "applicability_evidence_id FROM source_parameter_set_preselections "
                "ORDER BY preselection_id"
            ).fetchall(),
            "facts": con.execute(
                "SELECT selected_fact_id,stable_object_key,quantity_key,normalized_value,"
                "parameter_set_id,evidence_id FROM selected_facts ORDER BY selected_fact_id"
            ).fetchall(),
        }
    finally:
        con.close()

    return {
        "source_id": SOURCE_ID,
        "release_id": str(member["release_ids"][SOURCE_ID]),
        "evidence_build_id": str(member["build_id"]),
        "eligible_binding_subjects": eligible,
        "accepted_binding_subjects": accepted,
        "binding_outcomes": outcomes,
        "preselected_parameter_sets": preselected,
        "selected_facts": selected_facts,
        "facts_by_quantity": facts_by_quantity,
        "winning_models": winning_models,
        "checks": checks,
        "logical_content_sha256": compiler.stable_sha256(logical_rows),
    }


def verify(state_dir: Path, policy_path: Path) -> dict[str, Any]:
    policy = compiler.load_json(policy_path)
    _, release_manifest = compiler.release_set_paths(state_dir, policy)
    compiler.validate_policy(policy, release_manifest)
    source = selected_source(policy)
    first = compile_projection(
        state_dir=state_dir, policy=policy, release_manifest=release_manifest
    )
    second = compile_projection(
        state_dir=state_dir, policy=policy, release_manifest=release_manifest
    )
    expected_preselected = int(
        source["parameter_set_preselection"]["expected_selected_parameter_sets"]
    )
    expected_selected = int(source["expected_selected_facts"])
    failures = {
        key: value
        for key, value in {
            "binding_outcomes": first["binding_outcomes"]
            != source["expected_binding_outcomes"],
            "preselection_count": first["preselected_parameter_sets"]
            != expected_preselected,
            "selected_fact_count": first["selected_facts"] != expected_selected,
            "facts_per_model": first["selected_facts"]
            != first["preselected_parameter_sets"] * 3,
            "deterministic_projection": first != second,
            "scientific_checks": any(first["checks"].values()),
        }.items()
        if value
    }
    return {
        "schema_version": "spacegate.e5_white_dwarf_selection_verification.v1",
        "status": "fail" if failures else "pass",
        "policy_version": policy["policy_version"],
        "identity_graph_id": policy["identity_graph_id"],
        "expected_binding_outcomes": source["expected_binding_outcomes"],
        "expected_preselected_parameter_sets": expected_preselected,
        "expected_selected_facts": expected_selected,
        "failures": failures,
        "projection": first,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("/data/spacegate/state"))
    parser.add_argument("--policy", type=Path, default=compiler.DEFAULT_POLICY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = verify(args.state_dir.resolve(), args.policy.resolve())
    compiler.atomic_json(args.report, report)
    projection = report["projection"]
    print(
        f"E5 white-dwarf selection {report['status']}: "
        f"eligible={projection['eligible_binding_subjects']} "
        f"accepted={projection['accepted_binding_subjects']} "
        f"models={projection['preselected_parameter_sets']} "
        f"selected={projection['selected_facts']}"
    )
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
