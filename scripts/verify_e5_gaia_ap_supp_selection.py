#!/usr/bin/env python3
"""Verify focused E5 selection for Gaia DR3 supplementary AP evidence."""

from __future__ import annotations

import argparse
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

import duckdb

import compile_selected_facts as compiler


SOURCE_ID = "gaia.dr3.astrophysical_parameters_supp"
MAIN_SOURCE_ID = "gaia.dr3.astrophysical_parameters"
CURRENT_COMPETITOR_GROUPS = {"stellar_atmosphere", "stellar_fundamental"}
MAIN_ADDED_GROUPS = {
    "stellar_alpha_abundance",
    "stellar_projected_rotation",
    "stellar_gravitational_redshift",
}
DEFAULT_REPORT = Path(
    "/data/spacegate/state/reports/evidence_lake_v2/"
    "e5_gaia_ap_supp_selection_verification.json"
)
DEFAULT_SCRATCH = Path("/mnt/space/spacegate/e5-focused-ap-supp")


def selected_source(policy: dict[str, Any], source_id: str) -> dict[str, Any]:
    matches = [
        source
        for source in policy.get("selection_sources") or []
        if source.get("source_id") == source_id
    ]
    if len(matches) != 1:
        raise ValueError(f"expected one {source_id} policy, found {len(matches)}")
    source = dict(matches[0])
    source["_policy_version"] = str(policy["policy_version"])
    return source


def timed(
    timings: list[dict[str, Any]], phase: str, operation: Callable[[], Any]
) -> Any:
    started = time.monotonic()
    result = operation()
    timings.append(
        {"phase": phase, "wall_seconds": round(time.monotonic() - started, 6)}
    )
    return result


def fingerprint(
    con: duckdb.DuckDBPyConnection, table: str, columns: list[str]
) -> dict[str, Any]:
    row = con.execute(
        f"SELECT COUNT(*),CAST(coalesce(bit_xor(hash({','.join(columns)})),0) "
        f"AS VARCHAR) FROM {table}"
    ).fetchone()
    return {"rows": int(row[0]), "xor_hash64": str(row[1])}


def create_focused_winners(con: duckdb.DuckDBPyConnection) -> None:
    groups = ",".join(
        compiler.sql_literal(value) for value in sorted(CURRENT_COMPETITOR_GROUPS)
    )
    con.execute(
        f"""
        CREATE TEMP TABLE focused_candidate_sets AS
        WITH new_sets AS (
          SELECT object_type,stable_object_key,system_stable_object_key,
                 quantity_group,parameter_set_id,source_record_id,source_id,
                 release_id,evidence_build_id,MIN(authority_rank)::INTEGER authority_rank,
                 MIN(authority_reason) authority_reason,
                 MAX(selection_quality_score) selection_quality_score,
                 COUNT(DISTINCT quantity_key)::INTEGER quantity_count,
                 COUNT(DISTINCT CASE WHEN uncertainty_lower IS NOT NULL
                   OR uncertainty_upper IS NOT NULL THEN quantity_key END)::INTEGER uncertainty_count,
                 COUNT(DISTINCT CASE WHEN NULLIF(TRIM(reference_raw),'') IS NOT NULL
                   THEN quantity_key END)::INTEGER reference_count,
                 'new_candidate'::VARCHAR candidate_origin
          FROM fact_candidates GROUP BY ALL
        ), current_winners AS (
          SELECT object_type,stable_object_key,system_stable_object_key,
                 quantity_group,selected_parameter_set_id parameter_set_id,
                 selected_source_record_id source_record_id,
                 selected_source_id source_id,selected_release_id release_id,
                 selected_evidence_build_id evidence_build_id,authority_rank,
                 authority_reason,selection_quality_score,
                 selected_quantity_count quantity_count,
                 selected_uncertainty_count uncertainty_count,
                 1::INTEGER reference_count,
                 'current_winner'::VARCHAR candidate_origin
          FROM current_e5.parameter_set_selection_decisions
          WHERE quantity_group IN ({groups})
        )
        SELECT * FROM new_sets UNION ALL SELECT * FROM current_winners;

        CREATE TEMP TABLE focused_ranked_sets AS
        SELECT *,ROW_NUMBER() OVER (
          PARTITION BY object_type,stable_object_key,quantity_group
          ORDER BY authority_rank,quantity_count DESC,uncertainty_count DESC,
                   reference_count DESC,selection_quality_score DESC NULLS LAST,
                   parameter_set_id
        ) selection_rank,
        LEAD(authority_rank) OVER (
          PARTITION BY object_type,stable_object_key,quantity_group
          ORDER BY authority_rank,quantity_count DESC,uncertainty_count DESC,
                   reference_count DESC,selection_quality_score DESC NULLS LAST,
                   parameter_set_id
        ) runner_up_authority_rank
        FROM focused_candidate_sets;

        CREATE TEMP TABLE focused_winners AS
        SELECT * FROM focused_ranked_sets WHERE selection_rank=1;
        """
    )


def compile_projection(
    *,
    state_dir: Path,
    scratch_root: Path,
    policy: dict[str, Any],
    release_manifest: dict[str, Any],
    pass_name: str,
    attestor: compiler.FileHashAttestor,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    members = compiler.member_by_source(release_manifest)
    supp_source = selected_source(policy, SOURCE_ID)
    main_source = selected_source(policy, MAIN_SOURCE_ID)
    supp_member = members[SOURCE_ID]
    main_member = members[MAIN_SOURCE_ID]
    timings: list[dict[str, Any]] = []
    timed(
        timings,
        "immutable_input_verification",
        lambda: compiler.verify_e4_member_inputs(
            state_dir=state_dir,
            sources=[supp_source, main_source],
            members=members,
            attestor=attestor,
            workers=2,
        ),
    )
    supp_artifact = state_dir / str(supp_member["artifact_path"])
    main_artifact = state_dir / str(main_member["artifact_path"])
    supp_db = supp_artifact / str(supp_member["database"])
    main_db = main_artifact / str(main_member["database"])
    current_db = (
        state_dir / "derived/evidence_lake_v2/selected_facts/current"
    ).resolve() / "selected_facts.duckdb"
    identity_db = (
        state_dir
        / "derived/evidence_lake_v2/identity"
        / str(policy["identity_graph_id"])
        / "identity_graph.duckdb"
    )
    if not current_db.is_file() or not identity_db.is_file():
        raise ValueError("focused AP verification input is missing")

    scratch_root.mkdir(parents=True, exist_ok=True)
    run_dir = Path(tempfile.mkdtemp(prefix=f"{pass_name}-", dir=scratch_root))
    spill = run_dir / "spill"
    spill.mkdir()
    database = run_dir / "focused_selection.duckdb"
    con = duckdb.connect(
        str(database),
        config={
            "memory_limit": "24GB",
            "threads": "8",
            "temp_directory": str(spill),
            "preserve_insertion_order": "false",
        },
    )
    try:
        timed(
            timings,
            "schema",
            lambda: (compiler.create_schema(con), compiler.create_candidate_table(con)),
        )
        timed(
            timings,
            "attach_inputs",
            lambda: (
                con.execute(
                    f"ATTACH {compiler.sql_literal(str(identity_db))} AS identity (READ_ONLY)"
                ),
                con.execute(
                    f"ATTACH {compiler.sql_literal(str(supp_db))} AS e4_supp (READ_ONLY)"
                ),
                con.execute(
                    f"ATTACH {compiler.sql_literal(str(main_db))} AS e4_main (READ_ONLY)"
                ),
                con.execute(
                    f"ATTACH {compiler.sql_literal(str(current_db))} AS current_e5 (READ_ONLY)"
                ),
            ),
        )
        release_id = str(supp_member["release_ids"][SOURCE_ID])
        eligible, accepted = timed(
            timings,
            "supplement_binding",
            lambda: compiler.create_binding(
                con,
                source=supp_source,
                source_alias="e4_supp",
                member=supp_member,
                release_id=release_id,
            ),
        )
        timed(
            timings,
            "reuse_main_accepted_bindings",
            lambda: con.execute(
                "INSERT INTO evidence_object_bindings SELECT * FROM "
                "current_e5.evidence_object_bindings WHERE source_id=? "
                "AND binding_status='accepted'",
                [MAIN_SOURCE_ID],
            ),
        )
        timed(
            timings,
            "supplement_candidates",
            lambda: compiler.insert_candidates(
                con,
                source=supp_source,
                source_alias="e4_supp",
                member=supp_member,
                release_id=release_id,
            ),
        )
        focused_main = dict(main_source)
        focused_main["quantity_groups"] = [
            group
            for group in main_source["quantity_groups"]
            if group["group_key"] in MAIN_ADDED_GROUPS
        ]
        timed(
            timings,
            "new_main_ap_candidates",
            lambda: compiler.insert_candidates(
                con,
                source=focused_main,
                source_alias="e4_main",
                member=main_member,
                release_id=str(main_member["release_ids"][MAIN_SOURCE_ID]),
            ),
        )
        timed(timings, "focused_selection", lambda: create_focused_winners(con))

        binding_outcomes = {
            str(status): int(count)
            for status, count in con.execute(
                "SELECT binding_status,COUNT(*) FROM evidence_object_bindings "
                "WHERE source_id=? GROUP BY 1 ORDER BY 1",
                [SOURCE_ID],
            ).fetchall()
        }
        candidate_facts = {
            f"{source_id}:{group}": int(count)
            for source_id, group, count in con.execute(
                "SELECT source_id,quantity_group,COUNT(*) FROM fact_candidates "
                "GROUP BY 1,2 ORDER BY 1,2"
            ).fetchall()
        }
        selected_candidate_facts = {
            f"{source_id}:{group}": int(count)
            for source_id, group, count in con.execute(
                "SELECT c.source_id,c.quantity_group,COUNT(*) FROM fact_candidates c "
                "JOIN focused_winners w ON w.object_type=c.object_type "
                "AND w.stable_object_key=c.stable_object_key "
                "AND w.quantity_group=c.quantity_group "
                "AND w.parameter_set_id=c.parameter_set_id "
                "WHERE w.candidate_origin='new_candidate' GROUP BY 1,2 ORDER BY 1,2"
            ).fetchall()
        }
        winner_sets = {
            f"{source_id}:{group}": int(count)
            for source_id, group, count in con.execute(
                "SELECT source_id,quantity_group,COUNT(*) FROM focused_winners "
                "GROUP BY 1,2 ORDER BY 1,2"
            ).fetchall()
        }
        checks = {
            "binding_subject_accounting": int(
                con.execute(
                    "SELECT COUNT(*) FROM evidence_object_bindings WHERE source_id=?",
                    [SOURCE_ID],
                ).fetchone()[0]
            )
            - eligible,
            "accepted_binding_accounting": binding_outcomes.get("accepted", 0)
            - accepted,
            "binding_outcome_drift": int(
                binding_outcomes != supp_source["expected_binding_outcomes"]
            ),
            "supplement_gspphot_candidates": int(
                con.execute(
                    "SELECT COUNT(*) FROM fact_candidates WHERE source_id=? "
                    "AND method='gaia_dr3_gspphot'",
                    [SOURCE_ID],
                ).fetchone()[0]
            ),
            "supplement_non_spectroscopic_flame_candidates": int(
                con.execute(
                    "SELECT COUNT(*) FROM fact_candidates WHERE source_id=? "
                    "AND method='gaia_dr3_flame' AND model<>'FLAME_spectroscopic'",
                    [SOURCE_ID],
                ).fetchone()[0]
            ),
            "ann_candidates_outside_official_best_quality": int(
                con.execute(
                    "SELECT COUNT(*) FROM fact_candidates c JOIN e4_supp.source_records r "
                    "USING(source_record_id) WHERE c.source_id=? "
                    "AND c.method='gaia_dr3_gspspec_ann' AND NOT (try_cast("
                    "json_extract_string(r.source_context_json,'$.flags_gspspec_ann') "
                    "AS UBIGINT)<10000)",
                    [SOURCE_ID],
                ).fetchone()[0]
            ),
            "lower_authority_winners": int(
                con.execute(
                    "SELECT COUNT(*) FROM focused_winners WHERE "
                    "runner_up_authority_rank<authority_rank"
                ).fetchone()[0]
            ),
            "supplement_selected_outside_configured_channels": int(
                con.execute(
                    "SELECT COUNT(*) FROM fact_candidates c JOIN focused_winners w "
                    "ON w.object_type=c.object_type AND w.stable_object_key=c.stable_object_key "
                    "AND w.quantity_group=c.quantity_group "
                    "AND w.parameter_set_id=c.parameter_set_id "
                    "WHERE c.source_id=? AND c.method NOT IN "
                    "('gaia_dr3_gspspec_ann','gaia_dr3_flame')",
                    [SOURCE_ID],
                ).fetchone()[0]
            ),
        }
        fingerprints = {
            "supplement_bindings": fingerprint(
                con,
                "(SELECT * FROM evidence_object_bindings WHERE source_id='"
                + SOURCE_ID
                + "')",
                ["binding_id", "binding_status", "coalesce(stable_object_key,'')"],
            ),
            "new_candidates": fingerprint(
                con,
                "fact_candidates",
                [
                    "source_id",
                    "stable_object_key",
                    "quantity_group",
                    "quantity_key",
                    "parameter_set_id",
                    "value_raw",
                ],
            ),
            "focused_winners": fingerprint(
                con,
                "focused_winners",
                [
                    "source_id",
                    "stable_object_key",
                    "quantity_group",
                    "parameter_set_id",
                    "authority_rank",
                ],
            ),
        }
        projection = {
            "source_id": SOURCE_ID,
            "release_id": release_id,
            "evidence_build_id": str(supp_member["build_id"]),
            "current_selected_fact_build_id": current_db.parent.name,
            "eligible_binding_subjects": eligible,
            "accepted_binding_subjects": accepted,
            "binding_outcomes": binding_outcomes,
            "candidate_facts": candidate_facts,
            "selected_candidate_facts": selected_candidate_facts,
            "winner_sets": winner_sets,
            "checks": checks,
            "fingerprints": fingerprints,
            "logical_content_sha256": compiler.stable_sha256(fingerprints),
        }
    finally:
        con.close()
        shutil.rmtree(run_dir, ignore_errors=True)
    return projection, timings


def verify(
    *, state_dir: Path, policy_path: Path, scratch_root: Path
) -> dict[str, Any]:
    policy = compiler.load_json(policy_path)
    _, release_manifest = compiler.release_set_paths(state_dir, policy)
    compiler.validate_policy(policy, release_manifest)
    attestor = compiler.FileHashAttestor()
    first, first_timings = compile_projection(
        state_dir=state_dir,
        scratch_root=scratch_root,
        policy=policy,
        release_manifest=release_manifest,
        pass_name="first",
        attestor=attestor,
    )
    second, second_timings = compile_projection(
        state_dir=state_dir,
        scratch_root=scratch_root,
        policy=policy,
        release_manifest=release_manifest,
        pass_name="second",
        attestor=attestor,
    )
    failures = {
        key: value
        for key, value in {
            "scientific_checks": any(first["checks"].values()),
            "deterministic_projection": first != second,
            "no_supplement_selected_facts": not any(
                key.startswith(SOURCE_ID + ":")
                for key in first["selected_candidate_facts"]
            ),
        }.items()
        if value
    }
    return {
        "schema_version": "spacegate.e5_gaia_ap_supp_selection_verification.v1",
        "status": "fail" if failures else "pass",
        "policy_version": policy["policy_version"],
        "identity_graph_id": policy["identity_graph_id"],
        "failures": failures,
        "projection": first,
        "timings": {"first_pass": first_timings, "second_pass": second_timings},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("/data/spacegate/state"))
    parser.add_argument("--policy", type=Path, default=compiler.DEFAULT_POLICY)
    parser.add_argument("--scratch-root", type=Path, default=DEFAULT_SCRATCH)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = verify(
        state_dir=args.state_dir.resolve(),
        policy_path=args.policy.resolve(),
        scratch_root=args.scratch_root.resolve(),
    )
    compiler.atomic_json(args.report, report)
    projection = report["projection"]
    print(
        f"E5 Gaia AP supplement {report['status']}: "
        f"eligible={projection['eligible_binding_subjects']} "
        f"accepted={projection['accepted_binding_subjects']} "
        f"candidate_facts={sum(projection['candidate_facts'].values())} "
        f"selected_new_facts={sum(projection['selected_candidate_facts'].values())}"
    )
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
