#!/usr/bin/env python3
"""Account legacy E6 parameter losses through retained coherent alternatives."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import resource
import time
from typing import Any

import duckdb


NASA_SOURCE_ID = "nasa_exoplanet_archive.planetary_systems"
QUANTITIES = {
    "teff_k": "nasa_exoplanet_archive.st_teff",
    "mass_msun": "nasa_exoplanet_archive.st_mass",
    "radius_rsun": "nasa_exoplanet_archive.st_rad",
    "luminosity_log10_lsun": "nasa_exoplanet_archive.st_lum",
}


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def load_contract(policy_path: Path) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    programs = [
        row
        for row in policy.get("selection_sources") or []
        if row.get("source_id") == NASA_SOURCE_ID
        and row.get("object_type") == "star"
        and row.get("binding_scope") == "host"
    ]
    if len(programs) != 1:
        raise ValueError("expected one NASA star/host selection program")

    quantity_rows: list[tuple[Any, ...]] = []
    authority_rows: list[tuple[Any, ...]] = []
    mapped: set[str] = set()
    for group in programs[0].get("quantity_groups") or []:
        group_key = str(group["group_key"])
        inverse = {str(target): str(source) for source, target in group["quantities"].items()}
        for target, source in QUANTITIES.items():
            if inverse.get(target) != source:
                continue
            quantity_rows.append((target, source, group_key))
            mapped.add(target)
        for authority in group.get("authorities") or []:
            authority_rows.append(
                (
                    group_key,
                    str(authority["source_table"]),
                    int(authority["rank"]),
                    str(authority["context_field"])
                    if authority.get("context_field")
                    else None,
                    str(authority["context_value"])
                    if authority.get("context_value") is not None
                    else None,
                )
            )
    if mapped != set(QUANTITIES):
        raise ValueError(f"NASA host policy does not map all audited quantities: {mapped}")
    return quantity_rows, authority_rows


def audit(
    *,
    candidate_arm: Path,
    reference_arm: Path,
    selected_facts: Path,
    nasa_evidence: Path,
    policy_path: Path,
    build_id: str,
) -> dict[str, Any]:
    started = time.perf_counter()
    quantity_rows, authority_rows = load_contract(policy_path)
    con = duckdb.connect()
    try:
        con.execute(f"ATTACH {sql_literal(candidate_arm.resolve())} AS candidate (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(reference_arm.resolve())} AS reference (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(selected_facts.resolve())} AS selected (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(nasa_evidence.resolve())} AS evidence (READ_ONLY)")
        con.execute(
            "CREATE TEMP TABLE quantity_contract("
            "quantity_key VARCHAR,source_quantity_key VARCHAR,quantity_group VARCHAR)"
        )
        con.executemany("INSERT INTO quantity_contract VALUES (?,?,?)", quantity_rows)
        con.execute(
            "CREATE TEMP TABLE authority_contract("
            "quantity_group VARCHAR,source_table VARCHAR,authority_rank INTEGER,"
            "context_field VARCHAR,context_value VARCHAR)"
        )
        con.executemany("INSERT INTO authority_contract VALUES (?,?,?,?,?)", authority_rows)

        loss_selects: list[str] = []
        for quantity in QUANTITIES:
            loss_selects.append(
                f"""
                SELECT {sql_literal(quantity)} quantity_key,l.star_id,
                       l.stable_object_key,l.source_catalog legacy_source_catalog,
                       l.parameter_source legacy_parameter_source,l.{quantity} legacy_value
                FROM candidate.e6_selected_stellar_parameters c
                JOIN (
                  SELECT star_id,stable_object_key,source_catalog,parameter_source,{quantity}
                  FROM reference.stellar_parameters
                  QUALIFY row_number() OVER (
                    PARTITION BY star_id ORDER BY
                      CASE parameter_source
                        WHEN 'nasa_pscomppars_host' THEN 0
                        WHEN 'gaia_dr3_backbone' THEN 1 ELSE 9
                      END,
                      ((mass_msun IS NOT NULL)::INTEGER +
                       (radius_rsun IS NOT NULL)::INTEGER +
                       (luminosity_log10_lsun IS NOT NULL)::INTEGER +
                       (teff_k IS NOT NULL)::INTEGER) DESC,
                      stellar_parameter_id
                  )=1
                ) l USING (star_id)
                WHERE c.{quantity} IS NULL AND l.{quantity} IS NOT NULL
                """
            )
        con.execute("CREATE TEMP TABLE losses AS " + " UNION ALL ".join(loss_selects))
        con.execute(
            """
            CREATE TEMP TABLE alternatives AS
            SELECT l.quantity_key,l.star_id,l.stable_object_key,l.legacy_value,
                   sr.source_record_id,sr.source_table,ps.parameter_set_id,
                   ev.evidence_id,ev.normalized_value,a.authority_rank,
                   abs(ev.normalized_value-l.legacy_value) <=
                     greatest(1e-9,abs(l.legacy_value)*1e-9) AS value_matches_legacy
            FROM losses l
            JOIN quantity_contract q USING (quantity_key)
            JOIN selected.evidence_object_bindings b
              ON b.stable_object_key=l.stable_object_key
             AND b.source_id=? AND b.object_type='star'
             AND b.binding_scope='host' AND b.binding_status='accepted'
            JOIN evidence.source_records sr USING (source_record_id)
            JOIN evidence.stellar_parameter_sets ps USING (source_record_id)
            JOIN evidence.stellar_parameter_evidence ev USING (parameter_set_id)
            JOIN authority_contract a
              ON a.quantity_group=q.quantity_group AND a.source_table=sr.source_table
             AND (
               a.context_field IS NULL OR
               json_extract_string(sr.source_context_json,'$.' || a.context_field)=a.context_value
             )
            WHERE ev.quantity_key=q.source_quantity_key
              AND ev.normalized_value IS NOT NULL
            """,
            [NASA_SOURCE_ID],
        )
        con.execute(
            """
            CREATE TEMP TABLE accounted AS
            SELECT l.*,q.quantity_group,d.decision_id,d.selected_source_id,
                   d.selected_source_record_id,d.selected_parameter_set_id,
                   d.authority_rank selected_authority_rank,
                   count(DISTINCT sf.selected_fact_id) selected_fact_count,
                   count(DISTINCT a.evidence_id) alternative_evidence_count,
                   min(a.authority_rank) alternative_best_rank,
                   count(DISTINCT a.evidence_id) FILTER (
                     WHERE a.value_matches_legacy
                   ) matching_current_evidence_count
            FROM losses l
            JOIN quantity_contract q USING (quantity_key)
            LEFT JOIN selected.parameter_set_selection_decisions d
              ON d.stable_object_key=l.stable_object_key
             AND d.object_type='star' AND d.quantity_group=q.quantity_group
            LEFT JOIN selected.selected_facts sf
              ON sf.stable_object_key=l.stable_object_key
             AND sf.object_type='star' AND sf.quantity_key=l.quantity_key
            LEFT JOIN alternatives a
              ON a.quantity_key=l.quantity_key AND a.star_id=l.star_id
            GROUP BY ALL
            """
        )

        rows = con.execute(
            """
            SELECT quantity_key,count(*) losses,
                   count(*) FILTER (WHERE decision_id IS NOT NULL) with_decision,
                   count(*) FILTER (WHERE selected_fact_count>0) selected_fact_materialization_gap,
                   count(*) FILTER (WHERE alternative_evidence_count=0) without_alternative,
                   count(*) FILTER (WHERE matching_current_evidence_count>0)
                     exact_current_release_match,
                   count(*) FILTER (
                     WHERE alternative_evidence_count>0 AND matching_current_evidence_count=0
                   ) superseded_source_release_value,
                   count(*) FILTER (
                     WHERE alternative_best_rank<selected_authority_rank
                   ) higher_authority_alternative,
                   min(selected_authority_rank),max(selected_authority_rank),
                   min(alternative_best_rank),max(alternative_best_rank)
            FROM accounted GROUP BY 1 ORDER BY 1
            """
        ).fetchall()
        by_quantity = {
            str(row[0]): {
                "losses": int(row[1]),
                "with_selection_decision": int(row[2]),
                "selected_fact_materialization_gaps": int(row[3]),
                "without_acceptable_alternative": int(row[4]),
                "exact_current_release_value_matches": int(row[5]),
                "superseded_source_release_values": int(row[6]),
                "higher_authority_alternative_rows": int(row[7]),
                "selected_authority_rank_min": int(row[8]) if row[8] is not None else None,
                "selected_authority_rank_max": int(row[9]) if row[9] is not None else None,
                "alternative_authority_rank_min": int(row[10]) if row[10] is not None else None,
                "alternative_authority_rank_max": int(row[11]) if row[11] is not None else None,
            }
            for row in rows
        }
        total_losses = int(con.execute("SELECT count(*) FROM accounted").fetchone()[0])
        checks = {
            "duplicate_loss_rows": int(
                con.execute(
                    "SELECT count(*) FROM (SELECT quantity_key,star_id FROM accounted "
                    "GROUP BY 1,2 HAVING count(*)<>1)"
                ).fetchone()[0]
            ),
            "unexpected_legacy_sources": int(
                con.execute(
                    "SELECT count(*) FROM accounted WHERE legacy_source_catalog<>"
                    "'nasa_exoplanet_archive' OR legacy_parameter_source<>"
                    "'nasa_pscomppars_host'"
                ).fetchone()[0]
            ),
            "missing_selection_decisions": int(
                con.execute("SELECT count(*) FROM accounted WHERE decision_id IS NULL").fetchone()[0]
            ),
            "selected_fact_materialization_gaps": int(
                con.execute("SELECT count(*) FROM accounted WHERE selected_fact_count>0").fetchone()[0]
            ),
            "missing_acceptable_alternatives": int(
                con.execute(
                    "SELECT count(*) FROM accounted WHERE alternative_evidence_count=0"
                ).fetchone()[0]
            ),
            "higher_authority_alternative_rows": int(
                con.execute(
                    "SELECT count(*) FROM accounted "
                    "WHERE alternative_best_rank<selected_authority_rank"
                ).fetchone()[0]
            ),
        }
        failing = {key: value for key, value in checks.items() if value != 0}
        observations = {
            "superseded_source_release_values": int(
                con.execute(
                    "SELECT count(*) FROM accounted "
                    "WHERE alternative_evidence_count>0 AND matching_current_evidence_count=0"
                ).fetchone()[0]
            )
        }
        samples = [
            {
                "quantity_key": str(row[0]),
                "stable_object_key": str(row[1]),
                "legacy_value": float(row[2]),
                "selected_source_id": str(row[3]),
                "selected_source_record_id": str(row[4]),
                "selected_parameter_set_id": str(row[5]),
                "selected_authority_rank": int(row[6]),
                "alternative_best_rank": int(row[7]),
                "alternative_evidence_count": int(row[8]),
            }
            for row in con.execute(
                """
                SELECT quantity_key,stable_object_key,legacy_value,selected_source_id,
                       selected_source_record_id,selected_parameter_set_id,
                       selected_authority_rank,alternative_best_rank,
                       alternative_evidence_count
                FROM accounted ORDER BY quantity_key,stable_object_key LIMIT 40
                """
            ).fetchall()
        ]
        failing_samples = [
            {
                "quantity_key": str(row[0]),
                "stable_object_key": str(row[1]),
                "legacy_value": float(row[2]),
                "selected_source_id": str(row[3]) if row[3] is not None else None,
                "selected_authority_rank": int(row[4]) if row[4] is not None else None,
                "alternative_best_rank": int(row[5]) if row[5] is not None else None,
                "alternative_evidence_count": int(row[6]),
                "matching_current_evidence_count": int(row[7]),
            }
            for row in con.execute(
                """
                SELECT quantity_key,stable_object_key,legacy_value,selected_source_id,
                       selected_authority_rank,alternative_best_rank,
                       alternative_evidence_count,matching_current_evidence_count
                FROM accounted
                WHERE decision_id IS NULL OR selected_fact_count>0
                   OR alternative_evidence_count=0
                   OR alternative_best_rank<selected_authority_rank
                ORDER BY quantity_key,stable_object_key LIMIT 100
                """
            ).fetchall()
        ]
    finally:
        con.close()

    return {
        "schema_version": "spacegate.e6_parameter_loss_accounting.v1",
        "status": "pass" if not failing else "fail",
        "build_id": build_id,
        "policy_version": json.loads(policy_path.read_text(encoding="utf-8"))[
            "policy_version"
        ],
        "total_legacy_parameter_losses": total_losses,
        "dispositions": {
            "exact_current_release_value": "retained_lower_authority_coherent_alternative",
            "changed_current_release_value": "superseded_source_release_value",
        },
        "by_quantity": by_quantity,
        "checks": checks,
        "observations": observations,
        "failing_checks": failing,
        "failing_samples": failing_samples,
        "samples": samples,
        "wall_seconds": round(time.perf_counter() - started, 6),
        "peak_rss_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-arm", type=Path, required=True)
    parser.add_argument("--reference-arm", type=Path, required=True)
    parser.add_argument("--selected-facts", type=Path, required=True)
    parser.add_argument("--nasa-evidence", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = audit(
        candidate_arm=args.candidate_arm,
        reference_arm=args.reference_arm,
        selected_facts=args.selected_facts,
        nasa_evidence=args.nasa_evidence,
        policy_path=args.policy,
        build_id=args.build_id,
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
