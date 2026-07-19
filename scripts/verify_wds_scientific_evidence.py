#!/usr/bin/env python3
"""Audit WDS observation evidence and the candidate WDS-Gaia bridge."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


WDS_SOURCE = "multiplicity.wds"
XMATCH_SOURCE = "multiplicity.wds_gaia_xmatch"
EXPECTED_SOURCE_RECORDS = {
    (WDS_SOURCE, "wdsweb_format"): 177,
    (WDS_SOURCE, "wdsweb_summ2"): 157_299,
    (XMATCH_SOURCE, "wds_gaia_xmatch_best"): 140_416,
}
EXPECTED_IDENTIFIERS = {
    (WDS_SOURCE, "durchmusterung_designation"): 57_892,
    (WDS_SOURCE, "wds_discoverer_designation"): 157_299,
    (WDS_SOURCE, "wds_id"): 157_299,
    (WDS_SOURCE, "wds_observation_pair"): 157_299,
    (WDS_SOURCE, "wds_observation_pair_component_scope"): 42_375,
    (XMATCH_SOURCE, "gaia_dr3_name"): 140_416,
    (XMATCH_SOURCE, "gaia_dr3_source_id"): 140_416,
    (XMATCH_SOURCE, "wds_id"): 140_416,
    (XMATCH_SOURCE, "wds_observation_pair_component_scope"): 35_569,
}
EXPECTED_ASTROMETRY = {
    "first_satisfactory_observation_year": 157_241,
    "last_satisfactory_observation_year": 157_241,
    "primary_proper_motion_dec_wds_source_convention": 154_975,
    "primary_proper_motion_ra_wds_source_convention": 154_974,
    "relative_astrometry_measure_count": 155_741,
    "relative_position_angle_first": 155_751,
    "relative_position_angle_last": 155_751,
    "relative_separation_first": 157_219,
    "relative_separation_last": 157_216,
    "secondary_proper_motion_dec_wds_source_convention": 107_878,
    "secondary_proper_motion_ra_wds_source_convention": 107_877,
    "subsystem_primary_j2000_coordinate_string": 157_299,
}
EXPECTED_PHOTOMETRY = {
    "apparent_magnitude_first_component": 157_195,
    "apparent_magnitude_second_component": 155_532,
}


def scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    result = con.execute(query)
    columns = [value[0] for value in result.description]
    return [dict(zip(columns, values, strict=True)) for values in result.fetchall()]


def mismatch_count(actual: dict[Any, int], expected: dict[Any, int]) -> int:
    return sum(abs(actual.get(key, 0) - count) for key, count in expected.items()) + sum(
        count for key, count in actual.items() if key not in expected
    )


def audit(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    source_counts = {
        (str(source), str(table)): int(count)
        for source, table, count in con.execute(
            "select source_id,source_table,count(*) from source_records group by all"
        ).fetchall()
    }
    identifier_counts = {
        (str(source), str(namespace)): int(count)
        for source, namespace, count in con.execute(
            """
            select r.source_id,i.namespace,count(*)
            from identifier_claim_evidence i join source_records r using(source_record_id)
            group by all
            """
        ).fetchall()
    }
    astrometry_counts = {
        str(quantity): int(count)
        for quantity, count in con.execute(
            """
            select e.quantity_key,count(*)
            from astrometry_distance_evidence e join source_records r using(source_record_id)
            where r.source_id=? group by e.quantity_key
            """,
            [WDS_SOURCE],
        ).fetchall()
    }
    photometry_counts = {
        str(quantity): int(count)
        for quantity, count in con.execute(
            """
            select e.quantity_key,count(*)
            from photometry_extinction_evidence e join source_records r using(source_record_id)
            where r.source_id=? group by e.quantity_key
            """,
            [WDS_SOURCE],
        ).fetchall()
    }
    checks = {
        "unexpected_source_record_counts": mismatch_count(
            source_counts, EXPECTED_SOURCE_RECORDS
        ),
        "collapsed_or_duplicate_source_rows": scalar(
            con,
            "select count(*) from source_records where source_duplicate_count<>1",
        ),
        "unexpected_identifier_counts": mismatch_count(
            identifier_counts, EXPECTED_IDENTIFIERS
        ),
        "unexpected_astrometry_counts": mismatch_count(
            astrometry_counts, EXPECTED_ASTROMETRY
        ),
        "unexpected_photometry_counts": mismatch_count(
            photometry_counts, EXPECTED_PHOTOMETRY
        ),
        "unexpected_classification_count": abs(
            scalar(con, "select count(*) from stellar_classification_evidence")
            - 73_779
        ),
        "unaccounted_or_pending_fields": scalar(
            con,
            "select count(*) from source_field_dispositions "
            "where mapping_status<>'materialized'",
        ),
        "unexpected_field_count": abs(
            scalar(con, "select count(*) from source_field_dispositions") - 43
        ),
        "source_records_without_binding_outcome": scalar(
            con,
            """
            select count(*) from source_records r
            left join object_binding_outcomes b using(source_record_id)
            where b.source_record_id is null
            """,
        ),
        "premature_non_unresolved_bindings": scalar(
            con,
            "select count(*) from object_binding_outcomes where binding_status<>'unresolved'",
        ),
        "wds_observation_promoted_to_relation_or_orbit": scalar(
            con,
            f"""
            select count(*) from (
              select source_record_id from relation_claim_evidence
              union all select source_record_id from orbital_solution_evidence
            ) e join source_records r using(source_record_id)
            where r.source_id='{WDS_SOURCE}'
            """,
        ),
        "unexpected_xmatch_relation_contract": scalar(
            con,
            f"""
            select count(*) from relation_claim_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{XMATCH_SOURCE}' and (
              e.relation_kind<>'candidate_positional_crossmatch'
              or e.relation_scope<>'wds_entry_to_gaia_dr3_source'
              or e.evidence_polarity<>'candidate'
              or e.left_identity_namespace<>'wds_id'
              or e.left_component_scope<>'wds_entry'
              or e.right_identity_namespace<>'gaia_dr3_source_id'
              or e.right_component_scope<>'gaia_candidate'
            )
            """,
        )
        + abs(
            scalar(
                con,
                f"""
                select count(*) from relation_claim_evidence e
                join source_records r using(source_record_id)
                where r.source_id='{XMATCH_SOURCE}'
                """,
            )
            - 140_416
        ),
        "xmatch_probability_fabrication": scalar(
            con,
            f"""
            select count(*) from relation_claim_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{XMATCH_SOURCE}' and e.probability is not null
            """,
        ),
        "invalid_or_mismatched_xmatch_statistic": scalar(
            con,
            f"""
            select count(*) from relation_claim_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{XMATCH_SOURCE}' and (
              e.confidence_statistic_key<>'angular_separation'
              or e.confidence_statistic_unit<>'arcsec'
              or e.confidence_statistic_value not between 0 and 2
              or e.confidence_statistic_value_raw<>
                json_extract_string(r.logical_key_json, '$.angDist')
            )
            """,
        ),
        "xmatch_citation_gap": abs(
            scalar(
                con,
                f"""
                select count(*) from evidence_citations c
                join relation_claim_evidence e
                  on c.evidence_table='relation_claim_evidence'
                 and c.evidence_id=e.evidence_id
                join source_records r using(source_record_id)
                where r.source_id='{XMATCH_SOURCE}'
                """,
            )
            - 140_416
        ),
        "copied_gaia_context_promoted_as_independent_science": scalar(
            con,
            f"""
            select count(*) from (
              select source_record_id from astrometry_distance_evidence
              union all select source_record_id from photometry_extinction_evidence
              union all select source_record_id from stellar_parameter_evidence
              union all select source_record_id from stellar_classification_evidence
              union all select source_record_id from orbital_solution_evidence
            ) e join source_records r using(source_record_id)
            where r.source_id='{XMATCH_SOURCE}'
            """,
        ),
        "bare_component_label_claim": scalar(
            con,
            """
            select count(*) from identifier_claim_evidence
            where namespace in ('wds_component','wds_component_label')
              or (namespace='wds_observation_pair_component_scope'
                  and strpos(identifier_raw, ':')=0)
            """,
        ),
        "wds_spectral_text_prematurely_component_scoped": scalar(
            con,
            "select count(*) from stellar_classification_evidence "
            "where component_scope is not null",
        ),
        "wds_astrometry_sentinel_or_domain_leak": scalar(
            con,
            """
            select count(*) from astrometry_distance_evidence where
              (quantity_key like 'relative_position_angle_%'
                and normalized_value not between 0 and 359)
              or (quantity_key like '%observation_year' and normalized_value<1)
              or (quantity_key='relative_astrometry_measure_count'
                and normalized_value<1)
              or (quantity_key like 'relative_separation_%' and normalized_value<0)
              or (quantity_key<>'subsystem_primary_j2000_coordinate_string'
                and normalized_value is null)
              or (quantity_key='subsystem_primary_j2000_coordinate_string'
                and normalized_value is not null)
            """,
        ),
        "wds_magnitude_sentinel_or_normalization_gap": scalar(
            con,
            "select count(*) from photometry_extinction_evidence "
            "where value_raw='.' or normalized_value is null",
        ),
    }
    summaries = {
        "source_records_by_table": rows(
            con,
            "select source_id,source_table,count(*) row_count from source_records "
            "group by all order by source_id,source_table",
        ),
        "identifiers_by_namespace": rows(
            con,
            """
            select r.source_id,i.namespace,count(*) claim_count,
              count(distinct i.identifier_normalized) distinct_value_count
            from identifier_claim_evidence i join source_records r using(source_record_id)
            group by all order by r.source_id,i.namespace
            """,
        ),
        "astrometry_by_quantity": rows(
            con,
            "select quantity_key,count(*) evidence_count from astrometry_distance_evidence "
            "group by quantity_key order by quantity_key",
        ),
        "relations": rows(
            con,
            """
            select relation_kind,evidence_polarity,count(*) evidence_count,
              count(probability) strict_probability_count,
              min(confidence_statistic_value) minimum_separation_arcsec,
              max(confidence_statistic_value) maximum_separation_arcsec
            from relation_claim_evidence group by relation_kind,evidence_polarity
            """,
        ),
        "bindings": rows(
            con,
            "select binding_status,binding_scope,count(*) outcome_count "
            "from object_binding_outcomes group by all order by binding_scope",
        ),
    }
    return {
        "schema_version": "spacegate.wds_scientific_evidence_audit.v1",
        "status": "pass" if not any(checks.values()) else "fail",
        "checks": checks,
        "summaries": summaries,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--report",
        type=Path,
        default=(
            DEFAULT_STATE
            / "reports"
            / "evidence_lake_v2"
            / "e4_wds_scope_audit.json"
        ),
    )
    args = parser.parse_args()
    manifest = load_json(args.manifest)
    database = args.manifest.parent / str(manifest["database"])
    with duckdb.connect(str(database), read_only=True) as con:
        report = audit(con)
    report["build_id"] = str(manifest["build_id"])
    report["database"] = str(database)
    write_json(args.report, report)
    print(f"WDS scientific evidence audit {report['status']}: {report['build_id']}")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
