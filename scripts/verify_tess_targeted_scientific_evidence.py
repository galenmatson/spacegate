#!/usr/bin/env python3
"""Audit the targeted TIC/TOI scientific-evidence checkpoint."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


EXPECTED_SOURCE_ROWS = {
    "gaia_dr2_neighbourhood_targeted": 29_302,
    "gaia_dr3_targets": 29_409,
    "gaia_external_crossmatches": 137,
    "mast_tic_targeted": 27_930,
    "nasa_toi": 8_064,
    "tess_target_set": 27_930,
}
EXPECTED_IDENTIFIERS = {
    "allwise_designation": 26_775,
    "apass_id": 23_427,
    "ctoi_id": 8_064,
    "gaia_dr2_source_id": 57_077,
    "gaia_dr3_source_id": 58_848,
    "hip_id": 3_560,
    "kepler_target_id": 2_733,
    "mast_tic_object_id": 27_930,
    "sdss_dr9_id": 2_403,
    "tic_id": 64_002,
    "toi_host_prefix": 8_064,
    "toi_id": 16_128,
    "twomass_designation": 27_911,
    "tyc_id": 12_704,
    "ucac4_id": 27_076,
}
EXPECTED_CLASSIFICATIONS = {
    "gaia_photometric_white_dwarf_region_flag": 27_358,
    "tic_luminosity_class": 26_406,
    "tic_radius_dwarf_giant_flag": 26_704,
}
EXPECTED_LIFECYCLE = {
    ("APC", "CANDIDATE", "candidate"): 483,
    ("CP", "CONFIRMED", "positive"): 739,
    ("FA", "FALSE_ALARM", "negative"): 100,
    ("FP", "FALSE_POSITIVE", "negative"): 1_246,
    ("KP", "CONFIRMED", "positive"): 593,
    ("PC", "CANDIDATE", "candidate"): 4_900,
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


def symmetric_difference_count(
    con: duckdb.DuckDBPyConnection,
    left: str,
    right: str,
) -> int:
    return scalar(
        con,
        "select sum(difference_count) from ("
        f"select count(*) difference_count from (({left}) except ({right})) "
        f"union all select count(*) from (({right}) except ({left})))",
    )


def audit(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    source_counts = {
        str(table): int(count)
        for table, count in con.execute(
            "select source_table,count(*) from source_records group by source_table"
        ).fetchall()
    }
    identifier_counts = {
        str(namespace): int(count)
        for namespace, count in con.execute(
            "select namespace,count(*) from identifier_claim_evidence group by namespace"
        ).fetchall()
    }
    classification_counts = {
        str(scheme): int(count)
        for scheme, count in con.execute(
            "select classification_scheme,count(*) "
            "from stellar_classification_evidence group by classification_scheme"
        ).fetchall()
    }
    lifecycle_counts = {
        (str(raw), str(normalized), str(polarity)): int(count)
        for raw, normalized, polarity, count in con.execute(
            "select disposition_raw,disposition_normalized,evidence_polarity,count(*) "
            "from planet_lifecycle_evidence group by all"
        ).fetchall()
    }
    target_tics = """
      select distinct i.identifier_normalized
      from identifier_claim_evidence i join source_records r using(source_record_id)
      where r.source_table='tess_target_set' and i.namespace='tic_id'
        and (i.quality_json->>'source_field')='tic_id'
    """
    mast_tics = """
      select distinct i.identifier_normalized
      from identifier_claim_evidence i join source_records r using(source_record_id)
      where r.source_table='mast_tic_targeted' and i.namespace='tic_id'
        and (i.quality_json->>'source_field')='ID'
    """
    toi_tics = """
      select distinct i.identifier_normalized
      from identifier_claim_evidence i join source_records r using(source_record_id)
      where r.source_table='nasa_toi' and i.namespace='tic_id'
    """
    uncertainty_tables = [
        "astrometry_distance_evidence",
        "photometry_extinction_evidence",
        "planet_parameter_evidence",
        "stellar_parameter_evidence",
        "transit_observation_evidence",
    ]
    nonfinite = sum(
        scalar(
            con,
            f"select count(*) from {table} where "
            "(normalized_value is not null and not isfinite(normalized_value)) or "
            "(uncertainty_lower is not null and not isfinite(uncertainty_lower)) or "
            "(uncertainty_upper is not null and not isfinite(uncertainty_upper))",
        )
        for table in uncertainty_tables
    )
    checks = {
        "unexpected_source_rows": mismatch_count(source_counts, EXPECTED_SOURCE_ROWS),
        "collapsed_or_duplicate_source_rows": scalar(
            con, "select count(*) from source_records where source_duplicate_count<>1"
        ),
        "unaccounted_or_pending_fields": scalar(
            con,
            "select count(*) from source_field_dispositions "
            "where mapping_status<>'materialized'",
        ),
        "unexpected_field_count": abs(
            scalar(con, "select count(*) from source_field_dispositions") - 239
        ),
        "unexpected_identifier_counts": mismatch_count(
            identifier_counts, EXPECTED_IDENTIFIERS
        ),
        "target_and_mast_tic_set_difference": symmetric_difference_count(
            con, target_tics, mast_tics
        ),
        "toi_host_tic_outside_target_set": scalar(
            con, f"select count(*) from (({toi_tics}) except ({target_tics}))"
        ),
        "toi_raw_and_display_identifier_mismatch": abs(
            scalar(
                con,
                """
                select count(*) from (
                  select source_record_id,
                    count(*) filter(where (quality_json->>'source_field')='toi') numeric_count,
                    count(*) filter(where (quality_json->>'source_field')='toidisplay') display_count,
                    count(distinct identifier_normalized) normalized_count
                  from identifier_claim_evidence
                  where namespace='toi_id'
                  group by source_record_id
                  having numeric_count=1 and display_count=1 and normalized_count=1
                )
                """,
            )
            - 8_064
        ),
        "unexpected_lifecycle_counts": mismatch_count(
            lifecycle_counts, EXPECTED_LIFECYCLE
        ),
        "unexpected_unclassified_toi_count": abs(
            (
                scalar(
                    con,
                    "select count(*) from source_records where source_table='nasa_toi'",
                )
                - scalar(con, "select count(*) from planet_lifecycle_evidence")
            )
            - 3
        ),
        "unexpected_classification_counts": mismatch_count(
            classification_counts, EXPECTED_CLASSIFICATIONS
        ),
        "gaia_release_relation_namespace_mismatch": scalar(
            con,
            "select count(*) from relation_claim_evidence where "
            "relation_kind='official_gaia_release_neighbourhood' and not ("
            "left_identity_namespace='gaia_dr2_source_id' and "
            "right_identity_namespace='gaia_dr3_source_id')",
        ),
        "tic_gaia_relation_namespace_mismatch": scalar(
            con,
            "select count(*) from relation_claim_evidence where "
            "relation_kind='catalog_identity_association' and not ("
            "left_identity_namespace='tic_id' and "
            "right_identity_namespace='gaia_dr2_source_id')",
        ),
        "unexpected_gaia_neighbourhood_relation_count": abs(
            scalar(
                con,
                "select count(*) from relation_claim_evidence where "
                "relation_kind='official_gaia_release_neighbourhood'",
            )
            - 29_302
        ),
        "unexpected_tic_gaia_relation_count": abs(
            scalar(
                con,
                "select count(*) from relation_claim_evidence where "
                "relation_kind='catalog_identity_association'",
            )
            - 27_775
        ),
        "unexpected_tic_duplicate_relation_count": abs(
            scalar(
                con,
                "select count(*) from relation_claim_evidence where "
                "relation_kind='tic_catalog_duplicate_or_split_association'",
            )
            - 78
        ),
        "external_crossmatch_member_lineage_missing": scalar(
            con,
            "select count(*) from source_records where "
            "source_table='gaia_external_crossmatches' and "
            "nullif((logical_key_json->>'source_member_path'),'') is null",
        ),
        "external_crossmatch_namespace_mismatch": scalar(
            con,
            """
            select count(*)
            from relation_claim_evidence e join source_records r using(source_record_id)
            where r.source_table='gaia_external_crossmatches' and not (
              ((r.logical_key_json->>'source_member_path') like 'hip_%'
               and e.right_identity_namespace='hip_id') or
              ((r.logical_key_json->>'source_member_path') like 'twomass_%'
               and e.right_identity_namespace='twomass_designation') or
              ((r.logical_key_json->>'source_member_path') like 'tyc_%'
               and e.right_identity_namespace='tyc_id')
            )
            """,
        ),
        "unexpected_external_crossmatch_member_counts": scalar(
            con,
            """
            select abs(count(*) filter(where (logical_key_json->>'source_member_path')
                         like 'hip_%')-19)
                 + abs(count(*) filter(where (logical_key_json->>'source_member_path')
                         like 'twomass_%')-118)
                 + count(*) filter(where (logical_key_json->>'source_member_path')
                         not like 'hip_%' and (logical_key_json->>'source_member_path')
                         not like 'twomass_%')
            from source_records where source_table='gaia_external_crossmatches'
            """,
        ),
        "relation_endpoint_claim_or_binding_gap": scalar(
            con,
            """
            with endpoints as (
              select evidence_id,source_record_id,left_identity_namespace namespace,
                left_identity_raw identifier,left_component_scope component_scope
              from relation_claim_evidence
              union all
              select evidence_id,source_record_id,right_identity_namespace,
                right_identity_raw,right_component_scope
              from relation_claim_evidence
            )
            select count(distinct e.evidence_id)
            from endpoints e
            left join identifier_claim_evidence i
              on i.source_record_id=e.source_record_id and i.namespace=e.namespace
             and i.identifier_normalized=e.identifier
             and i.component_scope is not distinct from e.component_scope
            left join object_binding_outcomes b
              on b.source_record_id=e.source_record_id and b.binding_scope=i.claim_scope
             and b.component_scope is not distinct from i.component_scope
            where i.evidence_id is null or b.binding_outcome_id is null
            """,
        ),
        "source_records_without_binding_outcome": scalar(
            con,
            "select count(*) from source_records r where not exists ("
            "select 1 from object_binding_outcomes b "
            "where b.source_record_id=r.source_record_id)",
        ),
        "premature_non_unresolved_binding": scalar(
            con,
            "select count(*) from object_binding_outcomes "
            "where binding_status<>'unresolved' or spacegate_object_id is not null "
            "or stable_object_key is not null",
        ),
        "canonical_inventory_table_present": scalar(
            con,
            "select count(*) from information_schema.tables where "
            "table_schema='main' and table_name in ('systems','stars','planets','aliases')",
        ),
        "nonfinite_normalized_measurement": nonfinite,
        "asymmetric_tic_measurement_count_mismatch": abs(
            sum(
                scalar(
                    con,
                    f"select count(*) from {table} e join source_records r "
                    "using(source_record_id) where r.source_table='mast_tic_targeted' "
                    "and e.uncertainty_lower is distinct from e.uncertainty_upper",
                )
                for table in (
                    "astrometry_distance_evidence",
                    "photometry_extinction_evidence",
                    "stellar_parameter_evidence",
                )
            )
            - 131_309
        ),
        "asymmetric_tic_measurement_without_field_lineage": sum(
            scalar(
                con,
                f"select count(*) from {table} e join source_records r "
                "using(source_record_id) where r.source_table='mast_tic_targeted' "
                "and e.uncertainty_lower is distinct from e.uncertainty_upper and ("
                "coalesce(nullif((e.quality_json->>'uncertainty_lower_field'),''), "
                "nullif((e.quality_json->>'error_lower_field'),'')) is null or "
                "coalesce(nullif((e.quality_json->>'uncertainty_upper_field'),''), "
                "nullif((e.quality_json->>'error_upper_field'),'')) is null)",
            )
            for table in (
                "astrometry_distance_evidence",
                "photometry_extinction_evidence",
                "stellar_parameter_evidence",
            )
        ),
        "high_proper_motion_target_missing": int(
            scalar(
                con,
                """
                select count(*) from (
                  select e.source_record_id,
                    max(e.normalized_value) filter(where e.quantity_key=
                      'proper_motion_ra_source_convention') pmra,
                    max(e.normalized_value) filter(where e.quantity_key=
                      'proper_motion_dec') pmdec
                  from astrometry_distance_evidence e
                  join source_records r using(source_record_id)
                  where r.source_table='mast_tic_targeted'
                  group by e.source_record_id
                ) where sqrt(pmra*pmra+pmdec*pmdec)>=500
                """,
            )
            == 0
        ),
        "tess_eb_seed_family_missing": int(
            scalar(
                con,
                "select count(*) from source_records where source_table='tess_target_set' "
                "and (source_context_json->>'source_families') like '%tess_eb%'",
            )
            == 0
        ),
    }
    summaries = {
        "source_rows": source_counts,
        "identifiers_by_namespace": identifier_counts,
        "classifications_by_scheme": classification_counts,
        "lifecycle": rows(
            con,
            "select disposition_raw,disposition_normalized,evidence_polarity,count(*) "
            "evidence_count from planet_lifecycle_evidence group by all order by 1",
        ),
        "relations": rows(
            con,
            "select relation_kind,evidence_polarity,count(*) evidence_count "
            "from relation_claim_evidence group by all order by 1,2",
        ),
        "bindings": rows(
            con,
            "select binding_scope,binding_status,count(*) outcome_count "
            "from object_binding_outcomes group by all order by 1,2",
        ),
    }
    return {
        "schema_version": "spacegate.tess_targeted_scientific_evidence_audit.v1",
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
            / "e4_tess_targeted_scientific_evidence_audit.json"
        ),
    )
    args = parser.parse_args()
    manifest = load_json(args.manifest)
    database = args.manifest.parent / str(manifest["database"])
    with duckdb.connect(str(database), read_only=True) as con:
        report = audit(con)
    report["build_id"] = str(manifest["build_id"])
    report["logical_content_sha256"] = str(manifest["logical_content_sha256"])
    report["database"] = str(database)
    write_json(args.report, report)
    print(
        f"targeted TESS scientific evidence audit {report['status']}: "
        f"{report['build_id']}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
