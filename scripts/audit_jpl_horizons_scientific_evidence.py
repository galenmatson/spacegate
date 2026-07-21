#!/usr/bin/env python3
"""Audit natural and artificial JPL Horizons E4 scientific evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json  # noqa: E402


AUTHORITY = "solar_system.jpl_horizons_authority"
ARTIFICIAL = "solar_system.jpl_horizons_artificial"
SOURCE_IDS = {AUTHORITY, ARTIFICIAL}
EXPECTED_RELEASES = {
    AUTHORITY: "horizons_s1_2016_01_01",
    ARTIFICIAL: "horizons_s4_2026_07_21",
}
EXPECTED_SOURCE_ROWS = {
    (AUTHORITY, "sol_authority_horizons_responses"): 60,
    (AUTHORITY, "sol_system_objects"): 60,
    (ARTIFICIAL, "sol_artificial_horizons_responses"): 11,
    (ARTIFICIAL, "sol_artificial_objects"): 11,
}
EXPECTED_FIELD_MAPPING = {
    (AUTHORITY, "excluded"): 1,
    (AUTHORITY, "materialized"): 32,
    (ARTIFICIAL, "excluded"): 1,
    (ARTIFICIAL, "materialized"): 33,
}
EXPECTED_IDENTIFIER_CLAIMS = {
    (AUTHORITY, "jpl_horizons_target"): 120,
    (AUTHORITY, "spacegate_operator_seed_name"): 60,
    (AUTHORITY, "spacegate_operator_seed_target_key"): 60,
    (ARTIFICIAL, "jpl_horizons_target"): 22,
    (ARTIFICIAL, "spacegate_operator_seed_name"): 11,
    (ARTIFICIAL, "spacegate_operator_seed_target_key"): 11,
}
EXPECTED_RELATIONS = {
    (AUTHORITY, "jpl_horizons_orbit_center"): 60,
    (ARTIFICIAL, "jpl_horizons_trajectory_center"): 11,
}
EXPECTED_CITATIONS = {AUTHORITY: 61, ARTIFICIAL: 12}
EXPECTED_BINDING_OUTCOMES = {
    ("unresolved", "artificial_object_horizons_response"): 11,
    ("unresolved", "artificial_object_target"): 11,
    ("unresolved", "natural_solar_system_horizons_response"): 60,
    ("unresolved", "natural_solar_system_target"): 60,
    ("unresolved", "orbit_center_target"): 71,
    ("unresolved", "reviewed_artificial_target_and_horizons_trajectory"): 11,
    ("unresolved", "reviewed_natural_solar_system_target_and_horizons_solution"): 60,
    ("unresolved", "reviewed_operator_seed_target"): 71,
}
EXPECTED_EVIDENCE_LINKS = {
    ("orbital_solution_evidence", "source_reference"): 71,
    ("relation_claim_evidence", "source_reference"): 71,
    ("solar_system_object_parameter_sets", "source_reference"): 36,
}
DEFAULT_REPORT = DEFAULT_STATE / "reports" / "evidence_lake_v2" / (
    "e4_jpl_horizons_scientific_evidence_audit.json"
)


def count_map(
    con: duckdb.DuckDBPyConnection,
    query: str,
    parameters: list[Any] | None = None,
) -> dict[Any, int]:
    rows = con.execute(query, parameters or []).fetchall()
    return {
        tuple(str(value) for value in row[:-1]) if len(row) > 2 else str(row[0]): int(
            row[-1]
        )
        for row in rows
    }


def deltas(actual: dict[Any, int], expected: dict[Any, int]) -> dict[str, int]:
    keys = sorted(set(actual) | set(expected), key=str)
    return {
        "|".join(key) if isinstance(key, tuple) else str(key): (
            actual.get(key, 0) - expected.get(key, 0)
        )
        for key in keys
        if actual.get(key, 0) != expected.get(key, 0)
    }


def audit(con: duckdb.DuckDBPyConnection, manifest: dict[str, Any]) -> dict[str, Any]:
    releases = {
        str(source_id): str(release_id)
        for source_id, release_id in con.execute(
            "select source_id,release_id from evidence_sources order by source_id"
        ).fetchall()
    }
    source_rows = count_map(
        con,
        "select source_id,source_table,count(*) from source_records "
        "group by 1,2 order by 1,2",
    )
    field_mapping = count_map(
        con,
        "select source_id,mapping_status,count(*) from source_field_dispositions "
        "group by 1,2 order by 1,2",
    )
    identifier_claims = count_map(
        con,
        "select r.source_id,i.namespace,count(*) "
        "from identifier_claim_evidence i "
        "join source_records r using(source_record_id) "
        "group by 1,2 order by 1,2",
    )
    relations = count_map(
        con,
        "select r.source_id,e.relation_kind,count(*) "
        "from relation_claim_evidence e "
        "join source_records r using(source_record_id) "
        "group by 1,2 order by 1,2",
    )
    citations = {
        str(source_id): int(count)
        for source_id, count in con.execute(
            "select source_id,count(*) from citations group by 1 order by 1"
        ).fetchall()
    }
    binding_outcomes = count_map(
        con,
        "select binding_status,binding_scope,count(*) "
        "from object_binding_outcomes group by 1,2 order by 1,2",
    )
    evidence_links = count_map(
        con,
        "select evidence_table,citation_role,count(*) "
        "from evidence_citations group by 1,2 order by 1,2",
    )
    row_counts = {
        str(table): int(count)
        for table, count in con.execute(
            "select 'observation_product_lineage',count(*) "
            "from observation_product_lineage union all "
            "select 'orbital_solution_evidence',count(*) "
            "from orbital_solution_evidence union all "
            "select 'relation_claim_evidence',count(*) "
            "from relation_claim_evidence union all "
            "select 'solar_system_object_parameter_sets',count(*) "
            "from solar_system_object_parameter_sets"
        ).fetchall()
    }
    schema_rows = con.execute(
        "select source_id,schema_json from coherent_parameter_set_schemas "
        "where destination='solar_system_object_parameter_sets' order by source_id"
    ).fetchall()
    schema_summaries = {}
    invalid_schema_metadata = 0
    for source_id, schema_raw in schema_rows:
        fields = list(json.loads(str(schema_raw)).get("fields") or [])
        summary = {
            str(field["name"]): {
                "unit": field.get("unit"),
                "description": field.get("description"),
            }
            for field in fields
        }
        schema_summaries[str(source_id)] = summary
        invalid_schema_metadata += summary != {
            "radius_km": {
                "unit": "km",
                "description": "Source-published mean radius",
            },
            "mass_kg": {
                "unit": "kg",
                "description": "Source-published mass",
            },
        }
    checks: dict[str, Any] = {
        "release_mismatches": {
            source_id: releases.get(source_id)
            for source_id in sorted(set(releases) | set(EXPECTED_RELEASES))
            if releases.get(source_id) != EXPECTED_RELEASES.get(source_id)
        },
        "source_record_count_deltas": deltas(source_rows, EXPECTED_SOURCE_ROWS),
        "field_mapping_count_deltas": deltas(
            field_mapping, EXPECTED_FIELD_MAPPING
        ),
        "identifier_claim_count_deltas": deltas(
            identifier_claims, EXPECTED_IDENTIFIER_CLAIMS
        ),
        "relation_count_deltas": deltas(relations, EXPECTED_RELATIONS),
        "citation_count_deltas": deltas(citations, EXPECTED_CITATIONS),
        "binding_outcome_count_deltas": deltas(
            binding_outcomes, EXPECTED_BINDING_OUTCOMES
        ),
        "evidence_citation_count_deltas": deltas(
            evidence_links, EXPECTED_EVIDENCE_LINKS
        ),
        "domain_row_count_deltas": deltas(
            row_counts,
            {
                "observation_product_lineage": 71,
                "orbital_solution_evidence": 71,
                "relation_claim_evidence": 71,
                "solar_system_object_parameter_sets": 36,
            },
        ),
        "coherent_schema_count_delta": len(schema_rows) - 2,
        "invalid_coherent_schema_metadata": invalid_schema_metadata,
        "identifier_normalization_rejections": int(
            con.execute(
                "select count(*) from identifier_normalization_rejections"
            ).fetchone()[0]
        ),
        "operator_fields_in_jpl_namespace": int(
            con.execute(
                "select count(*) from identifier_claim_evidence "
                "where namespace='jpl_horizons_target' "
                "and json_extract_string(quality_json,'$.source_field') "
                "not in ('horizons_command','center_target_command')"
            ).fetchone()[0]
        ),
        "jpl_fields_in_operator_namespace": int(
            con.execute(
                "select count(*) from identifier_claim_evidence "
                "where namespace like 'spacegate_operator_seed_%' "
                "and json_extract_string(quality_json,'$.source_field') "
                "not in ('source_pk','object_name')"
            ).fetchone()[0]
        ),
        "relation_endpoint_namespace_mismatch": int(
            con.execute(
                "select count(*) from relation_claim_evidence "
                "where left_identity_namespace<>'jpl_horizons_target' "
                "or right_identity_namespace<>'jpl_horizons_target'"
            ).fetchone()[0]
        ),
        "orbits_without_exact_relation": int(
            con.execute(
                "select count(*) from orbital_solution_evidence "
                "where relation_claim_id is null"
            ).fetchone()[0]
        ),
        "invalid_orbit_contract_metadata": int(
            con.execute(
                "select count(*) from orbital_solution_evidence "
                "where frame_raw<>" 
                "'ICRF with ecliptic reference plane; TDB epoch; AU-D output units' "
                "or model<>'JPL Horizons osculating elements' "
                "or json_extract_string(quality_json, "
                "'$.parameter_metadata.eccentricity.unit')<>'dimensionless' "
                "or json_extract_string(quality_json, "
                "'$.parameter_metadata.inclination_deg.unit')<>'deg' "
                "or json_extract_string(quality_json, "
                "'$.parameter_metadata.semi_major_axis_au.unit')<>'au' "
                "or json_extract_string(quality_json, "
                "'$.parameter_metadata.orbital_period_days.unit')<>'d'"
            ).fetchone()[0]
        ),
        "invalid_response_products": int(
            con.execute(
                "select count(*) from observation_product_lineage "
                "where product_kind<>'jpl_horizons_api_response' "
                "or retrieval_policy<>'local_immutable_raw_snapshot' "
                "or processing_level<>'raw_api_response' "
                "or not starts_with(product_locator,'responses/') "
                "or contains(product_locator,'..') "
                "or not regexp_full_match(checksum,'sha256:[0-9a-f]{64}') "
                "or bytes<=0"
            ).fetchone()[0]
        ),
        "operator_parent_leaked_into_relation_quality": int(
            con.execute(
                "select count(*) from relation_claim_evidence "
                "where json_exists(quality_json,'$.parent_object_name')"
            ).fetchone()[0]
        ),
    }
    failed = any(bool(value) for value in checks.values())
    return {
        "schema_version": "spacegate.jpl_horizons_scientific_evidence_audit.v1",
        "status": "fail" if failed else "pass",
        "build_id": manifest["build_id"],
        "source_ids": sorted(SOURCE_IDS),
        "checks": checks,
        "summaries": {
            "releases": releases,
            "source_records": {
                "|".join(key): value for key, value in source_rows.items()
            },
            "field_mapping": {
                "|".join(key): value for key, value in field_mapping.items()
            },
            "identifier_claims": {
                "|".join(key): value for key, value in identifier_claims.items()
            },
            "relations": {
                "|".join(key): value for key, value in relations.items()
            },
            "citations": citations,
            "binding_outcomes": {
                "|".join(key): value for key, value in binding_outcomes.items()
            },
            "evidence_citations": {
                "|".join(key): value for key, value in evidence_links.items()
            },
            "domain_rows": row_counts,
            "physical_parameter_schemas": schema_summaries,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    manifest = load_json(args.manifest)
    database = args.manifest.parent / str(manifest["database"])
    with duckdb.connect(str(database), read_only=True) as con:
        report = audit(con, manifest)
    write_json(args.report, report)
    print(
        f"JPL Horizons scientific evidence audit {report['status']}: "
        f"build={manifest['build_id']} rows={sum(EXPECTED_SOURCE_ROWS.values())}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
