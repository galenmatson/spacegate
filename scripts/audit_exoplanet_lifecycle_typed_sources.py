#!/usr/bin/env python3
"""Audit pinned supplemental exoplanet lifecycle typed sources."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from compile_scientific_evidence import (  # noqa: E402
    DEFAULT_REGISTRY,
    DEFAULT_STATE,
    load_json,
    source_input,
    write_json,
)


SOURCE_IDS = (
    "exoplanet_lifecycle.exoplanet_eu",
    "exoplanet_lifecycle.open_exoplanet_catalogue",
    "exoplanet_lifecycle.hwc",
)
EXPECTED_TABLES = {
    "exoplanet_lifecycle.exoplanet_eu": {
        "exoplanet_eu_catalog_20260721": (8_261, 98),
    },
    "exoplanet_lifecycle.open_exoplanet_catalogue": {
        "oec_archive_members": (7_047, 4),
        "oec_objects": (23_785, 7),
        "oec_names": (59_876, 5),
        "oec_parameters": (160_582, 7),
        "oec_relations": (16_750, 9),
    },
    "exoplanet_lifecycle.hwc": {
        "hwc_catalog_20260721": (5_599, 118),
    },
}
EXPECTED_OEC_KINDS = {
    "asteroid": 1,
    "binary": 224,
    "planet": 9_253,
    "satellite": 18,
    "star": 7_254,
    "system": 7_035,
}
EXPECTED_OEC_DISPOSITIONS = {
    "Confirmed planets": 5_287,
    "Kepler Objects of Interest": 3_844,
    "Controversial": 100,
    "Retracted planet candidate": 12,
    "Solar System": 9,
    "Planets in binary systems, S-type": 1,
}
EXPECTED_HWC_TYPES = {
    "Jovian": 1_706,
    "Neptunian": 1_401,
    "Superterran": 1_347,
    "Terran": 1_060,
    "Subterran": 69,
    "Miniterran": 9,
    "": 7,
}
DEFAULT_REPORT = (
    DEFAULT_STATE
    / "reports"
    / "evidence_lake_v2"
    / "e4_exoplanet_lifecycle_typed_source_audit.json"
)


def grouped(con: duckdb.DuckDBPyConnection, query: str, path: Path) -> dict[str, int]:
    return {
        str(key or ""): int(count)
        for key, count in con.execute(query, [str(path)]).fetchall()
    }


def deltas(actual: dict[str, int], expected: dict[str, int]) -> dict[str, int]:
    return {
        key: actual.get(key, 0) - expected.get(key, 0)
        for key in sorted(set(actual) | set(expected))
        if actual.get(key, 0) != expected.get(key, 0)
    }


def table_paths(resolved: dict[str, Any]) -> dict[str, Path]:
    return {
        str(row["source_name"]): resolved["typed_path"] / str(row["parquet_path"])
        for row in resolved["typed_manifest"]["tables"]
    }


def audit(state_dir: Path, registry: dict[str, Any]) -> dict[str, Any]:
    sources = {str(row["source_id"]): row for row in registry["sources"]}
    resolved = {source_id: source_input(state_dir, sources[source_id]) for source_id in SOURCE_IDS}
    checks: dict[str, Any] = {}
    summaries: dict[str, Any] = {}
    with duckdb.connect() as con:
        paths_by_source: dict[str, dict[str, Path]] = {}
        for source_id, source in resolved.items():
            manifest = source["typed_manifest"]
            paths = table_paths(source)
            paths_by_source[source_id] = paths
            actual_tables = {
                str(row["source_name"]): (
                    int(row["row_count"]), len(row.get("columns") or [])
                )
                for row in manifest["tables"]
            }
            checks[f"{source_id}.table_shape_deltas"] = {
                name: {
                    "rows": actual_tables.get(name, (0, 0))[0] - expected[0],
                    "fields": actual_tables.get(name, (0, 0))[1] - expected[1],
                }
                for name, expected in EXPECTED_TABLES[source_id].items()
                if actual_tables.get(name) != expected
            }
            checks[f"{source_id}.unexpected_tables"] = sorted(
                set(actual_tables) - set(EXPECTED_TABLES[source_id])
            )
            checks[f"{source_id}.pending_tables"] = sorted(
                str(row["source_name"])
                for row in manifest["tables"]
                if row.get("status") != "typed"
            )
            summaries[source_id] = {
                "raw_snapshot_id": manifest["snapshot_id"],
                "typed_snapshot_id": manifest["typed_snapshot_id"],
                "typed_content_sha256": manifest["content_sha256"],
                "tables": actual_tables,
            }

        eu = paths_by_source[SOURCE_IDS[0]]["exoplanet_eu_catalog_20260721"]
        eu_statuses = grouped(
            con,
            "select planet_status,count(*) from read_parquet(?) group by all",
            eu,
        )
        checks["exoplanet_eu_status_deltas"] = deltas(
            eu_statuses, {"Confirmed": 8_261}
        )
        checks["exoplanet_eu_identity_defects"] = int(
            con.execute(
                "select count(*) filter(where nullif(trim(name),'') is null) + "
                "count(*) - count(distinct name) from read_parquet(?)",
                [str(eu)],
            ).fetchone()[0]
        )
        summaries[SOURCE_IDS[0]]["status_counts"] = eu_statuses

        oec = paths_by_source[SOURCE_IDS[1]]
        kinds = grouped(
            con,
            "select object_kind,count(*) from read_parquet(?) group by all",
            oec["oec_objects"],
        )
        dispositions = grouped(
            con,
            "select list_disposition_raw,count(*) from read_parquet(?) "
            "where list_disposition_raw is not null group by all",
            oec["oec_objects"],
        )
        checks["oec_object_kind_deltas"] = deltas(kinds, EXPECTED_OEC_KINDS)
        checks["oec_disposition_deltas"] = deltas(
            dispositions, EXPECTED_OEC_DISPOSITIONS
        )
        checks["oec_structural_key_defects"] = int(
            con.execute(
                "select count(*)-count(distinct (source_member,source_node_path)) "
                "from read_parquet(?)",
                [str(oec["oec_objects"])],
            ).fetchone()[0]
        )
        checks["oec_name_key_defects"] = int(
            con.execute(
                "select count(*)-count(distinct "
                "(source_member,source_node_path,name_occurrence)) "
                "from read_parquet(?)",
                [str(oec["oec_names"])],
            ).fetchone()[0]
        )
        checks["oec_parameter_key_defects"] = int(
            con.execute(
                "select count(*)-count(distinct "
                "(source_member,source_node_path,parameter_name,parameter_occurrence)) "
                "from read_parquet(?)",
                [str(oec["oec_parameters"])],
            ).fetchone()[0]
        )
        checks["oec_relation_key_defects"] = int(
            con.execute(
                "select count(*)-count(distinct "
                "(source_member,parent_node_path,child_node_path)) from read_parquet(?)",
                [str(oec["oec_relations"])],
            ).fetchone()[0]
        )
        checks["oec_orphan_relation_endpoints"] = int(
            con.execute(
                "select count(*) from read_parquet(?) r left join read_parquet(?) p "
                "on p.source_member=r.source_member and "
                "p.source_node_path=r.parent_node_path left join read_parquet(?) c "
                "on c.source_member=r.source_member and "
                "c.source_node_path=r.child_node_path where p.source_node_path is null "
                "or c.source_node_path is null",
                [
                    str(oec["oec_relations"]),
                    str(oec["oec_objects"]),
                    str(oec["oec_objects"]),
                ],
            ).fetchone()[0]
        )
        summaries[SOURCE_IDS[1]]["object_kinds"] = kinds
        summaries[SOURCE_IDS[1]]["lifecycle_dispositions"] = dispositions

        hwc = paths_by_source[SOURCE_IDS[2]]["hwc_catalog_20260721"]
        hwc_types = grouped(
            con,
            "select coalesce(P_TYPE,''),count(*) from read_parquet(?) group by all",
            hwc,
        )
        checks["hwc_type_deltas"] = deltas(hwc_types, EXPECTED_HWC_TYPES)
        checks["hwc_identity_defects"] = int(
            con.execute(
                "select count(*) filter(where nullif(trim(P_NAME),'') is null or "
                "nullif(trim(S_NAME),'') is null) + count(*)-count(distinct P_NAME) "
                "from read_parquet(?)",
                [str(hwc)],
            ).fetchone()[0]
        )
        checks["hwc_habitability_domain_defects"] = int(
            con.execute(
                "select count(*) from read_parquet(?) where try_cast(P_HABITABLE as integer) "
                "not in (0,1,2) or try_cast(P_ESI as double) not between 0 and 1",
                [str(hwc)],
            ).fetchone()[0]
        )
        summaries[SOURCE_IDS[2]]["type_counts"] = hwc_types

    failed = any(bool(value) for value in checks.values())
    return {
        "schema_version": "spacegate.exoplanet_lifecycle_typed_source_audit.v1",
        "status": "fail" if failed else "pass",
        "checks": checks,
        "summaries": summaries,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = audit(args.state_dir, load_json(args.registry))
    write_json(args.report, report)
    print(
        f"Exoplanet lifecycle typed-source audit {report['status']}: "
        f"sources={len(report['summaries'])}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
