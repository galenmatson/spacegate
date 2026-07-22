#!/usr/bin/env python3
"""Independently verify an E7 clean runtime CORE artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ARTIFACT_ROOT = Path("/mnt/space/spacegate/e7-clean-runtime-core")
EXPECTED_TABLES = {
    "aliases", "build_metadata", "compact_objects", "eclipsing_binaries",
    "extended_object_aliases", "extended_object_identifiers",
    "extended_object_identity_quarantine", "extended_object_search_terms",
    "extended_object_source_reconciliation", "extended_objects",
    "identifier_quarantine", "object_identifiers", "open_cluster_memberships",
    "open_clusters", "planets", "stars", "superstellar_objects",
    "system_search_terms", "systems",
}
REQUIRED_COLUMNS = {
    "systems": {
        "system_id", "stable_object_key", "system_name", "star_count", "planet_count",
        "spectral_classes_json", "spectral_class_mask", "ra_deg", "dec_deg", "dist_ly",
        "x_helio_ly", "y_helio_ly", "z_helio_ly",
    },
    "stars": {
        "star_id", "system_id", "stable_object_key", "star_name", "component",
        "ra_deg", "dec_deg", "dist_ly", "parallax_mas", "teff_k", "spectral_class",
        "object_family", "object_type", "classification_evidence_json",
        "selected_astrometry_lineage_json",
    },
    "planets": {
        "planet_id", "system_id", "star_id", "stable_object_key", "planet_name",
        "orbital_period_days", "semi_major_axis_au", "radius_earth", "mass_earth",
        "planet_status", "is_default_visible", "is_tombstoned",
        "planet_size_mass_class", "planet_insolation_class", "planet_orbit_class",
    },
}


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0] or 0)


def sql_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def verify(build_dir: Path) -> dict[str, Any]:
    manifest = load_object(build_dir / "manifest.json")
    failures: dict[str, Any] = {}
    if manifest.get("schema_version") != "spacegate.e7_clean_runtime_core_manifest.v1":
        failures["manifest_schema"] = manifest.get("schema_version")
    if manifest.get("status") != "pass":
        failures["manifest_status"] = manifest.get("status")
    if manifest.get("stability_databases_opened") != []:
        failures["stability_databases_opened"] = manifest.get("stability_databases_opened")

    product_failures: dict[str, Any] = {}
    for relative, expected in sorted((manifest.get("products") or {}).items()):
        path = build_dir / relative
        if not path.is_file():
            product_failures[relative] = "missing"
            continue
        actual = {"bytes": path.stat().st_size, "sha256": file_sha256(path)}
        if actual != {"bytes": expected.get("bytes"), "sha256": expected.get("sha256")}:
            product_failures[relative] = {"expected": expected, "actual": actual}
    if product_failures:
        failures["products"] = product_failures

    core = build_dir / "core.duckdb"
    con = duckdb.connect(str(core), read_only=True)
    try:
        tables = {
            str(row[0])
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='main' AND table_type='BASE TABLE'"
            ).fetchall()
        }
        if tables != EXPECTED_TABLES:
            failures["table_set"] = {
                "missing": sorted(EXPECTED_TABLES - tables),
                "unexpected": sorted(tables - EXPECTED_TABLES),
            }
        for table, required in REQUIRED_COLUMNS.items():
            columns = {str(row[0]) for row in con.execute(f"DESCRIBE {table}").fetchall()}
            missing = sorted(required - columns)
            if missing:
                failures[f"{table}_columns"] = missing

        checks = {
            "duplicate_system_ids": scalar(con, "SELECT count(*) FROM (SELECT system_id FROM systems GROUP BY 1 HAVING count(*)<>1)"),
            "duplicate_star_ids": scalar(con, "SELECT count(*) FROM (SELECT star_id FROM stars GROUP BY 1 HAVING count(*)<>1)"),
            "duplicate_planet_ids": scalar(con, "SELECT count(*) FROM (SELECT planet_id FROM planets GROUP BY 1 HAVING count(*)<>1)"),
            "duplicate_compact_keys": scalar(con, "SELECT count(*) FROM (SELECT stable_object_key FROM compact_objects GROUP BY 1 HAVING count(*)<>1)"),
            "orphan_stars": scalar(con, "SELECT count(*) FROM stars s LEFT JOIN systems y USING(system_id) WHERE y.system_id IS NULL"),
            "bound_planet_orphans": scalar(con, "SELECT count(*) FROM planets p LEFT JOIN systems y USING(system_id) WHERE p.system_id IS NOT NULL AND y.system_id IS NULL"),
            "invalid_planet_status": scalar(con, "SELECT count(*) FROM planets WHERE planet_status NOT IN ('confirmed','candidate','controversial','retracted')"),
            "negative_planet_visible": scalar(con, "SELECT count(*) FROM planets WHERE planet_status='retracted' AND (is_default_visible OR NOT is_tombstoned)"),
            "invalid_planet_class": scalar(con, "SELECT count(*) FROM planets WHERE planet_size_mass_class NOT IN ('terrestrial','jupiter') OR planet_insolation_class NOT IN ('hot','temperate','cold')"),
            "cluster_membership_containment_column": scalar(con, "SELECT count(*) FROM information_schema.columns WHERE table_name='open_cluster_memberships' AND column_name LIKE '%containment%'"),
            "metadata_stability_opened": scalar(con, "SELECT count(*) FROM build_metadata WHERE key='stability_database_opened' AND value<>'0'"),
            "manifest_count_delta": sum(
                abs(
                    scalar(con, f"SELECT count(*) FROM {table}")
                    - int((manifest.get("verification", {}).get("counts") or {}).get(table, -1))
                )
                for table in EXPECTED_TABLES
            ),
        }
        nonzero = {key: value for key, value in checks.items() if value}
        if nonzero:
            failures["invariants"] = nonzero
        counts = {table: scalar(con, f"SELECT count(*) FROM {table}") for table in sorted(EXPECTED_TABLES)}
        status_counts = dict(con.execute("SELECT planet_status,count(*) FROM planets GROUP BY 1 ORDER BY 1").fetchall())
        class_counts = dict(con.execute("SELECT object_type,count(*) FROM compact_objects GROUP BY 1 ORDER BY 1").fetchall())
    finally:
        con.close()

    hierarchy = build_dir / "canonical_hierarchy.duckdb"
    hcon = duckdb.connect(str(hierarchy), read_only=True)
    try:
        hierarchy_checks = {
            "missing_nodes": int(scalar(hcon, "SELECT count(*)=0 FROM hierarchy_nodes")),
            "missing_edges": int(scalar(hcon, "SELECT count(*)=0 FROM hierarchy_edges")),
            "stability_opened": scalar(hcon, "SELECT count(*) FROM build_metadata WHERE key='stability_database_opened' AND value<>'0'"),
            "wrong_build_id": scalar(hcon, f"SELECT count(*) FROM build_metadata WHERE key='build_id' AND value<>{sql_literal(manifest.get('build_id'))}"),
        }
        hierarchy_nonzero = {key: value for key, value in hierarchy_checks.items() if value}
        if hierarchy_nonzero:
            failures["hierarchy"] = hierarchy_nonzero
        hierarchy_counts = {
            "nodes": scalar(hcon, "SELECT count(*) FROM hierarchy_nodes"),
            "edges": scalar(hcon, "SELECT count(*) FROM hierarchy_edges"),
        }
    finally:
        hcon.close()

    return {
        "schema_version": "spacegate.e7_clean_runtime_core_verification.v1",
        "build_id": manifest.get("build_id"),
        "status": "pass" if not failures else "fail",
        "counts": counts,
        "planet_status_counts": status_counts,
        "compact_object_type_counts": class_counts,
        "hierarchy_counts": hierarchy_counts,
        "failing_checks": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = verify(args.artifact_root.resolve() / args.build_id)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
