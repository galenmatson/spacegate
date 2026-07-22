#!/usr/bin/env python3
"""Independently audit an Evidence Lake v2 E6 shadow foundation build."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

import compile_e6_shadow_build as compiler


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_POLICY = ROOT / "config/evidence_lake/e6_shadow_build.json"


def scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def audit_build(
    *, state: Path, build_id: str, policy_path: Path, report_path: Path | None = None
) -> dict[str, Any]:
    state = state.resolve()
    policy = compiler.load_json(policy_path.resolve())
    compiler.validate_policy(policy)
    build_dir = state / "out" / build_id
    manifest_path = build_dir / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError(f"missing E6 manifest: {manifest_path}")
    manifest = compiler.load_json(manifest_path)
    if manifest.get("build_id") != build_id:
        raise ValueError("E6 manifest build identity mismatch")
    if (manifest.get("report") or {}).get("status") != "pass":
        raise ValueError("E6 compiler report is not passing")

    failures: dict[str, int] = {}
    observed_hashes: dict[str, dict[str, Any]] = {}
    for filename, declared in manifest["report"]["product_files"].items():
        path = build_dir / filename
        observed = {
            "bytes": path.stat().st_size,
            "sha256": compiler.file_sha256(path),
        }
        observed_hashes[filename] = observed
        failures[f"product_integrity:{filename}"] = int(observed != declared)

    base_id = str(policy["stability_reference_build_id"])
    base_dir = state / "out" / base_id
    core = build_dir / "core.duckdb"
    arm = build_dir / "arm.duckdb"
    hierarchy = build_dir / "canonical_hierarchy.duckdb"
    disc = build_dir / "disc.duckdb"
    con = duckdb.connect()
    try:
        con.execute(f"ATTACH {compiler.sql_literal(str(core))} AS core (READ_ONLY)")
        con.execute(
            f"ATTACH {compiler.sql_literal(str(base_dir / 'core.duckdb'))} AS base (READ_ONLY)"
        )
        con.execute(f"ATTACH {compiler.sql_literal(str(arm))} AS arm (READ_ONLY)")
        con.execute(f"ATTACH {compiler.sql_literal(str(hierarchy))} AS hierarchy (READ_ONLY)")
        con.execute(f"ATTACH {compiler.sql_literal(str(disc))} AS disc (READ_ONLY)")
        con.execute(
            f"ATTACH {compiler.sql_literal(str(base_dir / 'canonical_hierarchy.duckdb'))} "
            "AS base_hierarchy (READ_ONLY)"
        )

        inventory: dict[str, dict[str, int]] = {}
        for table in ("systems", "stars", "planets"):
            before = scalar(con, f"SELECT count(*) FROM base.{compiler.sql_identifier(table)}")
            after = scalar(con, f"SELECT count(*) FROM core.{compiler.sql_identifier(table)}")
            inventory[table] = {"before": before, "after": after, "delta": after - before}
            failures[f"inventory_delta:{table}"] = abs(after - before)
            key = "stable_object_key"
            failures[f"duplicate_key:{table}"] = scalar(
                con,
                f"SELECT count(*)-count(DISTINCT {key}) FROM core.{compiler.sql_identifier(table)}",
            )
            failures[f"missing_key:{table}"] = scalar(
                con,
                f"SELECT count(*) FROM base.{compiler.sql_identifier(table)} b "
                f"WHERE NOT EXISTS (SELECT 1 FROM core.{compiler.sql_identifier(table)} s "
                f"WHERE s.{key}=b.{key})",
            )

        hierarchy_counts: dict[str, dict[str, int]] = {}
        for table in ("hierarchy_nodes", "hierarchy_edges"):
            before = scalar(
                con, f"SELECT count(*) FROM base_hierarchy.{compiler.sql_identifier(table)}"
            )
            after = scalar(con, f"SELECT count(*) FROM hierarchy.{compiler.sql_identifier(table)}")
            hierarchy_counts[table] = {"before": before, "after": after}
            failures[f"hierarchy_count_delta:{table}"] = abs(after - before)
            failures[f"hierarchy_missing_rows:{table}"] = scalar(
                con,
                f"SELECT count(*) FROM (SELECT * FROM base_hierarchy.{compiler.sql_identifier(table)} "
                f"EXCEPT SELECT * FROM hierarchy.{compiler.sql_identifier(table)})",
            )
            failures[f"hierarchy_added_rows:{table}"] = scalar(
                con,
                f"SELECT count(*) FROM (SELECT * FROM hierarchy.{compiler.sql_identifier(table)} "
                f"EXCEPT SELECT * FROM base_hierarchy.{compiler.sql_identifier(table)})",
            )

        quantity_tables: dict[tuple[str, str], str] = {}
        for group in policy["star_projection_groups"]:
            for quantity in group["quantities"]:
                quantity_tables[("star", str(quantity))] = str(group["table"])
        for quantity in policy["planet_projection"]["quantities"]:
            quantity_tables[("planet", str(quantity))] = str(
                policy["planet_projection"]["table"]
            )

        projection_lineage: dict[str, dict[str, int]] = {}
        for group in [*policy["star_projection_groups"], policy["planet_projection"]]:
            table = str(group["table"])
            expressions = []
            for quantity in group["quantities"]:
                quantity = str(quantity)
                value = compiler.sql_identifier(quantity)
                fact = compiler.sql_identifier(f"{quantity}_fact_id")
                expressions.append(
                    f"count(*) FILTER (WHERE ({value} IS NULL) <> ({fact} IS NULL)) "
                    f"AS {compiler.sql_identifier(quantity)}"
                )
            row = con.execute(
                f"SELECT {','.join(expressions)} FROM arm.{compiler.sql_identifier(table)}"
            ).fetchone()
            projection_lineage[table] = {
                str(quantity): int(row[index])
                for index, quantity in enumerate(group["quantities"])
            }
            for quantity, count in projection_lineage[table].items():
                failures[f"value_fact_id_mismatch:{table}:{quantity}"] = count
            id_column = (
                "planet_id"
                if table == str(policy["planet_projection"]["table"])
                else "star_id"
            )
            failures[f"duplicate_projection_id:{table}"] = scalar(
                con,
                f"SELECT count(*)-count(DISTINCT {id_column}) "
                f"FROM arm.{compiler.sql_identifier(table)}",
            )

        core_checks: list[dict[str, Any]] = []
        for mapping in policy["core_scalar_updates"]:
            object_type = str(mapping["object_type"])
            quantity = str(mapping["quantity"])
            column = str(mapping["column"])
            table = "stars" if object_type == "star" else "planets"
            id_column = "star_id" if object_type == "star" else "planet_id"
            projection = quantity_tables[(object_type, quantity)]
            mismatch = scalar(
                con,
                f"""
                SELECT count(*) FROM core.{compiler.sql_identifier(table)} c
                JOIN arm.{compiler.sql_identifier(projection)} p USING ({id_column})
                WHERE p.{compiler.sql_identifier(quantity)} IS NOT NULL
                  AND (c.{compiler.sql_identifier(column)} IS NULL OR
                       abs(c.{compiler.sql_identifier(column)}-
                           p.{compiler.sql_identifier(quantity)})>1e-12)
                """,
            )
            missing_fact = scalar(
                con,
                f"SELECT count(*) FROM arm.{compiler.sql_identifier(projection)} "
                f"WHERE {compiler.sql_identifier(quantity)} IS NOT NULL AND "
                f"{compiler.sql_identifier(quantity + '_fact_id')} IS NULL",
            )
            core_checks.append(
                {
                    "object_type": object_type,
                    "quantity": quantity,
                    "projection": projection,
                    "value_mismatches": mismatch,
                    "missing_fact_ids": missing_fact,
                }
            )
            failures[f"core_value_mismatch:{object_type}:{quantity}"] = mismatch
            failures[f"core_fact_id_missing:{object_type}:{quantity}"] = missing_fact

        registry_rows = con.execute(
            "SELECT family,build_id,artifact_path,manifest_sha256,database_sha256 "
            "FROM arm.e6_evidence_artifact_registry ORDER BY family"
        ).fetchall()
        failures["artifact_registry_row_delta"] = abs(
            len(registry_rows) - len(policy["selected_artifacts"])
        )
        expected_artifacts = {row["family"]: row for row in policy["selected_artifacts"]}
        for family, artifact_id, relpath, manifest_sha, database_sha in registry_rows:
            expected = expected_artifacts.get(str(family))
            failures[f"artifact_registry_policy:{family}"] = int(
                expected is None or str(expected["build_id"]) != str(artifact_id)
            )
            artifact_dir = state / str(relpath)
            failures[f"artifact_registry_manifest:{family}"] = int(
                compiler.file_sha256(artifact_dir / "manifest.json") != manifest_sha
            )
            failures[f"artifact_registry_database:{family}"] = int(
                compiler.file_sha256(artifact_dir / str(expected["database"])) != database_sha
            )

        aliases = {
            "official_rows": scalar(
                con,
                "SELECT count(*) FROM core.aliases WHERE source_catalog='naming.iau_wgsn' "
                "AND source_version='evidence_lake_v2'",
            ),
            "primary_rows": scalar(
                con,
                "SELECT count(*) FROM core.aliases WHERE source_catalog='naming.iau_wgsn' "
                "AND source_version='evidence_lake_v2' AND is_primary",
            ),
            "duplicate_target_aliases": scalar(
                con,
                "SELECT count(*) FROM (SELECT target_type,target_id,alias_norm,count(*) n "
                "FROM core.aliases GROUP BY ALL HAVING n>1)",
            ),
        }
        failures["official_alias_primary_rows"] = aliases["primary_rows"]
        failures["duplicate_target_aliases"] = aliases["duplicate_target_aliases"]

        lifecycle_inventory_mutations = scalar(
            con,
            "SELECT coalesce(sum(canonical_inventory_mutation),0) "
            "FROM arm.e6_planet_evidence_planet_lifecycle_projection",
        ) if "canonical_inventory_mutation" in {
            str(row[0])
            for row in con.execute(
                "DESCRIBE arm.e6_planet_evidence_planet_lifecycle_projection"
            ).fetchall()
        } else 0
        failures["planet_lifecycle_inventory_mutations"] = lifecycle_inventory_mutations

        coolness_policy = policy["coolness_profile"]
        coolness = {
            "rows": scalar(con, "SELECT count(*) FROM disc.coolness_scores"),
            "expected_rows": scalar(con, "SELECT count(*) FROM core.systems"),
            "duplicate_systems": scalar(
                con,
                "SELECT count(*)-count(DISTINCT system_id) FROM disc.coolness_scores",
            ),
            "wrong_build": scalar(
                con,
                f"SELECT count(*) FROM disc.coolness_scores WHERE build_id<>"
                f"{compiler.sql_literal(build_id)}",
            ),
            "wrong_profile": scalar(
                con,
                "SELECT count(*) FROM disc.coolness_scores WHERE "
                f"profile_id<>{compiler.sql_literal(coolness_policy['profile_id'])} OR "
                f"profile_version<>{compiler.sql_literal(coolness_policy['profile_version'])}",
            ),
        }
        disc_metadata = dict(
            con.execute(
                "SELECT key,value FROM disc.build_metadata WHERE key LIKE 'e6_coolness_%'"
            ).fetchall()
        )
        failures["coolness_inventory_delta"] = abs(
            coolness["rows"] - coolness["expected_rows"]
        )
        failures["coolness_duplicate_systems"] = coolness["duplicate_systems"]
        failures["coolness_wrong_build"] = coolness["wrong_build"]
        failures["coolness_wrong_profile"] = coolness["wrong_profile"]
        failures["coolness_manifest_status"] = int(
            (manifest["report"].get("coolness_report") or {}).get("status") != "pass"
        )
        failures["coolness_metadata_profile_id"] = int(
            disc_metadata.get("e6_coolness_profile_id") != coolness_policy["profile_id"]
        )
        failures["coolness_metadata_profile_version"] = int(
            disc_metadata.get("e6_coolness_profile_version")
            != coolness_policy["profile_version"]
        )
        failures["coolness_metadata_profile_hash"] = int(
            disc_metadata.get("e6_coolness_profile_hash")
            != coolness_policy["profile_hash"]
        )

        selected_consumer_report = manifest["report"].get("selected_consumer_report") or {}
        stellar_leaf_report = manifest["report"].get("stellar_leaf_report") or {}
        star_count = inventory["stars"]["after"]
        consumer_counts = {
            "parameter_rows": scalar(
                con, "SELECT count(*) FROM arm.e6_selected_stellar_parameters"
            ),
            "classification_rows": scalar(
                con,
                "SELECT count(*) FROM arm.e6_selected_stellar_display_classifications",
            ),
            "leaf_rows": scalar(
                con, "SELECT count(*) FROM arm.stellar_leaf_display_classifications"
            ),
        }
        failures["selected_consumer_manifest_status"] = int(
            selected_consumer_report.get("status") != "pass"
        )
        failures["stellar_leaf_manifest_status"] = int(
            stellar_leaf_report.get("status") != "pass"
        )
        failures["selected_parameter_inventory_delta"] = abs(
            consumer_counts["parameter_rows"] - star_count
        )
        failures["selected_classification_inventory_delta"] = abs(
            consumer_counts["classification_rows"] - star_count
        )
        failures["selected_parameter_manifest_delta"] = abs(
            consumer_counts["parameter_rows"]
            - int(selected_consumer_report.get("stellar_parameter_rows", -1))
        )
        failures["selected_classification_manifest_delta"] = abs(
            consumer_counts["classification_rows"]
            - int(selected_consumer_report.get("stellar_classification_rows", -1))
        )
        failures["stellar_leaf_manifest_delta"] = abs(
            consumer_counts["leaf_rows"] - int(stellar_leaf_report.get("rows", -1))
        )
        failures["duplicate_selected_classification"] = scalar(
            con,
            "SELECT count(*)-count(DISTINCT star_id) "
            "FROM arm.e6_selected_stellar_display_classifications",
        )
        failures["invalid_selected_classification"] = scalar(
            con,
            "SELECT count(*) FROM arm.e6_selected_stellar_display_classifications "
            "WHERE classification_value NOT IN "
            "('O','B','A','F','G','K','M','L','T','Y','WR','WD','NS','PULSAR',"
            "'MAGNETAR','BLACK HOLE','UNKNOWN')",
        )
        failures["missing_selected_classification_lineage"] = scalar(
            con,
            "SELECT count(*) FROM arm.e6_selected_stellar_display_classifications "
            "WHERE classification_value<>'UNKNOWN' AND lineage_id IS NULL",
        )
        failures["duplicate_stellar_leaf"] = scalar(
            con,
            "SELECT count(*)-count(DISTINCT hierarchy_node_key) "
            "FROM arm.stellar_leaf_display_classifications",
        )
        failures["invalid_stellar_leaf"] = scalar(
            con,
            "SELECT count(*) FROM arm.stellar_leaf_display_classifications "
            "WHERE classification_value NOT IN "
            "('O','B','A','F','G','K','M','L','T','Y','WR','WD','NS','PULSAR',"
            "'MAGNETAR','BLACK HOLE','UNKNOWN') "
            "OR classification_status NOT IN ('source','derived','assumed','missing')",
        )
    finally:
        con.close()

    failing = {key: value for key, value in failures.items() if value != 0}
    report = {
        "schema_version": "spacegate.e6_shadow_build_audit.v1",
        "status": "pass" if not failing else "fail",
        "build_id": build_id,
        "manifest_sha256": compiler.file_sha256(manifest_path),
        "product_files": observed_hashes,
        "inventory": inventory,
        "hierarchy": hierarchy_counts,
        "projection_lineage": projection_lineage,
        "core_checks": core_checks,
        "official_aliases": aliases,
        "selected_consumers": consumer_counts,
        "coolness": {**coolness, "metadata": disc_metadata},
        "checks": failures,
        "failing_checks": failing,
    }
    if report_path:
        compiler.atomic_json(report_path, report)
    if failing:
        raise ValueError(f"E6 shadow audit failed: {failing}")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = audit_build(
        state=args.state_dir,
        build_id=args.build_id,
        policy_path=args.policy,
        report_path=args.report,
    )
    print(f"E6 shadow audit {report['build_id']} {report['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
