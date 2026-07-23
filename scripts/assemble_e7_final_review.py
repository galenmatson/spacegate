#!/usr/bin/env python3
"""Assemble the E7 pre-promotion scientific review from authoritative reports."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config/evidence_lake/e7_final_review.json"
INVENTORY_TABLES = (
    "systems",
    "stars",
    "planets",
    "aliases",
    "extended_objects",
    "system_search_terms",
)
CANONICAL_INVENTORY_TABLES = INVENTORY_TABLES[:-1]


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0] or 0)


def validate_config(config: dict[str, Any]) -> None:
    if config.get("schema_version") != "spacegate.e7_final_review_config.v1":
        raise ValueError("unsupported final-review config")
    expected_candidate = {
        "public_build_id", "clean_science", "core", "arm", "disc", "bundle"
    }
    if set(config.get("candidate") or {}) != expected_candidate:
        raise ValueError("candidate identity contract is incomplete")
    expected_reports = {
        "classification_ab", "planet_cutover_ab", "map_verification",
        "map_reproduction", "scene_cold", "scene_warm", "browser",
        "api_integration", "api_known_systems", "science_reproduction",
        "core_reproduction", "arm_reproduction", "disc_reproduction",
    }
    if set(config.get("reports") or {}) != expected_reports:
        raise ValueError("final-review report contract is incomplete")
    for key in ("reference_build_dir", "candidate_build_dir", "bundle_manifest", "arm_manifest"):
        if not Path(str(config.get(key) or "")).is_absolute():
            raise ValueError(f"{key} must be absolute")
    if not all(Path(path).is_absolute() for path in config["reports"].values()):
        raise ValueError("report paths must be absolute")


def inventory_counts(reference: Path, candidate: Path) -> dict[str, dict[str, int]]:
    con = duckdb.connect(config={"threads": "8", "memory_limit": "8GB"})
    try:
        con.execute(f"ATTACH '{str(reference / 'core.duckdb').replace(chr(39), chr(39) * 2)}' AS ref (READ_ONLY)")
        con.execute(f"ATTACH '{str(candidate / 'core.duckdb').replace(chr(39), chr(39) * 2)}' AS cand (READ_ONLY)")
        return {
            table: {
                "reference": scalar(con, f"SELECT count(*) FROM ref.{table}"),
                "candidate": scalar(con, f"SELECT count(*) FROM cand.{table}"),
            }
            for table in INVENTORY_TABLES
        }
    finally:
        con.close()


def assemble(config_path: Path) -> dict[str, Any]:
    config = load_object(config_path)
    validate_config(config)
    candidate = config["candidate"]
    reference_dir = Path(config["reference_build_dir"])
    candidate_dir = Path(config["candidate_build_dir"])
    bundle_manifest = load_object(Path(config["bundle_manifest"]))
    arm_manifest = load_object(Path(config["arm_manifest"]))
    report_paths = {name: Path(path) for name, path in config["reports"].items()}
    reports = {
        name: load_object(path)
        for name, path in report_paths.items()
        if name not in {"api_integration", "api_known_systems"}
    }
    api_integration = report_paths["api_integration"].read_text(encoding="utf-8")
    api_known = report_paths["api_known_systems"].read_text(encoding="utf-8")
    inventory = inventory_counts(reference_dir, candidate_dir)

    metadata_con = duckdb.connect(str(candidate_dir / "core.duckdb"), read_only=True)
    try:
        metadata = dict(metadata_con.execute("SELECT key,value FROM build_metadata").fetchall())
    finally:
        metadata_con.close()

    identity_checks = {
        "public_build_id": metadata.get("build_id") == candidate["public_build_id"],
        "bootstrap_bundle": metadata.get("bootstrap_source_build_id") == candidate["bundle"],
        "bundle_build_id": bundle_manifest.get("build_id") == candidate["bundle"],
        "bundle_core": (bundle_manifest.get("inputs") or {}).get("core", {}).get("build_id") == candidate["core"],
        "bundle_arm": (bundle_manifest.get("inputs") or {}).get("arm", {}).get("build_id") == candidate["arm"],
        "bundle_disc": (bundle_manifest.get("inputs") or {}).get("disc", {}).get("build_id") == candidate["disc"],
        "arm_build_id": arm_manifest.get("build_id") == candidate["arm"],
        "arm_science": (arm_manifest.get("inputs") or {}).get("clean_science", {}).get("build_id") == candidate["clean_science"],
        "selected_facts_only": metadata.get("scientific_values_from_selected_facts_only") == "1",
        "stability_database_closed": metadata.get("stability_database_opened") == "0",
    }
    canonical_inventory_stable = all(
        inventory[table]["candidate"] == inventory[table]["reference"]
        for table in CANONICAL_INVENTORY_TABLES
    )
    search_vocabulary_not_lost = (
        inventory["system_search_terms"]["candidate"]
        >= inventory["system_search_terms"]["reference"]
    )
    classification = reports["classification_ab"]
    planet = reports["planet_cutover_ab"]
    map_verification = reports["map_verification"]
    map_reproduction = reports["map_reproduction"]
    scene_cold = reports["scene_cold"]
    scene_warm = reports["scene_warm"]
    browser = reports["browser"]
    science_reproduction = reports["science_reproduction"]
    core_reproduction = reports["core_reproduction"]
    arm_reproduction = reports["arm_reproduction"]
    disc_reproduction = reports["disc_reproduction"]

    gates = {
        "candidate_identity_pass": all(identity_checks.values()),
        "canonical_inventory_stable": canonical_inventory_stable,
        "search_vocabulary_not_lost": search_vocabulary_not_lost,
        "classification_ab_pass": (
            classification.get("status") == "pass"
            and classification.get("build_id") == candidate["arm"]
            and not classification.get("failing_checks")
        ),
        "planet_cutover_ab_pass": (
            planet.get("status") == "pass"
            and planet.get("candidate_build_id") == candidate["public_build_id"]
            and not planet.get("failing_checks")
        ),
        "shared_selected_fact_consumer_pass": (
            planet.get("checks", {}).get("arm_projection_left_only") == 0
            and planet.get("checks", {}).get("arm_projection_right_only") == 0
            and planet.get("checks", {}).get("public_selected_value_mismatches") == 0
            and planet.get("checks", {}).get("missing_luminosity_status") == 0
            and planet.get("checks", {}).get("luminosity_derivation_count_mismatch") == 0
        ),
        "science_reproduction_pass": (
            science_reproduction.get("status") == "pass"
            and science_reproduction.get("build_id") == candidate["clean_science"]
            and science_reproduction.get("scratch_removed") is True
            and not science_reproduction.get("differing_byte_exact_products")
        ),
        "core_reproduction_pass": (
            core_reproduction.get("status") == "pass"
            and core_reproduction.get("build_id") == candidate["core"]
            and core_reproduction.get("scratch_removed") is True
            and not core_reproduction.get("differing_files")
            and core_reproduction.get("hierarchy_logical_match") is True
        ),
        "arm_reproduction_pass": (
            arm_reproduction.get("status") == "pass"
            and arm_reproduction.get("build_id") == candidate["arm"]
            and not arm_reproduction.get("differing_tables")
            and not arm_reproduction.get("failing_checks")
        ),
        "disc_reproduction_pass": (
            disc_reproduction.get("status") == "pass"
            and disc_reproduction.get("build_id") == candidate["disc"]
            and disc_reproduction.get("scratch_removed") is True
            and not disc_reproduction.get("field_differences")
            and all(
                value is True
                for value in (disc_reproduction.get("canonical_parquet_match") or {}).values()
            )
        ),
        "map_pass": (
            map_verification.get("passed") is True
            and map_verification.get("build_id") == candidate["public_build_id"]
            and all(item.get("passed") is True for item in map_verification.get("results") or [])
        ),
        "map_reproduction_pass": map_reproduction.get("passed") is True,
        "scene_pass": (
            scene_cold.get("ok") is True
            and scene_cold.get("build_id") == candidate["public_build_id"]
            and scene_cold.get("requested") == scene_cold.get("generated") == 24
            and scene_cold.get("failed") == 0
            and scene_warm.get("ok") is True
            and scene_warm.get("build_id") == candidate["public_build_id"]
            and scene_warm.get("requested") == scene_warm.get("reused") == 24
            and scene_warm.get("failed") == 0
        ),
        "api_pass": (
            "Integration test passed." in api_integration
            and "Known-system API benchmark passed:" in api_known
        ),
        "browser_pass": (
            not browser.get("errors")
            and browser.get("stats", {}).get("expected") == 12
            and browser.get("stats", {}).get("skipped") == 4
            and browser.get("stats", {}).get("unexpected") == 0
            and browser.get("stats", {}).get("flaky") == 0
        ),
        "local_promotion": False,
        "rollback_drill": False,
        "antiproton_deployed": False,
    }
    prepromotion_gate_names = tuple(
        name for name in gates
        if name not in {"local_promotion", "rollback_drill", "antiproton_deployed"}
    )
    source_reports = {
        name: {"path": str(path), "sha256": file_sha256(path)}
        for name, path in report_paths.items()
    }
    failing = {
        name: value for name, value in gates.items()
        if name in prepromotion_gate_names and value is not True
    }
    return {
        "schema_version": "spacegate.e7_final_scientific_ab.v2",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "pass" if not failing else "fail",
        "operator_acceptance": "pending",
        "candidate": candidate,
        "reference_build_id": reference_dir.name,
        "identity_checks": identity_checks,
        "inventory": inventory,
        "classification_summary": {
            "status": classification.get("status"),
            "build_id": classification.get("build_id"),
            "checks": classification.get("checks"),
            "leaf_rows": classification.get("leaf_rows"),
        },
        "planet_cutover_summary": {
            "status": planet.get("status"),
            "quantity_deltas": planet.get("quantity_deltas"),
            "category_deltas": planet.get("category_deltas"),
            "luminosity_lineage": planet.get("luminosity_lineage"),
            "checks": planet.get("checks"),
        },
        "map_summary": {
            "verification": {
                "build_id": map_verification.get("build_id"),
                "passed": map_verification.get("passed"),
                "results": map_verification.get("results"),
            },
            "reproduction": {
                "passed": map_reproduction.get("passed"),
                "timestamp_fields_ignored": map_reproduction.get("timestamp_fields_ignored"),
                "radius_results": map_reproduction.get("radius_results"),
            },
        },
        "reproduction_summary": {
            "science": {
                "status": science_reproduction.get("status"),
                "scratch_removed": science_reproduction.get("scratch_removed"),
                "differing_byte_exact_products": science_reproduction.get("differing_byte_exact_products"),
            },
            "core": {
                "status": core_reproduction.get("status"),
                "scratch_removed": core_reproduction.get("scratch_removed"),
                "differing_files": core_reproduction.get("differing_files"),
                "hierarchy_logical_match": core_reproduction.get("hierarchy_logical_match"),
            },
            "arm": {
                "status": arm_reproduction.get("status"),
                "differing_tables": arm_reproduction.get("differing_tables"),
                "failing_checks": arm_reproduction.get("failing_checks"),
            },
            "disc": {
                "status": disc_reproduction.get("status"),
                "scratch_removed": disc_reproduction.get("scratch_removed"),
                "canonical_parquet_match": disc_reproduction.get("canonical_parquet_match"),
            },
        },
        "browser_summary": browser.get("stats"),
        "gates": gates,
        "failing_prepromotion_gates": failing,
        "source_reports": source_reports,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    result = assemble(args.config.resolve())
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    args.report.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.report.with_name(f".{args.report.name}.{os.getpid()}.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    os.replace(temporary, args.report)
    print(rendered, end="")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
