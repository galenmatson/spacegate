#!/usr/bin/env python3
"""Stage a parity-safe public-read artifact with selected hierarchy-leaf badges."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

import build_public_read_models as builder


UPGRADER_VERSION = "public_read_search_parity_upgrader_v1"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def create_overlay_table(target: sqlite3.Connection) -> None:
    target.execute(
        """
        CREATE TABLE IF NOT EXISTS stellar_badge_overlays (
          system_id INTEGER NOT NULL,
          badge_order INTEGER NOT NULL,
          hierarchy_node_key TEXT,
          leaf_component_key TEXT,
          evidence_component_key TEXT,
          star_id_text TEXT,
          stable_object_key TEXT,
          display_name TEXT,
          catalog_component_label TEXT,
          classification_value TEXT NOT NULL,
          classification_status TEXT NOT NULL,
          evidence_basis TEXT,
          selected_fact_id TEXT,
          source_catalog TEXT,
          source_version TEXT,
          PRIMARY KEY (system_id, badge_order)
        ) WITHOUT ROWID
        """
    )


def refresh_modified_logical_hashes(
    target: sqlite3.Connection,
    manifest: dict[str, Any],
) -> dict[str, str]:
    logical_hashes = dict(manifest.get("logical_hashes") or {})
    logical_hashes["metadata"] = builder.logical_digest(
        target,
        "metadata",
        ["key", "value"],
    )
    logical_hashes["systems"] = builder.logical_digest(
        target,
        "systems",
        [
            "system_id",
            "stable_object_key",
            "system_name_norm",
            "star_count",
            "planet_count",
        ],
    )
    logical_hashes["stellar_badge_overlays"] = builder.logical_digest(
        target,
        "stellar_badge_overlays",
        [
            "system_id",
            "badge_order",
            "leaf_component_key",
            "classification_value",
        ],
    )
    return logical_hashes


def canonicalize_database(database: Path) -> None:
    canonical = database.with_name(
        f".{database.name}.canonical.tmp.{os.getpid()}"
    )
    canonical.unlink(missing_ok=True)
    target = sqlite3.connect(str(database), timeout=60)
    try:
        target.execute("PRAGMA journal_mode=DELETE")
        target.execute("PRAGMA synchronous=FULL")
        target.execute("ANALYZE")
        target.commit()
        target.execute("VACUUM INTO ?", (str(canonical),))
    finally:
        target.close()
    if not canonical.is_file():
        raise RuntimeError("SQLite canonicalization produced no artifact")
    os.replace(canonical, database)


def refresh_manifest_accounting(
    manifest: dict[str, Any],
    *,
    singleton_seed_count: int,
    overlay_rows: int,
) -> None:
    manifest["counts"]["stellar_badge_overlays"] = overlay_rows
    manifest["counts"]["singleton_scene_seeds"] = singleton_seed_count
    verification = dict(manifest.get("verification") or {})
    verification_counts = dict(verification.get("counts") or {})
    verification_counts["singleton_scene_seeds"] = singleton_seed_count
    verification_counts["stellar_badge_overlays"] = overlay_rows
    verification["counts"] = verification_counts
    expected_counts = dict(verification.get("expected_counts") or {})
    expected_counts["singleton_scene_seeds"] = singleton_seed_count
    expected_counts["stellar_badge_overlays"] = overlay_rows
    verification["expected_counts"] = expected_counts
    verification["count_mismatches"] = {
        key: value
        for key, value in (verification.get("count_mismatches") or {}).items()
        if key not in {"singleton_scene_seeds", "stellar_badge_overlays"}
    }
    manifest["verification"] = verification


def refresh_existing(args: argparse.Namespace) -> dict[str, Any]:
    staging_dir = Path(args.staging_dir).resolve(strict=True)
    database = staging_dir / "public_read.sqlite"
    manifest_path = staging_dir / "manifest.json"
    policy = builder.load_json(builder.DEFAULT_POLICY)
    for path in (database, manifest_path):
        if not path.is_file():
            raise SystemExit(f"Missing required staged artifact: {path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    target = sqlite3.connect(str(database), timeout=60)
    target.row_factory = sqlite3.Row
    target.execute("PRAGMA journal_mode=DELETE")
    target.execute("PRAGMA synchronous=FULL")
    if (
        target.execute(
            """
            SELECT 1 FROM sqlite_schema
            WHERE type='table' AND name='stellar_badge_overlays'
            """
        ).fetchone()
        is None
    ):
        target.close()
        raise SystemExit("Staged artifact lacks stellar_badge_overlays")
    target.execute(
        "INSERT OR REPLACE INTO metadata(key,value) VALUES (?,?)",
        (
            "stellar_badge_overlay_schema_version",
            policy["stellar_badge_overlay_schema_version"],
        ),
    )
    target.commit()
    singleton_seed_count = int(
        target.execute("SELECT COUNT(*) FROM singleton_scene_seeds").fetchone()[0]
    )
    overlay_rows = int(
        target.execute("SELECT COUNT(*) FROM stellar_badge_overlays").fetchone()[0]
    )
    target.close()
    canonicalize_database(database)
    target = sqlite3.connect(str(database), timeout=60)
    target.row_factory = sqlite3.Row
    logical_hashes = refresh_modified_logical_hashes(target, manifest)
    integrity = str(target.execute("PRAGMA integrity_check").fetchone()[0])
    target.close()
    if integrity != "ok":
        raise RuntimeError(f"Staged public-read integrity failed: {integrity}")

    manifest["stellar_badge_overlay_schema_version"] = policy[
        "stellar_badge_overlay_schema_version"
    ]
    refresh_manifest_accounting(
        manifest,
        singleton_seed_count=singleton_seed_count,
        overlay_rows=overlay_rows,
    )
    manifest["logical_hashes"] = logical_hashes
    manifest.setdefault("artifact", {})["bytes"] = database.stat().st_size
    manifest["artifact"]["sha256"] = builder.sha256_file(database)
    manifest["artifact"]["hash_status"] = "verified"
    atomic_json(manifest_path, manifest)
    report = {
        "schema_version": "spacegate.public_read_search_parity_refresh.v1",
        "status": "pass",
        "build_id": manifest.get("build_id"),
        "staging_dir": str(staging_dir),
        "artifact": manifest["artifact"],
        "logical_hashes": logical_hashes,
        "sqlite_integrity": integrity,
        "canonicalization": "analyze_vacuum_into_v1",
        "generated_at_utc": utc_now(),
    }
    if args.report_dir:
        report_dir = Path(args.report_dir).resolve()
        report_dir.mkdir(parents=True, exist_ok=True)
        atomic_json(report_dir / "search_parity_refresh.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return report


def run(args: argparse.Namespace) -> dict[str, Any]:
    state_dir = Path(args.state_dir).resolve()
    build_dir = Path(args.build_dir).resolve(strict=True)
    build_id = build_dir.name
    source_dir = (
        Path(args.public_read_dir).resolve()
        if args.public_read_dir
        else state_dir / "derived" / "public_read" / build_id
    )
    staging_dir = (
        Path(args.staging_dir).resolve()
        if args.staging_dir
        else source_dir.with_name(f".{build_id}.search-parity.tmp")
    )
    source_database = source_dir / "public_read.sqlite"
    source_manifest_path = source_dir / "manifest.json"
    for path in (
        source_database,
        source_manifest_path,
        build_dir / "core.duckdb",
        build_dir / "arm.duckdb",
        builder.DEFAULT_POLICY,
    ):
        if not path.is_file():
            raise SystemExit(f"Missing required artifact: {path}")
    if staging_dir.exists():
        if not args.replace:
            raise SystemExit(f"Staging directory already exists: {staging_dir}")
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True)
    database = staging_dir / "public_read.sqlite"
    manifest_path = staging_dir / "manifest.json"
    shutil.copy2(source_database, database)
    shutil.copy2(source_manifest_path, manifest_path)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("build_id") != build_id or manifest.get("sample_limit") is not None:
        raise SystemExit("Source artifact is not the complete matching public build")

    source = duckdb.connect(str(build_dir / "core.duckdb"), read_only=True)
    builder.attach_if_present(source, build_dir / "arm.duckdb", "arm_db")
    builder.attach_if_present(source, build_dir / "disc.duckdb", "disc_db")
    policy = builder.load_json(builder.DEFAULT_POLICY)
    target = sqlite3.connect(str(database), timeout=60)
    target.row_factory = sqlite3.Row
    target.execute("PRAGMA journal_mode=DELETE")
    target.execute("PRAGMA synchronous=FULL")
    target.execute(
        "INSERT OR REPLACE INTO metadata(key,value) VALUES (?,?)",
        (
            "stellar_badge_overlay_schema_version",
            policy["stellar_badge_overlay_schema_version"],
        ),
    )
    target.commit()
    (
        hierarchy_bundle_ids,
        compact_seed_ids,
        full_scene_ids,
        representation_counts,
    ) = builder.representation_policies(source, policy, sample_limit=None)

    target.execute("BEGIN IMMEDIATE")
    create_overlay_table(target)
    target.execute("DELETE FROM stellar_badge_overlays")
    target.commit()
    overlay_rows, overlay_systems = builder.insert_stellar_badge_overlays(
        source,
        target,
        sample_limit=None,
    )

    target.execute("BEGIN IMMEDIATE")
    target.execute(
        "UPDATE systems SET hierarchy_representation='singleton_seed'"
    )
    target.execute("UPDATE systems SET scene_representation='singleton_seed'")
    if compact_seed_ids:
        target.executemany(
            "UPDATE systems SET scene_representation='compact_seed' WHERE system_id=?",
            [(value,) for value in sorted(compact_seed_ids - full_scene_ids)],
        )
    target.executemany(
        """
        UPDATE systems
        SET hierarchy_representation='bundle_required',
            scene_representation='full_scene'
        WHERE system_id=?
        """,
        [(value,) for value in sorted(full_scene_ids)],
    )
    remaining_bundle_ids = hierarchy_bundle_ids - full_scene_ids
    target.executemany(
        "UPDATE systems SET hierarchy_representation='bundle_required' WHERE system_id=?",
        [(value,) for value in sorted(remaining_bundle_ids)],
    )
    target.executemany(
        "INSERT OR REPLACE INTO metadata(key,value) VALUES (?,?)",
        [
            ("search_parity_upgrader_version", UPGRADER_VERSION),
            ("stellar_badge_overlay_policy", "selected_leaf_difference_v1"),
            ("full_scene_selection_policy", "selected_leaf_multistar_or_planet_host_v1"),
        ],
    )
    target.commit()
    singleton_seed_count = int(
        target.execute("SELECT COUNT(*) FROM singleton_scene_seeds").fetchone()[0]
    )
    target.execute("ANALYZE")
    target.commit()
    integrity = str(target.execute("PRAGMA integrity_check").fetchone()[0])
    logical_hashes = refresh_modified_logical_hashes(target, manifest)
    full_scene_count = int(
        target.execute(
            "SELECT COUNT(*) FROM systems WHERE scene_representation='full_scene'"
        ).fetchone()[0]
    )
    bundle_required_count = int(
        target.execute(
            "SELECT COUNT(*) FROM systems WHERE hierarchy_representation='bundle_required'"
        ).fetchone()[0]
    )
    target.close()
    source.close()
    if integrity != "ok":
        raise RuntimeError(f"Staged public-read integrity failed: {integrity}")
    if full_scene_count != len(full_scene_ids):
        raise RuntimeError("Full-scene policy count disagrees with staged systems")
    if bundle_required_count != len(hierarchy_bundle_ids):
        raise RuntimeError("Hierarchy-bundle policy count disagrees with staged systems")

    manifest["status"] = "warming"
    manifest["artifact"]["bytes"] = database.stat().st_size
    manifest["artifact"]["sha256"] = None
    manifest["artifact"]["hash_status"] = "pending_finalization"
    refresh_manifest_accounting(
        manifest,
        singleton_seed_count=singleton_seed_count,
        overlay_rows=overlay_rows,
    )
    manifest["representation_counts"] = representation_counts
    manifest["stellar_badge_overlay_system_count"] = overlay_systems
    manifest["stellar_badge_overlay_schema_version"] = policy[
        "stellar_badge_overlay_schema_version"
    ]
    manifest["logical_hashes"] = logical_hashes
    manifest["search_parity_upgrade"] = {
        "upgrader_version": UPGRADER_VERSION,
        "overlay_rows": overlay_rows,
        "overlay_systems": overlay_systems,
        "full_scene_targets": full_scene_count,
        "hierarchy_bundle_targets": bundle_required_count,
    }
    atomic_json(manifest_path, manifest)

    report = {
        "schema_version": "spacegate.public_read_search_parity_stage.v1",
        "status": "pass",
        "build_id": build_id,
        "upgrader_version": UPGRADER_VERSION,
        "source_artifact_sha256": builder.sha256_file(source_database),
        "staging_dir": str(staging_dir),
        "overlay_rows": overlay_rows,
        "overlay_systems": overlay_systems,
        "full_scene_targets": full_scene_count,
        "hierarchy_bundle_targets": bundle_required_count,
        "singleton_scene_seeds": singleton_seed_count,
        "representation_counts": representation_counts,
        "logical_hashes": logical_hashes,
        "sqlite_integrity": integrity,
        "generated_at_utc": utc_now(),
    }
    report_dir = (
        Path(args.report_dir).resolve()
        if args.report_dir
        else state_dir / "reports" / "public_read" / build_id
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    atomic_json(report_dir / "search_parity_stage.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-dir")
    parser.add_argument(
        "--state-dir",
        default=os.getenv("SPACEGATE_STATE_DIR", "/data/spacegate/state"),
    )
    parser.add_argument("--public-read-dir")
    parser.add_argument("--staging-dir")
    parser.add_argument("--report-dir")
    parser.add_argument("--replace", action="store_true")
    parser.add_argument(
        "--refresh-existing",
        action="store_true",
        help="refresh schema metadata, logical hashes, and the physical checksum",
    )
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.refresh_existing:
        if not arguments.staging_dir:
            raise SystemExit("--refresh-existing requires --staging-dir")
        refresh_existing(arguments)
    else:
        if not arguments.build_dir:
            raise SystemExit("--build-dir is required")
        run(arguments)
