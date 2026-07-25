#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "config" / "public_read" / "projection_v2.json"
PROJECTION_SCHEMA = "spacegate.public_read.v2"
LINEAGE_VERSION = "spacegate.planet_selected_fact_lineage.v1"
UPGRADER_VERSION = "public_read_projection_v2_upgrader_v1"

QUANTITIES = {
    "orbital_period_days": "orbital_period_days",
    "semi_major_axis_au": "semi_major_axis_au",
    "eccentricity": "eccentricity",
    "inclination_deg": "inclination_deg",
    "radius_earth": "radius_earth",
    "radius_jup": "radius_jup",
    "mass_earth": "best_mass_earth",
    "mass_jup": "best_mass_jup",
    "eq_temp_k": "eq_temp_k",
    "insol_earth": "insol_earth",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def lineage_payload(row: dict[str, Any]) -> str:
    payload: dict[str, Any] = {"lineage_version": LINEAGE_VERSION}
    for public_name, selected_name in QUANTITIES.items():
        payload[public_name] = {
            "lower": row[f"{selected_name}_lower"],
            "upper": row[f"{selected_name}_upper"],
            "fact_id": row[f"{selected_name}_fact_id"],
        }
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def run(args: argparse.Namespace) -> dict[str, Any]:
    build_dir = Path(args.build_dir).resolve(strict=True)
    build_id = build_dir.name
    public_read_dir = (
        Path(args.public_read_dir).resolve()
        if args.public_read_dir
        else Path(args.state_dir).resolve() / "derived" / "public_read" / build_id
    )
    database = public_read_dir / "public_read.sqlite"
    manifest_path = public_read_dir / "manifest.json"
    arm_path = build_dir / "arm.duckdb"
    core_path = build_dir / "core.duckdb"
    for required in (database, manifest_path, arm_path, core_path, POLICY_PATH):
        if not required.is_file():
            raise SystemExit(f"Missing required artifact: {required}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("build_id") != build_id:
        raise SystemExit("Public-read and scientific build identities differ")
    if manifest.get("sample_limit") is not None:
        raise SystemExit("Use a clean v2 compiler build for sample artifacts")

    source = duckdb.connect(str(core_path), read_only=True)
    source.execute(f"ATTACH '{str(arm_path).replace(chr(39), chr(39) * 2)}' AS arm_db (READ_ONLY)")
    selected_columns: list[str] = []
    for selected_name in QUANTITIES.values():
        selected_columns.extend(
            [
                f"sp.{selected_name}",
                f"sp.{selected_name}_lower",
                f"sp.{selected_name}_upper",
                f"sp.{selected_name}_fact_id",
            ]
        )
    cursor = source.execute(
        f"""
        SELECT CAST(p.planet_id AS BIGINT) AS planet_id,
               {",".join(selected_columns)}
        FROM planets p
        LEFT JOIN arm_db.e6_selected_planet_parameters sp USING (planet_id)
        WHERE p.system_id IS NOT NULL
        ORDER BY p.planet_id
        """
    )
    columns = [column[0] for column in cursor.description]
    selected = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    target = sqlite3.connect(str(database), timeout=60)
    target.row_factory = sqlite3.Row
    projected = {
        int(row["planet_id"]): dict(row)
        for row in target.execute(
            """
            SELECT planet_id,orbital_period_days,semi_major_axis_au,eccentricity,
                   inclination_deg,radius_earth,radius_jup,mass_earth,mass_jup,
                   eq_temp_k,insol_earth
            FROM planets
            ORDER BY planet_id
            """
        )
    }
    failures: list[dict[str, Any]] = []
    updates: list[tuple[str, int]] = []
    for row in selected:
        planet_id = int(row["planet_id"])
        current = projected.get(planet_id)
        if current is None:
            failures.append({"planet_id": planet_id, "reason": "missing_projection_row"})
            continue
        for public_name, selected_name in QUANTITIES.items():
            if current[public_name] != row[selected_name]:
                failures.append(
                    {
                        "planet_id": planet_id,
                        "quantity": public_name,
                        "reason": "selected_value_mismatch",
                    }
                )
            if (
                current[public_name] is not None
                and not row[f"{selected_name}_fact_id"]
            ):
                failures.append(
                    {
                        "planet_id": planet_id,
                        "quantity": public_name,
                        "reason": "selected_value_without_fact_id",
                    }
                )
        updates.append((lineage_payload(row), planet_id))
    if len(projected) != len(selected):
        failures.append(
            {
                "reason": "planet_row_count_mismatch",
                "projected": len(projected),
                "selected": len(selected),
            }
        )
    if failures:
        target.close()
        source.close()
        raise RuntimeError(
            f"Planet lineage verification failed: {json.dumps(failures[:20], sort_keys=True)}"
        )

    columns_present = {
        str(row[1]) for row in target.execute("PRAGMA table_info(planets)")
    }
    target.execute("BEGIN IMMEDIATE")
    if "selected_fact_lineage_json" not in columns_present:
        target.execute(
            "ALTER TABLE planets ADD COLUMN selected_fact_lineage_json TEXT NOT NULL DEFAULT '{}'"
        )
    target.executemany(
        "UPDATE planets SET selected_fact_lineage_json=? WHERE planet_id=?",
        updates,
    )
    target.executemany(
        "INSERT OR REPLACE INTO metadata(key,value) VALUES (?,?)",
        [
            ("projection_schema_version", PROJECTION_SCHEMA),
            ("planet_lineage_version", LINEAGE_VERSION),
            ("projection_upgrader_version", UPGRADER_VERSION),
        ],
    )
    target.execute(
        "UPDATE hierarchy_bundles SET bundle_version=?",
        [PROJECTION_SCHEMA],
    )
    target.commit()
    populated = int(
        target.execute(
            """
            SELECT COUNT(*) FROM planets
            WHERE json_extract(selected_fact_lineage_json,'$.lineage_version')=?
            """,
            [LINEAGE_VERSION],
        ).fetchone()[0]
    )
    integrity = str(target.execute("PRAGMA integrity_check").fetchone()[0])
    target.close()
    source.close()
    if populated != len(projected) or integrity != "ok":
        raise RuntimeError(
            f"Projection v2 verification failed: populated={populated}, "
            f"expected={len(projected)}, integrity={integrity}"
        )

    policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    manifest["status"] = "warming"
    manifest["builder_version"] = "public_read_compiler_v2"
    manifest["projection_schema_version"] = PROJECTION_SCHEMA
    manifest["policy"] = {
        "path": str(POLICY_PATH.relative_to(ROOT)),
        "sha256": sha256_file(POLICY_PATH),
    }
    manifest["artifact"]["bytes"] = database.stat().st_size
    manifest["artifact"]["sha256"] = None
    manifest["artifact"]["hash_status"] = "pending_finalization"
    manifest["planet_selected_fact_lineage"] = {
        "version": LINEAGE_VERSION,
        "rows": populated,
        "upgrader_version": UPGRADER_VERSION,
    }
    atomic_json(manifest_path, manifest)

    report = {
        "schema_version": "spacegate.public_read_projection_upgrade.v1",
        "status": "pass",
        "build_id": build_id,
        "from_projection_schema": "spacegate.public_read.v1",
        "to_projection_schema": PROJECTION_SCHEMA,
        "upgrader_version": UPGRADER_VERSION,
        "planet_rows": len(projected),
        "planet_lineage_rows": populated,
        "selected_value_mismatches": 0,
        "selected_values_without_fact_ids": 0,
        "sqlite_integrity": integrity,
        "generated_at_utc": utc_now(),
    }
    report_dir = (
        Path(args.report_dir).resolve()
        if args.report_dir
        else Path(args.state_dir).resolve() / "reports" / "public_read" / build_id
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    atomic_json(report_dir / "projection_v2_upgrade.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upgrade a complete public-read v1 artifact with planet fact lineage."
    )
    parser.add_argument("--build-dir", required=True)
    parser.add_argument(
        "--state-dir",
        default=os.getenv("SPACEGATE_STATE_DIR", "/data/spacegate/state"),
    )
    parser.add_argument("--public-read-dir")
    parser.add_argument("--report-dir")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
