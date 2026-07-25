#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb


PROJECTION_SCHEMA = "spacegate.public_read.v2"
PLANET_LINEAGE_VERSION = "spacegate.planet_selected_fact_lineage.v1"
VERIFY_VERSION = "public_read_artifact_verifier_v2"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def id_set(rows: Iterable[tuple[Any, ...]]) -> set[int]:
    return {int(row[0]) for row in rows if row[0] is not None}


def duck_rows(
    cursor: duckdb.DuckDBPyConnection, batch_size: int = 100_000
) -> Iterable[tuple[Any, ...]]:
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            return
        yield from batch


def stream_identity_digest(rows: Iterable[tuple[Any, ...]]) -> tuple[int, str]:
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        digest.update(
            json.dumps(
                tuple(row),
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        digest.update(b"\n")
        count += 1
    return count, digest.hexdigest()


def stellar_overlay_source_rows(
    source: duckdb.DuckDBPyConnection,
) -> Iterable[tuple[Any, ...]]:
    return duck_rows(
        source.execute(
            """
            WITH leaf_counts AS (
              SELECT CAST(system_id AS BIGINT) AS system_id, COUNT(*) AS leaf_count
              FROM arm_db.stellar_leaf_display_classifications
              WHERE system_id IS NOT NULL
              GROUP BY 1
            ),
            mismatched AS (
              SELECT DISTINCT CAST(leaf.system_id AS BIGINT) AS system_id
              FROM arm_db.stellar_leaf_display_classifications leaf
              LEFT JOIN arm_db.e6_selected_stellar_display_classifications selected
                ON CAST(selected.system_id AS BIGINT) = CAST(leaf.system_id AS BIGINT)
               AND selected.star_id = leaf.star_id
              WHERE leaf.system_id IS NOT NULL
                AND (
                  leaf.star_id IS NULL
                  OR COALESCE(leaf.classification_value, 'UNKNOWN')
                     <> COALESCE(selected.classification_value, 'UNKNOWN')
                )
            ),
            eligible AS (
              SELECT counts.system_id
              FROM leaf_counts counts
              JOIN systems system_row USING (system_id)
              WHERE counts.leaf_count <> COALESCE(system_row.star_count, 0)
                 OR counts.system_id IN (SELECT system_id FROM mismatched)
            )
            SELECT
              CAST(leaf.system_id AS BIGINT),
              ROW_NUMBER() OVER (
                PARTITION BY leaf.system_id
                ORDER BY leaf.hierarchy_node_key, leaf.leaf_component_key
              ) - 1 AS badge_order,
              leaf.hierarchy_node_key,
              leaf.leaf_component_key,
              leaf.evidence_component_key,
              CAST(leaf.star_id AS VARCHAR),
              leaf.stable_object_key,
              leaf.display_name,
              leaf.catalog_component_label,
              COALESCE(leaf.classification_value, 'UNKNOWN'),
              COALESCE(leaf.classification_status, 'missing'),
              leaf.evidence_basis,
              leaf.selected_fact_id,
              leaf.source_catalog,
              leaf.source_version
            FROM arm_db.stellar_leaf_display_classifications leaf
            JOIN eligible
              ON eligible.system_id = CAST(leaf.system_id AS BIGINT)
            WHERE leaf.system_id IS NOT NULL
            ORDER BY CAST(leaf.system_id AS BIGINT), badge_order
            """
        )
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
    core_path = build_dir / "core.duckdb"
    arm_path = build_dir / "arm.duckdb"
    for required in (database, manifest_path, core_path, arm_path):
        if not required.is_file():
            raise SystemExit(f"Missing required artifact: {required}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    source = duckdb.connect(str(core_path), read_only=True)
    source.execute(f"ATTACH '{str(arm_path).replace(chr(39), chr(39) * 2)}' AS arm_db (READ_ONLY)")
    target = sqlite3.connect(str(database))
    target.row_factory = sqlite3.Row

    source_system_count, source_system_digest = stream_identity_digest(
        duck_rows(
            source.execute(
                """
                SELECT CAST(system_id AS BIGINT),stable_object_key
                FROM systems ORDER BY system_id
                """
            )
        )
    )
    projected_system_count, projected_system_digest = stream_identity_digest(
        target.execute(
            """
            SELECT system_id,stable_object_key
            FROM systems ORDER BY system_id
            """
        )
    )
    source_public_count, source_public_digest = stream_identity_digest(
        duck_rows(
            source.execute(
                """
                WITH leaf_counts AS (
                  SELECT CAST(system_id AS BIGINT) AS system_id, COUNT(*) AS leaf_count
                  FROM arm_db.stellar_leaf_display_classifications
                  WHERE system_id IS NOT NULL
                  GROUP BY 1
                )
                SELECT
                  CAST(s.system_id AS BIGINT),
                  CAST(COALESCE(leaves.leaf_count, s.star_count, 0) AS BIGINT),
                  CAST(COALESCE(s.planet_count, 0) AS BIGINT)
                FROM systems s
                LEFT JOIN leaf_counts leaves USING (system_id)
                ORDER BY s.system_id
                """
            )
        )
    )
    projected_public_count, projected_public_digest = stream_identity_digest(
        target.execute(
            """
            SELECT system_id,star_count,planet_count
            FROM systems ORDER BY system_id
            """
        )
    )
    source_overlay_count, source_overlay_digest = stream_identity_digest(
        stellar_overlay_source_rows(source)
    )
    projected_overlay_count, projected_overlay_digest = stream_identity_digest(
        target.execute(
            """
            SELECT
              system_id,badge_order,hierarchy_node_key,leaf_component_key,
              evidence_component_key,star_id_text,stable_object_key,display_name,
              catalog_component_label,classification_value,classification_status,
              evidence_basis,selected_fact_id,source_catalog,source_version
            FROM stellar_badge_overlays
            ORDER BY system_id,badge_order
            """
        )
    )

    full_scene_source = id_set(
        source.execute(
            """
            SELECT system_id FROM systems
            WHERE coalesce(star_count,0)>=2 OR coalesce(planet_count,0)>=1
            ORDER BY system_id
            """
        ).fetchall()
    )
    full_scene_projected = id_set(
        target.execute(
            "SELECT system_id FROM systems WHERE scene_representation='full_scene'"
        )
    )
    eclipsing_systems = id_set(
        source.execute(
            """
            SELECT DISTINCT system_id FROM eclipsing_binaries
            WHERE system_id IS NOT NULL ORDER BY system_id
            """
        ).fetchall()
    )
    infrared_systems = id_set(
        source.execute(
            """
            SELECT DISTINCT system_id FROM arm_db.infrared_source_matches
            WHERE system_id IS NOT NULL ORDER BY system_id
            """
        ).fetchall()
    )
    bundle_required = id_set(
        target.execute(
            """
            SELECT system_id FROM systems
            WHERE hierarchy_representation='bundle_required'
            ORDER BY system_id
            """
        )
    )
    materialized_bundles = id_set(
        target.execute("SELECT system_id FROM hierarchy_bundles ORDER BY system_id")
    )

    bundle_payload_failures: list[dict[str, Any]] = []
    bundle_compressed_bytes = 0
    bundle_uncompressed_bytes = 0
    for row in target.execute(
        """
        SELECT system_id,payload_gzip,payload_sha256,uncompressed_bytes
        FROM hierarchy_bundles ORDER BY system_id
        """
    ):
        encoded = bytes(row["payload_gzip"])
        payload = gzip.decompress(encoded)
        bundle_compressed_bytes += len(encoded)
        bundle_uncompressed_bytes += len(payload)
        if (
            len(payload) != int(row["uncompressed_bytes"])
            or hashlib.sha256(payload).hexdigest() != row["payload_sha256"]
        ):
            bundle_payload_failures.append(
                {"system_id": int(row["system_id"]), "reason": "payload_hash_or_size"}
            )

    seed_count = int(
        target.execute("SELECT COUNT(*) FROM singleton_scene_seeds").fetchone()[0]
    )
    invalid_seed_count = int(
        target.execute(
            """
            SELECT COUNT(*) FROM singleton_scene_seeds seed
            JOIN systems s USING(system_id)
            WHERE s.star_count<>1 OR s.planet_count<>0
               OR s.scene_representation NOT IN ('singleton_seed','compact_seed')
            """
        ).fetchone()[0]
    )

    planet_failures: list[dict[str, Any]] = []
    planet_rows = 0
    for row in target.execute(
        """
        SELECT planet_id,orbital_period_days,semi_major_axis_au,eccentricity,
               inclination_deg,radius_earth,radius_jup,mass_earth,mass_jup,
               eq_temp_k,insol_earth,selected_fact_lineage_json
        FROM planets ORDER BY planet_id
        """
    ):
        planet_rows += 1
        try:
            lineage = json.loads(row["selected_fact_lineage_json"])
        except (TypeError, ValueError):
            planet_failures.append(
                {"planet_id": int(row["planet_id"]), "reason": "invalid_lineage_json"}
            )
            continue
        if lineage.get("lineage_version") != PLANET_LINEAGE_VERSION:
            planet_failures.append(
                {"planet_id": int(row["planet_id"]), "reason": "lineage_version"}
            )
        for quantity in (
            "orbital_period_days",
            "semi_major_axis_au",
            "eccentricity",
            "inclination_deg",
            "radius_earth",
            "radius_jup",
            "mass_earth",
            "mass_jup",
            "eq_temp_k",
            "insol_earth",
        ):
            if row[quantity] is not None and not (lineage.get(quantity) or {}).get(
                "fact_id"
            ):
                planet_failures.append(
                    {
                        "planet_id": int(row["planet_id"]),
                        "quantity": quantity,
                        "reason": "selected_value_without_fact_id",
                    }
                )

    counts = {
        "source_systems": source_system_count,
        "projected_systems": projected_system_count,
        "projected_stars": int(target.execute("SELECT COUNT(*) FROM stars").fetchone()[0]),
        "stellar_badge_overlays": projected_overlay_count,
        "projected_planets": planet_rows,
        "full_scene_policy_minimum": len(full_scene_source),
        "full_scene_targets": len(full_scene_projected),
        "eclipsing_systems": len(eclipsing_systems),
        "infrared_systems": len(infrared_systems),
        "bundle_required": len(bundle_required),
        "bundles_materialized": len(materialized_bundles),
        "singleton_seeds": seed_count,
    }
    failures = {
        "build_identity_mismatch": int(manifest.get("build_id") != build_id),
        "manifest_not_final": int(manifest.get("status") != "pass"),
        "projection_schema_mismatch": int(
            manifest.get("projection_schema_version") != PROJECTION_SCHEMA
        ),
        "system_identity_digest_mismatch": int(
            source_system_digest != projected_system_digest
        ),
        "public_count_digest_mismatch": int(
            source_public_count != projected_public_count
            or source_public_digest != projected_public_digest
        ),
        "stellar_badge_overlay_digest_mismatch": int(
            source_overlay_count != projected_overlay_count
            or source_overlay_digest != projected_overlay_digest
        ),
        "manifest_stellar_badge_overlay_count_mismatch": int(
            int((manifest.get("counts") or {}).get("stellar_badge_overlays", -1))
            != projected_overlay_count
        ),
        "required_full_scene_systems_missing": len(
            full_scene_source - full_scene_projected
        ),
        "eclipsing_systems_without_bundle_policy": len(
            eclipsing_systems - bundle_required
        ),
        "infrared_systems_without_bundle_policy": len(
            infrared_systems - bundle_required
        ),
        "required_bundles_not_materialized": len(
            bundle_required - materialized_bundles
        ),
        "unexpected_materialized_bundles": len(
            materialized_bundles - bundle_required
        ),
        "bundle_payload_failures": len(bundle_payload_failures),
        "invalid_singleton_seeds": invalid_seed_count,
        "planet_lineage_failures": len(planet_failures),
    }
    status = "pass" if not any(failures.values()) else "fail"
    report = {
        "schema_version": "spacegate.public_read_artifact_verification.v1",
        "status": status,
        "build_id": build_id,
        "verifier_version": VERIFY_VERSION,
        "counts": counts,
        "identity": {
            "source_system_sha256": source_system_digest,
            "projected_system_sha256": projected_system_digest,
            "source_public_count_sha256": source_public_digest,
            "projected_public_count_sha256": projected_public_digest,
            "source_stellar_badge_overlay_sha256": source_overlay_digest,
            "projected_stellar_badge_overlay_sha256": projected_overlay_digest,
        },
        "bundles": {
            "compressed_bytes": bundle_compressed_bytes,
            "uncompressed_bytes": bundle_uncompressed_bytes,
            "compression_ratio": (
                round(bundle_uncompressed_bytes / bundle_compressed_bytes, 4)
                if bundle_compressed_bytes
                else None
            ),
            "payload_failure_examples": bundle_payload_failures[:20],
        },
        "planet_lineage_failure_examples": planet_failures[:20],
        "failures": failures,
        "generated_at_utc": utc_now(),
    }
    target.close()
    source.close()

    report_dir = (
        Path(args.report_dir).resolve()
        if args.report_dir
        else Path(args.state_dir).resolve() / "reports" / "public_read" / build_id
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    atomic_json(report_dir / "artifact_verification.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    if status != "pass":
        raise SystemExit(1)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify final public-read projection coverage and payload integrity."
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
