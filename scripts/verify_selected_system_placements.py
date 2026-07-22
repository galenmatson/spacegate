#!/usr/bin/env python3
"""Independently verify an evidence-backed selected-system-placement artifact."""

from __future__ import annotations

import argparse
import json
import resource
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

import compile_selected_system_placements as compiler


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def verify(
    policy_path: Path, state_dir: Path, manifest_path: Path
) -> dict[str, Any]:
    started = time.monotonic()
    cpu_started = time.process_time()
    policy = compiler.load_object(policy_path)
    compiler.validate_policy(policy)
    manifest = compiler.load_object(manifest_path)
    artifact_dir = manifest_path.parent
    paths = compiler.resolve_inputs(policy, state_dir)
    current_input_hashes = {
        key: compiler.file_sha256(path) for key, path in paths.items()
    }
    current_attestation = compiler.attest_selected_fact_inputs(
        policy, paths, current_input_hashes
    )
    checks = {
        "manifest_status_pass": manifest.get("status") == "pass",
        "policy_version_match": manifest.get("policy_version") == policy["policy_version"],
        "policy_sha256_match": manifest.get("policy_sha256") == compiler.file_sha256(policy_path),
        "compiler_version_match": manifest.get("compiler_version") == policy["compiler_version"],
        "compiler_sha256_match": manifest.get("compiler_sha256")
        == compiler.file_sha256(Path(compiler.__file__).resolve()),
        "input_sha256_match": manifest.get("input_sha256") == current_input_hashes,
        "input_attestation_match": manifest.get("input_attestation") == current_attestation,
    }
    products = manifest.get("products") or {}
    product_paths: dict[str, Path] = {}
    for product_name in (
        "selected_system_placements",
        "selected_system_placement_lineage",
    ):
        registered = products.get(product_name) or {}
        path = artifact_dir / str(registered.get("path", ""))
        product_paths[product_name] = path
        checks[f"{product_name}_exists"] = path.is_file()
        checks[f"{product_name}_bytes"] = (
            path.is_file() and path.stat().st_size == int(registered.get("bytes", -1))
        )
        checks[f"{product_name}_sha256"] = (
            path.is_file()
            and compiler.file_sha256(path) == registered.get("sha256")
        )

    metrics: dict[str, Any] = {}
    if all(checks[f"{name}_exists"] for name in product_paths):
        con = duckdb.connect()
        try:
            placement = compiler.sql_literal(str(product_paths["selected_system_placements"]))
            lineage = compiler.sql_literal(str(product_paths["selected_system_placement_lineage"]))
            identity = compiler.sql_literal(str(paths["identity"]))
            con.execute(f"CREATE VIEW placements AS SELECT * FROM read_parquet({placement})")
            con.execute(f"CREATE VIEW lineage AS SELECT * FROM read_parquet({lineage})")
            con.execute(f"ATTACH {identity} AS identity (READ_ONLY)")
            con.execute(
                f"ATTACH {compiler.sql_literal(str(paths['selected_components']))} "
                "AS components (READ_ONLY)"
            )
            winner_counts = {
                str(source): int(count)
                for source, count in con.execute(
                    "SELECT placement_source,count(*) FROM placements GROUP BY 1 ORDER BY 1"
                ).fetchall()
            }
            placement_count = int(con.execute("SELECT count(*) FROM placements").fetchone()[0])
            lineage_count = int(con.execute("SELECT count(*) FROM lineage").fetchone()[0])
            canonical_count = int(con.execute(
                "SELECT count(*) FROM identity.canonical_object_nodes WHERE object_type='system'"
            ).fetchone()[0])
            metrics.update({
                "canonical_system_count": canonical_count,
                "placement_count": placement_count,
                "lineage_count": lineage_count,
                "winner_counts": winner_counts,
                "duplicate_placements": int(con.execute(
                    "SELECT count(*) FROM (SELECT system_stable_object_key FROM placements GROUP BY 1 HAVING count(*)>1)"
                ).fetchone()[0]),
                "duplicate_lineage": int(con.execute(
                    "SELECT count(*) FROM (SELECT system_stable_object_key FROM lineage GROUP BY 1 HAVING count(*)>1)"
                ).fetchone()[0]),
                "missing_canonical_systems": int(con.execute(
                    "SELECT count(*) FROM identity.canonical_object_nodes s LEFT JOIN placements p ON p.system_stable_object_key=s.stable_object_key WHERE s.object_type='system' AND p.system_stable_object_key IS NULL"
                ).fetchone()[0]),
                "unknown_systems": int(con.execute(
                    "SELECT count(*) FROM placements p LEFT JOIN identity.canonical_object_nodes s ON s.object_type='system' AND s.stable_object_key=p.system_stable_object_key WHERE s.stable_object_key IS NULL"
                ).fetchone()[0]),
                "invalid_coordinates": int(con.execute(
                    "SELECT count(*) FROM placements WHERE ra_deg NOT BETWEEN 0 AND 360 OR dec_deg NOT BETWEEN -90 AND 90 OR distance_pc<0"
                ).fetchone()[0]),
                "missing_coordinate_metadata": int(con.execute(
                    "SELECT count(*) FROM placements WHERE coordinate_frame IS NULL OR coordinate_epoch IS NULL"
                ).fetchone()[0]),
                "cartesian_norm_mismatches": int(con.execute(
                    "SELECT count(*) FROM placements WHERE abs(sqrt(x_helio_ly*x_helio_ly+y_helio_ly*y_helio_ly+z_helio_ly*z_helio_ly)-dist_ly)>1e-8"
                ).fetchone()[0]),
                "lineage_contract_mismatches": int(con.execute(
                    "SELECT count(*) FROM placements p FULL JOIN lineage l USING(system_stable_object_key) WHERE p.system_stable_object_key IS NULL OR l.system_stable_object_key IS NULL OR p.placement_source<>l.placement_source OR p.placement_method<>l.placement_method OR p.policy_version<>l.policy_version"
                ).fetchone()[0]),
                "origin_contract_mismatches": int(con.execute(
                    "SELECT count(*) FROM placements WHERE placement_source='heliocentric_origin' AND (distance_pc<>0 OR dist_ly<>0 OR x_helio_ly<>0 OR y_helio_ly<>0 OR z_helio_ly<>0)"
                ).fetchone()[0]),
                "sbx_source_release_mismatches": int(con.execute(
                    "SELECT count(*) FROM placements p JOIN lineage l USING(system_stable_object_key) WHERE p.placement_source='sbx_system_context' AND NOT EXISTS (SELECT 1 FROM components.sbx_system_bindings b WHERE b.binding_status='accepted' AND b.canonical_system_stable_object_key=p.system_stable_object_key AND b.source_id=l.source_id AND b.release_id=l.release_id)"
                ).fetchone()[0]),
                "sbx_position_epoch_mismatches": int(con.execute(
                    "SELECT count(*) FROM placements p WHERE p.placement_source='sbx_system_context' AND NOT EXISTS (SELECT 1 FROM components.sbx_astrometry_projection a WHERE a.projection_status='context_only_evidence' AND a.quantity_key='right_ascension' AND a.projected_relation_id=p.representative_object_key AND p.coordinate_epoch='J'||a.epoch_raw)"
                ).fetchone()[0]),
            })
        finally:
            con.close()
        checks.update({
            "inventory_exact": metrics["placement_count"] == metrics["canonical_system_count"],
            "lineage_inventory_exact": metrics["lineage_count"] == metrics["placement_count"],
            "winner_counts_match": metrics["winner_counts"] == policy["expected_winner_counts"],
            "duplicate_placements_zero": metrics["duplicate_placements"] == 0,
            "duplicate_lineage_zero": metrics["duplicate_lineage"] == 0,
            "missing_canonical_systems_zero": metrics["missing_canonical_systems"] == 0,
            "unknown_systems_zero": metrics["unknown_systems"] == 0,
            "invalid_coordinates_zero": metrics["invalid_coordinates"] == 0,
            "missing_coordinate_metadata_zero": metrics["missing_coordinate_metadata"] == 0,
            "cartesian_norm_mismatches_zero": metrics["cartesian_norm_mismatches"] == 0,
            "lineage_contract_mismatches_zero": metrics["lineage_contract_mismatches"] == 0,
            "origin_contract_mismatches_zero": metrics["origin_contract_mismatches"] == 0,
            "sbx_source_release_mismatches_zero": metrics["sbx_source_release_mismatches"] == 0,
            "sbx_position_epoch_mismatches_zero": metrics["sbx_position_epoch_mismatches"] == 0,
        })
    failing = sorted(name for name, passed in checks.items() if not passed)
    return {
        "schema_version": "spacegate.selected_system_placements_verification.v1",
        "generated_at": utc_now(),
        "build_id": manifest.get("build_id"),
        "verifier_sha256": compiler.file_sha256(Path(__file__).resolve()),
        "status": "pass" if not failing else "fail",
        "checks": checks,
        "failing_checks": failing,
        "metrics": metrics,
        "timing": {
            "wall_seconds": round(time.monotonic() - started, 6),
            "cpu_seconds": round(time.process_time() - cpu_started, 6),
            "peak_rss_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=compiler.DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=compiler.DEFAULT_STATE)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = verify(
        args.policy.resolve(), args.state_dir.resolve(), args.manifest.resolve()
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    raise SystemExit(0 if report["status"] == "pass" else 1)


if __name__ == "__main__":
    main()
