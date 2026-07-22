#!/usr/bin/env python3
"""Independently verify a clean E7 extended-object artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path("/mnt/space/spacegate/e7-clean-extended-objects")


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify(build_dir: Path) -> dict[str, Any]:
    started = time.monotonic()
    manifest = load_object(build_dir / "manifest.json")
    failures: dict[str, Any] = {}
    if manifest.get("status") != "pass":
        failures["manifest_status"] = manifest.get("status")
    if manifest.get("stability_databases_opened") != []:
        failures["stability_databases_opened"] = manifest.get("stability_databases_opened")
    if manifest.get("identity_seed_scientific_authority") is not False:
        failures["identity_seed_scientific_authority"] = manifest.get("identity_seed_scientific_authority")
    product_failures = {}
    for relative, expected in sorted((manifest.get("products") or {}).items()):
        path = build_dir / relative
        actual = None if not path.is_file() else {
            "bytes": path.stat().st_size, "sha256": file_hash(path),
        }
        declared = {"bytes": expected.get("bytes"), "sha256": expected.get("sha256")}
        if actual != declared:
            product_failures[relative] = {"expected": declared, "actual": actual}
    if product_failures:
        failures["products"] = product_failures
    db = build_dir / "clean_extended_objects.duckdb"
    checks: dict[str, int] = {}
    summaries: dict[str, Any] = {}
    if db.is_file():
        con = duckdb.connect(str(db), read_only=True)
        try:
            checks = {
                "duplicate_object_ids": int(con.execute("SELECT count(*) FROM (SELECT extended_object_id FROM extended_objects GROUP BY 1 HAVING count(*)>1)").fetchone()[0]),
                "duplicate_stable_keys": int(con.execute("SELECT count(*) FROM (SELECT stable_object_key FROM extended_objects GROUP BY 1 HAVING count(*)>1)").fetchone()[0]),
                "orphan_aliases": int(con.execute("SELECT count(*) FROM extended_object_aliases a LEFT JOIN extended_objects e USING(extended_object_id) WHERE e.extended_object_id IS NULL").fetchone()[0]),
                "orphan_identifiers": int(con.execute("SELECT count(*) FROM extended_object_identifiers i LEFT JOIN extended_objects e USING(extended_object_id) WHERE e.extended_object_id IS NULL").fetchone()[0]),
                "orphan_search_terms": int(con.execute("SELECT count(*) FROM extended_object_search_terms t LEFT JOIN extended_objects e USING(extended_object_id) WHERE e.extended_object_id IS NULL").fetchone()[0]),
                "invalid_coordinates": int(con.execute("SELECT count(*) FROM extended_objects WHERE ra_deg NOT BETWEEN 0 AND 360 OR dec_deg NOT BETWEEN -90 AND 90").fetchone()[0]),
                "partial_coordinates": int(con.execute("SELECT count(*) FROM extended_objects WHERE (ra_deg IS NULL)<>(dec_deg IS NULL)").fetchone()[0]),
                "distance_without_selected_evidence": int(con.execute("SELECT count(*) FROM extended_objects o WHERE o.dist_pc IS NOT NULL AND NOT EXISTS (SELECT 1 FROM selected_extended_object_distance d WHERE d.extended_object_id=o.extended_object_id AND d.dist_pc=o.dist_pc)").fetchone()[0]),
                "distance_from_unapproved_source": int(con.execute("SELECT count(*) FROM selected_extended_object_distance WHERE source_id NOT IN ('clusters.hunt_reffert_2024','clusters.cantat_gaudin_2020','derived.extended_object_relation')").fetchone()[0]),
                "nonpositive_distance": int(con.execute("SELECT count(*) FROM extended_objects WHERE dist_pc<=0").fetchone()[0]),
                "distance_unit_mismatch": int(con.execute("SELECT count(*) FROM extended_objects WHERE dist_pc IS NOT NULL AND abs(dist_ly-dist_pc*3.26156)>greatest(1e-9,dist_ly*1e-12)").fetchone()[0]),
                "local_3d_missing_cartesian": int(con.execute("SELECT count(*) FROM extended_objects WHERE map_domain='local_3d' AND (x_helio_ly IS NULL OR y_helio_ly IS NULL OR z_helio_ly IS NULL)").fetchone()[0]),
                "local_3d_outside_policy_radius": int(con.execute("SELECT count(*) FROM extended_objects WHERE map_domain='local_3d' AND dist_ly>1000").fetchone()[0]),
                "cartesian_norm_mismatch": int(con.execute("SELECT count(*) FROM extended_objects WHERE map_domain='local_3d' AND abs(sqrt(x_helio_ly*x_helio_ly+y_helio_ly*y_helio_ly+z_helio_ly*z_helio_ly)-dist_ly)>greatest(1e-9,dist_ly*1e-12)").fetchone()[0]),
                "galaxy_family_wrong_map_domain": int(con.execute("SELECT count(*) FROM extended_objects WHERE object_family='galaxy' AND map_domain<>'extragalactic_sky'").fetchone()[0]),
                "accepted_relation_without_system": int(con.execute("SELECT count(*) FROM extended_object_relation_bindings WHERE binding_status='accepted' AND target_system_stable_object_key IS NULL").fetchone()[0]),
                "selected_relation_without_placement": int(con.execute("SELECT count(*) FROM selected_extended_object_relation_distance WHERE selection_status='selected' AND dist_pc IS NULL").fetchone()[0]),
                "missing_m45_search": int(con.execute("SELECT count(*)=0 FROM extended_object_search_terms WHERE term_norm='m 45'").fetchone()[0]),
                "missing_ic4592_search": int(con.execute("SELECT count(*)=0 FROM extended_object_search_terms WHERE term_norm='ic 4592'").fetchone()[0]),
                "missing_lbn1113_search": int(con.execute("SELECT count(*)=0 FROM extended_object_search_terms WHERE term_norm='lbn 1113'").fetchone()[0]),
                "green_wrong_source_url": int(con.execute("SELECT count(*) FROM extended_objects WHERE source_catalog='extended.green_snr' AND source_url NOT LIKE '%mrao.cam.ac.uk%'").fetchone()[0]),
            }
            expected_counts = manifest.get("counts") or {}
            mismatches = {
                table: {"expected": expected, "actual": int(con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])}
                for table, expected in expected_counts.items()
                if int(con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]) != int(expected)
            }
            if mismatches:
                failures["counts"] = mismatches
            summaries = {
                "geometry_status": dict(con.execute("SELECT geometry_status,count(*) FROM extended_objects GROUP BY 1 ORDER BY 1").fetchall()),
                "shape_kind": dict(con.execute("SELECT shape_kind,count(*) FROM extended_objects GROUP BY 1 ORDER BY 1").fetchall()),
                "selected_sources": dict(con.execute("SELECT coalesce(source_catalog,'missing'),count(*) FROM extended_objects GROUP BY 1 ORDER BY 1").fetchall()),
                "selected_distance_sources": dict(con.execute("SELECT source_id,count(*) FROM selected_extended_object_distance GROUP BY 1 ORDER BY 1").fetchall()),
                "map_domains": dict(con.execute("SELECT map_domain,count(*) FROM extended_objects GROUP BY 1 ORDER BY 1").fetchall()),
                "relation_binding_status": dict(con.execute("SELECT binding_status,count(*) FROM extended_object_relation_bindings GROUP BY 1 ORDER BY 1").fetchall()),
                "relation_distance_selection": dict(con.execute("SELECT selection_status,count(*) FROM selected_extended_object_relation_distance GROUP BY 1 ORDER BY 1").fetchall()),
            }
        finally:
            con.close()
    else:
        failures["database"] = "missing"
    nonzero = {name: value for name, value in checks.items() if value}
    if nonzero:
        failures["invariants"] = nonzero
    return {
        "schema_version": "spacegate.e7_clean_extended_objects_verification.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_id": manifest.get("build_id"),
        "status": "pass" if not failures else "fail",
        "checks": checks, "summaries": summaries, "failing_checks": failures,
        "wall_seconds": round(time.monotonic() - started, 6),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = verify(args.artifact_root.resolve() / args.build_id)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    raise SystemExit(0 if report["status"] == "pass" else 1)


if __name__ == "__main__":
    main()
