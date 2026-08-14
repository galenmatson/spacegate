#!/usr/bin/env python3
"""Audit clean simulation scenes for physical stellar-orbit coverage."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


SELECTED_STELLAR_MASS_POLICY_VERSION = "stellar_leaf_mass_selection_v3"


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _field(fields: Any, key: str) -> dict[str, Any]:
    if isinstance(fields, dict):
        value = fields.get(key)
        return value if isinstance(value, dict) else {}
    return {}


def _accepted(field: dict[str, Any]) -> bool:
    return (
        _number(field.get("value")) is not None
        and str(field.get("status") or "").lower()
        in {"source", "source_model", "derived"}
    )


def _accepted_selected_mass(field: dict[str, Any]) -> bool:
    return (
        field.get("selection_policy_version") == SELECTED_STELLAR_MASS_POLICY_VERSION
        and _accepted(field)
    )


def _accepted_orbital_period(field: dict[str, Any]) -> bool:
    return (
        _number(field.get("value")) is not None
        and str(field.get("status") or "").lower() in {"source", "source_model"}
    )


def _has_table(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(
        con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name=?",
            [table],
        ).fetchone()[0]
    )


def _arm_accounting(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {"status": "not_requested"}
    if not path.is_file():
        return {"status": "fail", "reason": "arm_database_missing", "path": str(path)}
    con = duckdb.connect(str(path), read_only=True)
    try:
        output: dict[str, Any] = {"status": "pass", "path": str(path)}
        if _has_table(con, "stellar_orbit_relation_bindings"):
            output["relation_bindings"] = {
                f"{status}|simulation_eligible={str(bool(eligible)).lower()}": int(count)
                for status, eligible, count in con.execute(
                    """
                    SELECT binding_status,simulation_eligible,count(*)
                    FROM stellar_orbit_relation_bindings
                    GROUP BY 1,2 ORDER BY 1,2
                    """
                ).fetchall()
            }
            if _has_table(con, "selected_stellar_orbit_relations"):
                output["relation_sources"] = {
                    f"{source_id}|{binding_status}|simulation_eligible={str(bool(eligible)).lower()}": int(count)
                    for source_id, binding_status, eligible, count in con.execute(
                        """
                        SELECT selected.source_id,binding.binding_status,
                          binding.simulation_eligible,count(*)
                        FROM stellar_orbit_relation_bindings binding
                        JOIN selected_stellar_orbit_relations selected USING(relation_id)
                        GROUP BY 1,2,3 ORDER BY 1,2,3
                        """
                    ).fetchall()
                }
        if _has_table(con, "stellar_orbit_endpoint_bindings"):
            output["endpoint_bindings"] = {
                f"{status}|{kind}|{reason}": int(count)
                for status, kind, reason, count in con.execute(
                    """
                    SELECT binding_status,coalesce(endpoint_kind,'unknown'),
                      coalesce(binding_reason,'unspecified'),count(*)
                    FROM stellar_orbit_endpoint_bindings
                    GROUP BY 1,2,3 ORDER BY 1,2,3
                    """
                ).fetchall()
            }
        if _has_table(con, "stellar_leaf_parameter_binding_outcomes"):
            output["component_mass_bindings"] = {
                f"{source_id}|{status}|{reason}": int(count)
                for source_id, status, reason, count in con.execute(
                    """
                    SELECT source_id,binding_status,coalesce(binding_reason,'unspecified'),count(*)
                    FROM stellar_leaf_parameter_binding_outcomes
                    GROUP BY 1,2,3 ORDER BY 1,2,3
                    """
                ).fetchall()
            }
            columns = {
                str(row[0])
                for row in con.execute(
                    "DESCRIBE stellar_leaf_parameter_binding_outcomes"
                ).fetchall()
            }
            if "msc_mass_code" in columns:
                output["msc_mass_codes"] = {
                    str(code or "missing"): int(count)
                    for code, count in con.execute(
                        """
                        SELECT msc_mass_code,count(*)
                        FROM stellar_leaf_parameter_binding_outcomes
                        WHERE source_id='multiplicity.msc'
                        GROUP BY 1 ORDER BY 1
                        """
                    ).fetchall()
                }
        if _has_table(con, "stellar_leaf_parameter_evidence"):
            output["component_mass_applicability"] = {
                f"{source_id}|{decision}": int(count)
                for source_id, decision, count in con.execute(
                    """
                    SELECT source_id,coalesce(applicability_decision,'unspecified'),count(*)
                    FROM stellar_leaf_parameter_evidence
                    WHERE quantity_key='mass_msun'
                    GROUP BY 1,2 ORDER BY 1,2
                    """
                ).fetchall()
            }
        if _has_table(con, "stellar_leaf_selected_parameters"):
            output["selected_component_masses"] = {
                f"{selection_status}|{value_status}|{reason}": int(count)
                for selection_status, value_status, reason, count in con.execute(
                    """
                    SELECT selection_status,value_status,
                      coalesce(selection_reason,'unspecified'),count(*)
                    FROM stellar_leaf_selected_parameters
                    WHERE quantity_key='mass_msun'
                    GROUP BY 1,2,3 ORDER BY 1,2,3
                    """
                ).fetchall()
            }
        return output
    finally:
        con.close()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _focus_depths(focus_graph: dict[str, Any]) -> dict[str, int]:
    nodes = focus_graph.get("nodes") or {}
    depths: dict[str, int] = {}
    for focus_key, node in nodes.items():
        orbit_key = str((node or {}).get("orbit_key") or "")
        if not orbit_key:
            continue
        depth = 0
        cursor = node
        seen = {focus_key}
        while cursor and cursor.get("parent_focus_key"):
            parent_key = str(cursor["parent_focus_key"])
            if parent_key in seen:
                break
            seen.add(parent_key)
            depth += 1
            cursor = nodes.get(parent_key)
        depths[orbit_key] = depth
    return depths


def _classification_bucket(stars_by_key: dict[str, dict[str, Any]], keys: list[str]) -> str:
    values: list[str] = []
    for key in keys:
        star = stars_by_key.get(key) or {}
        fields = star.get("fields") or {}
        value = _field(fields, "visual_stellar_class").get("value") or star.get("spectral_class")
        values.append(str(value or "UNKNOWN"))
    return "+".join(sorted(values)) if values else "no_endpoints"


def audit(
    cache_dir: Path,
    *,
    label: str,
    expected_scenes: int | None,
    arm_db: Path | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    paths = sorted(cache_dir.glob("system_*.json.gz"))
    counts = Counter()
    breakdowns: dict[str, Counter[str]] = defaultdict(Counter)
    examples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    artifact_hashes: list[tuple[str, str]] = []
    build_ids = Counter()
    materializer_versions = Counter()
    physical_contract_versions = Counter()
    relation_inventory: list[dict[str, Any]] = []

    for path in paths:
        artifact_hashes.append((path.name, _file_sha256(path)))
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        build_ids[str(payload.get("build_id") or "missing")] += 1
        materializer_versions[str((payload.get("materialization") or {}).get("materializer_version") or "missing")] += 1
        render = payload.get("render_scene") or {}
        physical_contract_versions[str((render.get("physical_scale") or {}).get("schema_version") or "missing")] += 1
        stars = (render.get("bodies") or {}).get("stars") or []
        stars_by_key = {
            str(star.get("render_key") or star.get("key")): star
            for star in stars
            if star.get("render_key") or star.get("key")
        }
        focus_depths = _focus_depths(render.get("focus_graph") or {})
        system = payload.get("system") or {}
        distance_state = "available" if _number(system.get("dist_ly")) is not None else "missing"

        for orbit in render.get("orbits") or []:
            counts["stellar_relation_rows"] += 1
            fields = orbit.get("fields") or {}
            extent = orbit.get("physical_extent") or {}
            source = str((orbit.get("source") or {}).get("source_catalog") or "unknown")
            relation_kind = str(orbit.get("relation_kind") or "unknown")
            endpoint_kind = str(orbit.get("endpoint_kind") or "unknown")
            orbit_key = str(orbit.get("orbit_key") or "")
            period = _field(fields, "period_days")
            source_axis = _field(fields, "semi_major_axis_au")
            projected = _field(fields, "projected_separation_au")
            coherence = extent.get("coherence") or {}
            applicability = str(extent.get("applicability") or "unavailable")
            axis_basis = str(extent.get("axis_basis") or "unavailable")
            primary_keys = [str(value) for value in orbit.get("primary_child_body_keys") or []]
            secondary_keys = [str(value) for value in orbit.get("secondary_child_body_keys") or []]
            endpoint_keys = sorted(set(primary_keys + secondary_keys))
            endpoint_mass_fields = [
                _field((stars_by_key.get(key) or {}).get("fields"), "mass_msun")
                for key in endpoint_keys
            ]
            legacy_known_masses = sum(_accepted(field) for field in endpoint_mass_fields)
            legacy_missing_masses = len(endpoint_mass_fields) - legacy_known_masses
            known_masses = sum(_accepted_selected_mass(field) for field in endpoint_mass_fields)
            missing_masses = len(endpoint_mass_fields) - known_masses

            if applicability == "physical" and axis_basis == "accepted_orbit_axis":
                state = "physical"
                counts["physically_scalable"] += 1
                counts["source_axis"] += 1
            elif applicability == "physical":
                state = "derived"
                counts["physically_scalable"] += 1
                counts["kepler_derived_axis"] += 1
            elif str(coherence.get("status") or "") == "rejected":
                state = "rejected"
                counts["rejected"] += 1
            else:
                state = "unavailable"
                counts["unavailable"] += 1

            if _accepted_orbital_period(period):
                counts["accepted_period"] += 1
                if legacy_missing_masses:
                    counts["accepted_period_incomplete_endpoint_masses_legacy"] += 1
                if missing_masses:
                    counts[
                        "accepted_period_incomplete_endpoint_masses_selected_projection"
                    ] += 1
                    if state == "unavailable":
                        counts[
                            "unavailable_with_accepted_period_incomplete_endpoint_masses"
                        ] += 1
            if str(coherence.get("status") or "") == "rejected" and _accepted(source_axis):
                counts["accepted_axis_rejected_by_coherence"] += 1
            if (
                not _accepted(source_axis)
                and not _accepted_orbital_period(period)
                and not _accepted(projected)
            ):
                counts["no_accepted_axis_period_or_projected_separation"] += 1

            relation_key = "|".join(
                (
                    str(system.get("stable_object_key") or system.get("system_id") or "unknown"),
                    str(orbit.get("orbit_edge_id") or orbit_key or "unknown"),
                    source,
                )
            )
            relation_inventory.append(
                {
                    "relation_key": relation_key,
                    "system_id": system.get("system_id"),
                    "system_stable_object_key": system.get("stable_object_key"),
                    "system_name": system.get("display_name") or system.get("system_name"),
                    "orbit_key": orbit_key,
                    "orbit_edge_id": orbit.get("orbit_edge_id"),
                    "display_name": orbit.get("display_name"),
                    "source": source,
                    "relation_kind": relation_kind,
                    "endpoint_kind": endpoint_kind,
                    "state": state,
                    "axis_basis": axis_basis,
                    "semi_major_axis_au": (extent.get("semi_major_axis_au") or {}).get("value"),
                    "axis_value_lower": (extent.get("semi_major_axis_au") or {}).get("value_lower"),
                    "axis_value_upper": (extent.get("semi_major_axis_au") or {}).get("value_upper"),
                    "period_days": period.get("value"),
                    "known_endpoint_masses": known_masses,
                    "missing_endpoint_masses": missing_masses,
                    "legacy_known_endpoint_masses": legacy_known_masses,
                    "legacy_missing_endpoint_masses": legacy_missing_masses,
                    "total_mass_msun": extent.get("total_mass_msun"),
                    "total_mass_interval_msun": extent.get("total_mass_interval_msun"),
                    "mass_basis": extent.get("mass_basis"),
                    "coherence_status": coherence.get("status"),
                    "coherence_reason": coherence.get("reason"),
                }
            )

            for dimension, value in (
                ("state", state),
                ("source", source),
                ("relation_kind", relation_kind),
                ("endpoint_scope", endpoint_kind),
                ("distance", distance_state),
                ("hierarchy_depth", str(focus_depths.get(orbit_key, -1))),
                ("period_authority", str(period.get("source_catalog") or period.get("basis") or "missing")),
                ("mass_coverage", f"known_{known_masses}_missing_{missing_masses}"),
                (
                    "legacy_mass_coverage",
                    f"known_{legacy_known_masses}_missing_{legacy_missing_masses}",
                ),
                ("classification", _classification_bucket(stars_by_key, endpoint_keys)),
                ("mass_basis", str(extent.get("mass_basis") or "legacy_or_unavailable")),
            ):
                breakdowns[f"{dimension}_by_state"][f"{state}|{value}"] += 1
            if len(examples[state]) < 25:
                examples[state].append(
                    {
                        "system_id": system.get("system_id"),
                        "system_name": system.get("display_name") or system.get("system_name"),
                        "orbit_key": orbit_key,
                        "display_name": orbit.get("display_name"),
                        "source": source,
                        "relation_kind": relation_kind,
                        "endpoint_kind": endpoint_kind,
                        "period_status": period.get("status"),
                        "period_days": period.get("value"),
                        "known_endpoint_masses": known_masses,
                        "missing_endpoint_masses": missing_masses,
                        "legacy_known_endpoint_masses": legacy_known_masses,
                        "legacy_missing_endpoint_masses": legacy_missing_masses,
                        "axis_basis": axis_basis,
                        "coherence": coherence,
                    }
                )

    for state in ("ambiguous", "quarantined", "excluded"):
        counts.setdefault(state, 0)
    digest = hashlib.sha256()
    for name, checksum in artifact_hashes:
        digest.update(f"{name}\0{checksum}\n".encode("utf-8"))
    scene_count_ok = expected_scenes is None or len(paths) == expected_scenes
    arm_accounting = _arm_accounting(arm_db)
    relation_key_counts = Counter(
        item["relation_key"] for item in relation_inventory
    )
    duplicate_relation_keys = {
        key: count for key, count in relation_key_counts.items() if count != 1
    }
    return {
        "schema_version": "spacegate.physical_extent_coverage_audit.v1",
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "label": label,
        "cache_dir": str(cache_dir),
        "scene_count": len(paths),
        "expected_scene_count": expected_scenes,
        "scene_count_matches": scene_count_ok,
        "artifact_set_sha256": digest.hexdigest(),
        "build_ids": dict(sorted(build_ids.items())),
        "materializer_versions": dict(sorted(materializer_versions.items())),
        "physical_contract_versions": dict(sorted(physical_contract_versions.items())),
        "counts": dict(sorted(counts.items())),
        "breakdowns": {
            key: dict(sorted(value.items())) for key, value in sorted(breakdowns.items())
        },
        "examples": dict(sorted(examples.items())),
        "relation_inventory": sorted(
            relation_inventory, key=lambda item: item["relation_key"]
        ),
        "duplicate_relation_keys": duplicate_relation_keys,
        "arm_accounting": arm_accounting,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "status": (
            "pass"
            if scene_count_ok
            and len(build_ids) == 1
            and len(materializer_versions) == 1
            and not duplicate_relation_keys
            and arm_accounting.get("status") in {"pass", "not_requested"}
            else "fail"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", required=True, type=Path)
    parser.add_argument("--label", required=True)
    parser.add_argument("--expected-scenes", type=int)
    parser.add_argument("--arm-db", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    report = audit(
        args.cache_dir.resolve(),
        label=args.label,
        expected_scenes=args.expected_scenes,
        arm_db=args.arm_db.resolve() if args.arm_db else None,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"status": report["status"], "counts": report["counts"]}, indent=2))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
