from __future__ import annotations

import datetime
import errno
import grp
import hashlib
import json
import math
import os
import pwd
import re
import shutil
import stat
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import duckdb
from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from .runtime_perms import apply_configured_umask

apply_configured_umask()

from . import admin_actions
from . import admin_db
from . import auth
from . import db
from . import inference_registry
from .db import DatabaseUnavailable
from .queries import (
    choose_display_name,
    fetch_arm_evidence_for_stars,
    fetch_eclipsing_for_system,
    fetch_aliases_for_stars,
    fetch_aliases_for_system,
    fetch_build_id,
    fetch_counts_for_system,
    fetch_map_systems,
    fetch_planets_for_system,
    fetch_spectral_mix,
    fetch_snapshot_for_system,
    fetch_system_hierarchy_for_system,
    fetch_stars_for_system,
    fetch_system_by_id,
    fetch_system_by_key,
    search_systems,
    summarize_star_temperatures,
)
from .utils import (
    decode_cursor,
    encode_cursor,
    normalize_query_text,
    parse_bool,
    parse_identifier_query,
    parse_spectral_classes,
)


app = FastAPI(title="Spacegate API", version="0.1")
ROOT_DIR = Path(__file__).resolve().parents[3]
SCORE_COOLNESS_SCRIPT = ROOT_DIR / "scripts" / "score_coolness.py"
SUPPORTED_SEARCH_SORTS = {"name", "distance", "coolness"}
SUPPORTED_SPECTRAL_FILTERS = {"O", "B", "A", "F", "G", "K", "M", "L", "T", "Y", "D"}
SIM_PROCEDURAL_ASSUMPTION_VERSION = "procedural_prior_v1"
SIM_VISUAL_SCALE_POLICY_VERSION = "visual_scale_beta_v1"
SIM_VISUAL_SCALE_POLICY = {
    "schema_version": SIM_VISUAL_SCALE_POLICY_VERSION,
    "scale_mode": "clarity_scaled_not_physical",
    "default_scale_mode": "structure",
    "available_scale_modes": [
        {
            "mode": "structure",
            "label": "Structure/Clarity",
            "preserves": "nested hierarchy readability and inspectable bodies",
            "sacrifices": "physical body-size and orbit-spacing ratios",
        },
        {
            "mode": "true_orbits",
            "label": "True Orbits",
            "preserves": "relative planet semi-major-axis spacing within the scene",
            "sacrifices": "body-size realism and close-in orbit readability",
        },
        {
            "mode": "true_bodies",
            "label": "True Bodies",
            "preserves": "more physical body-size contrast than clarity mode",
            "sacrifices": "small-body visibility and practical physical orbit scale",
        },
        {
            "mode": "log",
            "label": "Log Scale",
            "preserves": "rank order across large size and orbit ranges",
            "sacrifices": "linear physical ratios",
        },
    ],
    "scene_unit": "arbitrary_scene_unit",
    "presentation_only": True,
    "policy_note": (
        "Radii, orbit spacing, and subsystem guide radii are presentation-scale "
        "transforms for inspectability. Source physical values remain in fields "
        "and arm/core rows."
    ),
    "star_radius": {
        "source_field": "radius_rsun",
        "transform": "clamp(sqrt(radius_rsun_or_fallback) * factor, min_scene, max_scene)",
        "fallback_rsun": 0.55,
        "factor": 0.45,
        "min_scene": 0.18,
        "max_scene": 1.35,
    },
    "planet_radius": {
        "source_field": "radius_earth",
        "transform": "clamp(sqrt(radius_earth_or_fallback) * factor, min_scene, max_scene)",
        "fallback_rearth": 1.0,
        "factor": 0.085,
        "min_scene": 0.105,
        "max_scene": 0.34,
    },
    "planet_orbit_radius": {
        "source_field": "semi_major_axis_au",
        "transform": "min_scene + sqrt(semi_major_axis_au_or_fallback / max_scene_au) * span_scene",
        "fallback_au": 0.08,
        "min_scene": 0.75,
        "span_scene": 2.7,
        "normalization": "per_scene_max_planet_semi_major_axis",
    },
    "binary_orbit_radius": {
        "source_field": "render_scene.orbits.display_radius_scene",
        "transform": "source-aware display radius when available; deterministic presentation radius otherwise",
        "direct_pair_multiplier": 1.0,
        "group_pair_motion_multiplier": 0.55,
    },
    "collision_policy": {
        "applies_to_modes": ["structure", "log"],
        "star_radius_fraction_of_nearest_sep": 0.28,
        "min_visible_star_radius_scene": 0.045,
        "min_halo_radius_scene": 0.16,
        "min_pick_radius_scene": 0.28,
        "note": "Visible star meshes are capped against nearest rendered stellar separation; halo and pick radii remain separate presentation aids.",
    },
}

COOLNESS_WEIGHT_KEYS = [
    ("luminosity", "luminosity_feature"),
    ("proper_motion", "proper_motion_feature"),
    ("multiplicity", "multiplicity_feature"),
    ("nice_planets", "nice_planets_feature"),
    ("weird_planets", "weird_planets_feature"),
    ("proximity", "proximity_feature"),
    ("system_complexity", "system_complexity_feature"),
    ("exotic_star", "exotic_star_feature"),
]

DATASET_STATUS_CACHE_TTL_S = 30.0
_DATASET_STATUS_CACHE: Dict[str, Any] = {}


def _state_dir() -> Path:
    configured = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if configured:
        return Path(configured)
    db_path = Path(db.get_db_path())
    # Expected default: <state>/served/current/core.duckdb
    if db_path.name == "core.duckdb" and len(db_path.parents) >= 3:
        return db_path.parents[2]
    return ROOT_DIR / "data"


def _resolve_disc_db_path() -> Optional[str]:
    candidate = Path(db.get_db_path()).with_name("disc.duckdb")
    if candidate.exists():
        return str(candidate)
    return None


def _json_canonical(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _simulation_assumption_key(record: Dict[str, Any], build_id: Optional[str] = None) -> str:
    payload = {
        "build_id": build_id or record.get("build_id"),
        "object_type": record.get("object_type"),
        "system_id": record.get("system_id"),
        "star_id": record.get("star_id"),
        "planet_id": record.get("planet_id"),
        "orbit_edge_id": record.get("orbit_edge_id"),
        "stable_object_key": record.get("stable_object_key"),
        "stable_component_key": record.get("stable_component_key"),
        "render_key": record.get("render_key"),
        "parameter_key": record.get("parameter_key"),
        "value_json": record.get("value_json"),
        "assumption_version": record.get("assumption_version"),
        "input_context_json": record.get("input_context_json"),
        "replacement_target": record.get("replacement_target"),
    }
    return hashlib.sha256(_json_canonical(payload).encode("utf-8")).hexdigest()


def _load_persisted_simulation_assumption_keys(
    system_id: int,
    *,
    disc_db_path: Optional[str],
    build_id: Optional[str],
) -> set[str]:
    if not disc_db_path:
        return set()
    try:
        con = duckdb.connect(str(disc_db_path), read_only=True)
    except Exception:
        return set()
    try:
        exists = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'simulation_assumptions'
            LIMIT 1
            """
        ).fetchone()
        if not exists:
            return set()
        params: List[Any] = [system_id]
        build_filter = ""
        if build_id:
            build_filter = " AND build_id = ?"
            params.append(build_id)
        rows = con.execute(
            f"""
            SELECT assumption_key
            FROM simulation_assumptions
            WHERE system_id = ?
              AND assumption_key IS NOT NULL
              {build_filter}
            """,
            params,
        ).fetchall()
        return {str(row[0]) for row in rows if row and row[0]}
    except Exception:
        return set()
    finally:
        con.close()


def _resolve_arm_db_path() -> Optional[str]:
    candidate = Path(db.get_db_path()).with_name("arm.duckdb")
    if candidate.exists():
        return str(candidate)
    return None


def _resolve_canonical_hierarchy_db_path() -> Optional[str]:
    candidate = Path(db.get_db_path()).with_name("canonical_hierarchy.duckdb")
    if candidate.exists():
        return str(candidate)
    return None


def _summarize_arm_star_evidence(star_evidence: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
    catalog_counts: Dict[str, int] = {}
    high_variability_count = 0
    ultracool_count = 0
    stars_with_arm_evidence = 0
    for payload in star_evidence.values():
        catalogs = payload.get("catalogs") or []
        if catalogs:
            stars_with_arm_evidence += 1
        for token in catalogs:
            key = str(token or "").strip().lower()
            if not key:
                continue
            catalog_counts[key] = int(catalog_counts.get(key, 0)) + 1
        vsx = payload.get("vsx")
        if isinstance(vsx, dict) and bool(vsx.get("any_high_variability")):
            high_variability_count += 1
        if isinstance(payload.get("ultracoolsheet"), dict):
            ultracool_count += 1
    return {
        "stars_with_arm_evidence": int(stars_with_arm_evidence),
        "catalog_counts": catalog_counts,
        "high_variability_stars": int(high_variability_count),
        "ultracool_overlay_stars": int(ultracool_count),
    }


def _snapshot_asset_url(build_id: str, artifact_path: str) -> str:
    build_token = quote(str(build_id), safe="")
    rel = str(artifact_path or "").lstrip("/")
    rel_token = quote(rel, safe="/._-")
    return f"/api/v1/snapshots/{build_token}/{rel_token}"


def _attach_snapshot_url(payload: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = payload.get("snapshot")
    if not isinstance(snapshot, dict):
        return payload
    build_id = snapshot.get("build_id")
    artifact_path = snapshot.get("artifact_path")
    if build_id and artifact_path:
        snapshot["url"] = _snapshot_asset_url(str(build_id), str(artifact_path))
    payload["snapshot"] = snapshot
    return payload


OBJECT_PROVENANCE_REQUIRED_KEYS = [
    "source_catalog",
    "source_version",
    "source_url",
    "source_download_url",
    "license",
    "redistribution_ok",
    "license_note",
    "retrieved_at",
    "ingested_at",
    "transform_version",
]


def _duckdb_has_table(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return row is not None


def _rows_to_dicts(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def _object_public_system_payload(system_id: int) -> Dict[str, Any]:
    disc_db_path = _resolve_disc_db_path()
    arm_db_path = _resolve_arm_db_path()
    canonical_hierarchy_db_path = _resolve_canonical_hierarchy_db_path()
    with db.connection_scope() as con:
        system = fetch_system_by_id(con, system_id)
        if not system:
            raise KeyError(f"System not found: {system_id}")
        stars = fetch_stars_for_system(con, system_id)
        planets = fetch_planets_for_system(con, system_id)
        eclipsing_binaries = fetch_eclipsing_for_system(con, system_id)
        star_count, planet_count = fetch_counts_for_system(con, system_id)
        aliases = fetch_aliases_for_system(con, system_id)
        star_aliases = fetch_aliases_for_stars(
            con,
            [int(row.get("star_id")) for row in stars if row.get("star_id") is not None],
        )
        arm_star_evidence = fetch_arm_evidence_for_stars(
            con,
            [int(row.get("star_id")) for row in stars if row.get("star_id") is not None],
            arm_db_path=arm_db_path,
        )
        snapshot = fetch_snapshot_for_system(
            con,
            system_id=system_id,
            stable_object_key=system.get("stable_object_key"),
            disc_db_path=disc_db_path,
        )
        hierarchy = fetch_system_hierarchy_for_system(
            con,
            system_id=system_id,
            stable_object_key=system.get("stable_object_key"),
            wds_id=system.get("wds_id"),
            canonical_hierarchy_db_path=canonical_hierarchy_db_path,
            arm_db_path=arm_db_path,
        )

    effective_star_count = max(
        int(star_count or 0),
        int(((hierarchy or {}).get("counts") or {}).get("stars") or 0),
    )
    system["star_count"] = effective_star_count
    system["planet_count"] = planet_count
    system.update(summarize_star_temperatures(stars))
    system["snapshot"] = snapshot
    system["aliases"] = aliases
    system["arm_evidence_summary"] = _summarize_arm_star_evidence(arm_star_evidence)
    system_display_name, system_display_aliases = choose_display_name(system.get("system_name"), aliases)
    system["display_name"] = system_display_name
    system["display_aliases"] = system_display_aliases
    for star in stars:
        sid = star.get("star_id")
        if sid is None:
            star["aliases"] = []
            star["display_name"] = star.get("star_name")
            star["display_aliases"] = []
            continue
        aliases_for_star = star_aliases.get(int(sid), [])
        star["aliases"] = aliases_for_star
        star_display_name, star_display_aliases = choose_display_name(star.get("star_name"), aliases_for_star)
        star["display_name"] = star_display_name
        star["display_aliases"] = star_display_aliases
        star_arm_evidence = arm_star_evidence.get(int(sid), {})
        star["arm_evidence"] = star_arm_evidence
        star["arm_catalogs"] = star_arm_evidence.get("catalogs", [])
    _attach_snapshot_url(system)
    return {
        "system": system,
        "stars": stars,
        "planets": planets,
        "eclipsing_binaries": eclipsing_binaries,
        "hierarchy": hierarchy,
    }


def _provenance_diagnostics(objects: List[Dict[str, Any]], object_type: str, id_key: str) -> Dict[str, Any]:
    checked = 0
    incomplete: List[Dict[str, Any]] = []
    for item in objects:
        checked += 1
        provenance = item.get("provenance") if isinstance(item.get("provenance"), dict) else {}
        missing = [key for key in OBJECT_PROVENANCE_REQUIRED_KEYS if provenance.get(key) in (None, "")]
        if provenance.get("source_row_id") in (None, "") and provenance.get("source_row_hash") in (None, ""):
            missing.append("source_row_id_or_hash")
        if missing:
            incomplete.append(
                {
                    "object_type": object_type,
                    "object_id": item.get(id_key),
                    "stable_object_key": item.get("stable_object_key"),
                    "missing": missing,
                }
            )
    return {
        "checked": checked,
        "incomplete_count": len(incomplete),
        "examples": incomplete[:12],
    }


def _coolness_explanation_from_row(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for weight_key, feature_column in COOLNESS_WEIGHT_KEYS:
        feature_value = row.get(feature_column)
        score_value = row.get(f"score_{weight_key}")
        weight_value: Optional[float] = None
        try:
            if feature_value is not None and score_value is not None and float(feature_value) != 0:
                weight_value = float(score_value) / float(feature_value)
        except Exception:
            weight_value = None
        out.append(
            {
                "key": weight_key,
                "feature_column": feature_column,
                "score_column": f"score_{weight_key}",
                "feature_value": feature_value,
                "score_contribution": score_value,
                "effective_weight": weight_value,
            }
        )
    out.sort(
        key=lambda item: (
            -float(item.get("score_contribution") or 0.0),
            str(item.get("key") or ""),
        )
    )
    return out


def _stellar_luminosity_proxy_lsun(star: Dict[str, Any]) -> Optional[float]:
    spectral_class = str(star.get("spectral_class") or "").strip().upper()
    if spectral_class not in {"O", "B", "A", "F", "G", "K", "M"}:
        return None
    luminosity_class = str(star.get("luminosity_class") or "").strip().upper()
    spectral_type_raw = str(star.get("spectral_type_raw") or "").lower()
    if luminosity_class not in {"", "V"}:
        return None
    if re.search(r"giant|supergiant|\biii\b|\bii\b|\biv\b", spectral_type_raw):
        return None
    return {
        "O": 30000.0,
        "B": 1000.0,
        "A": 20.0,
        "F": 4.0,
        "G": 1.0,
        "K": 0.4,
        "M": 0.04,
    }.get(spectral_class)


def _stellar_main_sequence_proxy(star: Dict[str, Any]) -> Dict[str, Optional[float]]:
    spectral_class = str(star.get("spectral_class") or "").strip().upper()
    luminosity_class = str(star.get("luminosity_class") or "").strip().upper()
    spectral_type_raw = str(star.get("spectral_type_raw") or "").lower()
    if spectral_class not in {"O", "B", "A", "F", "G", "K", "M"}:
        return {}
    if luminosity_class not in {"", "V"}:
        return {}
    if re.search(r"giant|supergiant|\biii\b|\bii\b|\biv\b", spectral_type_raw):
        return {}
    return {
        "teff_k": {
            "O": 30000.0,
            "B": 12000.0,
            "A": 8000.0,
            "F": 6500.0,
            "G": 5500.0,
            "K": 4200.0,
            "M": 3200.0,
        }.get(spectral_class),
        "mass_msun": {
            "O": 16.0,
            "B": 2.1,
            "A": 1.7,
            "F": 1.2,
            "G": 1.0,
            "K": 0.7,
            "M": 0.3,
        }.get(spectral_class),
        "radius_rsun": {
            "O": 6.6,
            "B": 3.0,
            "A": 1.7,
            "F": 1.3,
            "G": 1.0,
            "K": 0.8,
            "M": 0.4,
        }.get(spectral_class),
        "luminosity_lsun": _stellar_luminosity_proxy_lsun(star),
    }


def _float_or_none(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def _simulation_field(
    *,
    key: str,
    label: str,
    value: Any = None,
    unit: Optional[str] = None,
    status: str,
    basis: str,
    layer: str,
    confidence_tier: str,
    replacement_target: str,
    source_catalog: Optional[str] = None,
    source_reference: Optional[str] = None,
    seed: Optional[str] = None,
    generator_version: Optional[str] = None,
    confidence: Optional[float] = None,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "key": key,
        "label": label,
        "value": value,
        "unit": unit,
        "status": status,
        "basis": basis,
        "layer": layer,
        "confidence_tier": confidence_tier,
        "replacement_target": replacement_target,
    }
    if source_catalog:
        out["source_catalog"] = source_catalog
    if source_reference:
        out["source_reference"] = source_reference
    if seed:
        out["seed"] = seed
    if generator_version:
        out["generator_version"] = generator_version
    if confidence is not None:
        out["confidence"] = confidence
    if notes:
        out["notes"] = notes
    return out


def _stable_seed(*parts: Any) -> str:
    text = "|".join(str(part or "") for part in parts)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _seed_unit(seed: str, salt: str = "") -> float:
    digest = hashlib.sha256(f"{seed}|{salt}".encode("utf-8")).hexdigest()
    return int(digest[:12], 16) / float(0xFFFFFFFFFFFF)


def _seed_centered(seed: str, salt: str = "") -> float:
    value = (_seed_unit(seed, f"{salt}:a") + _seed_unit(seed, f"{salt}:b") + _seed_unit(seed, f"{salt}:c")) / 3.0
    return max(-1.0, min(1.0, (value - 0.5) * 2.0))


def _procedural_field(
    *,
    key: str,
    label: str,
    value: Any,
    unit: Optional[str],
    basis: str,
    seed: str,
    confidence_tier: str = "illustrative",
    confidence: float = 0.25,
    replacement_target: str,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    return _simulation_field(
        key=key,
        label=label,
        value=value,
        unit=unit,
        status="assumed",
        basis=f"{SIM_PROCEDURAL_ASSUMPTION_VERSION}:{basis}",
        layer="disc_assumption",
        confidence_tier=confidence_tier,
        replacement_target=replacement_target,
        seed=seed,
        generator_version=SIM_PROCEDURAL_ASSUMPTION_VERSION,
        confidence=confidence,
        notes=notes or "Deterministic visualization prior only; not canonical astronomy.",
    )


def _clamp_float(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _median_float(values: List[float]) -> Optional[float]:
    clean = sorted(value for value in values if value is not None and math.isfinite(value))
    if not clean:
        return None
    mid = len(clean) // 2
    if len(clean) % 2:
        return clean[mid]
    return (clean[mid - 1] + clean[mid]) / 2.0


def _planet_source_inclination_planes(
    planets: List[Dict[str, Any]],
    planet_readiness_by_id: Dict[int, Dict[str, Any]],
) -> Dict[str, Any]:
    by_host_values: Dict[str, List[float]] = {}
    system_values: List[float] = []
    for planet in planets:
        try:
            planet_id = int(planet.get("planet_id"))
        except Exception:
            continue
        readiness = planet_readiness_by_id.get(planet_id) or {}
        fields = _field_map(readiness.get("fields") or [])
        inclination = fields.get("inclination_deg") or {}
        if str(inclination.get("status") or "").lower() != "source":
            continue
        value = _float_or_none(inclination.get("value"))
        if value is None:
            continue
        value = _clamp_float(value, 0.0, 180.0)
        system_values.append(value)
        host_key = str(planet.get("star_id") or "").strip()
        if host_key:
            by_host_values.setdefault(host_key, []).append(value)

    by_host: Dict[str, Dict[str, Any]] = {}
    for host_key, values in by_host_values.items():
        median = _median_float(values)
        if median is not None:
            by_host[host_key] = {"inclination_deg": median, "source_count": len(values)}
    system_median = _median_float(system_values)
    return {
        "by_host": by_host,
        "system": (
            {"inclination_deg": system_median, "source_count": len(system_values)}
            if system_median is not None
            else None
        ),
    }


def _planet_visual_inclination_prior(
    *,
    seed: str,
    replacement_target: str,
    plane_refs: Dict[str, Any],
    host_key: Optional[str] = None,
) -> Dict[str, Any]:
    host_ref = (plane_refs.get("by_host") or {}).get(str(host_key or "").strip())
    system_ref = plane_refs.get("system")
    ref = host_ref or system_ref
    if ref:
        offset = 1.25 * _seed_centered(seed, "coplanar_inc_offset")
        value = round(_clamp_float(float(ref["inclination_deg"]) + offset, 0.0, 180.0), 6)
        scope = "same host" if host_ref else "same system"
        source_count = int(ref.get("source_count") or 1)
        return _procedural_field(
            key="inclination_deg",
            label="Inclination",
            value=value,
            unit="deg",
            basis="coplanar_with_source_planet_inclination_visual_prior",
            seed=seed,
            confidence=0.35,
            replacement_target=replacement_target,
            notes=(
                f"Deterministic visualization prior seeded from {source_count} "
                f"source-backed planet inclination(s) in the {scope}; not canonical astronomy."
            ),
        )
    return _procedural_field(
        key="inclination_deg",
        label="Inclination",
        value=round(3.0 * _seed_centered(seed, "inc"), 6),
        unit="deg",
        basis="centered_low_inclination_visual_prior",
        seed=seed,
        replacement_target=replacement_target,
    )


def _best_stellar_parameters_by_star_id(arm: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    rows = ((arm.get("stellar_parameters") or {}).get("items") or [])
    out: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        star_id = row.get("star_id")
        if star_id is None:
            continue
        sid = int(star_id)
        current = out.get(sid)
        row_score = sum(1 for key in ("teff_k", "radius_rsun", "mass_msun", "luminosity_log10_lsun") if row.get(key) is not None)
        current_score = sum(1 for key in ("teff_k", "radius_rsun", "mass_msun", "luminosity_log10_lsun") if current and current.get(key) is not None)
        if current is None or row_score > current_score:
            out[sid] = row
    return out


def _derived_parameter_lookup(arm: Dict[str, Any]) -> Dict[tuple[str, int, str], Dict[str, Any]]:
    rows = ((arm.get("derived_physical_parameters") or {}).get("items") or [])
    out: Dict[tuple[str, int, str], Dict[str, Any]] = {}
    for row in rows:
        object_type = str(row.get("object_type") or "")
        object_id_raw = row.get("star_id") if object_type == "star" else row.get("planet_id")
        parameter_key = str(row.get("parameter_key") or "")
        if not object_type or object_id_raw is None or not parameter_key:
            continue
        try:
            object_id = int(object_id_raw)
        except Exception:
            continue
        key = (object_type, object_id, parameter_key)
        current = out.get(key)
        if current is None or float(row.get("confidence_score") or 0.0) > float(current.get("confidence_score") or 0.0):
            out[key] = row
    return out


def _derived_parameter_field(
    row: Optional[Dict[str, Any]],
    *,
    key: str,
    label: str,
    replacement_target: str,
) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return _simulation_field(
        key=key,
        label=label,
        value=_float_or_none(row.get("value")),
        unit=row.get("unit"),
        status="derived",
        basis=f"arm.derived_physical_parameters:{row.get('derivation_method') or 'derived'}",
        layer="arm",
        confidence_tier=row.get("confidence_tier") or "medium",
        replacement_target=replacement_target,
    )


def _star_simulation_fields(
    star: Dict[str, Any],
    params: Optional[Dict[str, Any]],
    derived_lookup: Dict[tuple[str, int, str], Dict[str, Any]],
) -> Dict[str, Any]:
    proxy = _stellar_main_sequence_proxy(star)
    star_id = int(star["star_id"]) if star.get("star_id") is not None else -1
    source_teff = _float_or_none(star.get("teff_k")) or _float_or_none((params or {}).get("teff_k"))
    source_mass = _float_or_none(star.get("mass_msun")) or _float_or_none((params or {}).get("mass_msun"))
    source_radius = _float_or_none(star.get("radius_rsun")) or _float_or_none((params or {}).get("radius_rsun"))
    source_lum = _float_or_none(star.get("luminosity_lsun"))
    log_lum = _float_or_none((params or {}).get("luminosity_log10_lsun"))
    if source_lum is None and log_lum is not None:
        source_lum = math.pow(10.0, log_lum)
    arm_lum_field = _derived_parameter_field(
        derived_lookup.get(("star", star_id, "luminosity_lsun")),
        key="luminosity_lsun",
        label="Luminosity",
        replacement_target="source stellar luminosity or source radius+teff with uncertainty",
    )
    derived_lum = None
    if source_lum is None and source_radius is not None and source_teff is not None and source_teff > 0:
        derived_lum = source_radius * source_radius * math.pow(source_teff / 5772.0, 4.0)

    fields = [
        _simulation_field(
            key="teff_k",
            label="Effective temperature",
            value=source_teff if source_teff is not None else proxy.get("teff_k"),
            unit="K",
            status="source" if source_teff is not None else ("derived" if proxy.get("teff_k") is not None else "missing"),
            basis="core/arm source teff_k" if source_teff is not None else ("main-sequence spectral-class proxy" if proxy.get("teff_k") is not None else "no source temperature or supported spectral-class proxy"),
            layer="core/arm" if source_teff is not None else ("arm_candidate" if proxy.get("teff_k") is not None else "none"),
            confidence_tier="high" if source_teff is not None else ("low" if proxy.get("teff_k") is not None else "missing"),
            replacement_target="source stellar effective temperature with uncertainty",
        ),
        _simulation_field(
            key="luminosity_lsun",
            label="Luminosity",
            value=source_lum if source_lum is not None else ((arm_lum_field or {}).get("value") if arm_lum_field else (derived_lum if derived_lum is not None else proxy.get("luminosity_lsun"))),
            unit="Lsun",
            status="source" if source_lum is not None else ("derived" if arm_lum_field is not None or derived_lum is not None or proxy.get("luminosity_lsun") is not None else "missing"),
            basis=(
                "arm source luminosity_log10_lsun"
                if source_lum is not None
                else ((arm_lum_field or {}).get("basis") if arm_lum_field else ("Stefan-Boltzmann from source radius and teff" if derived_lum is not None else ("main-sequence spectral-class proxy" if proxy.get("luminosity_lsun") is not None else "no source luminosity, radius+teff, or supported spectral-class proxy")))
            ),
            layer="arm" if source_lum is not None or arm_lum_field is not None or derived_lum is not None else ("arm_candidate" if proxy.get("luminosity_lsun") is not None else "none"),
            confidence_tier="high" if source_lum is not None else ((arm_lum_field or {}).get("confidence_tier") if arm_lum_field else ("medium" if derived_lum is not None else ("low" if proxy.get("luminosity_lsun") is not None else "missing"))),
            replacement_target="source stellar luminosity or source radius+teff with uncertainty",
        ),
        _simulation_field(
            key="mass_msun",
            label="Mass",
            value=source_mass if source_mass is not None else proxy.get("mass_msun"),
            unit="Msun",
            status="source" if source_mass is not None else ("derived" if proxy.get("mass_msun") is not None else "missing"),
            basis="arm source mass_msun" if source_mass is not None else ("main-sequence spectral-class proxy" if proxy.get("mass_msun") is not None else "no source mass or supported spectral-class proxy"),
            layer="arm" if source_mass is not None else ("arm_candidate" if proxy.get("mass_msun") is not None else "none"),
            confidence_tier="high" if source_mass is not None else ("low" if proxy.get("mass_msun") is not None else "missing"),
            replacement_target="source stellar mass with uncertainty",
        ),
        _simulation_field(
            key="radius_rsun",
            label="Radius",
            value=source_radius if source_radius is not None else proxy.get("radius_rsun"),
            unit="Rsun",
            status="source" if source_radius is not None else ("derived" if proxy.get("radius_rsun") is not None else "missing"),
            basis="arm source radius_rsun" if source_radius is not None else ("main-sequence spectral-class proxy" if proxy.get("radius_rsun") is not None else "no source radius or supported spectral-class proxy"),
            layer="arm" if source_radius is not None else ("arm_candidate" if proxy.get("radius_rsun") is not None else "none"),
            confidence_tier="high" if source_radius is not None else ("low" if proxy.get("radius_rsun") is not None else "missing"),
            replacement_target="source stellar radius with uncertainty",
        ),
    ]
    return {
        "object_type": "star",
        "object_id": star.get("star_id"),
        "display_name": star.get("display_name") or star.get("star_name") or star.get("stable_object_key"),
        "stable_object_key": star.get("stable_object_key"),
        "fields": fields,
    }


def _field_value(fields: List[Dict[str, Any]], key: str) -> Optional[float]:
    for field in fields:
        if field.get("key") == key:
            return _float_or_none(field.get("value"))
    return None


def _planet_mass_earth(planet: Dict[str, Any]) -> Optional[float]:
    mass_earth = _float_or_none(planet.get("mass_earth"))
    if mass_earth is not None:
        return mass_earth
    mass_jup = _float_or_none(planet.get("mass_jup"))
    if mass_jup is not None:
        return mass_jup * 317.8
    return None


def _planet_simulation_fields(
    planet: Dict[str, Any],
    star_fields_by_id: Dict[int, Dict[str, Any]],
    default_star_fields: Optional[Dict[str, Any]],
    derived_lookup: Dict[tuple[str, int, str], Dict[str, Any]],
    arm_orbit_solution: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    try:
        star_id = int(planet.get("star_id")) if planet.get("star_id") is not None else None
    except Exception:
        star_id = None
    host = star_fields_by_id.get(star_id) if star_id is not None else default_star_fields
    host_fields = (host or {}).get("fields") or []
    host_mass = _field_value(host_fields, "mass_msun")
    host_mass_field = next((field for field in host_fields if field.get("key") == "mass_msun"), {})
    arm_orbit_solution = arm_orbit_solution or {}
    arm_period_days = _float_or_none(arm_orbit_solution.get("period_days"))
    arm_sma = _float_or_none(arm_orbit_solution.get("semi_major_axis_au"))
    arm_ecc = _float_or_none(arm_orbit_solution.get("eccentricity"))
    arm_inc = _float_or_none(arm_orbit_solution.get("inclination_deg"))
    arm_source_catalog = arm_orbit_solution.get("solution_source_catalog") or arm_orbit_solution.get("source_catalog")
    arm_confidence_tier = arm_orbit_solution.get("confidence_tier") or "medium"
    arm_confidence = _float_or_none(arm_orbit_solution.get("confidence_score"))
    period_days = arm_period_days if arm_period_days is not None else _float_or_none(planet.get("orbital_period_days"))
    source_sma = _float_or_none(planet.get("semi_major_axis_au"))
    selected_sma = arm_sma if arm_sma is not None else source_sma
    planet_id = int(planet["planet_id"]) if planet.get("planet_id") is not None else -1
    arm_sma_field = _derived_parameter_field(
        derived_lookup.get(("planet", planet_id, "semi_major_axis_au")),
        key="semi_major_axis_au",
        label="Semi-major axis",
        replacement_target="source semi-major axis or full orbital solution",
    )
    derived_sma = None
    if source_sma is None and period_days is not None and period_days > 0 and host_mass is not None and host_mass > 0:
        period_years = period_days / 365.25
        derived_sma = math.pow(host_mass * period_years * period_years, 1.0 / 3.0)
    mass_earth = _planet_mass_earth(planet)
    environment = planet.get("environment_evidence") or {}
    env_basis = str(environment.get("evidence_basis") or "missing")
    env_status = "source" if env_basis in {"source_eq_temp", "source_insolation"} else ("derived" if env_basis == "stellar_class_luminosity_proxy" else "missing")
    env_confidence = "high" if env_status == "source" else ("low" if env_status == "derived" else "missing")
    arm_insol_field = _derived_parameter_field(
        derived_lookup.get(("planet", planet_id, "insol_earth")),
        key="candidate_insol_earth",
        label="Incident flux",
        replacement_target="source insolation or luminosity plus orbit with uncertainty",
    )
    arm_eq_temp_field = _derived_parameter_field(
        derived_lookup.get(("planet", planet_id, "eq_temp_k")),
        key="candidate_eq_temp_k",
        label="Equilibrium temperature",
        replacement_target="source equilibrium temperature or source insolation/luminosity inputs",
    )
    fields = [
        _simulation_field(
            key="orbital_period_days",
            label="Orbital period",
            value=period_days,
            unit="days",
            status="source" if period_days is not None else "missing",
            basis=(
                f"arm.orbital_solutions:{arm_source_catalog or 'source'}"
                if arm_period_days is not None
                else "core.planets:promoted_orbital_period_days_summary"
                if period_days is not None
                else "no period source value"
            ),
            layer="arm" if arm_period_days is not None else ("core" if period_days is not None else "none"),
            confidence_tier=arm_confidence_tier if arm_period_days is not None else ("high" if period_days is not None else "missing"),
            replacement_target="source orbital period with uncertainty",
            source_catalog=str(arm_source_catalog) if arm_period_days is not None and arm_source_catalog else None,
            confidence=arm_confidence if arm_period_days is not None else None,
        ),
        _simulation_field(
            key="semi_major_axis_au",
            label="Semi-major axis",
            value=selected_sma if selected_sma is not None else ((arm_sma_field or {}).get("value") if arm_sma_field else derived_sma),
            unit="au",
            status="source" if selected_sma is not None else ("derived" if arm_sma_field is not None or derived_sma is not None else "missing"),
            basis=(
                f"arm.orbital_solutions:{arm_source_catalog or 'source'}"
                if arm_sma is not None
                else "core.planets:promoted_semi_major_axis_au_summary"
                if source_sma is not None
                else (
                    (arm_sma_field or {}).get("basis")
                    if arm_sma_field
                    else f"Kepler estimate from source period and {host_mass_field.get('status', 'unknown')} host mass"
                    if derived_sma is not None
                    else "no semi-major axis and insufficient period/host-mass inputs"
                )
            ),
            layer="arm" if arm_sma is not None else ("core" if source_sma is not None else ("arm" if arm_sma_field else ("arm_candidate" if derived_sma is not None else "none"))),
            confidence_tier=arm_confidence_tier if arm_sma is not None else ("high" if source_sma is not None else ((arm_sma_field or {}).get("confidence_tier") if arm_sma_field else ("medium" if host_mass_field.get("status") == "source" else ("low" if derived_sma is not None else "missing")))),
            replacement_target="source semi-major axis or full orbital solution",
            source_catalog=str(arm_source_catalog) if arm_sma is not None and arm_source_catalog else None,
            confidence=arm_confidence if arm_sma is not None else None,
        ),
        _simulation_field(
            key="eccentricity",
            label="Eccentricity",
            value=arm_ecc if arm_ecc is not None else (_float_or_none(planet.get("eccentricity")) if _float_or_none(planet.get("eccentricity")) is not None else 0.0),
            unit=None,
            status="source" if arm_ecc is not None or _float_or_none(planet.get("eccentricity")) is not None else "assumed",
            basis=(
                f"arm.orbital_solutions:{arm_source_catalog or 'source'}"
                if arm_ecc is not None
                else "core.planets:promoted_eccentricity_summary"
                if _float_or_none(planet.get("eccentricity")) is not None
                else "circular orbit visualization default"
            ),
            layer="arm" if arm_ecc is not None else ("core" if _float_or_none(planet.get("eccentricity")) is not None else "disc_assumption"),
            confidence_tier=arm_confidence_tier if arm_ecc is not None else ("high" if _float_or_none(planet.get("eccentricity")) is not None else "illustrative"),
            replacement_target="source eccentricity or reviewed orbital solution",
            source_catalog=str(arm_source_catalog) if arm_ecc is not None and arm_source_catalog else None,
            confidence=arm_confidence if arm_ecc is not None else None,
        ),
        _simulation_field(
            key="inclination_deg",
            label="Inclination",
            value=arm_inc if arm_inc is not None else _float_or_none(planet.get("inclination_deg")),
            unit="deg",
            status="source" if arm_inc is not None or _float_or_none(planet.get("inclination_deg")) is not None else "missing",
            basis=(
                f"arm.orbital_solutions:{arm_source_catalog or 'source'}"
                if arm_inc is not None
                else "core.planets:promoted_inclination_deg_summary"
                if _float_or_none(planet.get("inclination_deg")) is not None
                else "no inclination source value"
            ),
            layer="arm" if arm_inc is not None else ("core" if _float_or_none(planet.get("inclination_deg")) is not None else "none"),
            confidence_tier=arm_confidence_tier if arm_inc is not None else ("high" if _float_or_none(planet.get("inclination_deg")) is not None else "missing"),
            replacement_target="source planet inclination",
            source_catalog=str(arm_source_catalog) if arm_inc is not None and arm_source_catalog else None,
            confidence=arm_confidence if arm_inc is not None else None,
        ),
        _simulation_field(
            key="radius_earth",
            label="Radius",
            value=_float_or_none(planet.get("radius_earth")),
            unit="Rearth",
            status="source" if _float_or_none(planet.get("radius_earth")) is not None else "missing",
            basis="core/source radius_earth" if _float_or_none(planet.get("radius_earth")) is not None else "no radius source value",
            layer="core" if _float_or_none(planet.get("radius_earth")) is not None else "none",
            confidence_tier="high" if _float_or_none(planet.get("radius_earth")) is not None else "missing",
            replacement_target="source planet radius with uncertainty",
        ),
        _simulation_field(
            key="mass_earth",
            label="Mass",
            value=mass_earth,
            unit="Mearth",
            status="source" if mass_earth is not None else "missing",
            basis="core/source mass_earth or converted mass_jup" if mass_earth is not None else "no mass source value",
            layer="core" if mass_earth is not None else "none",
            confidence_tier="high" if mass_earth is not None else "missing",
            replacement_target="source planet mass with uncertainty",
        ),
        _simulation_field(
            key="candidate_insol_earth",
            label="Incident flux",
            value=(arm_insol_field or {}).get("value") if arm_insol_field and _float_or_none(planet.get("insol_earth")) is None else _float_or_none(environment.get("candidate_insol_earth")),
            unit="Earth=1",
            status="source" if _float_or_none(planet.get("insol_earth")) is not None else ("derived" if arm_insol_field else env_status),
            basis="core/source insol_earth" if _float_or_none(planet.get("insol_earth")) is not None else ((arm_insol_field or {}).get("basis") if arm_insol_field else (env_basis if env_status != "missing" else (environment.get("missing_reason") or "no temperature/insolation derivation inputs"))),
            layer="core" if _float_or_none(planet.get("insol_earth")) is not None else ("arm" if arm_insol_field else ("arm_candidate" if env_status == "derived" else "none")),
            confidence_tier="high" if _float_or_none(planet.get("insol_earth")) is not None else ((arm_insol_field or {}).get("confidence_tier") if arm_insol_field else env_confidence),
            replacement_target="source insolation or luminosity plus orbit with uncertainty",
        ),
        _simulation_field(
            key="candidate_eq_temp_k",
            label="Equilibrium temperature",
            value=(arm_eq_temp_field or {}).get("value") if arm_eq_temp_field and _float_or_none(planet.get("eq_temp_k")) is None else _float_or_none(environment.get("candidate_eq_temp_k")),
            unit="K",
            status="source" if _float_or_none(planet.get("eq_temp_k")) is not None else ("derived" if arm_eq_temp_field else env_status),
            basis="core/source eq_temp_k" if _float_or_none(planet.get("eq_temp_k")) is not None else ((arm_eq_temp_field or {}).get("basis") if arm_eq_temp_field else (env_basis if env_status != "missing" else (environment.get("missing_reason") or "no temperature/insolation derivation inputs"))),
            layer="core" if _float_or_none(planet.get("eq_temp_k")) is not None else ("arm" if arm_eq_temp_field else ("arm_candidate" if env_status == "derived" else "none")),
            confidence_tier="high" if _float_or_none(planet.get("eq_temp_k")) is not None else ((arm_eq_temp_field or {}).get("confidence_tier") if arm_eq_temp_field else env_confidence),
            replacement_target="source equilibrium temperature or source insolation/luminosity inputs",
        ),
    ]
    return {
        "object_type": "planet",
        "object_id": planet.get("planet_id"),
        "display_name": planet.get("planet_name") or planet.get("stable_object_key"),
        "stable_object_key": planet.get("stable_object_key"),
        "host_star_id": star_id,
        "host_display_name": (host or {}).get("display_name"),
        "fields": fields,
    }


def _field_dict_value(fields: Dict[str, Any], key: str) -> Any:
    field = fields.get(key) if isinstance(fields, dict) else None
    return field.get("value") if isinstance(field, dict) else None


def _planet_visual_kind_from_fields(fields: Dict[str, Any]) -> str:
    radius_earth = _float_or_none(_field_dict_value(fields, "radius_earth")) or 1.0
    eq_temp_k = _float_or_none(_field_dict_value(fields, "candidate_eq_temp_k"))
    insol_earth = _float_or_none(_field_dict_value(fields, "candidate_insol_earth"))
    if radius_earth >= 6.0:
        return "gas_giant"
    if radius_earth >= 2.1:
        return "ice_giant"
    if (eq_temp_k is not None and eq_temp_k >= 650.0) or (insol_earth is not None and insol_earth >= 15.0):
        return "hot_rock"
    if (eq_temp_k is not None and eq_temp_k <= 180.0) or (insol_earth is not None and insol_earth <= 0.35):
        return "cold_rock"
    return "temperate_rock"


def _usable_render_field(fields: Dict[str, Any], key: str) -> Optional[Dict[str, Any]]:
    field = fields.get(key) if isinstance(fields, dict) else None
    if not isinstance(field, dict):
        return None
    return field if field.get("value") not in (None, "") else None


def _planet_visual_class_field(fields: Dict[str, Any], seed: str) -> Dict[str, Any]:
    kind = _planet_visual_kind_from_fields(fields)
    radius_field = _usable_render_field(fields, "radius_earth")
    temp_field = _usable_render_field(fields, "candidate_eq_temp_k")
    insol_field = _usable_render_field(fields, "candidate_insol_earth")
    source_field = radius_field if kind in {"gas_giant", "ice_giant"} else (temp_field or insol_field or radius_field)
    if source_field:
        return _simulation_field(
            key="planet_visual_class",
            label="Visual class",
            value=kind,
            unit=None,
            status="derived",
            basis=f"render_scene:{kind}:from_{source_field.get('key') or 'available_planet_fields'}",
            layer="render_scene",
            confidence_tier="illustrative",
            replacement_target="reviewed planet class or atmospheric/rendering model",
            source_catalog=source_field.get("source_catalog"),
            source_reference=source_field.get("source_reference"),
            generator_version="system_preview_planet_visual_class_v1",
            confidence=0.55,
            notes="Presentation-only visual material class derived from available planet radius, temperature, or insolation fields.",
        )
    return _simulation_field(
        key="planet_visual_class",
        label="Visual class",
        value=kind,
        unit=None,
        status="assumed",
        basis=f"render_scene:{kind}:fallback_visual_prior",
        layer="render_scene",
        confidence_tier="illustrative",
        replacement_target="reviewed planet class or atmospheric/rendering model",
        seed=seed,
        generator_version="system_preview_planet_visual_class_v1",
        confidence=0.2,
        notes="Presentation-only visual material class using fallback renderer defaults because class-driving planet fields are missing.",
    )


def _simulation_readiness_diagnostics(
    stars: List[Dict[str, Any]],
    planets: List[Dict[str, Any]],
    arm: Dict[str, Any],
) -> Dict[str, Any]:
    params_by_star_id = _best_stellar_parameters_by_star_id(arm)
    derived_lookup = _derived_parameter_lookup(arm)
    planet_orbit_lookup = _planet_orbit_solutions_by_stable_key(arm)
    star_rows = [
        _star_simulation_fields(star, params_by_star_id.get(int(star["star_id"])), derived_lookup)
        for star in stars
        if star.get("star_id") is not None
    ]
    star_fields_by_id = {int(row["object_id"]): row for row in star_rows if row.get("object_id") is not None}
    default_star_fields = star_rows[0] if star_rows else None
    planet_rows = [
        _planet_simulation_fields(
            planet,
            star_fields_by_id,
            default_star_fields,
            derived_lookup,
            planet_orbit_lookup.get(str(planet.get("stable_object_key") or "")),
        )
        for planet in planets
    ]
    all_fields = [field for row in [*star_rows, *planet_rows] for field in row.get("fields", [])]
    counts: Dict[str, int] = {"source": 0, "derived": 0, "assumed": 0, "missing": 0}
    for field in all_fields:
        status = str(field.get("status") or "missing")
        counts[status] = int(counts.get(status, 0)) + 1
    total = sum(counts.values())
    score = (counts.get("source", 0) + 0.75 * counts.get("derived", 0) + 0.35 * counts.get("assumed", 0)) / total if total else 0.0
    return {
        "score": score,
        "counts": counts,
        "required_field_count": total,
        "status": "ok" if total and counts.get("missing", 0) == 0 and counts.get("assumed", 0) == 0 else ("warn" if total else "missing"),
        "stars": star_rows,
        "planets": planet_rows,
        "notes": [
            "Runtime diagnostics are shaped like future arm/disc rows but are not yet persisted.",
            "Derived numeric science candidates should be materialized in arm with provenance before becoming simulation inputs.",
            "Visualization-only defaults such as circular unknown orbits belong in disc assumptions and should be replaced when literature values are found.",
        ],
    }


def _field_by_key(fields: List[Dict[str, Any]], key: str) -> Optional[Dict[str, Any]]:
    for field in fields or []:
        if field.get("key") == key:
            return field
    return None


def _field_map(fields: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {str(field.get("key")): field for field in fields or [] if field.get("key")}


def _visual_star_color_class(star: Dict[str, Any], fallback: Optional[str] = None) -> str:
    value = str(star.get("spectral_class") or fallback or "").strip().upper()
    return value[:1] if value[:1] in {"O", "B", "A", "F", "G", "K", "M", "L", "T", "Y", "D"} else "M"


def _stellar_body_class(star: Dict[str, Any], fallback: Optional[str] = None) -> str:
    raw_type = str(star.get("object_type") or fallback or "").strip().lower()
    spectral_class = str(star.get("spectral_class") or "").strip().upper()
    spectral_type = str(star.get("spectral_type_raw") or star.get("spectral_type") or "").strip().upper()
    if raw_type in {"white_dwarf", "neutron_star", "black_hole", "pulsar", "magnetar", "brown_dwarf"}:
        return raw_type
    if spectral_type.startswith("D") or spectral_class == "D":
        return "white_dwarf"
    if spectral_class in {"L", "T", "Y"}:
        return "brown_dwarf"
    return raw_type or "star"


def _compact_type_for_body_class(body_class: str) -> Optional[str]:
    value = str(body_class or "").strip().lower()
    return value if value in {"white_dwarf", "neutron_star", "black_hole", "pulsar", "magnetar"} else None


def _stellar_body_class_field(*, value: str, basis: str, layer: str = "core", source_catalog: Optional[str] = None) -> Dict[str, Any]:
    return _simulation_field(
        key="object_type",
        label="Object type",
        value=value or "star",
        unit=None,
        status="source" if layer == "core" and value else "derived",
        basis=basis,
        layer=layer,
        confidence_tier="high" if layer == "core" and value else "illustrative",
        replacement_target="reviewed stellar/compact object classification",
        source_catalog=source_catalog,
        confidence=0.9 if layer == "core" and value else 0.45,
    )


def _component_key_for_hierarchy_star_node(node: Dict[str, Any]) -> str:
    key = str(node.get("stable_component_key") or "")
    if key.startswith("canon:leaf:msc:"):
        return "comp:msc:wds:" + key[len("canon:leaf:msc:"):]
    if key.startswith("canon:star:"):
        return "comp:star:" + key
    return key


def _iter_hierarchy_render_star_nodes(node: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(node, dict):
        return []
    children = [child for child in (node.get("children") or []) if isinstance(child, dict)]
    child_stars: List[Dict[str, Any]] = []
    for child in children:
        child_stars.extend(_iter_hierarchy_render_star_nodes(child))
    component_type = str(node.get("component_type") or node.get("component_family") or "")
    node_kind = str(node.get("node_kind") or "")
    is_star = component_type in {"star", "stellar_component"} or node_kind in {"star", "inferred_star_leaf"}
    if not is_star:
        return child_stars
    # If a hierarchy node has explicit stellar descendants, render those leaves
    # rather than also rendering the unresolved parent as a physical sphere.
    return child_stars if child_stars else [node]


def _iter_hierarchy_subsystem_nodes(node: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(node, dict):
        return []
    nodes: List[Dict[str, Any]] = []
    component_type = str(node.get("component_type") or node.get("component_family") or "")
    node_kind = str(node.get("node_kind") or "")
    stable_key = str(node.get("stable_component_key") or "")
    if (
        component_type == "subsystem" or node_kind == "subsystem"
    ) and stable_key and not stable_key.startswith("synthetic:orbit:"):
        nodes.append(node)
    for child in node.get("children") or []:
        if isinstance(child, dict):
            nodes.extend(_iter_hierarchy_subsystem_nodes(child))
    return nodes


def _field_from_hierarchy_quick_fact(
    *,
    node: Dict[str, Any],
    facts: Dict[str, Any],
    key: str,
    label: str,
    unit: Optional[str],
    replacement_target: str,
) -> Dict[str, Any]:
    value = facts.get(key)
    synthetic = bool(node.get("synthetic")) or str(node.get("node_kind") or "") == "inferred_star_leaf"
    if value in (None, ""):
        return _simulation_field(
            key=key,
            label=label,
            value=None,
            unit=unit,
            status="missing",
            basis="canonical_hierarchy:missing_quick_fact",
            layer="none",
            confidence_tier="missing",
            replacement_target=replacement_target,
        )
    return _simulation_field(
        key=key,
        label=label,
        value=value,
        unit=unit,
        status="derived" if synthetic else "source",
        basis=(
            "canonical_hierarchy:msc_inferred_leaf_quick_fact"
            if synthetic
            else "canonical_hierarchy:core_quick_fact"
        ),
        layer="arm" if synthetic else "core",
        confidence_tier="illustrative" if synthetic else "high",
        replacement_target=replacement_target,
        source_catalog=node.get("source_catalog"),
        confidence=0.45 if synthetic else 0.9,
    )


def _teff_from_spectral_type(spectral_type: Any) -> Optional[float]:
    token = str(spectral_type or "").strip().upper()[:1]
    return {
        "O": 32000.0,
        "B": 18000.0,
        "A": 8500.0,
        "F": 6500.0,
        "G": 5600.0,
        "K": 4400.0,
        "M": 3300.0,
        "L": 2200.0,
        "T": 1200.0,
        "Y": 500.0,
        "D": 9000.0,
    }.get(token)


def _teff_from_mass_visual_proxy(mass_msun: Optional[float]) -> Optional[float]:
    if mass_msun is None:
        return None
    if mass_msun <= 0.08:
        return 2200.0
    if mass_msun <= 0.65:
        return 3300.0
    if mass_msun <= 0.9:
        return 4800.0
    if mass_msun <= 1.2:
        return 5800.0
    if mass_msun <= 1.7:
        return 7000.0
    return 8500.0


def _radius_from_mass_visual_proxy(mass_msun: Optional[float]) -> Optional[float]:
    if mass_msun is None:
        return None
    return round(max(0.08, min(2.8, mass_msun ** 0.8)), 6)


def _clone_assumed_visual_field(
    *,
    source_field: Optional[Dict[str, Any]],
    key: str,
    label: str,
    fallback_value: float,
    unit: Optional[str],
    seed: str,
    basis: str,
    scale: float = 1.0,
) -> Dict[str, Any]:
    value = _float_or_none((source_field or {}).get("value"))
    if value is None:
        value = fallback_value
    return _procedural_field(
        key=key,
        label=label,
        value=round(value * scale, 6),
        unit=unit,
        basis=basis,
        seed=seed,
        confidence=0.2,
        replacement_target=f"source {label.lower()} for this component",
    )


def _solution_by_orbit_edge_id(arm: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    for row in ((arm.get("orbital_solutions") or {}).get("items") or []):
        try:
            orbit_edge_id = int(row.get("orbit_edge_id"))
        except Exception:
            continue
        current = out.get(orbit_edge_id)
        if current is None or int(row.get("solution_rank") or 9999) < int(current.get("solution_rank") or 9999):
            out[orbit_edge_id] = row
    return out


def _planet_orbit_solutions_by_stable_key(arm: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    solution_by_edge_id = _solution_by_orbit_edge_id(arm)
    out: Dict[str, Dict[str, Any]] = {}
    for edge in ((arm.get("orbit_edges") or {}).get("items") or []):
        if str(edge.get("relation_kind") or "") != "planetary_orbit":
            continue
        secondary_key = str(edge.get("secondary_component_key") or "")
        prefix = "comp:planet:"
        if not secondary_key.startswith(prefix):
            continue
        try:
            orbit_edge_id = int(edge.get("orbit_edge_id"))
        except Exception:
            continue
        solution = solution_by_edge_id.get(orbit_edge_id)
        if not solution:
            continue
        stable_key = secondary_key[len(prefix):]
        out[stable_key] = solution
    return out


def _render_assumption_kind(field_key: str) -> str:
    if field_key in {"phase_rad", "inclination_deg", "eccentricity", "period_days", "orbital_period_days"}:
        return "simulation_default"
    if field_key in {"spectral_type_raw", "teff_k", "mass_msun"}:
        return "classification_hint"
    return "visual_default"


def _render_assumption_records(
    *,
    system: Dict[str, Any],
    build_id: Optional[str],
    owner_type: str,
    owner_key: str,
    display_name: Optional[str],
    fields: Dict[str, Any],
    source: Optional[Dict[str, Any]] = None,
    orbit_edge_id: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    source = source or {}
    for field_key, field in sorted((fields or {}).items()):
        if not isinstance(field, dict) or str(field.get("status") or "").lower() != "assumed":
            continue
        input_context = {
            "render_key": owner_key,
            "field_key": field_key,
            "seed": field.get("seed"),
            "basis": field.get("basis"),
            "source_layer": source.get("layer"),
        }
        record = {
            "object_type": owner_type,
            "system_id": system.get("system_id"),
            "star_id": source.get("star_id") if owner_type == "star" else None,
            "planet_id": source.get("planet_id") if owner_type == "planet" else None,
            "orbit_edge_id": orbit_edge_id if owner_type == "orbit" else None,
            "stable_object_key": source.get("stable_object_key"),
            "stable_component_key": source.get("stable_component_key") or source.get("source_component_key"),
            "render_key": owner_key,
            "display_name": display_name,
            "parameter_key": field.get("key") or field_key,
            "value": field.get("value"),
            "value_json": json.dumps(field.get("value"), sort_keys=True),
            "unit": field.get("unit"),
            "assumption_kind": _render_assumption_kind(str(field.get("key") or field_key)),
            "assumption_method": field.get("basis"),
            "assumption_version": field.get("generator_version") or SIM_PROCEDURAL_ASSUMPTION_VERSION,
            "input_context": input_context,
            "input_context_json": json.dumps(input_context, sort_keys=True),
            "replacement_target": field.get("replacement_target"),
            "visibility_label": "assumed",
            "layer": field.get("layer") or "disc_assumption",
            "seed": field.get("seed"),
            "generator_version": field.get("generator_version") or SIM_PROCEDURAL_ASSUMPTION_VERSION,
            "confidence": field.get("confidence"),
            "confidence_tier": field.get("confidence_tier"),
            "notes": field.get("notes"),
            "build_id": build_id,
            "field": field,
        }
        record["assumption_key"] = _simulation_assumption_key(record, build_id=build_id)
        records.append(record)
    return records


def _render_scene_contract(
    system: Dict[str, Any],
    stars: List[Dict[str, Any]],
    planets: List[Dict[str, Any]],
    arm: Dict[str, Any],
    simulation_readiness: Dict[str, Any],
    hierarchy: Optional[Dict[str, Any]] = None,
    build_id: Optional[str] = None,
    persisted_assumption_keys: Optional[set[str]] = None,
) -> Dict[str, Any]:
    star_readiness_by_id = {
        int(row["object_id"]): row
        for row in simulation_readiness.get("stars", [])
        if row.get("object_id") is not None
    }
    planet_readiness_by_id = {
        int(row["object_id"]): row
        for row in simulation_readiness.get("planets", [])
        if row.get("object_id") is not None
    }
    core_star_by_id = {int(row["star_id"]): row for row in stars if row.get("star_id") is not None}
    core_star_by_role = {
        str(row.get("component") or "").strip().upper()[:1]: row
        for row in stars
        if str(row.get("component") or "").strip()
    }
    equivalent_core_star_ids: Dict[int, set[int]] = {}
    star_identity_groups: Dict[tuple[str, str], set[int]] = {}
    for row in stars:
        try:
            star_id = int(row.get("star_id"))
        except Exception:
            continue
        for id_key in ("gaia_id", "hip_id", "hd_id", "wds_id"):
            id_value = str(row.get(id_key) or "").strip()
            if id_value:
                star_identity_groups.setdefault((id_key, id_value), set()).add(star_id)
    for id_group in star_identity_groups.values():
        if len(id_group) < 2:
            continue
        for star_id in id_group:
            equivalent_core_star_ids.setdefault(star_id, set()).update(id_group - {star_id})
    component_rows = ((arm.get("components") or {}).get("items") or [])
    orbit_rows = ((arm.get("orbit_edges") or {}).get("items") or [])
    solution_by_edge_id = _solution_by_orbit_edge_id(arm)
    orbit_component_keys = {
        str(edge.get(key) or "")
        for edge in orbit_rows
        for key in ("primary_component_key", "secondary_component_key")
        if edge.get(key)
    }
    components_by_key = {
        str(row.get("stable_component_key") or ""): row
        for row in component_rows
        if row.get("stable_component_key")
    }
    planet_components_by_key = {
        key: row
        for key, row in components_by_key.items()
        if str(row.get("component_type") or "") == "planet"
    }
    msc_endpoint_evidence_by_key: Dict[str, Dict[str, Any]] = {}
    for row in ((arm.get("msc_system_details") or {}).get("items") or []):
        for side in ("primary", "secondary"):
            component_key = str(row.get(f"{side}_component_key") or "")
            if not component_key or ":msc:wds:" not in component_key:
                continue
            mass = _float_or_none(row.get(f"mass_{side}_msun"))
            if mass is not None and mass <= 0:
                mass = None
            spectral_type = str(row.get(f"spectral_type_{side}") or "").strip()
            vmag = _float_or_none(row.get(f"vmag_{side}"))
            existing = msc_endpoint_evidence_by_key.get(component_key)
            existing_score = 0 if existing is None else sum(
                1 for key in ("spectral_type_raw", "mass_msun", "vmag") if existing.get(key) not in (None, "")
            )
            score = sum(1 for value in (spectral_type, mass, vmag) if value not in (None, ""))
            if existing is None or score > existing_score:
                msc_endpoint_evidence_by_key[component_key] = {
                    "spectral_type_raw": spectral_type or None,
                    "mass_msun": mass,
                    "mass_code": row.get(f"mass_code_{side}"),
                    "vmag": vmag,
                    "source_catalog": row.get("source_catalog") or "msc",
                    "source_reference": row.get("source_pk") or row.get("comment"),
                    "confidence": 0.85,
                }

    render_stars: Dict[str, Dict[str, Any]] = {}
    rendered_core_star_ids: set[int] = set()
    rendered_display_names: set[str] = set()

    def single_star_display_name() -> tuple[Optional[str], Optional[str]]:
        if len(stars) != 1:
            return None, None
        aliases = system.get("aliases") if isinstance(system.get("aliases"), list) else []
        preferred_alias_kinds = (
            "proper_name",
            "member_proper_name",
            "common_name",
            "iau_name",
            "planet_host_name",
        )
        for alias_kind in preferred_alias_kinds:
            for alias in aliases:
                if not isinstance(alias, dict) or str(alias.get("alias_kind") or "") != alias_kind:
                    continue
                value = str(alias.get("alias_raw") or "").strip()
                if value:
                    return value, f"system_alias:{alias_kind}"
        for key in ("display_name", "system_name"):
            value = str(system.get(key) or "").strip()
            if value:
                return value, f"system:{key}"
        return None, None

    def add_core_star(star: Dict[str, Any]) -> str:
        star_id = int(star.get("star_id") or -1)
        render_key = str(star.get("stable_object_key") or f"star:{star_id}")
        if render_key in render_stars:
            return render_key
        readiness = star_readiness_by_id.get(star_id) or {}
        fields = _field_map(readiness.get("fields") or [])
        body_class = _stellar_body_class(star)
        fields["object_type"] = _stellar_body_class_field(
            value=body_class,
            basis="core.stars:object_type_or_spectral_class",
            layer="core",
            source_catalog=star.get("source_catalog"),
        )
        display_name, display_name_basis = single_star_display_name()
        if not display_name:
            display_name = star.get("display_name") or star.get("star_name") or render_key
            display_name_basis = "core.star_name"
        render_stars[render_key] = {
            "render_key": render_key,
            "source_component_key": None,
            "object_type": "star",
            "body_class": body_class,
            "compact_type": _compact_type_for_body_class(body_class),
            "display_name": display_name,
            "component": star.get("component"),
            "spectral_class": _visual_star_color_class(star),
            "fields": fields,
            "source": {
                "layer": "core",
                "stable_object_key": star.get("stable_object_key"),
                "star_id": star.get("star_id"),
                "display_name_basis": display_name_basis,
            },
        }
        if star_id >= 0:
            rendered_core_star_ids.add(star_id)
        if render_stars[render_key].get("display_name"):
            rendered_display_names.add(str(render_stars[render_key]["display_name"]).strip().lower())
        return render_key

    def add_component_star(component: Dict[str, Any]) -> str:
        component_key = str(component.get("stable_component_key") or "")
        if not component_key:
            return ""
        if component_key in render_stars:
            return component_key
        core_id = component.get("core_object_id")
        if str(component.get("core_object_type") or "") == "star" and core_id is not None:
            try:
                star = core_star_by_id.get(int(core_id))
            except Exception:
                star = None
            if star:
                core_key = add_core_star(star)
                render_stars[component_key] = dict(render_stars[core_key])
                render_stars[component_key]["render_key"] = component_key
                render_stars[component_key]["source_component_key"] = component_key
                if len(stars) != 1:
                    render_stars[component_key]["display_name"] = component.get("display_name") or render_stars[core_key]["display_name"]
                if core_key != component_key:
                    render_stars.pop(core_key, None)
                if component.get("display_name"):
                    rendered_display_names.add(str(component.get("display_name")).strip().lower())
                return component_key

        label = str(component.get("catalog_component_label") or "").strip().upper()
        if len(label) == 1:
            star = core_star_by_role.get(label)
            if star:
                core_key = add_core_star(star)
                render_stars[component_key] = dict(render_stars[core_key])
                render_stars[component_key]["render_key"] = component_key
                render_stars[component_key]["source_component_key"] = component_key
                render_stars[component_key]["display_name"] = (
                    component.get("display_name") or render_stars[core_key]["display_name"]
                )
                render_stars[component_key]["component"] = (
                    component.get("catalog_component_label") or render_stars[component_key].get("component")
                )
                render_stars[component_key]["source"] = {
                    **(render_stars[component_key].get("source") or {}),
                    "stable_component_key": component_key,
                    "component_match_basis": "single_letter_component_role",
                }
                if core_key != component_key:
                    render_stars.pop(core_key, None)
                if component.get("display_name"):
                    rendered_display_names.add(str(component.get("display_name")).strip().lower())
                return component_key

        evidence = msc_endpoint_evidence_by_key.get(component_key) or {}
        seed = _stable_seed(system.get("stable_object_key"), component_key, "star_visual")
        spectral_type_raw = evidence.get("spectral_type_raw")
        source_mass = _float_or_none(evidence.get("mass_msun"))
        source_teff = _teff_from_spectral_type(spectral_type_raw)
        mass_proxy_teff = _teff_from_mass_visual_proxy(source_mass)
        radius_proxy = _radius_from_mass_visual_proxy(source_mass)
        spectral_class = _visual_star_color_class({}, fallback=spectral_type_raw) if spectral_type_raw else (
            _visual_star_color_class({}, fallback="M") if source_mass is not None and source_mass <= 0.65 else None
        )
        body_class = _stellar_body_class({"spectral_class": spectral_class, "spectral_type_raw": spectral_type_raw})
        render_stars[component_key] = {
            "render_key": component_key,
            "source_component_key": component_key,
            "object_type": "star",
            "body_class": body_class,
            "compact_type": _compact_type_for_body_class(body_class),
            "display_name": component.get("display_name") or component.get("catalog_component_label") or component_key,
            "component": component.get("catalog_component_label"),
            "spectral_class": spectral_class,
            "fields": {
                "object_type": _stellar_body_class_field(
                    value=body_class,
                    basis="arm.msc_system_details:spectral_type_body_class",
                    layer="arm",
                    source_catalog=evidence.get("source_catalog") or "msc",
                ),
                "spectral_type_raw": (
                    _simulation_field(
                        key="spectral_type_raw",
                        label="Spectral type",
                        value=spectral_type_raw,
                        unit=None,
                        status="source",
                        basis="arm.msc_system_details:endpoint_spectral_type",
                        layer="arm",
                        confidence_tier="medium",
                        replacement_target="component-specific reviewed spectral type",
                        source_catalog=evidence.get("source_catalog") or "msc",
                        source_reference=evidence.get("source_reference"),
                        confidence=_float_or_none(evidence.get("confidence")),
                    )
                    if spectral_type_raw
                    else _simulation_field(
                        key="spectral_type_raw",
                        label="Spectral type",
                        value=None,
                        unit=None,
                        status="missing",
                        basis="no_component_specific_spectral_type",
                        layer="arm",
                        confidence_tier="illustrative",
                        replacement_target="component-specific spectral type",
                    )
                ),
                "teff_k": (
                    _simulation_field(
                        key="teff_k",
                        label="Effective temperature",
                        value=source_teff,
                        unit="K",
                        status="derived",
                        basis="arm.msc_system_details:spectral_type_visual_proxy",
                        layer="arm",
                        confidence_tier="medium",
                        replacement_target="source effective temperature for this component",
                        source_catalog=evidence.get("source_catalog") or "msc",
                        source_reference=evidence.get("source_reference"),
                        confidence=0.45,
                    )
                    if source_teff is not None
                    else (
                        _simulation_field(
                            key="teff_k",
                            label="Effective temperature",
                            value=mass_proxy_teff,
                            unit="K",
                            status="derived",
                            basis="arm.msc_system_details:mass_visual_proxy",
                            layer="arm",
                            confidence_tier="illustrative",
                            replacement_target="source effective temperature for this component",
                            source_catalog=evidence.get("source_catalog") or "msc",
                            source_reference=evidence.get("source_reference"),
                            confidence=0.25,
                        )
                        if mass_proxy_teff is not None
                        else _procedural_field(
                            key="teff_k",
                            label="Effective temperature",
                            value=3200.0,
                            unit="K",
                            seed=seed,
                            basis="unknown_component_cool_visual_default",
                            confidence=0.1,
                            replacement_target="component-specific effective temperature",
                        )
                    )
                ),
                "mass_msun": (
                    _simulation_field(
                        key="mass_msun",
                        label="Mass",
                        value=source_mass,
                        unit="Msun",
                        status="source",
                        basis="arm.msc_system_details:endpoint_mass",
                        layer="arm",
                        confidence_tier="medium",
                        replacement_target="component-specific reviewed mass",
                        source_catalog=evidence.get("source_catalog") or "msc",
                        source_reference=evidence.get("source_reference"),
                        confidence=0.75,
                        notes=f"MSC mass code: {evidence.get('mass_code')}" if evidence.get("mass_code") else None,
                    )
                    if source_mass is not None
                    else _procedural_field(
                        key="mass_msun",
                        label="Mass",
                        value=0.35,
                        unit="Msun",
                        seed=seed,
                        basis="unknown_component_cool_visual_default",
                        confidence=0.1,
                        replacement_target="component-specific mass",
                    )
                ),
                "radius_rsun": (
                    _simulation_field(
                        key="radius_rsun",
                        label="Radius",
                        value=radius_proxy,
                        unit="Rsun",
                        status="derived",
                        basis="arm.msc_system_details:mass_radius_visual_proxy",
                        layer="arm",
                        confidence_tier="illustrative",
                        replacement_target="source radius for this component",
                        source_catalog=evidence.get("source_catalog") or "msc",
                        source_reference=evidence.get("source_reference"),
                        confidence=0.25,
                    )
                    if radius_proxy is not None
                    else _procedural_field(
                        key="radius_rsun",
                        label="Radius",
                        value=0.35,
                        unit="Rsun",
                        seed=seed,
                        basis="unknown_component_cool_visual_default",
                        confidence=0.1,
                        replacement_target="component-specific radius",
                    )
                ),
            },
            "source": {
                "layer": "arm",
                "stable_component_key": component_key,
                "physical_evidence": "msc_system_details" if evidence else "missing_component_specific_evidence",
            },
        }
        if render_stars[component_key].get("display_name"):
            rendered_display_names.add(str(render_stars[component_key]["display_name"]).strip().lower())
        return component_key

    def add_hierarchy_star(node: Dict[str, Any]) -> str:
        facts = node.get("quick_facts") if isinstance(node.get("quick_facts"), dict) else {}
        display_name = str(node.get("display_name") or node.get("stable_component_key") or "").strip()
        display_key = display_name.lower()
        if display_key and display_key in rendered_display_names:
            return ""
        try:
            core_id = int(node.get("core_object_id"))
        except Exception:
            core_id = -1
        if core_id >= 0 and core_id in rendered_core_star_ids:
            return ""
        render_key = _component_key_for_hierarchy_star_node(node)
        if not render_key or render_key in render_stars:
            return render_key
        spectral_type = facts.get("spectral_type_raw")
        teff = _float_or_none(facts.get("teff_k"))
        if teff is None and spectral_type:
            teff = _teff_from_spectral_type(spectral_type)
        mass = _float_or_none(facts.get("mass_msun"))
        radius = _float_or_none(facts.get("radius_rsun"))
        if radius is None:
            radius = _radius_from_mass_visual_proxy(mass)
        seed = _stable_seed(system.get("stable_object_key"), render_key, "hierarchy_star_visual")
        body_class = _stellar_body_class(
            {
                "object_type": node.get("object_type") or node.get("component_type"),
                "spectral_class": facts.get("spectral_class"),
                "spectral_type_raw": spectral_type,
            }
        )
        fields = {
            "object_type": _stellar_body_class_field(
                value=body_class,
                basis="canonical_hierarchy:object_type_or_spectral_class",
                layer="arm" if node.get("synthetic") else "core",
                source_catalog=node.get("source_catalog"),
            ),
            "spectral_type_raw": _field_from_hierarchy_quick_fact(
                node=node,
                facts=facts,
                key="spectral_type_raw",
                label="Spectral type",
                unit=None,
                replacement_target="component-specific spectral type",
            ),
            "teff_k": (
                _simulation_field(
                    key="teff_k",
                    label="Effective temperature",
                    value=teff,
                    unit="K",
                    status="derived" if node.get("synthetic") else "source",
                    basis=(
                        "canonical_hierarchy:spectral_type_visual_proxy"
                        if facts.get("teff_k") in (None, "") and spectral_type
                        else "canonical_hierarchy:quick_fact"
                    ),
                    layer="arm" if node.get("synthetic") else "core",
                    confidence_tier="illustrative" if node.get("synthetic") else "high",
                    replacement_target="source effective temperature for this component",
                    source_catalog=node.get("source_catalog"),
                    confidence=0.35 if node.get("synthetic") else 0.8,
                )
                if teff is not None
                else _procedural_field(
                    key="teff_k",
                    label="Effective temperature",
                    value=3200.0,
                    unit="K",
                    seed=seed,
                    basis="hierarchy_unknown_component_cool_visual_default",
                    confidence=0.1,
                    replacement_target="component-specific effective temperature",
                )
            ),
            "mass_msun": (
                _field_from_hierarchy_quick_fact(
                    node=node,
                    facts=facts,
                    key="mass_msun",
                    label="Mass",
                    unit="Msun",
                    replacement_target="component-specific mass",
                )
                if mass is not None
                else _procedural_field(
                    key="mass_msun",
                    label="Mass",
                    value=0.35,
                    unit="Msun",
                    seed=seed,
                    basis="hierarchy_unknown_component_cool_visual_default",
                    confidence=0.1,
                    replacement_target="component-specific mass",
                )
            ),
            "radius_rsun": (
                _simulation_field(
                    key="radius_rsun",
                    label="Radius",
                    value=radius,
                    unit="Rsun",
                    status="source" if facts.get("radius_rsun") not in (None, "") else "derived",
                    basis=(
                        "canonical_hierarchy:quick_fact"
                        if facts.get("radius_rsun") not in (None, "")
                        else "canonical_hierarchy:mass_radius_visual_proxy"
                    ),
                    layer="core" if facts.get("radius_rsun") not in (None, "") and not node.get("synthetic") else "arm",
                    confidence_tier="high" if facts.get("radius_rsun") not in (None, "") and not node.get("synthetic") else "illustrative",
                    replacement_target="source radius for this component",
                    source_catalog=node.get("source_catalog"),
                    confidence=0.25 if facts.get("radius_rsun") in (None, "") else 0.75,
                )
                if radius is not None
                else _procedural_field(
                    key="radius_rsun",
                    label="Radius",
                    value=0.35,
                    unit="Rsun",
                    seed=seed,
                    basis="hierarchy_unknown_component_cool_visual_default",
                    confidence=0.1,
                    replacement_target="component-specific radius",
                )
            ),
        }
        render_stars[render_key] = {
            "render_key": render_key,
            "source_component_key": render_key,
            "object_type": "star",
            "body_class": body_class,
            "compact_type": _compact_type_for_body_class(body_class),
            "display_name": display_name or render_key,
            "component": node.get("catalog_component_label") or node.get("member_role"),
            "spectral_class": _visual_star_color_class({}, fallback=facts.get("spectral_class") or spectral_type),
            "fields": fields,
            "source": {
                "layer": "arm" if node.get("synthetic") else "core",
                "stable_component_key": node.get("stable_component_key"),
                "canonical_key": node.get("canonical_key"),
                "node_kind": node.get("node_kind"),
            },
        }
        if core_id >= 0:
            rendered_core_star_ids.add(core_id)
        if display_key:
            rendered_display_names.add(display_key)
        return render_key

    has_stellar_orbit_edges = any(
        str(edge.get("relation_kind") or "") != "planetary_orbit"
        for edge in orbit_rows
    )
    for edge in orbit_rows:
        if has_stellar_orbit_edges and str(edge.get("relation_kind") or "") == "planetary_orbit":
            continue
        for key_name in ("primary_component_key", "secondary_component_key"):
            component_key = str(edge.get(key_name) or "")
            component = components_by_key.get(component_key)
            if component and str(component.get("component_type") or "") == "star":
                add_component_star(component)

    if not render_stars:
        for star in stars:
            add_core_star(star)
    else:
        for star in stars:
            if len(render_stars) < max(2, len(stars)) and not orbit_component_keys:
                add_core_star(star)

    hierarchy_star_nodes = _iter_hierarchy_render_star_nodes((hierarchy or {}).get("root"))
    hierarchy_star_count = int(((hierarchy or {}).get("counts") or {}).get("stars") or 0)
    if hierarchy_star_nodes and len(render_stars) < max(hierarchy_star_count, len(stars)):
        for node in hierarchy_star_nodes:
            if len(render_stars) >= max(hierarchy_star_count, len(stars)):
                break
            add_hierarchy_star(node)

    render_star_key_by_core_star_id: Dict[int, str] = {}
    for render_key, render_star in render_stars.items():
        source = render_star.get("source") if isinstance(render_star.get("source"), dict) else {}
        try:
            source_star_id = int(source.get("star_id"))
        except Exception:
            source_star_id = -1
        if source_star_id >= 0 and source_star_id not in render_star_key_by_core_star_id:
            render_star_key_by_core_star_id[source_star_id] = str(render_key)

    def resolve_planet_host_body_key(planet: Dict[str, Any]) -> tuple[Optional[str], str]:
        try:
            host_star_id = int(planet.get("star_id"))
        except Exception:
            host_star_id = -1
        if host_star_id >= 0:
            host_key = render_star_key_by_core_star_id.get(host_star_id)
            if host_key:
                return host_key, "core.planets.star_id_to_render_star"
            for equivalent_star_id in sorted(equivalent_core_star_ids.get(host_star_id, set())):
                host_key = render_star_key_by_core_star_id.get(equivalent_star_id)
                if host_key:
                    return host_key, "core.planets.star_id_catalog_equivalent_to_render_star"
            return None, "core.planets.star_id_unrendered"
        if len(render_stars) == 1:
            return next(iter(render_stars.keys())), "single_render_star_fallback"
        return None, "missing_or_ambiguous_host"

    def resolve_render_child_keys(component_key: str) -> List[str]:
        if component_key in render_stars:
            return [component_key]
        prefix = "comp:msc_group:wds:"
        if not component_key.startswith(prefix):
            return []
        group_ref = component_key[len(prefix):]
        if ":" not in group_ref:
            return []
        wds_id, group_label = group_ref.rsplit(":", 1)
        labels = {group_label.lower()}
        if group_label.lower() == "ab":
            labels = {"a", "b"}
        star_prefix = f"comp:msc:wds:{wds_id}:"
        resolved = []
        for render_key in sorted(render_stars):
            if not render_key.startswith(star_prefix):
                continue
            component_label = render_key[len(star_prefix):].lower()
            if any(component_label.startswith(label) for label in labels):
                resolved.append(render_key)
        return resolved

    def hierarchy_descendant_render_star_keys(node: Dict[str, Any]) -> List[str]:
        children = [child for child in (node.get("children") or []) if isinstance(child, dict)]
        if children:
            resolved: List[str] = []
            for child in children:
                resolved.extend(hierarchy_descendant_render_star_keys(child))
            return sorted(set(resolved))
        node_key = _component_key_for_hierarchy_star_node(node)
        if node_key in render_stars:
            return [node_key]
        stable_key = str(node.get("stable_component_key") or "")
        return resolve_render_child_keys(stable_key)

    render_subsystems: List[Dict[str, Any]] = []
    rendered_subsystem_keys: set[str] = set()
    for node in _iter_hierarchy_subsystem_nodes((hierarchy or {}).get("root")):
        subsystem_key = str(node.get("stable_component_key") or "")
        if not subsystem_key or subsystem_key in rendered_subsystem_keys:
            continue
        child_body_keys = resolve_render_child_keys(subsystem_key) or hierarchy_descendant_render_star_keys(node)
        if len(child_body_keys) < 2:
            continue
        display_name = str(node.get("display_name") or subsystem_key)
        component_label = node.get("catalog_component_label") or node.get("member_role") or node.get("component_type") or "subsystem"
        node_kind = node.get("node_kind") or node.get("component_type") or "subsystem"
        fields = {
            "component_label": _simulation_field(
                key="component_label",
                label="Component label",
                value=component_label,
                unit=None,
                status="source" if node.get("catalog_component_label") or node.get("member_role") else "derived",
                basis="canonical_hierarchy:component_label",
                layer="arm",
                confidence_tier="medium" if node.get("catalog_component_label") or node.get("member_role") else "illustrative",
                replacement_target="source-native subsystem/component label",
                source_catalog=node.get("source_catalog"),
                confidence=0.75 if node.get("catalog_component_label") or node.get("member_role") else 0.45,
            ),
            "hierarchy_basis": _simulation_field(
                key="hierarchy_basis",
                label="Hierarchy basis",
                value=node_kind,
                unit=None,
                status="derived",
                basis="canonical_hierarchy:render_subsystem_handle",
                layer="arm",
                confidence_tier="illustrative",
                replacement_target="reviewed subsystem hierarchy role and evidence chain",
                source_catalog=node.get("source_catalog"),
                confidence=0.55,
            ),
            "rendered_child_star_count": _simulation_field(
                key="rendered_child_star_count",
                label="Rendered child stars",
                value=len(child_body_keys),
                unit=None,
                status="derived",
                basis="canonical_hierarchy:descendant_render_star_count",
                layer="arm",
                confidence_tier="illustrative",
                replacement_target="reviewed subsystem child membership",
                source_catalog=node.get("source_catalog"),
                confidence=0.5,
            )
        }
        render_subsystems.append(
            {
                "render_key": subsystem_key,
                "object_type": "subsystem",
                "display_name": display_name,
                "component": node.get("catalog_component_label") or node.get("member_role"),
                "node_kind": node.get("node_kind"),
                "child_body_keys": child_body_keys,
                "fields": fields,
                "source": {
                    "layer": "arm",
                    "stable_component_key": subsystem_key,
                    "source_catalog": node.get("source_catalog"),
                    "basis": "canonical_hierarchy_subsystem",
                },
                "sort_index": len(render_subsystems),
            }
        )
        rendered_subsystem_keys.add(subsystem_key)

    render_orbits: List[Dict[str, Any]] = []
    for idx, edge in enumerate(orbit_rows):
        primary_key = str(edge.get("primary_component_key") or "")
        secondary_key = str(edge.get("secondary_component_key") or "")
        primary_child_keys = resolve_render_child_keys(primary_key)
        secondary_child_keys = resolve_render_child_keys(secondary_key)
        is_direct_star_orbit = primary_key in render_stars and secondary_key in render_stars
        is_group_orbit = (
            not is_direct_star_orbit
            and str(edge.get("relation_kind") or "") == "hierarchical_pair"
            and primary_child_keys
            and secondary_child_keys
        )
        if not is_direct_star_orbit and not is_group_orbit:
            continue
        try:
            orbit_edge_id = int(edge.get("orbit_edge_id"))
        except Exception:
            orbit_edge_id = idx
        solution = solution_by_edge_id.get(orbit_edge_id) or {}
        seed = _stable_seed(system.get("stable_object_key"), orbit_edge_id, primary_key, secondary_key)
        source_period = _float_or_none(solution.get("period_days"))
        source_sma = _float_or_none(solution.get("semi_major_axis_au"))
        source_ecc = _float_or_none(solution.get("eccentricity"))
        source_inc = _float_or_none(solution.get("inclination_deg"))
        assumed_period = round(0.65 + 24.0 * _seed_unit(seed, "period"), 6)
        assumed_radius = round(0.72 + 0.46 * _seed_unit(seed, "display_radius"), 6)
        render_orbits.append(
            {
                "orbit_key": f"orbit:{orbit_edge_id}",
                "orbit_edge_id": edge.get("orbit_edge_id"),
                "display_name": edge.get("edge_label") or f"{primary_key} - {secondary_key}",
                "relation_kind": edge.get("relation_kind") or "binary",
                "primary_body_key": primary_key,
                "secondary_body_key": secondary_key,
                "endpoint_kind": "star_pair" if is_direct_star_orbit else "group_pair",
                "primary_child_body_keys": primary_child_keys,
                "secondary_child_body_keys": secondary_child_keys,
                "barycenter_key": edge.get("barycenter_key"),
                "cluster_phase_rad": round(_seed_unit(seed, "cluster_phase") * math.pi * 2.0, 6),
                "display_radius_scene": assumed_radius if is_direct_star_orbit else round(1.45 + 0.72 * _seed_unit(seed, "display_radius"), 6),
                "fields": {
                    "period_days": (
                        _simulation_field(
                            key="period_days",
                            label="Orbital period",
                            value=source_period,
                            unit="days",
                            status="source",
                            basis=f"arm.orbital_solutions:{solution.get('solution_source_catalog') or 'source'}",
                            layer="arm",
                            confidence_tier=solution.get("confidence_tier") or "medium",
                            replacement_target="reviewed orbital period",
                            source_catalog=solution.get("solution_source_catalog") or solution.get("source_catalog"),
                            confidence=_float_or_none(solution.get("confidence_score")),
                        )
                        if source_period is not None
                        else _procedural_field(
                            key="period_days",
                            label="Orbital period",
                            value=assumed_period,
                            unit="days",
                            basis="bounded_binary_visual_period",
                            seed=seed,
                            replacement_target="source binary orbital period",
                        )
                    ),
                    "semi_major_axis_au": (
                        _simulation_field(
                            key="semi_major_axis_au",
                            label="Semi-major axis",
                            value=source_sma,
                            unit="au",
                            status="source",
                            basis=f"arm.orbital_solutions:{solution.get('solution_source_catalog') or 'source'}",
                            layer="arm",
                            confidence_tier=solution.get("confidence_tier") or "medium",
                            replacement_target="reviewed semi-major axis",
                            source_catalog=solution.get("solution_source_catalog") or solution.get("source_catalog"),
                            confidence=_float_or_none(solution.get("confidence_score")),
                        )
                        if source_sma is not None
                        else _procedural_field(
                            key="semi_major_axis_au",
                            label="Semi-major axis",
                            value=None,
                            unit="au",
                            basis="visual_separation_only_no_science_axis",
                            seed=seed,
                            confidence=0.0,
                            replacement_target="source binary semi-major axis",
                        )
                    ),
                    "eccentricity": (
                        _simulation_field(
                            key="eccentricity",
                            label="Eccentricity",
                            value=source_ecc,
                            unit=None,
                            status="source",
                            basis=f"arm.orbital_solutions:{solution.get('solution_source_catalog') or 'source'}",
                            layer="arm",
                            confidence_tier=solution.get("confidence_tier") or "medium",
                            replacement_target="reviewed eccentricity",
                            source_catalog=solution.get("solution_source_catalog") or solution.get("source_catalog"),
                            confidence=_float_or_none(solution.get("confidence_score")),
                        )
                        if source_ecc is not None
                        else _procedural_field(
                            key="eccentricity",
                            label="Eccentricity",
                            value=round(max(0.0, min(0.22, 0.08 + 0.07 * _seed_centered(seed, "ecc"))), 6),
                            unit=None,
                            basis="centered_low_eccentricity_visual_prior",
                            seed=seed,
                            replacement_target="source binary eccentricity",
                        )
                    ),
                    "inclination_deg": (
                        _simulation_field(
                            key="inclination_deg",
                            label="Inclination",
                            value=source_inc,
                            unit="deg",
                            status="source",
                            basis=f"arm.orbital_solutions:{solution.get('solution_source_catalog') or 'source'}",
                            layer="arm",
                            confidence_tier=solution.get("confidence_tier") or "medium",
                            replacement_target="reviewed inclination",
                            source_catalog=solution.get("solution_source_catalog") or solution.get("source_catalog"),
                            confidence=_float_or_none(solution.get("confidence_score")),
                        )
                        if source_inc is not None
                        else _procedural_field(
                            key="inclination_deg",
                            label="Inclination",
                            value=round(8.0 * _seed_centered(seed, "inc"), 6),
                            unit="deg",
                            basis="centered_low_inclination_visual_prior",
                            seed=seed,
                            replacement_target="source binary inclination",
                        )
                    ),
                    "phase_rad": _procedural_field(
                        key="phase_rad",
                        label="Orbital phase",
                        value=round(_seed_unit(seed, "phase") * math.pi * 2.0, 6),
                        unit="rad",
                        basis="deterministic_visual_start_phase",
                        seed=seed,
                        confidence=0.0,
                        replacement_target="source epoch/periastron/mean anomaly",
                    ),
                },
                "source": {
                    "layer": "arm",
                    "source_catalog": edge.get("source_catalog"),
                    "confidence_tier": edge.get("confidence_tier"),
                    "confidence_score": edge.get("confidence_score"),
                },
            }
        )

    if not render_orbits and len(render_stars) == 2:
        primary_key, secondary_key = sorted(render_stars.keys())
        primary_name = str(render_stars[primary_key].get("display_name") or primary_key)
        secondary_name = str(render_stars[secondary_key].get("display_name") or secondary_key)
        seed = _stable_seed(system.get("stable_object_key"), primary_key, secondary_key, "visual_binary_fallback")
        render_orbits.append(
            {
                "orbit_key": f"visual-fallback:binary:{seed[:12]}",
                "orbit_edge_id": None,
                "display_name": f"{primary_name} - {secondary_name} visual binary fallback",
                "relation_kind": "visual_binary_fallback",
                "primary_body_key": primary_key,
                "secondary_body_key": secondary_key,
                "endpoint_kind": "star_pair",
                "primary_child_body_keys": [primary_key],
                "secondary_child_body_keys": [secondary_key],
                "barycenter_key": None,
                "cluster_phase_rad": round(_seed_unit(seed, "cluster_phase") * math.pi * 2.0, 6),
                "display_radius_scene": round(0.92 + 0.26 * _seed_unit(seed, "display_radius"), 6),
                "fields": {
                    "period_days": _procedural_field(
                        key="period_days",
                        label="Orbital period",
                        value=round(8.0 + 32.0 * _seed_unit(seed, "period"), 6),
                        unit="days",
                        basis="two_star_no_orbit_visual_period",
                        seed=seed,
                        confidence=0.0,
                        replacement_target="source binary orbital period",
                    ),
                    "semi_major_axis_au": _procedural_field(
                        key="semi_major_axis_au",
                        label="Semi-major axis",
                        value=None,
                        unit="au",
                        basis="two_star_no_orbit_visual_separation_only",
                        seed=seed,
                        confidence=0.0,
                        replacement_target="source binary semi-major axis",
                    ),
                    "eccentricity": _procedural_field(
                        key="eccentricity",
                        label="Eccentricity",
                        value=round(max(0.0, min(0.18, 0.06 + 0.05 * _seed_centered(seed, "ecc"))), 6),
                        unit=None,
                        basis="centered_low_eccentricity_visual_prior",
                        seed=seed,
                        confidence=0.0,
                        replacement_target="source binary eccentricity",
                    ),
                    "inclination_deg": _procedural_field(
                        key="inclination_deg",
                        label="Inclination",
                        value=round(7.0 * _seed_centered(seed, "inc"), 6),
                        unit="deg",
                        basis="centered_low_inclination_visual_prior",
                        seed=seed,
                        confidence=0.0,
                        replacement_target="source binary inclination",
                    ),
                    "phase_rad": _procedural_field(
                        key="phase_rad",
                        label="Orbital phase",
                        value=round(_seed_unit(seed, "phase") * math.pi * 2.0, 6),
                        unit="rad",
                        basis="deterministic_visual_start_phase",
                        seed=seed,
                        confidence=0.0,
                        replacement_target="source epoch/periastron/mean anomaly",
                    ),
                },
                "source": {
                    "layer": "disc_assumption",
                    "source_catalog": None,
                    "confidence_tier": "illustrative",
                    "confidence_score": 0.0,
                    "fallback_reason": "two_rendered_stars_without_source_orbit_edge",
                },
            }
        )

    render_planets: List[Dict[str, Any]] = []
    rendered_planet_keys: set[str] = set()
    planet_inclination_plane_refs = _planet_source_inclination_planes(planets, planet_readiness_by_id)
    for idx, planet in enumerate(planets):
        planet_id = int(planet.get("planet_id") or -1)
        readiness = planet_readiness_by_id.get(planet_id) or {}
        fields = _field_map(readiness.get("fields") or [])
        seed = _stable_seed(system.get("stable_object_key"), planet.get("stable_object_key"), "planet_visual")
        if "phase_rad" not in fields:
            fields["phase_rad"] = _procedural_field(
                key="phase_rad",
                label="Orbital phase",
                value=round(_seed_unit(seed, "phase") * math.pi * 2.0, 6),
                unit="rad",
                basis="deterministic_visual_start_phase",
                seed=seed,
                confidence=0.0,
                replacement_target="source transit epoch/periastron/mean anomaly",
            )
        inclination_field = fields.get("inclination_deg")
        if not isinstance(inclination_field, dict) or _float_or_none(inclination_field.get("value")) is None:
            inc = _float_or_none(planet.get("inclination_deg"))
            fields["inclination_deg"] = (
                _simulation_field(
                    key="inclination_deg",
                    label="Inclination",
                    value=inc,
                    unit="deg",
                    status="source",
                    basis="core/source inclination_deg",
                    layer="core",
                    confidence_tier="high",
                    replacement_target="source inclination with uncertainty",
                )
                if inc is not None
                else _planet_visual_inclination_prior(
                    seed=seed,
                    host_key=str(planet.get("star_id") or ""),
                    plane_refs=planet_inclination_plane_refs,
                    replacement_target="source planet inclination",
                )
            )
        fields["planet_visual_class"] = _planet_visual_class_field(fields, seed)
        host_body_key, host_resolution = resolve_planet_host_body_key(planet)
        render_planets.append(
            {
                "render_key": str(planet.get("stable_object_key") or f"planet:{planet_id}"),
                "object_type": "planet",
                "display_name": planet.get("planet_name") or planet.get("stable_object_key"),
                "host_star_id": planet.get("star_id"),
                "host_body_key": host_body_key,
                "fields": fields,
                "source": {
                    "layer": "core",
                    "stable_object_key": planet.get("stable_object_key"),
                    "planet_id": planet.get("planet_id"),
                    "host_resolution": host_resolution,
                },
                "sort_index": idx,
            }
        )
        stable_key = str(planet.get("stable_object_key") or "")
        if stable_key:
            rendered_planet_keys.add(stable_key)
            rendered_planet_keys.add(f"comp:planet:{stable_key}")

    hierarchy_planet_count = int(((hierarchy or {}).get("counts") or {}).get("type_counts", {}).get("planet") or 0)
    if len(render_planets) < hierarchy_planet_count:
        for idx, edge in enumerate(orbit_rows):
            if str(edge.get("relation_kind") or "") != "planetary_orbit":
                continue
            planet_key = str(edge.get("secondary_component_key") or "")
            if not planet_key or planet_key in rendered_planet_keys:
                continue
            component = planet_components_by_key.get(planet_key)
            if not component:
                continue
            try:
                orbit_edge_id = int(edge.get("orbit_edge_id"))
            except Exception:
                orbit_edge_id = -1
            solution = solution_by_edge_id.get(orbit_edge_id) or {}
            seed = _stable_seed(system.get("stable_object_key"), planet_key, "planet_visual")
            source_catalog = solution.get("solution_source_catalog") or solution.get("source_catalog")
            confidence_tier = solution.get("confidence_tier") or edge.get("confidence_tier") or "medium"
            confidence = _float_or_none(solution.get("confidence_score") or edge.get("confidence_score"))
            source_period = _float_or_none(solution.get("period_days"))
            source_sma = _float_or_none(solution.get("semi_major_axis_au"))
            source_ecc = _float_or_none(solution.get("eccentricity"))
            source_inc = _float_or_none(solution.get("inclination_deg"))
            fields = {
                "orbital_period_days": _simulation_field(
                    key="orbital_period_days",
                    label="Orbital period",
                    value=source_period,
                    unit="days",
                    status="source" if source_period is not None else "missing",
                    basis=(
                        f"arm.orbital_solutions:{source_catalog or 'source'}"
                        if source_period is not None
                        else "no period source value"
                    ),
                    layer="arm" if source_period is not None else "none",
                    confidence_tier=confidence_tier if source_period is not None else "missing",
                    replacement_target="source orbital period with uncertainty",
                    source_catalog=str(source_catalog) if source_catalog else None,
                    confidence=confidence if source_period is not None else None,
                ),
                "semi_major_axis_au": _simulation_field(
                    key="semi_major_axis_au",
                    label="Semi-major axis",
                    value=source_sma,
                    unit="au",
                    status="source" if source_sma is not None else "missing",
                    basis=(
                        f"arm.orbital_solutions:{source_catalog or 'source'}"
                        if source_sma is not None
                        else "no semi-major-axis source value"
                    ),
                    layer="arm" if source_sma is not None else "none",
                    confidence_tier=confidence_tier if source_sma is not None else "missing",
                    replacement_target="source semi-major axis or full orbital solution",
                    source_catalog=str(source_catalog) if source_catalog else None,
                    confidence=confidence if source_sma is not None else None,
                ),
                "eccentricity": (
                    _simulation_field(
                        key="eccentricity",
                        label="Eccentricity",
                        value=source_ecc,
                        unit=None,
                        status="source",
                        basis=f"arm.orbital_solutions:{source_catalog or 'source'}",
                        layer="arm",
                        confidence_tier=confidence_tier,
                        replacement_target="source eccentricity or reviewed orbital solution",
                        source_catalog=str(source_catalog) if source_catalog else None,
                        confidence=confidence,
                    )
                    if source_ecc is not None
                    else _procedural_field(
                        key="eccentricity",
                        label="Eccentricity",
                        value=0.0,
                        unit=None,
                        basis="circular_orbit_visual_default",
                        seed=seed,
                        confidence=0.0,
                        replacement_target="source eccentricity or reviewed orbital solution",
                    )
                ),
                "inclination_deg": (
                    _simulation_field(
                        key="inclination_deg",
                        label="Inclination",
                        value=source_inc,
                        unit="deg",
                        status="source",
                        basis=f"arm.orbital_solutions:{source_catalog or 'source'}",
                        layer="arm",
                        confidence_tier=confidence_tier,
                        replacement_target="source planet inclination",
                        source_catalog=str(source_catalog) if source_catalog else None,
                        confidence=confidence,
                    )
                    if source_inc is not None
                    else _planet_visual_inclination_prior(
                        seed=seed,
                        host_key=str(edge.get("primary_component_key") or edge.get("host_component_key") or ""),
                        plane_refs=planet_inclination_plane_refs,
                        replacement_target="source planet inclination",
                    )
                ),
                "radius_earth": _simulation_field(
                    key="radius_earth",
                    label="Radius",
                    value=None,
                    unit="Rearth",
                    status="missing",
                    basis="planet component discovered through arm orbit edge without direct core row in selected system payload",
                    layer="none",
                    confidence_tier="missing",
                    replacement_target="source planet radius with uncertainty",
                ),
                "phase_rad": _procedural_field(
                    key="phase_rad",
                    label="Orbital phase",
                    value=round(_seed_unit(seed, "phase") * math.pi * 2.0, 6),
                    unit="rad",
                    basis="deterministic_visual_start_phase",
                    seed=seed,
                    confidence=0.0,
                    replacement_target="source transit epoch/periastron/mean anomaly",
                ),
            }
            fields["planet_visual_class"] = _planet_visual_class_field(fields, seed)
            render_planets.append(
                {
                    "render_key": planet_key,
                    "object_type": "planet",
                    "display_name": component.get("display_name") or planet_key,
                    "host_body_key": edge.get("primary_component_key") or edge.get("host_component_key"),
                    "fields": fields,
                    "source": {
                        "layer": "arm",
                        "stable_component_key": planet_key,
                        "planet_id": component.get("core_object_id"),
                    },
                    "sort_index": len(render_planets) + idx,
                }
            )
            rendered_planet_keys.add(planet_key)
            if len(render_planets) >= hierarchy_planet_count:
                break

    def planet_render_sort_value(planet: Dict[str, Any]) -> tuple[int, float, int, str]:
        fields = planet.get("fields") if isinstance(planet.get("fields"), dict) else {}
        sma = _float_or_none((fields.get("semi_major_axis_au") or {}).get("value"))
        period = _float_or_none((fields.get("orbital_period_days") or {}).get("value"))
        original_index = int(planet.get("sort_index") or 0)
        label = str(planet.get("display_name") or planet.get("render_key") or "")
        if sma is not None and sma > 0:
            return (0, sma, original_index, label)
        if period is not None and period > 0:
            return (1, period, original_index, label)
        return (2, float(original_index), original_index, label)

    render_planets.sort(key=planet_render_sort_value)
    for idx, planet in enumerate(render_planets):
        planet["sort_index"] = idx

    assumption_records: List[Dict[str, Any]] = []
    for star in render_stars.values():
        assumption_records.extend(
            _render_assumption_records(
                system=system,
                build_id=build_id,
                owner_type="star",
                owner_key=str(star.get("render_key") or ""),
                display_name=star.get("display_name"),
                fields=star.get("fields") or {},
                source={
                    **(star.get("source") or {}),
                    "source_component_key": star.get("source_component_key"),
                },
            )
        )
    for planet in render_planets:
        assumption_records.extend(
            _render_assumption_records(
                system=system,
                build_id=build_id,
                owner_type="planet",
                owner_key=str(planet.get("render_key") or ""),
                display_name=planet.get("display_name"),
                fields=planet.get("fields") or {},
                source=planet.get("source") or {},
            )
        )
    for orbit in render_orbits:
        assumption_records.extend(
            _render_assumption_records(
                system=system,
                build_id=build_id,
                owner_type="orbit",
                owner_key=str(orbit.get("orbit_key") or ""),
                display_name=orbit.get("orbit_key"),
                fields=orbit.get("fields") or {},
                source=orbit.get("source") or {},
                orbit_edge_id=orbit.get("orbit_edge_id"),
            )
        )
    persisted_assumption_keys = persisted_assumption_keys or set()
    persisted_assumption_count = 0
    for assumption in assumption_records:
        if assumption.get("assumption_key") in persisted_assumption_keys:
            assumption["persistence_status"] = "persisted"
            assumption["persistence_table"] = "disc.simulation_assumptions"
            persisted_assumption_count += 1
        else:
            assumption["persistence_status"] = "transient"

    field_status_counts: Dict[str, int] = {"source": 0, "derived": 0, "assumed": 0, "missing": 0}
    for owner in [*render_stars.values(), *render_planets, *render_subsystems, *render_orbits]:
        fields = owner.get("fields") if isinstance(owner.get("fields"), dict) else {}
        for field in fields.values():
            if not isinstance(field, dict):
                continue
            status = str(field.get("status") or "missing").lower()
            field_status_counts[status] = int(field_status_counts.get(status, 0)) + 1
    orbit_endpoint_counts: Dict[str, int] = {}
    orbit_relation_counts: Dict[str, int] = {}
    for orbit in render_orbits:
        endpoint_kind = str(orbit.get("endpoint_kind") or "unknown")
        relation_kind = str(orbit.get("relation_kind") or "unknown")
        orbit_endpoint_counts[endpoint_kind] = int(orbit_endpoint_counts.get(endpoint_kind, 0)) + 1
        orbit_relation_counts[relation_kind] = int(orbit_relation_counts.get(relation_kind, 0)) + 1
    assumption_persistence_counts = {
        "persisted": persisted_assumption_count,
        "transient": max(0, len(assumption_records) - persisted_assumption_count),
    }

    return {
        "schema_version": "render_scene_v0.2",
        "assumption_generator_version": SIM_PROCEDURAL_ASSUMPTION_VERSION,
        "preferred_visualization": "live_3d",
        "fallback_visualization": "deterministic_snapshot",
        "time": {
            "default_days_per_second": 0.7,
            "phase_policy": "source epoch when present; otherwise deterministic disc visual prior",
        },
        "visual_scale": SIM_VISUAL_SCALE_POLICY,
        "bodies": {
            "stars": list(render_stars.values()),
            "planets": render_planets,
            "subsystems": render_subsystems,
        },
        "orbits": render_orbits,
        "assumptions": assumption_records,
        "assumption_count": len(assumption_records),
        "persisted_assumption_count": persisted_assumption_count,
        "diagnostics": {
            "body_counts": {
                "stars": len(render_stars),
                "planets": len(render_planets),
                "subsystems": len(render_subsystems),
            },
            "orbit_counts": {
                "total": len(render_orbits),
                "by_endpoint_kind": orbit_endpoint_counts,
                "by_relation_kind": orbit_relation_counts,
            },
            "field_status_counts": field_status_counts,
            "assumption_persistence_counts": assumption_persistence_counts,
        },
        "provenance_legend": {
            "source": "Catalog/source value from core or arm.",
            "derived": "Deterministic derived value; should be reviewed before stronger science claims.",
            "assumed": "Deterministic disc-layer visualization prior only.",
            "missing": "Required value not available.",
        },
    }


def _planet_environment_evidence(planet: Dict[str, Any], host_star: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    eq_temp = planet.get("eq_temp_k")
    insolation = planet.get("insol_earth")
    semi_major_axis = planet.get("semi_major_axis_au")
    luminosity_proxy = _stellar_luminosity_proxy_lsun(host_star or {}) if host_star else None
    candidate_insolation: Optional[float] = None
    candidate_eq_temp: Optional[float] = None
    evidence_basis = "missing"
    missing_reason = ""

    try:
        eq_value = float(eq_temp) if eq_temp is not None else None
    except Exception:
        eq_value = None
    try:
        insol_value = float(insolation) if insolation is not None else None
    except Exception:
        insol_value = None
    try:
        sma_value = float(semi_major_axis) if semi_major_axis is not None else None
    except Exception:
        sma_value = None

    if eq_value is not None:
        evidence_basis = "source_eq_temp"
        candidate_eq_temp = eq_value
    elif insol_value is not None and insol_value > 0:
        evidence_basis = "source_insolation"
        candidate_insolation = insol_value
        candidate_eq_temp = 278.5 * math.pow(insol_value, 0.25)
    elif sma_value is not None and sma_value > 0 and luminosity_proxy is not None:
        evidence_basis = "stellar_class_luminosity_proxy"
        candidate_insolation = luminosity_proxy / (sma_value * sma_value)
        candidate_eq_temp = 278.5 * math.pow(candidate_insolation, 0.25)
    else:
        if sma_value is None or sma_value <= 0:
            missing_reason = "missing semi-major axis for derived insolation"
        elif host_star is None:
            missing_reason = "missing host star row"
        else:
            missing_reason = "host spectral class/luminosity class is insufficient for proxy insolation"

    if candidate_insolation is None and insol_value is not None and insol_value > 0:
        candidate_insolation = insol_value

    broad_hz_candidate = False
    if candidate_insolation is not None:
        broad_hz_candidate = 0.35 <= candidate_insolation <= 1.70
    elif candidate_eq_temp is not None:
        broad_hz_candidate = 180.0 <= candidate_eq_temp <= 350.0

    try:
        mass_earth = planet.get("mass_earth")
        if mass_earth is None and planet.get("mass_jup") is not None:
            mass_earth = float(planet.get("mass_jup")) * 317.8
        mass_earth_value = float(mass_earth) if mass_earth is not None else None
    except Exception:
        mass_earth_value = None
    try:
        eccentricity_value = float(planet.get("eccentricity")) if planet.get("eccentricity") is not None else None
    except Exception:
        eccentricity_value = None

    nice_candidate = (
        broad_hz_candidate
        and mass_earth_value is not None
        and 0.3 <= mass_earth_value <= 8.0
        and (eccentricity_value is None or eccentricity_value <= 0.35)
    )
    return {
        "evidence_basis": evidence_basis,
        "missing_reason": missing_reason,
        "candidate_insol_earth": candidate_insolation,
        "candidate_eq_temp_k": candidate_eq_temp,
        "stellar_luminosity_proxy_lsun": luminosity_proxy,
        "broad_hz_candidate": broad_hz_candidate,
        "nice_planet_candidate": nice_candidate,
    }


def _attach_planet_environment_diagnostics(stars: List[Dict[str, Any]], planets: List[Dict[str, Any]]) -> Dict[str, Any]:
    stars_by_id = {int(row["star_id"]): row for row in stars if row.get("star_id") is not None}
    counts: Dict[str, int] = {
        "planets": len(planets),
        "source_eq_temp": 0,
        "source_insolation": 0,
        "proxy_derivable": 0,
        "missing_environment": 0,
        "broad_hz_candidates": 0,
        "nice_planet_candidates": 0,
    }
    missing_examples: List[Dict[str, Any]] = []
    for planet in planets:
        try:
            star_id = int(planet.get("star_id")) if planet.get("star_id") is not None else None
        except Exception:
            star_id = None
        evidence = _planet_environment_evidence(planet, stars_by_id.get(star_id) if star_id is not None else None)
        planet["environment_evidence"] = evidence
        basis = str(evidence.get("evidence_basis") or "missing")
        if basis == "source_eq_temp":
            counts["source_eq_temp"] += 1
        elif basis == "source_insolation":
            counts["source_insolation"] += 1
        elif basis == "stellar_class_luminosity_proxy":
            counts["proxy_derivable"] += 1
        else:
            counts["missing_environment"] += 1
            missing_examples.append(
                {
                    "planet_id": planet.get("planet_id"),
                    "planet_name": planet.get("planet_name"),
                    "stable_object_key": planet.get("stable_object_key"),
                    "reason": evidence.get("missing_reason") or "missing environment evidence",
                }
            )
        if evidence.get("broad_hz_candidate"):
            counts["broad_hz_candidates"] += 1
        if evidence.get("nice_planet_candidate"):
            counts["nice_planet_candidates"] += 1
    return {
        "counts": counts,
        "missing_examples": missing_examples[:12],
        "notes": [
            "Environment diagnostics prefer source equilibrium temperature, then source insolation, then a labeled stellar-class luminosity proxy.",
            "Broad HZ candidate is a triage signal only; it is not a canonical habitability claim.",
        ],
    }


def _disc_object_diagnostics(system: Dict[str, Any]) -> Dict[str, Any]:
    disc_path_raw = _resolve_disc_db_path()
    output: Dict[str, Any] = {
        "path": disc_path_raw,
        "exists": bool(disc_path_raw),
        "coolness": None,
        "snapshots": [],
        "errors": [],
    }
    if not disc_path_raw:
        return output
    disc_path = Path(disc_path_raw)
    system_id = int(system.get("system_id"))
    stable_key = str(system.get("stable_object_key") or "")
    con = None
    try:
        con = duckdb.connect(str(disc_path), read_only=True)
        if _duckdb_has_table(con, "coolness_scores"):
            cursor = con.execute(
                """
                SELECT *
                FROM coolness_scores
                WHERE system_id = ? OR stable_object_key = ?
                ORDER BY rank ASC
                LIMIT 1
                """,
                [system_id, stable_key],
            )
            rows = _rows_to_dicts(cursor)
            if rows:
                row = rows[0]
                output["coolness"] = {
                    "rank": row.get("rank"),
                    "score_total": row.get("score_total"),
                    "profile_id": row.get("profile_id"),
                    "profile_version": row.get("profile_version"),
                    "build_id": row.get("build_id"),
                    "counts": {
                        key: row.get(key)
                        for key in row
                        if key.endswith("_count")
                    },
                    "features": {
                        key: row.get(key)
                        for key in row
                        if key.endswith("_feature") or key.endswith("_score")
                    },
                    "explanation": _coolness_explanation_from_row(row),
                }
        if _duckdb_has_table(con, "snapshot_manifest"):
            rows = _rows_to_dicts(
                con.execute(
                    """
                    SELECT build_id, view_type, artifact_path, params_hash, width_px, height_px,
                           source_build_inputs_hash, created_at
                    FROM snapshot_manifest
                    WHERE object_type = 'system'
                      AND (system_id = ? OR stable_object_key = ?)
                    ORDER BY created_at DESC
                    LIMIT 12
                    """,
                    [system_id, stable_key],
                )
            )
            for row in rows:
                if row.get("build_id") and row.get("artifact_path"):
                    row["url"] = _snapshot_asset_url(str(row["build_id"]), str(row["artifact_path"]))
            output["snapshots"] = rows
    except Exception as exc:
        output["errors"].append(str(exc))
    finally:
        if con is not None:
            con.close()
    return output


def _object_readiness_item(
    *,
    key: str,
    status: str,
    label: str,
    detail: str,
    why: str = "",
    next_action: str = "",
    workspace: str = "",
) -> Dict[str, Any]:
    item: Dict[str, Any] = {
        "key": key,
        "status": status,
        "label": label,
        "detail": detail,
    }
    if why:
        item["why"] = why
    if next_action:
        item["next_action"] = next_action
    if workspace:
        item["workspace"] = workspace
    return item


def _component_label_from_key(component_by_key: Dict[str, Dict[str, Any]], key: Any) -> Optional[str]:
    key_text = str(key or "").strip()
    if not key_text:
        return None
    component = component_by_key.get(key_text) or {}
    label = str(component.get("display_name") or component.get("catalog_component_label") or "").strip()
    if label:
        return label
    parts = [part for part in key_text.split(":") if part]
    if not parts:
        return key_text
    return parts[-1].replace("-", " ").replace("_", " ").title()


def _orbit_edge_label(component_by_key: Dict[str, Dict[str, Any]], edge: Dict[str, Any]) -> str:
    primary = _component_label_from_key(component_by_key, edge.get("primary_component_key"))
    secondary = _component_label_from_key(component_by_key, edge.get("secondary_component_key"))
    host = _component_label_from_key(component_by_key, edge.get("host_component_key"))
    relation = str(edge.get("relation_kind") or "orbit").replace("_", " ")
    if primary and secondary:
        return f"{primary} - {secondary} ({relation})"
    if host and secondary:
        return f"{secondary} around {host} ({relation})"
    if host and primary:
        return f"{primary} around {host} ({relation})"
    return f"Orbit edge {edge.get('orbit_edge_id')}"


def _enrich_component_rows(
    component_rows: List[Dict[str, Any]],
    stars: List[Dict[str, Any]],
    planets: List[Dict[str, Any]],
) -> None:
    stars_by_id = {int(row["star_id"]): row for row in stars if row.get("star_id") is not None}
    planets_by_id = {int(row["planet_id"]): row for row in planets if row.get("planet_id") is not None}
    for row in component_rows:
        try:
            core_id = int(row.get("core_object_id"))
        except Exception:
            core_id = -1
        core_type = str(row.get("core_object_type") or "")
        if core_type == "star" and core_id in stars_by_id:
            star = stars_by_id[core_id]
            row["core_display_name"] = star.get("display_name") or star.get("star_name")
            row["spectral_class"] = star.get("spectral_class") or star.get("spectral_type")
            row["teff_k"] = star.get("teff_k")
        elif core_type == "planet" and core_id in planets_by_id:
            planet = planets_by_id[core_id]
            row["core_display_name"] = planet.get("planet_name")
            row["semi_major_axis_au"] = planet.get("semi_major_axis_au")
            row["orbital_period_days"] = planet.get("orbital_period_days")
            row["eq_temp_k"] = planet.get("eq_temp_k")
            row["sort_distance_au"] = planet.get("semi_major_axis_au")


def _arm_object_diagnostics(stars: List[Dict[str, Any]], planets: List[Dict[str, Any]], system: Dict[str, Any]) -> Dict[str, Any]:
    arm_path_raw = _resolve_arm_db_path()
    output: Dict[str, Any] = {
        "path": arm_path_raw,
        "exists": bool(arm_path_raw),
        "components": {"count": 0, "items": []},
        "hierarchy_edges": {"count": 0, "items": []},
        "orbit_edges": {"count": 0, "items": []},
        "orbital_solutions": {"count": 0, "items": []},
        "msc_system_details": {"count": 0, "items": []},
        "stellar_parameters": {"count": 0, "items": []},
        "derived_physical_parameters": {"count": 0, "items": []},
        "errors": [],
    }
    if not arm_path_raw:
        return output
    star_ids = [int(row["star_id"]) for row in stars if row.get("star_id") is not None]
    planet_ids = [int(row["planet_id"]) for row in planets if row.get("planet_id") is not None]
    con = None
    try:
        con = duckdb.connect(str(arm_path_raw), read_only=True)
        component_rows: List[Dict[str, Any]] = []
        component_by_key: Dict[str, Dict[str, Any]] = {}
        component_filters = ["(core_object_type = 'system' AND core_object_id = ?)"]
        component_params: List[Any] = [int(system.get("system_id"))]
        if star_ids:
            component_filters.append(f"(core_object_type = 'star' AND core_object_id IN ({','.join(['?'] * len(star_ids))}))")
            component_params.extend(star_ids)
        if planet_ids:
            component_filters.append(f"(core_object_type = 'planet' AND core_object_id IN ({','.join(['?'] * len(planet_ids))}))")
            component_params.extend(planet_ids)
        wds_id = str(system.get("wds_id") or "").strip()
        if wds_id:
            component_filters.append("stable_component_key = ?")
            component_params.append(f"comp:msc_system:wds:{wds_id}")
            component_filters.append("stable_component_key LIKE ?")
            component_params.append(f"comp:msc:wds:{wds_id}:%")
            component_filters.append("stable_component_key LIKE ?")
            component_params.append(f"comp:star:star:wds:{wds_id}:%")
        component_keys: List[str] = []
        if _duckdb_has_table(con, "component_entities"):
            rows = _rows_to_dicts(
                con.execute(
                    f"""
                    SELECT component_entity_id, stable_component_key, component_type,
                           core_object_type, core_object_id, display_name,
                           catalog_component_label, ra_deg, dec_deg, dist_pc,
                           source_catalog, source_version, source_pk
                    FROM component_entities
                    WHERE {' OR '.join(component_filters)}
                    ORDER BY component_type ASC, display_name ASC
                    LIMIT 120
                    """,
                    component_params,
                )
            )
            for row in rows:
                key = str(row.get("stable_component_key") or "")
                if not key or key in component_by_key:
                    continue
                component_by_key[key] = row
                component_rows.append(row)
            component_keys = list(component_by_key.keys())
        if component_keys and _duckdb_has_table(con, "system_hierarchy_edges"):
            placeholders = ",".join(["?"] * len(component_keys))
            rows = _rows_to_dicts(
                con.execute(
                    f"""
                    SELECT parent_component_key, child_component_key, edge_kind, member_role,
                           confidence_score, confidence_tier, source_catalog
                    FROM system_hierarchy_edges
                    WHERE parent_component_key IN ({placeholders})
                       OR child_component_key IN ({placeholders})
                    ORDER BY confidence_score DESC NULLS LAST
                    LIMIT 80
                    """,
                    [*component_keys, *component_keys],
                )
            )
            output["hierarchy_edges"] = {"count": len(rows), "items": rows[:120]}
            connected_keys = set(component_keys)
            for row in rows:
                for key_name in ("parent_component_key", "child_component_key"):
                    key = str(row.get(key_name) or "")
                    if key:
                        connected_keys.add(key)
            missing_keys = sorted(key for key in connected_keys if key not in component_by_key)
            if missing_keys and _duckdb_has_table(con, "component_entities"):
                placeholders = ",".join(["?"] * len(missing_keys))
                extra_rows = _rows_to_dicts(
                    con.execute(
                        f"""
                        SELECT component_entity_id, stable_component_key, component_type,
                               core_object_type, core_object_id, display_name,
                               catalog_component_label, ra_deg, dec_deg, dist_pc,
                               source_catalog, source_version, source_pk
                        FROM component_entities
                        WHERE stable_component_key IN ({placeholders})
                        ORDER BY component_type ASC, display_name ASC
                        LIMIT 160
                        """,
                        missing_keys,
                    )
                )
                for row in extra_rows:
                    key = str(row.get("stable_component_key") or "")
                    if not key or key in component_by_key:
                        continue
                    component_by_key[key] = row
                    component_rows.append(row)
            component_rows.sort(
                key=lambda row: (
                    str(row.get("component_type") or ""),
                    str(row.get("display_name") or row.get("stable_component_key") or ""),
                )
            )
            component_keys = list(component_by_key.keys())
        _enrich_component_rows(component_rows, stars, planets)
        output["components"] = {"count": len(component_rows), "items": component_rows[:120]}
        orbit_edge_ids: List[int] = []
        orbit_edge_by_id: Dict[int, Dict[str, Any]] = {}
        if component_keys and _duckdb_has_table(con, "orbit_edges"):
            placeholders = ",".join(["?"] * len(component_keys))
            rows = _rows_to_dicts(
                con.execute(
                    f"""
                    SELECT orbit_edge_id, host_component_key, primary_component_key, secondary_component_key,
                           relation_kind, barycenter_key, preferred_solution_id, confidence_score,
                           confidence_tier, source_catalog
                    FROM orbit_edges
                    WHERE host_component_key IN ({placeholders})
                       OR primary_component_key IN ({placeholders})
                       OR secondary_component_key IN ({placeholders})
                    ORDER BY confidence_score DESC NULLS LAST, orbit_edge_id ASC
                    LIMIT 80
                    """,
                    [*component_keys, *component_keys, *component_keys],
                )
            )
            orbit_connected_keys = {
                str(row.get(key_name) or "")
                for row in rows
                for key_name in ("host_component_key", "primary_component_key", "secondary_component_key")
                if row.get(key_name)
            }
            missing_orbit_keys = sorted(key for key in orbit_connected_keys if key not in component_by_key)
            if missing_orbit_keys and _duckdb_has_table(con, "component_entities"):
                placeholders = ",".join(["?"] * len(missing_orbit_keys))
                extra_rows = _rows_to_dicts(
                    con.execute(
                        f"""
                        SELECT component_entity_id, stable_component_key, component_type,
                               core_object_type, core_object_id, display_name,
                               catalog_component_label, ra_deg, dec_deg, dist_pc,
                               source_catalog, source_version, source_pk
                        FROM component_entities
                        WHERE stable_component_key IN ({placeholders})
                        ORDER BY component_type ASC, display_name ASC
                        LIMIT 160
                        """,
                        missing_orbit_keys,
                    )
                )
                for row in extra_rows:
                    key = str(row.get("stable_component_key") or "")
                    if not key or key in component_by_key:
                        continue
                    component_by_key[key] = row
                    component_rows.append(row)
                component_rows.sort(
                    key=lambda row: (
                        str(row.get("component_type") or ""),
                        str(row.get("display_name") or row.get("stable_component_key") or ""),
                    )
                )
                component_keys = list(component_by_key.keys())
                _enrich_component_rows(component_rows, stars, planets)
                output["components"] = {"count": len(component_rows), "items": component_rows[:120]}
            for row in rows:
                row["host_display_name"] = _component_label_from_key(component_by_key, row.get("host_component_key"))
                row["primary_display_name"] = _component_label_from_key(component_by_key, row.get("primary_component_key"))
                row["secondary_display_name"] = _component_label_from_key(component_by_key, row.get("secondary_component_key"))
                row["barycenter_display_name"] = _component_label_from_key(component_by_key, row.get("barycenter_key"))
                row["edge_label"] = _orbit_edge_label(component_by_key, row)
            orbit_edge_ids = [int(row["orbit_edge_id"]) for row in rows if row.get("orbit_edge_id") is not None]
            orbit_edge_by_id = {int(row["orbit_edge_id"]): row for row in rows if row.get("orbit_edge_id") is not None}
            output["orbit_edges"] = {"count": len(rows), "items": rows[:80]}
        if orbit_edge_ids and _duckdb_has_table(con, "orbital_solutions"):
            placeholders = ",".join(["?"] * len(orbit_edge_ids))
            rows = _rows_to_dicts(
                con.execute(
                    f"""
                    SELECT orbital_solution_id, orbit_edge_id, solution_source_catalog,
                           solution_rank, period_days, semi_major_axis_au,
                           semi_major_axis_arcsec, eccentricity, inclination_deg,
                           confidence_score, confidence_tier, normalization_method
                    FROM orbital_solutions
                    WHERE orbit_edge_id IN ({placeholders})
                    ORDER BY solution_rank ASC NULLS LAST, confidence_score DESC NULLS LAST
                    LIMIT 80
                    """,
                    orbit_edge_ids,
                )
            )
            for row in rows:
                edge = orbit_edge_by_id.get(int(row.get("orbit_edge_id") or -1), {})
                row["edge_label"] = edge.get("edge_label") or f"Orbit edge {row.get('orbit_edge_id')}"
                row["primary_display_name"] = edge.get("primary_display_name")
                row["secondary_display_name"] = edge.get("secondary_display_name")
                row["host_display_name"] = edge.get("host_display_name")
                row["relation_kind"] = edge.get("relation_kind")
            output["orbital_solutions"] = {"count": len(rows), "items": rows[:80]}
        if wds_id and _duckdb_has_table(con, "msc_system_details"):
            rows = _rows_to_dicts(
                con.execute(
                    """
                    SELECT msc_system_detail_id, wds_id, primary_label, secondary_label,
                           parent_label, parent_component_key, primary_component_key,
                           secondary_component_key, system_type, period_days,
                           separation_arcsec, separation_mas, position_angle_deg,
                           vmag_primary, spectral_type_primary, vmag_secondary,
                           spectral_type_secondary, mass_primary_msun,
                           mass_code_primary, mass_secondary_msun,
                           mass_code_secondary, comment, source_catalog,
                           source_version, source_pk
                    FROM msc_system_details
                    WHERE wds_id = ?
                    ORDER BY parent_label ASC, primary_label ASC, secondary_label ASC
                    LIMIT 160
                    """,
                    [wds_id],
                )
            )
            output["msc_system_details"] = {"count": len(rows), "items": rows[:120]}
        if star_ids and _duckdb_has_table(con, "stellar_parameters"):
            placeholders = ",".join(["?"] * len(star_ids))
            rows = _rows_to_dicts(
                con.execute(
                    f"""
                    SELECT star_id, stable_object_key, parameter_source, teff_k, radius_rsun,
                           mass_msun, luminosity_log10_lsun, age_gyr, spectral_type_raw,
                           source_catalog
                    FROM stellar_parameters
                    WHERE star_id IN ({placeholders})
                    ORDER BY parameter_source ASC, star_id ASC
                    LIMIT 80
                    """,
                    star_ids,
                )
            )
            output["stellar_parameters"] = {"count": len(rows), "items": rows[:30]}
        derived_filters: List[str] = []
        derived_params: List[Any] = []
        if star_ids:
            derived_filters.append(f"(object_type = 'star' AND star_id IN ({','.join(['?'] * len(star_ids))}))")
            derived_params.extend(star_ids)
        if planet_ids:
            derived_filters.append(f"(object_type = 'planet' AND planet_id IN ({','.join(['?'] * len(planet_ids))}))")
            derived_params.extend(planet_ids)
        if derived_filters and _duckdb_has_table(con, "derived_physical_parameters"):
            rows = _rows_to_dicts(
                con.execute(
                    f"""
                    SELECT derived_parameter_id, build_id, object_type, system_id, star_id,
                           planet_id, stable_object_key, parameter_key, value, unit,
                           value_lo, value_hi, derivation_method, derivation_version,
                           input_parameters_json, assumptions_json, lossy_transform,
                           superseded_by_source, replacement_priority, confidence_score,
                           confidence_tier, review_status, source_catalog, source_version,
                           source_pk, source_row_hash
                    FROM derived_physical_parameters
                    WHERE {' OR '.join(derived_filters)}
                    ORDER BY object_type ASC, stable_object_key ASC, parameter_key ASC
                    LIMIT 200
                    """,
                    derived_params,
                )
            )
            output["derived_physical_parameters"] = {"count": len(rows), "items": rows[:120]}
    except Exception as exc:
        output["errors"].append(str(exc))
    finally:
        if con is not None:
            con.close()
    return output


def _system_object_diagnostics(system_id: int) -> Dict[str, Any]:
    public = _object_public_system_payload(system_id)
    system = public["system"]
    stars = public["stars"]
    planets = public["planets"]
    planet_environment = _attach_planet_environment_diagnostics(stars, planets)
    disc = _disc_object_diagnostics(system)
    arm = _arm_object_diagnostics(stars, planets, system)
    simulation_readiness = _simulation_readiness_diagnostics(stars, planets, arm)
    provenance = {
        "system": _provenance_diagnostics([system], "system", "system_id"),
        "stars": _provenance_diagnostics(stars, "star", "star_id"),
        "planets": _provenance_diagnostics(planets, "planet", "planet_id"),
    }
    arm_component_count = int((arm.get("components") or {}).get("count") or 0)
    orbit_edge_count = int((arm.get("orbit_edges") or {}).get("count") or 0)
    orbital_solution_count = int((arm.get("orbital_solutions") or {}).get("count") or 0)
    readiness = [
        _object_readiness_item(
            key="public_detail",
            status="ok",
            label="Public Detail",
            detail="Core system detail assembled successfully.",
            why="The public v1 detail contract can resolve this system from the current served core projection.",
            next_action="Use the Overview and Members tabs to inspect object identity and public links.",
        ),
        _object_readiness_item(
            key="coolness",
            status="ok" if disc.get("coolness") else "missing",
            label="Coolness",
            detail="disc.coolness_scores row found." if disc.get("coolness") else "No disc.coolness_scores row found for this system.",
            why=(
                "Presentation ranking has been materialized in disc for this served build."
                if disc.get("coolness")
                else "The current disc artifact does not contain a matching coolness row, or this object was outside the scored slice."
            ),
            next_action=(
                "Review score contributions in the Presentation tab."
                if disc.get("coolness")
                else "Run Score Coolness from the Builds workspace for the served build, then refresh diagnostics."
            ),
            workspace="Builds" if not disc.get("coolness") else "Object Diagnostics",
        ),
        _object_readiness_item(
            key="snapshot",
            status="ok" if disc.get("snapshots") else "missing",
            label="Snapshot",
            detail="Snapshot manifest row found." if disc.get("snapshots") else "No snapshot_manifest row found for this system.",
            why=(
                "disc.snapshot_manifest includes at least one generated or reused presentation artifact for this system."
                if disc.get("snapshots")
                else "Snapshot generation has not produced a manifest row for this system/view in the current disc artifact."
            ),
            next_action=(
                "Open the snapshot URL from the Presentation tab."
                if disc.get("snapshots")
                else "Generate snapshots from the Builds workspace. If using coolness-ranked generation, score coolness first."
            ),
            workspace="Builds" if not disc.get("snapshots") else "Object Diagnostics",
        ),
        _object_readiness_item(
            key="arm_graph",
            status="ok" if arm_component_count else "missing",
            label="Arm Graph",
            detail=f"{arm_component_count} component row(s), {orbit_edge_count} orbit edge(s).",
            why=(
                "arm.component_entities and graph edge tables have diagnostics-visible rows connected to this system."
                if arm_component_count
                else "No connected arm.component_entities rows were found for this system in the current arm artifact."
            ),
            next_action=(
                "Use Members and Graph / Orbits to inspect containment and dynamic relationships."
                if arm_component_count
                else "Verify arm.duckdb exists for the served build and that ingest emitted component_entities for this object."
            ),
            workspace="Object Diagnostics",
        ),
        _object_readiness_item(
            key="orbital_solutions",
            status="ok" if orbital_solution_count else ("missing" if orbit_edge_count else "not_applicable"),
            label="Orbital Solutions",
            detail=f"{orbital_solution_count} normalized orbital solution row(s).",
            why=(
                "At least one connected arm.orbital_solutions row is available for reconstruction or narration."
                if orbital_solution_count
                else (
                    "Dynamic orbit edges exist, but none have normalized orbital element rows attached."
                    if orbit_edge_count
                    else "No connected dynamic orbit edges were found; a normalized solution may not be expected for this object yet."
                )
            ),
            next_action=(
                "Inspect the solution rows in Graph / Orbits."
                if orbital_solution_count
                else (
                    "Inspect orbit edges and source coverage, then add orbital-solution evidence/proposals through the arm adjudication path."
                    if orbit_edge_count
                    else "No immediate action unless this object should have known orbital evidence."
                )
            ),
            workspace="Object Diagnostics" if orbital_solution_count or not orbit_edge_count else "Agency",
        ),
        _object_readiness_item(
            key="simulation_readiness",
            status=simulation_readiness.get("status") or "missing",
            label="Simulation Readiness",
            detail=(
                f"{simulation_readiness.get('required_field_count', 0)} checked field(s): "
                f"{simulation_readiness.get('counts', {}).get('source', 0)} source, "
                f"{simulation_readiness.get('counts', {}).get('derived', 0)} derived, "
                f"{simulation_readiness.get('counts', {}).get('assumed', 0)} assumed, "
                f"{simulation_readiness.get('counts', {}).get('missing', 0)} missing."
            ),
            why="The 3D simulator needs numeric stellar, planet, orbit, and environment inputs with explicit evidence basis.",
            next_action=(
                "Open the Simulation tab. Promote defensible derived science candidates into arm and keep visualization-only assumptions in disc."
                if simulation_readiness.get("required_field_count")
                else "No simulation readiness fields were available to inspect for this object."
            ),
            workspace="Object Diagnostics",
        ),
    ]
    missing_provenance = sum(int((item or {}).get("incomplete_count") or 0) for item in provenance.values())
    readiness.append(
        _object_readiness_item(
            key="provenance",
            status="ok" if missing_provenance == 0 else "warn",
            label="Provenance",
            detail=f"{missing_provenance} object row(s) with incomplete required provenance diagnostics.",
            why=(
                "Required source, retrieval, and transform fields are present on checked core rows."
                if missing_provenance == 0
                else "At least one checked core row is missing required provenance fields or a source row id/hash."
            ),
            next_action=(
                "No provenance repair needed for checked core rows."
                if missing_provenance == 0
                else "Open the Layers tab for examples, then fix the deterministic ingest/provenance emitter rather than editing artifacts."
            ),
            workspace="Dataset" if missing_provenance else "Object Diagnostics",
        )
    )
    return {
        "generated_at_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "build": {
            "build_id": system.get("snapshot", {}).get("build_id") if isinstance(system.get("snapshot"), dict) else None,
            "core_db_path": db.get_db_path(),
            "arm_db_path": _resolve_arm_db_path(),
            "disc_db_path": _resolve_disc_db_path(),
        },
        "public": public,
        "diagnostics": {
            "provenance": provenance,
            "disc": disc,
            "arm": arm,
            "planet_environment": planet_environment,
            "simulation_readiness": simulation_readiness,
            "readiness": readiness,
            "public_urls": {
                "api_detail": f"/api/v1/systems/{system_id}",
                "public_detail": f"/systems/{system_id}",
            },
        },
    }


def _system_simulation_scene_payload(system_id: int) -> Dict[str, Any]:
    with db.connection_scope() as con:
        build_id = fetch_build_id(con)
    public = _object_public_system_payload(system_id)
    system = public["system"]
    stars = public["stars"]
    planets = public["planets"]
    arm = _arm_object_diagnostics(stars, planets, system)
    simulation_readiness = _simulation_readiness_diagnostics(stars, planets, arm)
    persisted_assumption_keys = _load_persisted_simulation_assumption_keys(
        system_id,
        disc_db_path=_resolve_disc_db_path(),
        build_id=build_id,
    )
    render_scene = _render_scene_contract(
        system,
        stars,
        planets,
        arm,
        simulation_readiness,
        hierarchy=public.get("hierarchy"),
        build_id=build_id,
        persisted_assumption_keys=persisted_assumption_keys,
    )
    arm_public = {
        "components": arm.get("components") or {"count": 0, "items": []},
        "hierarchy_edges": arm.get("hierarchy_edges") or {"count": 0, "items": []},
        "orbit_edges": arm.get("orbit_edges") or {"count": 0, "items": []},
        "orbital_solutions": arm.get("orbital_solutions") or {"count": 0, "items": []},
        "msc_system_details": arm.get("msc_system_details") or {"count": 0, "items": []},
        "stellar_parameters": arm.get("stellar_parameters") or {"count": 0, "items": []},
        "derived_physical_parameters": arm.get("derived_physical_parameters") or {"count": 0, "items": []},
        "errors": arm.get("errors") or [],
    }
    return {
        "schema_version": "simulation_scene_v0",
        "scope": "system_simulation_scene",
        "generated_at_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "build_id": build_id,
        "frame": "heliocentric_icrs_j2016",
        "system": system,
        "bodies": {
            "stars": stars,
            "planets": planets,
        },
        "hierarchy": public.get("hierarchy"),
        "arm": arm_public,
        "simulation_readiness": simulation_readiness,
        "render_scene": render_scene,
        "policy": {
            "canonical_layer": "core",
            "derived_layer": "arm",
            "presentation_assumption_layer": "disc",
            "fiction_overlay_layer": "rim",
            "time_policy": "static_epoch_scene_until_client_simulation_clock_contract",
            "missing_orbit_policy": "do_not_invent_canonical_orbits",
            "agency_policy": "unreviewed_agency_output_must_not_write_core",
        },
        "links": {
            "detail": f"/api/v1/systems/{system_id}",
            "public_detail": f"/systems/{system_id}",
        },
    }


def _admin_search_system_by_id(
    con: duckdb.DuckDBPyConnection,
    *,
    system_id: int,
    disc_db_path: Optional[str],
    arm_db_path: Optional[str],
) -> Optional[Dict[str, Any]]:
    rows, _ = search_systems(
        con,
        q_norm=None,
        q_raw=None,
        system_id_exact=system_id,
        id_query=None,
        max_dist_ly=None,
        min_dist_ly=None,
        min_star_count=None,
        max_star_count=None,
        min_planet_count=None,
        max_planet_count=None,
        min_temp_k=None,
        max_temp_k=None,
        spectral_classes=[],
        has_planets=None,
        has_habitable=None,
        min_coolness_score=None,
        max_coolness_score=None,
        sort="name",
        match_mode=True,
        limit=1,
        include_total=False,
        cursor_values=None,
        disc_db_path=disc_db_path,
        arm_db_path=arm_db_path,
    )
    return rows[0] if rows else None


def _admin_core_system_id_for_object(con: duckdb.DuckDBPyConnection, object_type: Any, object_id: Any) -> Optional[int]:
    object_type_text = str(object_type or "")
    try:
        object_id_int = int(object_id)
    except Exception:
        return None
    if object_type_text == "system":
        return object_id_int
    if object_type_text == "star":
        row = con.execute("SELECT system_id FROM stars WHERE star_id = ? LIMIT 1", [object_id_int]).fetchone()
        return int(row[0]) if row and row[0] is not None else None
    if object_type_text == "planet":
        row = con.execute("SELECT system_id FROM planets WHERE planet_id = ? LIMIT 1", [object_id_int]).fetchone()
        return int(row[0]) if row and row[0] is not None else None
    return None


def _admin_resolve_component_system_id(
    core_con: duckdb.DuckDBPyConnection,
    arm_con: duckdb.DuckDBPyConnection,
    component: Dict[str, Any],
) -> Optional[int]:
    direct = _admin_core_system_id_for_object(
        core_con,
        component.get("core_object_type"),
        component.get("core_object_id"),
    )
    if direct is not None:
        return direct

    seen: set[str] = set()
    frontier = [str(component.get("stable_component_key") or "")]
    for _ in range(12):
        frontier = [key for key in frontier if key and key not in seen]
        if not frontier:
            break
        seen.update(frontier)
        placeholders = ",".join(["?"] * len(frontier))
        parents = _rows_to_dicts(
            arm_con.execute(
                f"""
                SELECT DISTINCT parent_component_key
                FROM system_hierarchy_edges
                WHERE child_component_key IN ({placeholders})
                ORDER BY parent_component_key ASC
                LIMIT 80
                """,
                frontier,
            )
        )
        parent_keys = [str(row.get("parent_component_key") or "") for row in parents if row.get("parent_component_key")]
        if not parent_keys:
            break
        parent_placeholders = ",".join(["?"] * len(parent_keys))
        parent_components = _rows_to_dicts(
            arm_con.execute(
                f"""
                SELECT stable_component_key, core_object_type, core_object_id
                FROM component_entities
                WHERE stable_component_key IN ({parent_placeholders})
                """,
                parent_keys,
            )
        )
        for parent in parent_components:
            resolved = _admin_core_system_id_for_object(
                core_con,
                parent.get("core_object_type"),
                parent.get("core_object_id"),
            )
            if resolved is not None:
                return resolved
        frontier = parent_keys
    return None


def _admin_component_search_matches(
    core_con: duckdb.DuckDBPyConnection,
    *,
    q_raw: Optional[str],
    q_norm: str,
    limit: int,
) -> List[Dict[str, Any]]:
    if not q_norm or len(q_norm) < 2:
        return []
    arm_path_raw = _resolve_arm_db_path()
    if not arm_path_raw:
        return []
    raw_text = str(q_raw or q_norm or "").strip().lower()
    norm_pattern = f"%{q_norm}%"
    raw_pattern = f"%{raw_text}%" if raw_text else norm_pattern
    arm_con = None
    out: List[Dict[str, Any]] = []
    try:
        arm_con = duckdb.connect(str(arm_path_raw), read_only=True)
        if not _duckdb_has_table(arm_con, "component_entities"):
            return []
        rows = _rows_to_dicts(
            arm_con.execute(
                """
                SELECT component_entity_id, stable_component_key, component_type,
                       core_object_type, core_object_id, display_name,
                       catalog_component_label, source_catalog, source_pk
                FROM component_entities
                WHERE coalesce(core_object_type, '') != 'system'
                  AND (
                    regexp_replace(lower(coalesce(display_name, '')), '[^a-z0-9]+', ' ', 'g') = ?
                    OR regexp_replace(lower(coalesce(display_name, '')), '[^a-z0-9]+', ' ', 'g') LIKE ?
                    OR regexp_replace(lower(coalesce(catalog_component_label, '')), '[^a-z0-9]+', ' ', 'g') = ?
                    OR regexp_replace(lower(coalesce(catalog_component_label, '')), '[^a-z0-9]+', ' ', 'g') LIKE ?
                    OR regexp_replace(lower(coalesce(stable_component_key, '')), '[^a-z0-9]+', ' ', 'g') LIKE ?
                    OR lower(coalesce(stable_component_key, '')) LIKE ?
                    OR lower(coalesce(source_pk, '')) LIKE ?
                  )
                ORDER BY
                  CASE
                    WHEN regexp_replace(lower(coalesce(display_name, '')), '[^a-z0-9]+', ' ', 'g') = ? THEN 0
                    WHEN regexp_replace(lower(coalesce(catalog_component_label, '')), '[^a-z0-9]+', ' ', 'g') = ? THEN 1
                    WHEN lower(coalesce(stable_component_key, '')) = ? THEN 2
                    ELSE 3
                  END,
                  CASE component_type
                    WHEN 'system' THEN 0
                    WHEN 'star' THEN 1
                    WHEN 'main_sequence' THEN 1
                    WHEN 'planet' THEN 2
                    WHEN 'subplanet' THEN 3
                    WHEN 'moon' THEN 4
                    WHEN 'artificial' THEN 5
                    WHEN 'minor_body' THEN 6
                    ELSE 9
                  END,
                  display_name ASC NULLS LAST,
                  stable_component_key ASC
                LIMIT ?
                """,
                [
                    q_norm,
                    norm_pattern,
                    q_norm,
                    norm_pattern,
                    norm_pattern,
                    raw_pattern,
                    raw_pattern,
                    q_norm,
                    q_norm,
                    raw_text,
                    max(limit * 3, limit),
                ],
            )
        )
        seen: set[tuple[int, str]] = set()
        for row in rows:
            system_id = _admin_resolve_component_system_id(core_con, arm_con, row)
            key = str(row.get("stable_component_key") or "")
            if system_id is None or not key:
                continue
            dedupe_key = (int(system_id), key)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            out.append(
                {
                    "system_id": int(system_id),
                    "diagnostic_focus": {"type": "component", "key": key},
                    "object_match": {
                        "type": "component",
                        "label": row.get("display_name") or row.get("catalog_component_label") or key,
                        "component_type": row.get("component_type"),
                        "stable_component_key": key,
                        "source_catalog": row.get("source_catalog"),
                    },
                }
            )
            if len(out) >= limit:
                break
    except Exception:
        return []
    finally:
        if arm_con is not None:
            arm_con.close()
    return out


def _actor_user_id_from_request(request: Request) -> Optional[int]:
    context = getattr(request.state, "auth_user", None)
    if not isinstance(context, dict):
        return None
    value = context.get("user_id")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _should_audit_systems_search(
    *,
    q_raw: Optional[str],
    min_dist_ly: Optional[float],
    max_dist_ly: Optional[float],
    min_star_count: Optional[int],
    max_star_count: Optional[int],
    min_planet_count: Optional[int],
    max_planet_count: Optional[int],
    min_temp_k: Optional[float],
    max_temp_k: Optional[float],
    has_habitable: Optional[bool],
    has_planets: Optional[bool],
    min_coolness_score: Optional[float],
    max_coolness_score: Optional[float],
    spectral_classes: list[str],
    returned_count: int,
) -> bool:
    if returned_count == 0:
        return True
    if str(q_raw or "").strip():
        return True
    return any(
        value is not None
        for value in (
            min_dist_ly,
            max_dist_ly,
            min_star_count,
            max_star_count,
            min_planet_count,
            max_planet_count,
            min_temp_k,
            max_temp_k,
            has_habitable,
            has_planets,
            min_coolness_score,
            max_coolness_score,
        )
    ) or bool(spectral_classes)


def _audit_systems_search(
    request: Request,
    *,
    q_raw: Optional[str],
    q_norm: str,
    id_query: Optional[Dict[str, Any]],
    sort_key: str,
    limit: int,
    min_dist_ly: Optional[float],
    max_dist_ly: Optional[float],
    min_star_count: Optional[int],
    max_star_count: Optional[int],
    min_planet_count: Optional[int],
    max_planet_count: Optional[int],
    min_temp_k: Optional[float],
    max_temp_k: Optional[float],
    has_habitable: Optional[bool],
    has_planets: Optional[bool],
    min_coolness_score: Optional[float],
    max_coolness_score: Optional[float],
    spectral_classes: list[str],
    returned_count: int,
    has_more: Optional[bool],
    total_count: Optional[int],
    outcome: str,
    duration_ms: int,
    error_message: Optional[str] = None,
) -> None:
    if not _should_audit_systems_search(
        q_raw=q_raw,
        min_dist_ly=min_dist_ly,
        max_dist_ly=max_dist_ly,
        min_star_count=min_star_count,
        max_star_count=max_star_count,
        min_planet_count=min_planet_count,
        max_planet_count=max_planet_count,
        min_temp_k=min_temp_k,
        max_temp_k=max_temp_k,
        has_habitable=has_habitable,
        has_planets=has_planets,
        min_coolness_score=min_coolness_score,
        max_coolness_score=max_coolness_score,
        spectral_classes=spectral_classes,
        returned_count=returned_count,
    ):
        return

    def _short(value: Optional[str], limit_len: int = 180) -> str:
        text = str(value or "").strip()
        if len(text) <= limit_len:
            return text
        return text[: limit_len - 3] + "..."

    filters: Dict[str, Any] = {}
    if min_dist_ly is not None or max_dist_ly is not None:
        filters["distance_ly"] = {"min": min_dist_ly, "max": max_dist_ly}
    if min_star_count is not None or max_star_count is not None:
        filters["star_count"] = {"min": min_star_count, "max": max_star_count}
    if min_planet_count is not None or max_planet_count is not None:
        filters["planet_count"] = {"min": min_planet_count, "max": max_planet_count}
    if min_temp_k is not None or max_temp_k is not None:
        filters["temperature_k"] = {"min": min_temp_k, "max": max_temp_k}
    if min_coolness_score is not None or max_coolness_score is not None:
        filters["coolness_score"] = {"min": min_coolness_score, "max": max_coolness_score}
    if spectral_classes:
        filters["spectral_classes"] = spectral_classes
    if has_planets is not None:
        filters["has_planets"] = has_planets
    if has_habitable is not None:
        filters["has_habitable"] = has_habitable

    details: Dict[str, Any] = {
        "query_raw": _short(q_raw),
        "query_norm": _short(q_norm),
        "sort": sort_key,
        "limit": limit,
        "id_query": id_query,
        "filters": filters,
        "response": {
            "returned_count": returned_count,
            "has_more": has_more,
            "total_count": total_count,
            "zero_results": returned_count == 0,
        },
        "outcome": outcome,
        "duration_ms": duration_ms,
    }
    if error_message:
        details["error"] = _short(error_message, limit_len=260)

    auth.audit_event(
        request,
        event_type="api.search.systems",
        result="error" if outcome == "conflict" else "success",
        actor_user_id=_actor_user_id_from_request(request),
        details=details,
    )


@app.on_event("startup")
def startup_checks():
    auth.initialize()


@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request_id = f"req_{uuid.uuid4().hex[:12]}"
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-Id"] = request_id
    return response


@app.middleware("http")
async def attach_auth_context(request: Request, call_next):
    try:
        auth.attach_auth_context(request)
    except Exception:
        request.state.auth_user = None
        request.state.clear_auth_cookie = True
    response = await call_next(request)
    if getattr(request.state, "clear_auth_cookie", False):
        auth.clear_auth_cookies(response)
    return response


@app.middleware("http")
async def mark_v1_admin_api_deprecated(request: Request, call_next):
    response = await call_next(request)
    path = str(request.url.path)
    if path.startswith("/api/v1/auth/") or path.startswith("/api/v1/admin/"):
        response.headers["Deprecation"] = "true"
        response.headers["Sunset"] = "Wed, 01 Jul 2026 00:00:00 GMT"
        response.headers["Link"] = '</api/v2/admin/ui>; rel="successor-version"'
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    if isinstance(exc.detail, dict):
        code = exc.detail.get("code", "bad_request")
        message = exc.detail.get("message", "Bad request")
        details = exc.detail.get("details", {})
    else:
        code = exc.detail if isinstance(exc.detail, str) else "bad_request"
        message = exc.detail if isinstance(exc.detail, str) else "Bad request"
        details = {}
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": code,
                "message": message,
                "details": details,
                "request_id": getattr(request.state, "request_id", None),
            }
        },
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "internal_error",
                "message": "Internal server error",
                "details": {},
                "request_id": getattr(request.state, "request_id", None),
            }
        },
    )


@app.exception_handler(DatabaseUnavailable)
async def db_unavailable_handler(request: Request, exc: DatabaseUnavailable):
    return JSONResponse(
        status_code=503,
        content={
            "error": {
                "code": "db_unavailable",
                "message": "Database not available",
                "details": {"reason": str(exc)},
                "request_id": getattr(request.state, "request_id", None),
            }
        },
    )


raw_cors = (os.getenv("SPACEGATE_CORS_ORIGINS") or "").strip()
if raw_cors:
    cors_origins = [origin.strip() for origin in raw_cors.split(",") if origin.strip()]
    allow_credentials = cors_origins != ["*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.get("/api/v2/auth/login/google")
@app.get("/api/v1/auth/login/google")
def auth_login_google(
    request: Request,
    next_path: Optional[str] = Query(default=None, alias="next"),
):
    return auth.login_redirect(request, next_path=next_path)


@app.get("/api/v2/auth/callback/google")
@app.get("/api/v1/auth/callback/google")
def auth_callback_google(
    request: Request,
    code: str = Query(...),
    state: str = Query(...),
):
    return auth.auth_callback(request, code=code, state=state)


@app.post("/api/v2/auth/logout")
@app.post("/api/v1/auth/logout")
def auth_logout(request: Request):
    return auth.logout(request)


@app.get("/api/v2/auth/me")
@app.get("/api/v1/auth/me")
def auth_me(request: Request):
    return auth.auth_me(request)


@app.get("/api/v1/health")
def health():
    with db.connection_scope() as con:
        build_id = fetch_build_id(con)
    return {
        "status": "ok",
        "build_id": build_id,
        "time_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }


@app.get("/api/v1/stats/spectral")
def spectral_mix():
    with db.connection_scope() as con:
        mix = fetch_spectral_mix(con)
        build_id = fetch_build_id(con)
    return {
        "status": "ok",
        "build_id": build_id,
        "total_stars": mix.get("total_stars", 0),
        "rows": mix.get("rows", []),
        "time_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }


@app.get("/api/v1/systems/search")
def systems_search(
    request: Request,
    q: Optional[str] = Query(default=None),
    max_dist_ly: Optional[float] = Query(default=None, ge=0),
    min_dist_ly: Optional[float] = Query(default=None, ge=0),
    min_star_count: Optional[int] = Query(default=None, ge=0),
    max_star_count: Optional[int] = Query(default=None, ge=0),
    min_planet_count: Optional[int] = Query(default=None, ge=0),
    max_planet_count: Optional[int] = Query(default=None, ge=0),
    min_temp_k: Optional[float] = Query(default=None, ge=0),
    max_temp_k: Optional[float] = Query(default=None, ge=0),
    has_habitable: Optional[str] = Query(default=None),
    min_coolness_score: Optional[float] = Query(default=None),
    max_coolness_score: Optional[float] = Query(default=None),
    spectral_class: Optional[str] = Query(default=None),
    has_planets: Optional[str] = Query(default=None),
    sort: str = Query(default="name"),
    limit: int = Query(default=50, ge=1, le=200),
    include_total: Optional[str] = Query(default=None),
    cursor: Optional[str] = Query(default=None),
):
    if max_dist_ly is not None and min_dist_ly is not None and min_dist_ly > max_dist_ly:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid distance range",
                "details": {},
            },
        )
    if max_star_count is not None and min_star_count is not None and min_star_count > max_star_count:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid star-count range",
                "details": {},
            },
        )
    if max_planet_count is not None and min_planet_count is not None and min_planet_count > max_planet_count:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid planet-count range",
                "details": {},
            },
        )
    if max_temp_k is not None and min_temp_k is not None and min_temp_k > max_temp_k:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid temperature range",
                "details": {},
            },
        )
    if (
        max_coolness_score is not None
        and min_coolness_score is not None
        and min_coolness_score > max_coolness_score
    ):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid coolness-score range",
                "details": {},
            },
        )

    q_norm = normalize_query_text(q or "")
    id_query = parse_identifier_query(q_norm)
    system_id_exact: Optional[int] = None
    system_id_match = re.match(r"^(?:system|sys)\s+(\d+)$", q_norm or "")
    if system_id_match:
        try:
            system_id_exact = int(system_id_match.group(1))
        except ValueError:
            system_id_exact = None

    sort_key = sort.lower() if sort else "name"
    if sort_key not in SUPPORTED_SEARCH_SORTS:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid sort option",
                "details": {
                    "sort": sort,
                    "allowed": sorted(SUPPORTED_SEARCH_SORTS),
                },
            },
        )

    disc_db_path = _resolve_disc_db_path()
    arm_db_path = _resolve_arm_db_path()

    match_mode = bool(q_norm) or bool(id_query)

    cursor_values: Optional[Dict[str, Any]] = None
    if cursor:
        try:
            cursor_values = decode_cursor(cursor)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "invalid_cursor",
                    "message": "Invalid cursor",
                    "details": {},
                },
            )
        if cursor_values.get("sort") and cursor_values.get("sort") != (
            "match" if match_mode else sort_key
        ):
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "invalid_cursor",
                    "message": "Invalid cursor",
                    "details": {},
                },
            )

    spectral_classes = parse_spectral_classes(spectral_class)
    invalid_spectral = [value for value in spectral_classes if value not in SUPPORTED_SPECTRAL_FILTERS]
    if invalid_spectral:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid spectral_class filter",
                "details": {
                    "invalid": invalid_spectral,
                    "allowed": sorted(SUPPORTED_SPECTRAL_FILTERS),
                },
            },
        )
    has_planets_bool = parse_bool(has_planets)
    if has_planets is not None and has_planets_bool is None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid has_planets filter",
                "details": {"value": has_planets, "allowed": ["true", "false"]},
            },
        )
    has_habitable_bool = parse_bool(has_habitable)
    if has_habitable is not None and has_habitable_bool is None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid has_habitable filter",
                "details": {"value": has_habitable, "allowed": ["true", "false"]},
            },
        )

    include_total_bool = parse_bool(include_total)
    if include_total is not None and include_total_bool is None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid include_total filter",
                "details": {"value": include_total, "allowed": ["true", "false"]},
            },
        )

    started_at = datetime.datetime.utcnow()

    try:
        with db.connection_scope() as con:
            rows, total_count = search_systems(
                con,
                q_norm=q_norm or None,
                q_raw=q,
                system_id_exact=system_id_exact,
                id_query=id_query,
                max_dist_ly=max_dist_ly,
                min_dist_ly=min_dist_ly,
                min_star_count=min_star_count,
                max_star_count=max_star_count,
                min_planet_count=min_planet_count,
                max_planet_count=max_planet_count,
                min_temp_k=min_temp_k,
                max_temp_k=max_temp_k,
                spectral_classes=spectral_classes,
                has_planets=has_planets_bool,
                has_habitable=has_habitable_bool,
                min_coolness_score=min_coolness_score,
                max_coolness_score=max_coolness_score,
                sort=sort_key,
                match_mode=match_mode,
                limit=limit + 1,
                include_total=bool(include_total_bool),
                cursor_values=cursor_values,
                disc_db_path=disc_db_path,
                arm_db_path=arm_db_path,
            )
    except ValueError as exc:
        _audit_systems_search(
            request,
            q_raw=q,
            q_norm=q_norm,
            id_query=id_query,
            sort_key=sort_key,
            limit=limit,
            min_dist_ly=min_dist_ly,
            max_dist_ly=max_dist_ly,
            min_star_count=min_star_count,
            max_star_count=max_star_count,
            min_planet_count=min_planet_count,
            max_planet_count=max_planet_count,
            min_temp_k=min_temp_k,
            max_temp_k=max_temp_k,
            has_habitable=has_habitable_bool,
            has_planets=has_planets_bool,
            min_coolness_score=min_coolness_score,
            max_coolness_score=max_coolness_score,
            spectral_classes=spectral_classes,
            returned_count=0,
            has_more=None,
            total_count=None,
            outcome="conflict",
            duration_ms=max(0, int((datetime.datetime.utcnow() - started_at).total_seconds() * 1000)),
            error_message=str(exc),
        )
        raise HTTPException(
            status_code=409,
            detail={
                "code": "conflict",
                "message": str(exc),
                "details": {},
            },
        )

    has_more = len(rows) > limit
    items = rows[:limit]
    for item in items:
        _attach_snapshot_url(item)
    next_cursor = None
    if has_more and items:
        last = items[-1]
        if match_mode:
            dist_value = last.get("dist_ly")
            if dist_value is None:
                dist_value = 1e12
            next_cursor = encode_cursor(
                {
                    "sort": "match",
                    "match_rank": last.get("match_rank"),
                    "dist": dist_value,
                    "name": last.get("system_name_norm") or "",
                    "id": last.get("system_id"),
                }
            )
        elif sort_key == "distance":
            dist_value = last.get("dist_ly")
            if dist_value is None:
                dist_value = 1e12
            next_cursor = encode_cursor(
                {
                    "sort": "distance",
                    "dist": dist_value,
                    "id": last.get("system_id"),
                }
            )
        elif sort_key == "coolness":
            rank_value = last.get("coolness_rank")
            if rank_value is None:
                rank_value = 9223372036854775807
            next_cursor = encode_cursor(
                {
                    "sort": sort_key,
                    "cool_rank": rank_value,
                    "name": last.get("system_name_norm") or "",
                    "id": last.get("system_id"),
                }
            )
        else:
            next_cursor = encode_cursor(
                {
                    "sort": "name",
                    "name": last.get("system_name_norm") or "",
                    "id": last.get("system_id"),
                }
            )

    duration_ms = max(0, int((datetime.datetime.utcnow() - started_at).total_seconds() * 1000))

    _audit_systems_search(
        request,
        q_raw=q,
        q_norm=q_norm,
        id_query=id_query,
        sort_key=sort_key,
        limit=limit,
        min_dist_ly=min_dist_ly,
        max_dist_ly=max_dist_ly,
        min_star_count=min_star_count,
        max_star_count=max_star_count,
        min_planet_count=min_planet_count,
        max_planet_count=max_planet_count,
        min_temp_k=min_temp_k,
        max_temp_k=max_temp_k,
        has_habitable=has_habitable_bool,
        has_planets=has_planets_bool,
        min_coolness_score=min_coolness_score,
        max_coolness_score=max_coolness_score,
        spectral_classes=spectral_classes,
        returned_count=len(items),
        has_more=has_more,
        total_count=total_count,
        outcome="success",
        duration_ms=duration_ms,
    )

    return {
        "items": items,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "total_count": total_count,
        "query_time_ms": duration_ms,
    }


@app.get("/api/v1/map/systems")
def map_systems(
    max_dist_ly: float = Query(default=100.0, ge=0, le=100),
    limit: int = Query(default=20000, ge=1, le=50000),
    compact: bool = Query(default=False),
):
    disc_db_path = _resolve_disc_db_path()
    try:
        with db.connection_scope() as con:
            return fetch_map_systems(
                con,
                max_dist_ly=max_dist_ly,
                limit=limit,
                disc_db_path=disc_db_path,
                compact=compact,
            )
    except DatabaseUnavailable:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "database_unavailable",
                "message": "Database not available",
                "details": {},
            },
        )


@app.get("/api/v1/systems/{system_id}/simulation-scene")
def system_simulation_scene(system_id: int):
    try:
        return _system_simulation_scene_payload(system_id)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "System not found",
                "details": {"system_id": system_id},
            },
        )


@app.get("/api/v1/systems/{system_id}")
def system_detail(system_id: int):
    disc_db_path = _resolve_disc_db_path()
    arm_db_path = _resolve_arm_db_path()
    canonical_hierarchy_db_path = _resolve_canonical_hierarchy_db_path()
    with db.connection_scope() as con:
        system = fetch_system_by_id(con, system_id)
        if not system:
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "not_found",
                    "message": "System not found",
                    "details": {"system_id": system_id},
                },
            )
        stars = fetch_stars_for_system(con, system_id)
        planets = fetch_planets_for_system(con, system_id)
        eclipsing_binaries = fetch_eclipsing_for_system(con, system_id)
        star_count, planet_count = fetch_counts_for_system(con, system_id)
        aliases = fetch_aliases_for_system(con, system_id)
        star_aliases = fetch_aliases_for_stars(
            con,
            [int(row.get("star_id")) for row in stars if row.get("star_id") is not None],
        )
        arm_star_evidence = fetch_arm_evidence_for_stars(
            con,
            [int(row.get("star_id")) for row in stars if row.get("star_id") is not None],
            arm_db_path=arm_db_path,
        )
        snapshot = fetch_snapshot_for_system(
            con,
            system_id=system_id,
            stable_object_key=system.get("stable_object_key"),
            disc_db_path=disc_db_path,
        )
        hierarchy = fetch_system_hierarchy_for_system(
            con,
            system_id=system_id,
            stable_object_key=system.get("stable_object_key"),
            wds_id=system.get("wds_id"),
            canonical_hierarchy_db_path=canonical_hierarchy_db_path,
            arm_db_path=arm_db_path,
        )

    effective_star_count = max(
        int(star_count or 0),
        int(((hierarchy or {}).get("counts") or {}).get("stars") or 0),
    )
    system["star_count"] = effective_star_count
    system["planet_count"] = planet_count
    system.update(summarize_star_temperatures(stars))
    system["snapshot"] = snapshot
    system["aliases"] = aliases
    system["arm_evidence_summary"] = _summarize_arm_star_evidence(arm_star_evidence)
    system_display_name, system_display_aliases = choose_display_name(
        system.get("system_name"),
        aliases,
    )
    system["display_name"] = system_display_name
    system["display_aliases"] = system_display_aliases
    for star in stars:
        sid = star.get("star_id")
        if sid is None:
            star["aliases"] = []
            star["display_name"] = star.get("star_name")
            star["display_aliases"] = []
            continue
        aliases_for_star = star_aliases.get(int(sid), [])
        star["aliases"] = aliases_for_star
        star_display_name, star_display_aliases = choose_display_name(
            star.get("star_name"),
            aliases_for_star,
        )
        star["display_name"] = star_display_name
        star["display_aliases"] = star_display_aliases
        star_arm_evidence = arm_star_evidence.get(int(sid), {})
        star["arm_evidence"] = star_arm_evidence
        star["arm_catalogs"] = star_arm_evidence.get("catalogs", [])
    _attach_snapshot_url(system)
    return {
        "system": system,
        "stars": stars,
        "planets": planets,
        "eclipsing_binaries": eclipsing_binaries,
        "hierarchy": hierarchy,
    }


@app.get("/api/v1/systems/by-key/{stable_object_key}")
def system_detail_by_key(stable_object_key: str):
    disc_db_path = _resolve_disc_db_path()
    arm_db_path = _resolve_arm_db_path()
    canonical_hierarchy_db_path = _resolve_canonical_hierarchy_db_path()
    with db.connection_scope() as con:
        system = fetch_system_by_key(con, stable_object_key)
        if not system:
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "not_found",
                    "message": "System not found",
                    "details": {"stable_object_key": stable_object_key},
                },
            )
        system_id = system.get("system_id")
        stars = fetch_stars_for_system(con, system_id)
        planets = fetch_planets_for_system(con, system_id)
        eclipsing_binaries = fetch_eclipsing_for_system(con, system_id)
        star_count, planet_count = fetch_counts_for_system(con, system_id)
        aliases = fetch_aliases_for_system(con, int(system_id))
        star_aliases = fetch_aliases_for_stars(
            con,
            [int(row.get("star_id")) for row in stars if row.get("star_id") is not None],
        )
        arm_star_evidence = fetch_arm_evidence_for_stars(
            con,
            [int(row.get("star_id")) for row in stars if row.get("star_id") is not None],
            arm_db_path=arm_db_path,
        )
        snapshot = fetch_snapshot_for_system(
            con,
            system_id=int(system_id),
            stable_object_key=stable_object_key,
            disc_db_path=disc_db_path,
        )
        hierarchy = fetch_system_hierarchy_for_system(
            con,
            system_id=int(system_id),
            stable_object_key=stable_object_key,
            wds_id=system.get("wds_id"),
            canonical_hierarchy_db_path=canonical_hierarchy_db_path,
            arm_db_path=arm_db_path,
        )

    effective_star_count = max(
        int(star_count or 0),
        int(((hierarchy or {}).get("counts") or {}).get("stars") or 0),
    )
    system["star_count"] = effective_star_count
    system["planet_count"] = planet_count
    system.update(summarize_star_temperatures(stars))
    system["snapshot"] = snapshot
    system["aliases"] = aliases
    system["arm_evidence_summary"] = _summarize_arm_star_evidence(arm_star_evidence)
    system_display_name, system_display_aliases = choose_display_name(
        system.get("system_name"),
        aliases,
    )
    system["display_name"] = system_display_name
    system["display_aliases"] = system_display_aliases
    for star in stars:
        sid = star.get("star_id")
        if sid is None:
            star["aliases"] = []
            star["display_name"] = star.get("star_name")
            star["display_aliases"] = []
            continue
        aliases_for_star = star_aliases.get(int(sid), [])
        star["aliases"] = aliases_for_star
        star_display_name, star_display_aliases = choose_display_name(
            star.get("star_name"),
            aliases_for_star,
        )
        star["display_name"] = star_display_name
        star["display_aliases"] = star_display_aliases
        star_arm_evidence = arm_star_evidence.get(int(sid), {})
        star["arm_evidence"] = star_arm_evidence
        star["arm_catalogs"] = star_arm_evidence.get("catalogs", [])
    _attach_snapshot_url(system)
    return {
        "system": system,
        "stars": stars,
        "planets": planets,
        "eclipsing_binaries": eclipsing_binaries,
        "hierarchy": hierarchy,
    }


@app.get("/api/v1/snapshots/{build_id}/{artifact_path:path}")
def system_snapshot_asset(build_id: str, artifact_path: str):
    out_root = (_state_dir() / "out" / build_id).resolve()
    candidate = (out_root / artifact_path).resolve()
    if out_root not in candidate.parents and candidate != out_root:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Snapshot not found",
                "details": {},
            },
        )
    if not candidate.is_file():
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Snapshot not found",
                "details": {},
            },
        )
    ext = candidate.suffix.lower()
    media_type = "image/svg+xml" if ext == ".svg" else "application/octet-stream"
    return FileResponse(
        str(candidate),
        media_type=media_type,
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )


admin_router = APIRouter()


class ActionRunRequest(BaseModel):
    action: str = Field(min_length=1, max_length=64)
    params: Dict[str, Any] = Field(default_factory=dict)
    confirmation: Optional[str] = Field(default=None, max_length=256)


class CoolnessPreviewRequest(BaseModel):
    profile_id: Optional[str] = Field(default=None, max_length=128)
    profile_version: Optional[str] = Field(default=None, max_length=128)
    weights: Dict[str, float] = Field(default_factory=dict)
    top_n: int = Field(default=200, ge=20, le=1000)


class DatasetSlicePreviewRequest(BaseModel):
    max_distance_ly: Optional[float] = Field(default=1000.0, gt=0.0)
    min_parallax_over_error: Optional[float] = Field(default=None, ge=0.0)
    max_parallax_error_mas: Optional[float] = Field(default=None, ge=0.0)
    max_ruwe: Optional[float] = Field(default=None, ge=0.0)
    require_spectral_class: bool = Field(default=False)
    require_color_index: bool = Field(default=False)
    allowed_spectral_classes: List[str] = Field(default_factory=list)


class AgencyPortfolioSeedRequest(BaseModel):
    stable_object_key: str = Field(min_length=1, max_length=240)
    object_type: str = Field(default="system", min_length=1, max_length=32)
    display_name: Optional[str] = Field(default=None, max_length=240)
    queue_reason: str = Field(default="operator_seed", min_length=1, max_length=80)
    queue_priority: str = Field(default="normal", min_length=1, max_length=32)
    source_build_id: Optional[str] = Field(default=None, max_length=160)
    source: str = Field(default="manual", min_length=1, max_length=80)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AgencySourceAllowlistEntryRequest(BaseModel):
    domain: str = Field(min_length=1, max_length=253)
    tier: int = Field(default=2, ge=0, le=4)
    org: Optional[str] = Field(default="", max_length=240)
    source_type: Optional[str] = Field(default="", max_length=120)
    trust_score: float = Field(default=0.9, ge=0.0, le=1.0)
    allowed_uses: List[str] = Field(default_factory=list)
    notes: Optional[str] = Field(default="", max_length=1000)
    enabled: bool = Field(default=True)


class AgencySourceAllowlistRestoreRequest(BaseModel):
    version_id: str = Field(min_length=1, max_length=160)


class InferenceEndpointRequest(BaseModel):
    endpoint_key: Optional[str] = Field(default=None, min_length=1, max_length=80)
    display_name: str = Field(min_length=1, max_length=160)
    provider: str = Field(default="openai_compatible", min_length=1, max_length=64)
    base_url: str = Field(min_length=1, max_length=500)
    auth_mode: str = Field(default="none", min_length=1, max_length=32)
    api_key_env: Optional[str] = Field(default=None, max_length=160)
    api_key: Optional[str] = Field(default=None, max_length=4096)
    default_model: Optional[str] = Field(default=None, max_length=240)
    role_defaults: Dict[str, str] = Field(default_factory=dict)
    timeout_s: int = Field(default=30, ge=1, le=600)
    enabled: bool = Field(default=True)
    notes: Optional[str] = Field(default=None, max_length=1000)


class InferenceEndpointUpdateRequest(BaseModel):
    endpoint_key: Optional[str] = Field(default=None, min_length=1, max_length=80)
    display_name: Optional[str] = Field(default=None, min_length=1, max_length=160)
    provider: Optional[str] = Field(default=None, min_length=1, max_length=64)
    base_url: Optional[str] = Field(default=None, min_length=1, max_length=500)
    auth_mode: Optional[str] = Field(default=None, min_length=1, max_length=32)
    api_key_env: Optional[str] = Field(default=None, max_length=160)
    api_key: Optional[str] = Field(default=None, max_length=4096)
    clear_api_key: bool = Field(default=False)
    default_model: Optional[str] = Field(default=None, max_length=240)
    role_defaults: Optional[Dict[str, str]] = Field(default=None)
    timeout_s: Optional[int] = Field(default=None, ge=1, le=600)
    enabled: Optional[bool] = Field(default=None)
    notes: Optional[str] = Field(default=None, max_length=1000)


class InferenceSmokeTestRequest(BaseModel):
    role: str = Field(default="discover", min_length=1, max_length=64)
    model_id: Optional[str] = Field(default=None, max_length=240)
    prompt: Optional[str] = Field(default=None, max_length=2000)
    temperature: float = Field(default=0.0, ge=0.0, le=2.0)
    max_tokens: int = Field(default=32, ge=1, le=512)


def _run_score_coolness_json(args: list[str]) -> Dict[str, Any]:
    cmd = [str(SCORE_COOLNESS_SCRIPT), *args]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(
            status_code=504,
            detail={
                "code": "timeout",
                "message": "Coolness command timed out",
                "details": {"command": args},
            },
        )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        message = stderr or stdout or f"score_coolness exited {proc.returncode}"
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": message,
                "details": {"command": args},
            },
        )
    raw = (proc.stdout or "").strip()
    if not raw:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "internal_error",
                "message": "Coolness command returned empty output",
                "details": {"command": args},
            },
        )
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "internal_error",
                "message": "Coolness command returned non-JSON output",
                "details": {"command": args, "output_prefix": raw[:300]},
            },
        )


def _is_duckdb_lock_conflict(exc: Exception) -> bool:
    message = str(exc)
    return (
        "Could not set lock on file" in message
        or "Conflicting lock is held" in message
        or "Can't open a connection to same database file" in message
    )


def _coolness_preview_from_disc_db(weights: Dict[str, float], top_n: int) -> Dict[str, Any]:
    core_db_path = Path(db.get_db_path())
    disc_db_path = core_db_path.with_name("disc.duckdb")
    if not disc_db_path.exists():
        raise HTTPException(
            status_code=409,
            detail={
                "code": "conflict",
                "message": "Missing disc.duckdb for current build; run score_coolness first",
                "details": {"disc_db_path": str(disc_db_path)},
            },
        )

    score_terms = []
    score_params: list[float] = []
    subscore_terms = []
    subscore_params: list[float] = []
    subscore_aliases: list[tuple[str, str]] = []
    for weight_key, feature_col in COOLNESS_WEIGHT_KEYS:
        score_terms.append(f"? * COALESCE({feature_col}, 0.0)")
        score_params.append(float(weights.get(weight_key, 0.0)))
        alias = f"sub_{weight_key}"
        subscore_terms.append(f"ROUND(100.0 * (? * COALESCE({feature_col}, 0.0)), 6) AS {alias}")
        subscore_params.append(float(weights.get(weight_key, 0.0)))
        subscore_aliases.append((weight_key, alias))
    score_expr = "100.0 * (" + " + ".join(score_terms) + ")"
    subscore_expr = ",\n    ".join(subscore_terms)

    sql = f"""
WITH ranked AS (
  SELECT
    system_id,
    stable_object_key,
    system_name,
    dist_ly,
    dominant_spectral_class,
    star_count,
    planet_count,
    nice_planet_count,
    weird_planet_count,
    {subscore_expr},
    ROUND({score_expr}, 6) AS score_total
  FROM coolness_scores
)
SELECT *
FROM ranked
ORDER BY score_total DESC, system_id ASC
LIMIT ?
    """
    params = [*subscore_params, *score_params, int(top_n)]
    con = None
    try:
        con = duckdb.connect(str(disc_db_path), read_only=True)
        cur = con.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    except duckdb.Error as exc:
        if _is_duckdb_lock_conflict(exc):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "conflict",
                    "message": "Coolness preview is temporarily unavailable while scoring is writing outputs; retry in a few seconds",
                    "details": {"error": str(exc), "disc_db_path": str(disc_db_path), "retryable": True},
                },
            )
        raise HTTPException(
            status_code=500,
            detail={
                "code": "internal_error",
                "message": "Failed to compute coolness diversity preview",
                "details": {"error": str(exc), "disc_db_path": str(disc_db_path)},
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "internal_error",
                "message": "Failed to compute coolness diversity preview",
                "details": {"error": str(exc), "disc_db_path": str(disc_db_path)},
            },
        )
    finally:
        if con is not None:
            con.close()

    spectral_counts: Dict[str, int] = {}
    with_planets = 0
    without_planets = 0
    multi_star = 0
    single_star = 0
    weird_planet_systems = 0
    for row in rows:
        spectral = str(row.get("dominant_spectral_class") or "?")
        spectral_counts[spectral] = spectral_counts.get(spectral, 0) + 1
        if int(row.get("planet_count") or 0) > 0:
            with_planets += 1
        else:
            without_planets += 1
        if int(row.get("star_count") or 0) > 1:
            multi_star += 1
        else:
            single_star += 1
        if int(row.get("weird_planet_count") or 0) > 0:
            weird_planet_systems += 1

    top_systems = []
    for row in rows[:25]:
        subscores: Dict[str, float] = {}
        for weight_key, alias in subscore_aliases:
            subscores[weight_key] = float(row.get(alias) or 0.0)
        top_systems.append(
            {
                "system_id": int(row["system_id"]),
                "stable_object_key": row.get("stable_object_key"),
                "system_name": row.get("system_name"),
                "dist_ly": float(row.get("dist_ly")) if row.get("dist_ly") is not None else None,
                "dominant_spectral_class": row.get("dominant_spectral_class"),
                "star_count": int(row.get("star_count") or 0),
                "planet_count": int(row.get("planet_count") or 0),
                "nice_planet_count": int(row.get("nice_planet_count") or 0),
                "weird_planet_count": int(row.get("weird_planet_count") or 0),
                "subscores": subscores,
                "score_total": float(row.get("score_total") or 0.0),
            }
        )

    spectral_distribution = [
        {"spectral_class": key, "systems": count}
        for key, count in sorted(spectral_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    probs = [float(item["systems"]) / float(len(rows)) for item in spectral_distribution if len(rows) > 0 and item["systems"] > 0]
    shannon_nats = -sum(p * math.log(p) for p in probs) if probs else 0.0
    shannon_bits = shannon_nats / math.log(2.0) if shannon_nats > 0 else 0.0
    effective_classes = math.exp(shannon_nats) if shannon_nats > 0 else 1.0
    max_classes = len(probs)
    shannon_normalized = (shannon_nats / math.log(max_classes)) if max_classes > 1 else 0.0

    return {
        "disc_db_path": str(disc_db_path),
        "top_n": int(top_n),
        "sample_size": len(rows),
        "diversity_scores": {
            "spectral_shannon_bits": round(shannon_bits, 6),
            "spectral_shannon_normalized": round(shannon_normalized, 6),
            "spectral_effective_classes": round(effective_classes, 6),
        },
        "type_distribution": {
            "with_planets": with_planets,
            "without_planets": without_planets,
            "multi_star": multi_star,
            "single_star": single_star,
            "weird_planet_systems": weird_planet_systems,
        },
        "spectral_distribution": spectral_distribution,
        "top_systems": top_systems,
    }


def _parse_human_bytes(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    match = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)$", text)
    if not match:
        return None
    number = float(match.group(1))
    unit_raw = match.group(2).strip().lower()
    unit = unit_raw.replace("ib", "i").replace("b", "")
    scale = {
        "bytes": 1,
        "byte": 1,
        "": 1,
        "k": 10**3,
        "m": 10**6,
        "g": 10**9,
        "t": 10**12,
        "ki": 2**10,
        "mi": 2**20,
        "gi": 2**30,
        "ti": 2**40,
    }.get(unit)
    if scale is None:
        return None
    return int(number * scale)


def _path_size_bytes(path: Path) -> int:
    try:
        if not path.exists():
            return 0
        if path.is_symlink():
            return 0
        if path.is_file():
            return int(path.stat().st_size)
    except OSError:
        return 0
    total = 0
    stack = [path]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_symlink():
                            continue
                        if entry.is_file(follow_symlinks=False):
                            total += int(entry.stat(follow_symlinks=False).st_size)
                        elif entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
                    except OSError:
                        continue
        except OSError:
            continue
    return int(total)


def _read_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_proc_key_values(path: Path, sep: str = ":") -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.exists():
        return out
    try:
        for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if sep not in raw:
                continue
            key, value = raw.split(sep, 1)
            out[key.strip()] = value.strip()
    except Exception:
        return out
    return out


def _proc_kib_value(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    match = re.search(r"([0-9]+)", str(value))
    if not match:
        return None
    return int(match.group(1)) * 1024


def _determinism_key(payload: Dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(payload.get("source_inputs_fingerprint") or ""),
        str(payload.get("transform_version") or ""),
        str(payload.get("build_layer") or ""),
        str(payload.get("slice_profile_id") or ""),
        str(payload.get("slice_profile_version") or ""),
    )


def _determinism_compare_tables(current: Dict[str, Any], baseline: Dict[str, Any]) -> Dict[str, Any]:
    fields = ("row_count", "xor_hash_hex", "min_hash_uint64", "max_hash_uint64")
    result: Dict[str, Any] = {"matches": True, "mismatches": [], "tables": {}}
    current_tables = current.get("table_fingerprints") or {}
    baseline_tables = baseline.get("table_fingerprints") or {}
    for table in ("stars", "systems", "planets"):
        c = current_tables.get(table) or {}
        b = baseline_tables.get(table) or {}
        field_matches: Dict[str, Any] = {}
        table_ok = True
        for field in fields:
            c_val = c.get(field)
            b_val = b.get(field)
            is_match = c_val == b_val
            field_matches[field] = {
                "match": bool(is_match),
                "current": c_val,
                "baseline": b_val,
            }
            if not is_match:
                table_ok = False
                result["mismatches"].append(
                    {
                        "table": table,
                        "field": field,
                        "current": c_val,
                        "baseline": b_val,
                    }
                )
        result["tables"][table] = {"match": bool(table_ok), "fields": field_matches}
        if not table_ok:
            result["matches"] = False
    return result


def _determinism_status_payload(
    *,
    state_dir: Path,
    reports_dir: Path,
    build_id: str,
) -> Dict[str, Any]:
    output: Dict[str, Any] = {
        "status": "missing_current_report",
        "current_report_exists": False,
        "current_build_id": build_id,
        "current_report_path": str(reports_dir / "determinism_report.json"),
        "baseline_build_id": None,
        "baseline_report_path": None,
        "baseline_generated_at": None,
        "comparable_baselines": 0,
        "comparison": {},
        "source_inputs_fingerprint": None,
        "transform_version": None,
        "build_layer": None,
        "slice_profile_id": None,
        "slice_profile_version": None,
        "current_report_error": None,
    }
    current_report_path = reports_dir / "determinism_report.json"
    if not current_report_path.exists():
        return output
    try:
        current = _read_json_file(current_report_path)
    except Exception as exc:
        output["status"] = "current_report_unreadable"
        output["current_report_error"] = str(exc)
        return output
    if not current:
        return output

    output["current_report_exists"] = True
    output["status"] = "no_baseline"
    output["source_inputs_fingerprint"] = str(current.get("source_inputs_fingerprint") or "")
    output["transform_version"] = str(current.get("transform_version") or "")
    output["build_layer"] = str(current.get("build_layer") or "")
    output["slice_profile_id"] = str(current.get("slice_profile_id") or "")
    output["slice_profile_version"] = str(current.get("slice_profile_version") or "")

    current_key = _determinism_key(current)
    all_reports_dir = state_dir / "reports"
    candidates: List[tuple[str, Dict[str, Any]]] = []
    if all_reports_dir.exists() and all_reports_dir.is_dir():
        for child in all_reports_dir.iterdir():
            if not child.is_dir():
                continue
            baseline_build_id = child.name
            if baseline_build_id == build_id:
                continue
            report_path = child / "determinism_report.json"
            if not report_path.exists():
                continue
            try:
                report = _read_json_file(report_path)
            except Exception:
                continue
            if not report:
                continue
            if _determinism_key(report) != current_key:
                continue
            candidates.append((baseline_build_id, report))

    output["comparable_baselines"] = len(candidates)
    if not candidates:
        return output

    candidates.sort(
        key=lambda row: (
            str((row[1] or {}).get("generated_at") or ""),
            str(row[0]),
        )
    )
    baseline_build_id, baseline_report = candidates[-1]
    compare = _determinism_compare_tables(current, baseline_report)
    output["baseline_build_id"] = baseline_build_id
    output["baseline_report_path"] = str(all_reports_dir / baseline_build_id / "determinism_report.json")
    output["baseline_generated_at"] = str(baseline_report.get("generated_at") or "")
    output["comparison"] = compare
    output["status"] = "match" if bool(compare.get("matches")) else "mismatch"
    return output


def _dataset_status_payload(*, force_refresh: bool) -> Dict[str, Any]:
    now = time.time()
    with db.connection_scope() as con:
        build_id = fetch_build_id(con) or "unknown"
    cached = _DATASET_STATUS_CACHE.get(build_id)
    if (
        not force_refresh
        and isinstance(cached, dict)
        and (now - float(cached.get("cached_at_ts", 0.0))) < DATASET_STATUS_CACHE_TTL_S
    ):
        payload = dict(cached["payload"])
        payload["cache"] = {
            "hit": True,
            "ttl_s": DATASET_STATUS_CACHE_TTL_S,
            "age_s": round(now - float(cached.get("cached_at_ts", now)), 3),
        }
        return payload

    state_dir = _state_dir().resolve()
    db_path = Path(db.get_db_path()).resolve()
    build_dir = db_path.parent if db_path.name == "core.duckdb" else db_path.parent
    reports_dir = state_dir / "reports" / build_id
    raw_dir = state_dir / "raw"
    cooked_dir = state_dir / "cooked"
    served_dir = state_dir / "served"
    out_dir = state_dir / "out"
    disc_db_path = db_path.with_name("disc.duckdb")
    arm_db_path = db_path.with_name("arm.duckdb")
    admin_db_path = admin_db.get_admin_db_path().resolve()

    timings_ms: Dict[str, float] = {}

    def _timed(name: str, fn):
        start = time.perf_counter()
        result = fn()
        timings_ms[name] = round((time.perf_counter() - start) * 1000.0, 3)
        return result

    qc_report = _read_json_file(reports_dir / "qc_report.json")
    system_grouping_report = _read_json_file(reports_dir / "system_grouping_report.json")
    gaia_backbone_report = _read_json_file(reports_dir / "gaia_backbone_report.json")
    slice_policy_report = _read_json_file(reports_dir / "slice_policy_report.json")
    match_report = _read_json_file(reports_dir / "match_report.json")
    catalog_contribution_report = _read_json_file(reports_dir / "catalog_contribution_report.json")
    catalog_pipeline_report = _read_json_file(state_dir / "reports" / "catalog_pipeline_report.json")
    coolness_report = _read_json_file(reports_dir / "coolness_report.json")
    determinism_status = _determinism_status_payload(
        state_dir=state_dir,
        reports_dir=reports_dir,
        build_id=str(build_id),
    )

    with db.connection_scope() as con:
        db_size_row = _timed("duckdb_database_size", lambda: con.execute("PRAGMA database_size").fetchone())
        db_size_cols = [desc[0] for desc in con.description]
        db_size = (
            {db_size_cols[idx]: db_size_row[idx] for idx in range(len(db_size_cols))}
            if db_size_row
            else {}
        )
        star_cols = {
            str(row[1])
            for row in con.execute("select * from pragma_table_info('stars')").fetchall()
        }
        system_cols = {
            str(row[1])
            for row in con.execute("select * from pragma_table_info('systems')").fetchall()
        }
        star_has_sbx = "sbx_sn" in star_cols
        system_has_sbx = "has_sbx_evidence" in system_cols

        basic_counts_row = _timed(
            "basic_dataset_counts",
            lambda: con.execute(
                """
                SELECT
                  (SELECT COUNT(*)::bigint FROM systems) AS systems,
                  (SELECT COUNT(*)::bigint FROM stars) AS stars,
                  (SELECT COUNT(*)::bigint FROM planets) AS planets
                """
            ).fetchone(),
        )

        source_breakdown_rows = _timed(
            "stars_by_source_catalog",
            lambda: con.execute(
                """
                SELECT source_catalog, COUNT(*)::bigint AS star_count
                FROM stars
                GROUP BY 1
                ORDER BY star_count DESC, source_catalog ASC
                """
            ).fetchall(),
        )

        spectral_rows = _timed(
            "stars_by_spectral_class",
            lambda: con.execute(
                """
                SELECT COALESCE(NULLIF(spectral_class, ''), '?') AS spectral_class, COUNT(*)::bigint AS star_count
                FROM stars
                GROUP BY 1
                ORDER BY star_count DESC, spectral_class ASC
                """
            ).fetchall(),
        )

        spectral_standard_row = _timed(
            "stars_by_spectral_standard",
            lambda: con.execute(
                """
                SELECT
                  SUM(CASE WHEN spectral_class = 'O' THEN 1 ELSE 0 END)::bigint AS class_o,
                  SUM(CASE WHEN spectral_class = 'B' THEN 1 ELSE 0 END)::bigint AS class_b,
                  SUM(CASE WHEN spectral_class = 'A' THEN 1 ELSE 0 END)::bigint AS class_a,
                  SUM(CASE WHEN spectral_class = 'F' THEN 1 ELSE 0 END)::bigint AS class_f,
                  SUM(CASE WHEN spectral_class = 'G' THEN 1 ELSE 0 END)::bigint AS class_g,
                  SUM(CASE WHEN spectral_class = 'K' THEN 1 ELSE 0 END)::bigint AS class_k,
                  SUM(CASE WHEN spectral_class = 'M' THEN 1 ELSE 0 END)::bigint AS class_m,
                  SUM(CASE WHEN spectral_class = 'L' THEN 1 ELSE 0 END)::bigint AS class_l,
                  SUM(CASE WHEN spectral_class = 'T' THEN 1 ELSE 0 END)::bigint AS class_t,
                  SUM(CASE WHEN spectral_class = 'Y' THEN 1 ELSE 0 END)::bigint AS class_y,
                  SUM(
                    CASE
                      WHEN UPPER(COALESCE(spectral_type_raw, '')) LIKE 'D%'
                        OR COALESCE(object_type, '') = 'white_dwarf'
                      THEN 1 ELSE 0
                    END
                  )::bigint AS class_d,
                  SUM(CASE WHEN spectral_class IS NULL OR spectral_class = '' THEN 1 ELSE 0 END)::bigint AS class_unknown
                FROM stars
                """
            ).fetchone(),
        )

        compact_row = _timed(
            "compact_object_inferred_counts",
            lambda: con.execute(
                """
                WITH tagged AS (
                  SELECT
                    CASE
                      WHEN UPPER(COALESCE(spectral_type_raw, '')) LIKE 'D%'
                        OR UPPER(COALESCE(spectral_type_raw, '')) LIKE '%WHITE%DWARF%'
                        OR COALESCE(object_type, '') = 'white_dwarf'
                      THEN 'white_dwarf'
                      WHEN UPPER(COALESCE(spectral_type_raw, '')) LIKE '%BLACK HOLE%'
                        OR UPPER(COALESCE(spectral_type_raw, '')) LIKE 'BH%'
                        OR COALESCE(object_type, '') = 'black_hole'
                      THEN 'black_hole'
                      WHEN UPPER(COALESCE(spectral_type_raw, '')) LIKE '%PULSAR%'
                        OR UPPER(COALESCE(spectral_type_raw, '')) LIKE '%PSR%'
                        OR COALESCE(object_type, '') = 'pulsar'
                      THEN 'pulsar'
                      WHEN UPPER(COALESCE(spectral_type_raw, '')) LIKE '%NEUTRON%'
                        OR UPPER(COALESCE(spectral_type_raw, '')) LIKE 'NS%'
                        OR COALESCE(object_type, '') = 'neutron_star'
                      THEN 'neutron_star'
                      ELSE NULL
                    END AS compact_type
                  FROM stars
                )
                SELECT
                  SUM(CASE WHEN compact_type = 'white_dwarf' THEN 1 ELSE 0 END)::bigint AS white_dwarf,
                  SUM(CASE WHEN compact_type = 'black_hole' THEN 1 ELSE 0 END)::bigint AS black_hole,
                  SUM(CASE WHEN compact_type = 'neutron_star' THEN 1 ELSE 0 END)::bigint AS neutron_star,
                  SUM(CASE WHEN compact_type = 'pulsar' THEN 1 ELSE 0 END)::bigint AS pulsar,
                  SUM(CASE WHEN compact_type IS NOT NULL THEN 1 ELSE 0 END)::bigint AS compact_total
                FROM tagged
                """
            ).fetchone(),
        )

        exotic_row = _timed(
            "exotic_star_counts",
            lambda: con.execute(
                """
                SELECT
                  SUM(CASE WHEN spectral_class IN ('L','T','Y') THEN 1 ELSE 0 END)::bigint AS brown_dwarf_like,
                  SUM(
                    CASE
                      WHEN UPPER(COALESCE(spectral_type_raw, '')) LIKE 'D%'
                        OR COALESCE(object_type, '') = 'white_dwarf'
                      THEN 1 ELSE 0
                    END
                  )::bigint AS white_dwarf_like,
                  SUM(
                    CASE
                      WHEN sqrt(
                        coalesce(pm_ra_mas_yr, 0.0) * coalesce(pm_ra_mas_yr, 0.0) +
                        coalesce(pm_dec_mas_yr, 0.0) * coalesce(pm_dec_mas_yr, 0.0)
                      ) >= 1000.0
                      THEN 1 ELSE 0
                    END
                  )::bigint AS high_proper_motion_ge_1000_mas_yr
                FROM stars
                """
            ).fetchone(),
        )

        star_mult_row = _timed(
            "star_multiplicity_breakdown",
            lambda: con.execute(
                f"""
                WITH flags AS (
                  SELECT
                    CASE
                      WHEN coalesce(gaia_non_single_star, false)
                        OR coalesce(gaia_nss_solution_count, 0) > 0
                        OR multiplicity_source_catalogs_json LIKE '%"gaia_nss"%'
                        OR multiplicity_source_catalogs_json LIKE '%"gaia_nss_two_body"%'
                      THEN 1 ELSE 0
                    END AS has_nss,
                    CASE
                      WHEN wds_id IS NOT NULL
                        OR multiplicity_source_catalogs_json LIKE '%"wds_gaia_xmatch"%'
                      THEN 1 ELSE 0
                    END AS has_wds,
                    CASE
                      WHEN source_catalog = 'msc'
                        OR multiplicity_source_catalogs_json LIKE '%"msc"%'
                      THEN 1 ELSE 0
                    END AS has_msc
                    ,
                    {("CASE WHEN sbx_sn IS NOT NULL THEN 1 ELSE 0 END" if star_has_sbx else "0")} AS has_sbx
                  FROM stars
                )
                SELECT
                  COUNT(*)::bigint AS total_stars,
                  SUM(CASE WHEN has_nss = 1 THEN 1 ELSE 0 END)::bigint AS nss_any,
                  SUM(CASE WHEN has_wds = 1 THEN 1 ELSE 0 END)::bigint AS wds_any,
                  SUM(CASE WHEN has_msc = 1 THEN 1 ELSE 0 END)::bigint AS msc_any,
                  SUM(CASE WHEN has_sbx = 1 THEN 1 ELSE 0 END)::bigint AS sbx_any,
                  SUM(CASE WHEN has_nss = 0 AND has_wds = 0 AND has_msc = 0 AND has_sbx = 0 THEN 1 ELSE 0 END)::bigint AS none,
                  SUM(CASE WHEN has_nss = 1 AND has_wds = 0 AND has_msc = 0 THEN 1 ELSE 0 END)::bigint AS nss_only,
                  SUM(CASE WHEN has_nss = 0 AND has_wds = 1 AND has_msc = 0 THEN 1 ELSE 0 END)::bigint AS wds_only,
                  SUM(CASE WHEN has_nss = 0 AND has_wds = 0 AND has_msc = 1 THEN 1 ELSE 0 END)::bigint AS msc_only,
                  SUM(CASE WHEN has_nss = 0 AND has_wds = 0 AND has_msc = 0 AND has_sbx = 1 THEN 1 ELSE 0 END)::bigint AS sbx_only,
                  SUM(CASE WHEN has_nss = 1 AND has_wds = 1 AND has_msc = 0 THEN 1 ELSE 0 END)::bigint AS nss_wds,
                  SUM(CASE WHEN has_nss = 1 AND has_wds = 0 AND has_msc = 1 THEN 1 ELSE 0 END)::bigint AS nss_msc,
                  SUM(CASE WHEN has_nss = 1 AND has_wds = 0 AND has_msc = 0 AND has_sbx = 1 THEN 1 ELSE 0 END)::bigint AS nss_sbx,
                  SUM(CASE WHEN has_nss = 0 AND has_wds = 1 AND has_msc = 1 THEN 1 ELSE 0 END)::bigint AS wds_msc,
                  SUM(CASE WHEN has_nss = 0 AND has_wds = 1 AND has_msc = 0 AND has_sbx = 1 THEN 1 ELSE 0 END)::bigint AS wds_sbx,
                  SUM(CASE WHEN has_nss = 0 AND has_wds = 0 AND has_msc = 1 AND has_sbx = 1 THEN 1 ELSE 0 END)::bigint AS msc_sbx,
                  SUM(CASE WHEN has_nss = 1 AND has_wds = 1 AND has_msc = 1 THEN 1 ELSE 0 END)::bigint AS nss_wds_msc,
                  SUM(CASE WHEN has_nss = 1 AND has_wds = 1 AND has_msc = 1 AND has_sbx = 1 THEN 1 ELSE 0 END)::bigint AS nss_wds_msc_sbx
                FROM flags
                """
            ).fetchone(),
        )

        system_mult_row = _timed(
            "system_multiplicity_breakdown",
            lambda: con.execute(
                f"""
                SELECT
                  COUNT(*)::bigint AS total_systems,
                  SUM(CASE WHEN has_gaia_nss_evidence THEN 1 ELSE 0 END)::bigint AS nss_any,
                  SUM(CASE WHEN has_wds_evidence THEN 1 ELSE 0 END)::bigint AS wds_any,
                  SUM(CASE WHEN has_msc_evidence THEN 1 ELSE 0 END)::bigint AS msc_any,
                  SUM(CASE WHEN {("has_sbx_evidence" if system_has_sbx else "false")} THEN 1 ELSE 0 END)::bigint AS sbx_any,
                  SUM(CASE WHEN NOT has_gaia_nss_evidence AND NOT has_wds_evidence AND NOT has_msc_evidence AND NOT {("has_sbx_evidence" if system_has_sbx else "false")} THEN 1 ELSE 0 END)::bigint AS none,
                  SUM(CASE WHEN has_gaia_nss_evidence AND NOT has_wds_evidence AND NOT has_msc_evidence THEN 1 ELSE 0 END)::bigint AS nss_only,
                  SUM(CASE WHEN NOT has_gaia_nss_evidence AND has_wds_evidence AND NOT has_msc_evidence THEN 1 ELSE 0 END)::bigint AS wds_only,
                  SUM(CASE WHEN NOT has_gaia_nss_evidence AND NOT has_wds_evidence AND has_msc_evidence THEN 1 ELSE 0 END)::bigint AS msc_only,
                  SUM(CASE WHEN NOT has_gaia_nss_evidence AND NOT has_wds_evidence AND NOT has_msc_evidence AND {("has_sbx_evidence" if system_has_sbx else "false")} THEN 1 ELSE 0 END)::bigint AS sbx_only,
                  SUM(CASE WHEN has_gaia_nss_evidence AND has_wds_evidence AND NOT has_msc_evidence THEN 1 ELSE 0 END)::bigint AS nss_wds,
                  SUM(CASE WHEN has_gaia_nss_evidence AND NOT has_wds_evidence AND has_msc_evidence THEN 1 ELSE 0 END)::bigint AS nss_msc,
                  SUM(CASE WHEN has_gaia_nss_evidence AND NOT has_wds_evidence AND NOT has_msc_evidence AND {("has_sbx_evidence" if system_has_sbx else "false")} THEN 1 ELSE 0 END)::bigint AS nss_sbx,
                  SUM(CASE WHEN NOT has_gaia_nss_evidence AND has_wds_evidence AND has_msc_evidence THEN 1 ELSE 0 END)::bigint AS wds_msc,
                  SUM(CASE WHEN NOT has_gaia_nss_evidence AND has_wds_evidence AND NOT has_msc_evidence AND {("has_sbx_evidence" if system_has_sbx else "false")} THEN 1 ELSE 0 END)::bigint AS wds_sbx,
                  SUM(CASE WHEN NOT has_gaia_nss_evidence AND NOT has_wds_evidence AND has_msc_evidence AND {("has_sbx_evidence" if system_has_sbx else "false")} THEN 1 ELSE 0 END)::bigint AS msc_sbx,
                  SUM(CASE WHEN has_gaia_nss_evidence AND has_wds_evidence AND has_msc_evidence THEN 1 ELSE 0 END)::bigint AS nss_wds_msc,
                  SUM(CASE WHEN has_gaia_nss_evidence AND has_wds_evidence AND has_msc_evidence AND {("has_sbx_evidence" if system_has_sbx else "false")} THEN 1 ELSE 0 END)::bigint AS nss_wds_msc_sbx
                FROM systems
                """
            ).fetchone(),
        )

        planet_row = _timed(
            "planet_habitable_breakdown",
            lambda: con.execute(
                """
                SELECT
                  COUNT(*)::bigint AS total_exoplanets,
                  SUM(CASE WHEN eq_temp_k BETWEEN 180 AND 320 THEN 1 ELSE 0 END)::bigint AS temperate_exoplanets,
                  SUM(
                    CASE
                      WHEN eq_temp_k BETWEEN 180 AND 320
                       AND radius_earth BETWEEN 0.5 AND 2.5
                      THEN 1 ELSE 0
                    END
                  )::bigint AS candidate_habitable_exoplanets
                FROM planets
                """
            ).fetchone(),
        )
        planet_environment_row = _timed(
            "planet_environment_coverage",
            lambda: con.execute(
                """
                WITH host_star AS (
                  SELECT
                    star_id,
                    CASE
                      WHEN COALESCE(luminosity_class, '') IN ('', 'V')
                       AND NOT regexp_matches(LOWER(COALESCE(spectral_type_raw, '')), 'giant|supergiant|\\biii\\b|\\bii\\b|\\biv\\b')
                      THEN
                        CASE COALESCE(spectral_class, '')
                          WHEN 'O' THEN 30000.0
                          WHEN 'B' THEN 1000.0
                          WHEN 'A' THEN 20.0
                          WHEN 'F' THEN 4.0
                          WHEN 'G' THEN 1.0
                          WHEN 'K' THEN 0.4
                          WHEN 'M' THEN 0.04
                          ELSE NULL
                        END
                      ELSE NULL
                    END AS luminosity_proxy_lsun
                  FROM stars
                ),
                planet_env AS (
                  SELECT
                    p.*,
                    hs.luminosity_proxy_lsun,
                    COALESCE(p.mass_earth, p.mass_jup * 317.8) AS planet_mass_earth,
                    CASE
                      WHEN p.insol_earth IS NOT NULL AND p.insol_earth > 0.0 THEN p.insol_earth
                      WHEN p.semi_major_axis_au IS NOT NULL
                       AND p.semi_major_axis_au > 0.0
                       AND hs.luminosity_proxy_lsun IS NOT NULL
                      THEN hs.luminosity_proxy_lsun / (p.semi_major_axis_au * p.semi_major_axis_au)
                      ELSE NULL
                    END AS candidate_insol_earth,
                    CASE
                      WHEN p.eq_temp_k IS NOT NULL THEN p.eq_temp_k
                      WHEN p.insol_earth IS NOT NULL AND p.insol_earth > 0.0
                      THEN 278.5 * POW(p.insol_earth, 0.25)
                      WHEN p.semi_major_axis_au IS NOT NULL
                       AND p.semi_major_axis_au > 0.0
                       AND hs.luminosity_proxy_lsun IS NOT NULL
                      THEN 278.5 * POW(hs.luminosity_proxy_lsun / (p.semi_major_axis_au * p.semi_major_axis_au), 0.25)
                      ELSE NULL
                    END AS candidate_eq_temp_k,
                    CASE
                      WHEN p.eq_temp_k IS NOT NULL THEN 'source_eq_temp'
                      WHEN p.insol_earth IS NOT NULL AND p.insol_earth > 0.0 THEN 'source_insolation'
                      WHEN p.semi_major_axis_au IS NOT NULL
                       AND p.semi_major_axis_au > 0.0
                       AND hs.luminosity_proxy_lsun IS NOT NULL
                      THEN 'stellar_class_luminosity_proxy'
                      WHEN p.semi_major_axis_au IS NULL OR p.semi_major_axis_au <= 0.0 THEN 'missing_orbit'
                      WHEN p.star_id IS NULL THEN 'missing_host'
                      ELSE 'missing_host_luminosity_proxy'
                    END AS evidence_basis
                  FROM planets p
                  LEFT JOIN host_star hs ON p.star_id = hs.star_id
                )
                SELECT
                  COUNT(*)::bigint AS total_planets,
                  SUM(CASE WHEN eq_temp_k IS NOT NULL THEN 1 ELSE 0 END)::bigint AS source_eq_temp_count,
                  SUM(CASE WHEN eq_temp_k IS NULL AND insol_earth IS NOT NULL AND insol_earth > 0.0 THEN 1 ELSE 0 END)::bigint AS source_insolation_only_count,
                  SUM(CASE WHEN evidence_basis = 'stellar_class_luminosity_proxy' THEN 1 ELSE 0 END)::bigint AS proxy_derivable_count,
                  SUM(CASE WHEN evidence_basis IN ('missing_orbit','missing_host','missing_host_luminosity_proxy') THEN 1 ELSE 0 END)::bigint AS missing_environment_count,
                  SUM(CASE WHEN evidence_basis = 'missing_orbit' THEN 1 ELSE 0 END)::bigint AS missing_orbit_count,
                  SUM(CASE WHEN evidence_basis = 'missing_host' THEN 1 ELSE 0 END)::bigint AS missing_host_count,
                  SUM(CASE WHEN evidence_basis = 'missing_host_luminosity_proxy' THEN 1 ELSE 0 END)::bigint AS missing_host_luminosity_proxy_count,
                  SUM(CASE WHEN candidate_insol_earth BETWEEN 0.35 AND 1.70 OR (candidate_insol_earth IS NULL AND candidate_eq_temp_k BETWEEN 180.0 AND 350.0) THEN 1 ELSE 0 END)::bigint AS broad_hz_environment_count,
                  SUM(CASE WHEN (candidate_insol_earth BETWEEN 0.35 AND 1.70 OR (candidate_insol_earth IS NULL AND candidate_eq_temp_k BETWEEN 180.0 AND 350.0))
                            AND planet_mass_earth BETWEEN 0.3 AND 8.0
                            AND COALESCE(eccentricity, 0.0) <= 0.35
                           THEN 1 ELSE 0 END)::bigint AS nice_planet_like_count
                FROM planet_env
                """
            ).fetchone(),
        )
        planet_environment_examples_rows = _timed(
            "planet_environment_gap_examples",
            lambda: con.execute(
                """
                WITH host_star AS (
                  SELECT
                    star_id,
                    spectral_class,
                    luminosity_class,
                    spectral_type_raw,
                    CASE
                      WHEN COALESCE(luminosity_class, '') IN ('', 'V')
                       AND NOT regexp_matches(LOWER(COALESCE(spectral_type_raw, '')), 'giant|supergiant|\\biii\\b|\\bii\\b|\\biv\\b')
                      THEN
                        CASE COALESCE(spectral_class, '')
                          WHEN 'O' THEN 30000.0
                          WHEN 'B' THEN 1000.0
                          WHEN 'A' THEN 20.0
                          WHEN 'F' THEN 4.0
                          WHEN 'G' THEN 1.0
                          WHEN 'K' THEN 0.4
                          WHEN 'M' THEN 0.04
                          ELSE NULL
                        END
                      ELSE NULL
                    END AS luminosity_proxy_lsun
                  FROM stars
                )
                SELECT
                  p.planet_id,
                  p.planet_name,
                  p.system_id,
                  p.stable_object_key,
                  p.source_catalog,
                  CASE
                    WHEN p.semi_major_axis_au IS NULL OR p.semi_major_axis_au <= 0.0 THEN 'missing_orbit'
                    WHEN p.star_id IS NULL THEN 'missing_host'
                    ELSE 'missing_host_luminosity_proxy'
                  END AS gap_reason,
                  hs.spectral_class,
                  hs.luminosity_class,
                  hs.spectral_type_raw
                FROM planets p
                LEFT JOIN host_star hs ON p.star_id = hs.star_id
                WHERE p.eq_temp_k IS NULL
                  AND (p.insol_earth IS NULL OR p.insol_earth <= 0.0)
                  AND NOT (
                    p.semi_major_axis_au IS NOT NULL
                    AND p.semi_major_axis_au > 0.0
                    AND hs.luminosity_proxy_lsun IS NOT NULL
                  )
                ORDER BY
                  CASE
                    WHEN p.semi_major_axis_au IS NULL OR p.semi_major_axis_au <= 0.0 THEN 0
                    WHEN p.star_id IS NULL THEN 1
                    ELSE 2
                  END,
                  p.source_catalog ASC,
                  p.planet_name ASC
                LIMIT 12
                """
            ).fetchall(),
        )

    arm_counts = {
        "component_entities": 0,
        "system_hierarchy_edges": 0,
        "orbit_edges": 0,
        "vsx_variability": 0,
        "variability_summary": 0,
        "ultracoolsheet_objects": 0,
    }
    arm_high_variability = 0
    if arm_db_path.exists():
        arm_con = None
        try:
            arm_con = duckdb.connect(str(arm_db_path), read_only=True)
            for table_name in arm_counts:
                try:
                    row = _timed(
                        f"arm_count_{table_name}",
                        lambda t=table_name: arm_con.execute(f"SELECT COUNT(*)::bigint FROM {t}").fetchone(),
                    )
                    arm_counts[table_name] = int((row or [0])[0] or 0)
                except Exception:
                    arm_counts[table_name] = 0
            try:
                row = _timed(
                    "arm_count_variability_summary_high_variability",
                    lambda: arm_con.execute(
                        "SELECT COUNT(*)::bigint FROM variability_summary WHERE any_high_variability"
                    ).fetchone(),
                )
                arm_high_variability = int((row or [0])[0] or 0)
            except Exception:
                arm_high_variability = 0
        except Exception:
            arm_counts = {
                "component_entities": 0,
                "system_hierarchy_edges": 0,
                "orbit_edges": 0,
                "vsx_variability": 0,
                "variability_summary": 0,
                "ultracoolsheet_objects": 0,
            }
            arm_high_variability = 0
        finally:
            if arm_con is not None:
                arm_con.close()

    source_breakdown = [
        {"source_catalog": row[0], "star_count": int(row[1])}
        for row in source_breakdown_rows
    ]
    spectral_total = sum(int(row[1]) for row in spectral_rows) or 0
    spectral_breakdown = [
        {
            "spectral_class": str(row[0]),
            "star_count": int(row[1]),
            "pct_of_stars": (float(row[1]) / float(spectral_total) * 100.0) if spectral_total else 0.0,
        }
        for row in spectral_rows
    ]

    star_mult_keys = [
        "total_stars",
        "nss_any",
        "wds_any",
        "msc_any",
        "sbx_any",
        "none",
        "nss_only",
        "wds_only",
        "msc_only",
        "sbx_only",
        "nss_wds",
        "nss_msc",
        "nss_sbx",
        "wds_msc",
        "wds_sbx",
        "msc_sbx",
        "nss_wds_msc",
        "nss_wds_msc_sbx",
    ]
    system_mult_keys = [
        "total_systems",
        "nss_any",
        "wds_any",
        "msc_any",
        "sbx_any",
        "none",
        "nss_only",
        "wds_only",
        "msc_only",
        "sbx_only",
        "nss_wds",
        "nss_msc",
        "nss_sbx",
        "wds_msc",
        "wds_sbx",
        "msc_sbx",
        "nss_wds_msc",
        "nss_wds_msc_sbx",
    ]

    star_mult_breakdown = {
        key: int(star_mult_row[idx] or 0) for idx, key in enumerate(star_mult_keys)
    }
    system_mult_breakdown = {
        key: int(system_mult_row[idx] or 0) for idx, key in enumerate(system_mult_keys)
    }
    exotic_counts = {
        "brown_dwarf_like_lty": int(exotic_row[0] or 0),
        "white_dwarf_like_d_prefix": int(exotic_row[1] or 0),
        "high_proper_motion_ge_1000_mas_yr": int(exotic_row[2] or 0),
    }
    exoplanet_counts = {
        "total_exoplanets": int(planet_row[0] or 0),
        "temperate_exoplanets": int(planet_row[1] or 0),
        "candidate_habitable_exoplanets": int(planet_row[2] or 0),
    }
    planet_environment_keys = [
        "total_planets",
        "source_eq_temp_count",
        "source_insolation_only_count",
        "proxy_derivable_count",
        "missing_environment_count",
        "missing_orbit_count",
        "missing_host_count",
        "missing_host_luminosity_proxy_count",
        "broad_hz_environment_count",
        "nice_planet_like_count",
    ]
    planet_environment_coverage = {
        key: int((planet_environment_row or [0] * len(planet_environment_keys))[idx] or 0)
        for idx, key in enumerate(planet_environment_keys)
    }
    total_environment = int(planet_environment_coverage.get("total_planets") or 0)
    planet_environment_coverage["source_or_derivable_count"] = (
        int(planet_environment_coverage.get("source_eq_temp_count") or 0)
        + int(planet_environment_coverage.get("source_insolation_only_count") or 0)
        + int(planet_environment_coverage.get("proxy_derivable_count") or 0)
    )
    planet_environment_coverage["source_or_derivable_pct"] = (
        float(planet_environment_coverage["source_or_derivable_count"]) / float(total_environment) * 100.0
        if total_environment
        else 0.0
    )
    planet_environment_coverage["missing_pct"] = (
        float(planet_environment_coverage.get("missing_environment_count") or 0) / float(total_environment) * 100.0
        if total_environment
        else 0.0
    )
    planet_environment_examples = [
        {
            "planet_id": int(row[0]) if row[0] is not None else None,
            "planet_name": row[1],
            "system_id": int(row[2]) if row[2] is not None else None,
            "stable_object_key": row[3],
            "source_catalog": row[4],
            "gap_reason": row[5],
            "spectral_class": row[6],
            "luminosity_class": row[7],
            "spectral_type_raw": row[8],
        }
        for row in planet_environment_examples_rows
    ]
    spectral_standard_counts = {
        "O": int(spectral_standard_row[0] or 0),
        "B": int(spectral_standard_row[1] or 0),
        "A": int(spectral_standard_row[2] or 0),
        "F": int(spectral_standard_row[3] or 0),
        "G": int(spectral_standard_row[4] or 0),
        "K": int(spectral_standard_row[5] or 0),
        "M": int(spectral_standard_row[6] or 0),
        "L": int(spectral_standard_row[7] or 0),
        "T": int(spectral_standard_row[8] or 0),
        "Y": int(spectral_standard_row[9] or 0),
        "D": int(spectral_standard_row[10] or 0),
        "unknown": int(spectral_standard_row[11] or 0),
    }
    compact_object_counts = {
        "white_dwarf": int(compact_row[0] or 0),
        "black_hole": int(compact_row[1] or 0),
        "neutron_star": int(compact_row[2] or 0),
        "pulsar": int(compact_row[3] or 0),
        "compact_total": int(compact_row[4] or 0),
    }

    basic_counts = {
        "systems": int((basic_counts_row or [0, 0, 0])[0] or 0),
        "stars": int((basic_counts_row or [0, 0, 0])[1] or 0),
        "planets": int((basic_counts_row or [0, 0, 0])[2] or 0),
    }
    qc_counts = qc_report.get("counts") if isinstance(qc_report.get("counts"), dict) else {}
    stars_count = int(qc_counts.get("stars") or basic_counts["stars"])
    systems_count = int(qc_counts.get("systems") or basic_counts["systems"])
    planets_count = int(qc_counts.get("planets") or basic_counts["planets"])
    multi_systems_count = int(system_grouping_report.get("multi_star_systems") or 0)
    single_systems_count = max(systems_count - multi_systems_count, 0)

    backbone_input_rows = int(gaia_backbone_report.get("raw_row_count") or 0)
    sliced_in_stars = int(gaia_backbone_report.get("stars_from_backbone_count") or stars_count)
    sliced_out_rows = int(
        gaia_backbone_report.get("rows_dropped_before_star_emit")
        or max(backbone_input_rows - sliced_in_stars, 0)
    )
    sliced_out_pct = (float(sliced_out_rows) / float(backbone_input_rows) * 100.0) if backbone_input_rows else 0.0
    slice_policy_counts = slice_policy_report.get("counts") or {}
    policy_input_stars = int(slice_policy_counts.get("input_star_rows") or 0)
    policy_retained_stars = int(slice_policy_counts.get("retained_star_rows") or 0)
    policy_sliced_out_stars = int(slice_policy_counts.get("sliced_out_star_rows") or 0)
    policy_sliced_out_stars_pct = float(slice_policy_counts.get("sliced_out_star_pct") or 0.0)

    proc_status = _read_proc_key_values(Path("/proc/self/status"))
    proc_io = _read_proc_key_values(Path("/proc/self/io"))
    meminfo = _read_proc_key_values(Path("/proc/meminfo"))
    disk_usage = shutil.disk_usage(state_dir)

    core_db_bytes = db_path.stat().st_size if db_path.exists() else 0
    disc_db_bytes = disc_db_path.stat().st_size if disc_db_path.exists() else 0
    arm_db_bytes = arm_db_path.stat().st_size if arm_db_path.exists() else 0
    admin_db_bytes = admin_db_path.stat().st_size if admin_db_path.exists() else 0

    payload = {
        "status": "ok",
        "build_id": build_id,
        "generated_at_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "paths": {
            "project_root": str(ROOT_DIR),
            "state_dir": str(state_dir),
            "build_dir": str(build_dir),
            "db_path": str(db_path),
            "disc_db_path": str(disc_db_path) if disc_db_path.exists() else None,
            "arm_db_path": str(arm_db_path) if arm_db_path.exists() else None,
            "admin_db_path": str(admin_db_path) if admin_db_path.exists() else None,
            "reports_dir": str(reports_dir),
        },
        "sizes_bytes": {
            "project_total": _path_size_bytes(ROOT_DIR),
            "state_total": _path_size_bytes(state_dir),
            "build_total": _path_size_bytes(build_dir),
            "raw_total": _path_size_bytes(raw_dir),
            "cooked_total": _path_size_bytes(cooked_dir),
            "reports_total": _path_size_bytes(state_dir / "reports"),
            "out_total": _path_size_bytes(out_dir),
            "served_total": _path_size_bytes(served_dir),
            "core_db": int(core_db_bytes),
            "disc_db": int(disc_db_bytes),
            "arm_db": int(arm_db_bytes),
            "admin_db": int(admin_db_bytes),
            "parquet_total": _path_size_bytes(build_dir / "parquet"),
        },
        "disk": {
            "total_bytes": int(disk_usage.total),
            "used_bytes": int(disk_usage.used),
            "free_bytes": int(disk_usage.free),
            "used_pct": (float(disk_usage.used) / float(disk_usage.total) * 100.0) if disk_usage.total else 0.0,
        },
        "host_runtime": {
            "cpu_count": os.cpu_count(),
            "loadavg_1m": os.getloadavg()[0] if hasattr(os, "getloadavg") else None,
            "loadavg_5m": os.getloadavg()[1] if hasattr(os, "getloadavg") else None,
            "loadavg_15m": os.getloadavg()[2] if hasattr(os, "getloadavg") else None,
            "mem_total_bytes": _proc_kib_value(meminfo.get("MemTotal")),
            "mem_available_bytes": _proc_kib_value(meminfo.get("MemAvailable")),
            "mem_free_bytes": _proc_kib_value(meminfo.get("MemFree")),
            "cached_bytes": _proc_kib_value(meminfo.get("Cached")),
            "buffers_bytes": _proc_kib_value(meminfo.get("Buffers")),
        },
        "api_process_runtime": {
            "pid": os.getpid(),
            "rss_bytes": _proc_kib_value(proc_status.get("VmRSS")),
            "peak_rss_bytes": _proc_kib_value(proc_status.get("VmHWM")),
            "vm_size_bytes": _proc_kib_value(proc_status.get("VmSize")),
            "threads": int(re.search(r"[0-9]+", proc_status.get("Threads", "0")).group(0)) if re.search(r"[0-9]+", proc_status.get("Threads", "0")) else 0,
            "io_read_bytes": int(re.search(r"[0-9]+", proc_io.get("read_bytes", "0")).group(0)) if re.search(r"[0-9]+", proc_io.get("read_bytes", "0")) else 0,
            "io_write_bytes": int(re.search(r"[0-9]+", proc_io.get("write_bytes", "0")).group(0)) if re.search(r"[0-9]+", proc_io.get("write_bytes", "0")) else 0,
        },
        "duckdb_runtime": {
            "database_name": db_size.get("database_name"),
            "database_size_bytes": _parse_human_bytes(db_size.get("database_size")),
            "wal_size_bytes": _parse_human_bytes(db_size.get("wal_size")),
            "memory_usage_bytes": _parse_human_bytes(db_size.get("memory_usage")),
            "memory_limit_bytes": _parse_human_bytes(db_size.get("memory_limit")),
            "block_size": int(db_size.get("block_size") or 0),
            "total_blocks": int(db_size.get("total_blocks") or 0),
            "used_blocks": int(db_size.get("used_blocks") or 0),
            "free_blocks": int(db_size.get("free_blocks") or 0),
        },
        "dataset_counts": {
            "rows_total": systems_count + stars_count + planets_count,
            "systems": systems_count,
            "stars": stars_count,
            "planets": planets_count,
            "arm_component_entities": int(arm_counts["component_entities"]),
            "arm_hierarchy_edges": int(arm_counts["system_hierarchy_edges"]),
            "arm_orbit_edges": int(arm_counts["orbit_edges"]),
            "arm_vsx_variability": int(arm_counts["vsx_variability"]),
            "arm_variability_summary": int(arm_counts["variability_summary"]),
            "arm_variability_high": int(arm_high_variability),
            "arm_ultracoolsheet_objects": int(arm_counts["ultracoolsheet_objects"]),
            "multi_star_systems": multi_systems_count,
            "single_star_systems": single_systems_count,
            "exoplanets_total": exoplanet_counts["total_exoplanets"],
            "exoplanets_temperate": exoplanet_counts["temperate_exoplanets"],
            "exoplanets_candidate_habitable": exoplanet_counts["candidate_habitable_exoplanets"],
        },
        "slice_metrics": {
            "input_backbone_rows": backbone_input_rows,
            "sliced_in_stars": sliced_in_stars,
            "sliced_out_rows": sliced_out_rows,
            "sliced_out_pct": sliced_out_pct,
            "policy_input_stars": policy_input_stars,
            "policy_retained_stars": policy_retained_stars,
            "policy_sliced_out_stars": policy_sliced_out_stars,
            "policy_sliced_out_stars_pct": policy_sliced_out_stars_pct,
        },
        "breakdowns": {
            "stars_by_source_catalog": source_breakdown,
            "stars_by_spectral_class": spectral_breakdown,
            "spectral_class_standard_counts": spectral_standard_counts,
            "compact_object_counts": compact_object_counts,
            "star_multiplicity_evidence": star_mult_breakdown,
            "system_multiplicity_evidence": system_mult_breakdown,
            "exotic_star_counts": exotic_counts,
            "exoplanet_counts": exoplanet_counts,
            "planet_environment_coverage": planet_environment_coverage,
            "planet_environment_gap_examples": planet_environment_examples,
            "qc_report": qc_report,
            "system_grouping_report": system_grouping_report,
            "gaia_backbone_report": gaia_backbone_report,
            "slice_policy_report": slice_policy_report,
            "match_report": match_report,
            "catalog_contribution_report": catalog_contribution_report,
            "catalog_pipeline_report": catalog_pipeline_report,
            "coolness_report": coolness_report,
        },
        "determinism": determinism_status,
        "bottleneck_hints": {
            "likely_memory_bound": (
                (_parse_human_bytes(db_size.get("memory_usage")) or 0) > (8 * 1024 * 1024 * 1024)
                or ((_proc_kib_value(proc_status.get("VmRSS")) or 0) > (8 * 1024 * 1024 * 1024))
            ),
            "likely_io_bound": ((_parse_human_bytes(db_size.get("database_size")) or 0) > (3 * 1024 * 1024 * 1024)),
            "notes": [
                "Rows and source/spectral breakdowns are measured from current served core.duckdb.",
                "Peak memory uses process high-water mark (VmHWM) for this API process lifetime.",
                "High load average with low process RSS growth often indicates CPU-bound query or scan work.",
            ],
        },
        "timings_ms": timings_ms,
    }
    _DATASET_STATUS_CACHE[build_id] = {"cached_at_ts": now, "payload": payload}
    payload["cache"] = {"hit": False, "ttl_s": DATASET_STATUS_CACHE_TTL_S, "age_s": 0.0}
    return payload


AGENCY_WORKFLOW_STAGES: List[Dict[str, Any]] = [
    {
        "key": "seeded",
        "title": "Seeded",
        "description": "A target object has been queued from coolness, adjudication candidates, stale sources, or operator request.",
        "predecessor": None,
        "successor": "gathering",
    },
    {
        "key": "gathering",
        "title": "Gathering",
        "description": "Allowlisted source material is being discovered, retrieved, archived, hashed, and attached as Source Files.",
        "predecessor": "seeded",
        "successor": "extracted",
    },
    {
        "key": "extracted",
        "title": "Extracted",
        "description": "Extraction Sets and narrow Findings exist, but review has not accepted them for use.",
        "predecessor": "gathering",
        "successor": "review_ready",
    },
    {
        "key": "review_ready",
        "title": "Review Ready",
        "description": "Findings and Proposals have enough evidence for deterministic checks, adversarial review, and human verdicts.",
        "predecessor": "extracted",
        "successor": "published",
    },
    {
        "key": "published",
        "title": "Published",
        "description": "Accepted overlays, factsheets, expositions, or citations are available from reviewed arm/disc surfaces.",
        "predecessor": "review_ready",
        "successor": "stale",
    },
    {
        "key": "stale",
        "title": "Stale",
        "description": "Source set, canonical object state, or model/prompt policy changed enough to require refresh.",
        "predecessor": "published",
        "successor": "gathering",
    },
    {
        "key": "blocked",
        "title": "Blocked",
        "description": "The portfolio needs missing source access, schema review, identity resolution, model routing, or human decision.",
        "predecessor": None,
        "successor": None,
    },
]


AGENCY_DISC_TABLES = [
    "object_dossiers",
    "source_documents",
    "claim_bundles",
    "extracted_claims",
    "source_evidence_links",
    "factsheets",
    "expositions",
]


AGENCY_ADMIN_TABLES = {
    "object_dossiers": "agent_object_dossiers",
    "source_documents": "agent_source_documents",
    "claim_bundles": "agent_claim_bundles",
    "extracted_claims": "agent_extracted_claims",
    "portfolio_journal_entries": "agent_portfolio_journal_entries",
}


AGENT_SOURCE_ALLOWLIST_DEFAULT_PATH = ROOT_DIR / "config" / "agent_source_allowlist.json"
AGENT_SOURCE_ALLOWLIST_RUNTIME_REL = Path("config") / "agent_source_allowlist.json"
AGENT_SOURCE_ALLOWLIST_TIERS = {
    0: "canonical",
    1: "scientific literature",
    2: "institutional / observatory",
    3: "curated aggregator",
    4: "context / narrative only",
}


AGENCY_ARM_SIGNALS = [
    "orbital_solutions",
    "component_entities",
    "system_hierarchy_edges",
    "orbit_edges",
    "stellar_parameters",
]


def _agent_source_allowlist_runtime_path() -> Path:
    return _state_dir().resolve() / AGENT_SOURCE_ALLOWLIST_RUNTIME_REL


def _agent_source_allowlist_history_dir() -> Path:
    return _agent_source_allowlist_runtime_path().parent / "agent_source_allowlist.history"


def _normalize_allowlist_domain(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    if "://" in value:
        match = re.match(r"^[a-z][a-z0-9+.-]*://([^/]+)", value)
        value = match.group(1) if match else value
    value = value.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
    value = value.rsplit("@", 1)[-1].split(":", 1)[0].strip().strip(".")
    if value.startswith("www."):
        value = value[4:]
    labels = value.split(".")
    if len(labels) < 2 or any(not label for label in labels):
        raise HTTPException(status_code=400, detail={"code": "bad_request", "message": "Domain must be a fully qualified host name."})
    label_pattern = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    if not all(label_pattern.match(label) for label in labels):
        raise HTTPException(status_code=400, detail={"code": "bad_request", "message": f"Invalid domain: {raw!r}"})
    return value


def _normalize_allowlist_uses(raw: Any) -> List[str]:
    items = raw if isinstance(raw, list) else []
    out: List[str] = []
    seen = set()
    for item in items:
        text = re.sub(r"\s+", " ", str(item or "").strip())
        if not text or len(text) > 120:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out[:20]


def _normalize_allowlist_source(raw: Dict[str, Any]) -> Dict[str, Any]:
    domain = _normalize_allowlist_domain(raw.get("domain"))
    tier = int(raw.get("tier", 2))
    if tier not in AGENT_SOURCE_ALLOWLIST_TIERS:
        raise HTTPException(status_code=400, detail={"code": "bad_request", "message": "tier must be between 0 and 4"})
    trust_score = float(raw.get("trust_score", 0.0))
    if trust_score < 0.0 or trust_score > 1.0:
        raise HTTPException(status_code=400, detail={"code": "bad_request", "message": "trust_score must be between 0 and 1"})
    return {
        "domain": domain,
        "tier": tier,
        "tier_label": AGENT_SOURCE_ALLOWLIST_TIERS[tier],
        "org": re.sub(r"\s+", " ", str(raw.get("org") or "").strip())[:240],
        "source_type": re.sub(r"\s+", " ", str(raw.get("source_type") or raw.get("type") or "").strip())[:120],
        "trust_score": round(trust_score, 3),
        "allowed_uses": _normalize_allowlist_uses(raw.get("allowed_uses")),
        "notes": str(raw.get("notes") or "").strip()[:1000],
        "enabled": bool(raw.get("enabled", True)),
    }


def _normalize_agent_source_allowlist_doc(raw: Dict[str, Any]) -> Dict[str, Any]:
    sources_raw = raw.get("sources") if isinstance(raw.get("sources"), list) else []
    sources_by_domain: Dict[str, Dict[str, Any]] = {}
    for item in sources_raw:
        if not isinstance(item, dict):
            continue
        source = _normalize_allowlist_source(item)
        sources_by_domain[source["domain"]] = source
    sources = sorted(sources_by_domain.values(), key=lambda item: (int(item["tier"]), str(item["domain"])))
    policy = raw.get("policy") if isinstance(raw.get("policy"), dict) else {}
    return {
        "schema_version": int(raw.get("schema_version") or 1),
        "updated_at_utc": raw.get("updated_at_utc"),
        "updated_by": str(raw.get("updated_by") or "unknown"),
        "policy": policy,
        "sources": sources,
    }


def _read_agent_source_allowlist_json_file(path: Path) -> Dict[str, Any]:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"code": "allowlist_read_failed", "message": str(exc)})
    if not isinstance(loaded, dict):
        raise HTTPException(status_code=500, detail={"code": "allowlist_read_failed", "message": "Allowlist JSON root must be an object."})
    return loaded


def _allowlist_doc_hash(doc: Dict[str, Any]) -> str:
    raw = json.dumps(doc, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _backup_agent_source_allowlist(reason: str) -> Dict[str, Any] | None:
    runtime_path = _agent_source_allowlist_runtime_path()
    if not runtime_path.exists():
        return None
    raw = _read_agent_source_allowlist_json_file(runtime_path)
    normalized = _normalize_agent_source_allowlist_doc(raw)
    normalized["backup_reason"] = re.sub(r"\s+", "_", str(reason or "change").strip().lower())[:80] or "change"
    normalized["backed_up_at_utc"] = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    digest = _allowlist_doc_hash(normalized)[:12]
    backup_id = f"{datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{digest}_{uuid.uuid4().hex[:8]}.json"
    history_dir = _agent_source_allowlist_history_dir()
    history_dir.mkdir(parents=True, exist_ok=True)
    path = history_dir / backup_id
    path.write_text(json.dumps(normalized, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"version_id": backup_id, "path": str(path), "reason": normalized["backup_reason"]}


def _safe_allowlist_version_id(raw: str) -> str:
    value = str(raw or "").strip()
    if not re.match(r"^[0-9]{8}T[0-9]{6}Z_[0-9a-f]{12}_[0-9a-f]{8}\.json$", value):
        raise HTTPException(status_code=400, detail={"code": "bad_request", "message": "Invalid allowlist version id."})
    return value


def _list_agent_source_allowlist_versions(limit: int = 30) -> List[Dict[str, Any]]:
    history_dir = _agent_source_allowlist_history_dir()
    versions: List[Dict[str, Any]] = [
        {
            "version_id": "spacegate_default",
            "kind": "default",
            "label": "Spacegate shipped default",
            "path": str(AGENT_SOURCE_ALLOWLIST_DEFAULT_PATH),
            "mtime_utc": _utc_from_timestamp(AGENT_SOURCE_ALLOWLIST_DEFAULT_PATH.stat().st_mtime) if AGENT_SOURCE_ALLOWLIST_DEFAULT_PATH.exists() else None,
            "source_count": None,
            "enabled_count": None,
            "backup_reason": "repo_default",
        }
    ]
    if history_dir.exists() and history_dir.is_dir():
        for path in sorted(history_dir.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True)[: max(1, min(limit, 200))]:
            source_count = None
            enabled_count = None
            backup_reason = ""
            updated_at = None
            try:
                raw = _read_agent_source_allowlist_json_file(path)
                doc = _normalize_agent_source_allowlist_doc(raw)
                source_count = len(doc.get("sources", []))
                enabled_count = len([item for item in doc.get("sources", []) if item.get("enabled")])
                backup_reason = str(raw.get("backup_reason") or "")
                updated_at = doc.get("updated_at_utc")
            except Exception:
                pass
            versions.append(
                {
                    "version_id": path.name,
                    "kind": "history",
                    "label": path.name,
                    "path": str(path),
                    "mtime_utc": _utc_from_timestamp(path.stat().st_mtime),
                    "updated_at_utc": updated_at,
                    "source_count": source_count,
                    "enabled_count": enabled_count,
                    "backup_reason": backup_reason,
                }
            )
    return versions


def _load_agent_source_allowlist() -> Dict[str, Any]:
    runtime_path = _agent_source_allowlist_runtime_path()
    default_path = AGENT_SOURCE_ALLOWLIST_DEFAULT_PATH
    selected = runtime_path if runtime_path.exists() else default_path
    try:
        raw = _read_agent_source_allowlist_json_file(selected)
    except FileNotFoundError:
        raw = {"schema_version": 1, "policy": {}, "sources": []}
    doc = _normalize_agent_source_allowlist_doc(raw)
    doc["source_path"] = str(selected)
    doc["default_path"] = str(default_path)
    doc["runtime_path"] = str(runtime_path)
    doc["runtime_override_exists"] = runtime_path.exists()
    doc["history_dir"] = str(_agent_source_allowlist_history_dir())
    doc["versions"] = _list_agent_source_allowlist_versions()
    doc["generated_at_utc"] = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    doc["summary"] = {
        "total_sources": len(doc["sources"]),
        "enabled_sources": len([item for item in doc["sources"] if item.get("enabled")]),
        "tiers": [
            {
                "tier": tier,
                "label": label,
                "count": len([item for item in doc["sources"] if int(item.get("tier", -1)) == tier]),
                "enabled_count": len([item for item in doc["sources"] if int(item.get("tier", -1)) == tier and item.get("enabled")]),
            }
            for tier, label in AGENT_SOURCE_ALLOWLIST_TIERS.items()
        ],
    }
    return doc


def _write_agent_source_allowlist(doc: Dict[str, Any], *, backup_reason: str = "change") -> Dict[str, Any]:
    runtime_path = _agent_source_allowlist_runtime_path()
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    _backup_agent_source_allowlist(backup_reason)
    normalized = _normalize_agent_source_allowlist_doc(doc)
    tmp_path = runtime_path.with_name(f".{runtime_path.name}.{uuid.uuid4().hex}.tmp")
    tmp_path.write_text(json.dumps(normalized, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp_path, runtime_path)
    return _load_agent_source_allowlist()


def _upsert_agent_source_allowlist_entry(payload: AgencySourceAllowlistEntryRequest, user: Dict[str, Any]) -> Dict[str, Any]:
    doc = _load_agent_source_allowlist()
    entry = _normalize_allowlist_source(payload.dict())
    sources = [item for item in doc.get("sources", []) if item.get("domain") != entry["domain"]]
    sources.append(entry)
    doc["sources"] = sources
    doc["updated_at_utc"] = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    doc["updated_by"] = str(user.get("email_norm") or user.get("email") or user.get("user_id") or "admin")
    return _write_agent_source_allowlist(doc, backup_reason=f"upsert_{entry['domain']}")


def _delete_agent_source_allowlist_entry(domain: str, user: Dict[str, Any]) -> Dict[str, Any]:
    clean_domain = _normalize_allowlist_domain(domain)
    doc = _load_agent_source_allowlist()
    sources = [item for item in doc.get("sources", []) if item.get("domain") != clean_domain]
    if len(sources) == len(doc.get("sources", [])):
        raise HTTPException(status_code=404, detail={"code": "not_found", "message": f"Allowlist source not found: {clean_domain}"})
    doc["sources"] = sources
    doc["updated_at_utc"] = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    doc["updated_by"] = str(user.get("email_norm") or user.get("email") or user.get("user_id") or "admin")
    return _write_agent_source_allowlist(doc, backup_reason=f"delete_{clean_domain}")


def _restore_agent_source_allowlist_default(user: Dict[str, Any]) -> Dict[str, Any]:
    _backup_agent_source_allowlist("restore_default")
    runtime_path = _agent_source_allowlist_runtime_path()
    try:
        runtime_path.unlink()
    except FileNotFoundError:
        pass
    return _load_agent_source_allowlist()


def _restore_agent_source_allowlist_version(version_id: str, user: Dict[str, Any]) -> Dict[str, Any]:
    clean_version_id = _safe_allowlist_version_id(version_id)
    source_path = _agent_source_allowlist_history_dir() / clean_version_id
    if not source_path.exists() or not source_path.is_file():
        raise HTTPException(status_code=404, detail={"code": "not_found", "message": f"Allowlist version not found: {clean_version_id}"})
    raw = _read_agent_source_allowlist_json_file(source_path)
    doc = _normalize_agent_source_allowlist_doc(raw)
    doc["updated_at_utc"] = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    doc["updated_by"] = str(user.get("email_norm") or user.get("email") or user.get("user_id") or "admin")
    return _write_agent_source_allowlist(doc, backup_reason=f"restore_{clean_version_id}")


def _utc_from_timestamp(ts: float) -> str:
    return datetime.datetime.utcfromtimestamp(ts).replace(microsecond=0).isoformat() + "Z"


def _duckdb_table_counts(db_path: Path, expected_tables: List[str]) -> Dict[str, Any]:
    output: Dict[str, Any] = {
        "path": str(db_path),
        "exists": db_path.exists(),
        "tables": [],
        "expected": {},
    }
    if not db_path.exists():
        output["expected"] = {name: {"exists": False, "count": None} for name in expected_tables}
        return output
    con = None
    try:
        con = duckdb.connect(str(db_path), read_only=True)
        tables = [
            str(row[0])
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name"
            ).fetchall()
        ]
        output["tables"] = tables
        table_set = set(tables)
        expected: Dict[str, Any] = {}
        for table_name in expected_tables:
            if table_name not in table_set:
                expected[table_name] = {"exists": False, "count": None}
                continue
            try:
                row = con.execute(f"SELECT COUNT(*)::bigint FROM {table_name}").fetchone()
                expected[table_name] = {"exists": True, "count": int((row or [0])[0] or 0)}
            except Exception as exc:
                expected[table_name] = {"exists": True, "count": None, "error": str(exc)}
        output["expected"] = expected
        return output
    except Exception as exc:
        output["error"] = str(exc)
        output["expected"] = {name: {"exists": False, "count": None} for name in expected_tables}
        return output
    finally:
        if con is not None:
            con.close()


def _sqlite_table_counts(table_map: Dict[str, str]) -> Dict[str, Any]:
    admin_db.initialize()
    with admin_db.connection_scope() as con:
        table_rows = con.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
        table_names = {str(row["name"]) for row in table_rows}
        expected: Dict[str, Any] = {}
        for public_name, table_name in table_map.items():
            if table_name not in table_names:
                expected[public_name] = {
                    "storage_table": table_name,
                    "exists": False,
                    "count": None,
                }
                continue
            try:
                row = con.execute(f"SELECT COUNT(*) AS row_count FROM {table_name}").fetchone()
                expected[public_name] = {
                    "storage_table": table_name,
                    "exists": True,
                    "count": int((row or {"row_count": 0})["row_count"] or 0),
                }
            except Exception as exc:
                expected[public_name] = {
                    "storage_table": table_name,
                    "exists": True,
                    "count": None,
                    "error": str(exc),
                }
        return {
            "path": str(admin_db.get_admin_db_path()),
            "exists": admin_db.get_admin_db_path().exists(),
            "tables": sorted(table_names),
            "expected": expected,
        }


def _json_value(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return fallback


def _sqlite_row_dict(row: Any) -> Dict[str, Any]:
    if row is None:
        return {}
    return {key: row[key] for key in row.keys()}


def _portfolio_row_payload(row: Any) -> Dict[str, Any]:
    payload = _sqlite_row_dict(row)
    if not payload:
        return payload
    payload["metadata"] = _json_value(payload.pop("metadata_json", "{}"), {})
    return payload


def _source_document_payload(row: Any) -> Dict[str, Any]:
    payload = _sqlite_row_dict(row)
    if not payload:
        return payload
    payload["metadata"] = _json_value(payload.pop("metadata_json", "{}"), {})
    return payload


def _claim_bundle_payload(row: Any) -> Dict[str, Any]:
    payload = _sqlite_row_dict(row)
    if not payload:
        return payload
    payload["metadata"] = _json_value(payload.pop("metadata_json", "{}"), {})
    return payload


def _extracted_claim_payload(row: Any) -> Dict[str, Any]:
    payload = _sqlite_row_dict(row)
    if not payload:
        return payload
    payload["value"] = _json_value(payload.pop("value_json", "{}"), {})
    payload["citation_ids"] = _json_value(payload.pop("citation_ids_json", "[]"), [])
    payload["metadata"] = _json_value(payload.pop("metadata_json", "{}"), {})
    return payload


def _journal_entry_payload(row: Any) -> Dict[str, Any]:
    payload = _sqlite_row_dict(row)
    if not payload:
        return payload
    payload["linked"] = _json_value(payload.pop("linked_json", "{}"), {})
    payload["machine_payload"] = _json_value(payload.pop("machine_payload_json", "{}"), {})
    payload["token_usage"] = _json_value(payload.pop("token_usage_json", "{}"), {})
    return payload


def _list_agency_portfolios(
    limit: int,
    status: str | None = None,
    stable_object_key: str | None = None,
    object_type: str | None = None,
) -> Dict[str, Any]:
    admin_db.initialize()
    safe_limit = max(1, min(int(limit or 50), 200))
    params: List[Any] = []
    where_terms: List[str] = []
    if status:
        where_terms.append("d.dossier_status = ?")
        params.append(status)
    clean_stable_key = str(stable_object_key or "").strip()
    if clean_stable_key:
        where_terms.append("d.stable_object_key = ?")
        params.append(clean_stable_key)
    clean_object_type = str(object_type or "").strip().lower()
    if clean_object_type:
        where_terms.append("d.object_type = ?")
        params.append(clean_object_type)
    where = f"WHERE {' AND '.join(where_terms)}" if where_terms else ""
    params.append(safe_limit)
    with admin_db.connection_scope() as con:
        rows = con.execute(
            f"""
SELECT
  d.*,
  (SELECT COUNT(*) FROM agent_source_documents s WHERE s.dossier_id = d.dossier_id) AS source_count,
  (SELECT COUNT(*) FROM agent_claim_bundles b WHERE b.dossier_id = d.dossier_id) AS bundle_count,
  (SELECT COUNT(*) FROM agent_extracted_claims c WHERE c.dossier_id = d.dossier_id) AS claim_count,
  (SELECT COUNT(*) FROM agent_portfolio_journal_entries j WHERE j.dossier_id = d.dossier_id) AS journal_count
FROM agent_object_dossiers d
{where}
ORDER BY d.updated_at DESC, d.created_at DESC
LIMIT ?
            """,
            params,
        ).fetchall()
        status_rows = con.execute(
            """
SELECT dossier_status, COUNT(*) AS row_count
FROM agent_object_dossiers
GROUP BY dossier_status
ORDER BY row_count DESC, dossier_status ASC
            """
        ).fetchall()
    return {
        "status": "ok",
        "items": [_portfolio_row_payload(row) for row in rows],
        "counts_by_status": {str(row["dossier_status"]): int(row["row_count"] or 0) for row in status_rows},
        "limit": safe_limit,
        "filters": {
            "status": status,
            "stable_object_key": clean_stable_key or None,
            "object_type": clean_object_type or None,
        },
    }


def _current_build_id_or_none() -> Optional[str]:
    try:
        with db.connection_scope() as con:
            return fetch_build_id(con)
    except Exception:
        return None


def _agency_existing_dossiers_by_key(stable_keys: List[str], object_type: str = "system") -> Dict[str, Dict[str, Any]]:
    if not stable_keys:
        return {}
    admin_db.initialize()
    placeholders = ",".join(["?"] * len(stable_keys))
    params: List[Any] = [object_type, *stable_keys]
    with admin_db.connection_scope() as con:
        rows = con.execute(
            f"""
SELECT dossier_id, stable_object_key, dossier_status, display_name, updated_at
FROM agent_object_dossiers
WHERE object_type = ?
  AND archived_at IS NULL
  AND stable_object_key IN ({placeholders})
ORDER BY updated_at DESC
            """,
            params,
        ).fetchall()
    output: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = str(row["stable_object_key"])
        output.setdefault(
            key,
            {
                "dossier_id": row["dossier_id"],
                "dossier_status": row["dossier_status"],
                "display_name": row["display_name"],
                "updated_at": row["updated_at"],
            },
        )
    return output


def _agency_seed_candidates(limit: int) -> Dict[str, Any]:
    safe_limit = max(1, min(int(limit or 50), 200))
    core_db_path = Path(db.get_db_path())
    disc_db_path = core_db_path.with_name("disc.duckdb")
    build_id = _current_build_id_or_none()
    if not disc_db_path.exists():
        return {
            "status": "ok",
            "items": [],
            "limit": safe_limit,
            "source": "disc.coolness_scores",
            "source_build_id": build_id,
            "message": "disc.duckdb is not available for the current build; run the coolness scoring workflow before seeding ranked portfolios.",
        }

    con = None
    try:
        con = duckdb.connect(str(disc_db_path), read_only=True)
        table_exists = bool(
            con.execute(
                """
SELECT COUNT(*) > 0
FROM information_schema.tables
WHERE table_schema = 'main' AND table_name = 'coolness_scores'
                """
            ).fetchone()[0]
        )
        if not table_exists:
            return {
                "status": "ok",
                "items": [],
                "limit": safe_limit,
                "source": "disc.coolness_scores",
                "source_build_id": build_id,
                "message": "disc.coolness_scores is not present; score coolness before using ranked portfolio seeds.",
            }
        columns = {
            str(row[0])
            for row in con.execute(
                """
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'main' AND table_name = 'coolness_scores'
                """
            ).fetchall()
        }
        desired = [
            "rank",
            "system_id",
            "stable_object_key",
            "system_name",
            "score_total",
            "profile_id",
            "profile_version",
            "dist_ly",
            "dominant_spectral_class",
            "star_count",
            "planet_count",
            "nice_planet_count",
            "weird_planet_count",
        ]
        select_terms: List[str] = []
        for column in desired:
            if column in columns:
                select_terms.append(column)
            else:
                select_terms.append(f"NULL AS {column}")
        if "rank" not in columns:
            order_expr = []
            if "score_total" in columns:
                order_expr.append("score_total DESC")
            if "system_id" in columns:
                order_expr.append("system_id ASC")
            select_terms[0] = f"ROW_NUMBER() OVER (ORDER BY {', '.join(order_expr) or '1'}) AS rank"
        order_terms = []
        if "rank" in columns:
            order_terms.append("rank ASC")
        elif "score_total" in columns:
            order_terms.append("score_total DESC")
        if "system_id" in columns:
            order_terms.append("system_id ASC")
        elif "stable_object_key" in columns:
            order_terms.append("stable_object_key ASC")
        sql = f"""
SELECT {", ".join(select_terms)}
FROM coolness_scores
ORDER BY {", ".join(order_terms) or "rank ASC"}
LIMIT ?
        """
        cur = con.execute(sql, [safe_limit])
        result_columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(result_columns, row)) for row in cur.fetchall()]
    except Exception as exc:
        return {
            "status": "error",
            "items": [],
            "limit": safe_limit,
            "source": "disc.coolness_scores",
            "source_build_id": build_id,
            "message": str(exc),
        }
    finally:
        if con is not None:
            con.close()

    keys = [str(row.get("stable_object_key") or "").strip() for row in rows if str(row.get("stable_object_key") or "").strip()]
    existing = _agency_existing_dossiers_by_key(keys, object_type="system")
    items: List[Dict[str, Any]] = []
    for row in rows:
        stable_key = str(row.get("stable_object_key") or "").strip()
        if not stable_key:
            continue
        rank_value = row.get("rank")
        try:
            rank_int = int(rank_value) if rank_value is not None else None
        except (TypeError, ValueError):
            rank_int = None
        score_value = row.get("score_total")
        try:
            score_float = float(score_value) if score_value is not None else None
        except (TypeError, ValueError):
            score_float = None
        metadata = {
            key: row.get(key)
            for key in [
                "rank",
                "system_id",
                "score_total",
                "profile_id",
                "profile_version",
                "dist_ly",
                "dominant_spectral_class",
                "star_count",
                "planet_count",
                "nice_planet_count",
                "weird_planet_count",
            ]
            if row.get(key) is not None
        }
        existing_dossier = existing.get(stable_key)
        items.append(
            {
                "id": stable_key,
                "stable_object_key": stable_key,
                "object_type": "system",
                "display_name": row.get("system_name") or stable_key,
                "rank": rank_int,
                "score_total": score_float,
                "queue_reason": "coolness_rank",
                "queue_priority": "high" if rank_int is not None and rank_int <= 100 else "normal",
                "source": "coolness_scores",
                "source_build_id": build_id,
                "metadata": metadata,
                "existing_dossier_id": existing_dossier["dossier_id"] if existing_dossier else None,
                "existing_dossier_status": existing_dossier["dossier_status"] if existing_dossier else None,
            }
        )
    return {
        "status": "ok",
        "items": items,
        "limit": safe_limit,
        "source": "disc.coolness_scores",
        "source_build_id": build_id,
        "message": "Ranked candidates from the current disc coolness scores. Seeding creates only admin workflow rows and a journal entry.",
    }


def _seed_agency_portfolio(payload: AgencyPortfolioSeedRequest, user: Dict[str, Any]) -> Dict[str, Any]:
    stable_key = str(payload.stable_object_key or "").strip()
    object_type = str(payload.object_type or "").strip().lower()
    display_name = str(payload.display_name or "").strip() or None
    queue_reason = str(payload.queue_reason or "").strip() or "operator_seed"
    queue_priority = str(payload.queue_priority or "").strip().lower() or "normal"
    source = str(payload.source or "").strip() or "manual"
    if not stable_key:
        raise HTTPException(status_code=400, detail={"code": "bad_request", "message": "stable_object_key is required", "details": {}})
    if object_type not in {"system", "star", "planet"}:
        raise HTTPException(status_code=400, detail={"code": "bad_request", "message": "Unsupported object_type", "details": {"object_type": object_type}})
    if queue_priority not in {"low", "normal", "high", "urgent"}:
        raise HTTPException(status_code=400, detail={"code": "bad_request", "message": "Unsupported queue_priority", "details": {"queue_priority": queue_priority}})

    admin_db.initialize()
    now = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    dossier_id = f"dossier_{uuid.uuid4().hex}"
    journal_entry_id = f"journal_{uuid.uuid4().hex}"
    user_id = int(user["user_id"])
    actor_id = str(user.get("email") or user.get("user_id") or "admin")
    metadata = {
        "seed_source": source,
        "seeded_from_admin_v2": True,
        "seed_metadata": payload.metadata or {},
    }
    linked = {
        "stable_object_key": stable_key,
        "object_type": object_type,
        "source": source,
        "source_build_id": payload.source_build_id,
    }
    machine_payload = {
        "stable_object_key": stable_key,
        "object_type": object_type,
        "display_name": display_name,
        "queue_reason": queue_reason,
        "queue_priority": queue_priority,
        "source_build_id": payload.source_build_id,
        "source": source,
        "metadata": payload.metadata or {},
    }
    narrative_name = display_name or stable_key
    narrative = (
        f"Seeded an Evidence Portfolio for {narrative_name} from {source}. "
        "No source retrieval, extraction, model generation, claims, proposals, or publication steps were run."
    )
    with admin_db.connection_scope() as con:
        existing = con.execute(
            """
SELECT dossier_id, dossier_status, display_name
FROM agent_object_dossiers
WHERE stable_object_key = ?
  AND object_type = ?
  AND archived_at IS NULL
LIMIT 1
            """,
            (stable_key, object_type),
        ).fetchone()
        if existing is not None:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "conflict",
                    "message": "An active Evidence Portfolio already exists for this object",
                    "details": {
                        "dossier_id": existing["dossier_id"],
                        "dossier_status": existing["dossier_status"],
                        "display_name": existing["display_name"],
                    },
                },
            )
        con.execute(
            """
INSERT INTO agent_object_dossiers(
  dossier_id, stable_object_key, object_type, display_name, dossier_status,
  queue_reason, queue_priority, source_build_id, freshness_state, review_state,
  publication_state, metadata_json, created_by_user_id, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dossier_id,
                stable_key,
                object_type,
                display_name,
                "seeded",
                queue_reason,
                queue_priority,
                payload.source_build_id,
                "current",
                "unreviewed",
                "not_published",
                json.dumps(metadata, separators=(",", ":"), sort_keys=True),
                user_id,
                now,
                now,
            ),
        )
        con.execute(
            """
INSERT INTO agent_portfolio_journal_entries(
  journal_entry_id, dossier_id, actor_type, actor_id, stage, title, narrative,
  outcome, linked_json, machine_payload_json, token_usage_json, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                journal_entry_id,
                dossier_id,
                "operator",
                actor_id,
                "seeded",
                "Evidence Portfolio seeded",
                narrative,
                "created",
                json.dumps(linked, separators=(",", ":"), sort_keys=True),
                json.dumps(machine_payload, separators=(",", ":"), sort_keys=True),
                "{}",
                now,
            ),
        )
        con.commit()
    return _agency_portfolio_detail(dossier_id)


def _agency_portfolio_detail(dossier_id: str) -> Dict[str, Any]:
    admin_db.initialize()
    with admin_db.connection_scope() as con:
        dossier = con.execute(
            "SELECT * FROM agent_object_dossiers WHERE dossier_id = ? LIMIT 1",
            (dossier_id,),
        ).fetchone()
        if dossier is None:
            raise HTTPException(
                status_code=404,
                detail={"error": {"code": "not_found", "message": "Evidence Portfolio not found", "details": {"dossier_id": dossier_id}}},
            )
        sources = con.execute(
            """
SELECT * FROM agent_source_documents
WHERE dossier_id = ?
ORDER BY accessed_at DESC, created_at DESC
            """,
            (dossier_id,),
        ).fetchall()
        bundles = con.execute(
            """
SELECT * FROM agent_claim_bundles
WHERE dossier_id = ?
ORDER BY created_at DESC
            """,
            (dossier_id,),
        ).fetchall()
        claims = con.execute(
            """
SELECT * FROM agent_extracted_claims
WHERE dossier_id = ?
ORDER BY updated_at DESC, created_at DESC
            """,
            (dossier_id,),
        ).fetchall()
        journal = con.execute(
            """
SELECT * FROM agent_portfolio_journal_entries
WHERE dossier_id = ?
ORDER BY created_at ASC
            """,
            (dossier_id,),
        ).fetchall()
    return {
        "status": "ok",
        "dossier": _portfolio_row_payload(dossier),
        "source_documents": [_source_document_payload(row) for row in sources],
        "claim_bundles": [_claim_bundle_payload(row) for row in bundles],
        "extracted_claims": [_extracted_claim_payload(row) for row in claims],
        "journal_entries": [_journal_entry_payload(row) for row in journal],
    }


def _agent_eval_report_dirs(state_dir: Path) -> List[Path]:
    candidates = [
        state_dir / "reports" / "agent_eval",
        ROOT_DIR / "reports" / "agent_eval",
    ]
    seen: set[str] = set()
    output: List[Path] = []
    for path in candidates:
        key = str(path.resolve()) if path.exists() else str(path)
        if key in seen:
            continue
        seen.add(key)
        output.append(path)
    return output


def _agent_eval_role_summary(results: List[Any]) -> List[Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = {}
    for result in results:
        if not isinstance(result, dict):
            continue
        role_tokens: List[str] = []
        role_under_test = str(result.get("role_under_test") or "").strip()
        if role_under_test:
            role_tokens.append(role_under_test)
        else:
            for role in result.get("roles") or []:
                token = str(role or "").strip()
                if token:
                    role_tokens.append(token)
        score_payload = result.get("score") if isinstance(result.get("score"), dict) else {}
        score_value = score_payload.get("score")
        try:
            score_float = float(score_value) if score_value is not None else None
        except (TypeError, ValueError):
            score_float = None
        schema_valid = score_payload.get("schema_valid")
        for role in sorted(set(role_tokens)):
            bucket = buckets.setdefault(
                role,
                {
                    "role": role,
                    "case_count": 0,
                    "score_sum": 0.0,
                    "score_count": 0,
                    "schema_valid_count": 0,
                    "schema_valid_observed": 0,
                },
            )
            bucket["case_count"] += 1
            if score_float is not None:
                bucket["score_sum"] += score_float
                bucket["score_count"] += 1
            if schema_valid is not None:
                bucket["schema_valid_observed"] += 1
                if bool(schema_valid):
                    bucket["schema_valid_count"] += 1
    output: List[Dict[str, Any]] = []
    for role, bucket in buckets.items():
        score_count = int(bucket["score_count"] or 0)
        schema_count = int(bucket["schema_valid_observed"] or 0)
        output.append(
            {
                "role": role,
                "case_count": int(bucket["case_count"] or 0),
                "mean_score": round(float(bucket["score_sum"]) / score_count, 4) if score_count else None,
                "schema_valid_rate": round(float(bucket["schema_valid_count"]) / schema_count, 4) if schema_count else None,
            }
        )
    output.sort(key=lambda item: item["role"])
    return output


def _agent_eval_report_summary(path: Path) -> Dict[str, Any]:
    payload = _read_json_file(path)
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    results = payload.get("results") if isinstance(payload.get("results"), list) else []
    roles: set[str] = set()
    for result in results:
        if not isinstance(result, dict):
            continue
        for role in result.get("roles") or []:
            if role:
                roles.add(str(role))
        role_under_test = result.get("role_under_test")
        if role_under_test:
            roles.add(str(role_under_test))
    return {
        "report_id": path.stem,
        "path": str(path),
        "mtime_utc": _utc_from_timestamp(path.stat().st_mtime),
        "created_at": payload.get("created_at"),
        "provider": payload.get("provider"),
        "model_id": payload.get("model_id"),
        "prompt_version": payload.get("prompt_version"),
        "harness_version": payload.get("harness_version"),
        "aborted_reason": payload.get("aborted_reason"),
        "roles": sorted(roles),
        "case_count": int(summary.get("case_count") or len(results) or 0),
        "mean_score": summary.get("mean_score"),
        "schema_valid_rate": summary.get("schema_valid_rate"),
        "anomaly_count": int(summary.get("anomaly_count") or 0),
        "role_summary": _agent_eval_role_summary(results),
        "summary": summary,
    }


def _list_agent_eval_reports(state_dir: Path, limit: int = 12) -> Dict[str, Any]:
    dirs = _agent_eval_report_dirs(state_dir)
    reports: List[Dict[str, Any]] = []
    anomalies: List[Dict[str, Any]] = []
    role_candidates: Dict[str, List[Dict[str, Any]]] = {}
    for directory in dirs:
        if not directory.exists() or not directory.is_dir():
            continue
        for path in directory.glob("agent_eval_*.json"):
            try:
                report = _agent_eval_report_summary(path)
            except Exception:
                continue
            reports.append(report)
            for role_summary in report.get("role_summary") or []:
                if not isinstance(role_summary, dict):
                    continue
                role = str(role_summary.get("role") or "").strip()
                if not role:
                    continue
                role_candidates.setdefault(role, []).append(
                    {
                        "role": role,
                        "report_id": report["report_id"],
                        "created_at": report.get("created_at") or report.get("mtime_utc"),
                        "provider": report.get("provider"),
                        "model_id": report.get("model_id"),
                        "prompt_version": report.get("prompt_version"),
                        "harness_version": report.get("harness_version"),
                        "aborted_reason": report.get("aborted_reason"),
                        "case_count": int(role_summary.get("case_count") or 0),
                        "mean_score": role_summary.get("mean_score"),
                        "schema_valid_rate": role_summary.get("schema_valid_rate"),
                        "report_anomaly_count": int(report.get("anomaly_count") or 0),
                    }
                )
            inbox = ((report.get("summary") or {}).get("anomaly_inbox") or [])
            if isinstance(inbox, list):
                for item in inbox:
                    if not isinstance(item, dict):
                        continue
                    anomalies.append(
                        {
                            "report_id": report["report_id"],
                            "created_at": report.get("created_at"),
                            "provider": report.get("provider"),
                            "model_id": report.get("model_id"),
                            "case_id": item.get("case_id"),
                            "anomaly_type": item.get("anomaly_type"),
                            "severity": item.get("severity"),
                            "subject": item.get("subject"),
                            "summary": item.get("summary"),
                            "recommended_next_action": item.get("recommended_next_action"),
                            "status": "quarantined",
                        }
                    )
    reports.sort(key=lambda row: str(row.get("created_at") or row.get("mtime_utc") or ""), reverse=True)
    anomalies.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
    role_summary: List[Dict[str, Any]] = []
    for role, candidates in role_candidates.items():
        candidates.sort(
            key=lambda item: (
                float(item["mean_score"]) if item.get("mean_score") is not None else -1.0,
                float(item["schema_valid_rate"]) if item.get("schema_valid_rate") is not None else -1.0,
                int(item.get("case_count") or 0),
                str(item.get("created_at") or ""),
            ),
            reverse=True,
        )
        latest = sorted(candidates, key=lambda item: str(item.get("created_at") or ""), reverse=True)
        role_summary.append(
            {
                "role": role,
                "best_candidate": candidates[0] if candidates else None,
                "latest_candidate": latest[0] if latest else None,
                "candidates": candidates[:5],
                "candidate_count": len(candidates),
            }
        )
    role_summary.sort(key=lambda item: item["role"])
    return {
        "searched_dirs": [{"path": str(path), "exists": path.exists()} for path in dirs],
        "reports": reports[:limit],
        "report_count": len(reports),
        "role_summary": role_summary,
        "anomaly_inbox": anomalies[:50],
        "anomaly_count": len(anomalies),
    }


def _agency_status_payload() -> Dict[str, Any]:
    state_dir = _state_dir().resolve()
    core_db_path = Path(db.get_db_path()).resolve()
    disc_db_path = core_db_path.with_name("disc.duckdb")
    arm_db_path = core_db_path.with_name("arm.duckdb")
    admin_store = _sqlite_table_counts(AGENCY_ADMIN_TABLES)
    disc = _duckdb_table_counts(disc_db_path, AGENCY_DISC_TABLES)
    arm = _duckdb_table_counts(arm_db_path, AGENCY_ARM_SIGNALS)
    eval_reports = _list_agent_eval_reports(state_dir)
    source_allowlist = _load_agent_source_allowlist()
    admin_expected = admin_store.get("expected") or {}
    disc_expected = disc.get("expected") or {}
    live_portfolio_tables = {
        name: admin_expected.get(name, {"exists": False, "count": None})
        for name in ["object_dossiers", "source_documents", "claim_bundles", "extracted_claims", "portfolio_journal_entries"]
    }
    live_counts = {
        name: int(value.get("count") or 0)
        for name, value in live_portfolio_tables.items()
        if value.get("exists") and value.get("count") is not None
    }
    persistence_ready = all((admin_expected.get(name) or {}).get("exists") for name in live_portfolio_tables)
    return {
        "status": "ok",
        "generated_at_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "paths": {
            "state_dir": str(state_dir),
            "admin_db_path": str(admin_db.get_admin_db_path()),
            "core_db_path": str(core_db_path),
            "disc_db_path": str(disc_db_path),
            "arm_db_path": str(arm_db_path),
            "bulk_archive_root": "/mnt/space/spacegate/agent_archive",
        },
        "workflow_stages": AGENCY_WORKFLOW_STAGES,
        "storage_model": {
            "hot_layer": "admin operational rows for active dossiers, source files, extraction sets, findings, and journal entries",
            "disc_materialization": "disc remains the future/public materialized citation, factsheet, exposition, and evidence-link layer",
            "proposal_layer": "arm proposal/overlay rows for accepted supplemental science and adjudication candidates",
            "cold_archive": "/mnt/space/spacegate/agent_archive for compressed dossier packages and bulky source snapshots",
            "core_policy": "agents never write directly to core",
        },
        "readiness": {
            "portfolio_persistence_ready": persistence_ready,
            "live_counts": live_counts,
            "missing_admin_tables": [name for name in AGENCY_ADMIN_TABLES if not (admin_expected.get(name) or {}).get("exists")],
            "implemented_admin_tables": [name for name in AGENCY_ADMIN_TABLES if (admin_expected.get(name) or {}).get("exists")],
            "missing_disc_tables": [name for name in AGENCY_DISC_TABLES if not (disc_expected.get(name) or {}).get("exists")],
            "implemented_disc_tables": [name for name in AGENCY_DISC_TABLES if (disc_expected.get(name) or {}).get("exists")],
            "notes": [
                "This endpoint is read-only; operational dossier persistence is initialized in the admin database.",
                "Eval report anomalies are quarantine signals, not accepted science.",
                "Portfolio chat should be scoped to retrieved sources, extraction sets, findings, proposals, and journal entries.",
                "Public disc materialization remains separate from mutable admin workflow state.",
                "Source allowlist edits are stored as a runtime JSON policy under state/config.",
            ],
        },
        "source_allowlist": {
            "summary": source_allowlist.get("summary"),
            "source_path": source_allowlist.get("source_path"),
            "runtime_path": source_allowlist.get("runtime_path"),
            "runtime_override_exists": source_allowlist.get("runtime_override_exists"),
            "updated_at_utc": source_allowlist.get("updated_at_utc"),
            "updated_by": source_allowlist.get("updated_by"),
        },
        "admin_store": admin_store,
        "disc": disc,
        "arm": arm,
        "eval_reports": eval_reports,
        "interaction_model": {
            "recommended": "Build a Spacegate-native portfolio conversation workbench.",
            "why": "Generic chat tools do not understand Spacegate layer ownership, claim/proposal state, source allowlists, or citation obligations.",
            "minimum_features": [
                "portfolio-scoped context assembly",
                "visible source/finding/proposal selectors",
                "bounded prompts and recorded prompt/runtime metadata",
                "read-only agent Q&A by default",
                "explicit proposal creation instead of direct core edits",
                "journal entry for each meaningful exchange",
            ],
            "possible_sidecars": ["Open WebUI", "LibreChat", "AnythingLLM", "Dify"],
        },
    }


RUNTIME_ENV_SPECS: Dict[str, Dict[str, Any]] = {
    "SPACEGATE_STATE_DIR": {"required": True, "description": "Primary runtime state root."},
    "SPACEGATE_DATA_DIR": {"required": False, "description": "Legacy/fallback state root; not needed when SPACEGATE_STATE_DIR is set.", "satisfied_by": ["SPACEGATE_STATE_DIR"]},
    "SPACEGATE_CACHE_DIR": {"required": False, "description": "Optional cache override; defaults under state."},
    "SPACEGATE_LOG_DIR": {"required": False, "description": "Optional log override; defaults under state."},
    "SPACEGATE_ADMIN_DB_PATH": {"required": False, "description": "Optional admin DB path override; defaults under state."},
    "SPACEGATE_ADMIN_JOBS_DIR": {"required": False, "description": "Optional admin jobs path override; defaults under state."},
    "SPACEGATE_WEB_BIND": {"required": False, "description": "Web container bind setting; usually not present in the API container."},
    "SPACEGATE_WEB_TLS_BIND": {"required": False, "description": "Web container TLS bind setting; usually not present in the API container."},
    "SPACEGATE_WEB_HOST_PORT": {"required": False, "description": "Compose/web host port setting; usually not present in the API container."},
    "SPACEGATE_WEB_TLS_HOST_PORT": {"required": False, "description": "Compose/web TLS host port setting; usually not present in the API container."},
    "SPACEGATE_AUTH_ENABLE": {"required": True, "description": "Admin auth enable flag."},
    "SPACEGATE_OIDC_PROVIDER": {"required": True, "description": "OIDC provider name."},
    "SPACEGATE_OIDC_ISSUER": {"required": True, "description": "OIDC issuer URL."},
    "SPACEGATE_OIDC_REDIRECT_URI": {"required": True, "description": "OIDC callback URL."},
    "SPACEGATE_AUTH_SUCCESS_REDIRECT": {"required": False, "description": "Post-login redirect; defaults to the admin app."},
    "SPACEGATE_SESSION_COOKIE_SECURE": {"required": False, "description": "Secure cookie setting; defaults according to runtime mode."},
    "SPACEGATE_CSRF_ENABLE": {"required": False, "description": "CSRF protection flag; defaults on for authenticated admin operations."},
    "SPACEGATE_CONTAINER_LLM_BASE_URL": {"required": False, "description": "Optional container-specific local LLM URL override.", "satisfied_by": ["SPACEGATE_LLM_BASE_URL"]},
    "SPACEGATE_LLM_BASE_URL": {"required": False, "description": "Default local/OpenAI-compatible LLM base URL."},
    "SPACEGATE_OPENAI_BASE_URL": {"required": False, "description": "OpenAI API base URL override."},
    "SPACEGATE_GOOGLE_BASE_URL": {"required": False, "description": "Google Gemini API base URL override."},
    "SPACEGATE_FRONTIER_OPENAI_MODEL": {"required": False, "description": "Default OpenAI frontier model."},
    "SPACEGATE_FRONTIER_GOOGLE_MODEL": {"required": False, "description": "Default Google frontier model."},
    "SPACEGATE_BULK_DIR": {"required": True, "description": "Bulk research/document storage root."},
}
RUNTIME_ENV_KEYS = list(RUNTIME_ENV_SPECS.keys())


SENSITIVE_ENV_SPECS: Dict[str, Dict[str, Any]] = {
    "SPACEGATE_OIDC_CLIENT_ID": {"required": True, "description": "OIDC client id."},
    "SPACEGATE_OIDC_CLIENT_SECRET": {"required": True, "description": "OIDC client secret."},
    "SPACEGATE_SESSION_SECRET": {"required": True, "description": "Session signing secret."},
    "SPACEGATE_OPENAI_API_KEY": {"required": False, "description": "Preferred OpenAI API key env var."},
    "OPENAI_API_KEY": {"required": False, "description": "Legacy/provider alias; not needed when SPACEGATE_OPENAI_API_KEY is set.", "satisfied_by": ["SPACEGATE_OPENAI_API_KEY"]},
    "SPACEGATE_GOOGLE_API_KEY": {"required": False, "description": "Preferred Google API key env var."},
    "GOOGLE_API_KEY": {"required": False, "description": "Legacy/provider alias; not needed when SPACEGATE_GOOGLE_API_KEY is set.", "satisfied_by": ["SPACEGATE_GOOGLE_API_KEY"]},
}
SENSITIVE_ENV_KEYS = list(SENSITIVE_ENV_SPECS.keys())


def _nearest_existing_path(path: Path) -> Path:
    current = path
    while not current.exists() and current.parent != current:
        current = current.parent
    return current


def _nearest_mount_point(path: Path) -> Optional[Path]:
    current = path if path.exists() else _nearest_existing_path(path)
    while current.parent != current:
        if os.path.ismount(str(current)):
            return current
        current = current.parent
    return current if os.path.ismount(str(current)) else None


def _path_stat_payload(path: Path) -> Optional[Dict[str, Any]]:
    try:
        info = path.stat()
    except OSError:
        return None
    try:
        owner = pwd.getpwuid(info.st_uid).pw_name
    except KeyError:
        owner = None
    try:
        group = grp.getgrgid(info.st_gid).gr_name
    except KeyError:
        group = None
    mode = stat.S_IMODE(info.st_mode)
    return {
        "uid": int(info.st_uid),
        "gid": int(info.st_gid),
        "owner": owner,
        "group": group,
        "mode_octal": f"{mode:04o}",
        "setuid": bool(mode & stat.S_ISUID),
        "setgid": bool(mode & stat.S_ISGID),
        "sticky": bool(mode & stat.S_ISVTX),
    }


def _path_runtime_status(
    path: Path,
    *,
    expected_type: str = "any",
    require_writable: bool = False,
    required: bool = True,
    configured: bool = True,
    env_key: Optional[str] = None,
    description: Optional[str] = None,
    mount_expected: bool = False,
) -> Dict[str, Any]:
    target = _nearest_existing_path(path)
    usage = None
    disk_error = None
    if target.exists():
        try:
            disk = shutil.disk_usage(target)
            usage = {
                "total_bytes": int(disk.total),
                "used_bytes": int(disk.used),
                "free_bytes": int(disk.free),
                "used_pct": (float(disk.used) / float(disk.total) * 100.0) if disk.total else 0.0,
            }
        except Exception as exc:
            disk_error = str(exc)
            usage = None
    exists = path.exists()
    is_dir = path.is_dir()
    is_file = path.is_file()
    readable = os.access(str(path), os.R_OK) if exists else False
    searchable = os.access(str(path), os.X_OK) if exists and is_dir else None
    writable = os.access(str(path), os.W_OK) if exists else False
    parent = path.parent
    parent_exists = parent.exists()
    parent_writable = os.access(str(parent), os.W_OK) if parent_exists else False
    mount_point = _nearest_mount_point(path) if exists else None
    mounted = bool(mount_point and str(mount_point) != "/")

    issues: List[Dict[str, str]] = []
    if configured and required and not exists:
        issues.append(
            {
                "severity": "error",
                "code": "missing",
                "message": "Configured filesystem target is missing.",
                "next_action": "Create the directory/file or mount the host path into the API container, then refresh Runtime.",
            }
        )
    if exists and expected_type == "dir" and not is_dir:
        issues.append(
            {
                "severity": "error",
                "code": "not_directory",
                "message": "Configured filesystem target exists but is not a directory.",
                "next_action": "Fix the path or replace it with the expected directory.",
            }
        )
    if exists and expected_type == "file" and not is_file:
        issues.append(
            {
                "severity": "error",
                "code": "not_file",
                "message": "Configured filesystem target exists but is not a file.",
                "next_action": "Fix the path or restore the expected file.",
            }
        )
    if exists and not readable:
        issues.append(
            {
                "severity": "error",
                "code": "not_readable",
                "message": "API process cannot read this filesystem target.",
                "next_action": "Check ownership, mode bits, container user, and bind-mount permissions.",
            }
        )
    if exists and is_dir and searchable is False:
        issues.append(
            {
                "severity": "error",
                "code": "not_searchable",
                "message": "API process cannot traverse this directory.",
                "next_action": "Grant execute/search permission on this directory and its parents.",
            }
        )
    if exists and require_writable and not writable:
        issues.append(
            {
                "severity": "error",
                "code": "not_writable",
                "message": "API process cannot write to this configured destination.",
                "next_action": "Check ownership, group membership, mode bits, and Docker volume options.",
            }
        )
    if not exists and expected_type == "file" and require_writable and parent_exists and not parent_writable:
        issues.append(
            {
                "severity": "error",
                "code": "parent_not_writable",
                "message": "Target file is missing and its parent directory is not writable.",
                "next_action": "Restore the file or grant write permission to the parent directory.",
            }
        )
    if exists and mount_expected and not mounted:
        issues.append(
            {
                "severity": "warning",
                "code": "mount_not_visible",
                "message": "Path exists, but no non-root mount point is visible for this target.",
                "next_action": "Confirm the host filesystem is mounted and the Docker bind mount targets this path.",
            }
        )
    if exists and disk_error:
        issues.append(
            {
                "severity": "warning",
                "code": "disk_usage_unavailable",
                "message": "Disk usage could not be read for this target.",
                "next_action": "Check filesystem accessibility from inside the API container.",
            }
        )
    check_status = "ok"
    if any(issue["severity"] == "error" for issue in issues):
        check_status = "error"
    elif issues:
        check_status = "warning"
    return {
        "path": str(path),
        "description": description,
        "env_key": env_key,
        "configured": configured,
        "required": required,
        "expected_type": expected_type,
        "require_writable": require_writable,
        "exists": exists,
        "is_dir": is_dir,
        "is_file": is_file,
        "is_symlink": path.is_symlink(),
        "resolved": str(path.resolve()) if exists else None,
        "readable": readable,
        "searchable": searchable,
        "writable": writable,
        "parent": str(parent),
        "parent_exists": parent_exists,
        "parent_writable": parent_writable,
        "mount_expected": mount_expected,
        "mounted": mounted,
        "mount_point": str(mount_point) if mount_point else None,
        "check_status": check_status,
        "issues": issues,
        "disk": usage,
        "stat": _path_stat_payload(path) if exists else None,
    }


def _filesystem_alerts(paths: Dict[str, Dict[str, Any]]) -> List[Dict[str, str]]:
    alerts: List[Dict[str, str]] = []
    for key, item in paths.items():
        for issue in item.get("issues") or []:
            alerts.append(
                {
                    "severity": str(issue.get("severity") or "warning"),
                    "path_key": key,
                    "path": str(item.get("path") or ""),
                    "env_key": str(item.get("env_key") or ""),
                    "code": str(issue.get("code") or "filesystem_issue"),
                    "message": str(issue.get("message") or "Filesystem issue detected."),
                    "next_action": str(issue.get("next_action") or "Inspect the configured path from inside the API container."),
                }
            )
    severity_rank = {"error": 0, "warning": 1, "info": 2}
    alerts.sort(key=lambda alert: (severity_rank.get(alert["severity"], 9), alert["path_key"], alert["code"]))
    return alerts


def _env_runtime_status(key: str, spec: Dict[str, Any], *, include_value: bool) -> Dict[str, Any]:
    raw_value = os.getenv(key, "").strip()
    configured = bool(raw_value)
    satisfied_by = [
        alias
        for alias in spec.get("satisfied_by") or []
        if os.getenv(str(alias), "").strip()
    ]
    required = bool(spec.get("required"))
    if configured:
        status = "configured"
        note = spec.get("description") or ""
    elif satisfied_by:
        status = "alias_satisfied"
        note = f"Not set; satisfied by {', '.join(satisfied_by)}."
    elif required:
        status = "missing"
        note = spec.get("description") or "Required variable is missing from the API container."
    else:
        status = "optional_missing"
        note = spec.get("description") or "Optional variable is not set."
    payload = {
        "configured": configured,
        "required": required,
        "status": status,
        "satisfied_by": satisfied_by,
        "description": spec.get("description") or "",
        "note": note,
    }
    if include_value:
        payload["value"] = raw_value or None
    return payload


def _split_runtime_list(value: str) -> List[str]:
    return [item for item in (part.strip() for part in value.split("|")) if item]


def _config_source_role(path: str) -> str:
    if path == "/etc/spacegate/spacegate.env":
        return "host secrets/deployment config"
    if path.endswith("/.spacegate.env"):
        return "repo-local nonsecret override"
    if path.endswith("/.spacegate.local.env"):
        return "repo-local private override"
    if os.getenv("SPACEGATE_ENV_FILE", "").strip() == path:
        return "explicit override"
    if path.startswith("/srv/spacegate/") and path.endswith(".env"):
        return "host-local runtime config"
    return "env source"


def _config_sources_payload() -> Dict[str, Any]:
    candidate_files = _split_runtime_list(os.getenv("SPACEGATE_ENV_CANDIDATE_FILES", ""))
    loaded_files = set(_split_runtime_list(os.getenv("SPACEGATE_ENV_LOADED_FILES", "")))
    missing_files = set(_split_runtime_list(os.getenv("SPACEGATE_ENV_MISSING_FILES", "")))
    unreadable_files = set(_split_runtime_list(os.getenv("SPACEGATE_ENV_UNREADABLE_FILES", "")))
    rows: List[Dict[str, Any]] = []
    for index, path in enumerate(candidate_files, start=1):
        if path in loaded_files:
            status = "loaded"
        elif path in unreadable_files:
            status = "unreadable"
        elif path in missing_files:
            status = "missing"
        else:
            status = "unknown"
        rows.append(
            {
                "path": path,
                "status": status,
                "role": _config_source_role(path),
                "precedence": index,
            }
        )
    return {
        "host_name": os.getenv("SPACEGATE_ENV_HOST_NAME", "").strip() or None,
        "sources": rows,
        "candidate_count": len(candidate_files),
        "loaded_count": len(loaded_files),
        "missing_count": len(missing_files),
        "unreadable_count": len(unreadable_files),
        "notes": [
            "Config source diagnostics are launcher-observed metadata passed into the API container.",
            "Source file contents and secret values are never exposed by Runtime.",
            "Later files have higher precedence; existing process environment values override file values.",
        ],
    }


def _inference_credential_envs_payload() -> Dict[str, Any]:
    known = {
        "SPACEGATE_OPENAI_API_KEY": {"provider": "openai", "label": "OpenAI primary"},
        "OPENAI_API_KEY": {"provider": "openai", "label": "OpenAI legacy alias"},
        "SPACEGATE_GOOGLE_API_KEY": {"provider": "google", "label": "Google primary"},
        "GOOGLE_API_KEY": {"provider": "google", "label": "Google legacy alias"},
    }
    discovered = {
        key
        for key in os.environ
        if key.startswith("SPACEGATE_")
        and key.endswith("_API_KEY")
        and key not in known
    }
    items: List[Dict[str, Any]] = []
    for key in sorted(set(known) | discovered):
        meta = known.get(key) or {}
        upper = key.upper()
        provider = meta.get("provider")
        if not provider:
            if "OPENAI" in upper:
                provider = "openai"
            elif "GOOGLE" in upper or "GEMINI" in upper:
                provider = "google"
            else:
                provider = "custom"
        items.append(
            {
                "env_key": key,
                "provider": provider,
                "label": meta.get("label") or key.replace("SPACEGATE_", "").replace("_API_KEY", "").replace("_", " ").title(),
                "configured": bool(os.getenv(key, "").strip()),
                "preferred": key.startswith("SPACEGATE_"),
                "source": "known" if key in known else "discovered",
            }
        )
    items.sort(key=lambda item: (not item["configured"], item["provider"], item["env_key"]))
    return {
        "items": items,
        "notes": [
            "Only environment variable names and configured/missing flags are returned.",
            "Add named provider keys to /etc/spacegate/spacegate.env using SPACEGATE_*_API_KEY names, then recreate the containers.",
            "Inference endpoints may reference any env var name even if it is not currently configured.",
        ],
    }


def _git_head_short() -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", "-C", str(ROOT_DIR), "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=3,
        )
        return result.stdout.strip() or None
    except Exception:
        return None


def _parse_proc_int(value: Optional[str], default: int = 0) -> int:
    match = re.search(r"[0-9]+", str(value or ""))
    if not match:
        return default
    try:
        return int(match.group(0))
    except ValueError:
        return default


def _parse_proc_hex(value: Optional[str]) -> Optional[int]:
    raw = str(value or "").strip().split()[0] if value else ""
    if not raw:
        return None
    try:
        return int(raw, 16)
    except ValueError:
        return None


def _write_probe(path: Path) -> Dict[str, Any]:
    probe = path / f".spacegate_runtime_probe_{os.getpid()}_{uuid.uuid4().hex}"
    try:
        fd = os.open(str(probe), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        try:
            probe.unlink()
        except OSError:
            pass
        return {"status": "writable", "path": str(path), "ok": True}
    except OSError as exc:
        if exc.errno == errno.EROFS:
            status = "read_only_filesystem"
        elif exc.errno in {errno.EACCES, errno.EPERM}:
            status = "permission_denied"
        else:
            status = "blocked"
        return {
            "status": status,
            "path": str(path),
            "ok": False,
            "errno": exc.errno,
            "error": exc.strerror or str(exc),
        }


def _runtime_security_payload(proc_status: Dict[str, str], state_dir: Path) -> Dict[str, Any]:
    effective_uid = os.geteuid()
    effective_gid = os.getegid()
    groups = [int(group_id) for group_id in os.getgroups()]
    cap_eff = _parse_proc_hex(proc_status.get("CapEff"))
    cap_prm = _parse_proc_hex(proc_status.get("CapPrm"))
    cap_bnd = _parse_proc_hex(proc_status.get("CapBnd"))
    no_new_privs = _parse_proc_int(proc_status.get("NoNewPrivs")) == 1
    seccomp_mode = _parse_proc_int(proc_status.get("Seccomp"))
    root_probe = _write_probe(ROOT_DIR)
    tmp_probe = _write_probe(Path("/tmp"))
    state_probe = _write_probe(state_dir)
    try:
        expected_uid = int(os.getenv("SPACEGATE_CONTAINER_UID", "").strip())
    except ValueError:
        expected_uid = None
    try:
        expected_gid = int(os.getenv("SPACEGATE_CONTAINER_GID", "").strip())
    except ValueError:
        expected_gid = None
    uid_matches = expected_uid is None or expected_uid == effective_uid
    gid_matches = expected_gid is None or expected_gid == effective_gid
    hardening_checks = [
        {"key": "non_root_user", "ok": effective_uid != 0, "label": "API process is not running as root."},
        {"key": "expected_uid_gid", "ok": uid_matches and gid_matches, "label": "API UID/GID match launcher-provided container identity."},
        {"key": "no_new_privileges", "ok": no_new_privs, "label": "No-new-privileges is active."},
        {"key": "capabilities_dropped", "ok": cap_eff == 0 and cap_prm == 0, "label": "Effective/permitted Linux capabilities are empty."},
        {"key": "seccomp_filter", "ok": seccomp_mode == 2, "label": "Seccomp filter mode is active."},
        {"key": "read_only_rootfs", "ok": root_probe.get("status") == "read_only_filesystem", "label": "Repository filesystem write probe is blocked as read-only."},
        {"key": "tmp_writable", "ok": tmp_probe.get("ok") is True, "label": "Scratch tmpfs is writable."},
        {"key": "state_writable", "ok": state_probe.get("ok") is True, "label": "Configured state root is writable."},
    ]
    failed_checks = [item for item in hardening_checks if not item["ok"]]
    return {
        "effective_uid": int(effective_uid),
        "effective_gid": int(effective_gid),
        "groups": groups,
        "expected_uid": expected_uid,
        "expected_gid": expected_gid,
        "running_as_root": effective_uid == 0,
        "umask_configured": os.getenv("SPACEGATE_UMASK", "").strip() or None,
        "python_bytecode_disabled": os.getenv("PYTHONDONTWRITEBYTECODE", "").strip() in {"1", "true", "True"},
        "no_new_privileges": no_new_privs,
        "seccomp_mode": seccomp_mode,
        "seccomp_label": {0: "disabled", 1: "strict", 2: "filter"}.get(seccomp_mode, "unknown"),
        "capabilities": {
            "effective_hex": proc_status.get("CapEff"),
            "permitted_hex": proc_status.get("CapPrm"),
            "bounding_hex": proc_status.get("CapBnd"),
            "effective_empty": cap_eff == 0,
            "permitted_empty": cap_prm == 0,
            "bounding_empty": cap_bnd == 0,
        },
        "write_probes": {
            "project_root": root_probe,
            "tmp": tmp_probe,
            "state_dir": state_probe,
        },
        "hardening_checks": hardening_checks,
        "summary": {
            "status": "ok" if not failed_checks else "warning",
            "passed": len(hardening_checks) - len(failed_checks),
            "total": len(hardening_checks),
            "failed_keys": [item["key"] for item in failed_checks],
        },
        "notes": [
            "Runtime security is observed from inside the API process and does not require the Docker socket.",
            "Docker/Compose users can still inspect container environment through Docker privileges; treat Docker access as privileged.",
        ],
    }


def _runtime_status_payload() -> Dict[str, Any]:
    state_dir = _state_dir().resolve()
    core_db_path = Path(db.get_db_path()).resolve()
    proc_status = _read_proc_key_values(Path("/proc/self/status"))
    proc_io = _read_proc_key_values(Path("/proc/self/io"))
    meminfo = _read_proc_key_values(Path("/proc/meminfo"))
    build_id = _current_build_id_or_none()
    auth_status = auth.auth_runtime_status()
    endpoint_rows = inference_registry.list_endpoints()
    endpoint_summary = []
    for endpoint in endpoint_rows:
        last_probe = endpoint.get("last_probe") or {}
        endpoint_summary.append(
            {
                "endpoint_id": endpoint.get("endpoint_id"),
                "endpoint_key": endpoint.get("endpoint_key"),
                "display_name": endpoint.get("display_name"),
                "provider": endpoint.get("provider"),
                "enabled": endpoint.get("enabled"),
                "base_url": endpoint.get("base_url"),
                "auth_mode": endpoint.get("auth_mode"),
                "api_key_configured": endpoint.get("api_key_configured"),
                "default_model": endpoint.get("default_model"),
                "model_count": len(endpoint.get("models") or []),
                "last_probe_status": last_probe.get("status"),
                "last_probe_at": last_probe.get("probed_at"),
                "last_probe_error": last_probe.get("error_message"),
            }
        )
    bulk_env = os.getenv("SPACEGATE_BULK_DIR", "").strip()
    path_specs = {
        "project_root": {
            "path": ROOT_DIR,
            "expected_type": "dir",
            "require_writable": False,
            "description": "Repository root visible to the API runtime.",
        },
        "state_dir": {
            "path": state_dir,
            "expected_type": "dir",
            "require_writable": True,
            "env_key": "SPACEGATE_STATE_DIR",
            "description": "Primary Spacegate state root for builds, reports, admin DB, and runtime artifacts.",
        },
        "cache_dir": {
            "path": Path(os.getenv("SPACEGATE_CACHE_DIR") or state_dir / "cache"),
            "expected_type": "dir",
            "require_writable": True,
            "env_key": "SPACEGATE_CACHE_DIR",
            "description": "Runtime cache directory.",
        },
        "log_dir": {
            "path": Path(os.getenv("SPACEGATE_LOG_DIR") or state_dir / "logs"),
            "expected_type": "dir",
            "require_writable": True,
            "env_key": "SPACEGATE_LOG_DIR",
            "description": "Runtime log directory.",
        },
        "admin_db_path": {
            "path": admin_db.get_admin_db_path(),
            "expected_type": "file",
            "require_writable": True,
            "env_key": "SPACEGATE_ADMIN_DB_PATH",
            "description": "Admin SQLite database for auth, jobs, audit, and registries.",
        },
        "admin_jobs_dir": {
            "path": Path(os.getenv("SPACEGATE_ADMIN_JOBS_DIR") or state_dir / "admin" / "jobs"),
            "expected_type": "dir",
            "require_writable": True,
            "env_key": "SPACEGATE_ADMIN_JOBS_DIR",
            "description": "Admin job records and logs.",
        },
        "core_db_path": {
            "path": core_db_path,
            "expected_type": "file",
            "require_writable": False,
            "description": "Served immutable core science database.",
        },
        "disc_db_path": {
            "path": core_db_path.with_name("disc.duckdb"),
            "expected_type": "file",
            "require_writable": False,
            "description": "Served disc presentation/artifact database.",
        },
        "arm_db_path": {
            "path": core_db_path.with_name("arm.duckdb"),
            "expected_type": "file",
            "require_writable": False,
            "description": "Served arm supplemental science/proposal database.",
        },
        "reports_dir": {
            "path": state_dir / "reports",
            "expected_type": "dir",
            "require_writable": True,
            "description": "Build, verification, and agent/eval reports.",
        },
        "served_current": {
            "path": state_dir / "served" / "current",
            "expected_type": "dir",
            "require_writable": False,
            "description": "Current promoted immutable build symlink/directory.",
        },
        "bulk_research_root": {
            "path": Path(bulk_env or "/mnt/space/spacegate"),
            "expected_type": "dir",
            "require_writable": True,
            "env_key": "SPACEGATE_BULK_DIR",
            "mount_expected": bool(bulk_env),
            "description": "Bulk research, source documents, dossiers, OCR, and large reusable caches.",
        },
        "model_cache_root": {
            "path": Path("/data/models"),
            "expected_type": "dir",
            "required": False,
            "configured": False,
            "description": "Optional model cache visibility probe.",
        },
        "docker_data_root": {
            "path": Path("/data/docker"),
            "expected_type": "dir",
            "required": False,
            "configured": False,
            "description": "Optional Docker data-root visibility probe.",
        },
    }
    configured_env = {
        key: _env_runtime_status(key, spec, include_value=True)
        for key, spec in RUNTIME_ENV_SPECS.items()
    }
    sensitive_env = {
        key: _env_runtime_status(key, spec, include_value=False)
        for key, spec in SENSITIVE_ENV_SPECS.items()
    }
    paths = {
        key: _path_runtime_status(**spec)
        for key, spec in path_specs.items()
    }
    filesystem_alerts = _filesystem_alerts(paths)
    filesystem_summary = {
        "alert_count": len(filesystem_alerts),
        "error_count": sum(1 for alert in filesystem_alerts if alert["severity"] == "error"),
        "warning_count": sum(1 for alert in filesystem_alerts if alert["severity"] == "warning"),
        "checked_count": len(paths),
        "configured_target_count": sum(1 for item in paths.values() if item.get("configured")),
    }
    docker_socket = Path("/var/run/docker.sock")
    return {
        "status": "ok",
        "generated_at_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "build_id": build_id,
        "git": {
            "head_short": _git_head_short(),
        },
        "auth": auth_status,
        "paths": paths,
        "filesystem_alerts": filesystem_alerts,
        "filesystem_summary": filesystem_summary,
        "environment": {
            "configured": configured_env,
            "sensitive": sensitive_env,
            "config_sources": _config_sources_payload(),
            "notes": [
                "Sensitive values are reported only as configured/missing flags.",
                "Container-visible paths may differ from host paths when volumes are not mounted into the API container.",
            ],
        },
        "container_runtime": {
            "hostname": os.getenv("HOSTNAME"),
            "in_container": Path("/.dockerenv").exists(),
            "docker_socket_visible": docker_socket.exists(),
            "docker_socket_readable": docker_socket.exists() and os.access(str(docker_socket), os.R_OK),
            "docker_status_note": "Docker container health is not queried from the API container unless the Docker socket is deliberately mounted.",
        },
        "runtime_security": _runtime_security_payload(proc_status, state_dir),
        "host_runtime": {
            "cpu_count": os.cpu_count(),
            "loadavg_1m": os.getloadavg()[0] if hasattr(os, "getloadavg") else None,
            "loadavg_5m": os.getloadavg()[1] if hasattr(os, "getloadavg") else None,
            "loadavg_15m": os.getloadavg()[2] if hasattr(os, "getloadavg") else None,
            "mem_total_bytes": _proc_kib_value(meminfo.get("MemTotal")),
            "mem_available_bytes": _proc_kib_value(meminfo.get("MemAvailable")),
        },
        "api_process_runtime": {
            "pid": os.getpid(),
            "rss_bytes": _proc_kib_value(proc_status.get("VmRSS")),
            "peak_rss_bytes": _proc_kib_value(proc_status.get("VmHWM")),
            "vm_size_bytes": _proc_kib_value(proc_status.get("VmSize")),
            "threads": int(re.search(r"[0-9]+", proc_status.get("Threads", "0")).group(0)) if re.search(r"[0-9]+", proc_status.get("Threads", "0")) else 0,
            "io_read_bytes": int(re.search(r"[0-9]+", proc_io.get("read_bytes", "0")).group(0)) if re.search(r"[0-9]+", proc_io.get("read_bytes", "0")) else 0,
            "io_write_bytes": int(re.search(r"[0-9]+", proc_io.get("write_bytes", "0")).group(0)) if re.search(r"[0-9]+", proc_io.get("write_bytes", "0")) else 0,
        },
        "inference_endpoints": endpoint_summary,
    }


def _redacted_env_status(rows: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {
        key: {
            "configured": bool(item.get("configured")),
            "required": bool(item.get("required")),
            "status": item.get("status") or "unknown",
            "satisfied_by": item.get("satisfied_by") or [],
            "note": item.get("note") or item.get("description") or "",
        }
        for key, item in rows.items()
    }


def _runtime_diagnostics_payload(status: Dict[str, Any]) -> Dict[str, Any]:
    environment = status.get("environment") if isinstance(status.get("environment"), dict) else {}
    return {
        "kind": "spacegate.admin.runtime_diagnostics.v1",
        "redacted": True,
        "generated_at_utc": status.get("generated_at_utc"),
        "build_id": status.get("build_id"),
        "git": status.get("git") or {},
        "filesystem_summary": status.get("filesystem_summary") or {},
        "filesystem_alerts": status.get("filesystem_alerts") or [],
        "paths": status.get("paths") or {},
        "environment": {
            "configured": _redacted_env_status(environment.get("configured") or {}),
            "sensitive": _redacted_env_status(environment.get("sensitive") or {}),
            "config_sources": environment.get("config_sources") or {},
            "notes": environment.get("notes") or [],
        },
        "auth": status.get("auth") or {},
        "container_runtime": status.get("container_runtime") or {},
        "runtime_security": status.get("runtime_security") or {},
        "host_runtime": status.get("host_runtime") or {},
        "api_process_runtime": status.get("api_process_runtime") or {},
        "inference_endpoints": [
            {
                "endpoint_key": endpoint.get("endpoint_key"),
                "display_name": endpoint.get("display_name"),
                "provider": endpoint.get("provider"),
                "enabled": endpoint.get("enabled"),
                "base_url": endpoint.get("base_url"),
                "auth_mode": endpoint.get("auth_mode"),
                "api_key_configured": bool(endpoint.get("api_key_configured")),
                "default_model": endpoint.get("default_model"),
                "model_count": endpoint.get("model_count"),
                "last_probe_status": endpoint.get("last_probe_status"),
                "last_probe_at": endpoint.get("last_probe_at"),
                "last_probe_error": endpoint.get("last_probe_error"),
            }
            for endpoint in status.get("inference_endpoints") or []
            if isinstance(endpoint, dict)
        ],
        "redaction_notes": [
            "Environment variable values are omitted; only configured/missing/status metadata is included.",
            "Sensitive provider, OIDC, and session secret values are never included.",
            "Docker users can still inspect container environment through Docker privileges; treat Docker access as privileged.",
        ],
    }


@admin_router.get("/status")
def admin_status(request: Request):
    user = auth.require_admin(request)
    with db.connection_scope() as con:
        build_id = fetch_build_id(con)
    return {
        "status": "ok",
        "build_id": build_id,
        "db_path": db.get_db_path(),
        "auth": auth.auth_runtime_status(),
        "user": {
            "user_id": user["user_id"],
            "email": user["email"],
            "display_name": user["display_name"],
            "roles": sorted(set(user.get("roles", []))),
        },
        "time_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }


@admin_router.get("/status/dataset")
def admin_dataset_status(
    request: Request,
    refresh: bool = Query(default=False),
):
    auth.require_admin(request)
    return _dataset_status_payload(force_refresh=bool(refresh))


@admin_router.get("/objects/search")
def admin_objects_search(
    request: Request,
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=50),
):
    auth.require_admin(request)
    q_norm = normalize_query_text(q or "")
    id_query = parse_identifier_query(q_norm)
    system_id_exact: Optional[int] = None
    diagnostic_focus: Optional[Dict[str, Any]] = None
    system_id_match = re.match(r"^(?:system|sys)\s+(\d+)$", q_norm or "")
    if system_id_match:
        try:
            system_id_exact = int(system_id_match.group(1))
        except ValueError:
            system_id_exact = None
    object_id_match = re.match(r"^(star|planet)\s+(\d+)$", q_norm or "")
    started_at = datetime.datetime.utcnow()
    disc_db_path = _resolve_disc_db_path()
    arm_db_path = _resolve_arm_db_path()
    with db.connection_scope() as con:
        if system_id_exact is None and object_id_match:
            object_type = object_id_match.group(1)
            object_id = int(object_id_match.group(2))
            table_name = "stars" if object_type == "star" else "planets"
            id_column = "star_id" if object_type == "star" else "planet_id"
            row = con.execute(
                f"SELECT system_id FROM {table_name} WHERE {id_column} = ? LIMIT 1",
                [object_id],
            ).fetchone()
            if row:
                system_id_exact = int(row[0])
                diagnostic_focus = {"type": object_type, "id": object_id}
        component_matches = []
        if system_id_exact is None and not object_id_match:
            component_matches = _admin_component_search_matches(
                con,
                q_raw=q,
                q_norm=q_norm,
                limit=limit,
            )
        rows, total_count = search_systems(
            con,
            q_norm=q_norm or None,
            q_raw=q,
            system_id_exact=system_id_exact,
            id_query=id_query,
            max_dist_ly=None,
            min_dist_ly=None,
            min_star_count=None,
            max_star_count=None,
            min_planet_count=None,
            max_planet_count=None,
            min_temp_k=None,
            max_temp_k=None,
            spectral_classes=[],
            has_planets=None,
            has_habitable=None,
            min_coolness_score=None,
            max_coolness_score=None,
            sort="name",
            match_mode=bool(q_norm) or bool(id_query),
            limit=limit,
            include_total=True,
            cursor_values=None,
            disc_db_path=disc_db_path,
            arm_db_path=arm_db_path,
        )
        focused_rows: List[Dict[str, Any]] = []
        for match in component_matches:
            row = _admin_search_system_by_id(
                con,
                system_id=int(match["system_id"]),
                disc_db_path=disc_db_path,
                arm_db_path=arm_db_path,
            )
            if not row:
                continue
            row["diagnostic_focus"] = match["diagnostic_focus"]
            row["object_match"] = match["object_match"]
            focused_rows.append(row)
        if focused_rows:
            focused_system_ids = {int(row["system_id"]) for row in focused_rows if row.get("system_id") is not None}
            merged_rows = focused_rows[:]
            for row in rows:
                if row.get("system_id") is not None and int(row["system_id"]) in focused_system_ids:
                    continue
                merged_rows.append(row)
            rows = merged_rows
            total_count = max(int(total_count or 0), len(rows))
    for item in rows:
        _attach_snapshot_url(item)
        if diagnostic_focus and item.get("system_id") == system_id_exact:
            item["diagnostic_focus"] = diagnostic_focus
    duration_ms = max(0, int((datetime.datetime.utcnow() - started_at).total_seconds() * 1000))
    return {
        "items": rows[:limit],
        "total_count": total_count,
        "query_time_ms": duration_ms,
        "query": q or "",
    }


@admin_router.get("/objects/systems/{system_id}")
def admin_object_system_detail(request: Request, system_id: int):
    auth.require_admin(request)
    try:
        return _system_object_diagnostics(system_id)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "System not found",
                "details": {"system_id": system_id},
            },
        )


@admin_router.get("/runtime/status")
def admin_runtime_status(request: Request):
    auth.require_admin(request)
    return _runtime_status_payload()


@admin_router.get("/runtime/diagnostics")
def admin_runtime_diagnostics(request: Request, download: bool = Query(default=False)):
    auth.require_admin(request)
    status = _runtime_status_payload()
    payload = _runtime_diagnostics_payload(status)
    text = json.dumps(payload, indent=2, sort_keys=True)
    headers = {"Cache-Control": "no-store"}
    if download:
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        headers["Content-Disposition"] = f'attachment; filename="spacegate-runtime-diagnostics-{timestamp}.json"'
    return PlainTextResponse(text, media_type="application/json", headers=headers)


@admin_router.get("/agency/status")
def admin_agency_status(request: Request):
    auth.require_admin(request)
    return _agency_status_payload()


@admin_router.get("/agency/source-allowlist")
def admin_agency_source_allowlist(request: Request):
    auth.require_admin(request)
    return _load_agent_source_allowlist()


@admin_router.post("/agency/source-allowlist/sources")
def admin_agency_source_allowlist_upsert(request: Request, payload: AgencySourceAllowlistEntryRequest):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)
    updated = _upsert_agent_source_allowlist_entry(payload, user)
    auth.audit_event(
        request,
        event_type="admin.agency.source_allowlist.upsert",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={
            "domain": _normalize_allowlist_domain(payload.domain),
            "tier": payload.tier,
            "trust_score": payload.trust_score,
            "enabled": payload.enabled,
        },
    )
    return updated


@admin_router.post("/agency/source-allowlist/restore-default")
def admin_agency_source_allowlist_restore_default(request: Request):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)
    updated = _restore_agent_source_allowlist_default(user)
    auth.audit_event(
        request,
        event_type="admin.agency.source_allowlist.restore_default",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={"source": "spacegate_default"},
    )
    return updated


@admin_router.post("/agency/source-allowlist/restore-version")
def admin_agency_source_allowlist_restore_version(request: Request, payload: AgencySourceAllowlistRestoreRequest):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)
    clean_version_id = _safe_allowlist_version_id(payload.version_id)
    updated = _restore_agent_source_allowlist_version(clean_version_id, user)
    auth.audit_event(
        request,
        event_type="admin.agency.source_allowlist.restore_version",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={"version_id": clean_version_id},
    )
    return updated


@admin_router.delete("/agency/source-allowlist/sources/{domain}")
def admin_agency_source_allowlist_delete(request: Request, domain: str):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)
    clean_domain = _normalize_allowlist_domain(domain)
    updated = _delete_agent_source_allowlist_entry(clean_domain, user)
    auth.audit_event(
        request,
        event_type="admin.agency.source_allowlist.delete",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={"domain": clean_domain},
    )
    return updated


@admin_router.get("/agency/seed-candidates")
def admin_agency_seed_candidates(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
):
    auth.require_admin(request)
    return _agency_seed_candidates(limit=limit)


@admin_router.get("/agency/portfolios")
def admin_agency_portfolios(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    status: str | None = Query(default=None),
    stable_object_key: str | None = Query(default=None),
    object_type: str | None = Query(default=None),
):
    auth.require_admin(request)
    clean_status = str(status or "").strip() or None
    clean_stable_key = str(stable_object_key or "").strip() or None
    clean_object_type = str(object_type or "").strip().lower() or None
    return _list_agency_portfolios(
        limit=limit,
        status=clean_status,
        stable_object_key=clean_stable_key,
        object_type=clean_object_type,
    )


@admin_router.post("/agency/portfolios")
def admin_agency_portfolio_seed(request: Request, payload: AgencyPortfolioSeedRequest):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)
    detail = _seed_agency_portfolio(payload, user)
    auth.audit_event(
        request,
        event_type="admin.agency.portfolio.seed",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={
            "dossier_id": detail.get("dossier", {}).get("dossier_id"),
            "stable_object_key": payload.stable_object_key,
            "object_type": payload.object_type,
            "source": payload.source,
        },
    )
    return detail


@admin_router.get("/agency/portfolios/{dossier_id}")
def admin_agency_portfolio_detail(request: Request, dossier_id: str):
    auth.require_admin(request)
    return _agency_portfolio_detail(dossier_id)


@admin_router.post("/dataset/slice/preview")
def admin_dataset_slice_preview(request: Request, payload: DatasetSlicePreviewRequest):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)

    allowed_map = {
        "O": "O",
        "B": "B",
        "A": "A",
        "F": "F",
        "G": "G",
        "K": "K",
        "M": "M",
        "L": "L",
        "T": "T",
        "Y": "Y",
        "D": "D",
        "?": "UNKNOWN",
        "UNK": "UNKNOWN",
        "UNKNOWN": "UNKNOWN",
    }
    allowed_values: List[str] = []
    for raw in payload.allowed_spectral_classes or []:
        token = str(raw or "").strip().upper()
        if not token:
            continue
        mapped = allowed_map.get(token)
        if not mapped:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "bad_request",
                    "message": f"Invalid spectral class token: {raw!r}",
                    "details": {"allowed": sorted(set(allowed_map.keys()))},
                },
            )
        if mapped not in allowed_values:
            allowed_values.append(mapped)

    warnings: List[str] = []
    applied_filters: Dict[str, Any] = {}

    with db.connection_scope() as con:
        star_cols = {
            str(row[1])
            for row in con.execute("select * from pragma_table_info('stars')").fetchall()
        }

        where_clauses: List[str] = []
        where_params: List[Any] = []

        def _apply_optional_threshold(
            request_key: str,
            column_name: str,
            op: str,
            value: Optional[float],
            *,
            require_non_null: bool = True,
        ) -> None:
            if value is None:
                return
            if column_name not in star_cols:
                warnings.append(
                    f"Filter '{request_key}' ignored: stars.{column_name} not present in this build."
                )
                return
            if require_non_null:
                where_clauses.append(f"({column_name} is not null and {column_name} {op} ?)")
            else:
                where_clauses.append(f"({column_name} {op} ?)")
            where_params.append(float(value))
            applied_filters[request_key] = float(value)

        _apply_optional_threshold("max_distance_ly", "dist_ly", "<=", payload.max_distance_ly)
        _apply_optional_threshold(
            "min_parallax_over_error",
            "parallax_over_error",
            ">=",
            payload.min_parallax_over_error,
        )
        _apply_optional_threshold(
            "max_parallax_error_mas",
            "parallax_error_mas",
            "<=",
            payload.max_parallax_error_mas,
        )
        _apply_optional_threshold("max_ruwe", "ruwe", "<=", payload.max_ruwe)

        if payload.require_spectral_class:
            if "spectral_class" in star_cols:
                where_clauses.append("(spectral_class is not null and spectral_class <> '')")
                applied_filters["require_spectral_class"] = True
            else:
                warnings.append("Filter 'require_spectral_class' ignored: stars.spectral_class not present.")

        if payload.require_color_index:
            if "color_index" in star_cols:
                where_clauses.append("(color_index is not null)")
                applied_filters["require_color_index"] = True
            else:
                warnings.append("Filter 'require_color_index' ignored: stars.color_index not present.")

        if allowed_values:
            if "spectral_class" in star_cols:
                placeholders = ",".join(["?"] * len(allowed_values))
                where_clauses.append(
                    f"(coalesce(upper(spectral_class), 'UNKNOWN') in ({placeholders}))"
                )
                where_params.extend(allowed_values)
                applied_filters["allowed_spectral_classes"] = allowed_values
            else:
                warnings.append(
                    "Filter 'allowed_spectral_classes' ignored: stars.spectral_class not present."
                )

        where_sql = " and ".join(where_clauses) if where_clauses else "true"

        counts_row = con.execute(
            f"""
            with candidate_stars as (
              select star_id, system_id, spectral_class, color_index
              from stars
              where {where_sql}
            ),
            candidate_systems as (
              select distinct system_id
              from candidate_stars
              where system_id is not null
            ),
            candidate_planets as (
              select p.planet_id
              from planets p
              join candidate_systems s using (system_id)
            )
            select
              (select count(*)::bigint from stars) as stars_total,
              (select count(*)::bigint from candidate_stars) as stars_retained,
              (select count(*)::bigint from systems) as systems_total,
              (select count(*)::bigint from candidate_systems) as systems_retained,
              (select count(*)::bigint from planets) as planets_total,
              (select count(*)::bigint from candidate_planets) as planets_retained,
              (select count(*)::bigint from candidate_stars where spectral_class is null or spectral_class = '') as retained_missing_spectral,
              (select count(*)::bigint from candidate_stars where color_index is null) as retained_missing_color
            """,
            where_params,
        ).fetchone()

        spectral_rows = con.execute(
            f"""
            select
              coalesce(nullif(spectral_class, ''), 'UNKNOWN') as spectral_class,
              count(*)::bigint as star_count
            from stars
            where {where_sql}
            group by 1
            order by star_count desc, spectral_class asc
            limit 16
            """,
            where_params,
        ).fetchall()

    stars_total = int(counts_row[0] or 0)
    stars_retained = int(counts_row[1] or 0)
    systems_total = int(counts_row[2] or 0)
    systems_retained = int(counts_row[3] or 0)
    planets_total = int(counts_row[4] or 0)
    planets_retained = int(counts_row[5] or 0)

    def _pct(part: int, whole: int) -> float:
        if whole <= 0:
            return 0.0
        return (float(part) / float(whole)) * 100.0

    return {
        "status": "ok",
        "build_id": _dataset_status_payload(force_refresh=False).get("build_id"),
        "where_sql": where_sql,
        "applied_filters": applied_filters,
        "warnings": warnings,
        "counts": {
            "stars_total": stars_total,
            "stars_retained": stars_retained,
            "stars_sliced_out": max(stars_total - stars_retained, 0),
            "stars_retained_pct": _pct(stars_retained, stars_total),
            "systems_total": systems_total,
            "systems_retained": systems_retained,
            "systems_sliced_out": max(systems_total - systems_retained, 0),
            "systems_retained_pct": _pct(systems_retained, systems_total),
            "planets_total": planets_total,
            "planets_retained": planets_retained,
            "planets_sliced_out": max(planets_total - planets_retained, 0),
            "planets_retained_pct": _pct(planets_retained, planets_total),
            "retained_missing_spectral": int(counts_row[6] or 0),
            "retained_missing_color": int(counts_row[7] or 0),
        },
        "retained_spectral_breakdown": [
            {"spectral_class": str(row[0]), "star_count": int(row[1])}
            for row in spectral_rows
        ],
    }


@admin_router.get("/coolness/state")
def admin_coolness_state(request: Request):
    auth.require_admin(request)
    data = _run_score_coolness_json(["list"])
    active = data.get("active")
    active_profile = None
    profiles = data.get("profiles") or []
    if isinstance(active, dict):
        for row in profiles:
            if not isinstance(row, dict):
                continue
            if (
                str(row.get("profile_id")) == str(active.get("profile_id"))
                and str(row.get("profile_version")) == str(active.get("profile_version"))
            ):
                active_profile = row
                break
    return {
        "profile_store": data.get("profile_store"),
        "active": active,
        "active_profile": active_profile,
        "profiles": profiles,
    }


@admin_router.post("/coolness/preview")
def admin_coolness_preview(request: Request, payload: CoolnessPreviewRequest):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)

    args = ["preview"]
    profile_id = (payload.profile_id or "").strip()
    profile_version = (payload.profile_version or "").strip()
    if profile_id or profile_version:
        if not profile_id or not profile_version:
            # Tolerate partial selector input from UI and fall back to active baseline.
            profile_id = ""
            profile_version = ""
        else:
            args.extend(["--profile-id", profile_id, "--profile-version", profile_version])

    if payload.weights:
        args.extend(["--weights-json", json.dumps(payload.weights, sort_keys=True)])

    preview = _run_score_coolness_json(args)
    candidate = preview.get("candidate") if isinstance(preview, dict) else None
    if not isinstance(candidate, dict):
        raise HTTPException(
            status_code=500,
            detail={
                "code": "internal_error",
                "message": "Malformed preview payload from score_coolness",
                "details": {},
            },
        )
    candidate_weights = candidate.get("weights")
    if not isinstance(candidate_weights, dict):
        raise HTTPException(
            status_code=500,
            detail={
                "code": "internal_error",
                "message": "Preview candidate is missing weights",
                "details": {},
            },
        )

    diversity = _coolness_preview_from_disc_db(
        {k: float(v) for k, v in candidate_weights.items()},
        top_n=int(payload.top_n),
    )

    auth.audit_event(
        request,
        event_type="admin.coolness.preview",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={
            "profile_id": candidate.get("profile_id"),
            "profile_version": candidate.get("profile_version"),
            "top_n": int(payload.top_n),
        },
    )
    return {
        "preview": preview,
        "diversity": diversity,
    }


@admin_router.get("/inference/endpoints")
def admin_inference_endpoints(request: Request):
    auth.require_admin(request)
    return {"items": inference_registry.list_endpoints()}


@admin_router.get("/inference/credential-envs")
def admin_inference_credential_envs(request: Request):
    auth.require_admin(request)
    return _inference_credential_envs_payload()


@admin_router.post("/inference/endpoints")
def admin_inference_endpoint_create(request: Request, payload: InferenceEndpointRequest):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)
    try:
        endpoint = inference_registry.create_endpoint(payload.dict())
    except inference_registry.RegistryError as exc:
        auth.audit_event(
            request,
            event_type="admin.inference.endpoint.create",
            result="deny",
            actor_user_id=int(user["user_id"]),
            details={"message": str(exc), "endpoint_key": payload.endpoint_key},
        )
        raise HTTPException(
            status_code=400,
            detail={"code": "bad_request", "message": str(exc), "details": {}},
        )
    auth.audit_event(
        request,
        event_type="admin.inference.endpoint.create",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={"endpoint_id": endpoint["endpoint_id"], "endpoint_key": endpoint["endpoint_key"]},
    )
    return {"endpoint": endpoint}


@admin_router.patch("/inference/endpoints/{endpoint_id}")
def admin_inference_endpoint_update(
    request: Request,
    endpoint_id: int,
    payload: InferenceEndpointUpdateRequest,
):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)
    try:
        endpoint = inference_registry.update_endpoint(endpoint_id, payload.dict(exclude_unset=True))
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Inference endpoint not found",
                "details": {"endpoint_id": endpoint_id},
            },
        )
    except inference_registry.RegistryError as exc:
        auth.audit_event(
            request,
            event_type="admin.inference.endpoint.update",
            result="deny",
            actor_user_id=int(user["user_id"]),
            details={"message": str(exc), "endpoint_id": endpoint_id},
        )
        raise HTTPException(
            status_code=400,
            detail={"code": "bad_request", "message": str(exc), "details": {"endpoint_id": endpoint_id}},
        )
    auth.audit_event(
        request,
        event_type="admin.inference.endpoint.update",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={"endpoint_id": endpoint["endpoint_id"], "endpoint_key": endpoint["endpoint_key"]},
    )
    return {"endpoint": endpoint}


@admin_router.delete("/inference/endpoints/{endpoint_id}")
def admin_inference_endpoint_delete(request: Request, endpoint_id: int):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)
    try:
        endpoint = inference_registry.get_endpoint(endpoint_id, include_models=False)
        inference_registry.delete_endpoint(endpoint_id)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Inference endpoint not found",
                "details": {"endpoint_id": endpoint_id},
            },
        )
    auth.audit_event(
        request,
        event_type="admin.inference.endpoint.delete",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={"endpoint_id": endpoint_id, "endpoint_key": endpoint.get("endpoint_key")},
    )
    return {"status": "deleted", "endpoint_id": endpoint_id}


@admin_router.post("/inference/endpoints/{endpoint_id}/poll-models")
def admin_inference_endpoint_poll_models(request: Request, endpoint_id: int):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)
    try:
        result = inference_registry.poll_models(endpoint_id)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Inference endpoint not found",
                "details": {"endpoint_id": endpoint_id},
            },
        )
    except inference_registry.RegistryError as exc:
        auth.audit_event(
            request,
            event_type="admin.inference.endpoint.poll",
            result="error",
            actor_user_id=int(user["user_id"]),
            details={"endpoint_id": endpoint_id, "message": str(exc)},
        )
        raise HTTPException(
            status_code=502,
            detail={
                "code": "upstream_error",
                "message": str(exc),
                "details": {"endpoint_id": endpoint_id},
            },
        )
    auth.audit_event(
        request,
        event_type="admin.inference.endpoint.poll",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={
            "endpoint_id": endpoint_id,
            "model_count": len(result.get("models") or []),
            "latency_ms": (result.get("probe") or {}).get("latency_ms"),
        },
    )
    return result


@admin_router.post("/inference/endpoints/{endpoint_id}/smoke-test")
def admin_inference_endpoint_smoke_test(
    request: Request,
    endpoint_id: int,
    payload: InferenceSmokeTestRequest,
):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)
    try:
        result = inference_registry.smoke_test(endpoint_id, payload.dict())
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Inference endpoint not found",
                "details": {"endpoint_id": endpoint_id},
            },
        )
    except inference_registry.RegistryError as exc:
        auth.audit_event(
            request,
            event_type="admin.inference.endpoint.smoke_test",
            result="error",
            actor_user_id=int(user["user_id"]),
            details={
                "endpoint_id": endpoint_id,
                "role": payload.role,
                "model_id": payload.model_id,
                "message": str(exc),
            },
        )
        raise HTTPException(
            status_code=502,
            detail={
                "code": "upstream_error",
                "message": str(exc),
                "details": {"endpoint_id": endpoint_id},
            },
        )
    auth.audit_event(
        request,
        event_type="admin.inference.endpoint.smoke_test",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={
            "endpoint_id": endpoint_id,
            "role": result.get("role"),
            "model_id": result.get("model_id"),
            "latency_ms": result.get("latency_ms"),
            "total_tokens": (result.get("usage") or {}).get("total_tokens"),
        },
    )
    return result


@admin_router.get("/inference/stats")
def admin_inference_stats(request: Request):
    auth.require_admin(request)
    return inference_registry.usage_stats()


@admin_router.get("/inference/eval-reports")
def admin_inference_eval_reports(
    request: Request,
    limit: int = Query(default=24, ge=1, le=200),
):
    auth.require_admin(request)
    return _list_agent_eval_reports(_state_dir().resolve(), limit=limit)


@admin_router.get("/operations/status")
def admin_operations_status(request: Request):
    auth.require_admin(request)
    return admin_actions.operations_status()


@admin_router.get("/builds/status")
def admin_builds_status(request: Request):
    auth.require_admin(request)
    return admin_actions.builds_status()


@admin_router.get("/actions/catalog")
def admin_actions_catalog(request: Request):
    auth.require_admin(request)
    return {"items": admin_actions.list_actions()}


@admin_router.post("/actions/run")
def admin_action_run(request: Request, payload: ActionRunRequest):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)
    try:
        job = admin_actions.start_job(
            action=payload.action.strip(),
            params=payload.params,
            requested_by_user_id=int(user["user_id"]),
            user_roles=sorted(set(user.get("roles", []))),
            confirmation=payload.confirmation,
        )
    except admin_actions.ActionValidationError as exc:
        auth.audit_event(
            request,
            event_type="admin.action.run",
            result="deny",
            actor_user_id=int(user["user_id"]),
            details={"reason": "validation", "message": str(exc), "action": payload.action},
        )
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": str(exc),
                "details": {"action": payload.action},
            },
        )
    except admin_actions.ActionPermissionError as exc:
        auth.audit_event(
            request,
            event_type="admin.action.run",
            result="deny",
            actor_user_id=int(user["user_id"]),
            details={"reason": "permission", "message": str(exc), "action": payload.action},
        )
        raise HTTPException(
            status_code=403,
            detail={
                "code": "forbidden",
                "message": str(exc),
                "details": {"action": payload.action},
            },
        )
    except RuntimeError as exc:
        auth.audit_event(
            request,
            event_type="admin.action.run",
            result="deny",
            actor_user_id=int(user["user_id"]),
            details={"reason": "capacity", "message": str(exc), "action": payload.action},
        )
        raise HTTPException(
            status_code=409,
            detail={
                "code": "conflict",
                "message": str(exc),
                "details": {"action": payload.action},
            },
        )
    except Exception as exc:
        auth.audit_event(
            request,
            event_type="admin.action.run",
            result="error",
            actor_user_id=int(user["user_id"]),
            details={"reason": "unexpected", "message": str(exc), "action": payload.action},
        )
        raise HTTPException(
            status_code=500,
            detail={
                "code": "internal_error",
                "message": "Failed to start admin action",
                "details": {"action": payload.action},
            },
        )
    auth.audit_event(
        request,
        event_type="admin.action.run",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={
            "action": payload.action,
            "job_id": job["job_id"],
            "correlation_id": job["job_id"],
        },
    )
    return {"job": job}


@admin_router.get("/actions/jobs")
def admin_actions_jobs(
    request: Request,
    limit: int = Query(default=20, ge=1, le=200),
):
    auth.require_admin(request)
    return {"items": admin_actions.list_jobs(limit=limit)}


@admin_router.get("/actions/jobs/{job_id}")
def admin_actions_job(request: Request, job_id: str):
    auth.require_admin(request)
    try:
        return {"job": admin_actions.get_job(job_id)}
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Job not found",
                "details": {"job_id": job_id},
            },
        )


@admin_router.get("/actions/jobs/{job_id}/audit")
def admin_actions_job_audit(
    request: Request,
    job_id: str,
    limit: int = Query(default=50, ge=1, le=200),
):
    auth.require_admin(request)
    try:
        admin_actions.get_job(job_id)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Job not found",
                "details": {"job_id": job_id},
            },
        )
    items = _admin_audit_items(["a.details_json LIKE ?"], [f"%{job_id}%"], limit)
    return {"job_id": job_id, "items": items}


@admin_router.get("/actions/jobs/{job_id}/events")
def admin_actions_job_events(
    request: Request,
    job_id: str,
    limit: int = Query(default=100, ge=1, le=500),
):
    auth.require_admin(request)
    try:
        return {"job_id": job_id, "items": admin_actions.list_job_events(job_id, limit=limit)}
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Job not found",
                "details": {"job_id": job_id},
            },
        )


@admin_router.get("/actions/jobs/{job_id}/log")
def admin_actions_job_log(
    request: Request,
    job_id: str,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=65536, ge=1024, le=1048576),
):
    auth.require_admin(request)
    try:
        return admin_actions.read_job_log(job_id, offset=offset, limit=limit)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Job not found",
                "details": {"job_id": job_id},
            },
        )


@admin_router.get("/actions/jobs/{job_id}/log/download", response_class=PlainTextResponse)
def admin_actions_job_log_download(request: Request, job_id: str):
    auth.require_admin(request)
    try:
        payload = admin_actions.read_full_job_log(job_id)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Job not found",
                "details": {"job_id": job_id},
            },
        )
    headers = {
        "Content-Disposition": f'attachment; filename="{job_id}.log"',
        "X-Job-Status": str(payload["status"]),
    }
    return PlainTextResponse(content=str(payload["chunk"]), headers=headers)


@admin_router.get("/actions/jobs/{job_id}/log/text", response_class=PlainTextResponse)
def admin_actions_job_log_text(request: Request, job_id: str):
    auth.require_admin(request)
    try:
        payload = admin_actions.read_full_job_log(job_id)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Job not found",
                "details": {"job_id": job_id},
            },
        )
    headers = {
        "Content-Disposition": f'inline; filename="{job_id}.log"',
        "X-Job-Status": str(payload["status"]),
    }
    return PlainTextResponse(content=str(payload["chunk"]), headers=headers)


@admin_router.post("/actions/jobs/{job_id}/cancel")
def admin_actions_job_cancel(request: Request, job_id: str):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)
    try:
        job = admin_actions.cancel_job(job_id=job_id)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Job not found",
                "details": {"job_id": job_id},
            },
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "conflict",
                "message": str(exc),
                "details": {"job_id": job_id},
            },
        )
    auth.audit_event(
        request,
        event_type="admin.action.cancel",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={"job_id": job_id, "correlation_id": job_id, "action": job.get("action")},
    )
    return {"job": job}


@admin_router.get("/backups")
def admin_backups(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
):
    auth.require_admin(request)
    return admin_actions.list_backups(limit=limit)


def _admin_audit_select_sql(where_sql: str) -> str:
    return f"""
SELECT
    a.audit_id,
    a.actor_user_id,
    u.email_norm AS actor_email,
    u.display_name AS actor_display_name,
    a.event_type,
    a.result,
    a.request_id,
    a.route,
    a.method,
    a.details_json,
    a.created_at
FROM audit_log a
LEFT JOIN users u ON u.user_id = a.actor_user_id
{where_sql}
ORDER BY a.audit_id DESC
LIMIT ?
    """


def _admin_audit_roles_by_actor(con: Any, actor_ids: list[int]) -> dict[int, list[str]]:
    roles_by_actor: dict[int, list[str]] = {actor_id: [] for actor_id in actor_ids}
    if not actor_ids:
        return roles_by_actor
    placeholders = ",".join("?" for _ in actor_ids)
    role_rows = con.execute(
        f"""
SELECT ur.user_id, r.role_code
FROM user_roles ur
JOIN roles r ON r.role_id = ur.role_id
WHERE ur.user_id IN ({placeholders})
ORDER BY ur.user_id, r.role_code
        """,
        actor_ids,
    ).fetchall()
    for role_row in role_rows:
        roles_by_actor.setdefault(int(role_row["user_id"]), []).append(str(role_row["role_code"]))
    return roles_by_actor


def _serialize_admin_audit_rows(rows: list[Any], roles_by_actor: dict[int, list[str]]) -> list[dict[str, Any]]:
    items = []
    for row in rows:
        details: Any = {}
        raw_details = row["details_json"]
        if raw_details:
            try:
                details = json.loads(raw_details)
            except Exception:
                details = {"raw": raw_details}
        correlation_id = None
        if isinstance(details, dict):
            correlation_id = details.get("correlation_id") or details.get("job_id")
        actor = None
        if row["actor_user_id"] is not None:
            user_id = int(row["actor_user_id"])
            actor = {
                "user_id": user_id,
                "email": row["actor_email"],
                "display_name": row["actor_display_name"],
                "roles": roles_by_actor.get(user_id, []),
            }
        items.append(
            {
                "audit_id": row["audit_id"],
                "actor_user_id": row["actor_user_id"],
                "actor": actor,
                "event_type": row["event_type"],
                "result": row["result"],
                "request_id": row["request_id"],
                "route": row["route"],
                "method": row["method"],
                "details": details,
                "correlation_id": correlation_id,
                "created_at": row["created_at"],
            }
        )
    return items


def _admin_audit_items(where_clauses: list[str], params: list[Any], limit: int) -> list[dict[str, Any]]:
    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)
    query = _admin_audit_select_sql(where_sql)
    query_params = [*params, limit]
    with admin_db.connection_scope() as con:
        rows = con.execute(query, query_params).fetchall()
        actor_ids = sorted({int(row["actor_user_id"]) for row in rows if row["actor_user_id"] is not None})
        roles_by_actor = _admin_audit_roles_by_actor(con, actor_ids)
    return _serialize_admin_audit_rows(rows, roles_by_actor)


@admin_router.get("/audit")
def admin_audit_log(
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
    before_audit_id: Optional[int] = Query(default=None, ge=1),
    event_type: Optional[str] = Query(default=None, min_length=1, max_length=128),
    event_prefix: Optional[str] = Query(default=None, min_length=1, max_length=128),
    result: Optional[str] = Query(default=None, min_length=1, max_length=16),
    request_id: Optional[str] = Query(default=None, min_length=1, max_length=128),
    actor_user_id: Optional[int] = Query(default=None, ge=1),
    correlation_id: Optional[str] = Query(default=None, min_length=1, max_length=128),
):
    auth.require_admin(request)
    if result:
        normalized_result = result.strip().lower()
        if normalized_result not in {"success", "deny", "error"}:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "bad_request",
                    "message": "Invalid result filter",
                    "details": {"result": result},
                },
            )
        result = normalized_result
    if event_type:
        event_type = event_type.strip()
    if event_prefix:
        event_prefix = event_prefix.strip()
    if request_id:
        request_id = request_id.strip()
    if correlation_id:
        correlation_id = correlation_id.strip()

    where_clauses = []
    params: list[Any] = []
    if before_audit_id is not None:
        where_clauses.append("a.audit_id < ?")
        params.append(before_audit_id)
    if event_type:
        where_clauses.append("a.event_type = ?")
        params.append(event_type)
    if event_prefix:
        where_clauses.append("a.event_type LIKE ?")
        params.append(f"{event_prefix}%")
    if result:
        where_clauses.append("a.result = ?")
        params.append(result)
    if request_id:
        where_clauses.append("a.request_id = ?")
        params.append(request_id)
    if actor_user_id is not None:
        where_clauses.append("a.actor_user_id = ?")
        params.append(actor_user_id)
    if correlation_id:
        where_clauses.append("a.details_json LIKE ?")
        params.append(f"%{correlation_id}%")

    items = _admin_audit_items(where_clauses, params, limit)
    next_before = items[-1]["audit_id"] if items else None
    return {"items": items, "next_before_audit_id": next_before}


app.include_router(admin_router, prefix="/api/v2/admin")
app.include_router(admin_router, prefix="/api/v1/admin")


@app.get("/admin", response_class=HTMLResponse)
@app.get("/api/v2/admin/ui", response_class=HTMLResponse)
@app.get("/api/v1/admin/ui", response_class=HTMLResponse)
def admin_home(request: Request):
    if not auth.is_enabled():
        return HTMLResponse("<h1>Spacegate Admin</h1><p>Auth is disabled.</p>", status_code=503)
    request_path = str(request.url.path)
    if request_path.startswith("/api/v1/admin/") or request_path.startswith("/api/v2/admin/"):
        next_path = request_path
    else:
        next_path = auth.get_config().success_redirect
    if not getattr(request.state, "auth_user", None):
        return auth.login_redirect(request, next_path=next_path)
    user = auth.require_admin(request)
    email = user["email"]
    display_name = user["display_name"]
    csrf_cookie_name = auth.get_config().csrf_cookie_name
    body = f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Spacegate Admin</title>
    <link rel="icon" type="image/svg+xml" href="/favicon.svg" />
    <style>
      :root {{
        color-scheme: light;
        --bg: #f7f8fa;
        --text: #1f2937;
        --muted: #6b7280;
        --card: #ffffff;
        --border: #d1d5db;
        --ok: #15803d;
        --warn: #b45309;
        --err: #b91c1c;
        --brand: #0f766e;
        --panel-soft: #eef2f7;
        --code-bg: #0f172a;
        --code-ink: #e2e8f0;
        --font-body: ui-sans-serif, system-ui, sans-serif;
      }}
      :root[data-theme="simple_dark"] {{
        --bg: #0f1820;
        --text: #e4edf4;
        --muted: #9eb2c0;
        --card: #15242f;
        --border: rgba(170, 196, 214, 0.24);
        --ok: #6fd58f;
        --warn: #f0bf55;
        --err: #df736f;
        --brand: #74c5ff;
        --panel-soft: #1b2b37;
        --code-bg: #08111a;
        --code-ink: #dce8f6;
      }}
      :root[data-theme="cyberpunk"] {{
        --bg: #090013;
        --text: #ecfff7;
        --muted: #9ad9ff;
        --card: #121030;
        --border: rgba(125, 251, 255, 0.45);
        --ok: #63ff8f;
        --warn: #ffd166;
        --err: #ff6ba3;
        --brand: #ff4fd8;
        --panel-soft: #151a38;
        --code-bg: #0a0f23;
        --code-ink: #d9fff6;
        --font-body: "IBM Plex Mono", "Spline Sans Mono", ui-monospace, monospace;
      }}
      :root[data-theme="lcars"] {{
        --bg: #000000;
        --text: #ffd9ba;
        --muted: #d9b89f;
        --card: #09080c;
        --border: rgba(225, 156, 110, 0.42);
        --ok: #9cf6a8;
        --warn: #f6c94c;
        --err: #f08a96;
        --brand: #f5a22e;
        --panel-soft: #171221;
        --code-bg: #140f1b;
        --code-ink: #ffe2c9;
        --font-body: "Antonio", "Arial Narrow", "Space Grotesk", sans-serif;
      }}
      :root[data-theme="mission_control"] {{
        --bg: #0b121b;
        --text: #d9e6f4;
        --muted: #9bacbf;
        --card: #17212d;
        --border: rgba(130, 156, 183, 0.45);
        --ok: #6fd58f;
        --warn: #f0bf55;
        --err: #df736f;
        --brand: #6fb9ff;
        --panel-soft: #1c2734;
        --code-bg: #0c1320;
        --code-ink: #dce8f6;
      }}
      :root[data-theme="aurora"] {{
        --bg: #0d1530;
        --text: #edf3ff;
        --muted: #c2cbf5;
        --card: rgba(24, 33, 69, 0.72);
        --border: rgba(176, 214, 255, 0.4);
        --ok: #5dffd7;
        --warn: #ffe08a;
        --err: #ff88e7;
        --brand: #8d7dff;
        --panel-soft: rgba(27, 45, 84, 0.72);
        --code-bg: #111b3a;
        --code-ink: #e7efff;
      }}
      :root[data-theme="retro_90s"] {{
        --bg: #c3c3c3;
        --text: #101010;
        --muted: #393939;
        --card: #c0c0c0;
        --border: #7c7c7c;
        --ok: #008000;
        --warn: #b45309;
        --err: #b91c1c;
        --brand: #0000aa;
        --panel-soft: #d2d2d2;
        --code-bg: #1b1b1b;
        --code-ink: #ffff99;
        --font-body: "Tahoma", "Verdana", "Arial", sans-serif;
      }}
      :root[data-theme="deep_space_minimal"] {{
        --bg: #010109;
        --text: #e9eeff;
        --muted: #9da8c4;
        --card: rgba(6, 8, 20, 0.8);
        --border: rgba(155, 181, 239, 0.2);
        --ok: #8ad58a;
        --warn: #d8b96a;
        --err: #d77988;
        --brand: #8ab8ff;
        --panel-soft: rgba(10, 14, 30, 0.9);
        --code-bg: rgba(7, 10, 23, 0.96);
        --code-ink: #d9e4ff;
      }}
      body {{ font-family: var(--font-body); margin: 1.25rem; line-height: 1.4; background: var(--bg); color: var(--text); }}
      h1, h2, h3 {{ margin: 0.5rem 0; }}
      .toolbar {{ display: flex; gap: 0.5rem; flex-wrap: wrap; margin-bottom: 1rem; }}
      .screen-nav {{ display: flex; gap: 0.45rem; flex-wrap: wrap; margin-bottom: 0.9rem; }}
      .screen-nav button.active {{ border-color: var(--brand); color: var(--brand); }}
      .screen {{ display: none; }}
      .screen.active {{ display: block; }}
      .section {{ background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 0.75rem; margin-bottom: 0.9rem; }}
      .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 0.75rem; }}
      .coolness-layout {{ grid-template-columns: minmax(300px, 35fr) minmax(420px, 65fr); }}
      .inference-layout {{ grid-template-columns: minmax(280px, 1fr) minmax(520px, 2fr); align-items: start; }}
      .action-card {{ border: 1px solid var(--border); border-radius: 8px; padding: 0.65rem; }}
      .action-meta {{ color: var(--muted); font-size: 0.9rem; margin-bottom: 0.5rem; }}
      .field {{ margin-bottom: 0.45rem; }}
      .field label {{ display: block; font-size: 0.86rem; color: var(--muted); margin-bottom: 0.15rem; }}
      .field input[type=text], .field input[type=url], .field input[type=password], .field input[type=number], .field select, .field textarea {{ width: 100%; box-sizing: border-box; padding: 0.35rem; border: 1px solid var(--border); border-radius: 6px; background: var(--card); color: var(--text); }}
      .small {{ font-size: 0.82rem; color: var(--muted); }}
      code {{ background: var(--panel-soft); padding: 0.1rem 0.25rem; border-radius: 4px; }}
      button {{ padding: 0.45rem 0.65rem; cursor: pointer; border: 1px solid var(--border); background: var(--card); color: var(--text); border-radius: 6px; }}
      select {{ border: 1px solid var(--border); background: var(--card); color: var(--text); border-radius: 6px; padding: 0.35rem; }}
      button.primary {{ background: var(--brand); color: white; border-color: var(--brand); }}
      button.warn {{ border-color: var(--warn); color: var(--warn); }}
      button.danger {{ border-color: var(--err); color: var(--err); }}
      button.preset-btn.active {{ background: #ecfeff; border-color: #0891b2; color: #0e7490; font-weight: 700; }}
      .status-badge {{ display: inline-block; border-radius: 999px; padding: 0.05rem 0.45rem; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.02em; border: 1px solid; }}
      .status-running {{ color: #1d4ed8; border-color: #93c5fd; }}
      .status-queued {{ color: #4338ca; border-color: #c4b5fd; }}
      .status-succeeded {{ color: var(--ok); border-color: #86efac; }}
      .status-failed {{ color: var(--err); border-color: #fca5a5; }}
      .status-cancelled {{ color: #6b7280; border-color: #d1d5db; }}
      .inline {{ display: inline-flex; align-items: center; gap: 0.35rem; flex-wrap: wrap; }}
      .jobs-list, .audit-list, .backup-list {{ list-style: none; margin: 0; padding: 0; }}
      .jobs-list li, .audit-list li, .backup-list li {{ border-bottom: 1px solid var(--panel-soft); padding: 0.35rem 0; }}
      .jobs-list li:last-child, .audit-list li:last-child, .backup-list li:last-child {{ border-bottom: 0; }}
      pre {{ background: var(--code-bg); color: var(--code-ink); padding: 0.6rem; border-radius: 8px; overflow: auto; max-height: 28rem; }}
      .audit-presets button.active {{ border-color: var(--brand); color: var(--brand); }}
      .muted {{ color: var(--muted); }}
      .weight-grid {{ display: grid; gap: 0.45rem; }}
      .weight-row {{ display: grid; grid-template-columns: 140px 1fr 76px; gap: 0.5rem; align-items: center; }}
      .weight-row input[type=range] {{ width: 100%; }}
      .weight-row input[type=number] {{ width: 100%; }}
      .json-box {{ background: #0f172a; color: #e2e8f0; padding: 0.6rem; border-radius: 8px; overflow: auto; max-height: 16rem; }}
      .guidance {{ margin: 0.25rem 0 0.5rem 1.1rem; color: var(--muted); }}
      .guidance li {{ margin: 0.15rem 0; }}
      .note-box {{ border: 1px dashed var(--border); border-radius: 8px; padding: 0.5rem; background: var(--panel-soft); }}
      .preview-grid {{ display: grid; gap: 0.6rem; }}
      .kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(165px, 1fr)); gap: 0.5rem; }}
      .kpi {{ border: 1px solid var(--border); border-radius: 8px; padding: 0.45rem; background: var(--card); }}
      .kpi .k {{ color: var(--muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.02em; }}
      .kpi .v {{ font-size: 1rem; font-weight: 700; }}
      .changes-list {{ list-style: none; margin: 0; padding: 0; }}
      .changes-list li {{ border-bottom: 1px solid #eef2f7; padding: 0.28rem 0; }}
      .changes-list li:last-child {{ border-bottom: 0; }}
      .bar-list {{ display: grid; gap: 0.35rem; }}
      .bar-row {{ display: grid; grid-template-columns: 100px 1fr 90px; gap: 0.45rem; align-items: center; }}
      .bar-track {{ background: var(--panel-soft); border-radius: 999px; height: 10px; overflow: hidden; }}
      .bar-fill {{ background: var(--brand); height: 100%; border-radius: 999px; }}
      .bar-fill.warn {{ background: var(--warn); }}
      .bar-fill.err {{ background: var(--err); }}
      .chart-split {{ display: grid; gap: 0.6rem; grid-template-columns: minmax(220px, 0.95fr) minmax(220px, 1.05fr); align-items: start; }}
      .pie-panel {{ border: 1px solid var(--panel-soft); border-radius: 8px; padding: 0.45rem; background: color-mix(in srgb, var(--panel-soft) 68%, transparent); }}
      .pie-title {{ margin: 0 0 0.35rem 0; font-size: 0.84rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.02em; }}
      .pie-wrap {{ display: grid; gap: 0.5rem; align-items: center; grid-template-columns: 116px 1fr; }}
      .pie-plot {{ width: 108px; height: 108px; border-radius: 50%; position: relative; border: 1px solid var(--border); box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--bg) 42%, transparent); }}
      .pie-plot::after {{ content: ""; position: absolute; inset: 25%; border-radius: 50%; background: var(--card); border: 1px solid var(--panel-soft); }}
      .pie-legend {{ list-style: none; margin: 0; padding: 0; display: grid; gap: 0.22rem; }}
      .pie-legend li {{ display: grid; grid-template-columns: 10px 1fr auto; gap: 0.38rem; align-items: center; font-size: 0.78rem; }}
      .pie-dot {{ width: 10px; height: 10px; border-radius: 999px; border: 1px solid color-mix(in srgb, var(--bg) 45%, transparent); }}
      .metric-list {{ display: grid; gap: 0.35rem; }}
      .metric-row {{ display: grid; grid-template-columns: minmax(130px, 1fr) minmax(180px, 1.1fr); gap: 0.5rem; align-items: baseline; border-bottom: 1px solid var(--panel-soft); padding-bottom: 0.28rem; }}
      .metric-row:last-child {{ border-bottom: 0; padding-bottom: 0; }}
      .metric-k {{ color: var(--muted); font-size: 0.82rem; }}
      .metric-v {{ font-weight: 600; font-size: 0.88rem; word-break: break-word; }}
      .metric-note {{ color: var(--muted); font-size: 0.78rem; grid-column: 1 / -1; }}
      .mini-table {{ width: 100%; border-collapse: collapse; font-size: 0.86rem; }}
      .mini-table th, .mini-table td {{ border-bottom: 1px solid var(--panel-soft); padding: 0.28rem 0.2rem; text-align: left; }}
      .mini-table th {{ color: var(--muted); font-weight: 600; }}
      @media (max-width: 1100px) {{
        .coolness-layout {{ grid-template-columns: 1fr; }}
        .inference-layout {{ grid-template-columns: 1fr; }}
      }}
      @media (max-width: 880px) {{
        .chart-split {{ grid-template-columns: 1fr; }}
      }}
    </style>
  </head>
  <body>
    <h1>Spacegate Admin</h1>
    <p>Signed in as <strong>{display_name}</strong> (<code>{email}</code>).</p>
    <p class="muted">Actions are allowlisted. Destructive actions require explicit confirmation phrases.</p>

    <div class="toolbar">
      <button id="logout" class="danger">Log out</button>
      <label for="adminThemeSelect" class="small">Theme</label>
      <select id="adminThemeSelect" style="min-width: 12rem;">
        <option value="simple_light">Simple Light</option>
        <option value="simple_dark">Simple Dark</option>
        <option value="cyberpunk">Cyberpunk</option>
        <option value="lcars">Enterprise</option>
        <option value="mission_control">Mission Control</option>
        <option value="aurora">Aurora</option>
        <option value="retro_90s">Geocities</option>
        <option value="deep_space_minimal">Deep Space Minimal</option>
      </select>
      <button id="refreshStatus">Refresh Status</button>
      <button id="refreshDatasetStatus">Refresh Dataset Stats</button>
      <button id="refreshJobs">Refresh Jobs</button>
      <button id="refreshBackups">Refresh Backups</button>
      <button id="refreshAudit">Refresh Audit</button>
    </div>

    <div class="screen-nav">
      <button id="screenTabOperations">Operations</button>
      <button id="screenTabStatus" class="active">Status</button>
      <button id="screenTabDataset">Dataset</button>
      <button id="screenTabInference">Inference</button>
      <button id="screenTabCoolness">Coolness</button>
      <button id="screenTabActivity">Activity</button>
    </div>

    <div id="screenOperations" class="screen">
      <div class="section">
        <h2>Operations</h2>
        <div id="actionsOps" class="grid"></div>
      </div>
      <div class="section">
        <h2>Status</h2>
        <pre id="out"></pre>
      </div>
    </div>

    <div id="screenStatus" class="screen active">
      <div class="section">
        <h2>Status</h2>
        <p class="muted">Operational health metrics for current served build: runtime memory/IO hints, storage footprint, and capacity indicators.</p>
        <div class="inline">
          <button id="refreshDatasetStatusInline">Refresh Dataset Stats</button>
          <span id="datasetStatusMeta" class="small"></span>
        </div>
      </div>
      <div class="section">
        <h3>Top KPIs</h3>
        <div id="datasetKpis" class="kpis"></div>
      </div>
      <div class="section grid">
        <div>
          <h3>Storage Footprint</h3>
          <div id="datasetStorage" class="bar-list"></div>
        </div>
        <div>
          <h3>Runtime + Bottleneck Hints</h3>
          <div id="datasetRuntime" class="bar-list"></div>
        </div>
      </div>
      <div class="section">
        <h3>Capacity Usage</h3>
        <div id="datasetUsageBars" class="bar-list"></div>
      </div>
      <div class="section grid">
        <div>
          <h3>Deterministic Compare</h3>
          <div id="datasetDeterminism" class="bar-list"></div>
        </div>
        <div>
          <h3>Determinism Table Checks</h3>
          <table class="mini-table">
            <thead><tr><th>Table</th><th>Status</th></tr></thead>
            <tbody id="datasetDeterminismRows"></tbody>
          </table>
        </div>
      </div>
      <div class="section">
        <h3>Detailed Payload</h3>
        <details>
          <summary>Show humanized runtime/storage summary</summary>
          <pre id="datasetHumanSummary"></pre>
        </details>
        <details>
          <summary>Show raw dataset status JSON</summary>
          <pre id="datasetStatusRaw"></pre>
        </details>
      </div>
    </div>

    <div id="screenDataset" class="screen">
      <div class="section">
        <h2>Dataset</h2>
        <p class="muted">Catalog composition, multiplicity evidence, spectral distribution, and slice policy controls.</p>
      </div>
      <div class="section grid">
        <div>
          <h3>Stars by Source</h3>
          <div class="chart-split">
            <div id="datasetSourcePie" class="pie-panel"></div>
            <table class="mini-table">
              <thead><tr><th>Source</th><th>Stars</th></tr></thead>
              <tbody id="datasetSourceRows"></tbody>
            </table>
          </div>
        </div>
        <div>
          <h3>Stars by Spectral Class</h3>
          <div class="chart-split">
            <div id="datasetSpectralPie" class="pie-panel"></div>
            <table class="mini-table">
              <thead><tr><th>Class</th><th>Stars</th><th>%</th></tr></thead>
              <tbody id="datasetSpectralRows"></tbody>
            </table>
          </div>
        </div>
      </div>
      <div class="section grid">
        <div>
          <h3>Multiplicity Evidence (Systems)</h3>
          <div class="chart-split">
            <div id="datasetSystemMultPie" class="pie-panel"></div>
            <table class="mini-table">
              <thead><tr><th>Bucket</th><th>Systems</th></tr></thead>
              <tbody id="datasetSystemMultRows"></tbody>
            </table>
          </div>
        </div>
        <div>
          <h3>Multiplicity Evidence (Stars)</h3>
          <div class="chart-split">
            <div id="datasetStarMultPie" class="pie-panel"></div>
            <table class="mini-table">
              <thead><tr><th>Bucket</th><th>Stars</th></tr></thead>
              <tbody id="datasetStarMultRows"></tbody>
            </table>
          </div>
        </div>
      </div>
      <div class="section grid">
        <div>
          <h3>Spectral Class Standard</h3>
          <table class="mini-table">
            <thead><tr><th>Class</th><th>Stars</th></tr></thead>
            <tbody id="datasetSpectralStandardRows"></tbody>
          </table>
        </div>
        <div>
          <h3>Compact Objects (Inferred)</h3>
          <table class="mini-table">
            <thead><tr><th>Type</th><th>Stars</th></tr></thead>
            <tbody id="datasetCompactRows"></tbody>
          </table>
        </div>
      </div>
      <div class="section grid">
        <div>
          <h3>Catalog Utility (Ingest)</h3>
          <div id="datasetCatalogContributionBars" class="bar-list"></div>
          <table class="mini-table">
            <thead>
              <tr>
                <th>Catalog</th>
                <th>Domain</th>
                <th>Input</th>
                <th>Direct</th>
                <th>Evidence</th>
                <th>Linked</th>
                <th>Tier</th>
              </tr>
            </thead>
            <tbody id="datasetCatalogContributionRows"></tbody>
          </table>
        </div>
        <div>
          <h3>Catalog Overlap + Pipeline Stages</h3>
          <table class="mini-table">
            <thead><tr><th>Scope</th><th>Pair</th><th>Intersection</th><th>Jaccard</th><th>% Scope</th></tr></thead>
            <tbody id="datasetCatalogOverlapRows"></tbody>
          </table>
          <h4 style="margin-top:0.7rem;">Pipeline Stage Status</h4>
          <table class="mini-table">
            <thead><tr><th>Stage</th><th>Updated</th><th>Details</th></tr></thead>
            <tbody id="datasetPipelineRows"></tbody>
          </table>
        </div>
      </div>
      <div class="section">
        <h3>Slice Policy Controls</h3>
        <p class="muted">Preview and launch a sliced rebuild. This trims the served dataset by policy and records the policy in build metadata.</p>
        <div class="grid">
          <div class="action-card">
            <div class="field">
              <label for="sliceMaxDistanceLy">Max distance (ly)</label>
              <input id="sliceMaxDistanceLy" type="number" min="1" max="1000" step="1" value="1000" />
            </div>
            <div class="field">
              <label for="sliceMinParallaxOverError">Min parallax_over_error (optional)</label>
              <input id="sliceMinParallaxOverError" type="number" min="0" step="0.1" placeholder="e.g. 5" />
            </div>
            <div class="field">
              <label for="sliceMaxParallaxErrorMas">Max parallax error mas (optional)</label>
              <input id="sliceMaxParallaxErrorMas" type="number" min="0" step="0.01" placeholder="e.g. 0.2" />
            </div>
            <div class="field">
              <label for="sliceMaxRuwe">Max RUWE (optional)</label>
              <input id="sliceMaxRuwe" type="number" min="0" step="0.01" placeholder="e.g. 1.4" />
            </div>
            <div class="field">
              <label>Allowed spectral classes (optional)</label>
              <div id="sliceSpectralFilters" class="inline">
                <label><input type="checkbox" value="O"> O</label>
                <label><input type="checkbox" value="B"> B</label>
                <label><input type="checkbox" value="A"> A</label>
                <label><input type="checkbox" value="F"> F</label>
                <label><input type="checkbox" value="G"> G</label>
                <label><input type="checkbox" value="K"> K</label>
                <label><input type="checkbox" value="M"> M</label>
                <label><input type="checkbox" value="L"> L</label>
                <label><input type="checkbox" value="T"> T</label>
                <label><input type="checkbox" value="Y"> Y</label>
                <label><input type="checkbox" value="D"> D</label>
                <label><input type="checkbox" value="UNKNOWN"> UNKNOWN</label>
              </div>
            </div>
            <div class="inline" style="margin-bottom:0.45rem;">
              <label><input id="sliceRequireSpectral" type="checkbox" /> Require spectral class</label>
              <label><input id="sliceRequireColor" type="checkbox" /> Require color index</label>
            </div>
            <div class="inline" style="margin-bottom:0.45rem;">
              <label><input id="sliceFromCooked" type="checkbox" checked /> Rebuild from cooked catalogs</label>
              <label><input id="sliceOverwrite" type="checkbox" /> Overwrite downloads (full pipeline only)</label>
            </div>
            <div class="inline">
              <button id="slicePreviewBtn" type="button">Preview Slice</button>
              <button id="sliceRunBtn" type="button" class="warn">Build Sliced Core</button>
            </div>
            <div id="sliceRunStatus" class="note-box small" style="margin-top:0.45rem;">Status: idle</div>
          </div>
          <div class="action-card">
            <h4>Slice Preview Impact</h4>
            <div id="slicePreviewKpis" class="kpis"></div>
            <div>
              <h4>Retained Spectral Mix</h4>
              <table class="mini-table">
                <thead><tr><th>Class</th><th>Stars</th></tr></thead>
                <tbody id="slicePreviewSpectralRows"></tbody>
              </table>
            </div>
            <details style="margin-top:0.45rem;">
              <summary>Preview payload</summary>
              <pre id="slicePreviewRaw" class="json-box"></pre>
            </details>
          </div>
        </div>
      </div>
    </div>

    <div id="screenInference" class="screen">
      <div class="section">
        <h2>Inference Endpoints</h2>
        <p class="muted">Dynamic registry for local, LAN, and frontier model endpoints. Stored secrets are never displayed after save.</p>
        <div class="inline">
          <button id="refreshInference">Refresh Registry</button>
        </div>
      </div>
      <div class="section grid inference-layout">
        <div class="action-card">
          <h3>Add Endpoint</h3>
          <div class="field">
            <label for="infDisplayName">Display name</label>
            <input id="infDisplayName" type="text" placeholder="Photon vLLM" />
          </div>
          <div class="field">
            <label for="infEndpointKey">Endpoint key</label>
            <input id="infEndpointKey" type="text" placeholder="photon-vllm" />
          </div>
          <div class="field">
            <label for="infProvider">Provider</label>
            <select id="infProvider">
              <option value="openai_compatible">OpenAI-compatible</option>
              <option value="openai">OpenAI</option>
              <option value="google">Google Gemini</option>
              <option value="custom">Custom</option>
            </select>
          </div>
          <div class="field">
            <label for="infBaseUrl">Base URL</label>
            <input id="infBaseUrl" type="url" placeholder="http://127.0.0.1:8001/v1" />
          </div>
          <div class="field">
            <label for="infAuthMode">Auth mode</label>
            <select id="infAuthMode">
              <option value="none">None</option>
              <option value="env">Environment variable</option>
              <option value="stored">Stored encrypted key</option>
            </select>
          </div>
          <div class="field">
            <label for="infApiKeyEnv">API key env var</label>
            <input id="infApiKeyEnv" type="text" placeholder="SPACEGATE_OPENAI_API_KEY" />
          </div>
          <div class="field">
            <label for="infApiKey">API key</label>
            <input id="infApiKey" type="password" autocomplete="new-password" placeholder="Only saved if provided" />
          </div>
          <div class="field">
            <label for="infDefaultModel">Default model</label>
            <input id="infDefaultModel" type="text" placeholder="optional" />
          </div>
          <div class="field">
            <label for="infTimeout">Timeout seconds</label>
            <input id="infTimeout" type="number" min="1" max="600" step="1" value="30" />
          </div>
          <div class="field">
            <label for="infNotes">Notes</label>
            <textarea id="infNotes" rows="3" placeholder="role, hardware, intended jobs"></textarea>
          </div>
          <div class="inline" style="margin-bottom:0.45rem;">
            <label><input id="infEnabled" type="checkbox" checked /> Enabled</label>
          </div>
          <button id="infCreateBtn" type="button" class="primary">Add Endpoint</button>
          <div id="infFormStatus" class="note-box small" style="margin-top:0.45rem;">Status: ready</div>
        </div>
        <div>
          <h3>Registered Endpoints</h3>
          <div id="inferenceEndpoints" class="grid"></div>
        </div>
      </div>
      <div class="section">
        <h3>Usage Stats</h3>
        <table class="mini-table">
          <thead><tr><th>Endpoint</th><th>Model</th><th>Requests</th><th>Total tokens</th><th>Avg latency</th><th>Last used</th></tr></thead>
          <tbody id="inferenceStatsRows"></tbody>
        </table>
      </div>
    </div>

    <div id="screenCoolness" class="screen">
      <div class="section">
        <h2>Coolness Tuning</h2>
        <p class="muted">Sandbox mode: you can turn knobs and run without entering any IDs first.</p>
        <p class="muted">
          Why this exists: a raw catalog naturally over-represents common objects, so discovery can feel repetitive.
          Coolness tuning intentionally balances the ranking to surface a wider range of stellar and planetary systems
          while keeping scientific source data unchanged.
        </p>
        <p class="muted">
          In plain terms: this lets you steer what visitors discover first, so the map stays educational, surprising,
          and varied instead of collapsing into one kind of object.
        </p>
        <ul class="guidance">
          <li><strong>Preview</strong>: auto-refreshed summary from the latest run output; no manual preview action needed.</li>
          <li><strong>Run</strong>: writes disc ranking outputs (`disc.duckdb`, Parquet, report) for the current build using your current sliders, but does not persist a new profile version.</li>
          <li><strong>Save Profile</strong>: stores the current slider mix as an immutable profile version, without activating it.</li>
          <li><strong>Activate Profile</strong>: saves current weights as an immutable version (auto-bumping version if needed) and then activates that saved version.</li>
          <li>Core astronomy data is not modified by coolness tuning.</li>
          <li>Profiles are immutable by version: changed weights require a new profile version.</li>
        </ul>
        <details>
          <summary>Advanced script actions</summary>
          <div id="actionsCoolness" class="grid" style="margin-top:0.6rem;"></div>
        </details>
      </div>
      <div class="section">
        <h2>Tuning Preview</h2>
        <p class="muted">Adjust weights with sliders, run read-only preview, use Run for ephemeral scoring, Save Profile when you want to persist, and Activate Profile to save current edits then switch that version live.</p>
        <div class="grid coolness-layout">
          <div class="action-card">
            <h3>Weight Controls</h3>
            <div class="field">
              <label for="coolProfileId">Profile ID</label>
              <input id="coolProfileId" type="text" placeholder="default" />
            </div>
            <div class="field">
              <label for="coolProfileVersion">Profile version (optional for Save)</label>
              <input id="coolProfileVersion" type="text" placeholder="auto" />
            </div>
            <div class="field">
              <label for="coolSavedProfiles">Saved profiles</label>
              <div class="inline">
                <select id="coolSavedProfiles" style="min-width: 260px;"></select>
                <button id="coolLoadProfileBtn" type="button">Load Saved</button>
              </div>
            </div>
            <div class="field">
              <label for="coolTopN">Preview sample size (Top N)</label>
              <input id="coolTopN" type="number" min="20" max="1000" step="10" value="200" />
            </div>
            <div class="inline" style="margin-bottom: 0.45rem;">
              <button id="presetBalanced" type="button" class="preset-btn">Balanced</button>
              <button id="presetExotic" type="button" class="preset-btn">Exotic</button>
              <button id="presetHabitable" type="button" class="preset-btn">Habitable</button>
              <button id="presetNearby" type="button" class="preset-btn">Nearby</button>
              <button id="coolResetActive" type="button" class="preset-btn">Reset To Active</button>
            </div>
            <div id="coolnessSliders" class="weight-grid"></div>
            <div class="field">
              <details>
                <summary>Current weights JSON (advanced)</summary>
                <pre id="coolWeightsJson" class="json-box"></pre>
              </details>
            </div>
            <div class="inline">
              <button id="coolApplyBtn" type="button" class="primary">Run</button>
              <button id="coolSaveBtn" type="button" class="warn">Save Profile</button>
              <button id="coolActivateBtn" type="button">Activate Profile</button>
            </div>
            <div id="coolRunStatus" class="note-box small" style="margin-top:0.45rem;">Status: idle</div>
            <div id="coolPreviewNotice" class="note-box small" style="margin-top:0.45rem;"></div>
            <div class="field" style="margin-top:1rem; padding-top:0.8rem; border-top:1px solid rgba(255,255,255,0.08);">
              <h4 style="margin-bottom:0.35rem;">Snapshot (Re)generator</h4>
              <p class="muted">
                Generate deterministic system visuals for the current build. Filters are applied before the top-rank limit.
                Defaults to the top 100 coolness-ranked systems.
              </p>
              <div class="field">
                <label for="snapshotTopCoolness">Top coolness systems</label>
                <div class="inline">
                  <input id="snapshotTopCoolness" type="range" min="10" max="1000" step="10" value="100" style="flex:1;" />
                  <input id="snapshotTopCoolnessNumber" type="number" min="1" max="10000" step="10" value="100" style="width:6rem;" />
                </div>
              </div>
              <div class="field">
                <label for="snapshotMaxDistanceLy">Max distance (ly)</label>
                <div class="inline">
                  <input id="snapshotMaxDistanceLy" type="range" min="1" max="1000" step="1" value="1000" style="flex:1;" />
                  <input id="snapshotMaxDistanceLyNumber" type="number" min="1" max="1000" step="1" value="1000" style="width:6rem;" />
                </div>
              </div>
              <div class="field-grid">
                <div class="field">
                  <label for="snapshotMinStarCount">Min stars</label>
                  <div class="inline">
                    <input id="snapshotMinStarCount" type="range" min="0" max="12" step="1" value="0" style="flex:1;" />
                    <input id="snapshotMinStarCountNumber" type="number" min="0" max="12" step="1" value="0" style="width:5rem;" />
                  </div>
                </div>
                <div class="field">
                  <label for="snapshotMinPlanetCount">Min planets</label>
                  <div class="inline">
                    <input id="snapshotMinPlanetCount" type="range" min="0" max="20" step="1" value="0" style="flex:1;" />
                    <input id="snapshotMinPlanetCountNumber" type="number" min="0" max="20" step="1" value="0" style="width:5rem;" />
                  </div>
                </div>
              </div>
              <div class="field">
                <label for="snapshotMinCoolnessScore">Min coolness score</label>
                <div class="inline">
                  <input id="snapshotMinCoolnessScore" type="range" min="0" max="40" step="0.5" value="0" style="flex:1;" />
                  <input id="snapshotMinCoolnessScoreNumber" type="number" min="0" max="40" step="0.5" value="0" style="width:6rem;" />
                </div>
              </div>
              <div class="inline">
                <label for="snapshotForceRegenerate">Force regenerate existing images</label>
                <input id="snapshotForceRegenerate" type="checkbox" />
              </div>
              <div class="inline" style="margin-top:0.45rem;">
                <button id="snapshotRunBtn" type="button" class="primary">Generate Snapshots</button>
              </div>
              <div id="snapshotRunStatus" class="note-box small" style="margin-top:0.45rem;">Status: idle</div>
            </div>
          </div>
          <div class="action-card">
            <h3>Preview Summary</h3>
            <div class="preview-grid">
              <div id="coolPreviewSummary" class="kpis"></div>
              <div>
                <h4>Weight Changes vs Active</h4>
                <ul id="coolPreviewChanges" class="changes-list"></ul>
              </div>
              <div>
                <h4>Type Distribution</h4>
                <div class="chart-split">
                  <div id="coolPreviewTypePie" class="pie-panel"></div>
                  <div id="coolPreviewTypeDist" class="bar-list"></div>
                </div>
              </div>
              <div>
                <h4>Spectral Distribution</h4>
                <div class="chart-split">
                  <div id="coolPreviewSpectralPie" class="pie-panel"></div>
                  <div id="coolPreviewSpectralDist" class="bar-list"></div>
                </div>
              </div>
              <div>
                <h4>Top Systems (Preview)</h4>
                <table class="mini-table">
                  <thead>
                    <tr><th>Rank</th><th>System</th><th>Score</th><th>Dist (ly)</th><th>Stars</th><th>Planets</th><th>Type</th><th>Subscores</th></tr>
                  </thead>
                  <tbody id="coolPreviewTopSystems"></tbody>
                </table>
              </div>
              <details>
                <summary>Raw preview JSON</summary>
                <pre id="coolPreviewOut" class="json-box"></pre>
              </details>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div id="screenActivity" class="screen">
      <div class="section grid">
        <div>
          <h2>Jobs</h2>
          <ul id="jobs" class="jobs-list"></ul>
        </div>
        <div>
          <h2>Backups</h2>
          <h3>Admin DB Snapshots</h3>
          <ul id="backupsAdminDb" class="backup-list"></ul>
          <h3>Release Metadata Snapshots</h3>
          <ul id="backupsReleaseMeta" class="backup-list"></ul>
        </div>
      </div>

      <div class="section">
        <h2>Selected Job Log</h2>
        <div id="selectedJob" class="small"></div>
        <pre id="log"></pre>
      </div>

      <div class="section">
        <h2>Audit Log</h2>
        <div class="inline audit-presets">
          <button id="auditPresetAll" class="active">All</button>
          <button id="auditPresetAuth">Auth</button>
          <button id="auditPresetActions">Admin Actions</button>
          <button id="auditPresetInference">Inference</button>
          <button id="auditPresetQueries">Queries</button>
        </div>
        <div class="inline" style="margin-top: 0.45rem;">
          <label for="auditEventType">event_type</label>
          <input id="auditEventType" placeholder="auth.login.denied" />
          <label for="auditResult">result</label>
          <select id="auditResult">
            <option value="">(any)</option>
            <option value="success">success</option>
            <option value="deny">deny</option>
            <option value="error">error</option>
          </select>
          <label for="auditRequestId">request_id</label>
          <input id="auditRequestId" placeholder="req_..." />
          <button id="loadOlderAudit">Load Older</button>
        </div>
        <ul id="audit" class="audit-list"></ul>
        <h3>Selected Audit Details</h3>
        <pre id="auditDetails"></pre>
      </div>
    </div>

    <script>
      const out = document.getElementById('out');
      const API_VERSION_PREFIX = window.location.pathname.startsWith('/api/v2/') ? '/api/v2' : '/api/v1';
      const ADMIN_API_BASE = `${{API_VERSION_PREFIX}}/admin`;
      const AUTH_API_BASE = `${{API_VERSION_PREFIX}}/auth`;
      const actionsOpsEl = document.getElementById('actionsOps');
      const actionsCoolnessEl = document.getElementById('actionsCoolness');
      const adminThemeSelectEl = document.getElementById('adminThemeSelect');
      const datasetStatusMetaEl = document.getElementById('datasetStatusMeta');
      const datasetKpisEl = document.getElementById('datasetKpis');
      const datasetStorageEl = document.getElementById('datasetStorage');
      const datasetRuntimeEl = document.getElementById('datasetRuntime');
      const datasetUsageBarsEl = document.getElementById('datasetUsageBars');
      const datasetDeterminismEl = document.getElementById('datasetDeterminism');
      const datasetDeterminismRowsEl = document.getElementById('datasetDeterminismRows');
      const datasetSourceRowsEl = document.getElementById('datasetSourceRows');
      const datasetSourcePieEl = document.getElementById('datasetSourcePie');
      const datasetSpectralRowsEl = document.getElementById('datasetSpectralRows');
      const datasetSpectralPieEl = document.getElementById('datasetSpectralPie');
      const datasetSpectralStandardRowsEl = document.getElementById('datasetSpectralStandardRows');
      const datasetCompactRowsEl = document.getElementById('datasetCompactRows');
      const datasetSystemMultRowsEl = document.getElementById('datasetSystemMultRows');
      const datasetSystemMultPieEl = document.getElementById('datasetSystemMultPie');
      const datasetStarMultRowsEl = document.getElementById('datasetStarMultRows');
      const datasetStarMultPieEl = document.getElementById('datasetStarMultPie');
      const datasetCatalogContributionBarsEl = document.getElementById('datasetCatalogContributionBars');
      const datasetCatalogContributionRowsEl = document.getElementById('datasetCatalogContributionRows');
      const datasetCatalogOverlapRowsEl = document.getElementById('datasetCatalogOverlapRows');
      const datasetPipelineRowsEl = document.getElementById('datasetPipelineRows');
      const datasetHumanSummaryEl = document.getElementById('datasetHumanSummary');
      const datasetStatusRawEl = document.getElementById('datasetStatusRaw');
      const sliceMaxDistanceLyEl = document.getElementById('sliceMaxDistanceLy');
      const sliceMinParallaxOverErrorEl = document.getElementById('sliceMinParallaxOverError');
      const sliceMaxParallaxErrorMasEl = document.getElementById('sliceMaxParallaxErrorMas');
      const sliceMaxRuweEl = document.getElementById('sliceMaxRuwe');
      const sliceSpectralFiltersEl = document.getElementById('sliceSpectralFilters');
      const sliceRequireSpectralEl = document.getElementById('sliceRequireSpectral');
      const sliceRequireColorEl = document.getElementById('sliceRequireColor');
      const sliceFromCookedEl = document.getElementById('sliceFromCooked');
      const sliceOverwriteEl = document.getElementById('sliceOverwrite');
      const slicePreviewBtnEl = document.getElementById('slicePreviewBtn');
      const sliceRunBtnEl = document.getElementById('sliceRunBtn');
      const sliceRunStatusEl = document.getElementById('sliceRunStatus');
      const slicePreviewKpisEl = document.getElementById('slicePreviewKpis');
      const slicePreviewSpectralRowsEl = document.getElementById('slicePreviewSpectralRows');
      const slicePreviewRawEl = document.getElementById('slicePreviewRaw');
      const inferenceEndpointsEl = document.getElementById('inferenceEndpoints');
      const inferenceStatsRowsEl = document.getElementById('inferenceStatsRows');
      const infDisplayNameEl = document.getElementById('infDisplayName');
      const infEndpointKeyEl = document.getElementById('infEndpointKey');
      const infProviderEl = document.getElementById('infProvider');
      const infBaseUrlEl = document.getElementById('infBaseUrl');
      const infAuthModeEl = document.getElementById('infAuthMode');
      const infApiKeyEnvEl = document.getElementById('infApiKeyEnv');
      const infApiKeyEl = document.getElementById('infApiKey');
      const infDefaultModelEl = document.getElementById('infDefaultModel');
      const infTimeoutEl = document.getElementById('infTimeout');
      const infNotesEl = document.getElementById('infNotes');
      const infEnabledEl = document.getElementById('infEnabled');
      const infCreateBtnEl = document.getElementById('infCreateBtn');
      const infFormStatusEl = document.getElementById('infFormStatus');
      const jobsEl = document.getElementById('jobs');
      const selectedJobEl = document.getElementById('selectedJob');
      const logEl = document.getElementById('log');
      const auditEl = document.getElementById('audit');
      const auditDetailsEl = document.getElementById('auditDetails');
      const backupsAdminDbEl = document.getElementById('backupsAdminDb');
      const backupsReleaseMetaEl = document.getElementById('backupsReleaseMeta');
      const auditEventTypeEl = document.getElementById('auditEventType');
      const auditResultEl = document.getElementById('auditResult');
      const auditRequestIdEl = document.getElementById('auditRequestId');
      const coolProfileIdEl = document.getElementById('coolProfileId');
      const coolProfileVersionEl = document.getElementById('coolProfileVersion');
      const coolTopNEl = document.getElementById('coolTopN');
      const coolResetActiveEl = document.getElementById('coolResetActive');
      const coolSavedProfilesEl = document.getElementById('coolSavedProfiles');
      const coolLoadProfileBtnEl = document.getElementById('coolLoadProfileBtn');
      const coolnessSlidersEl = document.getElementById('coolnessSliders');
      const coolWeightsJsonEl = document.getElementById('coolWeightsJson');
      const coolPreviewOutEl = document.getElementById('coolPreviewOut');
      const coolPreviewSummaryEl = document.getElementById('coolPreviewSummary');
      const coolPreviewChangesEl = document.getElementById('coolPreviewChanges');
      const coolPreviewTypeDistEl = document.getElementById('coolPreviewTypeDist');
      const coolPreviewTypePieEl = document.getElementById('coolPreviewTypePie');
      const coolPreviewSpectralDistEl = document.getElementById('coolPreviewSpectralDist');
      const coolPreviewSpectralPieEl = document.getElementById('coolPreviewSpectralPie');
      const coolPreviewTopSystemsEl = document.getElementById('coolPreviewTopSystems');
	      const coolRunStatusEl = document.getElementById('coolRunStatus');
      const coolPreviewNoticeEl = document.getElementById('coolPreviewNotice');
      const coolApplyBtnEl = document.getElementById('coolApplyBtn');
      const coolSaveBtnEl = document.getElementById('coolSaveBtn');
      const coolActivateBtnEl = document.getElementById('coolActivateBtn');
      const snapshotTopCoolnessEl = document.getElementById('snapshotTopCoolness');
      const snapshotTopCoolnessNumberEl = document.getElementById('snapshotTopCoolnessNumber');
      const snapshotMaxDistanceLyEl = document.getElementById('snapshotMaxDistanceLy');
      const snapshotMaxDistanceLyNumberEl = document.getElementById('snapshotMaxDistanceLyNumber');
      const snapshotMinStarCountEl = document.getElementById('snapshotMinStarCount');
      const snapshotMinStarCountNumberEl = document.getElementById('snapshotMinStarCountNumber');
      const snapshotMinPlanetCountEl = document.getElementById('snapshotMinPlanetCount');
      const snapshotMinPlanetCountNumberEl = document.getElementById('snapshotMinPlanetCountNumber');
      const snapshotMinCoolnessScoreEl = document.getElementById('snapshotMinCoolnessScore');
      const snapshotMinCoolnessScoreNumberEl = document.getElementById('snapshotMinCoolnessScoreNumber');
      const snapshotForceRegenerateEl = document.getElementById('snapshotForceRegenerate');
      const snapshotRunBtnEl = document.getElementById('snapshotRunBtn');
      const snapshotRunStatusEl = document.getElementById('snapshotRunStatus');
      const presetBalancedBtnEl = document.getElementById('presetBalanced');
	      const presetExoticBtnEl = document.getElementById('presetExotic');
	      const presetHabitableBtnEl = document.getElementById('presetHabitable');
	      const presetNearbyBtnEl = document.getElementById('presetNearby');
	      const csrfCookieName = '{csrf_cookie_name}';
	      const actionCatalog = new Map();
	      let currentJobId = null;
      let currentOffset = 0;
      let nextAuditBeforeId = null;
      let auditPreset = 'all';
	      let currentScreen = 'status';
	      let activeCoolnessProfile = null;
	      let activeCoolnessPointer = null;
	      let coolnessProfiles = [];
	      let latestJobs = [];
	      let coolnessFollowJobId = null;
      let activePreset = 'custom';
      const coolnessDefaultWeights = {{
        luminosity: 0.22,
        proper_motion: 0.10,
        multiplicity: 0.14,
        nice_planets: 0.12,
        weird_planets: 0.14,
        proximity: 0.08,
        system_complexity: 0.12,
        exotic_star: 0.08,
      }};
      const coolnessPresetWeights = {{
        balanced: {{ ...coolnessDefaultWeights }},
        exotic: {{
          ...coolnessDefaultWeights,
          exotic_star: 0.22,
          weird_planets: 0.20,
          luminosity: 0.16,
          proximity: 0.04,
        }},
        habitable: {{
          ...coolnessDefaultWeights,
          nice_planets: 0.28,
          proximity: 0.16,
          luminosity: 0.14,
          exotic_star: 0.04,
        }},
        nearby: {{
          ...coolnessDefaultWeights,
          proximity: 0.28,
          proper_motion: 0.18,
          luminosity: 0.14,
          weird_planets: 0.08,
        }},
      }};
      const coolnessLabels = {{
        luminosity: 'Luminosity',
        proper_motion: 'Proper Motion',
        multiplicity: 'Multiplicity',
        nice_planets: 'Nice Planets',
        weird_planets: 'Weird Planets',
        proximity: 'Proximity',
        system_complexity: 'System Complexity',
        exotic_star: 'Exotic Star',
      }};
      let currentCoolnessWeights = {{ ...coolnessDefaultWeights }};

      function setScreen(screenName) {{
        currentScreen = screenName;
        document.getElementById('screenOperations').classList.toggle('active', screenName === 'operations');
        document.getElementById('screenStatus').classList.toggle('active', screenName === 'status');
        document.getElementById('screenDataset').classList.toggle('active', screenName === 'dataset');
        document.getElementById('screenInference').classList.toggle('active', screenName === 'inference');
        document.getElementById('screenCoolness').classList.toggle('active', screenName === 'coolness');
        document.getElementById('screenActivity').classList.toggle('active', screenName === 'activity');
        document.getElementById('screenTabOperations').classList.toggle('active', screenName === 'operations');
        document.getElementById('screenTabStatus').classList.toggle('active', screenName === 'status');
        document.getElementById('screenTabDataset').classList.toggle('active', screenName === 'dataset');
        document.getElementById('screenTabInference').classList.toggle('active', screenName === 'inference');
        document.getElementById('screenTabCoolness').classList.toggle('active', screenName === 'coolness');
        document.getElementById('screenTabActivity').classList.toggle('active', screenName === 'activity');
      }}

      function setAuditPreset(name) {{
        auditPreset = name;
        document.getElementById('auditPresetAll').classList.toggle('active', name === 'all');
        document.getElementById('auditPresetAuth').classList.toggle('active', name === 'auth');
        document.getElementById('auditPresetActions').classList.toggle('active', name === 'actions');
        document.getElementById('auditPresetInference').classList.toggle('active', name === 'inference');
        document.getElementById('auditPresetQueries').classList.toggle('active', name === 'queries');
      }}

      function csrfToken() {{
        const csrfCookie = document.cookie.split('; ').find(x => x.startsWith(csrfCookieName + '=')) || '';
        return decodeURIComponent((csrfCookie.split('=')[1] || ''));
      }}

	      async function fetchJson(url, options = undefined) {{
        const res = await fetch(url, options);
        let data = null;
        const text = await res.text();
        try {{
          data = text ? JSON.parse(text) : {{}};
        }} catch (_) {{
          data = {{ raw: text }};
	        }}
	        return {{ res, data }};
	      }}

	      function sleep(ms) {{
	        return new Promise((resolve) => setTimeout(resolve, ms));
	      }}

	      function isRunningStatus(status) {{
	        return status === 'queued' || status === 'running';
	      }}

      const adminThemeStorageKey = 'spacegate.theme';
      const adminThemeIds = new Set([
        'simple_light',
        'simple_dark',
        'cyberpunk',
        'lcars',
        'mission_control',
        'aurora',
        'retro_90s',
        'deep_space_minimal',
      ]);
      const adminThemeAliases = {{
        light: 'simple_light',
        midnight: 'simple_dark',
        mission: 'mission_control',
        enterprise: 'lcars',
      }};

      function normalizeAdminTheme(raw) {{
        const key = String(raw || '').trim().toLowerCase();
        const mapped = adminThemeAliases[key] || key;
        return adminThemeIds.has(mapped) ? mapped : 'simple_light';
      }}

      function resolveAdminTheme() {{
        const attrRaw = document.documentElement.getAttribute('data-theme');
        if (attrRaw && String(attrRaw).trim()) {{
          return normalizeAdminTheme(attrRaw);
        }}
        try {{
          const storedRaw = window.localStorage.getItem(adminThemeStorageKey);
          if (storedRaw && String(storedRaw).trim()) {{
            return normalizeAdminTheme(storedRaw);
          }}
        }} catch (_) {{
          // ignore storage errors
        }}
        return 'simple_light';
      }}

      function applyAdminTheme(theme) {{
        const normalized = normalizeAdminTheme(theme);
        document.documentElement.setAttribute('data-theme', normalized);
        if (adminThemeSelectEl) adminThemeSelectEl.value = normalized;
        try {{
          window.localStorage.setItem(adminThemeStorageKey, normalized);
        }} catch (_) {{
          // ignore storage errors
        }}
      }}

	      const profileTokenRe = /^[A-Za-z0-9_.-]+$/;

	      function sanitizeProfileToken(raw) {{
	        const trimmed = String(raw || '').trim();
	        if (!trimmed) return '';
	        if (profileTokenRe.test(trimmed)) return trimmed;
	        return trimmed
	          .replace(/[^A-Za-z0-9_.-]+/g, '-')
	          .replace(/-+/g, '-')
	          .replace(/^[.-]+|[.-]+$/g, '');
	      }}

	      function normalizeProfileFields() {{
	        const originalId = String(coolProfileIdEl.value || '').trim();
	        const originalVersion = String(coolProfileVersionEl.value || '').trim();
	        const profileId = sanitizeProfileToken(originalId);
	        const profileVersion = sanitizeProfileToken(originalVersion);
	        if (profileId !== originalId) coolProfileIdEl.value = profileId;
	        if (profileVersion !== originalVersion) coolProfileVersionEl.value = profileVersion;
	        return {{
	          originalId,
	          originalVersion,
	          profileId,
	          profileVersion,
	        }};
	      }}

	      function findActiveCoolnessJob() {{
	        return (latestJobs || []).find((job) => {{
	          if (!job || String(job.action || '') !== 'score_coolness') return false;
	          return isRunningStatus(String(job.status || ''));
	        }}) || null;
	      }}

	      async function waitForJobTerminal(jobId, timeoutMs = 20 * 60 * 1000) {{
	        const started = Date.now();
	        setRunStatus('running', `job ${{jobId}}`);
	        while (Date.now() - started <= timeoutMs) {{
	          const {{ res, data }} = await fetchJson(`${{ADMIN_API_BASE}}/actions/jobs/${{jobId}}`, {{ credentials: 'include' }});
	          if (!res.ok) {{
	            setRunStatus('error', `job lookup failed (${{jobId}})`);
	            return {{ ok: false, reason: 'lookup_failed', data }};
	          }}
	          const job = (data && data.job) || {{}};
	          const status = String(job.status || '');
	          if (!isRunningStatus(status)) {{
	            return {{ ok: true, job }};
	          }}
	          setRunStatus(status, `job ${{jobId}}`);
	          await sleep(1500);
	        }}
	        setRunStatus('timeout', `job ${{jobId}}`);
	        return {{ ok: false, reason: 'timeout', data: {{}} }};
	      }}

	      async function followCoolnessJobAndPreview(jobId) {{
	        coolnessFollowJobId = String(jobId || '');
	        const waitResult = await waitForJobTerminal(jobId);
	        if (coolnessFollowJobId !== String(jobId || '')) {{
	          return;
	        }}
	        await loadJobs();
	        if (!waitResult.ok) {{
	          if (waitResult.reason === 'timeout') {{
	            setRunStatus('running', `job ${{jobId}} still in progress`);
	            setPreviewNotice(`Scoring job ${{jobId}} is still running. Check Activity > Jobs and run preview when complete.`);
	            return;
	          }}
	          setRunStatus('error', `job ${{jobId}} status unavailable`);
	          setPreviewNotice(`Could not fetch status for scoring job ${{jobId}}. Check Activity > Jobs.`);
	          return;
	        }}
	        const finalJob = waitResult.job || {{}};
	        const finalStatus = String(finalJob.status || '');
	        if (finalStatus !== 'succeeded') {{
	          setRunStatus(finalStatus || 'failed', `job ${{jobId}}`);
	          setPreviewNotice(`Scoring job ${{jobId}} finished with status '${{finalStatus}}'. Check Activity > Jobs log.`);
	          return;
	        }}
	        setRunStatus('refreshing', `job ${{jobId}} completed`);
	        await loadCoolnessState({{ preserveEditor: true }});
	        for (let attempt = 0; attempt < 5; attempt += 1) {{
	          const outcome = await previewCoolness({{ suppressAlert: true }});
	          if (outcome && outcome.ok) {{
	            setRunStatus('ready', `job ${{jobId}} applied`);
	            setPreviewNotice(`Scoring job ${{jobId}} completed. Preview refreshed from persisted outputs.`);
	            return;
	          }}
	          const reason = String((outcome && outcome.reason) || '');
	          if (reason !== 'conflict' && reason !== 'job_running') {{
	            return;
	          }}
	          await sleep(1200);
	        }}
	        setRunStatus('ready', `job ${{jobId}} applied`);
	        setPreviewNotice(`Scoring job ${{jobId}} completed. Preview is temporarily busy; retry in a few seconds.`);
	      }}

	      async function followSaveProfileJob(jobId, profileId, profileVersion, options = undefined) {{
	        const suppressSuccessNotice = !!(options && options.suppressSuccessNotice);
	        const waitResult = await waitForJobTerminal(jobId);
	        await loadJobs();
        if (!waitResult.ok) {{
          if (waitResult.reason === 'timeout') {{
            setRunStatus('running', `save job ${{jobId}} still in progress`);
            setPreviewNotice(`Save job ${{jobId}} is still running. Check Activity > Jobs.`);
            return {{ ok: false, reason: 'timeout' }};
          }}
          setRunStatus('error', `save job ${{jobId}} status unavailable`);
          setPreviewNotice(`Could not fetch status for save job ${{jobId}}. Check Activity > Jobs.`);
          return {{ ok: false, reason: 'status_unavailable' }};
        }}
	        const finalJob = waitResult.job || {{}};
	        const finalStatus = String(finalJob.status || '');
	        if (finalStatus !== 'succeeded') {{
	          let failureHint = '';
	          try {{
	            const res = await fetch(`${{ADMIN_API_BASE}}/actions/jobs/${{jobId}}/log/download`, {{ credentials: 'include' }});
	            if (res.ok) {{
	              const text = await res.text();
	              const lines = String(text || '').split(/\\r?\\n/).map((line) => line.trim()).filter(Boolean);
	              for (let i = lines.length - 1; i >= 0; i -= 1) {{
	                const line = lines[i];
	                if (!line) continue;
	                if (line.startsWith('[error]')) {{
	                  failureHint = line.replace(/^\\[error\\]\\s*/, '').trim();
	                  break;
	                }}
	                if (line.startsWith('[20') && line.includes('Finished status=')) continue;
	                if (line.startsWith('[') && line.endsWith('Starting action')) continue;
	                if (line.startsWith('Execution:')) continue;
	                if (line.startsWith('Params:')) continue;
	                if (line.startsWith('Action:')) continue;
	                failureHint = line;
	                break;
	              }}
	            }}
	          }} catch (_) {{
	            failureHint = '';
	          }}
	          setRunStatus(finalStatus || 'failed', `save job ${{jobId}}`);
	          if (failureHint) {{
	            setPreviewNotice(`Save job ${{jobId}} failed: ${{failureHint}}`);
	          }} else {{
	            setPreviewNotice(`Save job ${{jobId}} finished with status '${{finalStatus}}'. Check Activity > Jobs log.`);
	          }}
	          return {{ ok: false, reason: 'job_failed', status: finalStatus, hint: failureHint }};
	        }}
        await loadCoolnessState({{ preserveEditor: true, suppressNotice: true }});
        renderSavedProfilesOptions();
        if (coolSavedProfilesEl) {{
          coolSavedProfilesEl.value = profileOptionValue(profileId, profileVersion);
        }}
        setRunStatus('saved', `${{profileId}}@${{profileVersion}}`);
	        if (!suppressSuccessNotice) {{
	          setPreviewNotice(
	            `Saved immutable profile ${{profileId}}@${{profileVersion}}. It is stored but not active; active profile remains ${{
	              String((activeCoolnessPointer || {{}}).profile_id || (activeCoolnessProfile || {{}}).profile_id || 'n/a')
            }}@${{
              String((activeCoolnessPointer || {{}}).profile_version || (activeCoolnessProfile || {{}}).profile_version || 'n/a')
            }}.`
	          );
	        }}
	        return {{ ok: true, profileId, profileVersion }};
	      }}

	      async function followActivateProfileJob(jobId, profileId, profileVersion) {{
	        const waitResult = await waitForJobTerminal(jobId);
	        await loadJobs();
	        if (!waitResult.ok) {{
	          if (waitResult.reason === 'timeout') {{
	            setRunStatus('running', `activate job ${{jobId}} still in progress`);
	            setPreviewNotice(`Activate job ${{jobId}} is still running. Check Activity > Jobs.`);
	            return;
	          }}
	          setRunStatus('error', `activate job ${{jobId}} status unavailable`);
	          setPreviewNotice(`Could not fetch status for activate job ${{jobId}}. Check Activity > Jobs.`);
	          return;
	        }}
	        const finalJob = waitResult.job || {{}};
	        const finalStatus = String(finalJob.status || '');
	        if (finalStatus !== 'succeeded') {{
	          let failureHint = '';
	          try {{
	            const res = await fetch(`${{ADMIN_API_BASE}}/actions/jobs/${{jobId}}/log/download`, {{ credentials: 'include' }});
	            if (res.ok) {{
	              const text = await res.text();
	              const lines = String(text || '').split(/\\r?\\n/).map((line) => line.trim()).filter(Boolean);
	              for (let i = lines.length - 1; i >= 0; i -= 1) {{
	                const line = lines[i];
	                if (!line) continue;
	                if (line.startsWith('[error]')) {{
	                  failureHint = line.replace(/^\\[error\\]\\s*/, '').trim();
	                  break;
	                }}
	                if (line.startsWith('[20') && line.includes('Finished status=')) continue;
	                if (line.startsWith('[') && line.endsWith('Starting action')) continue;
	                if (line.startsWith('Execution:')) continue;
	                if (line.startsWith('Params:')) continue;
	                if (line.startsWith('Action:')) continue;
	                failureHint = line;
	                break;
	              }}
	            }}
	          }} catch (_) {{
	            failureHint = '';
	          }}
	          setRunStatus(finalStatus || 'failed', `activate job ${{jobId}}`);
	          if (failureHint) {{
	            setPreviewNotice(`Activate job ${{jobId}} failed: ${{failureHint}}`);
	          }} else {{
	            setPreviewNotice(`Activate job ${{jobId}} finished with status '${{finalStatus}}'. Check Activity > Jobs log.`);
	          }}
	          return;
	        }}
	        await loadCoolnessState({{ preserveEditor: false, suppressNotice: true }});
	        const outcome = await previewCoolness({{ suppressAlert: true }});
	        setRunStatus('activated', `${{profileId}}@${{profileVersion}}`);
	        if (outcome && outcome.ok) {{
	          setPreviewNotice(`Activated profile ${{profileId}}@${{profileVersion}} and refreshed preview.`);
	        }} else {{
	          setPreviewNotice(`Activated profile ${{profileId}}@${{profileVersion}}.`);
	        }}
	      }}

	      function statusClass(status) {{
        const key = String(status || '').toLowerCase();
        if (key === 'running') return 'status-running';
        if (key === 'queued') return 'status-queued';
        if (key === 'succeeded') return 'status-succeeded';
        if (key === 'failed') return 'status-failed';
        if (key === 'cancelled') return 'status-cancelled';
        return '';
      }}

      function normalizeCoolnessWeights(raw) {{
        const outWeights = {{ ...coolnessDefaultWeights }};
        if (!raw || typeof raw !== 'object') return outWeights;
        Object.keys(coolnessDefaultWeights).forEach((key) => {{
          const value = Number(raw[key]);
          if (Number.isFinite(value) && value >= 0) outWeights[key] = value;
        }});
        return outWeights;
      }}

      function weightsEqual(left, right) {{
        const l = normalizeCoolnessWeights(left);
        const r = normalizeCoolnessWeights(right);
        return Object.keys(coolnessDefaultWeights).every((key) => Math.abs((l[key] || 0) - (r[key] || 0)) < 1e-12);
      }}

      function suggestNextProfileVersion(currentVersion) {{
        const raw = String(currentVersion || '').trim();
        if (!raw) return '1';
        if (/^\\d+$/.test(raw)) {{
          const n = Number.parseInt(raw, 10);
          if (Number.isFinite(n) && n >= 0) return String(n + 1);
        }}
        const suffixMatch = raw.match(/^(.*?)-(\\d+)$/);
        if (suffixMatch) {{
          const base = suffixMatch[1];
          const n = Number.parseInt(suffixMatch[2], 10);
          if (Number.isFinite(n) && n >= 0) return `${{base}}-${{n + 1}}`;
        }}
        return `${{raw}}.1`;
      }}

      function toNumber(value, fallback = 0) {{
        const n = Number(value);
        return Number.isFinite(n) ? n : fallback;
      }}

      function pct(part, total) {{
        const p = toNumber(part, 0);
        const t = toNumber(total, 0);
        if (t <= 0) return 0;
        return (p / t) * 100;
      }}

      function clearPreviewVisuals() {{
        coolPreviewSummaryEl.innerHTML = '';
        coolPreviewChangesEl.innerHTML = '';
        coolPreviewTypeDistEl.innerHTML = '';
        if (coolPreviewTypePieEl) coolPreviewTypePieEl.innerHTML = '';
        coolPreviewSpectralDistEl.innerHTML = '';
        if (coolPreviewSpectralPieEl) coolPreviewSpectralPieEl.innerHTML = '';
        coolPreviewTopSystemsEl.innerHTML = '';
      }}

      function setPreviewNotice(message) {{
        coolPreviewNoticeEl.textContent = message || '';
      }}

      function setRunStatus(status, detail = '') {{
        const head = String(status || 'idle').trim().toLowerCase() || 'idle';
        coolRunStatusEl.textContent = detail ? `Status: ${{head}} | ${{detail}}` : `Status: ${{head}}`;
      }}

      function setSnapshotRunStatus(status, detail = '') {{
        const head = String(status || 'idle').trim().toLowerCase() || 'idle';
        snapshotRunStatusEl.textContent = detail ? `Status: ${{head}} | ${{detail}}` : `Status: ${{head}}`;
      }}

      function bindRangeNumberPair(rangeEl, numberEl, fallback, options = undefined) {{
        const min = Number(options && options.min);
        const max = Number(options && options.max);
        const step = Number(options && options.step);
        const defaultValue = Number.isFinite(Number(fallback)) ? Number(fallback) : 0;
        const normalize = (raw) => {{
          let value = Number(raw);
          if (!Number.isFinite(value)) value = defaultValue;
          if (Number.isFinite(min)) value = Math.max(min, value);
          if (Number.isFinite(max)) value = Math.min(max, value);
          if (Number.isFinite(step) && step > 0) {{
            value = Math.round(value / step) * step;
          }}
          return value;
        }};
        const apply = (raw) => {{
          const value = normalize(raw);
          const display = Number.isFinite(step) && step > 0 && step < 1 ? value.toFixed(1) : String(Math.round(value));
          rangeEl.value = display;
          numberEl.value = display;
          return value;
        }};
        rangeEl.addEventListener('input', () => apply(rangeEl.value));
        numberEl.addEventListener('change', () => apply(numberEl.value));
        apply(numberEl.value || rangeEl.value || defaultValue);
      }}

      function setActivePreset(name) {{
        activePreset = String(name || 'custom');
        const mapping = {{
          balanced: presetBalancedBtnEl,
          exotic: presetExoticBtnEl,
          habitable: presetHabitableBtnEl,
          nearby: presetNearbyBtnEl,
          reset: coolResetActiveEl,
        }};
        Object.entries(mapping).forEach(([key, btn]) => {{
          if (!btn) return;
          btn.classList.toggle('active', key === activePreset);
        }});
      }}

      function renderKpiCard(label, value) {{
        const box = document.createElement('div');
        box.className = 'kpi';
        const k = document.createElement('div');
        k.className = 'k';
        k.textContent = label;
        const v = document.createElement('div');
        v.className = 'v';
        v.textContent = String(value);
        box.appendChild(k);
        box.appendChild(v);
        return box;
      }}

      function renderBarList(target, rows, total) {{
        target.innerHTML = '';
        if (!rows.length) {{
          const empty = document.createElement('div');
          empty.className = 'small muted';
          empty.textContent = 'No data';
          target.appendChild(empty);
          return;
        }}
        rows.forEach((row) => {{
          const line = document.createElement('div');
          line.className = 'bar-row';
          const label = document.createElement('div');
          label.textContent = String(row.label || '');
          const track = document.createElement('div');
          track.className = 'bar-track';
          const fill = document.createElement('div');
          fill.className = 'bar-fill';
          const amount = toNumber(row.value, 0);
          const share = Math.max(0, Math.min(100, pct(amount, total)));
          fill.style.width = `${{share.toFixed(1)}}%`;
          track.appendChild(fill);
          const value = document.createElement('div');
          value.className = 'small';
          value.textContent = `${{amount}} (${{share.toFixed(1)}}%)`;
          line.appendChild(label);
          line.appendChild(track);
          line.appendChild(value);
          target.appendChild(line);
        }});
      }}

      const PIE_COLORS = [
        '#4f46e5',
        '#0284c7',
        '#0f766e',
        '#16a34a',
        '#ca8a04',
        '#ea580c',
        '#dc2626',
        '#be185d',
        '#7c3aed',
        '#6b7280',
      ];

      function spectralPieColor(label) {{
        const key = String(label || '').trim().toUpperCase();
        if (key === 'O') return '#6aa9ff';
        if (key === 'B') return '#8cc8ff';
        if (key === 'A') return '#d7e9ff';
        if (key === 'F') return '#fff2b5';
        if (key === 'G') return '#ffd86b';
        if (key === 'K') return '#ffb36a';
        if (key === 'M') return '#f06a55';
        if (key === 'L') return '#cf6b57';
        if (key === 'T') return '#8f6bc7';
        if (key === 'Y') return '#6fc7d8';
        if (key === 'D') return '#c8d2de';
        return '#7f8ea3';
      }}

      function compactPieRows(rows, maxSlices = 8) {{
        const norm = (Array.isArray(rows) ? rows : [])
          .map((row) => ({{
            label: String(row && row.label ? row.label : '?'),
            value: Math.max(0, toNumber(row && row.value, 0)),
          }}))
          .filter((row) => row.value > 0)
          .sort((a, b) => b.value - a.value);
        if (norm.length <= maxSlices) return norm;
        const keep = norm.slice(0, Math.max(1, maxSlices - 1));
        const tail = norm.slice(Math.max(1, maxSlices - 1));
        const other = tail.reduce((acc, row) => acc + toNumber(row.value, 0), 0);
        if (other > 0) keep.push({{ label: 'Other', value: other }});
        return keep;
      }}

      function renderPieChart(target, rows, total, title = '') {{
        if (!target) return;
        target.innerHTML = '';
        const compactRows = compactPieRows(rows, 8);
        const computedTotal = compactRows.reduce((acc, row) => acc + toNumber(row.value, 0), 0);
        const denom = toNumber(total, 0) > 0 ? toNumber(total, 0) : computedTotal;
        if (!compactRows.length || denom <= 0) {{
          const empty = document.createElement('div');
          empty.className = 'small muted';
          empty.textContent = 'No data';
          target.appendChild(empty);
          return;
        }}
        if (title) {{
          const titleEl = document.createElement('div');
          titleEl.className = 'pie-title';
          titleEl.textContent = String(title);
          target.appendChild(titleEl);
        }}

        let cursorPct = 0;
        const gradientParts = [];
        const rowsWithColor = compactRows.map((row, idx) => {{
          const color = (row && row.color) ? String(row.color) : PIE_COLORS[idx % PIE_COLORS.length];
          const partPct = Math.max(0, Math.min(100, pct(row.value, denom)));
          const nextPct = Math.max(cursorPct, Math.min(100, cursorPct + partPct));
          gradientParts.push(`${{color}} ${{cursorPct.toFixed(2)}}% ${{nextPct.toFixed(2)}}%`);
          cursorPct = nextPct;
          return {{
            ...row,
            color,
            sharePct: partPct,
          }};
        }});

        const wrap = document.createElement('div');
        wrap.className = 'pie-wrap';
        const pie = document.createElement('div');
        pie.className = 'pie-plot';
        pie.style.background = `conic-gradient(${{gradientParts.join(', ')}})`;
        wrap.appendChild(pie);

        const legend = document.createElement('ul');
        legend.className = 'pie-legend';
        rowsWithColor.forEach((row) => {{
          const li = document.createElement('li');
          const dot = document.createElement('span');
          dot.className = 'pie-dot';
          dot.style.background = row.color;
          const label = document.createElement('span');
          label.textContent = row.label;
          const value = document.createElement('span');
          value.className = 'small';
          value.textContent = `${{formatInt(row.value)}} (${{formatPct(row.sharePct)}})`;
          li.appendChild(dot);
          li.appendChild(label);
          li.appendChild(value);
          legend.appendChild(li);
        }});
        wrap.appendChild(legend);
        target.appendChild(wrap);
      }}

      function renderCoolnessPreview(data) {{
        clearPreviewVisuals();
        if (!data || typeof data !== 'object') {{
          return;
        }}

        const preview = (data.preview && typeof data.preview === 'object') ? data.preview : {{}};
        const diversity = (data.diversity && typeof data.diversity === 'object') ? data.diversity : {{}};
        const diversityScores = (diversity.diversity_scores && typeof diversity.diversity_scores === 'object') ? diversity.diversity_scores : {{}};
        const candidate = (preview.candidate && typeof preview.candidate === 'object') ? preview.candidate : {{}};
        const diff = (preview.diff_vs_active && typeof preview.diff_vs_active === 'object') ? preview.diff_vs_active : {{}};
        const changed = Array.isArray(diff.changed) ? diff.changed : [];
        const sampleSize = toNumber(diversity.sample_size, 0);
        const topN = toNumber(diversity.top_n, toNumber(coolTopNEl.value, 200));

        coolPreviewSummaryEl.appendChild(renderKpiCard('Source', preview.source || 'n/a'));
        coolPreviewSummaryEl.appendChild(renderKpiCard('Profile', `${{candidate.profile_id || 'n/a'}}@${{candidate.profile_version || 'n/a'}}`));
        coolPreviewSummaryEl.appendChild(renderKpiCard('Weights Changed', toNumber(diff.changed_count, changed.length)));
        coolPreviewSummaryEl.appendChild(renderKpiCard('Sample Size', `${{sampleSize}} / ${{topN}}`));
        coolPreviewSummaryEl.appendChild(
          renderKpiCard(
            'Shannon Diversity',
            `${{(toNumber(diversityScores.spectral_shannon_normalized, 0) * 100).toFixed(1)}}%`
          )
        );
        coolPreviewSummaryEl.appendChild(
          renderKpiCard(
            'Effective Spectral Classes',
            toNumber(diversityScores.spectral_effective_classes, 1).toFixed(2)
          )
        );

        if (!changed.length) {{
          const li = document.createElement('li');
          li.className = 'small muted';
          li.textContent = 'No changes vs active weights.';
          coolPreviewChangesEl.appendChild(li);
        }} else {{
          changed.slice(0, 12).forEach((row) => {{
            const li = document.createElement('li');
            const delta = toNumber(row.delta, 0);
            const sign = delta >= 0 ? '+' : '';
            li.textContent = `${{row.key}}: ${{toNumber(row.left, 0).toFixed(2)}} -> ${{toNumber(row.right, 0).toFixed(2)}} (${{sign}}${{delta.toFixed(2)}})`;
            coolPreviewChangesEl.appendChild(li);
          }});
        }}

        const td = (diversity.type_distribution && typeof diversity.type_distribution === 'object') ? diversity.type_distribution : {{}};
        const typeRows = [
          {{ label: 'With planets', value: toNumber(td.with_planets, 0) }},
          {{ label: 'Without planets', value: toNumber(td.without_planets, 0) }},
          {{ label: 'Multi-star', value: toNumber(td.multi_star, 0) }},
          {{ label: 'Single-star', value: toNumber(td.single_star, 0) }},
          {{ label: 'Weird planets', value: toNumber(td.weird_planet_systems, 0) }},
        ];
        renderBarList(
          coolPreviewTypeDistEl,
          typeRows,
          sampleSize
        );
        renderPieChart(coolPreviewTypePieEl, typeRows, sampleSize, 'Type mix');

        const spectral = Array.isArray(diversity.spectral_distribution) ? diversity.spectral_distribution : [];
        const spectralRows = spectral.slice(0, 12).map((row) => ({{
          label: String(row.spectral_class || '?'),
          value: toNumber(row.systems, 0),
          color: spectralPieColor(row.spectral_class),
        }}));
        renderBarList(
          coolPreviewSpectralDistEl,
          spectralRows.slice(0, 8),
          sampleSize
        );
        renderPieChart(coolPreviewSpectralPieEl, spectralRows, sampleSize, 'Spectral mix');

        const topSystems = Array.isArray(diversity.top_systems) ? diversity.top_systems : [];
        if (!topSystems.length) {{
          const tr = document.createElement('tr');
          const tdEmpty = document.createElement('td');
          tdEmpty.colSpan = 8;
          tdEmpty.className = 'small muted';
          tdEmpty.textContent = 'No top systems in preview.';
          tr.appendChild(tdEmpty);
          coolPreviewTopSystemsEl.appendChild(tr);
        }} else {{
          const subscoreKeys = [
            ['L', 'luminosity'],
            ['PM', 'proper_motion'],
            ['Mul', 'multiplicity'],
            ['Nice', 'nice_planets'],
            ['Weird', 'weird_planets'],
            ['Near', 'proximity'],
            ['Complex', 'system_complexity'],
            ['Exotic', 'exotic_star'],
          ];
          topSystems.slice(0, 12).forEach((row, idx) => {{
            const tr = document.createElement('tr');
            const rank = document.createElement('td');
            rank.textContent = String(idx + 1);
            const name = document.createElement('td');
            name.textContent = String(row.system_name || row.stable_object_key || row.system_id || '?');
            const score = document.createElement('td');
            score.textContent = toNumber(row.score_total, 0).toFixed(2);
            const dist = document.createElement('td');
            const distLy = row.dist_ly;
            dist.textContent = (distLy === null || distLy === undefined) ? 'n/a' : toNumber(distLy, 0).toFixed(1);
            const stars = document.createElement('td');
            stars.textContent = String(toNumber(row.star_count, 0));
            const planets = document.createElement('td');
            planets.textContent = String(toNumber(row.planet_count, 0));
            const typ = document.createElement('td');
            typ.textContent = String(row.dominant_spectral_class || '?');
            const subs = document.createElement('td');
            const subscores = (row.subscores && typeof row.subscores === 'object') ? row.subscores : {{}};
            subs.textContent = subscoreKeys.map(([label, key]) => `${{label}}:${{toNumber(subscores[key], 0).toFixed(1)}}`).join(' ');
            tr.appendChild(rank);
            tr.appendChild(name);
            tr.appendChild(score);
            tr.appendChild(dist);
            tr.appendChild(stars);
            tr.appendChild(planets);
            tr.appendChild(typ);
            tr.appendChild(subs);
            coolPreviewTopSystemsEl.appendChild(tr);
          }});
        }}
      }}

      function updateCoolnessJsonPreview() {{
        coolWeightsJsonEl.textContent = JSON.stringify(currentCoolnessWeights, null, 2);
      }}

	      function setCoolnessWeights(nextWeights, presetName = 'custom') {{
	        currentCoolnessWeights = normalizeCoolnessWeights(nextWeights);
	        renderCoolnessSliders();
        setActivePreset(presetName);
	      }}

	      function findCoolnessProfile(profileId, profileVersion) {{
	        const pid = String(profileId || '');
	        const pver = String(profileVersion || '');
	        return (coolnessProfiles || []).find((row) => {{
	          if (!row || typeof row !== 'object') return false;
	          return String(row.profile_id || '') === pid && String(row.profile_version || '') === pver;
	        }}) || null;
	      }}

      function profileOptionValue(profileId, profileVersion) {{
        return `${{String(profileId || '')}}@@${{String(profileVersion || '')}}`;
      }}

      function splitProfileOptionValue(raw) {{
        const value = String(raw || '');
        const sep = value.indexOf('@@');
        if (sep < 0) return [value, ''];
        return [value.slice(0, sep), value.slice(sep + 2)];
      }}

      function renderSavedProfilesOptions() {{
        if (!coolSavedProfilesEl) return;
        const previous = coolSavedProfilesEl.value;
        const rows = (coolnessProfiles || [])
          .filter((row) => row && typeof row === 'object')
          .slice()
          .sort((a, b) => {{
            const ap = String(a.profile_id || '');
            const bp = String(b.profile_id || '');
            if (ap !== bp) return ap.localeCompare(bp);
            const av = String(a.profile_version || '');
            const bv = String(b.profile_version || '');
            if (/^\\d+$/.test(av) && /^\\d+$/.test(bv)) return Number.parseInt(bv, 10) - Number.parseInt(av, 10);
            return bv.localeCompare(av);
          }});

        coolSavedProfilesEl.innerHTML = '';
        if (!rows.length) {{
          const opt = document.createElement('option');
          opt.value = '';
          opt.textContent = '(no saved profiles)';
          coolSavedProfilesEl.appendChild(opt);
          return;
        }}
        const activeId = String((activeCoolnessPointer || {{}}).profile_id || (activeCoolnessProfile || {{}}).profile_id || '');
        const activeVersion = String((activeCoolnessPointer || {{}}).profile_version || (activeCoolnessProfile || {{}}).profile_version || '');
        rows.forEach((row) => {{
          const pid = String(row.profile_id || '');
          const pver = String(row.profile_version || '');
          const opt = document.createElement('option');
          opt.value = profileOptionValue(pid, pver);
          const activeTag = (pid === activeId && pver === activeVersion) ? ' [active]' : '';
          opt.textContent = `${{pid}}@${{pver}}${{activeTag}}`;
          coolSavedProfilesEl.appendChild(opt);
        }});
        const fallback = profileOptionValue(
          String(coolProfileIdEl.value || activeId || ''),
          String(coolProfileVersionEl.value || activeVersion || '')
        );
        const wanted = previous || fallback;
        if (Array.from(coolSavedProfilesEl.options).some((opt) => opt.value === wanted)) {{
          coolSavedProfilesEl.value = wanted;
        }}
      }}

      function loadSelectedSavedProfile() {{
        const [profileId, profileVersion] = splitProfileOptionValue(coolSavedProfilesEl ? coolSavedProfilesEl.value : '');
        if (!profileId || !profileVersion) {{
          setPreviewNotice('Pick a saved profile before loading.');
          return;
        }}
        const profile = findCoolnessProfile(profileId, profileVersion);
        if (!profile) {{
          setPreviewNotice(`Saved profile ${{profileId}}@${{profileVersion}} was not found.`);
          return;
        }}
        coolProfileIdEl.value = profileId;
        coolProfileVersionEl.value = profileVersion;
        setCoolnessWeights(profile.weights || coolnessDefaultWeights, 'custom');
        setRunStatus('loaded', `${{profileId}}@${{profileVersion}}`);
        setPreviewNotice(`Loaded saved profile ${{profileId}}@${{profileVersion}} into weight controls.`);
      }}

      function nextUnusedProfileVersion(profileId, startVersion = '') {{
        const pid = String(profileId || '').trim();
        const used = new Set(
          (coolnessProfiles || [])
            .filter((row) => row && typeof row === 'object' && String(row.profile_id || '') === pid)
            .map((row) => String(row.profile_version || ''))
        );

        let candidate = String(startVersion || '').trim();
        if (!candidate) {{
          let maxNumeric = 0;
          let hasNumeric = false;
          used.forEach((v) => {{
            if (!/^\\d+$/.test(v)) return;
            const n = Number.parseInt(v, 10);
            if (Number.isFinite(n)) {{
              hasNumeric = true;
              if (n > maxNumeric) maxNumeric = n;
            }}
          }});
          candidate = hasNumeric ? String(maxNumeric + 1) : '1';
        }}
        let guard = 0;
        while (used.has(candidate) && guard < 200) {{
          candidate = suggestNextProfileVersion(candidate);
          guard += 1;
        }}
        return candidate;
      }}

	      function resetCoolnessToActive() {{
	        const active = activeCoolnessPointer || {{}};
	        const activeProfile = activeCoolnessProfile || {{}};
	        coolProfileIdEl.value = String(active.profile_id || activeProfile.profile_id || 'default');
	        coolProfileVersionEl.value = String(active.profile_version || activeProfile.profile_version || '1');
	        setCoolnessWeights(activeProfile.weights || coolnessDefaultWeights, 'reset');
	        setPreviewNotice('Reset sliders and profile selectors to active profile values.');
	      }}

      function renderCoolnessSliders() {{
        coolnessSlidersEl.innerHTML = '';
        Object.keys(coolnessDefaultWeights).forEach((key) => {{
          const row = document.createElement('div');
          row.className = 'weight-row';

          const label = document.createElement('label');
          label.textContent = coolnessLabels[key] || key;
          label.htmlFor = `coolWeightRange-${{key}}`;
          row.appendChild(label);

          const range = document.createElement('input');
          range.type = 'range';
          range.min = '0';
          range.max = '1';
          range.step = '0.01';
          range.id = `coolWeightRange-${{key}}`;
          range.value = String(currentCoolnessWeights[key] ?? 0);
          row.appendChild(range);

          const number = document.createElement('input');
          number.type = 'number';
          number.min = '0';
          number.max = '1';
          number.step = '0.01';
          number.value = String(currentCoolnessWeights[key] ?? 0);
          row.appendChild(number);

	          const syncValue = (raw) => {{
	            const value = Number(raw);
	            if (!Number.isFinite(value) || value < 0) return;
	            const clamped = Math.min(1, Math.max(0, value));
	            currentCoolnessWeights[key] = clamped;
	            range.value = clamped.toFixed(2);
	            number.value = clamped.toFixed(2);
	            updateCoolnessJsonPreview();
              if (activePreset !== 'custom') setActivePreset('custom');
	          }};
          range.addEventListener('input', () => syncValue(range.value));
          number.addEventListener('change', () => syncValue(number.value));

          coolnessSlidersEl.appendChild(row);
        }});
        updateCoolnessJsonPreview();
      }}

	      async function loadCoolnessState(options = undefined) {{
	        const preserveEditor = !!(options && options.preserveEditor);
        const suppressNotice = !!(options && options.suppressNotice);
        const {{ res, data }} = await fetchJson(`${{ADMIN_API_BASE}}/coolness/state`, {{ credentials: 'include' }});
	        if (!res.ok) {{
	          coolPreviewOutEl.textContent = JSON.stringify(data, null, 2);
	          setPreviewNotice('Could not load active coolness profile.');
	          return;
        }}
	        const active = data.active || {{}};
	        const activeProfile = data.active_profile || {{}};
	        coolnessProfiles = Array.isArray(data.profiles) ? data.profiles : [];
	        activeCoolnessPointer = active;
	        activeCoolnessProfile = activeProfile;
        renderSavedProfilesOptions();
	        if (!preserveEditor) {{
	          coolProfileIdEl.value = String(active.profile_id || activeProfile.profile_id || 'default');
	          coolProfileVersionEl.value = String(active.profile_version || activeProfile.profile_version || '1');
	          const activeWeights = activeProfile.weights || coolnessDefaultWeights;
	          setCoolnessWeights(activeWeights, 'reset');
	        }}
		        if (!suppressNotice) setPreviewNotice(
		          `Active profile is ${{
		            String(active.profile_id || activeProfile.profile_id || 'default')
		          }}@${{
		            String(active.profile_version || activeProfile.profile_version || '1')
		          }}. Preview is read-only; Run updates disc outputs ephemerally; Save Profile persists versions; Activate Profile switches what is live.`
		        );
	      }}

	      async function previewCoolness(options = undefined) {{
	        setRunStatus('previewing', 'read-only simulation');
	        const suppressAlert = !!(options && options.suppressAlert);
	        const runningJob = findActiveCoolnessJob();
	        if (runningJob) {{
	          setRunStatus(String(runningJob.status || 'running'), `job ${{runningJob.job_id}}`);
	          setPreviewNotice(
	            `Scoring job ${{runningJob.job_id}} is ${{runningJob.status}}. Preview is temporarily unavailable until it finishes.`
	          );
	          return {{ ok: false, reason: 'job_running', data: {{ job: runningJob }} }};
	        }}
	        const topN = Math.max(20, Math.min(1000, Number.parseInt(String(coolTopNEl.value || '200'), 10) || 200));
	        coolTopNEl.value = String(topN);
	        const normalized = normalizeProfileFields();
	        const profileId = normalized.profileId;
	        const profileVersion = normalized.profileVersion;
	        const hasProfileRef = !!(profileId && profileVersion);
	        const payload = {{
	          profile_id: hasProfileRef ? profileId : null,
	          profile_version: hasProfileRef ? profileVersion : null,
	          weights: currentCoolnessWeights,
	          top_n: topN,
	        }};
        const {{ res, data }} = await fetchJson(`${{ADMIN_API_BASE}}/coolness/preview`, {{
          method: 'POST',
          credentials: 'include',
          headers: {{
            'Content-Type': 'application/json',
            'X-CSRF-Token': csrfToken(),
          }},
          body: JSON.stringify(payload),
        }});
	        coolPreviewOutEl.textContent = JSON.stringify(data, null, 2);
	        renderCoolnessPreview(data);
	        if (!res.ok) {{
	          const err = (data && data.error) || {{}};
	          if (String(err.code || '') === 'conflict') {{
	            setRunStatus('busy', 'scoring output lock');
	            setPreviewNotice(
	              err.message || 'Preview is temporarily unavailable while scoring writes outputs. Retry in a few seconds.'
	            );
	            return {{ ok: false, reason: 'conflict', res, data }};
	          }}
		          const message = String(
		            (err && err.message) ||
		            (data && data.error && data.error.message) ||
		            'Preview failed'
		          );
		          setRunStatus('error', 'preview failed');
		          setPreviewNotice(`Preview failed: ${{message}}`);
	          if (!suppressAlert) {{
	            alert(`Preview failed: ${{(data && data.error && data.error.message) || res.status}}`);
	          }}
	          return {{ ok: false, reason: 'error', res, data }};
	        }} else {{
	          setRunStatus('ready', 'preview updated');
	          setPreviewNotice('Preview completed. No data was persisted.');
	          return {{ ok: true, reason: 'ok', res, data }};
	        }}
	      }}

      async function applyCoolness() {{
        setRunStatus('running', 'starting score job');
        const runResult = await runAction('score_coolness', {{
          weights_json: JSON.stringify(currentCoolnessWeights),
          ephemeral: true,
        }});
        if (!runResult || !runResult.ok) {{
          setRunStatus('error', 'score job failed to start');
          setPreviewNotice('Run failed. Check job logs in Activity and retry.');
          return;
        }}
	        const job = (runResult.data && runResult.data.job) || {{}};
	        const jobId = String(job.job_id || '');
	        if (!jobId) {{
	          setRunStatus('queued', 'job created');
	          setPreviewNotice('Scoring job queued. Watch Activity > Jobs for completion and run preview when complete.');
		        await loadCoolnessState({{ preserveEditor: true }});
	          return;
	        }}
	        setRunStatus('queued', `job ${{jobId}}`);
        setPreviewNotice(`Scoring job ${{jobId}} queued. Waiting for completion, then refreshing preview automatically.`);
        await loadJobs();
        void followCoolnessJobAndPreview(jobId);
      }}

      async function runSnapshotGeneration() {{
        const topCoolness = Math.max(1, Number.parseInt(String(snapshotTopCoolnessNumberEl.value || '100'), 10) || 100);
        const maxDistanceLy = Math.max(1, Number.parseFloat(String(snapshotMaxDistanceLyNumberEl.value || '1000')) || 1000);
        const minStarCount = Math.max(0, Number.parseInt(String(snapshotMinStarCountNumberEl.value || '0'), 10) || 0);
        const minPlanetCount = Math.max(0, Number.parseInt(String(snapshotMinPlanetCountNumberEl.value || '0'), 10) || 0);
        const minCoolnessScore = Math.max(0, Number.parseFloat(String(snapshotMinCoolnessScoreNumberEl.value || '0')) || 0);
        snapshotTopCoolnessNumberEl.value = String(topCoolness);
        snapshotMaxDistanceLyNumberEl.value = String(Math.round(maxDistanceLy));
        snapshotMinStarCountNumberEl.value = String(minStarCount);
        snapshotMinPlanetCountNumberEl.value = String(minPlanetCount);
        snapshotMinCoolnessScoreNumberEl.value = minCoolnessScore.toFixed(1);
        setSnapshotRunStatus('running', 'starting snapshot job');
        const params = {{
          top_coolness: topCoolness,
          view_type: 'system_card',
          max_dist_ly: maxDistanceLy,
          min_star_count: minStarCount,
          min_planet_count: minPlanetCount,
          min_coolness_score: minCoolnessScore,
          force: !!(snapshotForceRegenerateEl && snapshotForceRegenerateEl.checked),
        }};
        const runResult = await runAction('generate_snapshots', params);
        if (!runResult || !runResult.ok) {{
          setSnapshotRunStatus('error', 'snapshot job failed to start');
          return;
        }}
        const job = (runResult.data && runResult.data.job) || {{}};
        const jobId = String(job.job_id || '');
        if (!jobId) {{
          setSnapshotRunStatus('queued', 'job created; check Activity > Jobs');
          return;
        }}
        setSnapshotRunStatus('queued', `job ${{jobId}}`);
        await loadJobs();
        void followActionJob(jobId, snapshotRunStatusEl, 'generate_snapshots');
      }}

      async function saveCoolnessProfileInternal(options = undefined) {{
        const forActivation = !!(options && options.forActivation);
        const suppressSavedNotice = !!(options && options.suppressSavedNotice);
        setRunStatus('saving', 'persisting immutable profile');
        if (!Array.isArray(coolnessProfiles) || !coolnessProfiles.length) {{
          await loadCoolnessState({{ preserveEditor: true }});
        }}
	        const fallbackProfileId = String((activeCoolnessPointer || {{}}).profile_id || (activeCoolnessProfile || {{}}).profile_id || 'default');
	        const normalized = normalizeProfileFields();
	        const profileId = normalized.profileId || fallbackProfileId;
	        const profileVersionRaw = normalized.profileVersion;
	        let profileVersion = profileVersionRaw;
	        coolProfileIdEl.value = profileId;
	        if (normalized.originalId && normalized.profileId && normalized.originalId !== normalized.profileId) {{
	          setPreviewNotice(`Normalized Profile ID for storage: '${{normalized.originalId}}' -> '${{normalized.profileId}}'.`);
	        }}
	        if (normalized.originalVersion && normalized.profileVersion && normalized.originalVersion !== normalized.profileVersion) {{
	          setPreviewNotice(`Normalized Profile version for storage: '${{normalized.originalVersion}}' -> '${{normalized.profileVersion}}'.`);
	        }}
	        if (!profileId) {{
	          setRunStatus('error', 'save failed');
	          setPreviewNotice('Save failed: profile ID is required.');
	          return;
	        }}
	        if (!profileVersion) {{
	          profileVersion = nextUnusedProfileVersion(profileId, '');
	          coolProfileVersionEl.value = profileVersion;
	          setPreviewNotice(`Auto-selected next profile version: ${{profileId}}@${{profileVersion}}.`);
	        }}

        const existingProfile = findCoolnessProfile(profileId, profileVersion);
        if (existingProfile && !weightsEqual(currentCoolnessWeights, existingProfile.weights || {{}})) {{
          const bumped = nextUnusedProfileVersion(profileId, suggestNextProfileVersion(profileVersion));
          profileVersion = bumped;
          coolProfileVersionEl.value = profileVersion;
          setPreviewNotice(
            `Auto-bumped profile version to ${{profileVersion}} because ${{profileId}}@${{String(existingProfile.profile_version || '')}} already exists with different immutable weights.`
          );
        }}

        const saveResult = await runAction('save_coolness_profile', {{
          profile_id: profileId,
          profile_version: profileVersion,
          weights_json: JSON.stringify(currentCoolnessWeights),
          notes: forActivation ? 'saved+activated from admin coolness tuning' : 'saved from admin coolness tuning',
        }});
        if (!saveResult || !saveResult.ok) {{
          setRunStatus('error', 'save failed');
          setPreviewNotice('Save failed. Pick a new profile version and retry.');
          return {{ ok: false, reason: 'save_start_failed' }};
        }}
        const job = (saveResult.data && saveResult.data.job) || {{}};
        const jobId = String(job.job_id || '');
        if (!jobId) {{
          setRunStatus('queued', 'save job created');
          if (forActivation) {{
            setPreviewNotice('Save job queued but no job ID was returned; cannot continue with activation automatically.');
            return {{ ok: false, reason: 'save_job_id_missing' }};
          }}
          setPreviewNotice('Save job queued. Check Activity > Jobs for completion.');
          return {{ ok: true, profileId, profileVersion, queued: true }};
        }}
        setRunStatus('queued', `save job ${{jobId}}`);
        setPreviewNotice(`Save job ${{jobId}} queued. Waiting for completion...`);
        await loadJobs();
	        return await followSaveProfileJob(jobId, profileId, profileVersion, {{
            suppressSuccessNotice: suppressSavedNotice,
          }});
	      }}

      async function saveCoolnessProfile() {{
        await saveCoolnessProfileInternal({{
          forActivation: false,
          suppressSavedNotice: false,
        }});
	      }}

	      async function activateCoolnessProfile() {{
	        setRunStatus('activating', 'saving profile before activation');
	        const saveOutcome = await saveCoolnessProfileInternal({{
            forActivation: true,
            suppressSavedNotice: true,
          }});
	        if (!saveOutcome || !saveOutcome.ok) {{
	          return;
	        }}
	        const profileId = String(saveOutcome.profileId || '');
	        const profileVersion = String(saveOutcome.profileVersion || '');
	        const profile = findCoolnessProfile(profileId, profileVersion);
	        if (!profile) {{
	          setRunStatus('error', 'activate failed');
	          setPreviewNotice(`Activate failed: saved profile ${{profileId}}@${{profileVersion}} is not visible yet. Refresh and retry.`);
	          return;
	        }}
	        coolProfileIdEl.value = profileId;
	        coolProfileVersionEl.value = profileVersion;
	        const result = await runAction('apply_coolness_profile', {{
	          profile_id: profileId,
	          profile_version: profileVersion,
	          reason: 'activated from admin coolness tuning',
	        }});
	        if (!result || !result.ok) {{
	          setRunStatus('error', 'activate failed');
	          setPreviewNotice('Activate failed. Check job logs in Activity and retry.');
	          return;
	        }}
	        const job = (result.data && result.data.job) || {{}};
	        const jobId = String(job.job_id || '');
	        if (!jobId) {{
	          setRunStatus('queued', 'activate job created');
	          setPreviewNotice('Activate job queued. Check Activity > Jobs for completion.');
	          return;
	        }}
	        setRunStatus('queued', `activate job ${{jobId}}`);
	        setPreviewNotice(`Activate job ${{jobId}} queued. Waiting for completion...`);
	        await loadJobs();
	        await followActivateProfileJob(jobId, profileId, profileVersion);
	      }}

	      async function callStatus() {{
        const {{ data }} = await fetchJson(`${{ADMIN_API_BASE}}/status`, {{ credentials: 'include' }});
        out.textContent = JSON.stringify(data, null, 2);
      }}

      function formatInt(value) {{
        const n = Number(value);
        if (!Number.isFinite(n)) return '0';
        return Math.round(n).toLocaleString('en-US');
      }}

      function formatFloat(value, digits = 2) {{
        const n = Number(value);
        if (!Number.isFinite(n)) return '0';
        return n.toLocaleString('en-US', {{ minimumFractionDigits: 0, maximumFractionDigits: digits }});
      }}

      function formatPct(value, digits = 2) {{
        const n = Number(value);
        if (!Number.isFinite(n)) return '0%';
        return `${{formatFloat(n, digits)}}%`;
      }}

      function formatBytes(value) {{
        const n = Number(value);
        if (!Number.isFinite(n) || n <= 0) return '0 B';
        const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
        let size = n;
        let idx = 0;
        while (size >= 1024 && idx < units.length - 1) {{
          size /= 1024;
          idx += 1;
        }}
        const digits = idx <= 1 ? 0 : 2;
        return `${{size.toLocaleString('en-US', {{ minimumFractionDigits: 0, maximumFractionDigits: digits }})}} ${{units[idx]}}`;
      }}

      function clampPct(value) {{
        const n = Number(value);
        if (!Number.isFinite(n)) return 0;
        if (n < 0) return 0;
        if (n > 100) return 100;
        return n;
      }}

      function pctFromPart(part, whole) {{
        const nPart = Number(part);
        const nWhole = Number(whole);
        if (!Number.isFinite(nPart) || !Number.isFinite(nWhole) || nWhole <= 0) return 0;
        return clampPct((nPart / nWhole) * 100.0);
      }}

      function renderMetricRows(containerEl, rows) {{
        if (!containerEl) return;
        containerEl.innerHTML = '';
        (rows || []).forEach((row) => {{
          const wrap = document.createElement('div');
          wrap.className = 'metric-row';

          const keyEl = document.createElement('div');
          keyEl.className = 'metric-k';
          keyEl.textContent = String(row.key || '');
          wrap.appendChild(keyEl);

          const valueEl = document.createElement('div');
          valueEl.className = 'metric-v';
          valueEl.textContent = String(row.value || '');
          wrap.appendChild(valueEl);

          if (row.note) {{
            const noteEl = document.createElement('div');
            noteEl.className = 'metric-note';
            noteEl.textContent = String(row.note);
            wrap.appendChild(noteEl);
          }}
          containerEl.appendChild(wrap);
        }});
      }}

      function renderUsageBars(containerEl, rows) {{
        if (!containerEl) return;
        containerEl.innerHTML = '';
        (rows || []).forEach((row) => {{
          const pct = clampPct(row.pct);
          const severity = pct >= 90 ? ' err' : (pct >= 75 ? ' warn' : '');

          const barRow = document.createElement('div');
          barRow.className = 'bar-row';

          const label = document.createElement('div');
          label.textContent = String(row.label || '');
          barRow.appendChild(label);

          const track = document.createElement('div');
          track.className = 'bar-track';
          const fill = document.createElement('div');
          fill.className = `bar-fill${{severity}}`;
          fill.style.width = `${{pct}}%`;
          track.appendChild(fill);
          barRow.appendChild(track);

          const value = document.createElement('div');
          value.className = 'small';
          value.textContent = String(row.value || formatPct(pct));
          barRow.appendChild(value);

          containerEl.appendChild(barRow);
        }});
      }}

      function renderKeyValueRows(tbodyEl, rows) {{
        if (!tbodyEl) return;
        tbodyEl.innerHTML = '';
        (rows || []).forEach((row) => {{
          const tr = document.createElement('tr');
          const tdK = document.createElement('td');
          tdK.textContent = String(row.key || '');
          const tdV = document.createElement('td');
          tdV.textContent = String(row.value || '');
          tr.appendChild(tdK);
          tr.appendChild(tdV);
          tbodyEl.appendChild(tr);
        }});
      }}

      function renderDatasetStatus(data) {{
        if (datasetStatusRawEl) datasetStatusRawEl.textContent = JSON.stringify(data, null, 2);
        const counts = data.dataset_counts || {{}};
        const sizes = data.sizes_bytes || {{}};
        const disk = data.disk || {{}};
        const host = data.host_runtime || {{}};
        const api = data.api_process_runtime || {{}};
        const duckdb = data.duckdb_runtime || {{}};
        const slice = data.slice_metrics || {{}};
        const cache = data.cache || {{}};
        const bottlenecks = data.bottleneck_hints || {{}};
        const breakdowns = data.breakdowns || {{}};
        const determinism = data.determinism || {{}};
        const policyInputStars = Number(slice.policy_input_stars) || 0;
        const policySlicedOutStars = Number(slice.policy_sliced_out_stars) || 0;
        const policySlicedOutStarsPct = Number(slice.policy_sliced_out_stars_pct) || 0;
        const slicedOutRows = Number(slice.sliced_out_rows) || 0;
        const slicedOutPct = Number(slice.sliced_out_pct) || 0;
        const sliceMetricsMatch = (
          policyInputStars > 0
          && policySlicedOutStars === slicedOutRows
          && Math.abs(policySlicedOutStarsPct - slicedOutPct) < 0.0001
        );

        datasetStatusMetaEl.textContent = `build=${{data.build_id || 'unknown'}} | generated=${{data.generated_at_utc || ''}} | cache=${{cache.hit ? 'hit' : 'miss'}} age=${{formatFloat(cache.age_s || 0, 3)}}s`;

        const hostMemTotal = Number(host.mem_total_bytes) || 0;
        const hostMemAvailable = Number(host.mem_available_bytes) || 0;
        const hostMemUsed = Math.max(hostMemTotal - hostMemAvailable, 0);
        const apiRss = Number(api.rss_bytes) || 0;
        const apiPeakRss = Number(api.peak_rss_bytes) || 0;
        const duckMemUsage = Number(duckdb.memory_usage_bytes) || 0;
        const duckMemLimit = Number(duckdb.memory_limit_bytes) || 0;

        const kpis = [
          {{ key: 'Stars', value: formatInt(counts.stars) }},
          {{ key: 'Systems', value: formatInt(counts.systems) }},
          {{ key: 'Planets', value: formatInt(counts.planets) }},
          {{ key: 'Arm Components', value: formatInt(counts.arm_component_entities) }},
          {{ key: 'Arm Orbit Edges', value: formatInt(counts.arm_orbit_edges) }},
          {{ key: 'VSX Overlay Rows', value: formatInt(counts.arm_vsx_variability) }},
          {{ key: 'High Variability', value: formatInt(counts.arm_variability_high) }},
          {{ key: 'Ultracool Overlay Rows', value: formatInt(counts.arm_ultracoolsheet_objects) }},
          {{ key: 'Multi-Star Systems', value: formatInt(counts.multi_star_systems) }},
          {{ key: 'Exoplanets', value: formatInt(counts.exoplanets_total) }},
          {{ key: 'Hab Zone Candidates', value: formatInt(counts.exoplanets_candidate_habitable) }},
          {{ key: 'Backbone Input', value: formatInt(slice.input_backbone_rows) }},
          {{ key: 'Sliced Out', value: `${{formatInt(slicedOutRows)}} (${{formatPct(slicedOutPct)}})` }},
          {{ key: 'Core DB', value: formatBytes(sizes.core_db) }},
          {{ key: 'State Dir', value: formatBytes(sizes.state_total) }},
          {{ key: 'API RSS', value: formatBytes(apiRss) }},
          {{ key: 'API Peak RSS', value: formatBytes(apiPeakRss) }},
          {{ key: 'Host Mem Available', value: formatBytes(hostMemAvailable) }},
          {{ key: '/data Used', value: `${{formatPct(disk.used_pct)}} (${{formatBytes(disk.used_bytes)}})` }},
        ];
        if (!sliceMetricsMatch && policyInputStars > 0) {{
          kpis.splice(8, 0, {{
            key: 'Policy Slice Out',
            value: `${{formatInt(policySlicedOutStars)}} (${{formatPct(policySlicedOutStarsPct)}})`,
          }});
        }}
        datasetKpisEl.innerHTML = '';
        kpis.forEach((item) => {{
          const card = document.createElement('div');
          card.className = 'kpi';
          const k = document.createElement('div');
          k.className = 'k';
          k.textContent = item.key;
          const v = document.createElement('div');
          v.className = 'v';
          v.textContent = item.value;
          card.appendChild(k);
          card.appendChild(v);
          datasetKpisEl.appendChild(card);
        }});

        renderMetricRows(datasetStorageEl, [
          {{ key: 'Project footprint', value: formatBytes(sizes.project_total) }},
          {{ key: 'State footprint', value: formatBytes(sizes.state_total) }},
          {{ key: 'Served build footprint', value: formatBytes(sizes.build_total), note: String((data.paths || {{}}).build_dir || '') }},
          {{ key: 'Raw / cooked / out', value: `${{formatBytes(sizes.raw_total)}} / ${{formatBytes(sizes.cooked_total)}} / ${{formatBytes(sizes.out_total)}}` }},
          {{ key: 'Reports / served / parquet', value: `${{formatBytes(sizes.reports_total)}} / ${{formatBytes(sizes.served_total)}} / ${{formatBytes(sizes.parquet_total)}}` }},
          {{ key: 'DB files', value: `core ${{formatBytes(sizes.core_db)}} | arm ${{formatBytes(sizes.arm_db)}} | disc ${{formatBytes(sizes.disc_db)}} | admin ${{formatBytes(sizes.admin_db)}}` }},
          {{ key: '/data partition', value: `${{formatBytes(disk.used_bytes)}} used of ${{formatBytes(disk.total_bytes)}}`, note: `${{formatBytes(disk.free_bytes)}} free (${{formatPct(disk.used_pct)}} used)` }},
        ]);

        const timingEntries = Object.entries(data.timings_ms || {{}})
          .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))
          .slice(0, 6)
          .map(([name, ms]) => `${{name}}=${{formatFloat(ms, 2)}}ms`);
        const loadLabel = `1m=${{formatFloat(host.loadavg_1m, 2)}} | 5m=${{formatFloat(host.loadavg_5m, 2)}} | 15m=${{formatFloat(host.loadavg_15m, 2)}}`;
        const bottleneckNotes = [
          bottlenecks.likely_memory_bound ? 'Memory-bound risk' : 'Memory headroom likely acceptable',
          bottlenecks.likely_io_bound ? 'IO-bound risk' : 'IO-bound risk low',
        ].join(' | ');
        renderMetricRows(datasetRuntimeEl, [
          {{ key: 'CPU', value: `${{formatInt(host.cpu_count)}} cores`, note: `load average: ${{loadLabel}}` }},
          {{ key: 'Host memory', value: `${{formatBytes(hostMemUsed)}} used of ${{formatBytes(hostMemTotal)}}`, note: `${{formatBytes(hostMemAvailable)}} available` }},
          {{ key: 'API process', value: `pid=${{api.pid || '?'}}, threads=${{formatInt(api.threads)}}`, note: `RSS=${{formatBytes(apiRss)}}, peak=${{formatBytes(apiPeakRss)}}, VM=${{formatBytes(api.vm_size_bytes)}}` }},
          {{ key: 'Process IO', value: `read=${{formatBytes(api.io_read_bytes)}}, write=${{formatBytes(api.io_write_bytes)}}` }},
          {{ key: 'DuckDB runtime', value: `db=${{formatBytes(duckdb.database_size_bytes)}}, wal=${{formatBytes(duckdb.wal_size_bytes)}}`, note: `memory=${{formatBytes(duckMemUsage)}} / ${{formatBytes(duckMemLimit)}}` }},
          {{ key: 'Bottleneck hints', value: bottleneckNotes }},
          {{ key: 'Top status query timings', value: timingEntries.length ? timingEntries.join(' | ') : 'n/a' }},
        ]);

        const usageRows = [
          {{
            label: '/data used',
            pct: disk.used_pct,
            value: `${{formatPct(disk.used_pct)}} (${{formatBytes(disk.used_bytes)}} / ${{formatBytes(disk.total_bytes)}})`,
          }},
          {{
            label: 'Host RAM used',
            pct: pctFromPart(hostMemUsed, hostMemTotal),
            value: `${{formatPct(pctFromPart(hostMemUsed, hostMemTotal))}} (${{formatBytes(hostMemUsed)}} / ${{formatBytes(hostMemTotal)}})`,
          }},
          {{
            label: 'API RSS / host',
            pct: pctFromPart(apiRss, hostMemTotal),
            value: `${{formatPct(pctFromPart(apiRss, hostMemTotal))}} (${{formatBytes(apiRss)}} / ${{formatBytes(hostMemTotal)}})`,
          }},
          {{
            label: 'API peak / host',
            pct: pctFromPart(apiPeakRss, hostMemTotal),
            value: `${{formatPct(pctFromPart(apiPeakRss, hostMemTotal))}} (${{formatBytes(apiPeakRss)}} / ${{formatBytes(hostMemTotal)}})`,
          }},
          {{
            label: 'DuckDB memory',
            pct: pctFromPart(duckMemUsage, duckMemLimit),
            value: `${{formatPct(pctFromPart(duckMemUsage, duckMemLimit))}} (${{formatBytes(duckMemUsage)}} / ${{formatBytes(duckMemLimit)}})`,
          }},
          {{
            label: 'Slice out',
            pct: slicedOutPct,
            value: `${{formatPct(slicedOutPct)}} (${{formatInt(slicedOutRows)}} rows)`,
          }},
        ];
        if (!sliceMetricsMatch && policyInputStars > 0) {{
          usageRows.push({{
            label: 'Policy out',
            pct: policySlicedOutStarsPct,
            value: `${{formatPct(policySlicedOutStarsPct)}} (${{formatInt(policySlicedOutStars)}} / ${{formatInt(policyInputStars || counts.stars)}} stars)`,
          }});
        }}
        renderUsageBars(datasetUsageBarsEl, usageRows);

        const detStatusRaw = String(determinism.status || '');
        const detStatusMap = {{
          match: 'match',
          mismatch: 'mismatch',
          no_baseline: 'no baseline',
          missing_current_report: 'missing report',
        }};
        const detStatusLabel = detStatusMap[detStatusRaw] || (detStatusRaw || 'unknown');
        const detComparison = determinism.comparison || {{}};
        const detMismatches = Array.isArray(detComparison.mismatches) ? detComparison.mismatches : [];
        renderMetricRows(datasetDeterminismEl, [
          {{
            key: 'Status',
            value: detStatusLabel,
            note: detStatusRaw === 'mismatch'
              ? `${{formatInt(detMismatches.length)}} mismatched fingerprint fields`
              : (detStatusRaw === 'match' ? 'fingerprints match baseline build' : ''),
          }},
          {{
            key: 'Current build',
            value: String(determinism.current_build_id || data.build_id || 'unknown'),
            note: String(determinism.current_report_exists ? 'determinism_report.json present' : 'determinism_report.json not found'),
          }},
          {{
            key: 'Baseline build',
            value: String(determinism.baseline_build_id || 'n/a'),
            note: `comparable baselines=${{formatInt(determinism.comparable_baselines)}}`,
          }},
          {{
            key: 'Input fingerprint',
            value: String(determinism.source_inputs_fingerprint || 'n/a'),
          }},
          {{
            key: 'Transform / layer',
            value: `${{String(determinism.transform_version || 'n/a')}} / ${{String(determinism.build_layer || 'n/a')}}`,
            note: `slice=${{String(determinism.slice_profile_id || 'n/a')}}@${{String(determinism.slice_profile_version || 'n/a')}}`,
          }},
        ]);
        const detTables = (detComparison.tables && typeof detComparison.tables === 'object')
          ? detComparison.tables
          : {{}};
        renderKeyValueRows(datasetDeterminismRowsEl, [
          {{ key: 'stars', value: detTables.stars ? (detTables.stars.match ? 'match' : 'mismatch') : 'n/a' }},
          {{ key: 'systems', value: detTables.systems ? (detTables.systems.match ? 'match' : 'mismatch') : 'n/a' }},
          {{ key: 'planets', value: detTables.planets ? (detTables.planets.match ? 'match' : 'mismatch') : 'n/a' }},
        ]);

        const sourceRows = breakdowns.stars_by_source_catalog || [];
        datasetSourceRowsEl.innerHTML = '';
        sourceRows.forEach((row) => {{
          const tr = document.createElement('tr');
          const sourceTd = document.createElement('td');
          sourceTd.textContent = String(row.source_catalog || '?');
          const countTd = document.createElement('td');
          countTd.textContent = formatInt(row.star_count);
          tr.appendChild(sourceTd);
          tr.appendChild(countTd);
          datasetSourceRowsEl.appendChild(tr);
        }});
        renderPieChart(
          datasetSourcePieEl,
          sourceRows.map((row) => ({{
            label: String(row.source_catalog || '?'),
            value: toNumber(row.star_count, 0),
          }})),
          toNumber(counts.stars, 0),
          'Star source share'
        );

        const spectralRows = (breakdowns.stars_by_spectral_class || []).slice(0, 16);
        datasetSpectralRowsEl.innerHTML = '';
        spectralRows.forEach((row) => {{
          const tr = document.createElement('tr');
          const classTd = document.createElement('td');
          classTd.textContent = String(row.spectral_class || '?');
          const countTd = document.createElement('td');
          countTd.textContent = formatInt(row.star_count);
          const pctTd = document.createElement('td');
          pctTd.textContent = formatPct(row.pct_of_stars);
          tr.appendChild(classTd);
          tr.appendChild(countTd);
          tr.appendChild(pctTd);
          datasetSpectralRowsEl.appendChild(tr);
        }});
        renderPieChart(
          datasetSpectralPieEl,
          spectralRows.map((row) => ({{
            label: String(row.spectral_class || '?'),
            value: toNumber(row.star_count, 0),
            color: spectralPieColor(row.spectral_class),
          }})),
          toNumber(counts.stars, 0),
          'Spectral share'
        );

        const spectralStandard = breakdowns.spectral_class_standard_counts || {{}};
        renderKeyValueRows(datasetSpectralStandardRowsEl, [
          {{ key: 'O', value: formatInt(spectralStandard.O) }},
          {{ key: 'B', value: formatInt(spectralStandard.B) }},
          {{ key: 'A', value: formatInt(spectralStandard.A) }},
          {{ key: 'F', value: formatInt(spectralStandard.F) }},
          {{ key: 'G', value: formatInt(spectralStandard.G) }},
          {{ key: 'K', value: formatInt(spectralStandard.K) }},
          {{ key: 'M', value: formatInt(spectralStandard.M) }},
          {{ key: 'L', value: formatInt(spectralStandard.L) }},
          {{ key: 'D', value: formatInt(spectralStandard.D) }},
          {{ key: 'T', value: formatInt(spectralStandard.T) }},
          {{ key: 'Y', value: formatInt(spectralStandard.Y) }},
          {{ key: 'unknown', value: formatInt(spectralStandard.unknown) }},
        ]);

        const compact = breakdowns.compact_object_counts || {{}};
        renderKeyValueRows(datasetCompactRowsEl, [
          {{ key: 'white_dwarf', value: formatInt(compact.white_dwarf) }},
          {{ key: 'neutron_star', value: formatInt(compact.neutron_star) }},
          {{ key: 'pulsar', value: formatInt(compact.pulsar) }},
          {{ key: 'black_hole', value: formatInt(compact.black_hole) }},
          {{ key: 'all_compact', value: formatInt(compact.compact_total) }},
        ]);

        const sysMult = breakdowns.system_multiplicity_evidence || {{}};
        const starMult = breakdowns.star_multiplicity_evidence || {{}};
        renderKeyValueRows(datasetSystemMultRowsEl, [
          {{ key: 'none', value: formatInt(sysMult.none) }},
          {{ key: 'nss_only', value: formatInt(sysMult.nss_only) }},
          {{ key: 'wds_only', value: formatInt(sysMult.wds_only) }},
          {{ key: 'msc_only', value: formatInt(sysMult.msc_only) }},
          {{ key: 'sbx_only', value: formatInt(sysMult.sbx_only) }},
          {{ key: 'nss_wds', value: formatInt(sysMult.nss_wds) }},
          {{ key: 'nss_msc', value: formatInt(sysMult.nss_msc) }},
          {{ key: 'nss_sbx', value: formatInt(sysMult.nss_sbx) }},
          {{ key: 'wds_msc', value: formatInt(sysMult.wds_msc) }},
          {{ key: 'wds_sbx', value: formatInt(sysMult.wds_sbx) }},
          {{ key: 'msc_sbx', value: formatInt(sysMult.msc_sbx) }},
          {{ key: 'nss_wds_msc', value: formatInt(sysMult.nss_wds_msc) }},
          {{ key: 'nss_wds_msc_sbx', value: formatInt(sysMult.nss_wds_msc_sbx) }},
        ]);
        renderPieChart(
          datasetSystemMultPieEl,
          [
            {{ label: 'none', value: toNumber(sysMult.none, 0) }},
            {{ label: 'nss_only', value: toNumber(sysMult.nss_only, 0) }},
            {{ label: 'wds_only', value: toNumber(sysMult.wds_only, 0) }},
            {{ label: 'msc_only', value: toNumber(sysMult.msc_only, 0) }},
            {{ label: 'sbx_only', value: toNumber(sysMult.sbx_only, 0) }},
            {{ label: 'nss_wds', value: toNumber(sysMult.nss_wds, 0) }},
            {{ label: 'nss_msc', value: toNumber(sysMult.nss_msc, 0) }},
            {{ label: 'nss_sbx', value: toNumber(sysMult.nss_sbx, 0) }},
            {{ label: 'wds_msc', value: toNumber(sysMult.wds_msc, 0) }},
            {{ label: 'wds_sbx', value: toNumber(sysMult.wds_sbx, 0) }},
            {{ label: 'msc_sbx', value: toNumber(sysMult.msc_sbx, 0) }},
            {{ label: 'nss_wds_msc', value: toNumber(sysMult.nss_wds_msc, 0) }},
            {{ label: 'nss_wds_msc_sbx', value: toNumber(sysMult.nss_wds_msc_sbx, 0) }},
          ],
          toNumber(counts.systems, 0),
          'System evidence'
        );
        renderKeyValueRows(datasetStarMultRowsEl, [
          {{ key: 'none', value: formatInt(starMult.none) }},
          {{ key: 'nss_only', value: formatInt(starMult.nss_only) }},
          {{ key: 'wds_only', value: formatInt(starMult.wds_only) }},
          {{ key: 'msc_only', value: formatInt(starMult.msc_only) }},
          {{ key: 'sbx_only', value: formatInt(starMult.sbx_only) }},
          {{ key: 'nss_wds', value: formatInt(starMult.nss_wds) }},
          {{ key: 'nss_msc', value: formatInt(starMult.nss_msc) }},
          {{ key: 'nss_sbx', value: formatInt(starMult.nss_sbx) }},
          {{ key: 'wds_msc', value: formatInt(starMult.wds_msc) }},
          {{ key: 'wds_sbx', value: formatInt(starMult.wds_sbx) }},
          {{ key: 'msc_sbx', value: formatInt(starMult.msc_sbx) }},
          {{ key: 'nss_wds_msc', value: formatInt(starMult.nss_wds_msc) }},
          {{ key: 'nss_wds_msc_sbx', value: formatInt(starMult.nss_wds_msc_sbx) }},
        ]);
        renderPieChart(
          datasetStarMultPieEl,
          [
            {{ label: 'none', value: toNumber(starMult.none, 0) }},
            {{ label: 'nss_only', value: toNumber(starMult.nss_only, 0) }},
            {{ label: 'wds_only', value: toNumber(starMult.wds_only, 0) }},
            {{ label: 'msc_only', value: toNumber(starMult.msc_only, 0) }},
            {{ label: 'sbx_only', value: toNumber(starMult.sbx_only, 0) }},
            {{ label: 'nss_wds', value: toNumber(starMult.nss_wds, 0) }},
            {{ label: 'nss_msc', value: toNumber(starMult.nss_msc, 0) }},
            {{ label: 'nss_sbx', value: toNumber(starMult.nss_sbx, 0) }},
            {{ label: 'wds_msc', value: toNumber(starMult.wds_msc, 0) }},
            {{ label: 'wds_sbx', value: toNumber(starMult.wds_sbx, 0) }},
            {{ label: 'msc_sbx', value: toNumber(starMult.msc_sbx, 0) }},
            {{ label: 'nss_wds_msc', value: toNumber(starMult.nss_wds_msc, 0) }},
            {{ label: 'nss_wds_msc_sbx', value: toNumber(starMult.nss_wds_msc_sbx, 0) }},
          ],
          toNumber(counts.stars, 0),
          'Star evidence'
        );

        const catalogContribution = breakdowns.catalog_contribution_report || {{}};
        const catalogContributionRows = Array.isArray(catalogContribution.catalog_contributions)
          ? catalogContribution.catalog_contributions.slice()
          : [];
        catalogContributionRows.sort((a, b) => {{
          const scoreDelta = toNumber(b.utility_score, 0) - toNumber(a.utility_score, 0);
          if (scoreDelta !== 0) return scoreDelta;
          const directDelta = toNumber(b.direct_rows, 0) - toNumber(a.direct_rows, 0);
          if (directDelta !== 0) return directDelta;
          return String(a.catalog || '').localeCompare(String(b.catalog || ''));
        }});

        if (datasetCatalogContributionBarsEl) {{
          const topUtilityRows = catalogContributionRows
            .slice(0, 10)
            .map((row) => ({{
              label: `${{String(row.catalog || '?')}} (${{String(row.domain || '?')}})`,
              value: toNumber(row.utility_score, 0),
            }}));
          renderBarList(datasetCatalogContributionBarsEl, topUtilityRows, 100);
        }}

        if (datasetCatalogContributionRowsEl) {{
          datasetCatalogContributionRowsEl.innerHTML = '';
          if (!catalogContributionRows.length) {{
            const tr = document.createElement('tr');
            const td = document.createElement('td');
            td.colSpan = 7;
            td.className = 'small muted';
            td.textContent = 'No catalog contribution report found for this build.';
            tr.appendChild(td);
            datasetCatalogContributionRowsEl.appendChild(tr);
          }} else {{
            catalogContributionRows.slice(0, 30).forEach((row) => {{
              const tr = document.createElement('tr');
              const cells = [
                String(row.catalog || '?'),
                String(row.domain || '?'),
                (row.input_rows === null || row.input_rows === undefined) ? 'n/a' : formatInt(row.input_rows),
                formatInt(row.direct_rows),
                formatInt(row.evidence_rows),
                formatInt(row.linked_rows),
                `${{String(row.utility_tier || 'n/a')}} (${{formatFloat(row.utility_score, 2)}})`,
              ];
              cells.forEach((value) => {{
                const td = document.createElement('td');
                td.textContent = value;
                tr.appendChild(td);
              }});
              datasetCatalogContributionRowsEl.appendChild(tr);
            }});
          }}
        }}

        if (datasetCatalogOverlapRowsEl) {{
          datasetCatalogOverlapRowsEl.innerHTML = '';
          const overlapStarRows = (((catalogContribution.overlaps || {{}}).star_evidence || {{}}).pairwise || [])
            .map((row) => ({{ ...row, scope: 'stars' }}));
          const overlapSystemRows = (((catalogContribution.overlaps || {{}}).system_evidence || {{}}).pairwise || [])
            .map((row) => ({{ ...row, scope: 'systems' }}));
          const overlapRows = overlapStarRows.concat(overlapSystemRows);
          if (!overlapRows.length) {{
            const tr = document.createElement('tr');
            const td = document.createElement('td');
            td.colSpan = 5;
            td.className = 'small muted';
            td.textContent = 'No overlap metrics available.';
            tr.appendChild(td);
            datasetCatalogOverlapRowsEl.appendChild(tr);
          }} else {{
            overlapRows.forEach((row) => {{
              const tr = document.createElement('tr');
              const values = [
                String(row.scope || '?'),
                `${{String(row.left_catalog || '?')}} ∩ ${{String(row.right_catalog || '?')}}`,
                formatInt(row.intersection_count),
                formatPct(row.jaccard_pct),
                formatPct(row.intersection_pct_of_scope),
              ];
              values.forEach((value) => {{
                const td = document.createElement('td');
                td.textContent = value;
                tr.appendChild(td);
              }});
              datasetCatalogOverlapRowsEl.appendChild(tr);
            }});
          }}
        }}

        if (datasetPipelineRowsEl) {{
          datasetPipelineRowsEl.innerHTML = '';
          const pipeline = breakdowns.catalog_pipeline_report || {{}};
          const stages = pipeline.stages || {{}};
          ['download', 'cook', 'ingest'].forEach((stageName) => {{
            const stageData = stages[stageName] || null;
            const tr = document.createElement('tr');
            const stageTd = document.createElement('td');
            stageTd.textContent = stageName;
            tr.appendChild(stageTd);
            const updatedTd = document.createElement('td');
            updatedTd.textContent = stageData ? String(stageData.updated_at || 'n/a') : 'n/a';
            tr.appendChild(updatedTd);
            const detailTd = document.createElement('td');
            if (!stageData) {{
              detailTd.textContent = 'no stage report';
            }} else if (stageName === 'download') {{
              detailTd.textContent = `${{formatInt(stageData.source_count)}} sources | manifests=${{formatInt(stageData.manifest_files_count)}}`;
            }} else if (stageName === 'cook') {{
              detailTd.textContent = `${{formatInt(stageData.existing_catalog_count)}}/${{formatInt(stageData.catalog_count)}} cooked files`;
            }} else {{
              detailTd.textContent = `build=${{String(stageData.build_id || 'n/a')}} | entries=${{formatInt(stageData.catalog_contribution_entries)}}`;
            }}
            tr.appendChild(detailTd);
            datasetPipelineRowsEl.appendChild(tr);
          }});
        }}

        const exotic = breakdowns.exotic_star_counts || {{}};
        const summaryLines = [
          `Build: ${{data.build_id || 'unknown'}}`,
          `Total rows: ${{formatInt(counts.rows_total)}} (systems=${{formatInt(counts.systems)}}, stars=${{formatInt(counts.stars)}}, planets=${{formatInt(counts.planets)}})`,
          `Multiplicity systems: ${{formatInt(counts.multi_star_systems)}} multi / ${{formatInt(counts.single_star_systems)}} single`,
          `Arm graph: components=${{formatInt(counts.arm_component_entities)}}, hierarchy edges=${{formatInt(counts.arm_hierarchy_edges)}}, orbit edges=${{formatInt(counts.arm_orbit_edges)}}`,
          `Arm overlays: VSX rows=${{formatInt(counts.arm_vsx_variability)}}, variability summary=${{formatInt(counts.arm_variability_summary)}}, high variability=${{formatInt(counts.arm_variability_high)}}, ultracool rows=${{formatInt(counts.arm_ultracoolsheet_objects)}}`,
          `Input vs sliced: ${{formatInt(slice.input_backbone_rows)}} input, ${{formatInt(slicedOutRows)}} sliced out (${{formatPct(slicedOutPct)}})`,
          `Storage: core=${{formatBytes(sizes.core_db)}}, arm=${{formatBytes(sizes.arm_db)}}, disc=${{formatBytes(sizes.disc_db)}}, admin=${{formatBytes(sizes.admin_db)}}, state=${{formatBytes(sizes.state_total)}}`,
          `Memory: host used=${{formatBytes(hostMemUsed)}} / ${{formatBytes(hostMemTotal)}}, API rss=${{formatBytes(apiRss)}}, API peak=${{formatBytes(apiPeakRss)}}, duckdb=${{formatBytes(duckMemUsage)}} / ${{formatBytes(duckMemLimit)}}`,
          `Exoplanets: total=${{formatInt(counts.exoplanets_total)}}, temperate=${{formatInt(counts.exoplanets_temperate)}}, habitable candidates=${{formatInt(counts.exoplanets_candidate_habitable)}}`,
          `Exotic highlights: L/T/Y=${{formatInt(exotic.brown_dwarf_like_lty)}}, WD-like=${{formatInt(exotic.white_dwarf_like_d_prefix)}}, high proper motion=${{formatInt(exotic.high_proper_motion_ge_1000_mas_yr)}}`,
          `Determinism: status=${{detStatusLabel}}, baseline=${{String(determinism.baseline_build_id || 'n/a')}}, comparable=${{formatInt(determinism.comparable_baselines)}}`,
          `Catalog contribution rows: ${{formatInt(catalogContributionRows.length)}}`,
        ];
        if (policyInputStars > 0) {{
          if (sliceMetricsMatch) {{
            summaryLines.push(`Policy slice matches sliced-out totals (${{formatInt(policySlicedOutStars)}} stars, ${{formatPct(policySlicedOutStarsPct)}}).`);
          }} else {{
            summaryLines.push(`Policy slice: ${{formatInt(policyInputStars)}} input stars, ${{formatInt(policySlicedOutStars)}} sliced out (${{formatPct(policySlicedOutStarsPct)}}).`);
          }}
        }}
        if (datasetHumanSummaryEl) datasetHumanSummaryEl.textContent = summaryLines.join('\\n');
      }}

      async function callDatasetStatus(forceRefresh = false) {{
        const query = forceRefresh ? '?refresh=1' : '';
        const {{ res, data }} = await fetchJson(`${{ADMIN_API_BASE}}/status/dataset${{query}}`, {{ credentials: 'include' }});
        if (!res.ok) {{
          datasetStatusMetaEl.textContent = `error loading dataset status (${{res.status}})`;
          datasetStatusRawEl.textContent = JSON.stringify(data, null, 2);
          return;
        }}
        renderDatasetStatus(data || {{}});
      }}

      function parseOptionalNumberInput(inputEl) {{
        const raw = String((inputEl && inputEl.value) || '').trim();
        if (!raw) return null;
        const n = Number(raw);
        if (!Number.isFinite(n)) return null;
        return n;
      }}

      function readSlicePayload() {{
        const payload = {{
          max_distance_ly: parseOptionalNumberInput(sliceMaxDistanceLyEl),
          min_parallax_over_error: parseOptionalNumberInput(sliceMinParallaxOverErrorEl),
          max_parallax_error_mas: parseOptionalNumberInput(sliceMaxParallaxErrorMasEl),
          max_ruwe: parseOptionalNumberInput(sliceMaxRuweEl),
          require_spectral_class: !!(sliceRequireSpectralEl && sliceRequireSpectralEl.checked),
          require_color_index: !!(sliceRequireColorEl && sliceRequireColorEl.checked),
          allowed_spectral_classes: [],
        }};
        if (sliceSpectralFiltersEl) {{
          const checks = sliceSpectralFiltersEl.querySelectorAll('input[type=checkbox]');
          checks.forEach((el) => {{
            if (el.checked) payload.allowed_spectral_classes.push(String(el.value || '').trim().toUpperCase());
          }});
        }}
        return payload;
      }}

      function setSliceStatus(state, message) {{
        if (!sliceRunStatusEl) return;
        const st = String(state || 'idle');
        const msg = String(message || '');
        sliceRunStatusEl.textContent = msg ? `Status: ${{st}} | ${{msg}}` : `Status: ${{st}}`;
      }}

      function renderSlicePreview(data) {{
        if (slicePreviewRawEl) slicePreviewRawEl.textContent = JSON.stringify(data || {{}}, null, 2);
        const counts = (data && data.counts) || {{}};
        if (slicePreviewKpisEl) {{
          slicePreviewKpisEl.innerHTML = '';
          const starsSlicedOutPct = (Number(counts.stars_total) > 0)
            ? (Number(counts.stars_sliced_out || 0) / Number(counts.stars_total)) * 100.0
            : 0.0;
          [
            {{ key: 'Stars Retained', value: `${{formatInt(counts.stars_retained)}} / ${{formatInt(counts.stars_total)}}` }},
            {{ key: 'Stars Sliced Out', value: `${{formatInt(counts.stars_sliced_out)}} (${{formatPct(starsSlicedOutPct)}})` }},
            {{ key: 'Systems Retained', value: `${{formatInt(counts.systems_retained)}} / ${{formatInt(counts.systems_total)}}` }},
            {{ key: 'Planets Retained', value: `${{formatInt(counts.planets_retained)}} / ${{formatInt(counts.planets_total)}}` }},
            {{ key: 'Missing Spectral (retained)', value: formatInt(counts.retained_missing_spectral) }},
            {{ key: 'Missing Color (retained)', value: formatInt(counts.retained_missing_color) }},
          ].forEach((item) => {{
            const card = document.createElement('div');
            card.className = 'kpi';
            const k = document.createElement('div');
            k.className = 'k';
            k.textContent = item.key;
            const v = document.createElement('div');
            v.className = 'v';
            v.textContent = item.value;
            card.appendChild(k);
            card.appendChild(v);
            slicePreviewKpisEl.appendChild(card);
          }});
        }}
        if (slicePreviewSpectralRowsEl) {{
          slicePreviewSpectralRowsEl.innerHTML = '';
          const rows = (data && data.retained_spectral_breakdown) || [];
          rows.forEach((row) => {{
            const tr = document.createElement('tr');
            const cls = document.createElement('td');
            cls.textContent = String(row.spectral_class || 'UNKNOWN');
            const cnt = document.createElement('td');
            cnt.textContent = formatInt(row.star_count);
            tr.appendChild(cls);
            tr.appendChild(cnt);
            slicePreviewSpectralRowsEl.appendChild(tr);
          }});
        }}
        const warnings = (data && data.warnings) || [];
        if (warnings.length > 0) {{
          setSliceStatus('preview-warning', warnings.join(' | '));
        }} else {{
          setSliceStatus('preview-ready', 'slice preview loaded');
        }}
      }}

      async function callSlicePreview() {{
        const payload = readSlicePayload();
        setSliceStatus('preview', 'computing impact...');
        const {{ res, data }} = await fetchJson(`${{ADMIN_API_BASE}}/dataset/slice/preview`, {{
          method: 'POST',
          credentials: 'include',
          headers: {{
            'Content-Type': 'application/json',
            'X-CSRF-Token': csrfToken(),
          }},
          body: JSON.stringify(payload),
        }});
        if (!res.ok) {{
          if (slicePreviewRawEl) slicePreviewRawEl.textContent = JSON.stringify(data, null, 2);
          setSliceStatus('error', `preview failed (${{res.status}})`);
          return;
        }}
        renderSlicePreview(data || {{}});
      }}

      async function runSliceBuild() {{
        const payload = readSlicePayload();
        const params = {{
          from_cooked: !!(sliceFromCookedEl && sliceFromCookedEl.checked),
          overwrite: !!(sliceOverwriteEl && sliceOverwriteEl.checked),
          max_distance_ly: payload.max_distance_ly === null ? '' : String(payload.max_distance_ly),
          min_parallax_over_error: payload.min_parallax_over_error === null ? '' : String(payload.min_parallax_over_error),
          max_parallax_error_mas: payload.max_parallax_error_mas === null ? '' : String(payload.max_parallax_error_mas),
          max_ruwe: payload.max_ruwe === null ? '' : String(payload.max_ruwe),
          require_spectral_class: !!payload.require_spectral_class,
          require_color_index: !!payload.require_color_index,
          allowed_spectral_classes: (payload.allowed_spectral_classes || []).join(','),
        }};
        setSliceStatus('submitting', 'starting sliced build...');
        const result = await runAction('build_database_slice', params, 'RUN build_database_slice');
        if (!result || !result.ok) {{
          setSliceStatus('error', 'failed to start sliced build');
          return;
        }}
        const job = (result.data && result.data.job) || {{}};
        const jobId = String(job.job_id || '');
        if (!jobId) {{
          setSliceStatus('queued', 'job created; check Activity > Jobs');
          return;
        }}
        setSliceStatus('queued', `job ${{jobId}} queued`);
        await loadJobs();
        void followActionJob(jobId, sliceRunStatusEl, 'build_database_slice');
      }}

      function parseFieldValue(type, input) {{
        if (type === 'boolean') {{
          return !!input.checked;
        }}
        if (type === 'integer') {{
          const value = (input.value || '').trim();
          if (!value) return null;
          return Number.parseInt(value, 10);
        }}
        return (input.value || '').trim();
      }}

      function createParamField(actionName, paramName, spec) {{
        const wrap = document.createElement('div');
        wrap.className = 'field';
        const label = document.createElement('label');
        label.htmlFor = `param-${{actionName}}-${{paramName}}`;
        label.textContent = spec.label || paramName;
        wrap.appendChild(label);
        let input;
        const typ = spec.type || 'string';
        if (typ === 'boolean') {{
          input = document.createElement('input');
          input.type = 'checkbox';
          input.checked = !!spec.default;
        }} else {{
          input = document.createElement('input');
          input.type = typ === 'integer' ? 'number' : 'text';
          if (spec.placeholder) input.placeholder = String(spec.placeholder);
          if (spec.default !== undefined && spec.default !== null && spec.default !== '') {{
            input.value = String(spec.default);
          }}
        }}
        input.id = `param-${{actionName}}-${{paramName}}`;
        input.dataset.paramName = paramName;
        input.dataset.paramType = typ;
        input.dataset.required = String(!!spec.required);
        wrap.appendChild(input);
        if (spec.required) {{
          const req = document.createElement('div');
          req.className = 'small';
          req.textContent = 'Required';
          wrap.appendChild(req);
        }}
        return wrap;
      }}

      async function runAction(actionName, params, confirmation) {{
        const payload = {{ action: actionName, params: params || {{}} }};
        if (confirmation) payload.confirmation = confirmation;
        const {{ res, data }} = await fetchJson(`${{ADMIN_API_BASE}}/actions/run`, {{
          method: 'POST',
          credentials: 'include',
          headers: {{
            'Content-Type': 'application/json',
            'X-CSRF-Token': csrfToken(),
          }},
          body: JSON.stringify(payload),
        }});
        if (!res.ok) {{
          alert(`Action failed: ${{(data && data.error && data.error.message) || res.status}}`);
          return {{ ok: false, data }};
        }}
        await loadJobs();
        await loadAudit(false);
        return {{ ok: true, data }};
      }}

      function setActionRunStatus(statusEl, state, message) {{
        if (!statusEl) return;
        const st = String(state || 'idle');
        const msg = String(message || '');
        statusEl.textContent = msg ? `Status: ${{st}} | ${{msg}}` : `Status: ${{st}}`;
      }}

      async function followActionJob(jobId, statusEl, actionName) {{
        const maxPolls = 300;
        for (let i = 0; i < maxPolls; i += 1) {{
          const {{ res, data }} = await fetchJson(`${{ADMIN_API_BASE}}/actions/jobs/${{jobId}}`, {{ credentials: 'include' }});
          if (!res.ok) {{
            setActionRunStatus(statusEl, 'error', `failed to fetch job ${{jobId}}`);
            return;
          }}
          const job = (data && data.job) || {{}};
          const status = String(job.status || 'unknown');
          if (status === 'succeeded') {{
            if (actionName === 'generate_snapshots') {{
              setActionRunStatus(statusEl, 'succeeded', `job ${{jobId}} complete; snapshots refreshed. Reload search results to display new images.`);
            }} else {{
              setActionRunStatus(statusEl, 'succeeded', `job ${{jobId}} complete.`);
            }}
            await loadJobs();
            await loadAudit(false);
            return;
          }}
          if (status === 'failed') {{
            const err = String(job.error_message || '').trim();
            setActionRunStatus(statusEl, 'failed', err ? `job ${{jobId}} failed: ${{err}}` : `job ${{jobId}} failed`);
            await loadJobs();
            await loadAudit(false);
            return;
          }}
          if (status === 'cancelled') {{
            setActionRunStatus(statusEl, 'cancelled', `job ${{jobId}} cancelled`);
            await loadJobs();
            await loadAudit(false);
            return;
          }}
          setActionRunStatus(statusEl, status, `job ${{jobId}} running...`);
          await sleep(1000);
        }}
        setActionRunStatus(statusEl, 'unknown', `job ${{jobId}} still running; check Activity > Jobs`);
      }}

      async function loadCatalog() {{
        const {{ data }} = await fetchJson(`${{ADMIN_API_BASE}}/actions/catalog`, {{ credentials: 'include' }});
        actionsOpsEl.innerHTML = '';
        actionsCoolnessEl.innerHTML = '';
        (data.items || []).forEach((item) => {{
          actionCatalog.set(item.name, item);
          const card = document.createElement('div');
          card.className = 'action-card';
          const title = document.createElement('h3');
          title.textContent = item.display_name || item.name;
          card.appendChild(title);
          const meta = document.createElement('div');
          meta.className = 'action-meta';
          const category = item.category || 'operations';
          meta.textContent = `category=${{category}} risk=${{item.risk_level || 'low'}} roles=${{(item.required_roles || []).join(',')}}`;
          card.appendChild(meta);
          const desc = document.createElement('div');
          desc.className = 'small';
          desc.textContent = item.description || '';
          card.appendChild(desc);

          const form = document.createElement('form');
          form.dataset.action = item.name;
          form.style.marginTop = '0.45rem';
          const schema = item.params_schema || {{}};
          Object.entries(schema).forEach(([paramName, spec]) => {{
            form.appendChild(createParamField(item.name, paramName, spec || {{}}));
          }});
          if (item.requires_confirmation) {{
            form.appendChild(createParamField(item.name, 'confirmation', {{
              type: 'string',
              required: true,
              allow_empty: false,
              placeholder: item.confirmation_phrase || '',
              label: 'Confirmation phrase',
            }}));
          }}
          const runBtn = document.createElement('button');
          runBtn.className = (item.risk_level === 'high') ? 'warn' : 'primary';
          runBtn.type = 'submit';
          runBtn.textContent = 'Run';
          form.appendChild(runBtn);
          const statusEl = document.createElement('div');
          statusEl.className = 'small';
          setActionRunStatus(statusEl, 'idle', '');
          form.appendChild(statusEl);

          form.onsubmit = async (e) => {{
            e.preventDefault();
            const params = {{}};
            let confirmation = '';
            const inputs = form.querySelectorAll('input[data-param-name]');
            for (const input of inputs) {{
              const paramName = input.dataset.paramName;
              const paramType = input.dataset.paramType || 'string';
              const required = input.dataset.required === 'true';
              const value = parseFieldValue(paramType, input);
              if (paramName === 'confirmation') {{
                confirmation = String(value || '').trim();
                if (required && !confirmation) {{
                  alert('Confirmation phrase is required.');
                  return;
                }}
                continue;
              }}
              if (required && (value === null || value === '')) {{
                alert(`Missing required parameter: ${{paramName}}`);
                return;
              }}
              if (value === null || value === '') continue;
              params[paramName] = value;
            }}
            setActionRunStatus(statusEl, 'submitting', 'starting job...');
            const result = await runAction(item.name, params, confirmation);
            if (!result || !result.ok) {{
              setActionRunStatus(statusEl, 'error', 'failed to start');
              return;
            }}
            const job = (result.data && result.data.job) || {{}};
            const jobId = String(job.job_id || '');
            if (!jobId) {{
              setActionRunStatus(statusEl, 'queued', 'job created; check Activity > Jobs');
              return;
            }}
            setActionRunStatus(statusEl, 'queued', `job ${{jobId}} queued`);
            await loadJobs();
            void followActionJob(jobId, statusEl, item.name);
          }};

          card.appendChild(form);
          if ((item.category || 'operations') === 'coolness') {{
            actionsCoolnessEl.appendChild(card);
          }} else {{
            actionsOpsEl.appendChild(card);
          }}
        }});
      }}

      async function cancelJob(jobId) {{
        const {{ res, data }} = await fetchJson(`${{ADMIN_API_BASE}}/actions/jobs/${{jobId}}/cancel`, {{
          method: 'POST',
          credentials: 'include',
          headers: {{ 'X-CSRF-Token': csrfToken() }},
        }});
        if (!res.ok) {{
          alert(`Cancel failed: ${{(data && data.error && data.error.message) || res.status}}`);
          return;
        }}
        await loadJobs();
        await loadAudit(false);
      }}

	      async function loadJobs() {{
	        const {{ data }} = await fetchJson(`${{ADMIN_API_BASE}}/actions/jobs?limit=50`, {{ credentials: 'include' }});
	        latestJobs = Array.isArray(data.items) ? data.items : [];
	        jobsEl.innerHTML = '';
	        latestJobs.forEach((job) => {{
          const li = document.createElement('li');
          const top = document.createElement('div');
          top.className = 'inline';
          const badge = document.createElement('span');
          badge.className = `status-badge ${{statusClass(job.status)}}`;
          badge.textContent = job.status;
          top.appendChild(badge);
          const title = document.createElement('strong');
          title.textContent = `${{job.action}} (${{job.job_id}})`;
          top.appendChild(title);
          li.appendChild(top);

          const controls = document.createElement('div');
          controls.className = 'inline';
          const viewBtn = document.createElement('button');
          viewBtn.textContent = 'View Log';
          viewBtn.onclick = async (e) => {{
            e.preventDefault();
            currentJobId = job.job_id;
            currentOffset = 0;
            logEl.textContent = '';
            selectedJobEl.textContent = `${{job.action}} (${{job.job_id}})`;
            await pollLog();
          }};
          controls.appendChild(viewBtn);

          const download = document.createElement('a');
          download.href = `${{ADMIN_API_BASE}}/actions/jobs/${{job.job_id}}/log/download`;
          download.textContent = 'Download Log';
          controls.appendChild(download);

          if (job.status === 'queued') {{
            const cancelBtn = document.createElement('button');
            cancelBtn.className = 'danger';
            cancelBtn.textContent = 'Cancel';
            cancelBtn.onclick = async (e) => {{
              e.preventDefault();
              await cancelJob(job.job_id);
            }};
            controls.appendChild(cancelBtn);
          }}

          li.appendChild(controls);
          jobsEl.appendChild(li);
        }});
      }}

      function fillActionParam(actionName, paramName, value) {{
        const el = document.getElementById(`param-${{actionName}}-${{paramName}}`);
        if (!el) return;
        if (el.type === 'checkbox') {{
          el.checked = !!value;
        }} else {{
          el.value = String(value || '');
        }}
      }}

      async function loadBackups() {{
        const {{ data }} = await fetchJson(`${{ADMIN_API_BASE}}/backups?limit=100`, {{ credentials: 'include' }});
        backupsAdminDbEl.innerHTML = '';
        backupsReleaseMetaEl.innerHTML = '';

        (data.admin_db || []).forEach((item) => {{
          const li = document.createElement('li');
          const useBtn = document.createElement('button');
          useBtn.textContent = 'Use';
          useBtn.onclick = () => fillActionParam('restore_admin_db', 'backup_name', item.name);
          li.appendChild(useBtn);
          const label = document.createElement('span');
          label.textContent = ` ${{item.name}} (${{item.bytes}} bytes)`;
          li.appendChild(label);
          backupsAdminDbEl.appendChild(li);
        }});

        (data.release_metadata || []).forEach((item) => {{
          const li = document.createElement('li');
          const useBtn = document.createElement('button');
          useBtn.textContent = 'Use';
          useBtn.onclick = () => fillActionParam('restore_release_metadata', 'backup_id', item.backup_id);
          li.appendChild(useBtn);
          const label = document.createElement('span');
          label.textContent = ` ${{item.backup_id}}`;
          li.appendChild(label);
          backupsReleaseMetaEl.appendChild(li);
        }});
      }}

      function setInferenceStatus(status, detail = '') {{
        if (!infFormStatusEl) return;
        const head = String(status || 'idle').trim().toLowerCase() || 'idle';
        infFormStatusEl.textContent = detail ? `Status: ${{head}} | ${{detail}}` : `Status: ${{head}}`;
      }}

      function readInferenceForm() {{
        const payload = {{
          display_name: String(infDisplayNameEl.value || '').trim(),
          endpoint_key: String(infEndpointKeyEl.value || '').trim() || null,
          provider: String(infProviderEl.value || 'openai_compatible'),
          base_url: String(infBaseUrlEl.value || '').trim(),
          auth_mode: String(infAuthModeEl.value || 'none'),
          api_key_env: String(infApiKeyEnvEl.value || '').trim() || null,
          api_key: String(infApiKeyEl.value || '').trim() || null,
          default_model: String(infDefaultModelEl.value || '').trim() || null,
          timeout_s: Number.parseInt(String(infTimeoutEl.value || '30'), 10),
          enabled: !!infEnabledEl.checked,
          notes: String(infNotesEl.value || '').trim() || null,
          role_defaults: {{}},
        }};
        if (!Number.isFinite(payload.timeout_s)) payload.timeout_s = 30;
        return payload;
      }}

      function resetInferenceForm() {{
        infDisplayNameEl.value = '';
        infEndpointKeyEl.value = '';
        infProviderEl.value = 'openai_compatible';
        infBaseUrlEl.value = '';
        infAuthModeEl.value = 'none';
        infApiKeyEnvEl.value = '';
        infApiKeyEl.value = '';
        infDefaultModelEl.value = '';
        infTimeoutEl.value = '30';
        infEnabledEl.checked = true;
        infNotesEl.value = '';
      }}

      function appendEndpointMeta(parent, label, value) {{
        const row = document.createElement('div');
        row.className = 'metric-row';
        const keyEl = document.createElement('div');
        keyEl.className = 'metric-k';
        keyEl.textContent = label;
        const valueEl = document.createElement('div');
        valueEl.className = 'metric-v';
        valueEl.textContent = String(value || '');
        row.appendChild(keyEl);
        row.appendChild(valueEl);
        parent.appendChild(row);
      }}

      function renderInferenceEndpoints(items) {{
        inferenceEndpointsEl.innerHTML = '';
        if (!items.length) {{
          const empty = document.createElement('div');
          empty.className = 'note-box small';
          empty.textContent = 'No endpoints registered.';
          inferenceEndpointsEl.appendChild(empty);
          return;
        }}
        items.forEach((endpoint) => {{
          const card = document.createElement('div');
          card.className = 'action-card';
          const titleRow = document.createElement('div');
          titleRow.className = 'inline';
          const title = document.createElement('h3');
          title.style.margin = '0';
          title.textContent = endpoint.display_name || endpoint.endpoint_key;
          titleRow.appendChild(title);
          const badge = document.createElement('span');
          badge.className = 'status-badge';
          badge.textContent = endpoint.enabled ? 'enabled' : 'disabled';
          titleRow.appendChild(badge);
          card.appendChild(titleRow);

          const meta = document.createElement('div');
          meta.className = 'metric-list';
          appendEndpointMeta(meta, 'Key', endpoint.endpoint_key);
          appendEndpointMeta(meta, 'Provider', endpoint.provider);
          appendEndpointMeta(meta, 'Base URL', endpoint.base_url);
          appendEndpointMeta(meta, 'Auth', `${{endpoint.auth_mode || 'none'}}${{endpoint.api_key_configured ? ' / configured' : ''}}`);
          appendEndpointMeta(meta, 'API key env', endpoint.api_key_env || '');
          appendEndpointMeta(meta, 'Default model', endpoint.default_model || '');
          const lastProbe = endpoint.last_probe || null;
          if (lastProbe) {{
            appendEndpointMeta(
              meta,
              'Last probe',
              `${{lastProbe.status}} at ${{lastProbe.probed_at || 'n/a'}}; models=${{formatInt(lastProbe.model_count)}}; latency=${{lastProbe.latency_ms === null || lastProbe.latency_ms === undefined ? 'n/a' : formatInt(lastProbe.latency_ms) + ' ms'}}`
            );
            if (lastProbe.error_message) appendEndpointMeta(meta, 'Probe error', lastProbe.error_message);
          }}
          if (endpoint.notes) appendEndpointMeta(meta, 'Notes', endpoint.notes);
          card.appendChild(meta);

          const models = Array.isArray(endpoint.models) ? endpoint.models : [];
          const details = document.createElement('details');
          details.style.marginTop = '0.45rem';
          const summary = document.createElement('summary');
          summary.textContent = `Models (${{formatInt(models.length)}})`;
          details.appendChild(summary);
          const table = document.createElement('table');
          table.className = 'mini-table';
          const thead = document.createElement('thead');
          thead.innerHTML = '<tr><th>Model</th><th>Context</th><th>Owner</th><th>Last seen</th></tr>';
          table.appendChild(thead);
          const tbody = document.createElement('tbody');
          if (!models.length) {{
            const tr = document.createElement('tr');
            const td = document.createElement('td');
            td.colSpan = 4;
            td.textContent = 'No cached models yet. Poll the endpoint.';
            tr.appendChild(td);
            tbody.appendChild(tr);
          }} else {{
            models.forEach((model) => {{
              const tr = document.createElement('tr');
              ['model_id', 'max_model_len', 'owned_by', 'last_seen_at'].forEach((key) => {{
                const td = document.createElement('td');
                td.textContent = String(model[key] || '');
                tr.appendChild(td);
              }});
              tbody.appendChild(tr);
            }});
          }}
          table.appendChild(tbody);
          details.appendChild(table);
          card.appendChild(details);

          const controls = document.createElement('div');
          controls.className = 'inline';
          controls.style.marginTop = '0.45rem';
          const pollBtn = document.createElement('button');
          pollBtn.type = 'button';
          pollBtn.textContent = 'Poll Models';
          pollBtn.onclick = () => {{ void pollInferenceEndpoint(endpoint.endpoint_id); }};
          controls.appendChild(pollBtn);
          const deleteBtn = document.createElement('button');
          deleteBtn.type = 'button';
          deleteBtn.className = 'danger';
          deleteBtn.textContent = 'Remove';
          deleteBtn.onclick = () => {{ void deleteInferenceEndpoint(endpoint.endpoint_id, endpoint.display_name || endpoint.endpoint_key); }};
          controls.appendChild(deleteBtn);
          card.appendChild(controls);
          inferenceEndpointsEl.appendChild(card);
        }});
      }}

      function renderInferenceStats(items) {{
        inferenceStatsRowsEl.innerHTML = '';
        if (!items.length) {{
          const tr = document.createElement('tr');
          const td = document.createElement('td');
          td.colSpan = 6;
          td.textContent = 'No usage events recorded yet.';
          tr.appendChild(td);
          inferenceStatsRowsEl.appendChild(tr);
          return;
        }}
        items.forEach((item) => {{
          const tr = document.createElement('tr');
          [
            item.display_name || item.endpoint_key || '',
            item.model_id || '',
            formatInt(item.request_count),
            formatInt(item.total_tokens),
            item.avg_latency_ms === null || item.avg_latency_ms === undefined ? '' : `${{formatFloat(item.avg_latency_ms, 1)}} ms`,
            item.last_used_at || '',
          ].forEach((value) => {{
            const td = document.createElement('td');
            td.textContent = String(value);
            tr.appendChild(td);
          }});
          inferenceStatsRowsEl.appendChild(tr);
        }});
      }}

      async function loadInferenceEndpoints() {{
        const {{ res, data }} = await fetchJson(`${{ADMIN_API_BASE}}/inference/endpoints`, {{ credentials: 'include' }});
        if (!res.ok) {{
          setInferenceStatus('error', `endpoint load failed (${{res.status}})`);
          renderInferenceEndpoints([]);
          return;
        }}
        renderInferenceEndpoints(Array.isArray(data.items) ? data.items : []);
      }}

      async function loadInferenceStats() {{
        const {{ res, data }} = await fetchJson(`${{ADMIN_API_BASE}}/inference/stats`, {{ credentials: 'include' }});
        if (!res.ok) {{
          renderInferenceStats([]);
          return;
        }}
        renderInferenceStats(Array.isArray(data.items) ? data.items : []);
      }}

      async function loadInference() {{
        await loadInferenceEndpoints();
        await loadInferenceStats();
      }}

      async function createInferenceEndpoint() {{
        const payload = readInferenceForm();
        if (!payload.display_name || !payload.base_url) {{
          setInferenceStatus('error', 'display name and base URL are required');
          return;
        }}
        setInferenceStatus('saving', 'creating endpoint...');
        const {{ res, data }} = await fetchJson(`${{ADMIN_API_BASE}}/inference/endpoints`, {{
          method: 'POST',
          credentials: 'include',
          headers: {{
            'Content-Type': 'application/json',
            'X-CSRF-Token': csrfToken(),
          }},
          body: JSON.stringify(payload),
        }});
        if (!res.ok) {{
          const msg = String((data && data.detail && data.detail.message) || data.message || res.status);
          setInferenceStatus('error', msg);
          return;
        }}
        resetInferenceForm();
        setInferenceStatus('saved', 'endpoint added');
        await loadInference();
        await loadAudit(false);
      }}

      async function pollInferenceEndpoint(endpointId) {{
        setInferenceStatus('polling', `endpoint ${{endpointId}}`);
        const {{ res, data }} = await fetchJson(`${{ADMIN_API_BASE}}/inference/endpoints/${{endpointId}}/poll-models`, {{
          method: 'POST',
          credentials: 'include',
          headers: {{ 'X-CSRF-Token': csrfToken() }},
        }});
        if (!res.ok) {{
          const msg = String((data && data.detail && data.detail.message) || data.message || res.status);
          setInferenceStatus('error', msg);
          await loadInferenceEndpoints();
          await loadAudit(false);
          return;
        }}
        const models = Array.isArray(data.models) ? data.models.length : 0;
        setInferenceStatus('ready', `polled ${{formatInt(models)}} models`);
        await loadInference();
        await loadAudit(false);
      }}

      async function deleteInferenceEndpoint(endpointId, label) {{
        if (!window.confirm(`Remove inference endpoint "${{label || endpointId}}"?`)) return;
        setInferenceStatus('removing', `endpoint ${{endpointId}}`);
        const {{ res, data }} = await fetchJson(`${{ADMIN_API_BASE}}/inference/endpoints/${{endpointId}}`, {{
          method: 'DELETE',
          credentials: 'include',
          headers: {{ 'X-CSRF-Token': csrfToken() }},
        }});
        if (!res.ok) {{
          const msg = String((data && data.detail && data.detail.message) || data.message || res.status);
          setInferenceStatus('error', msg);
          return;
        }}
        setInferenceStatus('removed', String(label || endpointId));
        await loadInference();
        await loadAudit(false);
      }}

      function buildAuditQuery(loadOlder) {{
        const params = new URLSearchParams();
        params.set('limit', '50');
        if (loadOlder && nextAuditBeforeId) {{
          params.set('before_audit_id', String(nextAuditBeforeId));
        }}
        if (auditPreset === 'auth') {{
          params.set('event_prefix', 'auth.');
        }} else if (auditPreset === 'actions') {{
          params.set('event_prefix', 'admin.action.');
        }} else if (auditPreset === 'inference') {{
          params.set('event_prefix', 'admin.inference.');
        }} else if (auditPreset === 'queries') {{
          params.set('event_prefix', 'api.search.');
        }}
        const eventType = (auditEventTypeEl.value || '').trim();
        const result = (auditResultEl.value || '').trim();
        const requestId = (auditRequestIdEl.value || '').trim();
        if (eventType) params.set('event_type', eventType);
        if (result) params.set('result', result);
        if (requestId) params.set('request_id', requestId);
        return params.toString();
      }}

      function renderAuditItems(items, append) {{
        if (!append) {{
          auditEl.innerHTML = '';
          auditDetailsEl.textContent = '';
        }}
        (items || []).forEach((entry) => {{
          const li = document.createElement('li');
          const a = document.createElement('a');
          const details = (entry && typeof entry.details === 'object' && entry.details) ? entry.details : {{}};
          const response = (details && typeof details.response === 'object' && details.response) ? details.response : {{}};
          const qRaw = String(details.query_raw || '').trim();
          const returnedCount = Number.isFinite(Number(response.returned_count)) ? Number(response.returned_count) : null;
          const totalCount = Number.isFinite(Number(response.total_count)) ? Number(response.total_count) : null;
          const zeroResults = response.zero_results === true;
          const hasMore = response.has_more === true;
          let querySummary = '';
          if (entry.event_type === 'api.search.systems') {{
            const qPart = qRaw ? ` q="${{qRaw}}"` : ' q=(filters)';
            const returnedPart = returnedCount === null ? ' returned=?' : ` returned=${{returnedCount}}`;
            const totalPart = totalCount === null ? '' : ` total=${{totalCount}}`;
            const morePart = hasMore ? ' more=yes' : '';
            const emptyPart = zeroResults ? ' ZERO' : '';
            querySummary = `${{qPart}}${{returnedPart}}${{totalPart}}${{morePart}}${{emptyPart}}`;
          }}
          a.href = '#';
          a.textContent = `#${{entry.audit_id}} [${{entry.result}}] ${{entry.event_type}}${{querySummary}} ${{entry.request_id || ''}} ${{entry.correlation_id || ''}}`;
          a.onclick = (e) => {{
            e.preventDefault();
            auditDetailsEl.textContent = JSON.stringify(entry, null, 2);
          }};
          li.appendChild(a);
          const meta = document.createElement('span');
          meta.textContent = ` (${{entry.created_at}} ${{entry.method || ''}} ${{entry.route || ''}})`;
          li.appendChild(meta);
          auditEl.appendChild(li);
        }});
      }}

      async function loadAudit(loadOlder = false) {{
        const query = buildAuditQuery(loadOlder);
        const {{ res, data }} = await fetchJson(`${{ADMIN_API_BASE}}/audit?${{query}}`, {{ credentials: 'include' }});
        if (!res.ok) {{
          auditDetailsEl.textContent = JSON.stringify(data, null, 2);
          return;
        }}
        const items = data.items || [];
        renderAuditItems(items, loadOlder);
        nextAuditBeforeId = data.next_before_audit_id || null;
      }}

      async function pollLog() {{
        if (!currentJobId) return;
        const {{ data }} = await fetchJson(`${{ADMIN_API_BASE}}/actions/jobs/${{currentJobId}}/log?offset=${{currentOffset}}&limit=65536`, {{ credentials: 'include' }});
        const chunk = data.chunk || '';
        if (chunk) {{
          logEl.textContent += chunk;
          currentOffset = data.next_offset || currentOffset;
          logEl.scrollTop = logEl.scrollHeight;
        }}
        if (!data.eof) {{
          setTimeout(pollLog, 1000);
        }}
      }}

      async function doLogout() {{
        await fetch(`${{AUTH_API_BASE}}/logout`, {{
          method: 'POST',
          credentials: 'include',
          headers: {{ 'X-CSRF-Token': csrfToken() }},
        }});
        window.location.href = `${{ADMIN_API_BASE}}/ui`;
      }}

      document.getElementById('logout').addEventListener('click', doLogout);
      if (adminThemeSelectEl) {{
        adminThemeSelectEl.addEventListener('change', (event) => {{
          applyAdminTheme((event && event.target && event.target.value) || 'simple_light');
        }});
      }}
      document.getElementById('screenTabOperations').addEventListener('click', () => setScreen('operations'));
      document.getElementById('screenTabStatus').addEventListener('click', () => setScreen('status'));
      document.getElementById('screenTabDataset').addEventListener('click', () => setScreen('dataset'));
      document.getElementById('screenTabInference').addEventListener('click', () => setScreen('inference'));
      document.getElementById('screenTabCoolness').addEventListener('click', () => setScreen('coolness'));
      document.getElementById('screenTabActivity').addEventListener('click', () => setScreen('activity'));
      document.getElementById('refreshStatus').addEventListener('click', callStatus);
      document.getElementById('refreshDatasetStatus').addEventListener('click', () => {{ void callDatasetStatus(true); }});
      document.getElementById('refreshDatasetStatusInline').addEventListener('click', () => {{ void callDatasetStatus(true); }});
      document.getElementById('refreshJobs').addEventListener('click', loadJobs);
      document.getElementById('refreshBackups').addEventListener('click', loadBackups);
      document.getElementById('refreshAudit').addEventListener('click', () => loadAudit(false));
      document.getElementById('loadOlderAudit').addEventListener('click', () => loadAudit(true));
      document.getElementById('auditPresetAll').addEventListener('click', () => {{ setAuditPreset('all'); loadAudit(false); }});
      document.getElementById('auditPresetAuth').addEventListener('click', () => {{ setAuditPreset('auth'); loadAudit(false); }});
      document.getElementById('auditPresetActions').addEventListener('click', () => {{ setAuditPreset('actions'); loadAudit(false); }});
      document.getElementById('auditPresetInference').addEventListener('click', () => {{ setAuditPreset('inference'); loadAudit(false); }});
      document.getElementById('auditPresetQueries').addEventListener('click', () => {{ setAuditPreset('queries'); loadAudit(false); }});
      document.getElementById('presetBalanced').addEventListener('click', () => setCoolnessWeights(coolnessPresetWeights.balanced, 'balanced'));
      document.getElementById('presetExotic').addEventListener('click', () => setCoolnessWeights(coolnessPresetWeights.exotic, 'exotic'));
      document.getElementById('presetHabitable').addEventListener('click', () => setCoolnessWeights(coolnessPresetWeights.habitable, 'habitable'));
      document.getElementById('presetNearby').addEventListener('click', () => setCoolnessWeights(coolnessPresetWeights.nearby, 'nearby'));
      if (slicePreviewBtnEl) slicePreviewBtnEl.addEventListener('click', () => {{ void callSlicePreview(); }});
      if (sliceRunBtnEl) sliceRunBtnEl.addEventListener('click', () => {{ void runSliceBuild(); }});
      document.getElementById('refreshInference').addEventListener('click', () => {{ void loadInference(); }});
      infCreateBtnEl.addEventListener('click', () => {{ void createInferenceEndpoint(); }});
      coolResetActiveEl.addEventListener('click', resetCoolnessToActive);
      coolApplyBtnEl.addEventListener('click', () => {{ void applyCoolness(); }});
      coolSaveBtnEl.addEventListener('click', () => {{ void saveCoolnessProfile(); }});
      coolActivateBtnEl.addEventListener('click', () => {{ void activateCoolnessProfile(); }});
      coolLoadProfileBtnEl.addEventListener('click', loadSelectedSavedProfile);
      bindRangeNumberPair(snapshotTopCoolnessEl, snapshotTopCoolnessNumberEl, 100, {{ min: 1, max: 10000, step: 10 }});
      bindRangeNumberPair(snapshotMaxDistanceLyEl, snapshotMaxDistanceLyNumberEl, 1000, {{ min: 1, max: 1000, step: 1 }});
      bindRangeNumberPair(snapshotMinStarCountEl, snapshotMinStarCountNumberEl, 0, {{ min: 0, max: 12, step: 1 }});
      bindRangeNumberPair(snapshotMinPlanetCountEl, snapshotMinPlanetCountNumberEl, 0, {{ min: 0, max: 20, step: 1 }});
      bindRangeNumberPair(snapshotMinCoolnessScoreEl, snapshotMinCoolnessScoreNumberEl, 0, {{ min: 0, max: 40, step: 0.5 }});
      snapshotRunBtnEl.addEventListener('click', () => {{ void runSnapshotGeneration(); }});
      applyAdminTheme(resolveAdminTheme());
      setScreen('status');
      renderCoolnessSliders();
      renderCoolnessPreview(null);
      setPreviewNotice('Preview is safe and read-only. Run updates ranking outputs ephemerally; Save Profile persists a chosen version; Activate Profile switches what is live.');
      setRunStatus('idle', 'ready');
      setSnapshotRunStatus('idle', 'top 100 coolness systems');
      setSliceStatus('idle', 'configure and preview slice policy');
      callStatus();
      callDatasetStatus(false);
      callSlicePreview();
      loadInference();
      loadCatalog();
      loadCoolnessState();
      loadJobs();
      loadBackups();
      loadAudit();
      setInterval(loadJobs, 5000);
    </script>
  </body>
</html>
    """
    return HTMLResponse(body, status_code=200)
