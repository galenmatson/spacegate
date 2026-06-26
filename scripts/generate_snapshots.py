#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import duckdb


DEFAULT_VIEW_TYPE = "system_card"
DEFAULT_GENERATOR_VERSION = "snapshot-v1.0.5"
DEFAULT_WIDTH_PX = 980
DEFAULT_HEIGHT_PX = 560
SYSTEM_KEY_SAFE_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _root_dir() -> Path:
    return Path(__file__).resolve().parents[1]


def _state_dir(root: Path) -> Path:
    return Path(os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR") or (root / "data"))


def _json_canonical(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _emit_progress(payload: Dict[str, Any]) -> None:
    print("[snapshot-progress] " + json.dumps(payload, sort_keys=True), flush=True)


def _progress_interval(total: int) -> int:
    if total <= 0:
        return 1
    return max(100, min(10000, max(1, total // 100)))


def _safe_system_key(stable_object_key: str, system_id: int) -> str:
    cleaned = SYSTEM_KEY_SAFE_RE.sub("_", str(stable_object_key or "").strip())
    cleaned = cleaned.strip("._-")
    if not cleaned:
        cleaned = f"system_{system_id}"
    return cleaned


def _resolve_build_dir(state_dir: Path, build_id: str | None) -> tuple[str, Path]:
    out_dir = state_dir / "out"
    if not out_dir.is_dir():
        raise SystemExit(f"Missing out directory: {out_dir}")

    if build_id:
        target = out_dir / build_id
        if not target.is_dir():
            raise SystemExit(f"Build directory not found: {target}")
        return build_id, target

    served = state_dir / "served" / "current"
    if served.exists():
        resolved = served.resolve()
        if resolved.is_dir():
            return resolved.name, resolved

    candidates = [p for p in out_dir.iterdir() if p.is_dir() and not p.name.endswith(".tmp")]
    if not candidates:
        raise SystemExit(f"No build directories found in: {out_dir}")
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    latest = candidates[0]
    return latest.name, latest


def _open_core_db(build_dir: Path) -> duckdb.DuckDBPyConnection:
    db_path = build_dir / "core.duckdb"
    if not db_path.exists():
        raise SystemExit(f"Missing core.duckdb in build: {db_path}")
    return duckdb.connect(str(db_path), read_only=True)


def _open_arm_db(build_dir: Path) -> duckdb.DuckDBPyConnection | None:
    db_path = build_dir / "arm.duckdb"
    if not db_path.exists():
        return None
    return duckdb.connect(str(db_path), read_only=True)


def _duckdb_has_table(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return row is not None


def _open_disc_db(
    build_dir: Path,
) -> tuple[duckdb.DuckDBPyConnection, Path | None, Path | None]:
    disc_path = build_dir / "disc.duckdb"
    disc_path.parent.mkdir(parents=True, exist_ok=True)
    writable_path = disc_path
    temp_copy: Path | None = None
    if disc_path.exists() and not os.access(disc_path, os.W_OK):
        temp_copy = build_dir / f".disc.duckdb.{os.getpid()}.tmp"
        shutil.copy2(disc_path, temp_copy)
        writable_path = temp_copy
    con = duckdb.connect(str(writable_path), read_only=False)
    return con, temp_copy, disc_path if temp_copy is not None else None


def _finalize_disc_db(
    disc_con: duckdb.DuckDBPyConnection,
    temp_path: Path | None,
    target_path: Path | None,
) -> None:
    disc_con.close()
    if temp_path is None or target_path is None:
        return
    temp_path.chmod(0o664)
    os.replace(temp_path, target_path)


def _disc_has_coolness_scores(disc_path: Path) -> bool:
    if not disc_path.exists():
        return False
    con = duckdb.connect(str(disc_path), read_only=True)
    try:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE lower(table_name) = 'coolness_scores'
            LIMIT 1
            """
        ).fetchone()
        return row is not None
    finally:
        con.close()


def _load_system_rows(
    core_con: duckdb.DuckDBPyConnection,
    *,
    system_ids: Sequence[int],
    limit: int,
    top_coolness: int,
    disc_path: Path,
    min_dist_ly: float | None,
    max_dist_ly: float | None,
    min_star_count: int | None,
    max_star_count: int | None,
    min_planet_count: int | None,
    max_planet_count: int | None,
    min_coolness_score: float | None,
    max_coolness_score: float | None,
) -> List[Dict[str, Any]]:
    id_rows: List[Dict[str, Any]] = []
    if system_ids:
        placeholders = ",".join(["?"] * len(system_ids))
        cur = core_con.execute(
            f"""
            SELECT
              system_id,
              stable_object_key,
              system_name,
              dist_ly,
              ra_deg,
              dec_deg,
              x_helio_ly,
              y_helio_ly,
              z_helio_ly
            FROM systems
            WHERE system_id IN ({placeholders})
            ORDER BY system_id ASC
            """,
            list(system_ids),
        )
        cols = [d[0] for d in cur.description]
        id_rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        return id_rows

    wants_coolness = (
        top_coolness > 0
        or min_coolness_score is not None
        or max_coolness_score is not None
    )
    has_coolness_scores = _disc_has_coolness_scores(disc_path) if disc_path.exists() else False
    if wants_coolness and not has_coolness_scores:
        raise SystemExit(
            "Snapshot selection requested coolness-ranked filtering, but disc.coolness_scores is unavailable. "
            "Run score_coolness first."
        )

    conditions: list[str] = []
    params: list[Any] = []
    if min_dist_ly is not None:
        conditions.append("s.dist_ly >= ?")
        params.append(min_dist_ly)
    if max_dist_ly is not None:
        conditions.append("s.dist_ly <= ?")
        params.append(max_dist_ly)
    if min_star_count is not None:
        conditions.append("(SELECT COUNT(*) FROM stars st WHERE st.system_id = s.system_id) >= ?")
        params.append(min_star_count)
    if max_star_count is not None:
        conditions.append("(SELECT COUNT(*) FROM stars st WHERE st.system_id = s.system_id) <= ?")
        params.append(max_star_count)
    if min_planet_count is not None:
        conditions.append("(SELECT COUNT(*) FROM planets p WHERE p.system_id = s.system_id) >= ?")
        params.append(min_planet_count)
    if max_planet_count is not None:
        conditions.append("(SELECT COUNT(*) FROM planets p WHERE p.system_id = s.system_id) <= ?")
        params.append(max_planet_count)
    if min_coolness_score is not None:
        conditions.append("c.score_total >= ?")
        params.append(min_coolness_score)
    if max_coolness_score is not None:
        conditions.append("c.score_total <= ?")
        params.append(max_coolness_score)
    where_sql = ""
    if conditions:
        where_sql = "WHERE " + " AND ".join(conditions)

    if has_coolness_scores:
        attached_disc = False
        try:
            escaped = str(disc_path).replace("'", "''")
            core_con.execute(f"ATTACH '{escaped}' AS disc_db (READ_ONLY)")
            attached_disc = True
            sql = f"""
                SELECT
                  s.system_id,
                  s.stable_object_key,
                  s.system_name,
                  s.dist_ly,
                  s.ra_deg,
                  s.dec_deg,
                  s.x_helio_ly,
                  s.y_helio_ly,
                  s.z_helio_ly
                FROM systems s
                JOIN disc_db.coolness_scores c USING (system_id)
                {where_sql}
            """
            query_params = list(params)
            if top_coolness > 0:
                sql += " ORDER BY c.rank ASC, s.system_id ASC LIMIT ?"
                query_params.append(top_coolness)
            else:
                sql += " ORDER BY COALESCE(c.rank, 9223372036854775807) ASC, s.system_id ASC"
                if limit > 0:
                    sql += " LIMIT ?"
                    query_params.append(limit)
            cur = core_con.execute(sql, query_params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            if attached_disc:
                try:
                    core_con.execute("DETACH disc_db")
                except Exception:
                    pass

    sql = """
        SELECT
          s.system_id,
          s.stable_object_key,
          s.system_name,
          s.dist_ly,
          s.ra_deg,
          s.dec_deg,
          s.x_helio_ly,
          s.y_helio_ly,
          s.z_helio_ly
        FROM systems s
        {where_sql}
        ORDER BY COALESCE(s.dist_ly, 1e18) ASC, s.system_id ASC
    """
    if limit > 0:
        sql += " LIMIT ?"
        params.append(limit)
    cur = core_con.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _load_system_stars(core_con: duckdb.DuckDBPyConnection, system_id: int) -> List[Dict[str, Any]]:
    cur = core_con.execute(
        """
        SELECT
          star_id,
          star_name,
          component,
          spectral_class,
          spectral_type_raw,
          vmag,
          dist_ly
        FROM stars
        WHERE system_id = ?
        ORDER BY component ASC NULLS LAST, star_name ASC NULLS LAST, star_id ASC
        """,
        [system_id],
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _derived_parameter_rows(
    arm_con: duckdb.DuckDBPyConnection | None,
    planet_ids: Sequence[int],
) -> Dict[tuple[int, str], Dict[str, Any]]:
    if arm_con is None or not planet_ids or not _duckdb_has_table(arm_con, "derived_physical_parameters"):
        return {}
    placeholders = ",".join(["?"] * len(planet_ids))
    cur = arm_con.execute(
        f"""
        SELECT
          planet_id,
          parameter_key,
          value,
          unit,
          derivation_method,
          confidence_score,
          confidence_tier,
          basis,
          input_parameters_json,
          assumptions_json
        FROM (
          SELECT
            planet_id,
            parameter_key,
            value,
            unit,
            derivation_method,
            confidence_score,
            confidence_tier,
            'arm.derived_physical_parameters:' || COALESCE(derivation_method, 'derived') AS basis,
            input_parameters_json,
            assumptions_json,
            row_number() OVER (
              PARTITION BY planet_id, parameter_key
              ORDER BY COALESCE(confidence_score, 0.0) DESC, derived_parameter_id ASC
            ) AS rn
          FROM derived_physical_parameters
          WHERE object_type = 'planet'
            AND parameter_key IN ('semi_major_axis_au', 'insol_earth', 'eq_temp_k')
            AND planet_id IN ({placeholders})
        )
        WHERE rn = 1
        """,
        list(planet_ids),
    )
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    return {
        (int(row["planet_id"]), str(row["parameter_key"])): row
        for row in rows
        if row.get("planet_id") is not None and row.get("parameter_key")
    }


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def _resolved_planet_value(
    planet: Dict[str, Any],
    derived_rows: Dict[tuple[int, str], Dict[str, Any]],
    source_key: str,
    parameter_key: str,
) -> tuple[float | None, str, str | None, str | None]:
    source = _float_or_none(planet.get(source_key))
    if source is not None:
        return source, "source", "core", None
    planet_id = planet.get("planet_id")
    try:
        derived = derived_rows.get((int(planet_id), parameter_key)) if planet_id is not None else None
    except Exception:
        derived = None
    value = _float_or_none((derived or {}).get("value"))
    if value is not None:
        return value, "derived", "arm", (derived or {}).get("basis")
    return None, "missing", None, None


def _load_system_planets(
    core_con: duckdb.DuckDBPyConnection,
    arm_con: duckdb.DuckDBPyConnection | None,
    system_id: int,
) -> List[Dict[str, Any]]:
    cur = core_con.execute(
        """
        SELECT
          planet_id,
          planet_name,
          semi_major_axis_au,
          orbital_period_days,
          eccentricity,
          radius_earth,
          mass_earth,
          insol_earth,
          eq_temp_k
        FROM planets
        WHERE system_id = ?
        ORDER BY planet_name ASC NULLS LAST, planet_id ASC
        """,
        [system_id],
    )
    cols = [d[0] for d in cur.description]
    planets = [dict(zip(cols, row)) for row in cur.fetchall()]
    planet_ids = [int(row["planet_id"]) for row in planets if row.get("planet_id") is not None]
    derived_rows = _derived_parameter_rows(arm_con, planet_ids)
    for planet in planets:
        sma, sma_status, sma_layer, sma_basis = _resolved_planet_value(
            planet,
            derived_rows,
            "semi_major_axis_au",
            "semi_major_axis_au",
        )
        insol, insol_status, insol_layer, insol_basis = _resolved_planet_value(
            planet,
            derived_rows,
            "insol_earth",
            "insol_earth",
        )
        eq_temp, eq_temp_status, eq_temp_layer, eq_temp_basis = _resolved_planet_value(
            planet,
            derived_rows,
            "eq_temp_k",
            "eq_temp_k",
        )
        planet["snapshot_semi_major_axis_au"] = sma
        planet["snapshot_semi_major_axis_status"] = sma_status
        planet["snapshot_semi_major_axis_layer"] = sma_layer
        planet["snapshot_semi_major_axis_basis"] = sma_basis
        planet["snapshot_insol_earth"] = insol
        planet["snapshot_insol_status"] = insol_status
        planet["snapshot_insol_layer"] = insol_layer
        planet["snapshot_insol_basis"] = insol_basis
        planet["snapshot_eq_temp_k"] = eq_temp
        planet["snapshot_eq_temp_status"] = eq_temp_status
        planet["snapshot_eq_temp_layer"] = eq_temp_layer
        planet["snapshot_eq_temp_basis"] = eq_temp_basis
    return sorted(
        planets,
        key=lambda row: (
            _float_or_none(row.get("snapshot_semi_major_axis_au")) is None,
            _float_or_none(row.get("snapshot_semi_major_axis_au")) or _float_or_none(row.get("orbital_period_days")) or float("inf"),
            str(row.get("planet_name") or ""),
            int(row.get("planet_id") or 0),
        ),
    )


def _hash_seed(*parts: str) -> int:
    text = "||".join(parts).encode("utf-8")
    return int(hashlib.sha256(text).hexdigest()[:16], 16)


def _star_color(spectral_class: Any) -> str:
    key = str(spectral_class or "").strip().upper()[:1]
    palette = {
        "O": "#89c8ff",
        "B": "#a7d8ff",
        "A": "#d8ecff",
        "F": "#fff2ce",
        "G": "#ffd98f",
        "K": "#ffba7a",
        "M": "#ff8c74",
        "L": "#e07f53",
        "T": "#b46f4f",
        "Y": "#8f664f",
    }
    return palette.get(key, "#dfe7ef")


def _planet_color(eq_temp_k: Any) -> str:
    if eq_temp_k is None:
        return "#9fc3d9"
    try:
        temp = float(eq_temp_k)
    except Exception:
        return "#9fc3d9"
    if temp >= 1500:
        return "#ff6b4a"
    if temp >= 900:
        return "#ffa15a"
    if temp >= 450:
        return "#f2c96a"
    if temp >= 220:
        return "#8bd2a8"
    return "#83b9ff"


def _planet_snapshot_order_key(row: Dict[str, Any]) -> tuple[bool, float, str, int]:
    orbit = _float_or_none(row.get("snapshot_semi_major_axis_au"))
    period = _float_or_none(row.get("orbital_period_days"))
    return (
        orbit is None,
        orbit if orbit is not None else (period if period is not None else float("inf")),
        str(row.get("planet_name") or ""),
        int(row.get("planet_id") or 0),
    )


def _orbit_radius_map(planets: Sequence[Dict[str, Any]], orbit_inner: int, orbit_step: int, max_orbits: int) -> Dict[int, float]:
    rendered = list(planets[:18])
    max_radius = orbit_inner + ((max_orbits - 1) * orbit_step)
    orbit_values = [
        _float_or_none(row.get("snapshot_semi_major_axis_au"))
        for row in rendered
        if _float_or_none(row.get("snapshot_semi_major_axis_au")) is not None
        and _float_or_none(row.get("snapshot_semi_major_axis_au")) > 0
    ]
    out: Dict[int, float] = {}
    if orbit_values:
        min_log = math.log10(max(min(orbit_values), 1e-6))
        max_log = math.log10(max(max(orbit_values), 1e-6))
        span = max(max_log - min_log, 1e-9)
        fallback_index = 0
        for idx, planet in enumerate(rendered):
            planet_id = int(planet.get("planet_id") or idx)
            orbit = _float_or_none(planet.get("snapshot_semi_major_axis_au"))
            if orbit is not None and orbit > 0:
                normalized = (math.log10(max(orbit, 1e-6)) - min_log) / span if span > 1e-8 else 0.5
                out[planet_id] = orbit_inner + (normalized * max(1.0, max_radius - orbit_inner))
            else:
                out[planet_id] = orbit_inner + (min(fallback_index, max_orbits - 1) * orbit_step)
                fallback_index += 1
        return out
    for idx, planet in enumerate(rendered):
        out[int(planet.get("planet_id") or idx)] = orbit_inner + (min(idx, max_orbits - 1) * orbit_step)
    return out


def _orbit_geometry(
    *,
    cx: int,
    cy: int,
    semi_major_px: float,
    eccentricity: Any,
    angle_rad: float,
) -> Dict[str, float]:
    ecc = _float_or_none(eccentricity)
    if ecc is None:
        ecc = 0.0
    ecc = max(0.0, min(0.85, ecc))
    semi_minor_px = semi_major_px * math.sqrt(max(0.0, 1.0 - (ecc * ecc)))
    center_offset = -ecc * semi_major_px
    cos_a = math.cos(angle_rad)
    sin_a = math.sin(angle_rad)
    center_x = float(cx) + (center_offset * cos_a)
    center_y = float(cy) + (center_offset * sin_a)
    return {
        "eccentricity": ecc,
        "semi_major_px": semi_major_px,
        "semi_minor_px": semi_minor_px,
        "center_x": center_x,
        "center_y": center_y,
        "rotation_deg": angle_rad * 180.0 / math.pi,
    }


def _orbit_point(
    *,
    cx: int,
    cy: int,
    semi_major_px: float,
    eccentricity: Any,
    apsis_angle_rad: float,
    anomaly_rad: float,
) -> tuple[float, float]:
    geom = _orbit_geometry(
        cx=cx,
        cy=cy,
        semi_major_px=semi_major_px,
        eccentricity=eccentricity,
        angle_rad=apsis_angle_rad,
    )
    ecc = geom["eccentricity"]
    semi_minor_px = geom["semi_minor_px"]
    x_local = semi_major_px * math.cos(anomaly_rad) - (ecc * semi_major_px)
    y_local = semi_minor_px * math.sin(anomaly_rad)
    cos_a = math.cos(apsis_angle_rad)
    sin_a = math.sin(apsis_angle_rad)
    x = float(cx) + (x_local * cos_a) - (y_local * sin_a)
    y = float(cy) + (x_local * sin_a) + (y_local * cos_a)
    return x, y


def _render_snapshot_svg(
    system_row: Dict[str, Any],
    stars: List[Dict[str, Any]],
    planets: List[Dict[str, Any]],
    *,
    width_px: int,
    height_px: int,
) -> str:
    w = int(width_px)
    h = int(height_px)
    orbit_cx = int(w * 0.50)
    orbit_cy = int(h * 0.52)
    max_orbits = min(max(len(planets), 1), 14)
    orbit_step = max(11, int(min(w, h) * 0.028))
    orbit_inner = int(min(w, h) * 0.10)
    orbit_outer = orbit_inner + (max_orbits * orbit_step)
    planets_for_render = sorted(planets, key=_planet_snapshot_order_key)
    orbit_radius_by_planet_id = _orbit_radius_map(planets_for_render, orbit_inner, orbit_step, max_orbits)

    system_name = str(system_row.get("system_name") or "Unknown system")
    stable_key = str(system_row.get("stable_object_key") or "unknown")
    star_count = len(stars)
    seed_prefix = f"{stable_key}|{system_row.get('system_id')}"
    callouts: List[Dict[str, Any]] = []

    pieces: List[str] = []
    scene: List[str] = []
    bbox = {
        "min_x": float("inf"),
        "min_y": float("inf"),
        "max_x": float("-inf"),
        "max_y": float("-inf"),
    }

    def _touch(x: float, y: float) -> None:
        if x < bbox["min_x"]:
            bbox["min_x"] = x
        if y < bbox["min_y"]:
            bbox["min_y"] = y
        if x > bbox["max_x"]:
            bbox["max_x"] = x
        if y > bbox["max_y"]:
            bbox["max_y"] = y

    def _touch_circle(cx: float, cy: float, radius: float) -> None:
        _touch(cx - radius, cy - radius)
        _touch(cx + radius, cy + radius)

    def _touch_text(x: float, y: float, text: str, anchor: str, font_size: float = 11.2) -> None:
        width = max(24.0, float(len(text)) * (font_size * 0.58))
        if anchor == "end":
            x0 = x - width
            x1 = x
        elif anchor == "middle":
            x0 = x - (width * 0.5)
            x1 = x + (width * 0.5)
        else:
            x0 = x
            x1 = x + width
        y0 = y - (font_size * 0.95)
        y1 = y + (font_size * 0.2)
        _touch(x0, y0)
        _touch(x1, y1)

    pieces.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" role="img" aria-labelledby="title desc">')
    pieces.append(f"<title id=\"title\">{html.escape(system_name)} snapshot</title>")
    pieces.append("<desc id=\"desc\">Deterministic Spacegate system snapshot.</desc>")
    pieces.append(
        "<defs>"
        "<linearGradient id=\"bg\" x1=\"0\" y1=\"0\" x2=\"1\" y2=\"1\">"
        "<stop offset=\"0%\" stop-color=\"#0f1f2e\"/>"
        "<stop offset=\"55%\" stop-color=\"#172a3a\"/>"
        "<stop offset=\"100%\" stop-color=\"#0d1722\"/>"
        "</linearGradient>"
        "<radialGradient id=\"halo\" cx=\"50%\" cy=\"50%\" r=\"50%\">"
        "<stop offset=\"0%\" stop-color=\"#ffd799\" stop-opacity=\"0.45\"/>"
        "<stop offset=\"100%\" stop-color=\"#ffd799\" stop-opacity=\"0\"/>"
        "</radialGradient>"
        "</defs>"
    )
    pieces.append(f'<rect x="0" y="0" width="{w}" height="{h}" fill="url(#bg)" rx="20" ry="20"/>')
    pieces.append(f'<rect x="18" y="18" width="{w - 36}" height="{h - 36}" fill="none" stroke="#3a5064" stroke-opacity="0.42" rx="16" ry="16"/>')

    scene.append(
        f'<circle cx="{orbit_cx}" cy="{orbit_cy}" r="{orbit_outer + 38}" fill="none" stroke="#355166" stroke-opacity="0.28" stroke-dasharray="3 8"/>'
    )
    _touch_circle(float(orbit_cx), float(orbit_cy), float(orbit_outer + 38))
    for idx, planet in enumerate(planets_for_render[:max_orbits]):
        planet_id = int(planet.get("planet_id") or idx)
        radius = orbit_radius_by_planet_id.get(planet_id, orbit_inner + (idx * orbit_step))
        seed = _hash_seed(seed_prefix, "orbit", str(planet.get("planet_id") or idx), str(planet.get("planet_name") or ""))
        apsis_angle = (seed % 1800) / 10.0 * (math.pi / 180.0)
        geom = _orbit_geometry(
            cx=orbit_cx,
            cy=orbit_cy,
            semi_major_px=float(radius),
            eccentricity=planet.get("eccentricity"),
            angle_rad=apsis_angle,
        )
        opacity = 0.24 if idx % 2 == 0 else 0.16
        scene.append(
            f'<ellipse cx="{geom["center_x"]:.2f}" cy="{geom["center_y"]:.2f}" '
            f'rx="{geom["semi_major_px"]:.2f}" ry="{geom["semi_minor_px"]:.2f}" '
            f'transform="rotate({geom["rotation_deg"]:.2f} {geom["center_x"]:.2f} {geom["center_y"]:.2f})" '
            f'fill="none" stroke="#97b5cc" stroke-opacity="{opacity:.2f}" />'
        )
        _touch_circle(float(orbit_cx), float(orbit_cy), float(radius) * (1.0 + geom["eccentricity"]))

    scene.append(f'<circle cx="{orbit_cx}" cy="{orbit_cy}" r="72" fill="url(#halo)"/>')
    _touch_circle(float(orbit_cx), float(orbit_cy), 72.0)
    if not stars:
        scene.append(f'<circle cx="{orbit_cx}" cy="{orbit_cy}" r="8" fill="#ffe4a8" stroke="#ffeec7" stroke-width="1"/>')
        _touch_circle(float(orbit_cx), float(orbit_cy), 8.0)
    else:
        for idx, star in enumerate(stars[:6]):
            seed = _hash_seed(seed_prefix, "star", str(star.get("star_id") or idx))
            angle = (seed % 360) * (math.pi / 180.0)
            radial = 6 + (idx * 9)
            x = orbit_cx + int(math.cos(angle) * radial)
            y = orbit_cy + int(math.sin(angle) * radial)
            vmag = star.get("vmag")
            try:
                r_base = 8.2 - float(vmag) * 0.35 if vmag is not None else 5.8
            except Exception:
                r_base = 5.8
            radius = max(3.2, min(10.5, r_base))
            color = _star_color(star.get("spectral_class"))
            scene.append(
                f'<circle cx="{x}" cy="{y}" r="{radius:.2f}" fill="{color}" stroke="#f5f8ff" stroke-opacity="0.45" stroke-width="0.6"/>'
            )
            _touch_circle(float(x), float(y), float(radius))
            if idx < 4:
                label = str(star.get("star_name") or "").strip()
                component = str(star.get("component") or "").strip()
                if not label:
                    label = f"{system_name} {component}".strip() if component else f"{system_name} star"
                callouts.append(
                    {
                        "kind": "star",
                        "priority": idx,
                        "label": label[:40],
                        "x": float(x),
                        "y": float(y),
                        "radius": float(radius),
                    }
                )

    for idx, planet in enumerate(planets_for_render[:18]):
        planet_id = int(planet.get("planet_id") or idx)
        radius = orbit_radius_by_planet_id.get(planet_id, orbit_inner + (min(idx, max_orbits - 1) * orbit_step))
        seed = _hash_seed(seed_prefix, "planet", str(planet.get("planet_id") or idx), str(planet.get("planet_name") or ""))
        apsis_angle = (_hash_seed(seed_prefix, "orbit", str(planet.get("planet_id") or idx), str(planet.get("planet_name") or "")) % 1800) / 10.0 * (math.pi / 180.0)
        anomaly = (seed % 3600) / 10.0 * (math.pi / 180.0)
        x, y = _orbit_point(
            cx=orbit_cx,
            cy=orbit_cy,
            semi_major_px=float(radius),
            eccentricity=planet.get("eccentricity"),
            apsis_angle_rad=apsis_angle,
            anomaly_rad=anomaly,
        )
        pr = planet.get("radius_earth")
        try:
            pr_val = float(pr) if pr is not None else 1.0
        except Exception:
            pr_val = 1.0
        dot_r = max(2.0, min(6.8, 1.8 + (pr_val ** 0.45)))
        color = _planet_color(planet.get("snapshot_eq_temp_k"))
        scene.append(
            f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{dot_r:.2f}" fill="{color}" stroke="#f9f9ff" stroke-opacity="0.38" stroke-width="0.55"/>'
        )
        _touch_circle(float(x), float(y), float(dot_r))
        if idx < 6:
            label = str(planet.get("planet_name") or "").strip() or f"{system_name} planet {idx + 1}"
            callouts.append(
                {
                    "kind": "planet",
                    "priority": idx,
                    "label": label[:40],
                    "x": float(x),
                    "y": float(y),
                    "radius": float(dot_r),
                }
            )

    def _resolve_side(points: List[Dict[str, Any]], side: str) -> List[Dict[str, Any]]:
        if not points:
            return []
        ordered = sorted(points, key=lambda row: row["y"])
        min_y = 40
        max_y = h - 24
        gap = 18
        placed: List[Dict[str, Any]] = []
        cursor_y = min_y - gap
        for row in ordered:
            target = int(round(float(row["y"])))
            y = max(target, cursor_y + gap)
            if y > max_y:
                y = max_y
            cursor_y = y
            out = dict(row)
            out["label_y"] = y
            out["side"] = side
            placed.append(out)
        return placed

    left_points = [row for row in callouts if row["x"] < orbit_cx]
    right_points = [row for row in callouts if row["x"] >= orbit_cx]
    left_draw = _resolve_side(left_points, "left")
    right_draw = _resolve_side(right_points, "right")
    draw_points = sorted(
        left_draw + right_draw,
        key=lambda row: (0 if row["kind"] == "star" else 1, row["priority"]),
    )
    for row in draw_points:
        dx = row["x"] - orbit_cx
        dy = row["y"] - orbit_cy
        norm = math.hypot(dx, dy)
        ux = (dx / norm) if norm > 0 else (1.0 if row["side"] == "right" else -1.0)
        uy = (dy / norm) if norm > 0 else 0.0
        start_x = row["x"] + ux * (row["radius"] + 1.2)
        start_y = row["y"] + uy * (row["radius"] + 1.2)
        elbow_x = row["x"] + ux * 20.0
        label_y = float(row["label_y"])
        if row["side"] == "right":
            edge_label_x = float(w - 20)
            edge_leader_x = edge_label_x - 8.0
            leader_x = elbow_x + ((edge_leader_x - elbow_x) * 0.5)
            label_x = leader_x + 6.0
            anchor = "start"
        else:
            edge_label_x = 20.0
            edge_leader_x = edge_label_x + 8.0
            leader_x = elbow_x + ((edge_leader_x - elbow_x) * 0.5)
            label_x = leader_x - 6.0
            anchor = "end"
        stroke = "#a7d9ff" if row["kind"] == "star" else "#ffd8a0"
        text_fill = "#dce9f2" if row["kind"] == "star" else "#ffe2be"
        scene.append(
            f'<polyline points="{start_x:.1f},{start_y:.1f} {elbow_x:.1f},{label_y:.1f} {leader_x:.1f},{label_y:.1f}" '
            f'fill="none" stroke="{stroke}" stroke-opacity="0.76" stroke-width="0.9"/>'
        )
        _touch(start_x, start_y)
        _touch(elbow_x, label_y)
        _touch(leader_x, label_y)
        scene.append(
            f'<text x="{label_x:.1f}" y="{label_y + 3.8:.1f}" text-anchor="{anchor}" fill="{text_fill}" '
            f'font-size="11.2" font-family="Spline Sans Mono, monospace">{html.escape(str(row["label"]))}</text>'
        )
        _touch_text(float(label_x), float(label_y + 3.8), str(row["label"]), anchor, 11.2)

    if math.isinf(bbox["min_x"]) or math.isinf(bbox["min_y"]) or math.isinf(bbox["max_x"]) or math.isinf(bbox["max_y"]):
        bbox["min_x"] = 0.0
        bbox["min_y"] = 0.0
        bbox["max_x"] = float(w)
        bbox["max_y"] = float(h)

    pad = 24.0
    bw = max(1.0, bbox["max_x"] - bbox["min_x"])
    bh = max(1.0, bbox["max_y"] - bbox["min_y"])
    sx = max(0.1, (float(w) - (2.0 * pad)) / bw)
    sy = max(0.1, (float(h) - (2.0 * pad)) / bh)
    scale = min(sx, sy)
    scale = min(scale, 1.8)
    tx = pad - (bbox["min_x"] * scale) + (((float(w) - (2.0 * pad)) - (bw * scale)) * 0.5)
    ty = pad - (bbox["min_y"] * scale) + (((float(h) - (2.0 * pad)) - (bh * scale)) * 0.5)

    pieces.append(f'<g transform="translate({tx:.2f} {ty:.2f}) scale({scale:.5f})">')
    pieces.extend(scene)
    pieces.append("</g>")

    pieces.append("</svg>")
    return "".join(pieces)


def _source_inputs_hash(
    system_row: Dict[str, Any],
    stars: List[Dict[str, Any]],
    planets: List[Dict[str, Any]],
    params: Dict[str, Any],
) -> str:
    payload = {
        "system": {
            "system_id": system_row.get("system_id"),
            "stable_object_key": system_row.get("stable_object_key"),
            "system_name": system_row.get("system_name"),
            "dist_ly": system_row.get("dist_ly"),
            "ra_deg": system_row.get("ra_deg"),
            "dec_deg": system_row.get("dec_deg"),
            "x_helio_ly": system_row.get("x_helio_ly"),
            "y_helio_ly": system_row.get("y_helio_ly"),
            "z_helio_ly": system_row.get("z_helio_ly"),
        },
        "stars": [
            {
                "star_id": row.get("star_id"),
                "star_name": row.get("star_name"),
                "component": row.get("component"),
                "spectral_class": row.get("spectral_class"),
                "spectral_type_raw": row.get("spectral_type_raw"),
                "vmag": row.get("vmag"),
                "dist_ly": row.get("dist_ly"),
            }
            for row in stars
        ],
        "planets": [
            {
                "planet_id": row.get("planet_id"),
                "planet_name": row.get("planet_name"),
                "semi_major_axis_au": row.get("semi_major_axis_au"),
                "snapshot_semi_major_axis_au": row.get("snapshot_semi_major_axis_au"),
                "snapshot_semi_major_axis_status": row.get("snapshot_semi_major_axis_status"),
                "snapshot_semi_major_axis_basis": row.get("snapshot_semi_major_axis_basis"),
                "orbital_period_days": row.get("orbital_period_days"),
                "eccentricity": row.get("eccentricity"),
                "radius_earth": row.get("radius_earth"),
                "mass_earth": row.get("mass_earth"),
                "insol_earth": row.get("insol_earth"),
                "snapshot_insol_earth": row.get("snapshot_insol_earth"),
                "snapshot_insol_status": row.get("snapshot_insol_status"),
                "snapshot_insol_basis": row.get("snapshot_insol_basis"),
                "eq_temp_k": row.get("eq_temp_k"),
                "snapshot_eq_temp_k": row.get("snapshot_eq_temp_k"),
                "snapshot_eq_temp_status": row.get("snapshot_eq_temp_status"),
                "snapshot_eq_temp_basis": row.get("snapshot_eq_temp_basis"),
            }
            for row in planets
        ],
        "params": params,
    }
    return hashlib.sha256(_json_canonical(payload).encode("utf-8")).hexdigest()


def _ensure_manifest_table(disc_con: duckdb.DuckDBPyConnection) -> None:
    disc_con.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshot_manifest (
          stable_object_key VARCHAR,
          system_id BIGINT,
          object_type VARCHAR,
          view_type VARCHAR,
          params_json VARCHAR,
          params_hash VARCHAR,
          generator_version VARCHAR,
          build_id VARCHAR,
          artifact_path VARCHAR,
          artifact_mime VARCHAR,
          width_px INTEGER,
          height_px INTEGER,
          source_build_inputs_hash VARCHAR,
          created_at TIMESTAMP
        )
        """
    )


def _upsert_manifest_rows(
    disc_con: duckdb.DuckDBPyConnection,
    rows: Iterable[Dict[str, Any]],
) -> int:
    count = 0
    for row in rows:
        disc_con.execute(
            """
            DELETE FROM snapshot_manifest
            WHERE build_id = ?
              AND view_type = ?
              AND stable_object_key = ?
              AND params_hash = ?
            """,
            [
                row["build_id"],
                row["view_type"],
                row["stable_object_key"],
                row["params_hash"],
            ],
        )
        disc_con.execute(
            """
            INSERT INTO snapshot_manifest (
              stable_object_key,
              system_id,
              object_type,
              view_type,
              params_json,
              params_hash,
              generator_version,
              build_id,
              artifact_path,
              artifact_mime,
              width_px,
              height_px,
              source_build_inputs_hash,
              created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::TIMESTAMP)
            """,
            [
                row["stable_object_key"],
                row["system_id"],
                row["object_type"],
                row["view_type"],
                row["params_json"],
                row["params_hash"],
                row["generator_version"],
                row["build_id"],
                row["artifact_path"],
                row["artifact_mime"],
                row["width_px"],
                row["height_px"],
                row["source_build_inputs_hash"],
                row["created_at"],
            ],
        )
        count += 1
    return count


def _export_manifest_parquet(
    disc_con: duckdb.DuckDBPyConnection,
    *,
    build_id: str,
    out_path: Path,
) -> Path:
    escaped_build = build_id.replace("'", "''")

    def _copy_to(target: Path) -> Path:
        target.parent.mkdir(parents=True, exist_ok=True)
        escaped_dst = str(target).replace("'", "''")
        disc_con.execute(
            f"""
            COPY (
              SELECT *
              FROM snapshot_manifest
              WHERE build_id = '{escaped_build}'
              ORDER BY stable_object_key ASC, view_type ASC, created_at DESC
            )
            TO '{escaped_dst}' (FORMAT PARQUET)
            """
        )
        return target

    try:
        return _copy_to(out_path)
    except duckdb.IOException:
        fallback_path = out_path.parent.parent / "snapshot_manifest.parquet"
        return _copy_to(fallback_path)


def run(args: argparse.Namespace) -> Dict[str, Any]:
    root = _root_dir()
    state_dir = _state_dir(root)
    build_id, build_dir = _resolve_build_dir(state_dir, args.build_id)

    core_con = _open_core_db(build_dir)
    arm_con = _open_arm_db(build_dir)

    params = {
        "schema": 1,
        "view_type": args.view_type,
        "width_px": int(args.width),
        "height_px": int(args.height),
        "style": "spacegate.v1.resolved-orbit-labeled-autofit",
    }
    params_hash = hashlib.sha256(_json_canonical(params).encode("utf-8")).hexdigest()[:16]

    system_rows = _load_system_rows(
        core_con,
        system_ids=args.system_id or [],
        limit=args.limit,
        top_coolness=args.top_coolness,
        disc_path=build_dir / "disc.duckdb",
        min_dist_ly=args.min_dist_ly,
        max_dist_ly=args.max_dist_ly,
        min_star_count=args.min_star_count,
        max_star_count=args.max_star_count,
        min_planet_count=args.min_planet_count,
        max_planet_count=args.max_planet_count,
        min_coolness_score=args.min_coolness_score,
        max_coolness_score=args.max_coolness_score,
    )

    disc_con, disc_temp_path, disc_target_path = _open_disc_db(build_dir)
    try:
        _ensure_manifest_table(disc_con)

        snapshots_root = build_dir / "snapshots" / args.view_type
        created_at = _utc_now()
        generated = 0
        reused = 0
        failed = 0
        skipped = 0
        selected_artifact_size_bytes = 0
        manifest_rows: List[Dict[str, Any]] = []
        preview_entries: List[Dict[str, Any]] = []
        total_requested = len(system_rows)
        progress_every = _progress_interval(total_requested)

        _emit_progress(
            {
                "build_id": build_id,
                "view_type": args.view_type,
                "stage": "selected",
                "requested": total_requested,
                "generated": generated,
                "reused": reused,
                "failed": failed,
                "skipped": skipped,
                "snapshot_root": str(snapshots_root),
                "params_hash": params_hash,
            }
        )

        for index, system_row in enumerate(system_rows, start=1):
            system_id = int(system_row["system_id"])
            stable_object_key = str(system_row.get("stable_object_key") or f"system_{system_id}")
            safe_key = _safe_system_key(stable_object_key, system_id)
            artifact_rel = Path("snapshots") / args.view_type / safe_key / f"{params_hash}.svg"
            artifact_abs = build_dir / artifact_rel
            artifact_abs.parent.mkdir(parents=True, exist_ok=True)

            stars = _load_system_stars(core_con, system_id)
            planets = _load_system_planets(core_con, arm_con, system_id)

            should_generate = args.force or (not artifact_abs.exists())
            if should_generate:
                svg = _render_snapshot_svg(
                    system_row,
                    stars,
                    planets,
                    width_px=int(args.width),
                    height_px=int(args.height),
                )
                artifact_abs.write_text(svg, encoding="utf-8")
                generated += 1
            else:
                reused += 1
            try:
                selected_artifact_size_bytes += artifact_abs.stat().st_size
            except OSError:
                pass

            source_hash = _source_inputs_hash(system_row, stars, planets, params)
            manifest_row = {
                "stable_object_key": stable_object_key,
                "system_id": system_id,
                "object_type": "system",
                "view_type": args.view_type,
                "params_json": _json_canonical(params),
                "params_hash": params_hash,
                "generator_version": args.generator_version,
                "build_id": build_id,
                "artifact_path": str(artifact_rel.as_posix()),
                "artifact_mime": "image/svg+xml",
                "width_px": int(args.width),
                "height_px": int(args.height),
                "source_build_inputs_hash": source_hash,
                "created_at": created_at,
            }
            manifest_rows.append(manifest_row)

            if len(preview_entries) < 8:
                preview_entries.append(
                    {
                        "system_id": system_id,
                        "stable_object_key": stable_object_key,
                        "system_name": system_row.get("system_name"),
                        "artifact_path": str(artifact_rel.as_posix()),
                    }
                )
            if index == total_requested or index % progress_every == 0:
                _emit_progress(
                    {
                        "build_id": build_id,
                        "view_type": args.view_type,
                        "stage": "rendering",
                        "processed": index,
                        "requested": total_requested,
                        "generated": generated,
                        "reused": reused,
                        "failed": failed,
                        "skipped": skipped,
                        "selected_artifact_size_bytes": selected_artifact_size_bytes,
                        "snapshot_root": str(snapshots_root),
                        "params_hash": params_hash,
                    }
                )

        manifest_count = _upsert_manifest_rows(disc_con, manifest_rows)
        manifest_parquet_path = _export_manifest_parquet(
            disc_con,
            build_id=build_id,
            out_path=build_dir / "disc" / "snapshot_manifest.parquet",
        )

        report_dir = state_dir / "reports" / build_id
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / "snapshot_report.json"
        report_payload = {
            "build_id": build_id,
            "generated_at": _utc_now(),
            "generator_version": args.generator_version,
            "view_type": args.view_type,
            "params": params,
            "selection": {
                "top_coolness": int(args.top_coolness),
                "limit": int(args.limit),
                "min_dist_ly": args.min_dist_ly,
                "max_dist_ly": args.max_dist_ly,
                "min_star_count": args.min_star_count,
                "max_star_count": args.max_star_count,
                "min_planet_count": args.min_planet_count,
                "max_planet_count": args.max_planet_count,
                "min_coolness_score": args.min_coolness_score,
                "max_coolness_score": args.max_coolness_score,
            },
            "params_hash": params_hash,
            "force": bool(args.force),
            "requested": len(system_rows),
            "generated": generated,
            "reused": reused,
            "failed": failed,
            "skipped": skipped,
            "manifest_rows_upserted": manifest_count,
            "manifest_parquet": str(manifest_parquet_path),
            "snapshot_root": str(snapshots_root),
            "selected_artifact_size_bytes": selected_artifact_size_bytes,
            "preview_entries": preview_entries,
        }
        report_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True), encoding="utf-8")

        _emit_progress(
            {
                "build_id": build_id,
                "view_type": args.view_type,
                "stage": "complete",
                "processed": total_requested,
                "requested": total_requested,
                "generated": generated,
                "reused": reused,
                "failed": failed,
                "skipped": skipped,
                "manifest_rows_upserted": manifest_count,
                "snapshot_root": str(snapshots_root),
                "selected_artifact_size_bytes": selected_artifact_size_bytes,
                "report_path": str(report_path),
                "params_hash": params_hash,
            }
        )

        return {
            "ok": True,
            "build_id": build_id,
            "view_type": args.view_type,
            "params_hash": params_hash,
            "generator_version": args.generator_version,
            "requested": len(system_rows),
            "generated": generated,
            "reused": reused,
            "failed": failed,
            "skipped": skipped,
            "manifest_rows_upserted": manifest_count,
            "snapshot_root": str(snapshots_root),
            "selected_artifact_size_bytes": selected_artifact_size_bytes,
            "manifest_parquet": str(manifest_parquet_path),
            "report_path": str(report_path),
            "examples": preview_entries,
        }
    finally:
        if arm_con is not None:
            arm_con.close()
        core_con.close()
        _finalize_disc_db(disc_con, disc_temp_path, disc_target_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate deterministic system snapshots and write snapshot_manifest "
            "rows into disc.duckdb."
        )
    )
    parser.add_argument("--build-id", default=None, help="Build ID to target (defaults to served/current).")
    parser.add_argument(
        "--system-id",
        action="append",
        type=int,
        default=[],
        help="Generate snapshots for specific system_id (can be repeated).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="How many systems to process when --system-id is not provided (default: 500).",
    )
    parser.add_argument(
        "--top-coolness",
        type=int,
        default=0,
        help="If >0 and disc.coolness_scores exists, generate for top-N coolness systems.",
    )
    parser.add_argument("--min-dist-ly", type=float, default=None)
    parser.add_argument("--max-dist-ly", type=float, default=None)
    parser.add_argument("--min-star-count", type=int, default=None)
    parser.add_argument("--max-star-count", type=int, default=None)
    parser.add_argument("--min-planet-count", type=int, default=None)
    parser.add_argument("--max-planet-count", type=int, default=None)
    parser.add_argument("--min-coolness-score", type=float, default=None)
    parser.add_argument("--max-coolness-score", type=float, default=None)
    parser.add_argument("--view-type", default=DEFAULT_VIEW_TYPE)
    parser.add_argument("--width", type=int, default=DEFAULT_WIDTH_PX)
    parser.add_argument("--height", type=int, default=DEFAULT_HEIGHT_PX)
    parser.add_argument("--generator-version", default=DEFAULT_GENERATOR_VERSION)
    parser.add_argument("--force", action="store_true", help="Regenerate snapshot assets even if files already exist.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.min_dist_ly is not None and args.min_dist_ly < 0:
        raise SystemExit("--min-dist-ly must be >= 0")
    if args.max_dist_ly is not None and args.max_dist_ly < 0:
        raise SystemExit("--max-dist-ly must be >= 0")
    if args.min_dist_ly is not None and args.max_dist_ly is not None and args.min_dist_ly > args.max_dist_ly:
        raise SystemExit("--min-dist-ly cannot be greater than --max-dist-ly")
    if args.min_star_count is not None and args.min_star_count < 0:
        raise SystemExit("--min-star-count must be >= 0")
    if args.max_star_count is not None and args.max_star_count < 0:
        raise SystemExit("--max-star-count must be >= 0")
    if args.min_star_count is not None and args.max_star_count is not None and args.min_star_count > args.max_star_count:
        raise SystemExit("--min-star-count cannot be greater than --max-star-count")
    if args.min_planet_count is not None and args.min_planet_count < 0:
        raise SystemExit("--min-planet-count must be >= 0")
    if args.max_planet_count is not None and args.max_planet_count < 0:
        raise SystemExit("--max-planet-count must be >= 0")
    if args.min_planet_count is not None and args.max_planet_count is not None and args.min_planet_count > args.max_planet_count:
        raise SystemExit("--min-planet-count cannot be greater than --max-planet-count")
    if args.min_coolness_score is not None and args.max_coolness_score is not None and args.min_coolness_score > args.max_coolness_score:
        raise SystemExit("--min-coolness-score cannot be greater than --max-coolness-score")
    result = run(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
