#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import duckdb


DEFAULT_VIEW_TYPE = "system_card"
DEFAULT_GENERATOR_VERSION = "snapshot-v1.0.4"
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


def _open_rich_db(build_dir: Path) -> duckdb.DuckDBPyConnection:
    rich_path = build_dir / "rich.duckdb"
    rich_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(rich_path), read_only=False)


def _load_system_rows(
    core_con: duckdb.DuckDBPyConnection,
    *,
    system_ids: Sequence[int],
    limit: int,
    top_coolness: int,
    rich_path: Path,
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

    if top_coolness > 0 and rich_path.exists():
        attached_rich = False
        try:
            escaped = str(rich_path).replace("'", "''")
            core_con.execute(f"ATTACH '{escaped}' AS rich_db (READ_ONLY)")
            attached_rich = True
            cur = core_con.execute(
                """
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
                JOIN rich_db.coolness_scores c USING (system_id)
                ORDER BY c.rank ASC, s.system_id ASC
                LIMIT ?
                """,
                [top_coolness],
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception:
            pass
        finally:
            if attached_rich:
                try:
                    core_con.execute("DETACH rich_db")
                except Exception:
                    pass

    sql = """
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
        ORDER BY COALESCE(dist_ly, 1e18) ASC, system_id ASC
    """
    params: list[Any] = []
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


def _load_system_planets(core_con: duckdb.DuckDBPyConnection, system_id: int) -> List[Dict[str, Any]]:
    cur = core_con.execute(
        """
        SELECT
          planet_id,
          planet_name,
          semi_major_axis_au,
          orbital_period_days,
          radius_earth,
          mass_earth,
          eq_temp_k
        FROM planets
        WHERE system_id = ?
        ORDER BY planet_name ASC NULLS LAST, planet_id ASC
        """,
        [system_id],
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


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
    for idx in range(max_orbits):
        radius = orbit_inner + (idx * orbit_step)
        opacity = 0.22 if idx % 2 == 0 else 0.14
        scene.append(
            f'<circle cx="{orbit_cx}" cy="{orbit_cy}" r="{radius}" fill="none" stroke="#97b5cc" stroke-opacity="{opacity:.2f}" />'
        )
        _touch_circle(float(orbit_cx), float(orbit_cy), float(radius))

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

    for idx, planet in enumerate(planets[:18]):
        ring_index = min(idx, max_orbits - 1)
        radius = orbit_inner + (ring_index * orbit_step)
        seed = _hash_seed(seed_prefix, "planet", str(planet.get("planet_id") or idx), str(planet.get("planet_name") or ""))
        angle = (seed % 3600) / 10.0 * (math.pi / 180.0)
        x = orbit_cx + int(math.cos(angle) * radius)
        y = orbit_cy + int(math.sin(angle) * radius)
        pr = planet.get("radius_earth")
        try:
            pr_val = float(pr) if pr is not None else 1.0
        except Exception:
            pr_val = 1.0
        dot_r = max(2.0, min(6.8, 1.8 + (pr_val ** 0.45)))
        color = _planet_color(planet.get("eq_temp_k"))
        scene.append(
            f'<circle cx="{x}" cy="{y}" r="{dot_r:.2f}" fill="{color}" stroke="#f9f9ff" stroke-opacity="0.38" stroke-width="0.55"/>'
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
                "orbital_period_days": row.get("orbital_period_days"),
                "radius_earth": row.get("radius_earth"),
                "mass_earth": row.get("mass_earth"),
                "eq_temp_k": row.get("eq_temp_k"),
            }
            for row in planets
        ],
        "params": params,
    }
    return hashlib.sha256(_json_canonical(payload).encode("utf-8")).hexdigest()


def _ensure_manifest_table(rich_con: duckdb.DuckDBPyConnection) -> None:
    rich_con.execute(
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
    rich_con: duckdb.DuckDBPyConnection,
    rows: Iterable[Dict[str, Any]],
) -> int:
    count = 0
    for row in rows:
        rich_con.execute(
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
        rich_con.execute(
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
    rich_con: duckdb.DuckDBPyConnection,
    *,
    build_id: str,
    out_path: Path,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    escaped_build = build_id.replace("'", "''")
    escaped_dst = str(out_path).replace("'", "''")
    rich_con.execute(
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


def run(args: argparse.Namespace) -> Dict[str, Any]:
    root = _root_dir()
    state_dir = _state_dir(root)
    build_id, build_dir = _resolve_build_dir(state_dir, args.build_id)

    core_con = _open_core_db(build_dir)

    params = {
        "schema": 1,
        "view_type": args.view_type,
        "width_px": int(args.width),
        "height_px": int(args.height),
        "style": "spacegate.v1.system-only-labeled-autofit",
    }
    params_hash = hashlib.sha256(_json_canonical(params).encode("utf-8")).hexdigest()[:16]

    system_rows = _load_system_rows(
        core_con,
        system_ids=args.system_id or [],
        limit=args.limit,
        top_coolness=args.top_coolness,
        rich_path=build_dir / "rich.duckdb",
    )

    rich_con = _open_rich_db(build_dir)
    _ensure_manifest_table(rich_con)

    snapshots_root = build_dir / "snapshots" / args.view_type
    created_at = _utc_now()
    generated = 0
    reused = 0
    manifest_rows: List[Dict[str, Any]] = []
    preview_entries: List[Dict[str, Any]] = []

    for system_row in system_rows:
        system_id = int(system_row["system_id"])
        stable_object_key = str(system_row.get("stable_object_key") or f"system_{system_id}")
        safe_key = _safe_system_key(stable_object_key, system_id)
        artifact_rel = Path("snapshots") / args.view_type / safe_key / f"{params_hash}.svg"
        artifact_abs = build_dir / artifact_rel
        artifact_abs.parent.mkdir(parents=True, exist_ok=True)

        stars = _load_system_stars(core_con, system_id)
        planets = _load_system_planets(core_con, system_id)

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

    manifest_count = _upsert_manifest_rows(rich_con, manifest_rows)
    manifest_parquet = build_dir / "rich" / "snapshot_manifest.parquet"
    _export_manifest_parquet(rich_con, build_id=build_id, out_path=manifest_parquet)

    report_dir = state_dir / "reports" / build_id
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "snapshot_report.json"
    report_payload = {
        "build_id": build_id,
        "generated_at": _utc_now(),
        "generator_version": args.generator_version,
        "view_type": args.view_type,
        "params": params,
        "params_hash": params_hash,
        "force": bool(args.force),
        "requested": len(system_rows),
        "generated": generated,
        "reused": reused,
        "manifest_rows_upserted": manifest_count,
        "manifest_parquet": str(manifest_parquet),
        "preview_entries": preview_entries,
    }
    report_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True), encoding="utf-8")

    core_con.close()
    rich_con.close()

    return {
        "ok": True,
        "build_id": build_id,
        "view_type": args.view_type,
        "params_hash": params_hash,
        "generator_version": args.generator_version,
        "requested": len(system_rows),
        "generated": generated,
        "reused": reused,
        "manifest_rows_upserted": manifest_count,
        "snapshot_root": str(snapshots_root),
        "manifest_parquet": str(manifest_parquet),
        "report_path": str(report_path),
        "examples": preview_entries,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate deterministic system snapshots and write snapshot_manifest "
            "rows into rich.duckdb."
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
        help="If >0 and rich.coolness_scores exists, generate for top-N coolness systems.",
    )
    parser.add_argument("--view-type", default=DEFAULT_VIEW_TYPE)
    parser.add_argument("--width", type=int, default=DEFAULT_WIDTH_PX)
    parser.add_argument("--height", type=int, default=DEFAULT_HEIGHT_PX)
    parser.add_argument("--generator-version", default=DEFAULT_GENERATOR_VERSION)
    parser.add_argument("--force", action="store_true", help="Regenerate snapshot assets even if files already exist.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
