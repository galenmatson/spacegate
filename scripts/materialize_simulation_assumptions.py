#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import requests

MATERIALIZATION_VERSION = "simulation_assumptions_materializer_v1"
BENCHMARK_QUERIES = (
    "Castor",
    "Nu Sco",
    "Alpha Centauri",
    "Sirius",
    "Proxima Centauri",
    "TRAPPIST-1",
    "55 Cnc",
    "Sol",
    "16 Cyg",
)


def _root_dir() -> Path:
    return Path(__file__).resolve().parents[1]


def _state_dir(root: Path) -> Path:
    return Path(os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR") or root / "data")


def _resolve_symlink(path: Path) -> Path:
    try:
        return path.resolve(strict=True)
    except FileNotFoundError:
        return path


def _resolve_build_dir(state_dir: Path, build_id: str | None) -> tuple[str, Path]:
    if build_id:
        build_dir = state_dir / "out" / build_id
        if not build_dir.is_dir():
            raise SystemExit(f"Build directory not found: {build_dir}")
        return build_id, build_dir

    served_link = state_dir / "served" / "current"
    if served_link.exists():
        build_dir = _resolve_symlink(served_link)
        return build_dir.name, build_dir

    out_dir = state_dir / "out"
    candidates = [path for path in out_dir.iterdir() if path.is_dir() and not path.name.endswith(".tmp")]
    if not candidates:
        raise SystemExit(f"No build directories found in: {out_dir}")
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0].name, candidates[0]


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_canonical(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _assumption_key(record: dict[str, Any], build_id: str | None) -> str:
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


def _get_json(base_url: str, path: str, *, params: dict[str, Any] | None = None, timeout: int = 30) -> dict[str, Any]:
    response = requests.get(f"{base_url.rstrip('/')}{path}", params=params, timeout=timeout)
    if response.status_code != 200:
        raise SystemExit(f"{path} expected 200, got {response.status_code}: {response.text[:500]}")
    payload = response.json()
    if not isinstance(payload, dict):
        raise SystemExit(f"{path} returned non-object JSON")
    return payload


def _resolve_query_system_id(base_url: str, query: str) -> int:
    payload = _get_json(base_url, "/systems/search", params={"q": query, "limit": 1})
    items = payload.get("items") or []
    if not items:
        raise SystemExit(f"Query returned no systems: {query!r}")
    system_id = items[0].get("system_id")
    if system_id is None:
        raise SystemExit(f"Query result missing system_id: {query!r}")
    return int(system_id)


def _open_disc_db(build_dir: Path) -> tuple[duckdb.DuckDBPyConnection, Path | None, Path | None]:
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
    con: duckdb.DuckDBPyConnection,
    temp_path: Path | None,
    target_path: Path | None,
) -> None:
    con.close()
    if temp_path is None or target_path is None:
        return
    temp_path.chmod(0o664)
    os.replace(temp_path, target_path)


def _table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchall()
    return {str(row[0]) for row in rows}


def _ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS simulation_assumptions (
          assumption_key VARCHAR,
          object_type VARCHAR,
          system_id BIGINT,
          star_id BIGINT,
          planet_id BIGINT,
          orbit_edge_id BIGINT,
          stable_object_key VARCHAR,
          stable_component_key VARCHAR,
          render_key VARCHAR,
          display_name VARCHAR,
          parameter_key VARCHAR,
          value_json VARCHAR,
          unit VARCHAR,
          assumption_kind VARCHAR,
          assumption_method VARCHAR,
          assumption_version VARCHAR,
          input_context_json VARCHAR,
          replacement_target VARCHAR,
          visibility_label VARCHAR,
          layer VARCHAR,
          seed VARCHAR,
          generator_version VARCHAR,
          confidence DOUBLE,
          confidence_tier VARCHAR,
          notes VARCHAR,
          field_json VARCHAR,
          source_scene_schema_version VARCHAR,
          render_scene_schema_version VARCHAR,
          materialization_version VARCHAR,
          build_id VARCHAR,
          created_at TIMESTAMP
        )
        """
    )
    required = {
        "assumption_key": "VARCHAR",
        "orbit_edge_id": "BIGINT",
        "render_key": "VARCHAR",
        "display_name": "VARCHAR",
        "field_json": "VARCHAR",
        "source_scene_schema_version": "VARCHAR",
        "render_scene_schema_version": "VARCHAR",
        "materialization_version": "VARCHAR",
    }
    columns = _table_columns(con, "simulation_assumptions")
    for column_name, column_type in required.items():
        if column_name not in columns:
            con.execute(f"ALTER TABLE simulation_assumptions ADD COLUMN {column_name} {column_type}")


def _rows_from_scene(scene: dict[str, Any], *, build_id: str, created_at: str) -> list[dict[str, Any]]:
    render_scene = scene.get("render_scene") or {}
    assumptions = render_scene.get("assumptions") or []
    rows: list[dict[str, Any]] = []
    for assumption in assumptions:
        if not isinstance(assumption, dict):
            continue
        key = str(assumption.get("assumption_key") or _assumption_key(assumption, build_id))
        rows.append(
            {
                "assumption_key": key,
                "object_type": assumption.get("object_type"),
                "system_id": assumption.get("system_id"),
                "star_id": assumption.get("star_id"),
                "planet_id": assumption.get("planet_id"),
                "orbit_edge_id": assumption.get("orbit_edge_id"),
                "stable_object_key": assumption.get("stable_object_key"),
                "stable_component_key": assumption.get("stable_component_key"),
                "render_key": assumption.get("render_key"),
                "display_name": assumption.get("display_name"),
                "parameter_key": assumption.get("parameter_key"),
                "value_json": assumption.get("value_json"),
                "unit": assumption.get("unit"),
                "assumption_kind": assumption.get("assumption_kind"),
                "assumption_method": assumption.get("assumption_method"),
                "assumption_version": assumption.get("assumption_version"),
                "input_context_json": assumption.get("input_context_json"),
                "replacement_target": assumption.get("replacement_target"),
                "visibility_label": assumption.get("visibility_label") or "assumed",
                "layer": assumption.get("layer"),
                "seed": assumption.get("seed"),
                "generator_version": assumption.get("generator_version"),
                "confidence": assumption.get("confidence"),
                "confidence_tier": assumption.get("confidence_tier"),
                "notes": assumption.get("notes"),
                "field_json": json.dumps(assumption.get("field"), sort_keys=True),
                "source_scene_schema_version": scene.get("schema_version"),
                "render_scene_schema_version": render_scene.get("schema_version"),
                "materialization_version": MATERIALIZATION_VERSION,
                "build_id": build_id,
                "created_at": created_at,
            }
        )
    return rows


def _upsert_rows(con: duckdb.DuckDBPyConnection, rows: Iterable[dict[str, Any]]) -> int:
    count = 0
    for row in rows:
        con.execute(
            """
            DELETE FROM simulation_assumptions
            WHERE assumption_key = ?
            """,
            [row["assumption_key"]],
        )
        con.execute(
            """
            INSERT INTO simulation_assumptions (
              assumption_key,
              object_type,
              system_id,
              star_id,
              planet_id,
              orbit_edge_id,
              stable_object_key,
              stable_component_key,
              render_key,
              display_name,
              parameter_key,
              value_json,
              unit,
              assumption_kind,
              assumption_method,
              assumption_version,
              input_context_json,
              replacement_target,
              visibility_label,
              layer,
              seed,
              generator_version,
              confidence,
              confidence_tier,
              notes,
              field_json,
              source_scene_schema_version,
              render_scene_schema_version,
              materialization_version,
              build_id,
              created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::TIMESTAMP)
            """,
            [
                row["assumption_key"],
                row["object_type"],
                row["system_id"],
                row["star_id"],
                row["planet_id"],
                row["orbit_edge_id"],
                row["stable_object_key"],
                row["stable_component_key"],
                row["render_key"],
                row["display_name"],
                row["parameter_key"],
                row["value_json"],
                row["unit"],
                row["assumption_kind"],
                row["assumption_method"],
                row["assumption_version"],
                row["input_context_json"],
                row["replacement_target"],
                row["visibility_label"],
                row["layer"],
                row["seed"],
                row["generator_version"],
                row["confidence"],
                row["confidence_tier"],
                row["notes"],
                row["field_json"],
                row["source_scene_schema_version"],
                row["render_scene_schema_version"],
                row["materialization_version"],
                row["build_id"],
                row["created_at"],
            ],
        )
        count += 1
    return count


def _export_parquet(con: duckdb.DuckDBPyConnection, build_dir: Path, build_id: str) -> Path:
    target = build_dir / "disc" / "simulation_assumptions.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    escaped_target = str(target).replace("'", "''")
    escaped_build_id = build_id.replace("'", "''")
    try:
        con.execute(
            f"""
            COPY (
              SELECT *
              FROM simulation_assumptions
              WHERE build_id = '{escaped_build_id}'
              ORDER BY system_id, object_type, render_key, parameter_key, assumption_key
            )
            TO '{escaped_target}' (FORMAT PARQUET)
            """
        )
        return target
    except duckdb.IOException:
        fallback = build_dir / "simulation_assumptions.parquet"
        escaped_fallback = str(fallback).replace("'", "''")
        con.execute(
            f"""
            COPY (
              SELECT *
              FROM simulation_assumptions
              WHERE build_id = '{escaped_build_id}'
              ORDER BY system_id, object_type, render_key, parameter_key, assumption_key
            )
            TO '{escaped_fallback}' (FORMAT PARQUET)
            """
        )
        return fallback


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/api/v1")
    parser.add_argument("--build-id")
    parser.add_argument("--system-id", action="append", type=int, default=[])
    parser.add_argument("--query", action="append", default=[])
    parser.add_argument("--benchmarks", action="store_true", help="Materialize the current simulator benchmark systems.")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = _root_dir()
    build_id, build_dir = _resolve_build_dir(_state_dir(root), args.build_id)
    health = _get_json(args.base_url, "/health")
    served_build_id = str(health.get("build_id") or "")
    if served_build_id and served_build_id != build_id:
        raise SystemExit(
            f"API build_id {served_build_id!r} does not match target build {build_id!r}; "
            "start the matching API or pass the matching --build-id."
        )

    system_ids = list(args.system_id)
    queries = list(args.query)
    if args.benchmarks:
        queries.extend(BENCHMARK_QUERIES)
    for query in queries:
        system_ids.append(_resolve_query_system_id(args.base_url, query))
    system_ids = sorted(set(system_ids))
    if not system_ids:
        raise SystemExit("No systems selected. Pass --system-id, --query, or --benchmarks.")

    created_at = _utc_now()
    all_rows: list[dict[str, Any]] = []
    per_system: dict[int, int] = {}
    for system_id in system_ids:
        scene = _get_json(args.base_url, f"/systems/{system_id}/simulation-scene")
        rows = _rows_from_scene(scene, build_id=build_id, created_at=created_at)
        all_rows.extend(rows)
        per_system[system_id] = len(rows)

    if args.dry_run:
        print(
            json.dumps(
                {
                    "build_id": build_id,
                    "systems": per_system,
                    "assumption_count": len(all_rows),
                    "dry_run": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    con, temp_path, target_path = _open_disc_db(build_dir)
    parquet_path: Path | None = None
    try:
        _ensure_table(con)
        upserted = _upsert_rows(con, all_rows)
        parquet_path = _export_parquet(con, build_dir, build_id)
        con.execute("CHECKPOINT")
    finally:
        _finalize_disc_db(con, temp_path, target_path)

    print(
        json.dumps(
            {
                "build_id": build_id,
                "disc_db": str(build_dir / "disc.duckdb"),
                "parquet": str(parquet_path) if parquet_path else None,
                "systems": per_system,
                "upserted": upserted,
                "materialization_version": MATERIALIZATION_VERSION,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
