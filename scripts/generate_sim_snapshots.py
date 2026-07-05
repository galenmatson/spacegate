#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import duckdb

from generate_snapshots import (  # type: ignore
    DEFAULT_HEIGHT_PX,
    DEFAULT_WIDTH_PX,
    DEFAULT_VIEW_TYPE,
    _emit_progress,
    _ensure_manifest_table,
    _export_manifest_parquet,
    _finalize_disc_db,
    _json_canonical,
    _load_system_rows,
    _open_core_db,
    _open_disc_db,
    _resolve_build_dir,
    _root_dir,
    _safe_system_key,
    _state_dir,
    _upsert_manifest_rows,
    _utc_now,
)


DEFAULT_GENERATOR_VERSION = "sim-snapshot-v0.1.0"
DEFAULT_STYLE = "spacegate.system_simulation.frame0.structure.v1"


def _metadata_build_id(core_con: duckdb.DuckDBPyConnection, fallback: str) -> str:
    row = core_con.execute(
        """
        SELECT value
        FROM build_metadata
        WHERE key = 'build_id'
        LIMIT 1
        """
    ).fetchone()
    value = str(row[0]).strip() if row and row[0] is not None else ""
    return value or fallback


def _source_inputs_hash(system_row: Dict[str, Any], params: Dict[str, Any]) -> str:
    payload = {
        "system": system_row,
        "params": params,
    }
    return hashlib.sha256(_json_canonical(payload).encode("utf-8")).hexdigest()


def _node_bin(root: Path) -> str:
    node = root / "srv" / "web" / "node_modules" / ".bin" / "node"
    if node.exists():
        return str(node)
    return "node"


def run(args: argparse.Namespace) -> Dict[str, Any]:
    root = _root_dir()
    state_dir = _state_dir(root)
    build_dir_id, build_dir = _resolve_build_dir(state_dir, args.build_id)
    core_con = _open_core_db(build_dir)
    build_id = _metadata_build_id(core_con, build_dir_id)

    params = {
        "schema": 1,
        "view_type": args.view_type,
        "width_px": int(args.width),
        "height_px": int(args.height),
        "style": DEFAULT_STYLE,
        "scale_mode": "structure",
        "simulation_days": 0,
        "camera": "system_preview_card_default_v1",
        "renderer": "three_r3f_system_preview",
        "theme": args.theme,
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
        jobs: List[Dict[str, Any]] = []
        manifest_by_system: Dict[int, Dict[str, Any]] = {}
        reused = 0
        skipped = 0
        failed = 0

        _emit_progress(
            {
                "build_id": build_id,
                "view_type": args.view_type,
                "stage": "selected",
                "requested": len(system_rows),
                "snapshot_root": str(snapshots_root),
                "params_hash": params_hash,
                "generator_version": args.generator_version,
            }
        )

        for system_row in system_rows:
            system_id = int(system_row["system_id"])
            stable_object_key = str(system_row.get("stable_object_key") or f"system_{system_id}")
            safe_key = _safe_system_key(stable_object_key, system_id)
            artifact_rel = Path("snapshots") / args.view_type / safe_key / f"{params_hash}.png"
            artifact_abs = build_dir / artifact_rel
            artifact_abs.parent.mkdir(parents=True, exist_ok=True)
            manifest_by_system[system_id] = {
                "stable_object_key": stable_object_key,
                "system_id": system_id,
                "object_type": "system",
                "view_type": args.view_type,
                "params_json": _json_canonical(params),
                "params_hash": params_hash,
                "generator_version": args.generator_version,
                "build_id": build_id,
                "artifact_path": str(artifact_rel.as_posix()),
                "artifact_mime": "image/png",
                "width_px": int(args.width),
                "height_px": int(args.height),
                "source_build_inputs_hash": _source_inputs_hash(system_row, params),
                "created_at": created_at,
            }
            if artifact_abs.exists() and not args.force:
                reused += 1
                continue
            jobs.append(
                {
                    "system_id": system_id,
                    "stable_object_key": stable_object_key,
                    "system_name": system_row.get("system_name"),
                    "output_path": str(artifact_abs),
                }
            )

        render_payload = {
            "base_url": args.base_url or os.getenv("SPACEGATE_SNAPSHOT_BASE_URL") or os.getenv("SPACEGATE_MAP_BASE_URL") or "https://10.0.0.12",
            "build_id": build_id,
            "view_type": args.view_type,
            "width_px": int(args.width),
            "height_px": int(args.height),
            "theme": args.theme,
            "jobs": jobs,
        }
        render_results: List[Dict[str, Any]] = []
        if jobs:
            with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as handle:
                json.dump(render_payload, handle, indent=2, sort_keys=True)
                jobs_path = Path(handle.name)
            try:
                cmd = [_node_bin(root), str(root / "scripts" / "render_sim_snapshots.mjs"), str(jobs_path)]
                proc = subprocess.run(cmd, cwd=str(root / "srv" / "web"), text=True, capture_output=True, check=False)
                if proc.returncode != 0:
                    raise SystemExit(
                        "sim snapshot renderer failed\n"
                        f"stdout:\n{proc.stdout}\n"
                        f"stderr:\n{proc.stderr}"
                    )
                renderer_payload = json.loads(proc.stdout or "{}")
                render_results = list(renderer_payload.get("results") or [])
            finally:
                try:
                    jobs_path.unlink()
                except OSError:
                    pass

        generated_ids = set()
        failed_entries: List[Dict[str, Any]] = []
        for result in render_results:
            system_id = int(result.get("system_id") or 0)
            if result.get("ok"):
                generated_ids.add(system_id)
            else:
                failed += 1
                failed_entries.append(result)

        manifest_rows: List[Dict[str, Any]] = []
        selected_artifact_size_bytes = 0
        for system_id, row in manifest_by_system.items():
            artifact_abs = build_dir / row["artifact_path"]
            if artifact_abs.exists() and (system_id in generated_ids or not jobs or not args.force):
                manifest_rows.append(row)
                try:
                    selected_artifact_size_bytes += artifact_abs.stat().st_size
                except OSError:
                    pass
            elif system_id not in generated_ids:
                skipped += 1

        manifest_count = _upsert_manifest_rows(disc_con, manifest_rows)
        manifest_parquet_path = _export_manifest_parquet(
            disc_con,
            build_id=build_id,
            out_path=build_dir / "disc" / "snapshot_manifest.parquet",
        )

        generated = len(generated_ids)
        report_dir = state_dir / "reports" / build_id
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / "sim_snapshot_report.json"
        report_payload = {
            "build_id": build_id,
            "generated_at": _utc_now(),
            "generator_version": args.generator_version,
            "view_type": args.view_type,
            "params": params,
            "params_hash": params_hash,
            "base_url": render_payload["base_url"],
            "requested": len(system_rows),
            "generated": generated,
            "reused": reused,
            "failed": failed,
            "skipped": skipped,
            "manifest_rows_upserted": manifest_count,
            "manifest_parquet": str(manifest_parquet_path),
            "snapshot_root": str(snapshots_root),
            "selected_artifact_size_bytes": selected_artifact_size_bytes,
            "failed_entries": failed_entries[:20],
        }
        report_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True), encoding="utf-8")
        _emit_progress(
            {
                "build_id": build_id,
                "view_type": args.view_type,
                "stage": "complete",
                "requested": len(system_rows),
                "generated": generated,
                "reused": reused,
                "failed": failed,
                "skipped": skipped,
                "manifest_rows_upserted": manifest_count,
                "snapshot_root": str(snapshots_root),
                "report_path": str(report_path),
                "params_hash": params_hash,
            }
        )
        return report_payload
    finally:
        core_con.close()
        _finalize_disc_db(disc_con, disc_temp_path, disc_target_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate deterministic frame-0 System Simulation PNG snapshots and write snapshot_manifest rows."
    )
    parser.add_argument("--build-id", default=None, help="Build ID to target (defaults to served/current).")
    parser.add_argument("--base-url", default=None, help="Public/local web base URL used by Playwright (default: env or https://10.0.0.12).")
    parser.add_argument("--system-id", action="append", type=int, default=[], help="Generate snapshots for specific system_id (can be repeated).")
    parser.add_argument("--limit", type=int, default=100, help="How many systems to process when --system-id is not provided.")
    parser.add_argument("--top-coolness", type=int, default=0, help="If >0 and disc.coolness_scores exists, generate for top-N coolness systems.")
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
    parser.add_argument("--height", type=int, default=552)
    parser.add_argument("--theme", default="simple_dark")
    parser.add_argument("--generator-version", default=DEFAULT_GENERATOR_VERSION)
    parser.add_argument("--force", action="store_true", help="Regenerate image assets even if files already exist.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.limit < 1 and not args.system_id:
        raise SystemExit("--limit must be >= 1 unless --system-id is provided")
    if args.width < 64 or args.height < 64:
        raise SystemExit("--width and --height must be >= 64")
    result = run(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
