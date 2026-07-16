#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import duckdb

MATERIALIZER_VERSION = "simulation_scene_artifact_v3"


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
    out_dir = state_dir / "out"
    if build_id:
        build_dir = out_dir / build_id
        if not build_dir.is_dir():
            raise SystemExit(f"Build directory not found: {build_dir}")
        return build_id, build_dir

    served_link = state_dir / "served" / "current"
    if served_link.exists():
        build_dir = _resolve_symlink(served_link)
        return build_dir.name, build_dir

    if not out_dir.is_dir():
        raise SystemExit(f"Missing build output directory: {out_dir}")
    candidates = [path for path in out_dir.iterdir() if path.is_dir() and not path.name.endswith(".tmp")]
    if not candidates:
        raise SystemExit(f"No build directories found in: {out_dir}")
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0].name, candidates[0]


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _rows_to_dicts(cur: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


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


def _select_system_rows(
    build_dir: Path,
    *,
    system_ids: Sequence[int],
    limit: int,
    sort: str,
    priority_profile: str,
    top_coolness_limit: int,
    min_dist_ly: float | None,
    max_dist_ly: float | None,
    min_star_count: int | None,
    min_planet_count: int | None,
) -> list[dict[str, Any]]:
    core_path = build_dir / "core.duckdb"
    disc_path = build_dir / "disc.duckdb"
    if not core_path.exists():
        raise SystemExit(f"Missing core.duckdb in build: {core_path}")

    con = duckdb.connect(str(core_path), read_only=True)
    attached_disc = False
    try:
        if system_ids:
            placeholders = ",".join(["?"] * len(system_ids))
            return _rows_to_dicts(
                con.execute(
                    f"""
                    SELECT system_id, stable_object_key, system_name, dist_ly
                    FROM systems
                    WHERE system_id IN ({placeholders})
                    ORDER BY system_id ASC
                    """,
                    list(system_ids),
                )
            )

        has_coolness = False
        if disc_path.exists():
            escaped = str(disc_path).replace("'", "''")
            con.execute(f"ATTACH '{escaped}' AS disc_db (READ_ONLY)")
            attached_disc = True
            has_coolness = _duckdb_has_table(con, "coolness_scores")

        conditions: list[str] = []
        params: list[Any] = []
        if min_dist_ly is not None:
            conditions.append("s.dist_ly >= ?")
            params.append(float(min_dist_ly))
        if max_dist_ly is not None:
            conditions.append("s.dist_ly <= ?")
            params.append(float(max_dist_ly))
        if min_star_count is not None:
            conditions.append("COALESCE(s.star_count, 0) >= ?")
            params.append(int(min_star_count))
        if min_planet_count is not None:
            conditions.append("COALESCE(s.planet_count, 0) >= ?")
            params.append(int(min_planet_count))
        if priority_profile == "search-preview":
            priority_clauses = [
                "COALESCE(s.planet_count, 0) > 0",
                "COALESCE(s.star_count, 0) > 1",
                """
                EXISTS (
                  SELECT 1
                  FROM stars st
                  WHERE st.system_id = s.system_id
                    AND (
                      UPPER(COALESCE(st.spectral_type_raw, '')) LIKE 'D%'
                      OR UPPER(COALESCE(st.spectral_class, '')) IN ('D', 'WR')
                    )
                )
                """,
                """
                lower(COALESCE(s.system_name, s.stable_object_key, '')) IN (
                  'tau ceti',
                  'trappist-1',
                  'alpha centauri',
                  'proxima centauri',
                  'sirius',
                  '55 cnc',
                  'epsilon eridani',
                  'barnard''s star',
                  'wolf 359',
                  'vega',
                  'fomalhaut'
                )
                """,
            ]
            if has_coolness and top_coolness_limit > 0:
                priority_clauses.append("COALESCE(c.rank, 9223372036854775807) <= ?")
                params.append(int(top_coolness_limit))
            conditions.append("(" + " OR ".join(priority_clauses) + ")")
        where_sql = "WHERE " + " AND ".join(conditions) if conditions else ""

        coolness_select = "NULL::DOUBLE AS coolness_score, NULL::BIGINT AS coolness_rank"
        coolness_join = ""
        if has_coolness:
            coolness_select = "c.score_total AS coolness_score, c.rank AS coolness_rank"
            coolness_join = "LEFT JOIN disc_db.coolness_scores c USING (system_id)"

        if sort == "coolness":
            if not has_coolness:
                raise SystemExit("Coolness sort requested, but disc.coolness_scores is unavailable.")
            order_sql = "COALESCE(c.rank, 9223372036854775807) ASC, COALESCE(s.dist_ly, 1e12) ASC, s.system_id ASC"
        elif sort == "name":
            order_sql = "lower(COALESCE(s.system_name, s.stable_object_key, '')) ASC, s.system_id ASC"
        else:
            order_sql = "COALESCE(s.dist_ly, 1e12) ASC, s.system_id ASC"

        sql = f"""
            SELECT
              s.system_id,
              s.stable_object_key,
              s.system_name,
              s.dist_ly,
              COALESCE(s.star_count, 0) AS star_count,
              COALESCE(s.planet_count, 0) AS planet_count,
              {coolness_select}
            FROM systems s
            {coolness_join}
            {where_sql}
            ORDER BY {order_sql}
            LIMIT ?
        """
        params.append(max(1, int(limit)))
        return _rows_to_dicts(con.execute(sql, params))
    finally:
        if attached_disc:
            try:
                con.execute("DETACH disc_db")
            except Exception:
                pass
        con.close()


def _emit_progress(payload: dict[str, Any]) -> None:
    print("[simulation-scene-progress] " + json.dumps(payload, sort_keys=True), flush=True)


def _progress_interval(total: int) -> int:
    return max(10, min(1000, max(1, total // 50)))


def _load_scene_builder(root: Path, build_dir: Path):
    api_root = root / "srv" / "api"
    sys.path.insert(0, str(api_root))
    os.environ["SPACEGATE_DB_PATH"] = str(build_dir / "core.duckdb")
    os.environ.setdefault("SPACEGATE_STATE_DIR", str(_state_dir(root)))
    from app.main import _system_simulation_scene_payload  # noqa: PLC0415

    return _system_simulation_scene_payload


def _write_scene(path: Path, payload: dict[str, Any]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    with path.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=6, mtime=0) as f:
            f.write(encoded)
    path.chmod(0o664)
    return path.stat().st_size


def _scene_artifact_reusable(path: Path, *, build_id: str) -> bool:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return False
    materialization = payload.get("materialization") if isinstance(payload, dict) else None
    return (
        isinstance(materialization, dict)
        and materialization.get("materializer_version") == MATERIALIZER_VERSION
        and materialization.get("build_id") == build_id
    )


def run(args: argparse.Namespace) -> dict[str, Any]:
    started = time.perf_counter()
    root = _root_dir()
    state_dir = _state_dir(root)
    if args.build_dir:
        build_dir = Path(args.build_dir).resolve()
        build_id = args.build_id or build_dir.name.removesuffix(".tmp")
        if not (build_dir / "core.duckdb").exists():
            raise SystemExit(f"Build directory does not contain core.duckdb: {build_dir}")
    else:
        build_id, build_dir = _resolve_build_dir(state_dir, args.build_id)
    build_dir = build_dir.resolve()
    scene_builder = _load_scene_builder(root, build_dir)

    system_rows = _select_system_rows(
        build_dir,
        system_ids=args.system_id,
        limit=args.limit,
        sort=args.sort,
        priority_profile=args.priority_profile,
        top_coolness_limit=args.top_coolness_limit,
        min_dist_ly=args.min_dist_ly,
        max_dist_ly=args.max_dist_ly,
        min_star_count=args.min_star_count,
        min_planet_count=args.min_planet_count,
    )
    output_dir = build_dir / "disc" / "simulation_scenes"
    reports_dir = state_dir / "reports" / build_id
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / "simulation_scene_cache_report.json"

    requested = len(system_rows)
    generated = 0
    reused = 0
    incompatible_existing = 0
    failed = 0
    total_bytes = 0
    examples: list[dict[str, Any]] = []
    interval = _progress_interval(requested)

    _emit_progress(
        {
            "stage": "start",
            "build_id": build_id,
            "requested": requested,
            "output_dir": str(output_dir),
            "sort": args.sort,
            "force": bool(args.force),
        }
    )

    for idx, row in enumerate(system_rows, start=1):
        system_id = int(row["system_id"])
        out_path = output_dir / f"system_{system_id}.json.gz"
        reusable = out_path.exists() and not args.force and _scene_artifact_reusable(
            out_path,
            build_id=build_id,
        )
        if reusable:
            reused += 1
            total_bytes += out_path.stat().st_size
        else:
            if out_path.exists() and not args.force:
                incompatible_existing += 1
            try:
                payload = scene_builder(system_id, build_id=build_id)
                payload.setdefault("materialization", {})
                payload["materialization"] = {
                    "materialized": True,
                    "materializer_version": MATERIALIZER_VERSION,
                    "build_id": build_id,
                    "materialized_at_utc": _utc_now(),
                    "artifact_path": str(out_path.relative_to(build_dir)),
                }
                total_bytes += _write_scene(out_path, payload)
                generated += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                if len(examples) < 12:
                    examples.append({"system_id": system_id, "error": str(exc)})
        if len(examples) < 8 and out_path.exists():
            examples.append(
                {
                    "system_id": system_id,
                    "system_name": row.get("system_name"),
                    "artifact_path": str(out_path.relative_to(build_dir)),
                    "size_bytes": out_path.stat().st_size,
                }
            )
        if idx == requested or idx % interval == 0:
            _emit_progress(
                {
                    "stage": "materializing",
                    "processed": idx,
                    "requested": requested,
                    "generated": generated,
                    "reused": reused,
                    "incompatible_existing": incompatible_existing,
                    "failed": failed,
                }
            )

    elapsed_s = time.perf_counter() - started
    report = {
        "ok": failed == 0,
        "build_id": build_id,
        "build_dir": str(build_dir),
        "materializer_version": MATERIALIZER_VERSION,
        "generated_at_utc": _utc_now(),
        "params": {
            "sort": args.sort,
            "priority_profile": args.priority_profile,
            "top_coolness_limit": args.top_coolness_limit,
            "limit": args.limit,
            "system_id": args.system_id,
            "min_dist_ly": args.min_dist_ly,
            "max_dist_ly": args.max_dist_ly,
            "min_star_count": args.min_star_count,
            "min_planet_count": args.min_planet_count,
            "force": bool(args.force),
        },
        "requested": requested,
        "generated": generated,
        "reused": reused,
        "incompatible_existing": incompatible_existing,
        "failed": failed,
        "selected_artifact_size_bytes": total_bytes,
        "output_dir": str(output_dir),
        "report_path": str(report_path),
        "elapsed_s": round(elapsed_s, 3),
        "examples": examples,
    }
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    _emit_progress({**{k: report[k] for k in ("build_id", "requested", "generated", "reused", "failed")}, "stage": "complete", "report_path": str(report_path)})
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prebuild compressed System Simulation scene JSON artifacts for a served build.")
    parser.add_argument("--build-id", default=None, help="Build ID to target (defaults to served/current).")
    parser.add_argument("--build-dir", default=None, help="Explicit build directory, including an unpromoted .tmp build.")
    parser.add_argument("--system-id", action="append", type=int, default=[], help="Specific system_id to materialize; can be repeated.")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum systems to select when --system-id is not provided.")
    parser.add_argument("--sort", choices=["distance", "coolness", "name"], default="distance")
    parser.add_argument("--priority-profile", choices=["none", "search-preview"], default="none", help="Optional priority selector for prebuilding high-value preview scenes.")
    parser.add_argument("--top-coolness-limit", type=int, default=500, help="When using --priority-profile search-preview, include this many top-ranked coolness systems if available.")
    parser.add_argument("--min-dist-ly", type=float, default=None)
    parser.add_argument("--max-dist-ly", type=float, default=100.0)
    parser.add_argument("--min-star-count", type=int, default=None)
    parser.add_argument("--min-planet-count", type=int, default=None)
    parser.add_argument("--force", action="store_true", help="Regenerate existing scene artifacts.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.limit <= 0 and not args.system_id:
        raise SystemExit("--limit must be > 0 unless --system-id is provided")
    if args.top_coolness_limit < 0:
        raise SystemExit("--top-coolness-limit must be >= 0")
    if args.min_dist_ly is not None and args.min_dist_ly < 0:
        raise SystemExit("--min-dist-ly must be >= 0")
    if args.max_dist_ly is not None and args.max_dist_ly < 0:
        raise SystemExit("--max-dist-ly must be >= 0")
    if args.min_dist_ly is not None and args.max_dist_ly is not None and args.min_dist_ly > args.max_dist_ly:
        raise SystemExit("--min-dist-ly cannot be greater than --max-dist-ly")
    result = run(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
