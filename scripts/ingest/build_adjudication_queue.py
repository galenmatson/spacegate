#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def state_dir(root: Path) -> Path:
    configured = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if configured:
        return Path(configured)
    shared_state = Path("/data/spacegate/data")
    if shared_state.exists():
        return shared_state
    return root / "data"


def resolve_build_dir(state: Path, build_id: str | None, prefer_latest_out: bool) -> tuple[str, Path]:
    out_dir = state / "out"
    if build_id:
        build_dir = out_dir / build_id
        if not build_dir.is_dir():
            raise SystemExit(f"Build directory not found: {build_dir}")
        return build_id, build_dir

    if prefer_latest_out:
        candidates = sorted(
            [path for path in out_dir.iterdir() if path.is_dir() and not path.name.endswith(".tmp")],
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            raise SystemExit(f"No build directories found in {out_dir}")
        return candidates[0].name, candidates[0]

    served = state / "served" / "current"
    if served.exists():
        build_dir = served.resolve(strict=True)
        return build_dir.name, build_dir

    raise SystemExit("No served/current build found and no build_id was provided.")


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_optional_coolness_map(state: Path, build_dir: Path) -> dict[str, dict[str, Any]]:
    candidates = [
        build_dir / "disc.duckdb",
        state / "served" / "current" / "disc.duckdb",
    ]
    for path in candidates:
        if not path.exists():
            continue
        con = duckdb.connect(str(path), read_only=True)
        try:
            rows = con.execute(
                """
                SELECT stable_object_key, rank, score_total, build_id
                FROM coolness_scores
                """
            ).fetchall()
        finally:
            con.close()
        return {
            str(stable_object_key): {
                "coolness_rank": int(rank) if rank is not None else None,
                "coolness_score": float(score_total) if score_total is not None else None,
                "coolness_build_id": str(build_id or "").strip() or None,
            }
            for stable_object_key, rank, score_total, build_id in rows
            if stable_object_key
        }
    return {}


def compute_issue_payload(row: dict[str, Any]) -> dict[str, Any]:
    issue_types: list[str] = []
    severity_score = 0

    dup_planet_key_count = int(row.get("dup_planet_key_count") or 0)
    dup_planet_extra_row_count = int(row.get("dup_planet_extra_row_count") or 0)
    dup_star_gaia_groups = int(row.get("dup_star_gaia_groups") or 0)
    dup_star_hip_groups = int(row.get("dup_star_hip_groups") or 0)
    dup_star_hd_groups = int(row.get("dup_star_hd_groups") or 0)
    dup_star_name_groups = int(row.get("dup_star_name_groups") or 0)
    partial_msc_hierarchy = bool(row.get("partial_msc_hierarchy"))

    if dup_planet_key_count > 0:
        issue_types.append("duplicate_planet_stable_key")
        severity_score += 100 + min(40, dup_planet_extra_row_count * 5)
    if dup_star_gaia_groups > 0:
        issue_types.append("duplicate_star_gaia_identity")
        severity_score += 85 + min(20, dup_star_gaia_groups * 5)
    if dup_star_hip_groups > 0:
        issue_types.append("duplicate_star_hip_identity")
        severity_score += 65 + min(20, dup_star_hip_groups * 5)
    if dup_star_hd_groups > 0:
        issue_types.append("duplicate_star_hd_identity")
        severity_score += 55 + min(20, dup_star_hd_groups * 5)
    if dup_star_name_groups > 0:
        issue_types.append("duplicate_star_name")
        severity_score += 20 + min(20, dup_star_name_groups * 2)
    if partial_msc_hierarchy:
        issue_types.append("partial_msc_hierarchy")
        severity_score += 45

    if severity_score >= 100 or len(issue_types) >= 2:
        queue_priority = "adjudication"
    elif severity_score >= 40:
        queue_priority = "review"
    else:
        queue_priority = "watch"

    if severity_score >= 120:
        severity_label = "high"
    elif severity_score >= 50:
        severity_label = "medium"
    else:
        severity_label = "low"

    summary_bits: list[str] = []
    if dup_planet_key_count > 0:
        summary_bits.append(
            f"{dup_planet_key_count} duplicate planet key group(s), {dup_planet_extra_row_count} extra planet row(s)"
        )
    if dup_star_gaia_groups > 0:
        summary_bits.append(f"{dup_star_gaia_groups} duplicate Gaia star group(s)")
    if dup_star_hip_groups > 0:
        summary_bits.append(f"{dup_star_hip_groups} duplicate HIP star group(s)")
    if dup_star_hd_groups > 0:
        summary_bits.append(f"{dup_star_hd_groups} duplicate HD star group(s)")
    if dup_star_name_groups > 0:
        summary_bits.append(f"{dup_star_name_groups} duplicate star-name group(s)")
    if partial_msc_hierarchy:
        summary_bits.append(
            f"MSC hierarchy shows {int(row.get('msc_root_star_count') or 0)} star leaf/leaves vs {int(row.get('core_star_count') or 0)} core star row(s)"
        )

    payload = dict(row)
    payload["issue_types"] = issue_types
    payload["issue_type_count"] = len(issue_types)
    payload["severity_score"] = severity_score
    payload["severity_label"] = severity_label
    payload["queue_priority"] = queue_priority
    payload["issue_summary"] = "; ".join(summary_bits)
    return payload


def create_adjudication_queue(
    *,
    build_id: str,
    build_dir: Path,
    reports_dir: Path,
    coolness_map: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    core_path = build_dir / "core.duckdb"
    arm_path = build_dir / "arm.duckdb"
    con = duckdb.connect(str(core_path), read_only=False)
    try:
        con.execute(f"ATTACH {sql_string_literal(str(arm_path))} AS arm_db (READ_ONLY)")
        base_rows = con.execute(
            """
            WITH planet_dupe_keys AS (
              SELECT system_id, stable_object_key, COUNT(*)::BIGINT AS row_count
              FROM planets
              GROUP BY system_id, stable_object_key
              HAVING COUNT(*) > 1
            ),
            planet_dupes AS (
              SELECT
                system_id,
                COUNT(*)::BIGINT AS dup_planet_key_count,
                SUM(row_count - 1)::BIGINT AS dup_planet_extra_row_count
              FROM planet_dupe_keys
              GROUP BY system_id
            ),
            gaia_dupes AS (
              SELECT system_id, COUNT(*)::BIGINT AS dup_star_gaia_groups
              FROM (
                SELECT system_id, gaia_id
                FROM stars
                WHERE gaia_id IS NOT NULL
                GROUP BY system_id, gaia_id
                HAVING COUNT(*) > 1
              )
              GROUP BY system_id
            ),
            hip_dupes AS (
              SELECT system_id, COUNT(*)::BIGINT AS dup_star_hip_groups
              FROM (
                SELECT system_id, hip_id
                FROM stars
                WHERE hip_id IS NOT NULL
                GROUP BY system_id, hip_id
                HAVING COUNT(*) > 1
              )
              GROUP BY system_id
            ),
            hd_dupes AS (
              SELECT system_id, COUNT(*)::BIGINT AS dup_star_hd_groups
              FROM (
                SELECT system_id, hd_id
                FROM stars
                WHERE hd_id IS NOT NULL
                GROUP BY system_id, hd_id
                HAVING COUNT(*) > 1
              )
              GROUP BY system_id
            ),
            name_dupes AS (
              SELECT system_id, COUNT(*)::BIGINT AS dup_star_name_groups
              FROM (
                SELECT system_id, star_name_norm
                FROM stars
                WHERE COALESCE(star_name_norm, '') <> ''
                GROUP BY system_id, star_name_norm
                HAVING COUNT(*) > 1
              )
              GROUP BY system_id
            ),
            msc_root_counts AS (
              SELECT
                s.system_id,
                COUNT(*)::BIGINT AS msc_root_star_count
              FROM systems s
              JOIN arm_db.system_hierarchy_edges h
                ON s.wds_id IS NOT NULL
               AND h.parent_component_key = ('comp:msc_system:wds:' || s.wds_id)
              JOIN arm_db.component_entities ce
                ON ce.stable_component_key = h.child_component_key
               AND ce.component_type = 'star'
              GROUP BY s.system_id
            )
            SELECT
              s.system_id,
              s.stable_object_key,
              s.system_name,
              s.wds_id,
              s.dist_ly,
              COALESCE(s.star_count, 0)::BIGINT AS core_star_count,
              COALESCE(s.planet_count, 0)::BIGINT AS planet_count,
              COALESCE(pd.dup_planet_key_count, 0)::BIGINT AS dup_planet_key_count,
              COALESCE(pd.dup_planet_extra_row_count, 0)::BIGINT AS dup_planet_extra_row_count,
              COALESCE(gd.dup_star_gaia_groups, 0)::BIGINT AS dup_star_gaia_groups,
              COALESCE(hd2.dup_star_hip_groups, 0)::BIGINT AS dup_star_hip_groups,
              COALESCE(dd.dup_star_hd_groups, 0)::BIGINT AS dup_star_hd_groups,
              COALESCE(nd.dup_star_name_groups, 0)::BIGINT AS dup_star_name_groups,
              COALESCE(msc.msc_root_star_count, 0)::BIGINT AS msc_root_star_count,
              CASE
                WHEN s.wds_id IS NOT NULL
                 AND COALESCE(msc.msc_root_star_count, 0) > 0
                 AND COALESCE(msc.msc_root_star_count, 0) < COALESCE(s.star_count, 0)
                THEN TRUE
                ELSE FALSE
              END AS partial_msc_hierarchy
            FROM systems s
            LEFT JOIN planet_dupes pd ON pd.system_id = s.system_id
            LEFT JOIN gaia_dupes gd ON gd.system_id = s.system_id
            LEFT JOIN hip_dupes hd2 ON hd2.system_id = s.system_id
            LEFT JOIN hd_dupes dd ON dd.system_id = s.system_id
            LEFT JOIN name_dupes nd ON nd.system_id = s.system_id
            LEFT JOIN msc_root_counts msc ON msc.system_id = s.system_id
            WHERE
              COALESCE(pd.dup_planet_key_count, 0) > 0
              OR COALESCE(gd.dup_star_gaia_groups, 0) > 0
              OR COALESCE(hd2.dup_star_hip_groups, 0) > 0
              OR COALESCE(dd.dup_star_hd_groups, 0) > 0
              OR COALESCE(nd.dup_star_name_groups, 0) > 0
              OR (
                s.wds_id IS NOT NULL
                AND COALESCE(msc.msc_root_star_count, 0) > 0
                AND COALESCE(msc.msc_root_star_count, 0) < COALESCE(s.star_count, 0)
              )
            ORDER BY s.system_id ASC
            """
        ).fetchall()
    finally:
        con.close()

    base_columns = [
        "system_id",
        "stable_object_key",
        "system_name",
        "wds_id",
        "dist_ly",
        "core_star_count",
        "planet_count",
        "dup_planet_key_count",
        "dup_planet_extra_row_count",
        "dup_star_gaia_groups",
        "dup_star_hip_groups",
        "dup_star_hd_groups",
        "dup_star_name_groups",
        "msc_root_star_count",
        "partial_msc_hierarchy",
    ]

    items: list[dict[str, Any]] = []
    for raw in base_rows:
        row = {key: raw[idx] for idx, key in enumerate(base_columns)}
        coolness = coolness_map.get(str(row["stable_object_key"]), {})
        row.update(coolness)
        payload = compute_issue_payload(row)
        items.append(payload)

    items.sort(
        key=lambda item: (
            -int(item["severity_score"]),
            1 if item.get("coolness_rank") is None else 0,
            int(item["coolness_rank"] or 10**12),
            -int(item["issue_type_count"]),
            int(item["system_id"]),
        )
    )
    for idx, item in enumerate(items, start=1):
        item["priority_rank"] = idx
        item["build_id"] = build_id
        item["issue_types_json"] = json.dumps(item.pop("issue_types"), separators=(",", ":"))

    parquet_dir = build_dir / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = parquet_dir / "adjudication_queue.parquet"
    report_path = reports_dir / "adjudication_queue.json"
    reports_dir.mkdir(parents=True, exist_ok=True)

    write_con = duckdb.connect()
    try:
        write_con.execute(
            """
            CREATE TABLE adjudication_queue (
              priority_rank BIGINT,
              build_id VARCHAR,
              system_id BIGINT,
              stable_object_key VARCHAR,
              system_name VARCHAR,
              wds_id VARCHAR,
              dist_ly DOUBLE,
              coolness_rank BIGINT,
              coolness_score DOUBLE,
              coolness_build_id VARCHAR,
              queue_priority VARCHAR,
              severity_score BIGINT,
              severity_label VARCHAR,
              issue_type_count BIGINT,
              issue_types_json VARCHAR,
              issue_summary VARCHAR,
              dup_planet_key_count BIGINT,
              dup_planet_extra_row_count BIGINT,
              dup_star_gaia_groups BIGINT,
              dup_star_hip_groups BIGINT,
              dup_star_hd_groups BIGINT,
              dup_star_name_groups BIGINT,
              core_star_count BIGINT,
              msc_root_star_count BIGINT,
              planet_count BIGINT,
              partial_msc_hierarchy BOOLEAN
            )
            """
        )
        write_con.executemany(
            """
            INSERT INTO adjudication_queue VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    int(item["priority_rank"]),
                    item["build_id"],
                    int(item["system_id"]),
                    item["stable_object_key"],
                    item["system_name"],
                    item.get("wds_id"),
                    float(item["dist_ly"]) if item.get("dist_ly") is not None else None,
                    int(item["coolness_rank"]) if item.get("coolness_rank") is not None else None,
                    float(item["coolness_score"]) if item.get("coolness_score") is not None else None,
                    item.get("coolness_build_id"),
                    item["queue_priority"],
                    int(item["severity_score"]),
                    item["severity_label"],
                    int(item["issue_type_count"]),
                    item["issue_types_json"],
                    item["issue_summary"],
                    int(item["dup_planet_key_count"]),
                    int(item["dup_planet_extra_row_count"]),
                    int(item["dup_star_gaia_groups"]),
                    int(item["dup_star_hip_groups"]),
                    int(item["dup_star_hd_groups"]),
                    int(item["dup_star_name_groups"]),
                    int(item["core_star_count"]),
                    int(item["msc_root_star_count"]),
                    int(item["planet_count"]),
                    bool(item["partial_msc_hierarchy"]),
                )
                for item in items
            ],
        )
        write_con.execute(f"COPY adjudication_queue TO {sql_string_literal(str(parquet_path))} (FORMAT PARQUET)")
    finally:
        write_con.close()

    output = {
        "generated_at": utc_now(),
        "build_id": build_id,
        "queue_size": len(items),
        "parquet_path": str(parquet_path),
        "items": items,
    }
    report_path.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a deterministic adjudication queue for sloppy systems.")
    parser.add_argument("--build-id", help="Specific build id to analyze.")
    parser.add_argument(
        "--latest-out",
        action="store_true",
        help="Analyze the latest out/<build_id> directory instead of served/current.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[2]
    state = state_dir(root)
    build_id, build_dir = resolve_build_dir(state, args.build_id, args.latest_out)
    reports_dir = state / "reports" / build_id

    coolness_map = load_optional_coolness_map(state, build_dir)
    output = create_adjudication_queue(
        build_id=build_id,
        build_dir=build_dir,
        reports_dir=reports_dir,
        coolness_map=coolness_map,
    )
    print(
        json.dumps(
            {
                "build_id": output["build_id"],
                "queue_size": output["queue_size"],
                "parquet_path": output["parquet_path"],
                "top_items": [
                    {
                        "priority_rank": item["priority_rank"],
                        "system_id": item["system_id"],
                        "system_name": item["system_name"],
                        "queue_priority": item["queue_priority"],
                        "severity_score": item["severity_score"],
                        "coolness_rank": item.get("coolness_rank"),
                        "issue_summary": item["issue_summary"],
                    }
                    for item in output["items"][:10]
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
