#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path

PLANET_SOURCE_SET = {
    "nasa_exoplanet_archive",
    "exoplanet_eu",
    "open_exoplanet_catalogue",
    "hwc",
    "emac_tt9",
}


def now_utc() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if value and value[0] not in "\"'":
            value = value.split("#", 1)[0].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ[key] = value


def init_env(root: Path) -> None:
    for env_path in (
        Path("/etc/spacegate/spacegate.env"),
        root / ".spacegate.env",
        root / ".spacegate.local.env",
    ):
        load_env_file(env_path)


def detect_state_dir(root: Path) -> Path:
    raw = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if raw:
        return Path(raw).expanduser()
    return root / "data"


def read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    return payload


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def safe_count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    try:
        row = con.execute(sql).fetchone()
        if not row:
            return 0
        return int(row[0] or 0)
    except Exception:
        return 0


def collect_lifecycle_impact_counts(core_db: Path, state_dir: Path) -> dict[str, int]:
    status_path = state_dir / "cooked" / "exoplanet_lifecycle" / "status_rows.csv"
    features_path = state_dir / "cooked" / "exoplanet_lifecycle" / "features_rows.csv"
    if not core_db.exists():
        return {
            "status_candidate_rows": 0,
            "status_matched_planets": 0,
            "feature_candidate_rows": 0,
            "feature_matched_planets": 0,
            "distinct_impacted_planets": 0,
        }
    try:
        import duckdb
    except ModuleNotFoundError:
        return {
            "status_candidate_rows": 0,
            "status_matched_planets": 0,
            "feature_candidate_rows": 0,
            "feature_matched_planets": 0,
            "distinct_impacted_planets": 0,
        }
    con = duckdb.connect(str(core_db), read_only=True)
    try:
        if status_path.exists():
            status_sql_path = str(status_path).replace("'", "''")
            con.execute(
                f"""
                create or replace temp view lifecycle_status_raw as
                select * from read_csv_auto('{status_sql_path}',
                    delim=',',
                    quote='\"',
                    escape='\"',
                    header=true,
                    strict_mode=false,
                    null_padding=true,
                    all_varchar=true
                )
                """
            )
        else:
            con.execute(
                """
                create or replace temp view lifecycle_status_raw as
                select
                  cast(null as varchar) as planet_name_norm,
                  cast(null as varchar) as observed_status
                where false
                """
            )

        if features_path.exists():
            features_sql_path = str(features_path).replace("'", "''")
            con.execute(
                f"""
                create or replace temp view lifecycle_features_raw as
                select * from read_csv_auto('{features_sql_path}',
                    delim=',',
                    quote='\"',
                    escape='\"',
                    header=true,
                    strict_mode=false,
                    null_padding=true,
                    all_varchar=true
                )
                """
            )
        else:
            con.execute(
                """
                create or replace temp view lifecycle_features_raw as
                select cast(null as varchar) as planet_name_norm where false
                """
            )

        status_candidate_rows = safe_count(
            con,
            """
            select count(*)::bigint
            from lifecycle_status_raw
            where lower(coalesce(observed_status, '')) in ('confirmed','candidate','controversial','retracted')
            """,
        )
        status_matched_planets = safe_count(
            con,
            """
            select count(distinct p.planet_id)::bigint
            from planets p
            join lifecycle_status_raw s
              on p.planet_name_norm is not null
             and p.planet_name_norm = lower(trim(coalesce(s.planet_name_norm, '')))
            where lower(coalesce(s.observed_status, '')) in ('confirmed','candidate','controversial','retracted')
            """,
        )
        feature_candidate_rows = safe_count(
            con,
            "select count(*)::bigint from lifecycle_features_raw",
        )
        feature_matched_planets = safe_count(
            con,
            """
            select count(distinct p.planet_id)::bigint
            from planets p
            join lifecycle_features_raw f
              on p.planet_name_norm is not null
             and p.planet_name_norm = lower(trim(coalesce(f.planet_name_norm, '')))
            """,
        )
        distinct_impacted_planets = safe_count(
            con,
            """
            with impacted as (
              select distinct p.planet_id
              from planets p
              join lifecycle_status_raw s
                on p.planet_name_norm is not null
               and p.planet_name_norm = lower(trim(coalesce(s.planet_name_norm, '')))
              where lower(coalesce(s.observed_status, '')) in ('confirmed','candidate','controversial','retracted')
              union
              select distinct p.planet_id
              from planets p
              join lifecycle_features_raw f
                on p.planet_name_norm is not null
               and p.planet_name_norm = lower(trim(coalesce(f.planet_name_norm, '')))
            )
            select count(*)::bigint from impacted
            """,
        )
    finally:
        con.close()
    return {
        "status_candidate_rows": status_candidate_rows,
        "status_matched_planets": status_matched_planets,
        "feature_candidate_rows": feature_candidate_rows,
        "feature_matched_planets": feature_matched_planets,
        "distinct_impacted_planets": distinct_impacted_planets,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Plan impacted domains/rows from source delta report."
    )
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--source-delta-report", default="")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    init_env(root)
    state_dir = detect_state_dir(root).resolve()
    source_delta_report_path = (
        Path(args.source_delta_report).expanduser().resolve()
        if args.source_delta_report
        else (state_dir / "reports" / "source_delta_report.json")
    )
    out_path = (
        Path(args.output).expanduser().resolve()
        if args.output
        else (state_dir / "reports" / "impacted_rows_plan.json")
    )

    source_delta_report = read_json(source_delta_report_path, default={})
    changed_sources = sorted(
        {
            str(item.get("source_name") or "").strip()
            for item in (source_delta_report.get("changed_sources") or [])
            if isinstance(item, dict)
        }
    )
    new_sources = sorted(
        {
            str(item.get("source_name") or "").strip()
            for item in (source_delta_report.get("new_sources") or [])
            if isinstance(item, dict)
        }
    )
    missing_sources = sorted(
        {
            str(item.get("source_name") or "").strip()
            for item in (source_delta_report.get("missing_sources") or [])
            if isinstance(item, dict)
        }
    )
    changed_or_new = sorted(set(changed_sources) | set(new_sources) | set(missing_sources))

    changed_planet_sources = sorted(source for source in changed_or_new if source in PLANET_SOURCE_SET)
    non_planet_changes = sorted(source for source in changed_or_new if source not in PLANET_SOURCE_SET)
    mode = (
        "planet_incremental_eligible"
        if changed_or_new and not non_planet_changes
        else "full_rebuild_required"
    )
    reason = (
        "Only planet/lifecycle catalogs changed."
        if mode == "planet_incremental_eligible"
        else "Backbone/multiplicity/compact/superstellar sources changed or no delta baseline available."
    )

    served_current = state_dir / "served" / "current"
    current_build_id = ""
    current_core_db = None
    if served_current.exists():
        try:
            resolved = served_current.resolve()
            current_build_id = resolved.name
            candidate_core = resolved / "core.duckdb"
            if candidate_core.exists():
                current_core_db = candidate_core
        except Exception:
            current_build_id = ""
            current_core_db = None

    totals = {
        "stars": 0,
        "systems": 0,
        "planets": 0,
    }
    if current_core_db:
        try:
            import duckdb
        except ModuleNotFoundError as exc:
            raise SystemExit(
                "duckdb module not found for impacted-row planning. "
                "Use the project venv: /srv/spacegate/app/.venv/bin/python scripts/plan_impacted_rows.py"
            ) from exc
        con = duckdb.connect(str(current_core_db), read_only=True)
        try:
            totals["stars"] = safe_count(con, "select count(*)::bigint from stars")
            totals["systems"] = safe_count(con, "select count(*)::bigint from systems")
            totals["planets"] = safe_count(con, "select count(*)::bigint from planets")
        finally:
            con.close()

    lifecycle_impact_counts = (
        collect_lifecycle_impact_counts(current_core_db, state_dir)
        if current_core_db and changed_planet_sources
        else {
            "status_candidate_rows": 0,
            "status_matched_planets": 0,
            "feature_candidate_rows": 0,
            "feature_matched_planets": 0,
            "distinct_impacted_planets": 0,
        }
    )

    impacted_domains = {
        "stars": bool(non_planet_changes),
        "systems": bool(non_planet_changes),
        "planets": bool(changed_planet_sources or non_planet_changes),
        "arm": bool(changed_or_new),
        "disc_rim_dependent": bool(changed_or_new),
    }

    payload = {
        "generated_at": now_utc(),
        "source_delta_report_path": str(source_delta_report_path),
        "current_build_id": current_build_id or None,
        "mode": mode,
        "mode_reason": reason,
        "changed_sources": changed_sources,
        "new_sources": new_sources,
        "missing_sources": missing_sources,
        "changed_or_new_sources": changed_or_new,
        "changed_planet_sources": changed_planet_sources,
        "non_planet_changes": non_planet_changes,
        "totals_current_build": totals,
        "impacted_domains": impacted_domains,
        "impacted_rows_estimate": {
            "planets_direct_catalog_delta": (
                totals["planets"] if "nasa_exoplanet_archive" in changed_planet_sources else 0
            ),
            "planets_lifecycle_candidates": lifecycle_impact_counts["distinct_impacted_planets"],
            "stars_potential": (totals["stars"] if non_planet_changes else 0),
            "systems_potential": (totals["systems"] if non_planet_changes else 0),
        },
        "lifecycle_impact_detail": lifecycle_impact_counts,
        "recommended_actions": (
            [
                "Run selective cook for NASA/lifecycle only.",
                "Run incremental planet refresh ingest.",
                "Promote + verify build.",
            ]
            if mode == "planet_incremental_eligible"
            else [
                "Run full cook.",
                "Run full ingest_core.",
                "Promote + verify build.",
            ]
        ),
    }
    write_json(out_path, payload)
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
