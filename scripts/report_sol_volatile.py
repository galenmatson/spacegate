#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import Any


def parse_iso_utc(raw: str | None) -> dt.datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(text).astimezone(dt.UTC)
    except Exception:
        return None


def latest_manifest_entry(path: Path, source_name: str) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    entries = payload if isinstance(payload, list) else [payload] if isinstance(payload, dict) else []
    best: dict[str, Any] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("source_name") or "").strip() != source_name:
            continue
        checked_at = str(entry.get("checked_at") or "")
        if checked_at >= str(best.get("checked_at") or ""):
            best = entry
    return best


def current_arm_path(state_dir: Path) -> Path | None:
    candidate = state_dir / "served" / "current" / "arm.duckdb"
    return candidate if candidate.is_file() else None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate Sol volatile-feed freshness report from manifests and current arm tables."
    )
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--fail-on-stale", action="store_true")
    parser.add_argument("--sol-authority-max-age-days", type=int, default=365)
    parser.add_argument("--sol-artificial-max-age-days", type=int, default=45)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    state_dir = Path(
        args.state_dir
        or os.getenv("SPACEGATE_STATE_DIR")
        or os.getenv("SPACEGATE_DATA_DIR")
        or (root / "data")
    ).resolve()
    manifests = state_dir / "reports" / "manifests"
    output_path = Path(args.output).resolve() if args.output else (state_dir / "reports" / "sol_volatile_report.json")
    now = dt.datetime.now(dt.UTC)

    sol_authority = latest_manifest_entry(manifests / "sol_authority_manifest.json", "sol_system_objects")
    sol_artificial = latest_manifest_entry(manifests / "sol_artificial_manifest.json", "sol_artificial_objects")

    def summarize(entry: dict[str, Any], *, max_age_days: int) -> dict[str, Any]:
        retrieved_raw = str(entry.get("retrieved_at") or "")
        retrieved_at = parse_iso_utc(retrieved_raw)
        age_days = None
        if retrieved_at is not None:
            age_days = max(int((now - retrieved_at).total_seconds() // 86400), 0)
        stale = bool(age_days is not None and age_days > max_age_days)
        return {
            "source_version": entry.get("source_version"),
            "retrieved_at": retrieved_raw or None,
            "row_count": int(entry.get("row_count") or 0) if entry else 0,
            "bytes_written": int(entry.get("bytes_written") or 0) if entry else 0,
            "sha256": entry.get("sha256"),
            "age_days": age_days,
            "max_age_days": int(max_age_days),
            "is_stale": stale,
            "url": entry.get("url"),
            "dest_path": entry.get("dest_path"),
        }

    summary_authority = summarize(sol_authority, max_age_days=max(1, int(args.sol_authority_max_age_days)))
    summary_artificial = summarize(sol_artificial, max_age_days=max(1, int(args.sol_artificial_max_age_days)))

    stale_small_body_rows = None
    stale_artificial_rows = None
    arm_rows = {}
    arm_path = current_arm_path(state_dir)
    if arm_path is not None:
        try:
            import duckdb  # type: ignore

            con = duckdb.connect(str(arm_path), read_only=True)
            tables = {
                row[0]
                for row in con.execute(
                    "select table_name from information_schema.tables where table_schema='main'"
                ).fetchall()
            }
            if "sol_small_body_objects" in tables:
                stale_small_body_rows = int(
                    con.execute("select count(*) from sol_small_body_objects where is_stale").fetchone()[0] or 0
                )
                arm_rows["sol_small_body_objects"] = int(
                    con.execute("select count(*) from sol_small_body_objects").fetchone()[0] or 0
                )
            if "sol_artificial_objects" in tables:
                stale_artificial_rows = int(
                    con.execute("select count(*) from sol_artificial_objects where is_stale").fetchone()[0] or 0
                )
                arm_rows["sol_artificial_objects"] = int(
                    con.execute("select count(*) from sol_artificial_objects").fetchone()[0] or 0
                )
            con.close()
        except Exception:
            pass

    stale_reasons = []
    if summary_authority["is_stale"]:
        stale_reasons.append("sol_authority_manifest_age")
    if summary_artificial["is_stale"]:
        stale_reasons.append("sol_artificial_manifest_age")
    if stale_small_body_rows is not None and stale_small_body_rows > 0:
        stale_reasons.append("arm_sol_small_body_rows")
    if stale_artificial_rows is not None and stale_artificial_rows > 0:
        stale_reasons.append("arm_sol_artificial_rows")

    report = {
        "generated_at": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "state_dir": str(state_dir),
        "status": "stale" if stale_reasons else "fresh",
        "sources": {
            "sol_authority": summary_authority,
            "sol_artificial": summary_artificial,
        },
        "arm": {
            "path": str(arm_path) if arm_path else None,
            "table_rows": arm_rows,
            "stale_small_body_rows": stale_small_body_rows,
            "stale_artificial_rows": stale_artificial_rows,
        },
        "stale_reasons": stale_reasons,
        "recommended_next_action": (
            "run scripts/refresh_sol_volatile.sh and then run ingest/promote to apply refreshed Sol rows"
            if stale_reasons
            else "no immediate Sol volatile refresh required"
        ),
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(str(output_path))

    if args.fail_on_stale and stale_reasons:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
