#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path

import duckdb


BENCHMARK_SYSTEMS = [
    "Castor",
    "16 Cyg",
    "Keid",
    "Rigil Kentaurus",
    "Sirius",
]


def has_column(con: duckdb.DuckDBPyConnection, table: str, column: str) -> bool:
    rows = con.execute(f"pragma table_info('{table}')").fetchall()
    return any(str(row[1]) == column for row in rows)


def fetch_metrics(db_path: Path) -> dict:
    con = duckdb.connect(str(db_path), read_only=True)
    stars_has_gaia_nss = has_column(con, "stars", "gaia_non_single_star")
    stars_has_gaia_nss_two_body = has_column(con, "stars", "gaia_nss_solution_count")
    systems_has_gaia_nss = has_column(con, "systems", "has_gaia_nss_evidence")
    systems_has_msc = has_column(con, "systems", "has_msc_evidence")
    metrics = {
        "stars": int(con.execute("select count(*) from stars").fetchone()[0] or 0),
        "systems": int(con.execute("select count(*) from systems").fetchone()[0] or 0),
        "multi_star_systems": int(
            con.execute(
                "select count(*) from (select system_id, count(*) as cnt from stars group by system_id having cnt > 1)"
            ).fetchone()[0]
            or 0
        ),
        "max_component_size": int(
            con.execute(
                "select max(cnt) from (select system_id, count(*) as cnt from stars group by system_id)"
            ).fetchone()[0]
            or 0
        ),
        "wds_group_systems": int(
            con.execute("select count(*) from systems where grouping_basis = 'wds'").fetchone()[0]
            or 0
        ),
        "msc_insert_stars": int(
            con.execute("select count(*) from stars where source_catalog = 'msc'").fetchone()[0]
            or 0
        ),
        "msc_evidence_systems": int(
            con.execute("select count(*) from systems where has_msc_evidence").fetchone()[0]
            if systems_has_msc
            else 0
        ),
        "gaia_nss_stars": int(
            con.execute("select count(*) from stars where gaia_non_single_star").fetchone()[0]
            if stars_has_gaia_nss
            else 0
        ),
        "gaia_nss_two_body_stars": int(
            con.execute("select count(*) from stars where coalesce(gaia_nss_solution_count, 0) > 0").fetchone()[0]
            if stars_has_gaia_nss_two_body
            else 0
        ),
        "gaia_nss_evidence_systems": int(
            con.execute("select count(*) from systems where has_gaia_nss_evidence").fetchone()[0]
            if systems_has_gaia_nss
            else 0
        ),
        "benchmarks": {},
    }

    for name in BENCHMARK_SYSTEMS:
        row = con.execute(
            """
            select count(st.star_id) as star_count
            from systems s
            left join stars st on st.system_id = s.system_id
            where lower(s.system_name) = lower(?)
            """,
            [name],
        ).fetchone()
        metrics["benchmarks"][name] = int((row[0] if row else 0) or 0)
    con.close()
    return metrics


def add_deltas(report: dict, baseline_mode: str) -> dict:
    baseline = report["modes"][baseline_mode]["metrics"]
    for mode_name, mode in report["modes"].items():
        metrics = mode["metrics"]
        if mode_name == baseline_mode:
            mode["delta_vs_baseline"] = {k: 0 for k in baseline.keys() if k != "benchmarks"}
            mode["benchmark_delta_vs_baseline"] = {
                k: 0 for k in baseline["benchmarks"].keys()
            }
            continue
        mode["delta_vs_baseline"] = {
            k: metrics[k] - baseline[k]
            for k in baseline.keys()
            if k != "benchmarks"
        }
        mode["benchmark_delta_vs_baseline"] = {
            k: metrics["benchmarks"][k] - baseline["benchmarks"][k]
            for k in baseline["benchmarks"].keys()
        }
    return report


def write_markdown(path: Path, report: dict, baseline_mode: str) -> None:
    modes = report.get("mode_order") or list(report["modes"].keys())
    metric_keys = [
        "stars",
        "systems",
        "multi_star_systems",
        "max_component_size",
        "wds_group_systems",
        "gaia_nss_stars",
        "gaia_nss_two_body_stars",
        "gaia_nss_evidence_systems",
        "msc_insert_stars",
        "msc_evidence_systems",
    ]
    lines: list[str] = []
    lines.append(f"# Multiplicity Mode Report ({report['run_id']})")
    lines.append("")
    lines.append("| mode | build_id | stars | systems | multi_star_systems | gaia_nss_stars | msc_insert_stars |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for mode in modes:
        entry = report["modes"][mode]
        m = entry["metrics"]
        lines.append(
            f"| {mode} | `{entry['build_id']}` | {m['stars']} | {m['systems']} | {m['multi_star_systems']} | {m['gaia_nss_stars']} | {m['msc_insert_stars']} |"
        )
    lines.append("")
    lines.append(f"## Delta vs {baseline_mode}")
    lines.append("")
    lines.append("| mode | " + " | ".join(metric_keys) + " |")
    lines.append("|---|" + "|".join(["---:"] * len(metric_keys)) + "|")
    for mode in modes:
        if mode == baseline_mode:
            continue
        d = report["modes"][mode]["delta_vs_baseline"]
        lines.append(
            "| "
            + mode
            + " | "
            + " | ".join(str(d[k]) for k in metric_keys)
            + " |"
        )
    lines.append("")
    lines.append("## Benchmarks")
    lines.append("")
    lines.append(
        "| mode | " + " | ".join(BENCHMARK_SYSTEMS) + " |"
    )
    lines.append("|---|" + "|".join(["---:"] * len(BENCHMARK_SYSTEMS)) + "|")
    for mode in modes:
        b = report["modes"][mode]["metrics"]["benchmarks"]
        lines.append("| " + mode + " | " + " | ".join(str(b[name]) for name in BENCHMARK_SYSTEMS) + " |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare multiplicity ingest modes by build_id (MSC mandatory baseline)."
    )
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--nss-off", required=True, help="Build id for NSS off (MSC on).")
    parser.add_argument("--nss-on", required=True, help="Build id for NSS on (MSC on).")
    parser.add_argument(
        "--nss-on-wds-xmatch",
        default=None,
        help="Optional build id for NSS on + WDS Gaia XMatch (MSC on).",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    state_dir = Path(
        args.state_dir
        or os.getenv("SPACEGATE_STATE_DIR")
        or os.getenv("SPACEGATE_DATA_DIR")
        or (root / "data")
    )
    out_dir = state_dir / "reports" / "multiplicity_modes"
    out_dir.mkdir(parents=True, exist_ok=True)

    run_id = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H%M%SZ")
    modes = {
        "nss_off": args.nss_off,
        "nss_on": args.nss_on,
    }
    if args.nss_on_wds_xmatch:
        modes["nss_on_wds_xmatch"] = args.nss_on_wds_xmatch
    mode_order = list(modes.keys())
    baseline_mode = "nss_off"

    report = {
        "run_id": run_id,
        "state_dir": str(state_dir),
        "baseline_mode": baseline_mode,
        "mode_order": mode_order,
        "modes": {},
    }
    for mode_name, build_id in modes.items():
        db_path = state_dir / "out" / build_id / "core.duckdb"
        if not db_path.exists():
            raise SystemExit(f"Missing build database: {db_path}")
        report["modes"][mode_name] = {
            "build_id": build_id,
            "db_path": str(db_path),
            "metrics": fetch_metrics(db_path),
        }

    report = add_deltas(report, baseline_mode)

    json_path = out_dir / f"{run_id}_multiplicity_mode_report.json"
    md_path = out_dir / f"{run_id}_multiplicity_mode_report.md"
    json_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    write_markdown(md_path, report, baseline_mode)
    print(str(json_path))
    print(str(md_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
