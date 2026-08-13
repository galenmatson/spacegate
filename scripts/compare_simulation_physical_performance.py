#!/usr/bin/env python3
"""Compare System Simulation browser baselines against physical-scale v1."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _by_profile(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row["profile"]): row for row in report.get("results") or []}


def _ratio(current: Any, baseline: Any) -> float | None:
    if not isinstance(current, (int, float)) or not isinstance(baseline, (int, float)) or baseline <= 0:
        return None
    return current / baseline


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", required=True, type=Path)
    parser.add_argument("--after", required=True, type=Path)
    parser.add_argument("--physical", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    before = _by_profile(_load(args.before))
    after = _by_profile(_load(args.after))
    physical = _by_profile(_load(args.physical))
    rows = []
    for profile in sorted(before):
        baseline = before[profile]
        current = after.get(profile) or {}
        physical_row = physical.get(profile) or {}
        baseline_median = baseline.get("frame_ms_median")
        baseline_p95 = baseline.get("frame_ms_p95")
        current_median = current.get("frame_ms_median")
        current_p95 = current.get("frame_ms_p95")
        lens = physical_row.get("lens") or {}
        budgets = {
            "ready_ms_max": max(2_000.0, float(baseline.get("ready_ms") or 0) * 1.25),
            "structure_frame_median_ms_max": max(17.0, float(baseline_median or 0) * 1.25),
            "structure_frame_p95_ms_max": max(50.0, float(baseline_p95 or 0) * 1.5),
            "selection_latency_ms_max": max(300.0, float(baseline.get("selection_latency_ms") or 0) * 1.25),
            "used_js_heap_bytes_max": max(64_000_000, int((baseline.get("memory") or {}).get("used_js_heap_bytes") or 0) * 1.25),
            "lens_frame_p95_ms_max": max(50.0, float(current_p95 or 0) * 1.25),
            "webgl_context_count_max": 1,
        }
        checks = {
            "ready": float(current.get("ready_ms") or 1e30) <= budgets["ready_ms_max"],
            "structure_frame_median": float(current_median or 1e30) <= budgets["structure_frame_median_ms_max"],
            "structure_frame_p95": float(current_p95 or 1e30) <= budgets["structure_frame_p95_ms_max"],
            "selection_latency": float(current.get("selection_latency_ms") or 1e30) <= budgets["selection_latency_ms_max"],
            "heap": float((current.get("memory") or {}).get("used_js_heap_bytes") or 1e30) <= budgets["used_js_heap_bytes_max"],
            "lens_frame_p95": float(lens.get("frame_ms_p95") or 1e30) <= budgets["lens_frame_p95_ms_max"],
            "one_webgl_context": int((physical_row.get("canvas") or {}).get("webgl_context_count") or 0) == 1,
            "lens_shared_context": str((physical_row.get("canvas") or {}).get("lens_uses_shared_context")) == "true",
            "nonblank": int((physical_row.get("canvas") or {}).get("png_data_url_bytes") or 0) > 2_000,
            "no_console_errors": not (current.get("console_errors") or physical_row.get("console_errors")),
        }
        rows.append(
            {
                "profile": profile,
                "status": "pass" if all(checks.values()) else "fail",
                "baseline": {
                    "ready_ms": baseline.get("ready_ms"),
                    "frame_ms_median": baseline_median,
                    "frame_ms_p95": baseline_p95,
                    "selection_latency_ms": baseline.get("selection_latency_ms"),
                    "used_js_heap_bytes": (baseline.get("memory") or {}).get("used_js_heap_bytes"),
                },
                "current_structure": {
                    "ready_ms": current.get("ready_ms"),
                    "frame_ms_median": current_median,
                    "frame_ms_p95": current_p95,
                    "selection_latency_ms": current.get("selection_latency_ms"),
                    "used_js_heap_bytes": (current.get("memory") or {}).get("used_js_heap_bytes"),
                },
                "physical_with_lens": {
                    "frame_ms_p95": physical_row.get("frame_ms_p95"),
                    "lens_frame_ms_p95": lens.get("frame_ms_p95"),
                    "webgl_context_count": (physical_row.get("canvas") or {}).get("webgl_context_count"),
                    "lens_uses_shared_context": (physical_row.get("canvas") or {}).get("lens_uses_shared_context"),
                },
                "ratios": {
                    "ready": _ratio(current.get("ready_ms"), baseline.get("ready_ms")),
                    "structure_frame_median": _ratio(current_median, baseline_median),
                    "structure_frame_p95": _ratio(current_p95, baseline_p95),
                    "selection_latency": _ratio(current.get("selection_latency_ms"), baseline.get("selection_latency_ms")),
                },
                "budgets": budgets,
                "checks": checks,
            }
        )

    report = {
        "schema_version": "spacegate.system_simulation_physical_performance_comparison.v1",
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "pass" if rows and all(row["status"] == "pass" for row in rows) else "fail",
        "inputs": {
            "before": str(args.before),
            "after": str(args.after),
            "physical": str(args.physical),
        },
        "notes": [
            "Headless Chromium frame intervals are quantized by the available software-rendering cadence.",
            "The structure median protects normal-mode parity; p95 permits one additional cadence interval.",
            "The lens must reuse one WebGL context and remain within a 25 percent p95 allowance over current structure mode.",
        ],
        "profiles": rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "output": str(args.output), "profiles": len(rows)}, indent=2))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
