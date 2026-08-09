#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import math
import statistics
from collections import Counter
from pathlib import Path
from typing import Any


SIM_DAYS_PER_SECOND = 0.7
RATE_OPTIONS = (0.25, 1, 5, 20, 100, 500, 1000, 5000, 10000)
REJECTED_PERIOD_STATUSES = {"", "assumed", "missing", "unknown", "ambiguous", "quarantined"}


def accepted_period(field: Any) -> float | None:
    if not isinstance(field, dict):
        return None
    status = str(field.get("status") or "").strip().lower()
    value = field.get("value")
    try:
        period = float(value)
    except (TypeError, ValueError):
        return None
    if status in REJECTED_PERIOD_STATUSES or not math.isfinite(period) or period <= 0:
        return None
    return period


def closest_rate(target: float) -> float:
    target = max(RATE_OPTIONS[0], min(RATE_OPTIONS[-1], target))
    return min(RATE_OPTIONS, key=lambda rate: abs(math.log(rate) - math.log(target)))


def quantiles(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "p25": None, "median": None, "p75": None, "p95": None, "max": None}
    ordered = sorted(values)
    pick = lambda fraction: ordered[min(len(ordered) - 1, round((len(ordered) - 1) * fraction))]
    return {
        "min": ordered[0],
        "p25": pick(0.25),
        "median": statistics.median(ordered),
        "p75": pick(0.75),
        "p95": pick(0.95),
        "max": ordered[-1],
    }


def analyze_scene(scene: dict[str, Any]) -> dict[str, Any]:
    render_scene = scene.get("render_scene") or {}
    render_bodies = render_scene.get("bodies") or {}
    planet_periods = []
    for planet in render_bodies.get("planets") or []:
        period = accepted_period((planet.get("fields") or {}).get("orbital_period_days"))
        if period is not None:
            planet_periods.append(period)

    stellar = []
    for orbit in render_scene.get("orbits") or []:
        period = accepted_period((orbit.get("fields") or {}).get("period_days"))
        if period is None:
            continue
        member_count = len(set(
            list(orbit.get("primary_child_body_keys") or [])
            + list(orbit.get("secondary_child_body_keys") or [])
        ))
        stellar.append({"period_days": period, "member_count": member_count})

    result = {
        "system_id": int(float((scene.get("system") or {}).get("system_id") or 0)),
        "display_name": (scene.get("system") or {}).get("display_name"),
        "planet_periods": planet_periods,
        "stellar_periods": [item["period_days"] for item in stellar],
    }
    if planet_periods:
        fastest = min(planet_periods)
        target = fastest / (SIM_DAYS_PER_SECOND * 5.0)
        result["planet_policy"] = {
            "anchor_period_days": fastest,
            "target_seconds": 5,
            "unbounded_multiplier": target,
            "nearest_manual_rate": closest_rate(target),
        }
    if stellar:
        shortest = min(stellar, key=lambda item: item["period_days"])
        top_level = max(stellar, key=lambda item: (item["member_count"], item["period_days"]))
        result["stellar_policy"] = {
            "shortest": {
                **shortest,
                "target_seconds": 60,
                "nearest_manual_rate": closest_rate(shortest["period_days"] / (SIM_DAYS_PER_SECOND * 60.0)),
            },
            "top_level": {
                **top_level,
                "target_seconds": 60,
                "nearest_manual_rate": closest_rate(top_level["period_days"] / (SIM_DAYS_PER_SECOND * 60.0)),
            },
        }
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure candidate scene-aware simulation rates without activating them.")
    parser.add_argument("--scene-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    rows = []
    build_ids = Counter()
    for path in sorted(args.scene_dir.glob("system_*.json.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            scene = json.load(handle)
        build_ids[str(scene.get("build_id") or "unknown")] += 1
        rows.append(analyze_scene(scene))

    planet_periods = [period for row in rows for period in row["planet_periods"]]
    stellar_periods = [period for row in rows for period in row["stellar_periods"]]
    planet_rates = Counter(str(row["planet_policy"]["nearest_manual_rate"]) for row in rows if row.get("planet_policy"))
    shortest_rates = Counter(str(row["stellar_policy"]["shortest"]["nearest_manual_rate"]) for row in rows if row.get("stellar_policy"))
    top_rates = Counter(str(row["stellar_policy"]["top_level"]["nearest_manual_rate"]) for row in rows if row.get("stellar_policy"))
    report = {
        "schema_version": "simulation_initial_rate_policy_review_v1",
        "activation": "disabled",
        "decision": "Keep predictable 1x initial speed. Candidate rates remain a measured visual-review input.",
        "input": {"scene_dir": str(args.scene_dir), "build_ids": dict(build_ids)},
        "policy": {
            "accepted_period_status": "positive render field whose status is not assumed, missing, unknown, ambiguous, or quarantined",
            "planet_candidate": "fastest accepted rendered planet orbit targets about five real seconds",
            "stellar_candidates": "shortest and widest-member accepted stellar orbits each target about sixty real seconds",
            "manual_rates": RATE_OPTIONS,
        },
        "coverage": {
            "scene_count": len(rows),
            "planet_scene_count": sum(bool(row.get("planet_policy")) for row in rows),
            "stellar_scene_count": sum(bool(row.get("stellar_policy")) for row in rows),
            "planet_period_count": len(planet_periods),
            "stellar_period_count": len(stellar_periods),
        },
        "period_days": {"planets": quantiles(planet_periods), "stellar": quantiles(stellar_periods)},
        "candidate_rate_counts": {
            "planet_fastest_five_seconds": dict(planet_rates),
            "stellar_shortest_sixty_seconds": dict(shortest_rates),
            "stellar_top_level_sixty_seconds": dict(top_rates),
        },
        "systems": rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), **report["coverage"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
