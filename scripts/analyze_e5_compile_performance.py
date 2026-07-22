#!/usr/bin/env python3
"""Summarize E5 compiler phase telemetry and measured optimization targets."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


TIMING_CONTRACT = "spacegate.e5_compile_performance.v1"
REPORT_CONTRACT = "spacegate.e5_compile_performance_analysis.v1"


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def phase_category(phase: str) -> str:
    if phase.startswith("source_"):
        return phase
    if phase.startswith("integrity_check."):
        return "integrity_checks"
    if phase in {
        "selected_fact_exports",
        "selection_decision_exports",
        "auxiliary_exports",
        "export_validation",
    }:
        return "exports"
    if phase in {
        "checkpoint",
        "artifact_hashing",
        "manifest_write",
        "artifact_promotion",
        "current_pointer_promotion",
    }:
        return "artifact_finalization"
    return phase


def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "phase_count": len(rows),
        "wall_seconds": round(sum(float(row["wall_seconds"]) for row in rows), 6),
        "cpu_seconds": round(sum(float(row["cpu_seconds"]) for row in rows), 6),
    }


def analyze(timing: dict[str, Any], compile_report: dict[str, Any]) -> dict[str, Any]:
    if timing.get("schema_version") != TIMING_CONTRACT or timing.get("status") != "pass":
        raise ValueError("performance analysis requires a passing E5 timing report")
    if compile_report.get("status") != "pass":
        raise ValueError("performance analysis requires a passing E5 compile report")
    if timing.get("build_id") != compile_report.get("build_id"):
        raise ValueError("timing and compile reports identify different builds")
    phases = timing.get("phases")
    if not isinstance(phases, list) or not phases:
        raise ValueError("timing report has no completed phases")
    if any(row.get("status") != "pass" for row in phases):
        raise ValueError("timing report contains a non-passing phase")

    total = summarize_rows(phases)
    total_wall = float(total["wall_seconds"])
    total_cpu = float(total["cpu_seconds"])
    categories: dict[str, list[dict[str, Any]]] = defaultdict(list)
    sources: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in phases:
        categories[phase_category(str(row["phase"]))].append(row)
        if row.get("source_id"):
            sources[str(row["source_id"])].append(row)

    def with_share(summary: dict[str, Any]) -> dict[str, Any]:
        return {
            **summary,
            "wall_percent": round(100 * float(summary["wall_seconds"]) / total_wall, 3),
            "cpu_percent": round(100 * float(summary["cpu_seconds"]) / total_cpu, 3),
        }

    category_rows = [
        {"category": name, **with_share(summarize_rows(rows))}
        for name, rows in categories.items()
    ]
    category_rows.sort(key=lambda row: (-float(row["wall_seconds"]), row["category"]))
    source_rows = [
        {
            "source_id": source_id,
            **with_share(summarize_rows(rows)),
            "phases": [
                {
                    "phase": row["phase"],
                    "wall_seconds": row["wall_seconds"],
                    "cpu_seconds": row["cpu_seconds"],
                    "details": row.get("details") or {},
                }
                for row in rows
            ],
        }
        for source_id, rows in sources.items()
    ]
    source_rows.sort(key=lambda row: (-float(row["wall_seconds"]), row["source_id"]))

    top_phases = sorted(
        (
            {
                "phase": row["phase"],
                "source_id": row.get("source_id"),
                "wall_seconds": row["wall_seconds"],
                "cpu_seconds": row["cpu_seconds"],
                "wall_percent": round(100 * float(row["wall_seconds"]) / total_wall, 3),
                "details": row.get("details") or {},
            }
            for row in phases
        ),
        key=lambda row: (-float(row["wall_seconds"]), str(row["phase"])),
    )

    candidate_by_key = {
        (str(row["phase"]), str(row.get("source_id") or "")): row for row in top_phases
    }
    optimization_candidates: list[dict[str, Any]] = []
    gaia_insert = candidate_by_key.get(
        ("source_candidate_insertion", "gaia.dr3.gaia_source")
    )
    if gaia_insert:
        optimization_candidates.append(
            {
                "priority": 1,
                "target": "gaia_source_direct_fact_materialization",
                "measured_wall_seconds": gaia_insert["wall_seconds"],
                "measured_wall_percent": gaia_insert["wall_percent"],
                "next_experiment": "Profile direct fact encoding and compare a Parquet-first or single-durable-representation path; one-time accepted-binding materialization was measured and rejected after increasing this phase from 540.0 to 661.5 seconds.",
                "constraint": "Fact identity, exact binding lineage, row counts, and deterministic partition hashes must remain unchanged.",
            }
        )
    global_selection = candidate_by_key.get(("global_parameter_set_selection", ""))
    if global_selection:
        optimization_candidates.append(
            {
                "priority": 2,
                "target": "global_authority_selection",
                "measured_wall_seconds": global_selection["wall_seconds"],
                "measured_wall_percent": global_selection["wall_percent"],
                "next_experiment": "Separate independent source-record scalar facts from coherent parameter-set competitions, then compare logical decisions and hashes against the unified selector.",
                "constraint": "Authority ordering, coherent-set selection, duplicate prevention, exact evidence lineage, and lower-authority rejection must remain unchanged.",
            }
        )
    exports = next((row for row in category_rows if row["category"] == "exports"), None)
    if exports:
        optimization_candidates.append(
            {
                "priority": 3,
                "target": "partitioned_parquet_exports",
                "measured_wall_seconds": exports["wall_seconds"],
                "measured_wall_percent": exports["wall_percent"],
                "next_experiment": "Compare the existing stable per-quantity COPY loop with a one-pass partitioned export in disposable scratch.",
                "constraint": "Preserve stable filenames, row ordering, compression, row accounting, and logical hashes before changing the artifact contract.",
            }
        )
    artifact_hashing = candidate_by_key.get(("artifact_hashing", ""))
    if artifact_hashing:
        optimization_candidates.append(
            {
                "priority": 4,
                "target": "artifact_hashing_readback",
                "measured_wall_seconds": artifact_hashing["wall_seconds"],
                "measured_wall_percent": artifact_hashing["wall_percent"],
                "next_experiment": "Measure export-integrated hashing or filesystem-backed immutable digests against the current post-export full read.",
                "constraint": "Preserve byte hashes, logical content hashes, corrupt-artifact detection, and clean reproduction; manifest trust alone is insufficient.",
            }
        )
    input_verification = next(
        (
            row
            for row in category_rows
            if row["category"] == "immutable_e4_input_verification"
        ),
        None,
    )
    if input_verification:
        optimization_candidates.append(
            {
                "priority": 5,
                "target": "immutable_input_checksum_verification",
                "measured_wall_seconds": input_verification["wall_seconds"],
                "measured_wall_percent": input_verification["wall_percent"],
                "next_experiment": "Compare one, two, and four parallel byte-hash workers while checking aggregate storage throughput and build contention.",
                "constraint": "Do not replace byte-level input verification with mtime, size, or manifest trust alone.",
            }
        )
    bj_source = next(
        (
            row
            for row in source_rows
            if row["source_id"] == "distance.gaia_edr3_bailer_jones"
        ),
        None,
    )
    if bj_source:
        optimization_candidates.append(
            {
                "priority": 6,
                "target": "bailer_jones_binding_and_direct_selection",
                "measured_wall_seconds": bj_source["wall_seconds"],
                "measured_wall_percent": bj_source["wall_percent"],
                "next_experiment": "Profile accepted-versus-missing identity joins and test deterministic namespace-bucketed binding.",
                "constraint": "Retain all accepted, missing, ambiguous, excluded, and quarantined accounting outcomes.",
            }
        )

    return {
        "schema_version": REPORT_CONTRACT,
        "status": "pass",
        "build_id": timing["build_id"],
        "compiler_version": timing["compiler_version"],
        "total": with_share(total),
        "peak_process_rss_kib": max(
            int(row.get("process_peak_rss_kib") or 0) for row in phases
        ),
        "peak_staging_allocated_bytes": max(
            int(row.get("peak_staging_allocated_bytes") or 0) for row in phases
        ),
        "peak_spill_allocated_bytes": max(
            int(row.get("peak_spill_allocated_bytes") or 0) for row in phases
        ),
        "table_counts": compile_report.get("table_counts") or {},
        "categories": category_rows,
        "sources": source_rows,
        "top_phases": top_phases,
        "optimization_candidates": optimization_candidates,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timing", type=Path, required=True)
    parser.add_argument("--compile-report", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = analyze(load_json(args.timing), load_json(args.compile_report))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.report.with_name(f".{args.report.name}.tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(args.report)
    print(
        f"E5 performance analysis pass: {report['build_id']} "
        f"wall={report['total']['wall_seconds']:.3f}s "
        f"phases={report['total']['phase_count']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
