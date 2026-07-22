#!/usr/bin/env python3
"""Account every accepted E4 source at the E5 selection boundary."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SELECTION = ROOT / "config/evidence_lake/e5_selection_policies.json"
DEFAULT_DISPOSITIONS = ROOT / "config/evidence_lake/e5_source_dispositions.json"
DEFAULT_RELEASE_SET = Path(
    "/data/spacegate/state/derived/evidence_lake_v2/scientific_evidence_sets/"
    "6c19de054e9b807674c37d3c/manifest.json"
)
DEFAULT_REPORT = Path(
    "/data/spacegate/state/reports/evidence_lake_v2/e5_source_disposition_audit.json"
)

ALLOWED_DISPOSITIONS = {
    "evidence_only",
    "negative_or_context_evidence_only",
    "identity_scope_evidence_only",
    "deferred_pending_identity_scope",
    "deferred_pending_quantity_policy",
    "deferred_pending_applicability_policy",
    "deferred_pending_projection_policy",
    "selected_relation_evidence_projection",
    "selected_component_evidence_projection",
    "selected_cluster_evidence_projection",
    "selected_extended_object_evidence_projection",
    "selected_solar_system_evidence_projection",
    "selected_planet_evidence_projection",
}


def load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def release_sources(release_set: dict[str, Any]) -> dict[str, str]:
    sources: dict[str, str] = {}
    for member in release_set.get("members") or []:
        releases = member.get("release_ids") or {}
        for source_id in member.get("source_ids") or []:
            if source_id in sources:
                raise ValueError(f"release set repeats source: {source_id}")
            sources[str(source_id)] = str(releases[source_id])
    return sources


def audit(
    release_set: dict[str, Any],
    selection: dict[str, Any],
    dispositions: dict[str, Any],
) -> dict[str, Any]:
    accepted = release_sources(release_set)
    configured_rows = selection.get("selection_sources") or []
    configured_counts = Counter(str(row["source_id"]) for row in configured_rows)
    configured = {str(row["source_id"]): row for row in configured_rows}
    explicit = dict(dispositions.get("explicit_dispositions") or {})

    metadata_errors = sorted(
        name
        for name, valid in {
            "release_set_schema": release_set.get("schema_version")
            == "spacegate.scientific_evidence_release_set.v1",
            "release_set_status": release_set.get("status") == "pass",
            "selection_policy_schema": selection.get("schema_version")
            == "spacegate.selected_fact_policy.v1",
            "disposition_schema": dispositions.get("schema_version")
            == "spacegate.e5_source_dispositions.v1",
            "disposition_version": bool(dispositions.get("disposition_version")),
        }.items()
        if not valid
    )
    duplicate_selection_sources = sorted(
        source_id for source_id, count in configured_counts.items() if count > 1
    )
    conflicts = sorted(set(configured) & set(explicit))
    stale = sorted(set(explicit) - set(accepted))
    unknown_selection_sources = sorted(set(configured) - set(accepted))
    unaccounted = sorted(set(accepted) - set(configured) - set(explicit))
    invalid_dispositions: list[str] = []
    incomplete_dispositions: list[str] = []
    rows: list[dict[str, Any]] = []

    for source_id, release_id in sorted(accepted.items()):
        if source_id in configured:
            source = configured[source_id]
            rows.append(
                {
                    "source_id": source_id,
                    "release_id": release_id,
                    "disposition": "selected_by_policy",
                    "owner": "E5",
                    "blocks_e5": False,
                    "reason": "Source has one or more explicit quantity-group selection policies.",
                    "selected_quantity_groups": sorted(
                        str(group["group_key"])
                        for group in source.get("quantity_groups") or []
                    ),
                }
            )
            continue

        row = explicit.get(source_id)
        if row is None:
            rows.append(
                {
                    "source_id": source_id,
                    "release_id": release_id,
                    "disposition": "unaccounted",
                    "owner": None,
                    "blocks_e5": True,
                    "reason": "Accepted E4 source has neither a selection policy nor an explicit E5 disposition.",
                    "selected_quantity_groups": [],
                }
            )
            continue

        disposition = str(row.get("disposition") or "")
        owner = str(row.get("owner") or "")
        reason = str(row.get("reason") or "")
        if disposition not in ALLOWED_DISPOSITIONS:
            invalid_dispositions.append(source_id)
        if not owner or not reason or not isinstance(row.get("blocks_e5"), bool):
            incomplete_dispositions.append(source_id)
        if disposition == "selected_relation_evidence_projection" and (
            not row.get("relation_policy_version")
            or not row.get("relation_artifact_id")
            or row.get("blocks_e5") is not False
        ):
            incomplete_dispositions.append(source_id)
        if disposition == "selected_component_evidence_projection" and (
            not row.get("component_policy_version")
            or not row.get("component_artifact_id")
            or row.get("blocks_e5") is not False
        ):
            incomplete_dispositions.append(source_id)
        if disposition == "selected_cluster_evidence_projection" and (
            not row.get("cluster_policy_version")
            or not row.get("cluster_artifact_id")
            or row.get("blocks_e5") is not False
        ):
            incomplete_dispositions.append(source_id)
        if disposition == "selected_extended_object_evidence_projection" and (
            not row.get("extended_object_policy_version")
            or not row.get("extended_object_artifact_id")
            or row.get("blocks_e5") is not False
        ):
            incomplete_dispositions.append(source_id)
        if disposition == "selected_solar_system_evidence_projection" and (
            not row.get("solar_system_policy_version")
            or not row.get("solar_system_artifact_id")
            or row.get("blocks_e5") is not False
        ):
            incomplete_dispositions.append(source_id)
        if disposition == "selected_planet_evidence_projection" and (
            not row.get("planet_policy_version")
            or not row.get("planet_artifact_id")
            or row.get("blocks_e5") is not False
        ):
            incomplete_dispositions.append(source_id)
        rows.append(
            {
                "source_id": source_id,
                "release_id": release_id,
                "disposition": disposition,
                "owner": owner or None,
                "blocks_e5": bool(row.get("blocks_e5", True)),
                "reason": reason,
                "selected_quantity_groups": [],
            }
        )

    blocking = sorted(row["source_id"] for row in rows if row["blocks_e5"])
    hard_failures = any(
        (
            conflicts,
            metadata_errors,
            duplicate_selection_sources,
            stale,
            unknown_selection_sources,
            unaccounted,
            invalid_dispositions,
            incomplete_dispositions,
        )
    )
    status = "fail" if hard_failures else "in_progress" if blocking else "pass"
    disposition_counts: dict[str, int] = {}
    for row in rows:
        disposition_counts[row["disposition"]] = (
            disposition_counts.get(row["disposition"], 0) + 1
        )

    return {
        "schema_version": "spacegate.e5_source_disposition_audit.v1",
        "disposition_version": dispositions.get("disposition_version"),
        "release_set_id": release_set.get("release_set_id"),
        "selection_policy_version": selection.get("policy_version"),
        "status": status,
        "checks": {
            "selection_disposition_conflicts": conflicts,
            "metadata_errors": metadata_errors,
            "duplicate_selection_sources": duplicate_selection_sources,
            "stale_explicit_dispositions": stale,
            "unknown_selection_sources": unknown_selection_sources,
            "unaccounted_sources": unaccounted,
            "invalid_dispositions": sorted(invalid_dispositions),
            "incomplete_dispositions": sorted(incomplete_dispositions),
            "blocking_sources": blocking,
        },
        "summaries": {
            "accepted_e4_sources": len(accepted),
            "selected_sources": len(configured),
            "explicit_dispositions": len(explicit),
            "disposition_counts": dict(sorted(disposition_counts.items())),
        },
        "sources": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release-set", type=Path, default=DEFAULT_RELEASE_SET)
    parser.add_argument("--selection", type=Path, default=DEFAULT_SELECTION)
    parser.add_argument("--dispositions", type=Path, default=DEFAULT_DISPOSITIONS)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = audit(load(args.release_set), load(args.selection), load(args.dispositions))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        f"E5 source dispositions {report['status']}: "
        f"accepted={report['summaries']['accepted_e4_sources']} "
        f"selected={report['summaries']['selected_sources']} "
        f"blocking={len(report['checks']['blocking_sources'])}"
    )
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
