#!/usr/bin/env python3
"""Compare two physical-extent coverage audits relation by relation."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    if value.get("schema_version") != "spacegate.physical_extent_coverage_audit.v1":
        raise ValueError(f"unsupported coverage report: {path}")
    return value


def _is_retired_legacy_mass_derivation(before: dict[str, Any], after: dict[str, Any]) -> bool:
    """Identify an old Kepler axis that depended on unselected endpoint masses.

    The baseline audit records both masses accepted by the shared exact-leaf
    projection and masses consumed by the legacy scene builder. A derived axis
    is not scientific parity when only the latter set was complete.
    """
    selected_before = int(before.get("known_endpoint_masses") or 0)
    legacy_before = int(before.get("legacy_known_endpoint_masses") or 0)
    selected_after = int(after.get("known_endpoint_masses") or 0)
    missing_after = int(after.get("missing_endpoint_masses") or 0)
    return (
        before.get("state") == "derived"
        and before.get("axis_basis") == "kepler_period_total_mass"
        and legacy_before > selected_before
        and after.get("state") == "unavailable"
        and after.get("axis_basis") == "unavailable"
        and selected_after >= selected_before
        and missing_after > 0
    )


def compare(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_rows = {
        str(row["relation_key"]): row
        for row in before.get("relation_inventory") or []
    }
    after_rows = {
        str(row["relation_key"]): row
        for row in after.get("relation_inventory") or []
    }
    shared = sorted(before_rows.keys() & after_rows.keys())
    transitions = Counter(
        f"{before_rows[key].get('state')}->{after_rows[key].get('state')}"
        for key in shared
    )
    recovered = [
        {"before": before_rows[key], "after": after_rows[key]}
        for key in shared
        if before_rows[key].get("state") in {"unavailable", "rejected"}
        and after_rows[key].get("state") in {"physical", "derived"}
    ]
    regression_candidates = [
        {"before": before_rows[key], "after": after_rows[key]}
        for key in shared
        if before_rows[key].get("state") in {"physical", "derived"}
        and after_rows[key].get("state") in {"unavailable", "rejected"}
    ]
    justified_retirements = [
        {
            **candidate,
            "retirement_reason": "legacy_kepler_axis_used_unselected_endpoint_mass",
        }
        for candidate in regression_candidates
        if _is_retired_legacy_mass_derivation(candidate["before"], candidate["after"])
    ]
    regressions = [
        candidate
        for candidate in regression_candidates
        if not _is_retired_legacy_mass_derivation(candidate["before"], candidate["after"])
    ]
    count_keys = sorted(
        set((before.get("counts") or {}).keys()) | set((after.get("counts") or {}).keys())
    )
    return {
        "schema_version": "spacegate.physical_extent_coverage_comparison.v2",
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "before_label": before.get("label"),
        "after_label": after.get("label"),
        "before_artifact_set_sha256": before.get("artifact_set_sha256"),
        "after_artifact_set_sha256": after.get("artifact_set_sha256"),
        "relation_accounting": {
            "before": len(before_rows),
            "after": len(after_rows),
            "shared": len(shared),
            "removed": sorted(before_rows.keys() - after_rows.keys()),
            "added": sorted(after_rows.keys() - before_rows.keys()),
        },
        "count_deltas": {
            key: {
                "before": int((before.get("counts") or {}).get(key, 0)),
                "after": int((after.get("counts") or {}).get(key, 0)),
                "delta": int((after.get("counts") or {}).get(key, 0))
                - int((before.get("counts") or {}).get(key, 0)),
            }
            for key in count_keys
        },
        "transitions": dict(sorted(transitions.items())),
        "recovered_count": len(recovered),
        "recovered_relations": recovered,
        "justified_retirement_count": len(justified_retirements),
        "justified_retirements": justified_retirements,
        "regression_count": len(regressions),
        "regressions": regressions,
        "status": "pass" if not regressions and len(before_rows) == len(after_rows) == len(shared) else "fail",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", required=True, type=Path)
    parser.add_argument("--after", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    report = compare(_load(args.before), _load(args.after))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "status": report["status"],
                "recovered_count": report["recovered_count"],
                "justified_retirement_count": report["justified_retirement_count"],
                "regression_count": report["regression_count"],
                "transitions": report["transitions"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
