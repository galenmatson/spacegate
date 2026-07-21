#!/usr/bin/env python3
"""Account every registered source at the E4 scientific-evidence boundary."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY = ROOT / "config/evidence_lake/source_releases.json"
DEFAULT_CONTRACT = ROOT / "config/evidence_lake/e4_scientific_evidence.json"
DEFAULT_SCOPE = ROOT / "config/evidence_lake/e4_source_scope.json"
DEFAULT_REPORT = Path(
    "/data/spacegate/state/reports/evidence_lake_v2/e4_source_scope_audit.json"
)


def load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def audit(
    registry: dict[str, Any],
    contract: dict[str, Any],
    scope: dict[str, Any],
) -> dict[str, Any]:
    registered = {str(row["source_id"]): row for row in registry["sources"]}
    adapters = set(contract["source_adapters"])
    explicit = dict(scope["explicit_dispositions"])
    conflicts = sorted(adapters & set(explicit))
    stale = sorted(set(explicit) - set(registered))
    unknown_adapters = sorted(adapters - set(registered))
    rows = []
    for source_id, source in sorted(registered.items()):
        if source_id in adapters:
            disposition = "scientific_evidence_adapter"
            owner = "E4"
            blocks_e4 = False
            reason = "Registered source has an explicit E4 adapter."
        elif source_id in explicit:
            disposition = str(explicit[source_id]["disposition"])
            owner = str(explicit[source_id]["owner"])
            blocks_e4 = bool(explicit[source_id]["blocks_e4"])
            reason = str(explicit[source_id]["reason"])
        else:
            disposition = "unaccounted"
            owner = None
            blocks_e4 = True
            reason = "Registered source has neither an E4 adapter nor an explicit boundary disposition."
        rows.append(
            {
                "source_id": source_id,
                "release_id": str(source["release_id"]),
                "registry_state": str(source["state"]),
                "disposition": disposition,
                "owner": owner,
                "blocks_e4": blocks_e4,
                "reason": reason,
            }
        )
    unaccounted = [row["source_id"] for row in rows if row["disposition"] == "unaccounted"]
    blocking = [row["source_id"] for row in rows if row["blocks_e4"]]
    status = (
        "fail"
        if conflicts or stale or unknown_adapters or unaccounted
        else "in_progress"
        if blocking
        else "pass"
    )
    disposition_counts: dict[str, int] = {}
    for row in rows:
        disposition_counts[row["disposition"]] = (
            disposition_counts.get(row["disposition"], 0) + 1
        )
    return {
        "schema_version": "spacegate.e4_source_scope_audit.v1",
        "scope_version": scope["scope_version"],
        "registry_version": registry["registry_version"],
        "contract_version": contract["contract_version"],
        "status": status,
        "checks": {
            "adapter_disposition_conflicts": conflicts,
            "stale_explicit_dispositions": stale,
            "unregistered_adapters": unknown_adapters,
            "unaccounted_sources": unaccounted,
            "blocking_sources": blocking,
        },
        "summaries": {
            "registered_sources": len(registered),
            "e4_adapters": len(adapters),
            "explicit_boundary_dispositions": len(explicit),
            "disposition_counts": dict(sorted(disposition_counts.items())),
        },
        "sources": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--scope", type=Path, default=DEFAULT_SCOPE)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = audit(load(args.registry), load(args.contract), load(args.scope))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        f"E4 source scope {report['status']}: "
        f"registered={report['summaries']['registered_sources']} "
        f"adapters={report['summaries']['e4_adapters']} "
        f"blocking={len(report['checks']['blocking_sources'])}"
    )
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
