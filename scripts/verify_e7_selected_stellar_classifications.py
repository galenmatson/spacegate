#!/usr/bin/env python3
"""Independently verify selected stellar source-model classifications."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_selected_stellar_classifications.json"
DEFAULT_ROOT = Path("/mnt/space/spacegate/e7-selected-stellar-classifications")


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify(build_dir: Path, policy_path: Path = DEFAULT_POLICY) -> dict[str, Any]:
    started = time.monotonic()
    manifest = load_object(build_dir / "manifest.json")
    policy = load_object(policy_path)
    failures: dict[str, Any] = {}
    if manifest.get("status") != "pass":
        failures["manifest_status"] = manifest.get("status")
    if manifest.get("policy_sha256") != file_sha256(policy_path):
        failures["policy_sha256"] = manifest.get("policy_sha256")
    if manifest.get("stability_databases_opened") != []:
        failures["stability_databases_opened"] = manifest.get("stability_databases_opened")
    nonzero_manifest = {
        key: value for key, value in (manifest.get("verification") or {}).items()
        if value != 0
    }
    if nonzero_manifest:
        failures["manifest_verification"] = nonzero_manifest
    product_failures: dict[str, Any] = {}
    for relative, expected in sorted((manifest.get("products") or {}).items()):
        path = build_dir / relative
        if not path.is_file():
            product_failures[relative] = "missing"
            continue
        actual = {"bytes": path.stat().st_size, "sha256": file_sha256(path)}
        if actual != {"bytes": expected.get("bytes"), "sha256": expected.get("sha256")}:
            product_failures[relative] = {"expected": expected, "actual": actual}
    if product_failures:
        failures["products"] = product_failures

    checks: dict[str, int] = {}
    summaries: dict[str, Any] = {}
    database = build_dir / "selected_stellar_classifications.duckdb"
    if database.is_file():
        con = duckdb.connect(str(database), read_only=True)
        try:
            contract = policy["gaia_dsc_white_dwarf"]
            counts = {
                "threshold_candidates": int(con.execute(
                    "SELECT count(*) FROM stellar_model_classification_bindings"
                ).fetchone()[0]),
                "selected_classifications": int(con.execute(
                    "SELECT count(*) FROM selected_stellar_model_classifications"
                ).fetchone()[0]),
            }
            outcomes = {
                str(status): int(count)
                for status, count in con.execute(
                    "SELECT binding_status,count(*) FROM stellar_model_classification_bindings "
                    "GROUP BY 1 ORDER BY 1"
                ).fetchall()
            }
            checks = {
                "threshold_candidate_delta": counts["threshold_candidates"]
                - int(contract["expected_threshold_candidates"]),
                "selected_delta": counts["selected_classifications"]
                - int(contract["expected_binding_outcomes"]["accepted"]),
                "binding_partition_delta": counts["threshold_candidates"] - sum(outcomes.values()),
                "unexpected_binding_status": int(con.execute(
                    "SELECT count(*) FROM stellar_model_classification_bindings "
                    "WHERE binding_status NOT IN ('accepted','ambiguous','missing')"
                ).fetchone()[0]),
                "duplicate_selected_stars": int(con.execute(
                    "SELECT count(*) FROM (SELECT star_id FROM selected_stellar_model_classifications "
                    "GROUP BY 1 HAVING count(*)<>1)"
                ).fetchone()[0]),
                "selected_contract_mismatch": int(con.execute(
                    "SELECT count(*) FROM selected_stellar_model_classifications "
                    "WHERE classification_value<>? OR classification_status<>? OR evidence_basis<>? "
                    "OR confidence_score<? OR evidence_id IS NULL OR selected_fact_id IS NULL",
                    [contract["classification_value"], contract["classification_status"],
                     contract["evidence_basis"], contract["probability_threshold"]],
                ).fetchone()[0]),
                "identity_or_containment_promotions": int(con.execute(
                    "SELECT count(*) FROM stellar_model_classification_bindings "
                    "WHERE creates_canonical_identity OR creates_canonical_containment"
                ).fetchone()[0]),
                "unaccepted_with_star": int(con.execute(
                    "SELECT count(*) FROM stellar_model_classification_bindings "
                    "WHERE binding_status<>'accepted' AND star_id IS NOT NULL"
                ).fetchone()[0]),
                "accepted_without_star": int(con.execute(
                    "SELECT count(*) FROM stellar_model_classification_bindings "
                    "WHERE binding_status='accepted' AND star_id IS NULL"
                ).fetchone()[0]),
            }
            for status, expected in contract["expected_binding_outcomes"].items():
                checks[f"binding_{status}_delta"] = outcomes.get(status, 0) - int(expected)
            summaries = {"counts": counts, "binding_outcomes": outcomes}
        finally:
            con.close()
    else:
        failures["database"] = "missing"
    nonzero = {key: value for key, value in checks.items() if value}
    if nonzero:
        failures["invariants"] = nonzero
    return {
        "schema_version": "spacegate.e7_selected_stellar_classifications_verification.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_id": manifest.get("build_id"),
        "status": "pass" if not failures else "fail",
        "checks": checks,
        "summaries": summaries,
        "failing_checks": failures,
        "wall_seconds": round(time.monotonic() - started, 6),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = verify(
        args.artifact_root.resolve() / args.build_id,
        args.policy.resolve(),
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
