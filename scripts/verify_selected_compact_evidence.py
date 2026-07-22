#!/usr/bin/env python3
"""Independently verify an E5 selected compact-object artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_compact_identity_policies.json"


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def sql_literal(value: Path | str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(canonical_json(value) + b"\n")
    os.replace(temporary, path)


def resolve_artifact(state: Path, artifact: Path | None) -> Path:
    if artifact is not None:
        return artifact.resolve()
    report = read_json(state / "reports/evidence_lake_v2/e5_selected_compact_report.json")
    return Path(str(report["artifact_path"])).resolve()


def verify(*, state: Path, policy_path: Path, artifact: Path, report_path: Path) -> dict[str, Any]:
    started = time.monotonic()
    policy = read_json(policy_path)
    manifest = read_json(artifact / "manifest.json")
    checks: dict[str, int] = {}

    checks["manifest_schema"] = int(manifest.get("schema_version") == "spacegate.e5_selected_compact.v1")
    checks["manifest_pass"] = int(manifest.get("status") == "pass")
    checks["policy_hash"] = int(manifest.get("policy_sha256") == hashlib.sha256(canonical_json(policy)).hexdigest())
    checks["acceptance_counts"] = int(manifest.get("observed") == policy.get("acceptance"))

    declared = manifest.get("deterministic_files") or {}
    checks["five_parquet_files"] = int(len(declared) == 5)
    for relative, metadata in sorted(declared.items()):
        candidate = (artifact / relative).resolve()
        safe = candidate.is_relative_to(artifact) and candidate.is_file()
        checks[f"safe_file:{relative}"] = int(safe)
        if safe:
            checks[f"size:{relative}"] = int(candidate.stat().st_size == int(metadata["bytes"]))
            checks[f"hash:{relative}"] = int(sha256_file(candidate) == metadata["sha256"])

    parquet = artifact / "parquet"
    con = duckdb.connect()
    con.execute("SET threads=4")
    con.execute(
        f"CREATE VIEW nodes AS SELECT * FROM read_parquet({sql_literal(parquet / 'compact_identity_nodes.parquet')})"
    )
    con.execute(
        f"CREATE VIEW aliases AS SELECT * FROM read_parquet({sql_literal(parquet / 'compact_identity_aliases.parquet')})"
    )
    con.execute(
        f"CREATE VIEW outcomes AS SELECT * FROM read_parquet({sql_literal(parquet / 'compact_envelope_outcomes.parquet')})"
    )
    con.execute(
        f"CREATE VIEW facts AS SELECT * FROM read_parquet({sql_literal(parquet / 'selected_compact_facts.parquet')})"
    )
    con.execute(
        f"CREATE VIEW quarantine AS SELECT * FROM read_parquet({sql_literal(parquet / 'compact_scope_quarantine.parquet')})"
    )

    zero_queries = {
        "duplicate_nodes": "SELECT count(*)-count(DISTINCT object_node_key) FROM nodes",
        "duplicate_stable_keys": "SELECT count(*)-count(DISTINCT stable_object_key) FROM nodes",
        "duplicate_aliases": "SELECT count(*)-count(DISTINCT alias_binding_id) FROM aliases",
        "node_outcome_accounting": "SELECT count(*) FROM (SELECT n.object_node_key,count(o.outcome_id) n FROM nodes n LEFT JOIN outcomes o USING(object_node_key) GROUP BY 1 HAVING count(o.outcome_id)<>1)",
        "orphan_aliases": "SELECT count(*) FROM aliases a LEFT JOIN nodes n USING(object_node_key) WHERE n.object_node_key IS NULL",
        "facts_outside_envelope": "SELECT count(*) FROM facts f LEFT JOIN outcomes o USING(object_node_key) WHERE o.outcome IS DISTINCT FROM 'accepted'",
        "facts_without_lineage": "SELECT count(*) FROM facts WHERE source_evidence_id IS NULL OR source_record_id IS NULL",
        "nonrelease_keys": "SELECT count(*) FROM nodes WHERE stable_object_key NOT LIKE 'compact:atnf:name:%' AND stable_object_key NOT LIKE 'compact:mcgill:magnetar:%'",
        "atnf_sign_erasure": "SELECT count(*) FROM nodes WHERE source_id='compact.atnf' AND inventory_identifier LIKE 'J%-%' AND stable_object_key NOT LIKE '%-%'",
        "unsafe_scope_merge": "SELECT count(*) FROM quarantine q JOIN nodes n ON n.stable_object_key=q.candidate_stable_object_key",
    }
    for name, query in zero_queries.items():
        checks[name] = int(con.execute(query).fetchone()[0] == 0)

    observed = {
        "identity_nodes": int(con.execute("SELECT count(*) FROM nodes").fetchone()[0]),
        "atnf_identity_nodes": int(con.execute("SELECT count(*) FROM nodes WHERE source_id='compact.atnf'").fetchone()[0]),
        "mcgill_identity_nodes": int(con.execute("SELECT count(*) FROM nodes WHERE source_id='compact.mcgill_magnetar'").fetchone()[0]),
        "identity_aliases": int(con.execute("SELECT count(*) FROM aliases").fetchone()[0]),
        "envelope_accepted": int(con.execute("SELECT count(*) FROM outcomes WHERE outcome='accepted'").fetchone()[0]),
        "envelope_excluded": int(con.execute("SELECT count(*) FROM outcomes WHERE outcome='excluded'").fetchone()[0]),
        "envelope_missing": int(con.execute("SELECT count(*) FROM outcomes WHERE outcome='missing'").fetchone()[0]),
        "selected_facts": int(con.execute("SELECT count(*) FROM facts").fetchone()[0]),
        "scope_quarantine": int(con.execute("SELECT count(*) FROM quarantine").fetchone()[0]),
    }
    checks["parquet_acceptance_counts"] = int(observed == policy.get("acceptance"))
    checks["source_identity_total"] = int(
        observed["identity_nodes"]
        == observed["atnf_identity_nodes"] + observed["mcgill_identity_nodes"]
    )
    checks["outcome_total"] = int(
        observed["identity_nodes"]
        == observed["envelope_accepted"]
        + observed["envelope_excluded"]
        + observed["envelope_missing"]
    )
    checks["counterpart_quarantined"] = int(
        con.execute(
            "SELECT count(*) FROM quarantine WHERE outcome='quarantined_component_scope_conflict'"
        ).fetchone()[0]
        == observed["scope_quarantine"]
    )
    con.close()

    failures = sorted(name for name, passed in checks.items() if passed != 1)
    report = {
        "schema_version": "spacegate.e5_selected_compact_verification.v1",
        "artifact_path": str(artifact),
        "build_id": manifest.get("build_id"),
        "policy_version": policy.get("policy_version"),
        "observed": observed,
        "checks": checks,
        "check_count": len(checks),
        "failures": failures,
        "status": "pass" if not failures else "fail",
        "wall_seconds": round(time.monotonic() - started, 3),
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    write_json(report_path, report)
    if failures:
        raise ValueError(f"selected compact verification failed: {failures}")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, default=Path(os.environ.get("SPACEGATE_STATE_DIR", DEFAULT_STATE)))
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    state = args.state.resolve()
    artifact = resolve_artifact(state, args.artifact)
    report_path = args.report or state / "reports/evidence_lake_v2/e5_selected_compact_verification.json"
    report = verify(
        state=state,
        policy_path=args.policy.resolve(),
        artifact=artifact,
        report_path=report_path.resolve(),
    )
    print(
        f"Selected compact verification pass: build={report['build_id']} "
        f"checks={report['check_count']} wall={report['wall_seconds']}s"
    )


if __name__ == "__main__":
    main()
