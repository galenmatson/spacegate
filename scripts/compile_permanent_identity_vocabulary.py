#!/usr/bin/env python3
"""Export the one-time permanent identity vocabulary migration seed."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_identity_vocabulary_seed.json"
DEFAULT_STATE = Path("/data/spacegate/state")


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


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.permanent_identity_vocabulary_policy.v1":
        raise ValueError("unsupported identity vocabulary policy")
    if policy.get("scientific_authority") is not False:
        raise ValueError("identity vocabulary must not be scientific authority")
    columns = [str(value) for value in policy.get("output_columns") or []]
    prohibited = {str(value) for value in policy.get("prohibited_scientific_columns") or []}
    if not columns or len(columns) != len(set(columns)):
        raise ValueError("identity vocabulary output columns are invalid")
    if set(columns) & prohibited:
        raise ValueError("identity vocabulary includes prohibited scientific columns")
    rules = policy.get("rules") or {}
    required = {
        "future_clean_build_reads_migration_core": False,
        "all_aliases_require_permanent_identity": True,
        "semantic_alias_duplicates_allowed": False,
        "empty_normalized_aliases_allowed": False,
    }
    if any(rules.get(key) is not value for key, value in required.items()):
        raise ValueError("unsafe identity vocabulary migration rules")


def compile_vocabulary(
    policy_path: Path,
    state_dir: Path,
    output_root: Path | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    cpu_started = time.process_time()
    policy = load_object(policy_path)
    validate_policy(policy)
    identity_db = (
        state_dir / "derived/evidence_lake_v2/identity"
        / policy["identity_graph_id"] / "identity_graph.duckdb"
    )
    migration_core = (
        state_dir / "out" / policy["migration_source_build_id"] / "core.duckdb"
    )
    for path in (identity_db, migration_core):
        if not path.is_file():
            raise FileNotFoundError(path)
    identity_sha = file_sha256(identity_db)
    core_sha = file_sha256(migration_core)
    if identity_sha != policy["identity_graph_sha256"]:
        raise ValueError("identity graph checksum mismatch")
    if core_sha != policy["migration_source_core_sha256"]:
        raise ValueError("migration CORE checksum mismatch")
    policy_sha = file_sha256(policy_path)
    compiler_sha = file_sha256(Path(__file__).resolve())
    seed_id = stable_hash({
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "identity_graph_sha256": identity_sha,
        "migration_source_core_sha256": core_sha,
    })[:24]
    root = output_root or (
        state_dir / "derived/evidence_lake_v2/permanent_identity_vocabulary"
    )
    final_dir = root / seed_id
    if (final_dir / "manifest.json").is_file():
        manifest = load_object(final_dir / "manifest.json")
        if manifest.get("seed_id") != seed_id:
            raise ValueError("identity vocabulary seed collision")
        return manifest
    root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{seed_id}.", dir=root))
    output = staging / "aliases.parquet"
    con = duckdb.connect()
    try:
        con.execute(f"ATTACH {sql_literal(str(identity_db))} AS identity (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(str(migration_core))} AS migration (READ_ONLY)")
        con.execute(
            """
            CREATE TABLE vocabulary AS
            SELECT row_number() OVER(ORDER BY n.stable_object_key,a.alias_priority,
                       a.alias_norm,a.alias_raw,a.alias_id)::BIGINT alias_seed_id,
                   n.stable_object_key,n.system_stable_object_key,a.target_type,
                   a.alias_raw,a.alias_norm,a.alias_kind,a.alias_priority,
                   a.is_primary,a.source_catalog,a.source_version,a.source_pk
            FROM migration.aliases a
            JOIN identity.canonical_object_nodes n
              ON n.object_type=a.target_type
             AND try_cast(n.canonical_row_id AS HUGEINT)=a.target_id
            ORDER BY n.stable_object_key,a.alias_priority,a.alias_norm,a.alias_raw,a.alias_id
            """
        )
        source_count = int(con.execute("SELECT count(*) FROM migration.aliases").fetchone()[0])
        output_count = int(con.execute("SELECT count(*) FROM vocabulary").fetchone()[0])
        checks = {
            "source_output_delta": output_count - source_count,
            "duplicate_alias_seed_ids": int(con.execute(
                "SELECT count(*) FROM (SELECT alias_seed_id FROM vocabulary GROUP BY 1 HAVING count(*)>1)"
            ).fetchone()[0]),
            "semantic_alias_duplicates": int(con.execute(
                "SELECT count(*) FROM (SELECT stable_object_key,target_type,alias_norm FROM vocabulary GROUP BY 1,2,3 HAVING count(*)>1)"
            ).fetchone()[0]),
            "empty_normalized_aliases": int(con.execute(
                "SELECT count(*) FROM vocabulary WHERE nullif(trim(alias_norm),'') IS NULL"
            ).fetchone()[0]),
            "missing_identity": int(con.execute(
                "SELECT count(*) FROM migration.aliases a LEFT JOIN identity.canonical_object_nodes n ON n.object_type=a.target_type AND try_cast(n.canonical_row_id AS HUGEINT)=a.target_id WHERE n.stable_object_key IS NULL"
            ).fetchone()[0]),
            "invalid_target_types": int(con.execute(
                "SELECT count(*) FROM vocabulary WHERE target_type NOT IN ('system','star','planet')"
            ).fetchone()[0]),
        }
        if any(checks.values()):
            raise ValueError(f"identity vocabulary verification failed: {checks}")
        con.execute(
            f"COPY vocabulary TO {sql_literal(str(output))} "
            "(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 250000)"
        )
    finally:
        con.close()
    actual_columns = [
        str(row[0]) for row in duckdb.connect().execute(
            f"DESCRIBE SELECT * FROM read_parquet({sql_literal(str(output))})"
        ).fetchall()
    ]
    if actual_columns != policy["output_columns"]:
        raise ValueError("identity vocabulary output schema mismatch")
    manifest = {
        "schema_version": "spacegate.permanent_identity_vocabulary_manifest.v1",
        "seed_id": seed_id,
        "status": "pass",
        "scientific_authority": False,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "policy_version": policy["policy_version"],
        "compiler_version": policy["compiler_version"],
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "identity_graph_id": policy["identity_graph_id"],
        "identity_graph_sha256": identity_sha,
        "migration_source_build_id": policy["migration_source_build_id"],
        "migration_source_core_sha256": core_sha,
        "products": {
            "aliases": {
                "path": output.name,
                "row_count": output_count,
                "bytes": output.stat().st_size,
                "sha256": file_sha256(output),
            }
        },
        "verification": checks,
        "timing": {
            "wall_seconds": round(time.monotonic() - started, 6),
            "cpu_seconds": round(time.process_time() - cpu_started, 6),
            "peak_rss_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
        },
    }
    (staging / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(staging, final_dir)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = compile_vocabulary(
        args.policy.resolve(), args.state_dir.resolve(),
        args.output_root.resolve() if args.output_root else None,
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
