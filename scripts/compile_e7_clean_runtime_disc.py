#!/usr/bin/env python3
"""Compile the clean E7 DISC coolness projection without stability databases."""

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
from typing import Any, Callable

import duckdb

import score_coolness


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_clean_runtime_disc.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_OUTPUT_ROOT = Path("/mnt/space/spacegate/e7-clean-runtime-disc")


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def write_object_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Timings:
    def __init__(self) -> None:
        self.started = time.monotonic()
        self.cpu_started = time.process_time()
        self.phases: list[dict[str, Any]] = []

    def run(self, name: str, fn: Callable[[], Any]) -> Any:
        started = time.monotonic()
        cpu_started = time.process_time()
        before = resource.getrusage(resource.RUSAGE_SELF)
        status = "fail"
        try:
            result = fn()
            status = "pass"
            return result
        finally:
            after = resource.getrusage(resource.RUSAGE_SELF)
            self.phases.append({
                "phase": name,
                "wall_seconds": round(time.monotonic() - started, 6),
                "cpu_seconds": round(time.process_time() - cpu_started, 6),
                "peak_rss_kib_after": int(after.ru_maxrss),
                "input_blocks_delta": int(after.ru_inblock - before.ru_inblock),
                "output_blocks_delta": int(after.ru_oublock - before.ru_oublock),
                "status": status,
            })

    def report(self) -> dict[str, Any]:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return {
            "wall_seconds": round(time.monotonic() - self.started, 6),
            "cpu_seconds": round(time.process_time() - self.cpu_started, 6),
            "peak_rss_kib": int(usage.ru_maxrss),
            "phases": self.phases,
        }


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.e7_clean_runtime_disc_policy.v1":
        raise ValueError("unsupported clean runtime DISC policy")
    rules = policy.get("rules") or {}
    required_rules = {
        "open_stability_databases": False,
        "require_selected_stellar_surfaces": True,
        "allow_core_classification_fallback": False,
        "allow_disc_values_to_mutate_science": False,
        "luminosity_proxy_is_presentation_assumption": True,
        "one_score_per_canonical_system": True,
    }
    if any(rules.get(key) is not expected for key, expected in required_rules.items()):
        raise ValueError("unsafe clean runtime DISC rules")
    if set(policy.get("inputs") or {}) != {"clean_runtime_core", "clean_runtime_arm"}:
        raise ValueError("clean runtime DISC inputs are incomplete")
    expected_input_dirs = {
        "clean_runtime_core": "e7-clean-runtime-core",
        "clean_runtime_arm": "e7-clean-runtime-arm",
    }
    for name, spec in policy["inputs"].items():
        relative = Path(str(spec.get("relative_path") or ""))
        if not spec.get("build_id") or len(str(spec.get("manifest_sha256") or "")) != 64:
            raise ValueError(f"invalid input identity: {name}")
        if relative.parts != (
            "..", expected_input_dirs[name], str(spec["build_id"])
        ):
            raise ValueError(f"invalid bounded input path: {name}")
    profile = policy.get("coolness_profile") or {}
    raw_weights = profile.get("weights") or {}
    if set(raw_weights) != set(score_coolness.DEFAULT_WEIGHTS):
        raise ValueError("coolness weight set is incomplete")
    weights = score_coolness.validate_weights(raw_weights)
    if score_coolness._hash_weights(weights) != profile.get("weights_hash"):
        raise ValueError("coolness weights hash mismatch")
    expected_profile_hash = stable_hash({
        "schema_version": 1,
        "profile_id": profile.get("profile_id"),
        "profile_version": profile.get("profile_version"),
        "weights": weights,
    })
    if profile.get("profile_hash") != expected_profile_hash:
        raise ValueError("coolness profile hash mismatch")


def resolve_input(output_root: Path, spec: dict[str, Any]) -> dict[str, Any]:
    root = (output_root / spec["relative_path"]).resolve()
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)
    actual_sha = file_sha256(manifest_path)
    if actual_sha != spec["manifest_sha256"]:
        raise ValueError(f"input manifest checksum mismatch: {root.name}")
    manifest = load_object(manifest_path)
    if manifest.get("build_id") != spec["build_id"] or manifest.get("status") != "pass":
        raise ValueError(f"unaccepted input manifest: {root.name}")
    return {"root": root, "manifest": manifest, "manifest_sha256": actual_sha}


def product_path(source: dict[str, Any], relative: str) -> Path:
    path = source["root"] / relative
    entry = (source["manifest"].get("products") or {}).get(relative)
    if not path.is_file() or not isinstance(entry, dict):
        raise FileNotFoundError(f"unregistered input product: {path}")
    if file_sha256(path) != entry.get("sha256"):
        raise ValueError(f"input product checksum mismatch: {path}")
    return path


def add_metadata(db_path: Path, metadata: dict[str, str]) -> None:
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE TABLE build_metadata(key VARCHAR PRIMARY KEY,value VARCHAR NOT NULL)")
        con.executemany("INSERT INTO build_metadata VALUES (?,?)", sorted(metadata.items()))
        con.execute("CHECKPOINT")
    finally:
        con.close()


def export_scores(db_path: Path, parquet_path: Path) -> None:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute(
            f"COPY (SELECT * FROM coolness_scores ORDER BY rank) TO "
            f"{sql_literal(parquet_path)} (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()


def scalar(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: list[Any] | None = None,
) -> int:
    return int(con.execute(sql, params or []).fetchone()[0] or 0)


def verify_internal(
    db_path: Path,
    core_db: Path,
    arm_db: Path,
    *,
    build_id: str,
    profile: dict[str, Any],
) -> dict[str, Any]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute(f"ATTACH {sql_literal(core_db)} AS core (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(arm_db)} AS arm (READ_ONLY)")
        counts = {
            "canonical_systems": scalar(con, "SELECT count(*) FROM core.systems"),
            "canonical_stars": scalar(con, "SELECT count(*) FROM core.stars"),
            "coolness_scores": scalar(con, "SELECT count(*) FROM coolness_scores"),
            "selected_display_classifications": scalar(
                con, "SELECT count(*) FROM arm.e6_selected_stellar_display_classifications"
            ),
            "selected_stellar_parameters": scalar(
                con, "SELECT count(*) FROM arm.e6_selected_stellar_parameters"
            ),
            "systems_with_planets": scalar(
                con, "SELECT count(*) FROM coolness_scores WHERE planet_count>0"
            ),
            "systems_with_proxy_planet_temperature": scalar(
                con, "SELECT count(*) FROM coolness_scores WHERE nice_planet_proxy_insolation_count>0"
            ),
        }
        checks = {
            "system_inventory_delta": counts["coolness_scores"] - counts["canonical_systems"],
            "selected_display_inventory_delta": counts["selected_display_classifications"] - counts["canonical_stars"],
            "selected_parameter_inventory_delta": counts["selected_stellar_parameters"] - counts["canonical_stars"],
            "duplicate_system_ids": scalar(con, "SELECT count(*) FROM (SELECT system_id FROM coolness_scores GROUP BY 1 HAVING count(*)<>1)"),
            "duplicate_stable_keys": scalar(con, "SELECT count(*) FROM (SELECT stable_object_key FROM coolness_scores GROUP BY 1 HAVING count(*)<>1)"),
            "missing_systems": scalar(con, "SELECT count(*) FROM core.systems s ANTI JOIN coolness_scores c USING(system_id)"),
            "orphan_scores": scalar(con, "SELECT count(*) FROM coolness_scores c ANTI JOIN core.systems s USING(system_id)"),
            "identity_mismatches": scalar(con, "SELECT count(*) FROM coolness_scores c JOIN core.systems s USING(system_id) WHERE c.stable_object_key IS DISTINCT FROM s.stable_object_key"),
            "invalid_rank_range": scalar(con, f"SELECT count(*) FROM coolness_scores WHERE rank<1 OR rank>{counts['canonical_systems']}"),
            "duplicate_ranks": scalar(con, "SELECT count(*) FROM (SELECT rank FROM coolness_scores GROUP BY 1 HAVING count(*)<>1)"),
            "invalid_scores": scalar(con, "SELECT count(*) FROM coolness_scores WHERE NOT isfinite(score_total) OR score_total<0 OR score_total>100"),
            "invalid_profile": scalar(con, "SELECT count(*) FROM coolness_scores WHERE build_id<>? OR profile_id<>? OR profile_version<>?", [build_id, profile["profile_id"], profile["profile_version"]]),
            "invalid_dominant_class": scalar(con, "SELECT count(*) FROM coolness_scores WHERE dominant_spectral_class NOT IN ('O','B','A','F','G','K','M','L','T','Y','WR','WD','NS','PULSAR','MAGNETAR','BLACK HOLE','UNKNOWN','?')"),
        }
        failing = {key: value for key, value in checks.items() if value != 0}
        return {"status": "pass" if not failing else "fail", "counts": counts, "checks": checks, "failing_checks": failing}
    finally:
        con.close()


def compile_runtime_disc(
    policy_path: Path,
    state: Path,
    output_root: Path,
    *,
    link_into_state: bool = True,
) -> dict[str, Any]:
    timing = Timings()
    policy = timing.run("load_and_validate_policy", lambda: load_object(policy_path))
    validate_policy(policy)
    core_input = timing.run(
        "verify_core_manifest", lambda: resolve_input(output_root, policy["inputs"]["clean_runtime_core"])
    )
    arm_input = timing.run(
        "verify_arm_manifest", lambda: resolve_input(output_root, policy["inputs"]["clean_runtime_arm"])
    )
    core_db = timing.run("verify_core_database", lambda: product_path(core_input, "core.duckdb"))
    arm_db = timing.run("verify_arm_database", lambda: product_path(arm_input, "arm.duckdb"))
    compiler_sha = file_sha256(Path(__file__).resolve())
    scorer_sha = file_sha256(Path(score_coolness.__file__).resolve())
    policy_sha = file_sha256(policy_path)
    input_identity = {
        name: {
            "build_id": spec["build_id"],
            "manifest_sha256": spec["manifest_sha256"],
        }
        for name, spec in sorted(policy["inputs"].items())
    }
    build_id = stable_hash({
        "compiler_sha256": compiler_sha,
        "scorer_sha256": scorer_sha,
        "policy_sha256": policy_sha,
        "inputs": input_identity,
    })[:24]
    final_dir = output_root / build_id
    if (final_dir / "manifest.json").is_file():
        manifest = load_object(final_dir / "manifest.json")
        if manifest.get("build_id") != build_id:
            raise ValueError("clean runtime DISC build collision")
        return manifest

    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    db_path = staging / "disc.duckdb"
    parquet_path = staging / "coolness_scores.parquet"
    profile = policy["coolness_profile"]
    try:
        timing.run(
            "score_coolness",
            lambda: score_coolness.build_scores(
                core_db_path=core_db,
                disc_db_path=db_path,
                arm_db_path=arm_db,
                weights=score_coolness.validate_weights(profile["weights"]),
                build_id=build_id,
                profile_id=profile["profile_id"],
                profile_version=profile["profile_version"],
                require_selected_surfaces=True,
                allow_core_classification_fallback=False,
            ),
        )
        timing.run(
            "build_metadata",
            lambda: add_metadata(db_path, {
                "build_id": build_id,
                "compiler_version": policy["compiler_version"],
                "core_build_id": policy["inputs"]["clean_runtime_core"]["build_id"],
                "arm_build_id": policy["inputs"]["clean_runtime_arm"]["build_id"],
                "profile_id": profile["profile_id"],
                "profile_version": profile["profile_version"],
                "profile_hash": profile["profile_hash"],
                "weights_hash": profile["weights_hash"],
                "classification_authority": "selected_arm_only",
                "luminosity_proxy_scope": "disc_presentation_assumption",
            }),
        )
        verification = timing.run(
            "internal_verification",
            lambda: verify_internal(
                db_path, core_db, arm_db, build_id=build_id, profile=profile
            ),
        )
        if verification["status"] != "pass":
            raise ValueError(f"clean runtime DISC verification failed: {verification['failing_checks']}")
        timing.run("canonical_parquet_export", lambda: export_scores(db_path, parquet_path))
        products = {
            "disc.duckdb": {
                "bytes": db_path.stat().st_size,
                "sha256": timing.run("database_hashing", lambda: file_sha256(db_path)),
                "determinism": "logical_tables",
            },
            "coolness_scores.parquet": {
                "bytes": parquet_path.stat().st_size,
                "sha256": timing.run("parquet_hashing", lambda: file_sha256(parquet_path)),
                "determinism": "byte_exact",
            },
        }
        manifest = {
            "schema_version": "spacegate.e7_clean_runtime_disc_manifest.v1",
            "build_id": build_id,
            "status": "pass",
            "generated_at": utc_now(),
            "policy_version": policy["policy_version"],
            "compiler_version": policy["compiler_version"],
            "policy_sha256": policy_sha,
            "compiler_sha256": compiler_sha,
            "scorer_sha256": scorer_sha,
            "inputs": input_identity,
            "coolness_profile": profile,
            "stability_databases_opened": [],
            "verification": verification,
            "products": products,
            "performance": timing.report(),
        }
        write_object_atomic(staging / "manifest.json", manifest)
        os.replace(staging, final_dir)
        if link_into_state:
            link_root = state / "derived/evidence_lake_v2/clean_runtime_disc"
            link_root.mkdir(parents=True, exist_ok=True)
            link = link_root / build_id
            if link.is_symlink() or link.exists():
                if link.resolve() != final_dir.resolve():
                    raise ValueError(f"clean runtime DISC state link collision: {link}")
            else:
                link.symlink_to(final_dir)
        return manifest
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--no-state-link", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    manifest = compile_runtime_disc(
        args.policy.resolve(), args.state_dir.resolve(), args.output_root.resolve(),
        link_into_state=not args.no_state_link,
    )
    if args.report:
        write_object_atomic(args.report.resolve(), manifest)
    print(json.dumps({
        "build_id": manifest["build_id"],
        "status": manifest["status"],
        "counts": manifest["verification"]["counts"],
        "wall_seconds": manifest["performance"]["wall_seconds"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
