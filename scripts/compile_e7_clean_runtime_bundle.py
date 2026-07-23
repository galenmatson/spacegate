#!/usr/bin/env python3
"""Compose pinned clean runtime databases for the public-slice compiler."""

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


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_clean_runtime_bundle.json"
DEFAULT_OUTPUT_ROOT = Path("/mnt/space/spacegate/e7-clean-runtime-bundle")
DEFAULT_STATE = Path("/data/spacegate/state")
PRODUCT_NAMES = {
    "core": {"core.duckdb", "canonical_hierarchy.duckdb"},
    "arm": {"arm.duckdb"},
    "disc": {"disc.duckdb"},
}
INPUT_DIRS = {
    "core": "e7-clean-runtime-core",
    "arm": "e7-clean-runtime-arm",
    "disc": "e7-clean-runtime-disc",
}
INPUT_FAMILIES = {
    "core": "clean_runtime_core",
    "arm": "clean_runtime_arm",
    "disc": "clean_runtime_disc",
}


class Timings:
    def __init__(self) -> None:
        self.started = time.monotonic()
        self.cpu_started = time.process_time()
        self.phases: list[dict[str, Any]] = []

    def run(self, name: str, function: Any) -> Any:
        started = time.monotonic()
        cpu_started = time.process_time()
        before = resource.getrusage(resource.RUSAGE_SELF)
        result = function()
        after = resource.getrusage(resource.RUSAGE_SELF)
        self.phases.append({
            "phase": name,
            "wall_seconds": round(time.monotonic() - started, 6),
            "cpu_seconds": round(time.process_time() - cpu_started, 6),
            "peak_rss_kib_after": int(after.ru_maxrss),
            "input_blocks_delta": int(after.ru_inblock - before.ru_inblock),
            "output_blocks_delta": int(after.ru_oublock - before.ru_oublock),
        })
        return result

    def report(self) -> dict[str, Any]:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return {
            "wall_seconds": round(time.monotonic() - self.started, 6),
            "cpu_seconds": round(time.process_time() - self.cpu_started, 6),
            "peak_rss_kib": int(usage.ru_maxrss),
            "phases": self.phases,
        }


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


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.e7_clean_runtime_bundle_policy.v1":
        raise ValueError("unsupported clean runtime bundle policy")
    if set(policy.get("inputs") or {}) != set(PRODUCT_NAMES):
        raise ValueError("clean runtime bundle inputs are incomplete")
    expected_rules = {
        "open_stability_databases": False,
        "mutate_component_artifacts": False,
        "bundle_is_served_directly": False,
        "public_slice_must_materialize_metadata": True,
        "all_links_are_manifest_pinned": True,
    }
    rules = policy.get("rules") or {}
    if any(rules.get(key) is not value for key, value in expected_rules.items()):
        raise ValueError("unsafe clean runtime bundle rules")
    for name, spec in policy["inputs"].items():
        build_id = str(spec.get("build_id") or "")
        relative = Path(str(spec.get("relative_path") or ""))
        if len(str(spec.get("manifest_sha256") or "")) != 64:
            raise ValueError(f"invalid manifest identity: {name}")
        if relative.parts != (
            "derived", "evidence_lake_v2", INPUT_FAMILIES[name],
            build_id,
        ):
            raise ValueError(f"invalid bounded input path: {name}")
        if set(spec.get("products") or []) != PRODUCT_NAMES[name]:
            raise ValueError(f"invalid product contract: {name}")


def resolve_inputs(policy: dict[str, Any], state: Path) -> dict[str, Any]:
    resolved: dict[str, Any] = {}
    for name, spec in sorted(policy["inputs"].items()):
        source_root = (state / spec["relative_path"]).resolve()
        manifest_path = source_root / "manifest.json"
        if not manifest_path.is_file() or file_sha256(manifest_path) != spec["manifest_sha256"]:
            raise ValueError(f"input manifest mismatch: {name}")
        manifest = load_object(manifest_path)
        if manifest.get("build_id") != spec["build_id"] or manifest.get("status") != "pass":
            raise ValueError(f"unaccepted input: {name}")
        products: dict[str, Any] = {}
        for product_name in sorted(PRODUCT_NAMES[name]):
            product_path = source_root / product_name
            product = (manifest.get("products") or {}).get(product_name)
            if not product_path.is_file() or not isinstance(product, dict):
                raise ValueError(f"missing registered product: {name}/{product_name}")
            if file_sha256(product_path) != product.get("sha256"):
                raise ValueError(f"product checksum mismatch: {name}/{product_name}")
            products[product_name] = {
                "path": product_path,
                "bytes": product_path.stat().st_size,
                "sha256": product["sha256"],
            }
        resolved[name] = {
            "build_id": spec["build_id"],
            "manifest_sha256": spec["manifest_sha256"],
            "products": products,
        }
    return resolved


def create_verified_links(staging: Path, resolved: dict[str, Any]) -> dict[str, Any]:
    products: dict[str, Any] = {}
    for source in resolved.values():
        for product_name, product in source["products"].items():
            link = staging / product_name
            relative_target = os.path.relpath(product["path"], start=staging)
            link.symlink_to(relative_target)
            if (
                not link.is_file()
                or link.resolve() != product["path"].resolve()
                or link.stat().st_size != product["bytes"]
            ):
                raise ValueError(f"bundle link verification failed: {product_name}")
            products[product_name] = {
                "bytes": product["bytes"],
                "sha256": product["sha256"],
                "link_target": relative_target,
            }
    return products


def compile_bundle(
    policy_path: Path,
    output_root: Path,
    state: Path,
    *,
    link_into_state: bool = True,
) -> dict[str, Any]:
    timing = Timings()
    policy = timing.run("load_policy", lambda: load_object(policy_path))
    timing.run("validate_policy", lambda: validate_policy(policy))
    resolved = timing.run("attest_input_products", lambda: resolve_inputs(policy, state))
    compiler_sha = file_sha256(Path(__file__).resolve())
    policy_sha = file_sha256(policy_path)
    identity = {
        name: {
            "build_id": value["build_id"],
            "manifest_sha256": value["manifest_sha256"],
        }
        for name, value in sorted(resolved.items())
    }
    build_id = stable_hash({
        "compiler_sha256": compiler_sha,
        "policy_sha256": policy_sha,
        "inputs": identity,
    })[:24]
    final_dir = output_root / build_id
    if (final_dir / "manifest.json").is_file():
        return load_object(final_dir / "manifest.json")
    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    try:
        products = timing.run(
            "create_and_verify_links", lambda: create_verified_links(staging, resolved)
        )
        manifest = {
            "schema_version": "spacegate.e7_clean_runtime_bundle_manifest.v1",
            "build_id": build_id,
            "status": "pass",
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "policy_version": policy["policy_version"],
            "compiler_version": policy["compiler_version"],
            "policy_sha256": policy_sha,
            "compiler_sha256": compiler_sha,
            "inputs": identity,
            "products": products,
            "stability_databases_opened": [],
            "served_directly": False,
            "performance": timing.report(),
        }
        write_object_atomic(staging / "manifest.json", manifest)
        os.replace(staging, final_dir)
        if link_into_state:
            link_root = state / "derived/evidence_lake_v2/clean_runtime_bundle"
            link_root.mkdir(parents=True, exist_ok=True)
            state_link = link_root / build_id
            if not state_link.exists() and not state_link.is_symlink():
                state_link.symlink_to(final_dir)
        return manifest
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--no-state-link", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    manifest = compile_bundle(
        args.policy.resolve(), args.output_root.resolve(), args.state_dir.resolve(),
        link_into_state=not args.no_state_link,
    )
    if args.report:
        write_object_atomic(args.report.resolve(), manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
