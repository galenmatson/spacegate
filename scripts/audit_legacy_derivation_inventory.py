#!/usr/bin/env python3
"""Audit the exhaustive E5 inventory of legacy derivations and assumptions."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INVENTORY = ROOT / "config/evidence_lake/e5_legacy_derivation_inventory.json"
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_selection_policies.json"
DEFAULT_BUILD = Path("/data/spacegate/state/served/current")
IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
VERSIONED_TOKEN = re.compile(r"^[a-z0-9_.:/-]+_v[0-9]+$")
DERIVATION_WORD = re.compile(
    r"(prior|proxy|derived|classification|projection|visual|environment|scale|"
    r"stefan|kepler|insolation|equilibrium|fallback)",
)
REQUIRED_PATH_FIELDS = {
    "path_id",
    "kind",
    "layer",
    "value_status",
    "outputs",
    "inputs",
    "algorithm_version",
    "applicability",
    "uncertainty",
    "confidence",
    "provenance",
    "supersession",
    "implementations",
    "materialized_markers",
}


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def python_symbols_and_strings(path: Path) -> tuple[set[str], set[str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    symbols: set[str] = set()
    strings: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            symbols.add(node.name)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    symbols.add(target.id)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            strings.add(node.value)
    return symbols, strings


def text_symbols_and_strings(path: Path) -> tuple[set[str], set[str]]:
    text = path.read_text(encoding="utf-8")
    symbols = set(re.findall(r"\b(?:function|const|let|var|class)\s+([A-Za-z_$][\w$]*)", text))
    strings = {
        match.group(2)
        for match in re.finditer(r"([\"'])([^\"'\n]{1,240})\1", text)
    }
    return symbols, strings


def source_inventory(repo_root: Path, inventory: dict[str, Any]) -> dict[str, Any]:
    file_cache: dict[str, tuple[set[str], set[str], str]] = {}
    missing_files: list[str] = []
    missing_symbols: list[str] = []
    missing_markers: list[str] = []
    marker_owners: dict[tuple[str, str], list[str]] = {}

    scan_paths: set[Path] = set()
    for pattern in inventory.get("source_scan_globs") or []:
        scan_paths.update(path for path in repo_root.glob(str(pattern)) if path.is_file())
    if not scan_paths:
        raise ValueError("source_scan_globs did not select any production files")
    for path in sorted(scan_paths):
        relative = path.relative_to(repo_root).as_posix()
        symbols, strings = (
            python_symbols_and_strings(path)
            if path.suffix == ".py"
            else text_symbols_and_strings(path)
        )
        file_cache[relative] = (symbols, strings, path.read_text(encoding="utf-8"))

    for item in inventory["paths"]:
        for implementation in item["implementations"]:
            relative = str(implementation["file"])
            path = (repo_root / relative).resolve()
            try:
                path.relative_to(repo_root.resolve())
            except ValueError as error:
                raise ValueError(f"implementation escapes repository: {relative}") from error
            if not path.is_file():
                missing_files.append(relative)
                continue
            if relative not in file_cache:
                symbols, strings = (
                    python_symbols_and_strings(path)
                    if path.suffix == ".py"
                    else text_symbols_and_strings(path)
                )
                file_cache[relative] = (symbols, strings, path.read_text(encoding="utf-8"))
            symbols, _, text = file_cache[relative]
            symbol = str(implementation["symbol"])
            if symbol not in symbols:
                missing_symbols.append(f"{relative}:{symbol}")
            for marker in implementation.get("markers") or []:
                marker = str(marker)
                marker_owners.setdefault((relative, marker), []).append(item["path_id"])
                if marker not in text:
                    missing_markers.append(f"{relative}:{marker}")

    discovered: list[dict[str, Any]] = []
    unaccounted: list[str] = []
    for relative, (_, strings, _) in sorted(file_cache.items()):
        for marker in sorted(strings):
            if VERSIONED_TOKEN.fullmatch(marker) and DERIVATION_WORD.search(marker):
                owners = sorted(marker_owners.get((relative, marker), []))
                discovered.append({"file": relative, "marker": marker, "path_ids": owners})
                if not owners:
                    unaccounted.append(f"{relative}:{marker}")
    return {
        "scanned_source_file_count": len(file_cache),
        "implementation_file_count": len(
            {
                str(implementation["file"])
                for item in inventory["paths"]
                for implementation in item["implementations"]
            }
        ),
        "implementation_binding_count": sum(
            len(item["implementations"]) for item in inventory["paths"]
        ),
        "discovered_versioned_markers": discovered,
        "missing_files": sorted(set(missing_files)),
        "missing_symbols": sorted(set(missing_symbols)),
        "missing_markers": sorted(set(missing_markers)),
        "unaccounted_versioned_markers": unaccounted,
    }


def parse_materialized_markers(inventory: dict[str, Any]) -> set[str]:
    markers: set[str] = set()
    for item in inventory["paths"]:
        for marker in item["materialized_markers"]:
            if not re.fullmatch(r"[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*=.+", marker):
                raise ValueError(f"invalid materialized marker: {marker}")
            markers.add(str(marker))
    return markers


def materialized_inventory(build_dir: Path | None, inventory: dict[str, Any]) -> dict[str, Any]:
    if build_dir is None:
        return {
            "build_id": None,
            "domains": [],
            "unaccounted_materialized_markers": [],
            "skipped": True,
        }
    resolved = build_dir.resolve(strict=True)
    registered = parse_materialized_markers(inventory)
    domains: list[dict[str, Any]] = []
    unaccounted: list[str] = []
    for domain in inventory["materialized_domains"]:
        database_name = str(domain["database"])
        table = str(domain["table"])
        marker_column = str(domain["marker_column"])
        status_column = str(domain["status_column"])
        if not all(IDENTIFIER.fullmatch(value) for value in (table, marker_column, status_column)):
            raise ValueError(f"unsafe materialized-domain identifier: {domain}")
        database = resolved / database_name
        if not database.is_file():
            if domain.get("optional"):
                domains.append({**domain, "status": "optional_database_missing", "rows": []})
                continue
            raise FileNotFoundError(database)
        con = duckdb.connect(str(database), read_only=True)
        try:
            table_exists = bool(
                con.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema='main' AND table_name=?",
                    [table],
                ).fetchone()[0]
            )
            if not table_exists:
                if domain.get("optional"):
                    domains.append({**domain, "status": "optional_table_missing", "rows": []})
                    continue
                raise ValueError(f"required materialized table missing: {database}:{table}")
            rows = [
                {"marker": str(row[0]), "status": str(row[1] or ""), "rows": int(row[2])}
                for row in con.execute(
                    f'SELECT CAST("{marker_column}" AS VARCHAR), '
                    f'CAST("{status_column}" AS VARCHAR), COUNT(*) '
                    f'FROM "{table}" GROUP BY 1,2 ORDER BY 1,2'
                ).fetchall()
            ]
        finally:
            con.close()
        for row in rows:
            marker = f"{table}.{marker_column}={row['marker']}"
            row["inventory_marker"] = marker
            row["accounted"] = marker in registered
            if not row["accounted"]:
                unaccounted.append(marker)
        domains.append({**domain, "status": "audited", "rows": rows})
    return {
        "build_id": resolved.name,
        "build_path": str(resolved),
        "domains": domains,
        "unaccounted_materialized_markers": sorted(set(unaccounted)),
        "skipped": False,
    }


def validate_inventory(inventory: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    if inventory.get("schema_version") != "spacegate.legacy_derivation_inventory.v1":
        errors.append("schema_version")
    paths = inventory.get("paths")
    if not isinstance(paths, list) or not paths:
        raise ValueError("inventory paths must be a nonempty array")
    path_ids = [str(item.get("path_id") or "") for item in paths]
    duplicate_ids = sorted({value for value in path_ids if path_ids.count(value) > 1})
    if duplicate_ids:
        errors.extend(f"duplicate_path:{value}" for value in duplicate_ids)
    for index, item in enumerate(paths):
        missing = REQUIRED_PATH_FIELDS - set(item)
        errors.extend(f"path_{index}_missing:{field}" for field in sorted(missing))
        if not item.get("outputs"):
            errors.append(f"path_{index}_empty_outputs")
        if not item.get("implementations"):
            errors.append(f"path_{index}_empty_implementations")
        supersession = item.get("supersession") or {}
        if not all(str(supersession.get(field) or "").strip() for field in ("state", "retirement_gate")):
            errors.append(f"path_{index}_invalid_supersession")

    policy_derivations = {
        str(item["derivation_key"]) for item in policy.get("derivations") or []
    }
    referenced: set[str] = set()
    invalid_references: list[str] = []
    for item in paths:
        raw = (item.get("supersession") or {}).get("e5_derivation_key")
        for value in str(raw or "").split("|"):
            if not value:
                continue
            referenced.add(value)
            if value not in policy_derivations:
                invalid_references.append(f"{item['path_id']}:{value}")
    return {
        "errors": errors,
        "path_count": len(paths),
        "path_ids": path_ids,
        "kinds": {
            kind: sum(1 for item in paths if item["kind"] == kind)
            for kind in sorted({item["kind"] for item in paths})
        },
        "layers": {
            layer: sum(1 for item in paths if item["layer"] == layer)
            for layer in sorted({item["layer"] for item in paths})
        },
        "supersession_states": {
            state: sum(1 for item in paths if item["supersession"]["state"] == state)
            for state in sorted({item["supersession"]["state"] for item in paths})
        },
        "policy_derivation_keys": sorted(policy_derivations),
        "referenced_policy_derivation_keys": sorted(referenced),
        "unreferenced_policy_derivation_keys": sorted(policy_derivations - referenced),
        "invalid_policy_derivation_references": sorted(invalid_references),
    }


def audit(
    *,
    repo_root: Path,
    inventory_path: Path,
    policy_path: Path,
    build_dir: Path | None,
) -> dict[str, Any]:
    repo_root = repo_root.resolve(strict=True)
    inventory = load_json(inventory_path)
    policy = load_json(policy_path)
    validation = validate_inventory(inventory, policy)
    sources = source_inventory(repo_root, inventory)
    materialized = materialized_inventory(build_dir, inventory)
    failing_checks = {
        "registry_validation_errors": len(validation["errors"]),
        "invalid_policy_derivation_references": len(validation["invalid_policy_derivation_references"]),
        "unreferenced_policy_derivation_keys": len(validation["unreferenced_policy_derivation_keys"]),
        "missing_implementation_files": len(sources["missing_files"]),
        "missing_implementation_symbols": len(sources["missing_symbols"]),
        "missing_implementation_markers": len(sources["missing_markers"]),
        "unaccounted_versioned_markers": len(sources["unaccounted_versioned_markers"]),
        "unaccounted_materialized_markers": len(materialized["unaccounted_materialized_markers"]),
    }
    failing_checks = {key: value for key, value in failing_checks.items() if value}
    return {
        "schema_version": "spacegate.legacy_derivation_inventory_audit.v1",
        "status": "fail" if failing_checks else "pass",
        "inventory_version": inventory["inventory_version"],
        "inventory_path": str(inventory_path.resolve()),
        "inventory_sha256": stable_hash(inventory),
        "selection_policy_version": policy["policy_version"],
        "selection_policy_path": str(policy_path.resolve()),
        "selection_policy_sha256": stable_hash(policy),
        "validation": validation,
        "source_audit": sources,
        "materialized_audit": materialized,
        "failing_checks": failing_checks,
        "checked_at": utc_now(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--inventory", type=Path, default=DEFAULT_INVENTORY)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--build-dir", type=Path, default=DEFAULT_BUILD)
    parser.add_argument("--source-only", action="store_true")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = audit(
        repo_root=args.repo_root,
        inventory_path=args.inventory,
        policy_path=args.policy,
        build_dir=None if args.source_only else args.build_dir,
    )
    atomic_json(args.report, report)
    print(
        f"legacy derivation inventory audit {report['status']}: "
        f"paths={report['validation']['path_count']} "
        f"failures={len(report['failing_checks'])}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
