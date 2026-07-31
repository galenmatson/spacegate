#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REGISTRY_SCHEMA = "spacegate.smart_tag_registry.v1"
DEFINITIONS_SCHEMA = "spacegate.smart_tag_definitions.v1"
COMPILED_SCHEMA = "spacegate.smart_tag_registry_snapshot.v1"
KEY_RE = re.compile(r"^(science|presentation|evidence|source|rim):[a-z0-9_.-]+$")
KNOWN_EVALUATORS = {
    "stellar_class_v1",
    "system_count_v1",
    "system_numeric_v1",
    "system_range_v1",
    "hierarchy_nested_v2",
    "planet_numeric_v1",
    "planet_category_v1",
    "habitable_zone_screen_v1",
}
VALID_TARGET_TYPES = {"system", "star", "planet", "extended_object"}
VALID_ROLLUPS = {"direct", "member_to_system", "none"}


class TagRegistryError(ValueError):
    pass


@dataclass(frozen=True)
class LoadedRegistry:
    registry: dict[str, Any]
    definitions: tuple[dict[str, Any], ...]
    proposal_inventory: dict[str, Any] | None
    legacy_token_inventory: dict[str, Any] | None
    source_presentation: dict[str, Any] | None
    registry_hash: str
    source_files: tuple[Path, ...]

    def snapshot(self) -> dict[str, Any]:
        return {
            "schema_version": COMPILED_SCHEMA,
            "registry_id": self.registry["registry_id"],
            "registry_version": self.registry["registry_version"],
            "registry_hash": self.registry_hash,
            "surface_limits": self.registry["surface_limits"],
            "context_evaluator_contract": self.registry[
                "context_evaluator_contract"
            ],
            "definitions": list(self.definitions),
            "proposal_inventory": self.proposal_inventory,
            "legacy_token_inventory": self.legacy_token_inventory,
            "source_presentation": self.source_presentation,
        }


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TagRegistryError(f"cannot load tag registry file {path}: {exc}") from exc


def _required_text(definition: dict[str, Any], key: str) -> str:
    value = str(definition.get(key) or "").strip()
    if not value:
        raise TagRegistryError(
            f"tag {definition.get('key', '<unknown>')} requires nonempty {key}"
        )
    return value


def validate_definition(definition: dict[str, Any]) -> None:
    if not isinstance(definition, dict):
        raise TagRegistryError("every tag definition must be an object")
    tag_key = _required_text(definition, "key")
    if not KEY_RE.fullmatch(tag_key):
        raise TagRegistryError(f"invalid namespaced tag key: {tag_key}")
    for field in (
        "label",
        "name",
        "category",
        "kind",
        "layer",
        "visual_token",
        "tooltip",
        "short_tooltip",
        "source_policy",
    ):
        _required_text(definition, field)
    targets = definition.get("target_types")
    if (
        not isinstance(targets, list)
        or not targets
        or any(target not in VALID_TARGET_TYPES for target in targets)
    ):
        raise TagRegistryError(f"tag {tag_key} has invalid target_types")
    priority = definition.get("priority")
    if not isinstance(priority, dict) or set(priority) != {
        "compact",
        "normal",
        "expanded",
    }:
        raise TagRegistryError(f"tag {tag_key} requires all surface priorities")
    if any(not isinstance(value, int) for value in priority.values()):
        raise TagRegistryError(f"tag {tag_key} priorities must be integers")
    evaluator = definition.get("evaluator")
    if not isinstance(evaluator, dict):
        raise TagRegistryError(f"tag {tag_key} requires an evaluator")
    evaluator_id = str(evaluator.get("id") or "")
    if evaluator_id not in KNOWN_EVALUATORS:
        raise TagRegistryError(f"tag {tag_key} uses unknown evaluator {evaluator_id}")
    if not isinstance(evaluator.get("version"), int):
        raise TagRegistryError(f"tag {tag_key} evaluator version must be an integer")
    if not isinstance(evaluator.get("params"), dict):
        raise TagRegistryError(f"tag {tag_key} evaluator params must be an object")
    if definition.get("rollup") not in VALID_ROLLUPS:
        raise TagRegistryError(f"tag {tag_key} has invalid rollup")
    serialized = canonical_json(definition).decode("ascii")
    if re.search(r"\b(select|insert|update|delete|pragma|attach)\b", serialized, re.I):
        raise TagRegistryError(f"tag {tag_key} contains forbidden SQL-like content")


def load_registry(registry_path: Path) -> LoadedRegistry:
    registry_path = registry_path.resolve(strict=True)
    registry = load_json(registry_path)
    if not isinstance(registry, dict) or registry.get("schema_version") != REGISTRY_SCHEMA:
        raise TagRegistryError("unsupported smart-tag registry schema")
    if registry.get("rules", {}).get("allow_raw_sql") is not False:
        raise TagRegistryError("smart-tag registry must prohibit raw SQL")
    if registry.get("rules", {}).get("allow_executable_expressions") is not False:
        raise TagRegistryError("smart-tag registry must prohibit executable expressions")
    for field in ("registry_id", "registry_version"):
        if not str(registry.get(field) or "").strip():
            raise TagRegistryError(f"registry requires {field}")

    definitions: list[dict[str, Any]] = []
    files = [registry_path]
    for relative in registry.get("definition_files") or []:
        path = (registry_path.parent / str(relative)).resolve(strict=True)
        if registry_path.parent not in path.parents:
            raise TagRegistryError(f"definition path escapes registry root: {relative}")
        payload = load_json(path)
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != DEFINITIONS_SCHEMA
        ):
            raise TagRegistryError(f"unsupported definition schema: {path}")
        rows = payload.get("definitions")
        if not isinstance(rows, list):
            raise TagRegistryError(f"definitions must be a list: {path}")
        for definition in rows:
            validate_definition(definition)
            definitions.append(definition)
        files.append(path)
    if not definitions:
        raise TagRegistryError("registry contains no tag definitions")
    keys = [row["key"] for row in definitions]
    if len(keys) != len(set(keys)):
        duplicates = sorted({key for key in keys if keys.count(key) > 1})
        raise TagRegistryError(f"duplicate tag definitions: {duplicates}")

    proposal_path = registry_path.parent / "proposal_inventory.json"
    proposal_inventory = load_json(proposal_path) if proposal_path.is_file() else None
    if proposal_inventory is not None:
        allowed = {"enabled", "deferred", "retired", "rejected", "compatibility-only"}
        proposals = proposal_inventory.get("proposals")
        if not isinstance(proposals, list) or not proposals:
            raise TagRegistryError("proposal inventory requires proposal rows")
        for row in proposals:
            if (
                not isinstance(row, dict)
                or not str(row.get("proposal") or "").strip()
                or row.get("status") not in allowed
                or not str(row.get("reason") or "").strip()
            ):
                raise TagRegistryError("invalid proposal inventory row")
        files.append(proposal_path.resolve())
    legacy_token_inventory = None
    legacy_relative = registry.get("legacy_token_inventory")
    if legacy_relative:
        repo_root = registry_path.parents[2]
        legacy_path = (repo_root / str(legacy_relative)).resolve(strict=True)
        if repo_root not in legacy_path.parents:
            raise TagRegistryError("legacy token inventory path escapes repository root")
        legacy_token_inventory = load_json(legacy_path)
        if (
            not isinstance(legacy_token_inventory, dict)
            or legacy_token_inventory.get("schema_version")
            != "spacegate.smart_tag_legacy_token_inventory.v1"
        ):
            raise TagRegistryError("unsupported legacy token inventory schema")
        surfaces = legacy_token_inventory.get("surfaces")
        if not isinstance(surfaces, list) or not surfaces:
            raise TagRegistryError("legacy token inventory requires surface rows")
        for row in surfaces:
            if (
                not isinstance(row, dict)
                or not str(row.get("surface") or "").strip()
                or row.get("status")
                not in {"enabled", "deferred", "retired", "rejected", "compatibility-only"}
                or not str(row.get("reason") or "").strip()
            ):
                raise TagRegistryError("invalid legacy token inventory row")
        files.append(legacy_path)
    source_presentation = None
    source_presentation_relative = registry.get("source_presentation")
    if source_presentation_relative:
        repo_root = registry_path.parents[2]
        source_presentation_path = (
            repo_root / str(source_presentation_relative)
        ).resolve(strict=True)
        if repo_root not in source_presentation_path.parents:
            raise TagRegistryError("source presentation path escapes repository root")
        source_presentation = load_json(source_presentation_path)
        if (
            not isinstance(source_presentation, dict)
            or source_presentation.get("schema_version")
            != "spacegate.smart_tag_source_presentation.v2"
        ):
            raise TagRegistryError("unsupported source presentation schema")
        for row in source_presentation.get("sources") or []:
            if (
                not isinstance(row, dict)
                or not str(row.get("source_id") or "").strip()
                or not str(row.get("public_name") or "").strip()
                or not str(row.get("short_name") or "").strip()
                or len(str(row.get("short_name") or "")) > 20
            ):
                raise TagRegistryError("invalid source presentation row")
        files.append(source_presentation_path)
    normalized = {
        "registry": registry,
        "definitions": sorted(definitions, key=lambda row: row["key"]),
        "proposal_inventory": proposal_inventory,
        "legacy_token_inventory": legacy_token_inventory,
        "source_presentation": source_presentation,
    }
    return LoadedRegistry(
        registry=registry,
        definitions=tuple(normalized["definitions"]),
        proposal_inventory=proposal_inventory,
        legacy_token_inventory=legacy_token_inventory,
        source_presentation=source_presentation,
        registry_hash=sha256_bytes(canonical_json(normalized)),
        source_files=tuple(files),
    )


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Validate the Spacegate smart-tag registry.")
    parser.add_argument(
        "--registry",
        type=Path,
        default=Path("config/tags/registry.json"),
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    loaded = load_registry(args.registry)
    report = {
        "status": "pass",
        "registry_id": loaded.registry["registry_id"],
        "registry_version": loaded.registry["registry_version"],
        "registry_hash": loaded.registry_hash,
        "definition_count": len(loaded.definitions),
        "evaluator_ids": sorted(
            {row["evaluator"]["id"] for row in loaded.definitions}
        ),
        "source_files": [str(path) for path in loaded.source_files],
    }
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
