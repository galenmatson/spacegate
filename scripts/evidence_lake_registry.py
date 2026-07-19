#!/usr/bin/env python3
"""Validate and audit the Evidence Lake v2 source registry.

This module deliberately uses only the standard library for E0. Collectors and
typed cooks may depend on Arrow/Astropy later, but registry and storage safety
must remain available in a minimally provisioned operator environment.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY = ROOT / "config" / "evidence_lake" / "source_releases.json"
DEFAULT_BASELINE = ROOT / "config" / "evidence_lake" / "schema_baseline.json"
ALLOWED_STATES = {
    "active",
    "active_expansion_pending",
    "active_replacement_pending",
    "transitional",
    "planned",
    "disabled",
}
ALLOWED_DISPOSITIONS = {"preserve", "normalize", "index_only", "omit"}
BUILD_ID_RE = re.compile(
    r"^(?:\d{4}-\d{2}-\d{2}|\d{8})T(?:\d{4,6}Z?)?_[A-Za-z0-9._-]+$"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def stable_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_registry(registry: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if registry.get("schema_version") != "spacegate.evidence_source_registry.v1":
        errors.append("unsupported or missing schema_version")
    if not registry.get("registry_version"):
        errors.append("missing registry_version")

    field_policy = registry.get("field_policy") or {}
    configured = set(field_policy.get("allowed_dispositions") or [])
    if configured != ALLOWED_DISPOSITIONS:
        errors.append(
            "field_policy.allowed_dispositions must be exactly "
            + ", ".join(sorted(ALLOWED_DISPOSITIONS))
        )
    if field_policy.get("default_disposition") not in ALLOWED_DISPOSITIONS:
        errors.append("field_policy.default_disposition is invalid")

    envelope = registry.get("ingestion_envelope") or {}
    public_radius = envelope.get("public_radius_ly")
    buffer_radius = envelope.get("buffer_radius_ly")
    if not isinstance(public_radius, (int, float)) or public_radius <= 0:
        errors.append("ingestion_envelope.public_radius_ly must be positive")
    if not isinstance(buffer_radius, (int, float)) or buffer_radius <= float(public_radius or 0):
        errors.append("ingestion_envelope.buffer_radius_ly must exceed public_radius_ly")

    budgets = registry.get("storage_budgets") or {}
    for key, value in budgets.items():
        if key == "policy":
            continue
        if not isinstance(value, (int, float)) or value < 0:
            errors.append(f"storage_budgets.{key} must be nonnegative")

    seen_sources: set[str] = set()
    seen_manifest_entries: set[tuple[str, str]] = set()
    sources = registry.get("sources")
    if not isinstance(sources, list) or not sources:
        return errors + ["sources must be a non-empty list"]

    for index, source in enumerate(sources):
        prefix = f"sources[{index}]"
        source_id = str(source.get("source_id") or "").strip()
        if not source_id:
            errors.append(f"{prefix}.source_id is required")
        elif source_id in seen_sources:
            errors.append(f"duplicate source_id: {source_id}")
        seen_sources.add(source_id)

        for key in (
            "release_id",
            "state",
            "authority_roles",
            "publisher",
            "citation_url",
            "license",
            "cadence",
            "identity_namespaces",
            "retrieval",
            "storage_class",
            "schema_policy",
        ):
            if source.get(key) in (None, "", [], {}):
                errors.append(f"{source_id or prefix}.{key} is required")

        state = source.get("state")
        if state not in ALLOWED_STATES:
            errors.append(f"{source_id or prefix}.state is invalid: {state!r}")

        roles = source.get("authority_roles") or {}
        known_domains = set((registry.get("authority_domains") or {}).keys())
        unknown_roles = sorted(set(roles) - known_domains)
        if unknown_roles:
            errors.append(f"{source_id}.authority_roles has unknown domains: {unknown_roles}")

        license_data = source.get("license") or {}
        if not license_data.get("name") or not license_data.get("url"):
            errors.append(f"{source_id}.license requires name and url")

        schema_policy = source.get("schema_policy") or {}
        disposition = schema_policy.get("default_disposition")
        if disposition not in ALLOWED_DISPOSITIONS:
            errors.append(f"{source_id}.schema_policy.default_disposition is invalid")
        if disposition == "omit" and not schema_policy.get("reason"):
            errors.append(f"{source_id}.schema_policy omit requires reason")
        if schema_policy.get("drift") != "fail_until_reviewed":
            errors.append(f"{source_id}.schema_policy.drift must be fail_until_reviewed")

        trailing_delimiters = schema_policy.get("trailing_layout_delimiters")
        if trailing_delimiters is not None:
            if schema_policy.get("kind") not in {
                "documented_fixed_width",
                "vizier_readme_fixed_width",
            }:
                errors.append(
                    f"{source_id}.schema_policy.trailing_layout_delimiters "
                    "requires a documented fixed-width schema"
                )
            if (
                not isinstance(trailing_delimiters, list)
                or not trailing_delimiters
                or any(
                    not isinstance(value, str) or len(value) != 1 or value.isspace()
                    for value in trailing_delimiters
                )
                or len(trailing_delimiters) != len(set(trailing_delimiters))
            ):
                errors.append(
                    f"{source_id}.schema_policy.trailing_layout_delimiters must be "
                    "a non-empty list of unique single non-whitespace characters"
                )

        entries = source.get("manifest_entries") or []
        if state not in {"planned", "disabled"} and not entries:
            errors.append(f"{source_id}.manifest_entries is required for active sources")
        for entry in entries:
            key = (str(entry.get("manifest") or ""), str(entry.get("source_name") or ""))
            if not all(key):
                errors.append(f"{source_id} has incomplete manifest entry")
            elif key in seen_manifest_entries:
                errors.append(f"manifest entry registered more than once: {key}")
            seen_manifest_entries.add(key)

        layout = schema_policy.get("artifact_layout") or {}
        if layout:
            if layout.get("kind") != "delimited_continuation":
                errors.append(f"{source_id}.schema_policy.artifact_layout.kind is unsupported")
            layout_names = {
                str(layout.get("header_artifact") or ""),
                *(str(value) for value in layout.get("continuation_artifacts") or []),
            }
            entry_names = {str(entry.get("source_name") or "") for entry in entries}
            if "" in layout_names or layout_names != entry_names:
                errors.append(
                    f"{source_id}.schema_policy.artifact_layout must account for every artifact"
                )
            if not layout.get("table_name"):
                errors.append(f"{source_id}.schema_policy.artifact_layout.table_name is required")
        entry_names = {str(entry.get("source_name") or "") for entry in entries}
        member_lineage_fields = schema_policy.get("member_lineage_fields")
        if member_lineage_fields is not None:
            if not isinstance(member_lineage_fields, dict) or not member_lineage_fields:
                errors.append(
                    f"{source_id}.schema_policy.member_lineage_fields must be a non-empty object"
                )
            else:
                unknown_artifacts = sorted(set(member_lineage_fields) - entry_names)
                if unknown_artifacts:
                    errors.append(
                        f"{source_id}.schema_policy.member_lineage_fields references "
                        f"unregistered artifacts: {unknown_artifacts}"
                    )
                lineage_names = [
                    str(value).strip() for value in member_lineage_fields.values()
                ]
                if any(not value for value in lineage_names) or len(lineage_names) != len(
                    set(lineage_names)
                ):
                    errors.append(
                        f"{source_id}.schema_policy.member_lineage_fields values must "
                        "be unique non-empty field names"
                    )
        html_table = schema_policy.get("html_table") or {}
        if html_table:
            html_source_name = str(html_table.get("source_name") or "")
            if schema_policy.get("kind") != "html_snapshot":
                errors.append(
                    f"{source_id}.schema_policy.html_table requires html_snapshot kind"
                )
            if not html_table.get("table_id"):
                errors.append(f"{source_id}.schema_policy.html_table.table_id is required")
            if html_source_name not in entry_names:
                errors.append(
                    f"{source_id}.schema_policy.html_table source_name is not registered"
                )
            html_fields = html_table.get("fields") or []
            if not html_fields:
                errors.append(f"{source_id}.schema_policy.html_table.fields is required")
            names = [str(field.get("name") or "") for field in html_fields]
            headers = [str(field.get("source_header") or "") for field in html_fields]
            if "" in names or len(names) != len(set(names)):
                errors.append(
                    f"{source_id}.schema_policy.html_table field names must be unique"
                )
            if "" in headers or len(headers) != len(set(headers)):
                errors.append(
                    f"{source_id}.schema_policy.html_table source headers must be unique"
                )
            for field in html_fields:
                if field.get("disposition") not in ALLOWED_DISPOSITIONS:
                    errors.append(
                        f"{source_id}.schema_policy.html_table field disposition is invalid"
                    )
        format_artifact = str(schema_policy.get("format_artifact") or "")
        if format_artifact and format_artifact not in entry_names:
            errors.append(f"{source_id}.schema_policy.format_artifact is not registered")
        for data_name, readme_name in (schema_policy.get("readme_bindings") or {}).items():
            if data_name not in entry_names or readme_name not in entry_names:
                errors.append(
                    f"{source_id}.schema_policy.readme_bindings references unregistered artifacts"
                )

    return errors


def iter_manifest_entries(manifest_dir: Path) -> Iterable[tuple[str, dict[str, Any]]]:
    if not manifest_dir.exists():
        return
    for path in sorted(manifest_dir.glob("*_manifest.json")):
        try:
            payload = load_json(path)
        except Exception as exc:
            yield path.name, {"_load_error": str(exc)}
            continue
        if not isinstance(payload, list):
            yield path.name, {"_load_error": "manifest root is not a list"}
            continue
        for entry in payload:
            if isinstance(entry, dict):
                yield path.name, entry


def resolve_artifact_path(state_dir: Path, dest_path: str) -> Path:
    path = Path(dest_path)
    return path if path.is_absolute() else state_dir / path


def delimited_fields(path: Path) -> list[str]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8-sig", errors="replace", newline="") as handle:
        header = handle.readline()
    try:
        dialect = csv.Sniffer().sniff(header, delimiters=",;\t|")
        delimiter = dialect.delimiter
    except csv.Error:
        delimiter = ","
    row = next(csv.reader([header], delimiter=delimiter), [])
    return [str(value).strip() for value in row if str(value).strip()]


def json_record_fields(path: Path) -> list[str]:
    payload = load_json(path)
    declared = payload.get("fields") if isinstance(payload, dict) else None
    if isinstance(declared, list):
        names = [
            str(item.get("name") or "").strip()
            for item in declared
            if isinstance(item, dict) and item.get("name")
        ]
        if names:
            return names
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return sorted(str(key) for key in data[0])
    return []


def discover_schema_fields(path: Path, schema_kind: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "artifact": str(path),
        "artifact_kind": "directory" if path.is_dir() else "file",
        "schema_kind": schema_kind,
        "fields": [],
        "field_accounting": "raw_preserved_documented_schema",
    }
    if not path.exists():
        result["field_accounting"] = "missing_artifact"
        return result

    delimited_kinds = {"delimited_header", "mixed_tabular_snapshot"}
    candidates: list[Path] = []
    if schema_kind in delimited_kinds:
        if path.is_file() and (path.suffix in {".csv", ".gz"} or path.name.endswith(".csv.gz")):
            candidates = [path]
        elif path.is_dir():
            candidates = sorted(
                child
                for child in path.rglob("*")
                if child.is_file()
                and (
                    child.suffix in {".csv", ".json"}
                    or child.name.endswith(".csv.gz")
                )
            )

    schemas: dict[str, list[str]] = {}
    errors: list[str] = []
    for candidate in candidates:
        try:
            fields = (
                json_record_fields(candidate)
                if candidate.suffix == ".json"
                else delimited_fields(candidate)
            )
            schemas[str(candidate.relative_to(path) if path.is_dir() else candidate.name)] = fields
        except Exception as exc:
            errors.append(f"{candidate}: {exc}")

    if schemas:
        unique_fields = sorted({field for fields in schemas.values() for field in fields})
        result["fields"] = unique_fields
        result["field_count"] = len(unique_fields)
        result["member_schemas"] = schemas
        result["field_accounting"] = "machine_enumerated"
    elif schema_kind == "fits_columns":
        result["field_accounting"] = "raw_preserved_fits_schema_pending_e1"
    elif schema_kind == "pending_source_schema":
        result["field_accounting"] = "planned_not_acquired"
    elif schema_kind in {"votable_binary_response_set", "tap_response_set"}:
        reports = [path] if path.is_file() and path.name == "product_manifest.json" else []
        if path.is_dir():
            reports = sorted(path.rglob("product_manifest.json"))
        fields = []
        for report_path in reports:
            payload = load_json(report_path)
            fields.extend(
                str(item["column_name"])
                for item in payload.get("field_dispositions") or []
                if item.get("disposition") == "preserve" and item.get("column_name")
            )
        if fields:
            result["fields"] = sorted(set(fields))
            result["field_count"] = len(result["fields"])
            result["field_accounting"] = "machine_enumerated_product_manifest"
        else:
            result["field_accounting"] = "missing_votable_product_manifest"
    elif schema_kind in delimited_kinds:
        result["field_accounting"] = "no_delimited_schema_found"
    if errors:
        result["errors"] = errors
    return result


def apply_artifact_layout_schema(
    source: dict[str, Any],
    records: list[dict[str, Any]],
) -> None:
    """Apply release-level layout rules that cannot be inferred per file."""
    layout = (source.get("schema_policy") or {}).get("artifact_layout") or {}
    if layout.get("kind") != "delimited_continuation":
        return
    header_source = str(layout.get("header_artifact") or "")
    header_record = next(
        (record for record in records if record.get("source_name") == header_source),
        None,
    )
    if not header_record or header_record.get("field_accounting") != "machine_enumerated":
        return
    fields = list(header_record.get("fields") or [])
    for source_name in layout.get("continuation_artifacts") or []:
        record = next(
            (item for item in records if item.get("source_name") == source_name),
            None,
        )
        if not record:
            continue
        record["fields"] = fields
        record["field_count"] = len(fields)
        record["member_schemas"] = {
            "inherited_from": header_source,
            "fields": fields,
        }
        record["field_accounting"] = "machine_enumerated_continuation"
        record.pop("format_contract_sha256", None)
        record["schema_sha256"] = stable_hash(
            {
                "schema_kind": record["schema_kind"],
                "fields": fields,
                "member_schemas": record["member_schemas"],
                "field_accounting": record["field_accounting"],
            }
        )


def apply_declared_html_table_schema(
    source: dict[str, Any],
    records: list[dict[str, Any]],
) -> None:
    contract = (source.get("schema_policy") or {}).get("html_table") or {}
    if not contract:
        return
    source_name = str(contract["source_name"])
    record = next(
        (item for item in records if item.get("source_name") == source_name),
        None,
    )
    if not record:
        return
    fields = [str(field["name"]) for field in contract["fields"]]
    record["fields"] = fields
    record["field_count"] = len(fields)
    record["member_schemas"] = {
        "table_id": str(contract["table_id"]),
        "fields": list(contract["fields"]),
    }
    record["field_accounting"] = "machine_declared_html_table"
    record.pop("format_contract_sha256", None)
    record["schema_sha256"] = stable_hash(
        {
            "schema_kind": record["schema_kind"],
            "fields": fields,
            "member_schemas": record["member_schemas"],
            "field_accounting": record["field_accounting"],
        }
    )


def collect_registry_audit(registry: dict[str, Any], state_dir: Path) -> dict[str, Any]:
    manifest_dir = state_dir / "reports" / "manifests"
    registered: dict[tuple[str, str], dict[str, Any]] = {}
    for source in registry["sources"]:
        for entry in source.get("manifest_entries") or []:
            registered[(entry["manifest"], entry["source_name"])] = source

    actual_entries: dict[tuple[str, str], dict[str, Any]] = {}
    manifest_load_errors: list[dict[str, str]] = []
    for manifest_name, entry in iter_manifest_entries(manifest_dir):
        if entry.get("_load_error"):
            manifest_load_errors.append({"manifest": manifest_name, "error": entry["_load_error"]})
            continue
        actual_entries[(manifest_name, str(entry.get("source_name") or ""))] = entry

    unregistered = sorted(
        (
            {"manifest": key[0], "source_name": key[1]}
            for key in set(actual_entries) - set(registered)
        ),
        key=lambda row: (row["manifest"], row["source_name"]),
    )
    missing_registered: list[dict[str, str]] = []
    source_reports: list[dict[str, Any]] = []
    all_schema_records: list[dict[str, Any]] = []

    for source in registry["sources"]:
        source_report: dict[str, Any] = {
            "source_id": source["source_id"],
            "release_id": source["release_id"],
            "state": source["state"],
            "schema_kind": source["schema_policy"]["kind"],
            "default_field_disposition": source["schema_policy"]["default_disposition"],
            "manifest_entries": [],
        }
        source_fields: set[str] = set()
        accounting_states: set[str] = set()
        source_schema_records: list[dict[str, Any]] = []
        for expected in source.get("manifest_entries") or []:
            key = (expected["manifest"], expected["source_name"])
            actual = actual_entries.get(key)
            if actual is None:
                missing_registered.append(
                    {"source_id": source["source_id"], "manifest": key[0], "source_name": key[1]}
                )
                source_report["manifest_entries"].append(
                    {"manifest": key[0], "source_name": key[1], "status": "missing"}
                )
                continue
            artifact = resolve_artifact_path(state_dir, str(actual.get("dest_path") or ""))
            schema_record = discover_schema_fields(artifact, source["schema_policy"]["kind"])
            try:
                artifact_locator = f"state://{artifact.relative_to(state_dir).as_posix()}"
            except ValueError:
                artifact_locator = f"external://{artifact.name}"
            schema_record.update(
                {
                    "artifact": artifact_locator,
                    "source_id": source["source_id"],
                    "release_id": source["release_id"],
                    "manifest": key[0],
                    "source_name": key[1],
                    "default_disposition": source["schema_policy"]["default_disposition"],
                }
            )
            if schema_record["field_accounting"] != "machine_enumerated":
                # Until E1 has a machine-readable parser for a documented or
                # opaque format, pin the exact source artifact. This is
                # intentionally conservative: any refresh requires review.
                schema_record["format_contract_sha256"] = actual.get("sha256")
            schema_record["schema_sha256"] = stable_hash(
                {
                    "schema_kind": schema_record["schema_kind"],
                    "fields": schema_record.get("fields") or [],
                    "member_schemas": schema_record.get("member_schemas") or {},
                    "field_accounting": schema_record["field_accounting"],
                }
            )
            all_schema_records.append(schema_record)
            source_schema_records.append(schema_record)
            source_fields.update(schema_record.get("fields") or [])
            accounting_states.add(schema_record["field_accounting"])
            source_report["manifest_entries"].append(
                {
                    "manifest": key[0],
                    "source_name": key[1],
                    "status": "present" if artifact.exists() else "artifact_missing",
                    "artifact": str(artifact),
                    "manifest_sha256": actual.get("sha256"),
                    "schema_sha256": schema_record["schema_sha256"],
                    "field_count": len(schema_record.get("fields") or []),
                    "field_accounting": schema_record["field_accounting"],
                }
            )
        apply_artifact_layout_schema(source, source_schema_records)
        apply_declared_html_table_schema(source, source_schema_records)
        source_fields = {
            field for record in source_schema_records for field in record.get("fields") or []
        }
        accounting_states = {
            str(record["field_accounting"]) for record in source_schema_records
        }
        for report_entry in source_report["manifest_entries"]:
            record = next(
                (
                    item
                    for item in source_schema_records
                    if item.get("source_name") == report_entry.get("source_name")
                ),
                None,
            )
            if record:
                report_entry["schema_sha256"] = record["schema_sha256"]
                report_entry["field_count"] = len(record.get("fields") or [])
                report_entry["field_accounting"] = record["field_accounting"]
        source_report["field_count"] = len(source_fields)
        source_report["field_disposition_counts"] = {
            source["schema_policy"]["default_disposition"]: len(source_fields)
        }
        source_report["field_accounting_states"] = sorted(accounting_states)
        source_reports.append(source_report)

    schema_snapshot = {
        "schema_version": "spacegate.evidence_schema_snapshot.v1",
        "registry_version": registry["registry_version"],
        "records": sorted(
            all_schema_records,
            key=lambda row: (row["source_id"], row["manifest"], row["source_name"]),
        ),
    }
    schema_snapshot["snapshot_sha256"] = stable_hash(schema_snapshot)

    active_missing = [
        row
        for row in missing_registered
        if next(source for source in registry["sources"] if source["source_id"] == row["source_id"])["state"]
        not in {"planned", "disabled"}
    ]
    errors: list[str] = []
    if manifest_load_errors:
        errors.append("one or more source manifests could not be loaded")
    if unregistered:
        errors.append("one or more manifest entries are not registered")
    if active_missing:
        errors.append("one or more active registry entries are missing from manifests")
    for record in all_schema_records:
        if record["field_accounting"] in {"missing_artifact", "no_delimited_schema_found"}:
            errors.append(
                f"schema accounting failed for {record['source_id']}:{record['source_name']}"
            )

    return {
        "schema_version": "spacegate.evidence_registry_audit.v1",
        "generated_at": utc_now(),
        "registry_version": registry["registry_version"],
        "registry_sha256": stable_hash(registry),
        "state_dir": str(state_dir),
        "status": "pass" if not errors else "fail",
        "errors": sorted(set(errors)),
        "manifest_load_errors": manifest_load_errors,
        "unregistered_manifest_entries": unregistered,
        "missing_registered_entries": missing_registered,
        "sources": source_reports,
        "schema_snapshot": schema_snapshot,
        "summary": {
            "registered_sources": len(registry["sources"]),
            "registered_manifest_entries": len(registered),
            "actual_manifest_entries": len(actual_entries),
            "planned_sources": sum(1 for source in registry["sources"] if source["state"] == "planned"),
            "machine_enumerated_fields": sum(
                len(record.get("fields") or []) for record in all_schema_records
            ),
            "unregistered_manifest_entries": len(unregistered),
            "missing_active_entries": len(active_missing),
        },
    }


def path_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    result = subprocess.run(
        ["du", "-sb", str(path)],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0 and result.stdout.strip():
        try:
            return int(result.stdout.split()[0])
        except (IndexError, ValueError):
            pass
    return 0


def build_references(state_dir: Path, download_dir: Path) -> dict[str, list[str]]:
    references: dict[str, set[str]] = {}
    out_dir = state_dir / "out"
    known_build_ids = sorted(
        (path.name for path in out_dir.iterdir() if path.is_dir()),
        key=len,
        reverse=True,
    ) if out_dir.exists() else []

    def add(build_id: str, reason: str) -> None:
        references.setdefault(build_id, set()).add(reason)

    served_dir = state_dir / "served"
    if served_dir.exists():
        for path in served_dir.iterdir():
            if not path.is_symlink():
                continue
            try:
                target = path.resolve(strict=True)
            except OSError:
                continue
            add(target.name, f"served symlink {path.name}")

    json_roots = [state_dir / "config", download_dir]
    for root in json_roots:
        if not root.exists():
            continue
        for path in root.rglob("*.json"):
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            for build_id in known_build_ids:
                if build_id in text:
                    add(build_id, f"referenced by {path}")

    return {key: sorted(value) for key, value in sorted(references.items())}


def collect_storage_audit(registry: dict[str, Any], state_dir: Path, download_dir: Path, bulk_dir: Path) -> dict[str, Any]:
    data_root = state_dir.parent
    disk = shutil.disk_usage(data_root)
    bulk_disk = shutil.disk_usage(bulk_dir) if bulk_dir.exists() else None
    categories = {
        "raw": state_dir / "raw",
        "typed_legacy_cooked": state_dir / "cooked",
        "immutable_builds": state_dir / "out",
        "reports": state_dir / "reports",
        "scratch": state_dir / "tmp",
        "runtime_cache": state_dir / "cache",
        "published_downloads": download_dir,
        "bulk_storage": bulk_dir,
    }
    category_bytes = {name: path_bytes(path) for name, path in categories.items()}
    references = build_references(state_dir, download_dir)

    out_dir = state_dir / "out"
    recognized: list[str] = []
    unrecognized: list[str] = []
    for path in sorted(out_dir.iterdir()) if out_dir.exists() else []:
        if not path.is_dir():
            continue
        if BUILD_ID_RE.match(path.name):
            recognized.append(path.name)
        else:
            unrecognized.append(path.name)

    free_gib = disk.free / (1024**3)
    budgets = registry["storage_budgets"]
    minimum = float(budgets["internal_min_free_before_acquisition_gib"])
    target = float(budgets["internal_target_free_after_retention_gib"])
    alerts: list[str] = []
    if free_gib < minimum:
        alerts.append("internal free space is below the acquisition floor")
    if free_gib < target:
        alerts.append("internal free space is below the post-retention target")
    if unrecognized:
        alerts.append("legacy/unrecognized build IDs require explicit retention accounting")

    return {
        "schema_version": "spacegate.evidence_storage_audit.v1",
        "generated_at": utc_now(),
        "registry_version": registry["registry_version"],
        "state_dir": str(state_dir),
        "download_dir": str(download_dir),
        "bulk_dir": str(bulk_dir),
        "internal_disk": {"total_bytes": disk.total, "used_bytes": disk.used, "free_bytes": disk.free},
        "bulk_disk": (
            {"total_bytes": bulk_disk.total, "used_bytes": bulk_disk.used, "free_bytes": bulk_disk.free}
            if bulk_disk
            else None
        ),
        "category_bytes": category_bytes,
        "storage_budgets_gib": budgets,
        "build_references": references,
        "recognized_build_ids": recognized,
        "unrecognized_build_ids": unrecognized,
        "alerts": alerts,
        "acquisition_ready": free_gib >= minimum and not unrecognized,
    }


def compare_baseline(snapshot: dict[str, Any], baseline_path: Path) -> list[str]:
    if not baseline_path.exists():
        return [f"schema baseline missing: {baseline_path}"]
    baseline = load_json(baseline_path)
    expected = baseline.get("snapshot_sha256")
    actual = snapshot.get("snapshot_sha256")
    if expected != actual:
        return [f"schema drift: baseline {expected} != current {actual}"]
    return []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("validate", help="validate the checked-in registry")

    audit = sub.add_parser("audit", help="audit manifests, artifacts, and field accounting")
    audit.add_argument("--state-dir", type=Path, required=True)
    audit.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)
    audit.add_argument("--report", type=Path, required=True)
    audit.add_argument("--allow-missing-baseline", action="store_true")

    snapshot = sub.add_parser("schema-snapshot", help="write a deterministic schema baseline")
    snapshot.add_argument("--state-dir", type=Path, required=True)
    snapshot.add_argument("--output", type=Path, required=True)

    storage = sub.add_parser("storage", help="audit storage budgets and protected build references")
    storage.add_argument("--state-dir", type=Path, required=True)
    storage.add_argument("--download-dir", type=Path, required=True)
    storage.add_argument("--bulk-dir", type=Path, required=True)
    storage.add_argument("--report", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    registry = load_json(args.registry)
    errors = validate_registry(registry)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    if args.command == "validate":
        print(
            f"Evidence registry OK: {len(registry['sources'])} sources, "
            f"version {registry['registry_version']}"
        )
        return 0

    if args.command == "audit":
        report = collect_registry_audit(registry, args.state_dir)
        if not args.allow_missing_baseline:
            report["errors"].extend(compare_baseline(report["schema_snapshot"], args.baseline))
            report["errors"] = sorted(set(report["errors"]))
            report["status"] = "pass" if not report["errors"] else "fail"
        write_json(args.report, report)
        print(
            f"Evidence registry audit {report['status']}: "
            f"{report['summary']['registered_sources']} sources, "
            f"{report['summary']['machine_enumerated_fields']} fields -> {args.report}"
        )
        for error in report["errors"]:
            print(f"ERROR: {error}")
        return 0 if report["status"] == "pass" else 1

    if args.command == "schema-snapshot":
        report = collect_registry_audit(registry, args.state_dir)
        if report["status"] != "pass":
            for error in report["errors"]:
                print(f"ERROR: {error}")
            return 1
        write_json(args.output, report["schema_snapshot"])
        print(f"Schema snapshot {report['schema_snapshot']['snapshot_sha256']} -> {args.output}")
        return 0

    if args.command == "storage":
        report = collect_storage_audit(registry, args.state_dir, args.download_dir, args.bulk_dir)
        write_json(args.report, report)
        print(
            f"Storage audit: {report['internal_disk']['free_bytes'] / (1024**3):.1f} GiB free, "
            f"acquisition_ready={report['acquisition_ready']} -> {args.report}"
        )
        for alert in report["alerts"]:
            print(f"ALERT: {alert}")
        return 0

    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
