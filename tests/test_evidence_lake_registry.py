from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from evidence_lake_registry import (  # noqa: E402
    collect_registry_audit,
    collect_storage_audit,
    stable_hash,
    validate_registry,
)


REGISTRY_PATH = ROOT / "config" / "evidence_lake" / "source_releases.json"


def load_registry() -> dict:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def minimal_registry() -> dict:
    return {
        "schema_version": "spacegate.evidence_source_registry.v1",
        "registry_version": "test.1",
        "ingestion_envelope": {
            "public_radius_ly": 1000,
            "buffer_radius_ly": 1250,
        },
        "storage_budgets": {
            "internal_min_free_before_acquisition_gib": 0,
            "internal_target_free_after_retention_gib": 0,
            "policy": "test",
        },
        "field_policy": {
            "allowed_dispositions": ["preserve", "normalize", "index_only", "omit"],
            "default_disposition": "preserve",
        },
        "authority_domains": {"inventory": "test inventory"},
        "sources": [
            {
                "source_id": "test.catalog",
                "release_id": "r1",
                "state": "active",
                "authority_roles": {"inventory": "test"},
                "publisher": "Test",
                "citation_url": "https://example.test/citation",
                "license": {"name": "Test", "url": "https://example.test/license"},
                "cadence": "pinned",
                "identity_namespaces": ["test_id"],
                "retrieval": {"kind": "csv", "implementation": "test"},
                "manifest_entries": [
                    {"manifest": "test_manifest.json", "source_name": "test_rows"}
                ],
                "storage_class": "internal_durable",
                "schema_policy": {
                    "kind": "delimited_header",
                    "drift": "fail_until_reviewed",
                    "default_disposition": "preserve",
                },
            }
        ],
    }


def test_checked_in_registry_is_valid_and_manifest_bindings_are_unique() -> None:
    registry = load_registry()
    assert validate_registry(registry) == []
    manifest_bindings = [
        (entry["manifest"], entry["source_name"])
        for source in registry["sources"]
        for entry in source["manifest_entries"]
    ]
    assert len(manifest_bindings) == len(set(manifest_bindings))
    assert registry["ingestion_envelope"]["buffer_radius_ly"] > 1000


def test_registry_audit_enumerates_fields_and_rejects_unregistered_entries(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    raw = state / "raw" / "test"
    manifests = state / "reports" / "manifests"
    raw.mkdir(parents=True)
    manifests.mkdir(parents=True)
    (raw / "rows.csv").write_text("source_id,value,quality\n1,2.5,A\n", encoding="utf-8")
    (manifests / "test_manifest.json").write_text(
        json.dumps(
            [
                {
                    "source_name": "test_rows",
                    "dest_path": "raw/test/rows.csv",
                    "sha256": "source-hash",
                }
            ]
        ),
        encoding="utf-8",
    )

    registry = minimal_registry()
    report = collect_registry_audit(registry, state)
    assert report["status"] == "pass"
    assert report["summary"]["machine_enumerated_fields"] == 3
    record = report["schema_snapshot"]["records"][0]
    assert record["fields"] == ["quality", "source_id", "value"]
    assert record["default_disposition"] == "preserve"

    payload = json.loads((manifests / "test_manifest.json").read_text(encoding="utf-8"))
    payload.append({"source_name": "silent_new_table", "dest_path": "raw/test/rows.csv"})
    (manifests / "test_manifest.json").write_text(json.dumps(payload), encoding="utf-8")
    drifted = collect_registry_audit(registry, state)
    assert drifted["status"] == "fail"
    assert drifted["unregistered_manifest_entries"] == [
        {"manifest": "test_manifest.json", "source_name": "silent_new_table"}
    ]


def test_storage_audit_finds_served_and_metadata_references(tmp_path: Path) -> None:
    state = tmp_path / "state"
    downloads = tmp_path / "downloads"
    bulk = tmp_path / "bulk"
    out = state / "out"
    served = state / "served"
    config = state / "config"
    for path in (out, served, config, downloads, bulk):
        path.mkdir(parents=True, exist_ok=True)

    active = "20260718T1200Z_active"
    rollback = "20260718T1100Z_rollback"
    (out / active).mkdir()
    (out / rollback).mkdir()
    (served / "current").symlink_to(Path("../out") / active)
    (downloads / "current.json").write_text(
        json.dumps({"rollback_build_id": rollback}), encoding="utf-8"
    )

    report = collect_storage_audit(minimal_registry(), state, downloads, bulk)
    assert report["build_references"][active] == ["served symlink current"]
    assert any("current.json" in reason for reason in report["build_references"][rollback])
    assert report["unrecognized_build_ids"] == []
    assert stable_hash({"a": 1}) == stable_hash({"a": 1})
