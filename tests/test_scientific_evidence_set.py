from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_scientific_evidence_set as release_set  # noqa: E402


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def make_artifact(
    state_dir: Path,
    build_id: str,
    sources: list[tuple[str, str]],
) -> None:
    build_dir = (
        state_dir / "derived/evidence_lake_v2/scientific_evidence" / build_id
    )
    build_dir.mkdir(parents=True)
    database = build_dir / "scientific_evidence.duckdb"
    database.write_bytes(build_id.encode("ascii"))
    database_sha = hashlib.sha256(database.read_bytes()).hexdigest()
    write_json(
        build_dir / "manifest.json",
        {
            "build_id": build_id,
            "database": database.name,
            "database_bytes": database.stat().st_size,
            "database_sha256": database_sha,
            "logical_content_sha256": "1" * 64,
            "report": {
                "status": "pass",
                "created_at": "2026-07-21T00:00:00Z",
                "compiler_version": "compiler-v1",
                "contract_version": "contract-v1",
                "scientific_content_sha256": "2" * 64,
                "sources": [
                    {
                        "source_id": source_id,
                        "release_id": release_id,
                        "source_records": 3,
                    }
                    for source_id, release_id in sources
                ],
                "tables": [
                    {
                        "table": "stellar_parameter_evidence",
                        "row_count": 6,
                        "logical_sha256": "3" * 64,
                        "logical_hash_algorithm": "test",
                    },
                    {
                        "table": "planet_parameter_evidence",
                        "row_count": 0,
                        "logical_sha256": "4" * 64,
                        "logical_hash_algorithm": "test",
                    },
                ],
            },
        },
    )


def fixture_contracts(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    policy = tmp_path / "policy.json"
    contract = tmp_path / "contract.json"
    scope = tmp_path / "scope.json"
    registry = tmp_path / "registry.json"
    write_json(
        policy,
        {
            "policy_version": "policy-v1",
            "members": {"source.a": "a" * 24, "source.b": "a" * 24},
        },
    )
    write_json(
        contract,
        {"source_adapters": {"source.a": {}, "source.b": {}}},
    )
    write_json(
        scope,
        {"explicit_dispositions": {"source.identity": {"owner": "E2"}}},
    )
    write_json(
        registry,
        {
            "sources": [
                {"source_id": "source.a", "release_id": "release-a"},
                {"source_id": "source.b", "release_id": "release-b"},
                {"source_id": "source.identity", "release_id": "release-i"},
            ]
        },
    )
    return policy, contract, scope, registry


def test_release_set_composes_shards_without_copying_databases(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    build_id = "a" * 24
    make_artifact(
        state_dir,
        build_id,
        [("source.a", "release-a"), ("source.b", "release-b")],
    )
    policy, contract, scope, registry = fixture_contracts(tmp_path)

    manifest = release_set.compile_release_set(
        state_dir=state_dir,
        policy_path=policy,
        contract_path=contract,
        scope_path=scope,
        registry_path=registry,
        verify_database_checksums=True,
    )

    assert manifest["status"] == "pass"
    assert manifest["adapter_source_count"] == 2
    assert manifest["boundary_source_count"] == 1
    assert manifest["artifact_count"] == 1
    assert manifest["source_record_count"] == 6
    assert len(manifest["table_shards"]["stellar_parameter_evidence"]) == 1
    assert "planet_parameter_evidence" not in manifest["table_shards"]
    release_root = state_dir / "derived/evidence_lake_v2/scientific_evidence_sets"
    assert (release_root / manifest["release_set_id"] / "manifest.json").is_file()
    assert (release_root / "current").resolve().name == manifest["release_set_id"]
    assert not (release_root / manifest["release_set_id"] / "scientific_evidence.duckdb").exists()

    repeated = release_set.compile_release_set(
        state_dir=state_dir,
        policy_path=policy,
        contract_path=contract,
        scope_path=scope,
        registry_path=registry,
    )
    assert repeated == manifest


def test_release_set_rejects_artifact_source_mismatch(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    make_artifact(state_dir, "a" * 24, [("source.a", "release-a")])
    policy, contract, scope, registry = fixture_contracts(tmp_path)
    with pytest.raises(ValueError, match="source mismatch"):
        release_set.compile_release_set(
            state_dir=state_dir,
            policy_path=policy,
            contract_path=contract,
            scope_path=scope,
            registry_path=registry,
        )


def test_checked_in_release_set_policy_accounts_every_e4_adapter() -> None:
    policy = release_set.load_json(release_set.DEFAULT_POLICY)
    contract = release_set.load_json(release_set.DEFAULT_CONTRACT)
    assert set(policy["members"]) == set(contract["source_adapters"])
    assert len(policy["members"]) == 39


def test_contained_child_requires_explicit_bulk_symlink_root(tmp_path: Path) -> None:
    root = tmp_path / "state-artifacts"
    bulk = tmp_path / "bulk-artifacts"
    root.mkdir()
    target = bulk / ("a" * 24)
    target.mkdir(parents=True)
    link = root / target.name
    link.symlink_to(target)

    with pytest.raises(ValueError, match="immediate child"):
        release_set.contained_child(root, link)
    assert release_set.contained_child(
        root,
        link,
        allowed_external_roots=(bulk,),
    ) == target.resolve()
