from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_smart_tags as compiler  # noqa: E402
import smart_tag_registry as registry_module  # noqa: E402
import verify_smart_tags as verifier  # noqa: E402

sys.path.insert(0, str(ROOT / "srv/api"))
from app import smart_tags as api_smart_tags  # noqa: E402


def make_public_read(path: Path) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
        INSERT INTO metadata VALUES ('build_id','tag-test-build');
        CREATE TABLE systems(
          system_id INTEGER PRIMARY KEY,stable_object_key TEXT,star_count INTEGER,
          planet_count INTEGER,dist_ly REAL,has_gaia_nss_evidence INTEGER,
          has_msc_evidence INTEGER,has_sbx_evidence INTEGER,
          has_wds_evidence INTEGER,has_orb6_evidence INTEGER
        );
        CREATE TABLE stars(
          stable_object_key TEXT,system_id INTEGER,selected_classification TEXT,
          classification_status TEXT,classification_fact_id TEXT,
          classification_confidence REAL
        );
        CREATE TABLE planets(
          stable_object_key TEXT,system_id INTEGER,planet_status TEXT,
          size_mass_class TEXT,insolation_class TEXT,classifier_version TEXT,
          orbital_period_days REAL,selected_fact_lineage_json TEXT
        );
        CREATE TABLE hierarchy_bundles(
          system_id INTEGER,payload_gzip BLOB,payload_sha256 TEXT
        );
        """
    )
    con.executemany(
        "INSERT INTO systems VALUES (?,?,?,?,?,?,?,?,?,?)",
        [
            (1, "canon:system:one", 1, 0, 10.0, 0, 0, 0, 0, 0),
            (2, "canon:system:two", 3, 1, 50.0, 1, 1, 1, 1, 1),
        ],
    )
    con.executemany(
        "INSERT INTO stars VALUES (?,?,?,?,?,?)",
        [
            ("canon:star:one", 1, "G", "source", "fact-g", 0.99),
            ("canon:star:two-a", 2, "WD", "source", "fact-wd", 1.0),
            ("canon:star:two-b", 2, "M", "assumed", "fact-m", 0.35),
        ],
    )
    con.execute(
        "INSERT INTO planets VALUES (?,?,?,?,?,?,?,?)",
        (
            "canon:planet:two-b",
            2,
            "confirmed",
            "terrestrial",
            "temperate",
            "planet-category-v1",
            0.75,
            json.dumps({"orbital_period_days": {"fact_id": "fact-period"}}),
        ),
    )
    hierarchy = {
        "hierarchy": {
            "root": {
                "node_kind": "system",
                "children": [
                    {
                        "node_kind": "stellar_group",
                        "component_family": "group",
                        "children": [],
                    }
                ],
            }
        }
    }
    con.execute(
        "INSERT INTO hierarchy_bundles VALUES (?,?,?)",
        (2, gzip.compress(json.dumps(hierarchy).encode()), "hierarchy-hash"),
    )
    con.commit()
    con.close()


def compile_fixture(tmp_path: Path, output_name: str = "out") -> Path:
    public_read = tmp_path / "public.sqlite"
    if not public_read.exists():
        make_public_read(public_read)
    output = tmp_path / output_name
    args = argparse.Namespace(
        registry=ROOT / "config/tags/registry.json",
        public_read=public_read,
        output_root=output,
        sample_limit=None,
        hash_input=False,
        force=False,
        repo_root=ROOT,
    )
    manifest = compiler.compile_tags(args)
    return output / manifest["build_id"] / manifest["registry_hash"]


def test_registry_is_typed_namespaced_and_expression_free() -> None:
    registry = registry_module.load_registry(ROOT / "config/tags/registry.json")
    assert len(registry.definitions) >= 30
    assert all(":" in definition["key"] for definition in registry.definitions)
    assert all(
        definition["evaluator"]["id"] in registry_module.KNOWN_EVALUATORS
        for definition in registry.definitions
    )
    statuses = {
        row["status"] for row in registry.proposal_inventory["families"]
    }
    assert statuses <= {"enabled", "deferred", "retired", "rejected"}


def test_compiler_materializes_object_assignments_rollups_and_sources(
    tmp_path: Path,
) -> None:
    artifact = compile_fixture(tmp_path)
    report = verifier.verify_artifact(artifact, "tag-test-build")
    assert report["status"] == "pass"
    con = sqlite3.connect(artifact / "smart_tags.sqlite")
    try:
        system_two = {
            row[0]
            for row in con.execute(
                "SELECT tag_key FROM system_tag_membership WHERE system_id=2"
            )
        }
        assert "science:stellar.white_dwarf" in system_two
        assert "science:stellar.m" in system_two
        assert "science:system.multiple" in system_two
        assert "science:system.hierarchical" in system_two
        assert "science:system.planet_host" in system_two
        assert "science:planet.temperate_terrestrial" in system_two
        assert "science:planet.habitable_zone_screen" in system_two
        assert "science:planet.ultrashort_period" in system_two
        assert con.execute(
            "SELECT count(*) FROM system_sources WHERE system_id=2"
        ).fetchone()[0] == 5
    finally:
        con.close()


def test_compiler_logical_output_is_deterministic(tmp_path: Path) -> None:
    first = compile_fixture(tmp_path, "first")
    second = compile_fixture(tmp_path, "second")
    first_manifest = json.loads((first / "manifest.json").read_text())
    second_manifest = json.loads((second / "manifest.json").read_text())
    assert first_manifest["logical_hashes"] == second_manifest["logical_hashes"]
    assert first_manifest["counts"] == second_manifest["counts"]


def test_runtime_attachment_exposes_typed_tags_and_bounded_filters(
    tmp_path: Path, monkeypatch
) -> None:
    artifact = compile_fixture(tmp_path)
    monkeypatch.setenv(
        "SPACEGATE_SMART_TAGS_PATH", str(artifact / "smart_tags.sqlite")
    )
    public = sqlite3.connect(tmp_path / "public.sqlite")
    public.row_factory = sqlite3.Row
    assert api_smart_tags.attach_to_public_read(public, "tag-test-build") is True
    payload = api_smart_tags.system_payload(public, 2)
    keys = {row["key"] for row in payload["smart_tags"]}
    assert "science:system.multiple" in keys
    assert payload["source_summary"]
    assert api_smart_tags.validate_filter_keys(
        public, ["science:system.multiple"]
    ) == ["science:system.multiple"]
    try:
        api_smart_tags.validate_filter_keys(public, ["science:no.such.tag"])
    except ValueError as exc:
        assert "unknown or non-filterable" in str(exc)
    else:
        raise AssertionError("unknown tag filter was accepted")
    public.close()
