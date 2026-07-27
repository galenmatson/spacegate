from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_smart_tags as compiler  # noqa: E402
import compare_smart_tag_builds as comparer  # noqa: E402
import package_smart_tags as packager  # noqa: E402
import smart_tag_registry as registry_module  # noqa: E402
import verify_smart_tags as verifier  # noqa: E402

sys.path.insert(0, str(ROOT / "srv/api"))
from app import smart_tags as api_smart_tags  # noqa: E402
from app import main as api_main  # noqa: E402


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
            ("canon:star:two-c", 2, "WD", "source_model", "fact-wd-model", 0.8),
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
                        "node_kind": "star",
                        "component_family": "star",
                        "child_count": 2,
                        "total_star_count": 2,
                        "source_catalog": "multiplicity.msc",
                        "stable_component_key": "comp:msc:test:ab",
                        "children": [
                            {
                                "node_kind": "source_star_leaf",
                                "component_family": "star",
                                "child_count": 0,
                                "total_star_count": 1,
                                "children": [],
                            },
                            {
                                "node_kind": "source_star_leaf",
                                "component_family": "star",
                                "child_count": 0,
                                "total_star_count": 1,
                                "children": [],
                            },
                        ],
                    },
                    {
                        "node_kind": "planet",
                        "source_catalog": "nasa_exoplanet_archive",
                        "stable_object_key": "canon:planet:two-b",
                        "children": [],
                    },
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
    assert statuses <= {
        "enabled",
        "deferred",
        "retired",
        "rejected",
        "compatibility-only",
    }
    proposals = registry.proposal_inventory["proposals"]
    assert len(proposals) >= 125
    assert all(row["reason"] for row in proposals)
    assert len(registry.legacy_token_inventory["surfaces"]) >= 20


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
                """
                SELECT d.tag_key
                FROM system_tag_membership m
                JOIN tag_definitions d USING(tag_id)
                WHERE m.system_id=2
                """
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
        status_rows = {
            row[0]: row[1]
            for row in con.execute(
                """
                SELECT d.tag_key,m.evidence_status_mask
                FROM system_tag_membership m
                JOIN tag_definitions d USING(tag_id)
                WHERE m.system_id=2
                """
            )
        }
        assert status_rows["science:stellar.m"] == compiler.EVIDENCE_STATUS_BITS["assumed"]
        assert status_rows["science:stellar.white_dwarf"] == (
            compiler.EVIDENCE_STATUS_BITS["source"]
            + compiler.EVIDENCE_STATUS_BITS["source_model"]
        )
        assert status_rows["science:planet.temperate_terrestrial"] == compiler.EVIDENCE_STATUS_BITS["derived"]
        assert status_rows["science:planet.habitable_zone_screen"] == compiler.EVIDENCE_STATUS_BITS["screen"]
        assert con.execute(
            "SELECT count(*) FROM system_sources WHERE system_id=2"
        ).fetchone()[0] == 2
        source_ids = {
            row[0]
            for row in con.execute(
            """
            SELECT d.source_id
            FROM system_sources s JOIN source_definitions d USING(source_num)
            WHERE s.system_id=2
            """
            )
        }
        assert source_ids == {
            "multiplicity.msc",
            "nasa_exoplanet_archive.planetary_systems",
        }
        assert "gaia.dr3.non_single_star" not in source_ids
    finally:
        con.close()


def test_compiler_logical_output_is_deterministic(tmp_path: Path) -> None:
    first = compile_fixture(tmp_path, "first")
    second = compile_fixture(tmp_path, "second")
    first_manifest = json.loads((first / "manifest.json").read_text())
    second_manifest = json.loads((second / "manifest.json").read_text())
    assert first_manifest["logical_hashes"] == second_manifest["logical_hashes"]
    assert first_manifest["counts"] == second_manifest["counts"]
    report = comparer.compare(first, second)
    assert report["status"] == "pass"


def test_release_package_is_byte_deterministic(tmp_path: Path) -> None:
    artifact = compile_fixture(tmp_path)
    first = tmp_path / "smart-tags-first.tar.gz"
    second = tmp_path / "smart-tags-second.tar.gz"
    first_result = packager.package(artifact, first)
    second_result = packager.package(artifact, second)
    assert first_result["sha256"] == second_result["sha256"]
    assert first.read_bytes() == second.read_bytes()


def test_sample_scope_uses_first_n_systems_not_an_id_ceiling() -> None:
    con = sqlite3.connect(":memory:")
    con.execute("ATTACH DATABASE ':memory:' AS public")
    con.execute("CREATE TABLE public.systems(system_id INTEGER PRIMARY KEY)")
    con.executemany("INSERT INTO public.systems VALUES (?)", [(10,), (20,), (30,)])
    assert compiler.prepare_sample_scope(con, 2) == 2
    assert con.execute(
        "SELECT system_id FROM compiler_system_scope ORDER BY system_id"
    ).fetchall() == [(10,), (20,)]
    con.close()


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
    tags = {row["key"]: row for row in payload["smart_tags"]}
    assert tags["science:stellar.m"]["assignment"]["evidence_statuses"] == [
        "assumed"
    ]
    assert tags["science:stellar.white_dwarf"]["assignment"][
        "evidence_statuses"
    ] == ["source", "source_model"]
    assert tags["science:planet.habitable_zone_screen"]["assignment"][
        "evidence_statuses"
    ] == ["screen"]
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
    assignments = api_smart_tags.assignments_payload(
        "tag-test-build", 2, limit=2
    )
    assert assignments["total"] >= 6
    assert len(assignments["assignments"]) == 2
    assert assignments["next_offset"] == 2
    assert assignments["source_contribution_total"] == 2
    assert len(assignments["source_contributions"]) == 2
    stats = api_smart_tags.runtime_stats()
    assert stats["artifact_identity"]["build_id"] == "tag-test-build"
    assert stats["counters"]["system_tags_queries"] >= 1
    public.close()


def test_registry_definition_and_source_api_endpoints(
    tmp_path: Path, monkeypatch
) -> None:
    artifact = compile_fixture(tmp_path)
    monkeypatch.setenv(
        "SPACEGATE_SMART_TAGS_PATH", str(artifact / "smart_tags.sqlite")
    )
    monkeypatch.setattr(api_main.db, "build_id", lambda: "tag-test-build")
    registry = api_main.smart_tag_registry()
    assert registry["registry_hash"]
    definition = api_main.smart_tag_definition(
        "science:system.multiple"
    )
    assert definition["tag"]["key"] == "science:system.multiple"
    source = api_main.smart_tag_source("source:multiplicity.msc")
    assert source["source"]["source_id"] == "multiplicity.msc"
    with pytest.raises(api_main.HTTPException) as missing:
        api_main.smart_tag_definition("science:no.such.tag")
    assert missing.value.status_code == 404
