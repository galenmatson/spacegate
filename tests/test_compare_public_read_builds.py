import importlib.util
import json
import sqlite3
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "compare_public_read_builds.py"
SPEC = importlib.util.spec_from_file_location("compare_public_read_builds", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


def _artifact(root: Path, *, candidate: bool) -> None:
    root.mkdir()
    counts = {
        "systems": 1,
        "stars": 1,
        "planets": 1,
        "aliases": 1,
        "search_terms": 1,
        "exact_identifiers": 1,
        "identifier_outcomes": 1,
        "identifier_quarantine": 1,
    }
    (root / "manifest.json").write_text(
        json.dumps({
            "build_id": root.name,
            "counts": counts,
            "logical_hashes": {"stars": "same-stars", "search_terms": "same-search"},
        }),
        encoding="utf-8",
    )
    con = sqlite3.connect(root / "public_read.sqlite")
    con.executescript(
        """
        CREATE TABLE systems(
          system_id INTEGER PRIMARY KEY, system_name TEXT, star_count INTEGER,
          spectral_classes_json TEXT, spectral_class_mask INTEGER,
          planet_category_mask INTEGER, hierarchy_representation TEXT,
          scene_representation TEXT, source_catalog TEXT, source_version TEXT,
          source_pk_text TEXT, source_row_hash TEXT, transform_version TEXT
        );
        CREATE TABLE stars(star_id INTEGER PRIMARY KEY,stable_object_key TEXT);
        CREATE TABLE planets(
          planet_id INTEGER PRIMARY KEY,system_id INTEGER,stable_object_key TEXT,
          planet_name TEXT,selected_fact_lineage_json TEXT,source_catalog TEXT,
          source_version TEXT,source_row_hash TEXT,transform_version TEXT
        );
        CREATE TABLE stellar_badge_overlays(
          system_id INTEGER,badge_order INTEGER,hierarchy_node_key TEXT,
          leaf_component_key TEXT,evidence_component_key TEXT,star_id_text TEXT,
          stable_object_key TEXT,display_name TEXT,catalog_component_label TEXT,
          classification_value TEXT,classification_status TEXT,evidence_basis TEXT,
          selected_fact_id TEXT,source_catalog TEXT,source_version TEXT
        );
        """
    )
    con.execute(
        "INSERT INTO systems VALUES (1,'System',?,?,?,?,?,?, 'source','v','pk','hash',?)",
        (
            1 if candidate else 2,
            '[\"F\"]' if candidate else '[\"F\",\"UNKNOWN\"]',
            1 if candidate else 3,
            64 if candidate else 0,
            "singleton_seed" if candidate else "bundle_required",
            "singleton_seed" if candidate else "full_scene",
            root.name,
        ),
    )
    con.execute("INSERT INTO stars VALUES (1,'canon:star:one')")
    con.execute(
        "INSERT INTO planets VALUES (1,1,'canon:planet:one','Planet',?,'source','v','hash','same')",
        (root.name,),
    )
    if not candidate:
        con.execute(
            "INSERT INTO stellar_badge_overlays VALUES (1,0,'canon:leaf:msc:x:ab',NULL,NULL,NULL,NULL,'False leaf','AB','UNKNOWN','missing','no_selected_leaf_classification',NULL,NULL,NULL)"
        )
    con.commit()
    con.close()


def test_reviewed_hierarchy_and_additive_planet_facets_pass(tmp_path, monkeypatch):
    baseline = tmp_path / "baseline"
    candidate = tmp_path / "candidate"
    _artifact(baseline, candidate=False)
    _artifact(candidate, candidate=True)
    hierarchy = tmp_path / "hierarchy.json"
    hierarchy.write_text(json.dumps({
        "status": "pass",
        "removed_nodes": [{
            "hierarchy_node_key": "canon:leaf:msc:x:ab",
            "source_basis": "msc_inferred_leaf",
        }],
    }), encoding="utf-8")
    report = tmp_path / "report.json"
    monkeypatch.setattr(sys, "argv", [
        str(SCRIPT), "--baseline", str(baseline), "--candidate", str(candidate),
        "--selected-hierarchy-ab", str(hierarchy), "--report", str(report),
    ])
    assert MODULE.main() == 0
    value = json.loads(report.read_text(encoding="utf-8"))
    assert value["status"] == "pass"
    assert value["stellar_badges"]["removed_count"] == 1
    assert value["system_presentation_changes"]["planet_category_bit_removal_count"] == 0


def test_unreviewed_leaf_removal_fails(tmp_path, monkeypatch):
    baseline = tmp_path / "baseline"
    candidate = tmp_path / "candidate"
    _artifact(baseline, candidate=False)
    _artifact(candidate, candidate=True)
    hierarchy = tmp_path / "hierarchy.json"
    hierarchy.write_text(json.dumps({"status": "pass", "removed_nodes": []}), encoding="utf-8")
    report = tmp_path / "report.json"
    monkeypatch.setattr(sys, "argv", [
        str(SCRIPT), "--baseline", str(baseline), "--candidate", str(candidate),
        "--selected-hierarchy-ab", str(hierarchy), "--report", str(report),
    ])
    assert MODULE.main() == 1
    value = json.loads(report.read_text(encoding="utf-8"))
    assert value["status"] == "fail"
    assert value["checks"]["removed_source_badges_equal_reviewed_leaf_removals"] is False
