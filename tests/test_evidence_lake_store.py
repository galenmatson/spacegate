from __future__ import annotations

import json
import tarfile
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from evidence_lake_store import (  # noqa: E402
    build_raw_snapshot,
    build_typed_snapshot,
    manifest_entries,
    verify_snapshot,
    write_delimited_parquet,
    write_mast_json_parquet,
)
from evidence_lake_native import (  # noqa: E402
    write_mcgill_magnetar_html_parquet,
    write_oec_archive_parquet,
)


def source_contract() -> dict:
    return {
        "source_id": "test.catalog",
        "release_id": "r1",
        "registry_version": "test.1",
        "authority_roles": {"inventory": "test"},
        "license": {"name": "Test", "url": "https://example.test/license"},
        "citation_url": "https://example.test/citation",
        "identity_namespaces": ["test_id"],
        "frame_epoch": "J2000",
        "schema_policy": {
            "kind": "delimited_header",
            "drift": "fail_until_reviewed",
            "default_disposition": "preserve",
        },
        "manifest_entries": [
            {"manifest": "test_manifest.json", "source_name": "test_rows"}
        ],
    }


def test_raw_snapshot_is_independent_and_typed_cook_is_deterministic(tmp_path: Path) -> None:
    state = tmp_path / "state"
    raw = state / "raw" / "test"
    manifests = state / "reports" / "manifests"
    raw.mkdir(parents=True)
    manifests.mkdir(parents=True)
    source_path = raw / "rows.csv"
    source_path.write_text("source_id,value,quality\n1,2.5,A\n2,3.5,B\n", encoding="utf-8")

    import hashlib

    source_sha = hashlib.sha256(source_path.read_bytes()).hexdigest()
    (manifests / "test_manifest.json").write_text(
        json.dumps(
            [
                {
                    "source_name": "test_rows",
                    "source_version": "r1",
                    "dest_path": "raw/test/rows.csv",
                    "sha256": source_sha,
                    "row_count": 2,
                    "url": "https://example.test/rows.csv",
                    "retrieved_at": "2026-07-18T00:00:00Z",
                }
            ]
        ),
        encoding="utf-8",
    )

    source = source_contract()
    entries = manifest_entries(manifests)
    raw_root = state / "raw" / "evidence_lake_v2"
    raw_manifest = build_raw_snapshot(source, entries, state, raw_root)
    snapshot_dir = raw_root / "test.catalog" / "r1" / raw_manifest["snapshot_id"]
    snapshotted = snapshot_dir / raw_manifest["artifacts"][0]["artifact_path"] / "rows.csv"

    assert snapshotted.read_bytes() == source_path.read_bytes()
    assert snapshotted.stat().st_ino != source_path.stat().st_ino
    assert raw_manifest["artifacts"][0]["materialization_counts"] == {"copy": 1, "hardlink": 0}

    source_path.write_text("source_id,value,quality\n9,9.9,Z\n", encoding="utf-8")
    assert snapshotted.read_text(encoding="utf-8").endswith("2,3.5,B\n")

    typed_root = state / "typed" / "evidence_lake_v2"
    typed_manifest = build_typed_snapshot(source, snapshot_dir, typed_root)
    typed_dir = (
        typed_root
        / "test.catalog"
        / "r1"
        / raw_manifest["snapshot_id"]
        / typed_manifest["typed_snapshot_id"]
    )
    table = typed_manifest["tables"][0]
    assert table["status"] == "typed"
    assert table["row_count"] == 2
    assert [column["name"] for column in table["columns"]] == [
        "source_id",
        "value",
        "quality",
    ]
    with duckdb.connect() as con:
        assert con.execute(
            f"select sum(cast(value as double)) from read_parquet('{typed_dir / table['parquet_path']}')"
        ).fetchone()[0] == 6.0

    repeated = build_typed_snapshot(source, snapshot_dir, typed_root)
    assert repeated["content_sha256"] == typed_manifest["content_sha256"]
    clean_rebuild = build_typed_snapshot(
        source, snapshot_dir, state / "typed_clean" / "evidence_lake_v2"
    )
    assert clean_rebuild["content_sha256"] == typed_manifest["content_sha256"]
    assert clean_rebuild["tables"][0]["sha256"] == table["sha256"]
    verification = verify_snapshot(snapshot_dir, typed_dir)
    assert verification["status"] == "pass"


def test_parser_pending_preserves_raw_contract(tmp_path: Path) -> None:
    state = tmp_path / "state"
    raw = state / "raw" / "test"
    manifests = state / "reports" / "manifests"
    raw.mkdir(parents=True)
    manifests.mkdir(parents=True)
    source_path = raw / "rows.dat"
    source_path.write_text("fixed width source row\n", encoding="utf-8")

    import hashlib

    (manifests / "test_manifest.json").write_text(
        json.dumps(
            [
                {
                    "source_name": "test_rows",
                    "dest_path": "raw/test/rows.dat",
                    "sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
                }
            ]
        ),
        encoding="utf-8",
    )
    source = source_contract()
    source["schema_policy"]["kind"] = "documented_fixed_width"
    raw_root = state / "raw" / "evidence_lake_v2"
    raw_manifest = build_raw_snapshot(
        source, manifest_entries(manifests), state, raw_root
    )
    snapshot_dir = raw_root / "test.catalog" / "r1" / raw_manifest["snapshot_id"]
    typed = build_typed_snapshot(source, snapshot_dir, state / "typed" / "evidence_lake_v2")
    assert typed["tables"][0]["status"] == "parser_pending"
    assert typed["tables"][0]["raw_tree_sha256"]


def test_documented_fixed_width_passes_configured_layout_delimiter_policy(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    source_dir = state / "raw" / "test"
    manifests = state / "reports" / "manifests"
    source_dir.mkdir(parents=True)
    manifests.mkdir(parents=True)
    readme = source_dir / "ReadMe"
    readme.write_text(
        "Byte-by-byte Description of file: rows.dat\n"
        "--------------------------------------------------------------------------------\n"
        " Bytes Format Units Label Explanations\n"
        "--------------------------------------------------------------------------------\n"
        "  1-  4 A4    ---   Name  Source name\n"
        "  5- 11 A7    ---   Type  Variable type\n"
        " 12- 17 A6    ---   Note  Source note\n"
        "--------------------------------------------------------------------------------\n",
        encoding="utf-8",
    )
    rows = source_dir / "rows.dat"
    rows.write_text("ABC|SR|Cst|TAIL||\n", encoding="utf-8")

    import hashlib

    entries = []
    for source_name, path in (("test_readme", readme), ("test_rows", rows)):
        entries.append(
            {
                "source_name": source_name,
                "dest_path": str(path.relative_to(state)),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    (manifests / "test_manifest.json").write_text(
        json.dumps(entries), encoding="utf-8"
    )
    source = source_contract()
    source["manifest_entries"] = [
        {"manifest": "test_manifest.json", "source_name": "test_readme"},
        {"manifest": "test_manifest.json", "source_name": "test_rows"},
    ]
    source["schema_policy"] = {
        "kind": "documented_fixed_width",
        "drift": "fail_until_reviewed",
        "default_disposition": "preserve",
        "readme_bindings": {"test_rows": "test_readme"},
        "trailing_layout_delimiters": ["|"],
    }
    raw_root = state / "raw" / "evidence_lake_v2"
    raw_manifest = build_raw_snapshot(
        source, manifest_entries(manifests), state, raw_root
    )
    snapshot_dir = raw_root / "test.catalog" / "r1" / raw_manifest["snapshot_id"]
    typed_root = state / "typed" / "evidence_lake_v2"
    typed = build_typed_snapshot(source, snapshot_dir, typed_root)
    table = next(row for row in typed["tables"] if row["source_name"] == "test_rows")
    assert table["parser"] == "documented_fixed_width_lexical_layout_delimiter_v2"
    assert table["source_row_accounting"][
        "trailing_layout_delimiter_stripped_count"
    ] == 3
    typed_dir = (
        typed_root
        / "test.catalog"
        / "r1"
        / raw_manifest["snapshot_id"]
        / typed["typed_snapshot_id"]
    )
    with duckdb.connect() as con:
        assert con.execute(
            f"select Name, Type, Note, raw_row from read_parquet("
            f"'{typed_dir / table['parquet_path']}')"
        ).fetchone() == ("ABC", "SR|Cst", "TAIL|", "ABC|SR|Cst|TAIL||")


def test_mast_json_uses_declared_union_schema_across_null_batches(tmp_path: Path) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    fields = [
        {"name": "ID", "type": "string"},
        {"name": "KIC", "type": "int"},
        {"name": "Tmag", "type": "float"},
    ]
    first.write_text(
        json.dumps({"fields": fields, "data": [{"ID": 123, "KIC": None, "Tmag": 10}]}),
        encoding="utf-8",
    )
    second.write_text(
        json.dumps({"fields": fields, "data": [{"ID": "456", "KIC": 789, "Tmag": 11.5}]}),
        encoding="utf-8",
    )
    output = tmp_path / "mast.parquet"
    schema = write_mast_json_parquet([first, second], output)
    assert schema == [
        {"name": "ID", "source_type": "string"},
        {"name": "KIC", "source_type": "int"},
        {"name": "Tmag", "source_type": "float"},
    ]
    with duckdb.connect() as con:
        assert con.execute(
            f"select ID, KIC, Tmag from read_parquet('{output}') order by ID"
        ).fetchall() == [("123", None, 10.0), ("456", 789, 11.5)]


def test_delimited_member_lineage_preserves_distinct_upstream_tables(
    tmp_path: Path,
) -> None:
    hip = tmp_path / "hip_00001.csv"
    tmass = tmp_path / "twomass_00001.csv"
    hip.write_text("source_id,external_id\n1,42\n", encoding="utf-8")
    tmass.write_text("source_id,external_id\n2,00420000+0000000\n", encoding="utf-8")
    output = tmp_path / "external.parquet"

    with duckdb.connect() as con:
        fields = write_delimited_parquet(
            [hip, tmass],
            output,
            con,
            member_lineage_field="source_member_path",
        )
        rows = con.execute(
            f"select source_id, external_id, source_member_path "
            f"from read_parquet('{output}') order by source_id"
        ).fetchall()

    assert fields == ["external_id", "source_id", "source_member_path"]
    assert rows == [
        ("1", "42", "hip_00001.csv"),
        ("2", "00420000+0000000", "twomass_00001.csv"),
    ]


def test_delimited_continuation_uses_one_header_and_accounts_for_all_rows(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    snapshot = state / "raw" / "snapshot"
    first_dir = snapshot / "artifacts" / "part_1"
    second_dir = snapshot / "artifacts" / "part_2"
    first_dir.mkdir(parents=True)
    second_dir.mkdir(parents=True)
    (first_dir / "part1.csv").write_text("id,value\n1,a\n", encoding="utf-8")
    (second_dir / "part2.csv").write_text("2,b\n3,c\n", encoding="utf-8")
    source = source_contract()
    source["source_id"] = "test.continuation"
    source["schema_policy"]["artifact_layout"] = {
        "kind": "delimited_continuation",
        "table_name": "combined",
        "header_artifact": "part_1",
        "continuation_artifacts": ["part_2"],
    }
    artifacts = []
    import hashlib

    for name, relative, filename in (
        ("part_1", "artifacts/part_1", "part1.csv"),
        ("part_2", "artifacts/part_2", "part2.csv"),
    ):
        path = snapshot / relative / filename
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        artifacts.append(
            {
                "source_name": name,
                "artifact_path": relative,
                "tree_sha256": digest,
                "files": [{"path": filename, "sha256": digest, "bytes": path.stat().st_size}],
            }
        )
    raw_manifest = {
        "snapshot_id": "raw1",
        "content_sha256": "abc",
        "artifacts": artifacts,
    }
    (snapshot / "snapshot_manifest.json").write_text(
        json.dumps(raw_manifest), encoding="utf-8"
    )
    typed = build_typed_snapshot(source, snapshot, state / "typed")
    table = typed["tables"][0]
    assert table["status"] == "typed"
    assert table["row_count"] == 3
    assert [row["row_count"] for row in table["source_row_accounting"]] == [1, 2]


def test_generic_fits_cook_ignores_acquisition_metadata_wrapper(tmp_path: Path) -> None:
    import hashlib

    import numpy as np
    from astropy.io import fits

    state = tmp_path / "state"
    snapshot = state / "raw" / "snapshot"
    artifact_dir = snapshot / "artifacts" / "wide_binary"
    artifact_dir.mkdir(parents=True)
    fits_path = artifact_dir / "wide_binary.fits"
    fits.BinTableHDU.from_columns(
        [
            fits.Column(name="source_id1", format="K", array=np.array([1, 2])),
            fits.Column(name="source_id2", format="K", array=np.array([3, 4])),
            fits.Column(name="chance_alignment", format="D", array=np.array([0.01, 0.5])),
        ]
    ).writeto(fits_path)
    metadata_path = artifact_dir / "product_manifest.json"
    metadata_path.write_text('{"schema_version":"test"}\n', encoding="utf-8")
    files = []
    for path in (fits_path, metadata_path):
        files.append(
            {
                "path": path.name,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "bytes": path.stat().st_size,
            }
        )
    raw_manifest = {
        "snapshot_id": "raw_fits",
        "content_sha256": "fits-content",
        "artifacts": [
            {
                "source_name": "wide_binary",
                "artifact_path": "artifacts/wide_binary",
                "tree_sha256": "tree-hash",
                "expected_row_count": 2,
                "files": files,
            }
        ],
    }
    (snapshot / "snapshot_manifest.json").write_text(
        json.dumps(raw_manifest), encoding="utf-8"
    )
    source = source_contract()
    source["source_id"] = "multiplicity.el_badry_2021_wide_binary"
    source["schema_policy"]["kind"] = "fits_and_method_documents"
    typed = build_typed_snapshot(source, snapshot, state / "typed")
    table = typed["tables"][0]
    assert table["status"] == "typed"
    assert table["parser"] == "fits_binary_table_source_native_v1"
    assert table["row_count"] == 2
    assert [column["name"] for column in table["columns"]] == [
        "source_id1",
        "source_id2",
        "chance_alignment",
    ]


def test_oec_archive_preserves_object_graph_names_parameters_and_disposition(
    tmp_path: Path,
) -> None:
    source = tmp_path / "oec"
    systems = source / "release" / "systems"
    systems.mkdir(parents=True)
    (systems / "Example.xml").write_text(
        "<system><name>Example</name><distance errorminus=\"1\">10</distance>"
        "<star><name>Example A</name><name>HD 1</name>"
        "<planet><name>Example b</name><list>Controversial</list>"
        "<mass errorplus=\"0.2\">1.5</mass></planet></star></system>",
        encoding="utf-8",
    )
    archive_path = tmp_path / "oec.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(source / "release", arcname="release")
    outputs = {
        name: tmp_path / f"{name}.parquet"
        for name in ("objects", "names", "parameters", "relations")
    }
    with tarfile.open(archive_path, "r:gz") as archive:
        counts = write_oec_archive_parquet(
            archive,
            objects_output=outputs["objects"],
            names_output=outputs["names"],
            parameters_output=outputs["parameters"],
            relations_output=outputs["relations"],
        )
    assert counts == {
        "xml_member_count": 1,
        "object_row_count": 3,
        "name_row_count": 4,
        "parameter_row_count": 3,
        "relation_row_count": 2,
    }
    with duckdb.connect() as con:
        assert con.execute(
            "select object_kind,primary_name_raw,list_disposition_raw "
            f"from read_parquet('{outputs['objects']}') order by source_node_path"
        ).fetchall() == [
            ("system", "Example", None),
            ("star", "Example A", None),
            ("planet", "Example b", "Controversial"),
        ]
        assert con.execute(
            "select parameter_name,value_raw,attributes_json "
            f"from read_parquet('{outputs['parameters']}') order by parameter_name"
        ).fetchall() == [
            ("distance", "10", '{"errorminus":"1"}'),
            ("list", "Controversial", "{}"),
            ("mass", "1.5", '{"errorplus":"0.2"}'),
        ]


def test_mcgill_html_preserves_rows_and_deduplicates_reference_codes(
    tmp_path: Path,
) -> None:
    cells = ["Magnetar A"] + ["..."] * 16
    cells[1] = (
        '1.23 <a href="http://adsabs.harvard.edu/abs/2020ApJ...123...45A">'
        "[abc+20]</a>"
    )
    cells[2] = (
        '4.56 <a href="http://adsabs.harvard.edu/abs/2020ApJ...123...45A">'
        "[abc+20]</a>"
    )
    html = tmp_path / "main.html"
    html.write_text(
        "<html><table><tr><th>Name</th></tr>"
        + "<tr>"
        + "".join(f"<td>{value}</td>" for value in cells)
        + "</tr><tr><td colspan='17'>section</td></tr></table></html>",
        encoding="utf-8",
    )
    rows = tmp_path / "rows.parquet"
    links = tmp_path / "links.parquet"
    references = tmp_path / "references.parquet"
    report = write_mcgill_magnetar_html_parquet(
        html,
        rows_output=rows,
        links_output=links,
        references_output=references,
        base_url="https://example.test/main.html",
    )
    assert report == {
        "source_table_count": 1,
        "source_data_row_count": 1,
        "source_section_row_count": 1,
        "typed_row_count": 2,
        "typed_link_count": 2,
        "typed_external_reference_count": 1,
    }
    with duckdb.connect() as con:
        assert con.execute(
            f"select row_kind,magnetar_name_raw from read_parquet('{rows}') "
            "order by source_row_number"
        ).fetchall() == [("data", "Magnetar A"), ("section", None)]
        assert con.execute(
            f"select reference_code_raw,bibcode_raw,occurrence_count "
            f"from read_parquet('{references}')"
        ).fetchall() == [("abc+20", "2020ApJ...123...45A", 2)]
