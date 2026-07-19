from __future__ import annotations

import gzip
import tarfile
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from evidence_lake_native import (  # noqa: E402
    parse_cds_readme_text,
    parse_wds_format,
    safe_tar_members,
    write_atnf_catalog_parquet,
    write_fits_table_parquet,
    write_fixed_width_parquet,
    write_green_snr_parquet,
    write_html_table_parquet,
    write_tokenized_parquet,
    write_votable_files_parquet,
)


def test_parse_cds_readme_preserves_spans_units_flags_and_unnamed_fields() -> None:
    tables = parse_cds_readme_text(
        """
Byte-by-byte Description of file: orbit.dat
--------------------------------------------------------------------------------
 Bytes Format Units Label Explanations
--------------------------------------------------------------------------------
  1-  4 I4    ---   Seq   System number
      6 I1    ---   o     Orbit number
  8- 23 F16.9 d     Per   Period
     25 A1    ---   ---   Access flag
--------------------------------------------------------------------------------
Note (1): prose after the schema is not a field table.
  0 - no grade or secondary component
"""
    )
    assert list(tables) == ["orbit.dat"]
    assert tables["orbit.dat"] == [
        {
            "name": "Seq",
            "source_label": "Seq",
            "start": 1,
            "end": 4,
            "source_format": "I4",
            "unit": "---",
            "description": "System number",
        },
        {
            "name": "o",
            "source_label": "o",
            "start": 6,
            "end": 6,
            "source_format": "I1",
            "unit": "---",
            "description": "Orbit number",
        },
        {
            "name": "Per",
            "source_label": "Per",
            "start": 8,
            "end": 23,
            "source_format": "F16.9",
            "unit": "d",
            "description": "Period",
        },
        {
            "name": "unnamed_25_25",
            "source_label": "---",
            "start": 25,
            "end": 25,
            "source_format": "A1",
            "unit": "---",
            "description": "Access flag",
        },
    ]


def test_wds_format_and_fixed_width_writer_account_for_short_rows(tmp_path: Path) -> None:
    format_path = tmp_path / "format.txt"
    format_path.write_text(
        "    1 -  10   A10             2000 Coordinates\n"
        "   11 -  17   A7              Discoverer & Number\n",
        encoding="utf-8",
    )
    fields = parse_wds_format(format_path)
    assert [field["name"] for field in fields] == [
        "2000_coordinates",
        "discoverer_number",
    ]
    raw_path = tmp_path / "rows.dat.gz"
    with gzip.open(raw_path, "wt", encoding="utf-8") as handle:
        handle.write("00001+0001ABC 123\n")
        handle.write("00002+0002\n")
        handle.write("\n")
    output = tmp_path / "rows.parquet"
    report = write_fixed_width_parquet(raw_path, fields, output)
    assert report == {
        "row_count": 2,
        "source_row_accounting": {
            "source_line_count": 3,
            "blank_line_count": 1,
            "excluded_line_count": 0,
            "short_row_count": 1,
            "max_row_chars": 17,
        },
    }
    with duckdb.connect() as con:
        assert con.execute(
            f'''select source_line_number, "2000_coordinates", "discoverer_number", raw_row
                from read_parquet('{output}') order by source_line_number'''
        ).fetchall() == [
            (1, "00001+0001", "ABC 123", "00001+0001ABC 123"),
            (2, "00002+0002", None, "00002+0002"),
        ]


def test_fixed_width_writer_strips_only_one_configured_trailing_delimiter(
    tmp_path: Path,
) -> None:
    raw_path = tmp_path / "rows.dat"
    raw_path.write_text("ABC|SR|Cst|TAIL||\n", encoding="utf-8")
    fields = [
        {"name": "name", "start": 1, "end": 4},
        {"name": "variable_type", "start": 5, "end": 11},
        {"name": "note", "start": 12, "end": 17},
    ]
    output = tmp_path / "rows.parquet"
    report = write_fixed_width_parquet(
        raw_path,
        fields,
        output,
        trailing_layout_delimiters=("|",),
    )
    assert (
        report["source_row_accounting"]["trailing_layout_delimiter_stripped_count"]
        == 3
    )
    assert report["source_row_accounting"][
        "trailing_layout_delimiter_stripped_by_field"
    ] == {"name": 1, "note": 1, "variable_type": 1}
    with duckdb.connect() as con:
        assert con.execute(
            f"select name, variable_type, note, raw_row from read_parquet('{output}')"
        ).fetchone() == ("ABC", "SR|Cst", "TAIL|", "ABC|SR|Cst|TAIL||")

    with pytest.raises(ValueError, match="unique single non-whitespace"):
        write_fixed_width_parquet(
            raw_path,
            fields,
            tmp_path / "invalid.parquet",
            trailing_layout_delimiters=(" ",),
        )


def test_atnf_catalog_preserves_duplicate_parameters_comments_and_irregular_rows(
    tmp_path: Path,
) -> None:
    parameters = tmp_path / "parameters.parquet"
    comments = tmp_path / "comments.parquet"
    result = write_atnf_catalog_parquet(
        "#CATALOGUE 2.8.0\n"
        "PSRJ     J0001+0001                    ref1\n"
        "# conflicting evidence\n"
        "W50      82                            ref1\n"
        "W50      77                       6    ref2\n"
        "NGLT 1\n"
        "@----------------\n",
        parameters,
        comments,
    )
    assert result == {
        "catalogue_block_count": 1,
        "pulsar_block_count": 1,
        "parameter_row_count": 4,
        "comment_row_count": 2,
    }
    with duckdb.connect() as con:
        assert con.execute(
            f"select parameter_name, parameter_occurrence, value_raw, uncertainty_raw "
            f"from read_parquet('{parameters}') order by source_line_number"
        ).fetchall() == [
            ("PSRJ", 1, "J0001+0001", None),
            ("W50", 1, "82", None),
            ("W50", 2, "77", "6"),
            ("NGLT", 1, "1", None),
        ]
        assert con.execute(
            f"select pulsar_name, comment_scope, comment_text "
            f"from read_parquet('{comments}') "
            "order by source_line_number"
        ).fetchall() == [
            (None, "catalogue_header", "CATALOGUE 2.8.0"),
            ("J0001+0001", "pulsar_record", "conflicting evidence"),
        ]


def test_safe_tar_members_rejects_traversal(tmp_path: Path) -> None:
    archive_path = tmp_path / "unsafe.tar"
    source = tmp_path / "row.txt"
    source.write_text("row", encoding="utf-8")
    with tarfile.open(archive_path, "w") as archive:
        archive.add(source, arcname="../row.txt")
    with tarfile.open(archive_path) as archive:
        with pytest.raises(ValueError, match="unsafe archive member"):
            safe_tar_members(archive)


def test_tokenized_writer_fails_on_field_loss(tmp_path: Path) -> None:
    source = tmp_path / "rows.txt"
    source.write_text("title\na|b|c\n1|2|3\n", encoding="utf-8")
    output = tmp_path / "rows.parquet"
    result = write_tokenized_parquet(
        source, output, ["one", "two", "three"], skip_lines=2, delimiter="|"
    )
    assert result["row_count"] == 1
    malformed = tmp_path / "malformed.txt"
    malformed.write_text("1|2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="expected 3"):
        write_tokenized_parquet(
            malformed,
            tmp_path / "malformed.parquet",
            ["one", "two", "three"],
            delimiter="|",
        )


def test_green_snr_writer_preserves_uncertain_values_and_other_names(tmp_path: Path) -> None:
    source = tmp_path / "snr.html"
    source.write_text(
        '<A HREF="snrs.G4.5+6.8.html">4.5  +6.8</A>  '
        "17 30 42  -21 29     3     S      19?     0.64     Kepler, SN1604\n",
        encoding="utf-8",
    )
    output = tmp_path / "snr.parquet"
    result = write_green_snr_parquet(source, output)
    assert result["row_count"] == 1
    with duckdb.connect() as con:
        assert con.execute(
            f"select flux_1ghz, spectral_index, other_names, detail_href "
            f"from read_parquet('{output}')"
        ).fetchone() == ("19?", "0.64", "Kepler, SN1604", "snrs.G4.5+6.8.html")


def test_html_table_writer_validates_schema_and_preserves_resources(tmp_path: Path) -> None:
    source = tmp_path / "names.html"
    source.write_text(
        '<table id="names"><thead><tr><th>Name</th><th>Designation</th></tr></thead>'
        '<tbody><tr id="names_row_0" data-row-index="0">'
        '<td>&alpha; Centauri</td><td><a href="/object/1">HIP 71683</a>'
        '<img src="/image/1.png" alt="chart"></td></tr></tbody>'
        '<tfoot><tr><td>Name</td><td>Designation</td></tr></tfoot></table>'
        '<table id="calendar"><tr><th>Day</th></tr><tr><td>1</td></tr></table>',
        encoding="utf-8",
    )
    output = tmp_path / "names.parquet"
    second_output = tmp_path / "names_second.parquet"
    result = write_html_table_parquet(
        source,
        output,
        table_id="names",
        fields=[
            {"source_header": "Name", "name": "proper_name"},
            {"source_header": "Designation", "name": "designation"},
        ],
    )
    write_html_table_parquet(
        source,
        second_output,
        table_id="names",
        fields=[
            {"source_header": "Name", "name": "proper_name"},
            {"source_header": "Designation", "name": "designation"},
        ],
    )
    assert output.read_bytes() == second_output.read_bytes()
    assert result["row_count"] == 1
    assert result["source_table_count"] == 2
    assert result["excluded_page_table_count"] == 1
    assert result["excluded_footer_row_count"] == 1
    with duckdb.connect() as con:
        row = con.execute(
            f"select source_table_id, source_row_number, source_row_id, "
            f"source_row_index, proper_name, designation, source_cell_resources_json "
            f"from read_parquet('{output}')"
        ).fetchone()
    assert row[:6] == ("names", 1, "names_row_0", 0, "\u03b1 Centauri", "HIP 71683")
    assert row[6] == (
        '[{"cell_index":1,"field_name":"designation","resources":'
        '[{"attributes":{"href":"/object/1"},"tag":"a"},'
        '{"attributes":{"alt":"chart","src":"/image/1.png"},"tag":"img"}]}]'
    )


def test_html_table_writer_rejects_source_schema_drift(tmp_path: Path) -> None:
    source = tmp_path / "names.html"
    source.write_text(
        '<table id="names"><tr><th>Name</th><th>Unexpected</th></tr>'
        '<tr><td>Sol</td><td>G2V</td></tr></table>',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="header drift"):
        write_html_table_parquet(
            source,
            tmp_path / "names.parquet",
            table_id="names",
            fields=[
                {"source_header": "Name", "name": "proper_name"},
                {"source_header": "Designation", "name": "designation"},
            ],
        )


def test_fits_writer_preserves_schema_and_normalizes_declared_nulls(tmp_path: Path) -> None:
    import numpy as np
    from astropy.io import fits

    plain = tmp_path / "sample.fits"
    columns = [
        fits.Column(name="source_id", format="K", null=-1, array=np.array([1, -1])),
        fits.Column(name="value", format="D", array=np.array([2.5, np.nan])),
        fits.Column(name="label", format="10A", array=np.array(["alpha", "beta"])),
        fits.Column(name="flag", format="L", array=np.array([True, False])),
    ]
    fits.BinTableHDU.from_columns(columns).writeto(plain)
    compressed = tmp_path / "sample.fits.gz"
    with plain.open("rb") as source, gzip.open(compressed, "wb") as destination:
        destination.write(source.read())
    output = tmp_path / "sample.parquet"
    result = write_fits_table_parquet(compressed, output, batch_size=1)
    assert result["row_count"] == 2
    assert [field["name"] for field in result["source_schema"]] == [
        "source_id",
        "value",
        "label",
        "flag",
    ]
    with duckdb.connect() as con:
        assert con.execute(
            f"select source_id, value, label, flag from read_parquet('{output}') "
            "order by label"
        ).fetchall() == [(1, 2.5, "alpha", True), (None, None, "beta", False)]


def test_fits_writer_selects_hdu_and_preserves_fixed_size_arrays(tmp_path: Path) -> None:
    import numpy as np
    from astropy.io import fits

    source = tmp_path / "multi.fits"
    rows = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="source_id", format="K", array=np.array([1, 2])),
            fits.Column(
                name="samples",
                format="3E",
                array=np.array([[1.0, 2.0, np.nan], [3.0, 4.0, 5.0]]),
            ),
        ]
    )
    metadata = fits.BinTableHDU.from_columns(
        [fits.Column(name="release", format="8A", array=np.array(["dr-test"]))]
    )
    fits.HDUList([fits.PrimaryHDU(), rows, metadata]).writeto(source)

    output = tmp_path / "rows.parquet"
    result = write_fits_table_parquet(source, output, hdu_index=1, batch_size=1)
    assert result["source_hdu"]["index"] == 1
    assert result["source_hdu"]["row_count"] == 2
    schema = pq.read_schema(output)
    assert schema.field("samples").type == pa.list_(pa.float32(), 3)
    assert pq.read_table(output)["samples"].to_pylist() == [
        [1.0, 2.0, None],
        [3.0, 4.0, 5.0],
    ]


def test_votable_writer_preserves_native_types_nulls_and_arrays(tmp_path: Path) -> None:
    import gzip
    import io
    import numpy as np
    from astropy.io.votable import from_table, writeto
    from astropy.table import MaskedColumn, Table

    table = Table()
    table["source_id"] = np.array([1, 2], dtype=np.int64)
    table["value"] = MaskedColumn([2.5, 3.5], mask=[False, True], unit="K")
    table["samples"] = np.array([[1.0, 2.0], [3.0, 4.0]])
    payload = io.BytesIO()
    writeto(from_table(table), payload, tabledata_format="binary2")
    source = tmp_path / "rows.vot.gz"
    source.write_bytes(gzip.compress(payload.getvalue(), mtime=0))
    output = tmp_path / "rows.parquet"
    report = write_votable_files_parquet([source], output)
    assert report["row_count"] == 2
    assert [field["name"] for field in report["source_schema"]] == [
        "source_id",
        "value",
        "samples",
    ]
    with duckdb.connect() as con:
        assert con.execute(
            f"select source_id, value, samples from read_parquet('{output}') order by source_id"
        ).fetchall() == [(1, 2.5, [1.0, 2.0]), (2, None, [3.0, 4.0])]


def test_votable_writer_preserves_case_only_source_field_distinctions(
    tmp_path: Path,
) -> None:
    source = tmp_path / "case-fields.vot"
    source.write_text(
        """<?xml version="1.0"?>
<VOTABLE version="1.3" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">
  <RESOURCE><TABLE>
    <FIELD name="b_rgeo" datatype="double" unit="pc" />
    <FIELD name="B_rgeo" datatype="double" unit="pc" />
    <DATA><TABLEDATA><TR><TD>10.0</TD><TD>20.0</TD></TR></TABLEDATA></DATA>
  </TABLE></RESOURCE>
</VOTABLE>
""",
        encoding="ascii",
    )
    output = tmp_path / "case-fields.parquet"
    report = write_votable_files_parquet([source], output)
    assert report["source_schema"][0]["name"] == "b_rgeo"
    assert report["source_schema"][1]["name"] == "B_rgeo__source_case_2"
    assert report["source_schema"][1]["source_name"] == "B_rgeo"
    assert (
        report["source_schema"][1]["name_normalization"]
        == "case_insensitive_collision_alias_v1"
    )
    with duckdb.connect() as con:
        assert con.execute(
            f'''select b_rgeo, "B_rgeo__source_case_2" from read_parquet('{output}')'''
        ).fetchone() == (10.0, 20.0)
