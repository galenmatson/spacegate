from __future__ import annotations

import csv
import gzip
import io
import json
import sys
from pathlib import Path

from astropy.io.votable import from_table, writeto
from astropy.table import Table


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import evidence_tap_acquire as acquire  # noqa: E402


class FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


def schema_payload() -> bytes:
    out = io.StringIO()
    writer = csv.DictWriter(
        out, fieldnames=["column_name", "datatype", "unit", "ucd", "description"]
    )
    writer.writeheader()
    writer.writerow(
        {
            "column_name": "source_id",
            "datatype": "long",
            "unit": "",
            "ucd": "meta.id",
            "description": "identifier",
        }
    )
    writer.writerow(
        {
            "column_name": "value",
            "datatype": "double",
            "unit": "K",
            "ucd": "phys.temperature",
            "description": "value",
        }
    )
    return out.getvalue().encode()


def test_sync_tap_request_bounds_socket_inactivity_timeout(monkeypatch) -> None:
    timeouts: list[int] = []

    def fake_urlopen(_request, timeout):
        timeouts.append(timeout)
        return FakeResponse(b"source_id\n1\n")

    monkeypatch.setattr(acquire.urllib.request, "urlopen", fake_urlopen)
    assert acquire.tap_request(
        "https://example.test/tap",
        "select source_id from test.rows",
        timeout_s=30,
        read_stall_timeout_s=4,
        retries=1,
        max_rec=10,
    ) == b"source_id\n1\n"
    assert timeouts == [4]


def test_resolve_product_fields_preserves_release_order_and_groups() -> None:
    schema = [
        {"column_name": "solution_id"},
        {"column_name": "source_id"},
        {"column_name": "teff_model"},
        {"column_name": "teff_model_lower"},
        {"column_name": "unused"},
    ]
    resolved = acquire.resolve_product_fields(
        {
            "product_name": "test",
            "table": "test.rows",
            "table_alias": "r",
            "include_fields": ["solution_id", "source_id"],
            "include_contains": ["teff_model"],
        },
        schema,
    )
    assert resolved["select"] == [
        "r.solution_id",
        "r.source_id",
        "r.teff_model",
        "r.teff_model_lower",
    ]


def test_resolve_product_fields_quotes_case_collisions() -> None:
    resolved = acquire.resolve_product_fields(
        {
            "product_name": "test",
            "table": "test.rows",
            "preserve_all_fields": True,
        },
        [
            {"column_name": "source_id"},
            {"column_name": "b_rgeo"},
            {"column_name": "B_rgeo"},
        ],
    )
    assert resolved["select"] == ["source_id", '"b_rgeo"', '"B_rgeo"']
    assert [acquire.selected_output_name(value) for value in resolved["select"]] == [
        "source_id",
        "b_rgeo",
        "B_rgeo",
    ]


def test_resolve_product_fields_quotes_nonregular_vizier_identifiers() -> None:
    resolved = acquire.resolve_product_fields(
        {
            "product_name": "clusters",
            "table": '"J/A+A/686/A42/clusters"',
            "table_alias": "c",
            "preserve_all_fields": True,
        },
        [
            {"column_name": "ID"},
            {"column_name": "CMDCl2.5"},
            {"column_name": "_RA.icrs"},
        ],
    )
    assert resolved["select"] == [
        "c.ID",
        'c."CMDCl2.5" as CMDCl2_5',
        'c."_RA.icrs" as _RA_icrs',
    ]
    assert resolved["selected_source_fields"] == ["ID", "CMDCl2.5", "_RA.icrs"]
    assert acquire.selected_output_name('c."CMDCl2.5" as CMDCl2_5') == "CMDCl2_5"


def test_acquire_product_is_exact_compressed_resumable_and_field_accounted(
    tmp_path: Path, monkeypatch
) -> None:
    calls: list[str] = []

    def fake_request(
        endpoint, adql, *, timeout_s, read_stall_timeout_s, retries, max_rec
    ):
        calls.append(adql)
        if "tap_schema.columns" in adql:
            return schema_payload()
        bucket = 0 if "= 0" in adql else 1
        return f"source_id,value\n{bucket + 1},{10 + bucket}\n".encode()

    monkeypatch.setattr(acquire, "tap_request", fake_request)
    program = {
        "schema_version": "spacegate.e3_acquisition_program.v1",
        "program_version": "test.1",
    }
    product = {
        "product_name": "test_rows",
        "source_id": "test.catalog",
        "release_id": "r1",
        "endpoint": "https://example.test/tap",
        "table": "test.rows",
        "table_alias": "r",
        "from": "test.rows r",
        "preserve_all_fields": True,
        "where": "r.source_id > 0",
        "partition_expression": "r.source_id",
        "order_by": "r.source_id",
        "buckets": 2,
        "max_rec": 10,
        "unselected_field_reason": "none",
    }
    first = acquire.acquire_product(
        program,
        product,
        state_dir=tmp_path,
        workers=2,
        timeout_s=10,
        retries=1,
        refresh=False,
    )
    assert first["row_count"] == 2
    root = Path(first["dest_path"])
    responses = sorted((root / "responses").glob("*.csv.gz"))
    assert len(responses) == 2
    assert gzip.decompress(responses[0].read_bytes()).startswith(b"source_id,value\n")
    report = json.loads((root / "product_manifest.json").read_text())
    assert report["upstream_field_count"] == 2
    assert report["omitted_field_count"] == 0
    assert report["responses_fetched"] == 2
    data_calls = len([call for call in calls if "tap_schema.columns" not in call])

    second = acquire.acquire_product(
        program,
        product,
        state_dir=tmp_path,
        workers=2,
        timeout_s=10,
        retries=1,
        refresh=False,
    )
    assert second["sha256"] == first["sha256"]
    assert len([call for call in calls if "tap_schema.columns" not in call]) == data_calls
    resumed = json.loads((root / "product_manifest.json").read_text())
    assert resumed == report


def test_coverage_report_accounts_for_completed_and_pending_products(tmp_path: Path) -> None:
    product_report = tmp_path / "product_manifest.json"
    product_report.write_text(
        json.dumps(
            {
                "field_dispositions": [
                    {"column_name": "source_id"},
                    {"column_name": "value"},
                ]
            }
        ),
        encoding="utf-8",
    )
    program = {
        "program_version": "test.1",
        "products": [
            {
                "product_name": "complete",
                "table": "test.rows",
                "preserve_all_fields": True,
            },
            {
                "product_name": "pending",
                "table": "test.other",
                "preserve_all_fields": True,
            },
        ],
    }
    report = acquire.build_coverage_report(
        program,
        [
            {
                "source_name": "complete",
                "field_disposition_report": str(product_report),
                "row_count": 2,
                "bytes_written": 10,
            }
        ],
    )
    assert report["status"] == "in_progress"
    assert report["summary"] == {
        "expected_products": 2,
        "completed_products": 1,
        "pending_products": 1,
        "completed_rows": 2,
        "completed_bytes": 10,
    }
    assert report["pending"] == ["pending"]
    assert report["table_field_coverage"][1]["selected_field_count"] == 2


def test_manifest_merge_preserves_unrelated_products(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text('[{"source_name":"first","sha256":"old"}]\n')
    merged = acquire.merge_manifest_rows(
        path,
        [
            {"source_name": "first", "sha256": "new"},
            {"source_name": "second", "sha256": "two"},
        ],
    )
    assert merged == [
        {"source_name": "first", "sha256": "new"},
        {"source_name": "second", "sha256": "two"},
    ]
    assert json.loads(path.read_text()) == merged


def test_uncompressed_binary_votable_metadata(tmp_path: Path) -> None:
    path = tmp_path / "rows.vot"
    votable = from_table(Table({"source_id": [1, 2], "value": [3.5, 4.5]}))
    writeto(votable, str(path), tabledata_format="binary")
    assert acquire.response_suffix("votable/b") == ".vot"
    assert acquire.response_metadata(path.read_bytes(), "votable/b") == (
        ["source_id", "value"],
        2,
    )
