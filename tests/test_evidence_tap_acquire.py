from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import sys
from pathlib import Path

from astropy.io.votable import from_table, writeto
from astropy.table import Table


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import evidence_tap_acquire as acquire  # noqa: E402


def test_gaia_derived_products_cover_both_envelope_branches() -> None:
    program = json.loads(
        (ROOT / "config" / "evidence_lake" / "e3_acquisition_program.json").read_text()
    )
    products = {product["product_name"]: product for product in program["products"]}
    pairs = {
        "gaia_dr3_ap_classifier_v2": "gaia_dr3_ap_classifier_uncertain_distance_supplement_v1",
        "gaia_dr3_ap_photometry_flame_v2": "gaia_dr3_ap_photometry_flame_uncertain_distance_supplement_v1",
        "gaia_dr3_ap_spectroscopy_v2": "gaia_dr3_ap_spectroscopy_uncertain_distance_supplement_v1",
        "gaia_dr3_ap_activity_specialized_v2": "gaia_dr3_ap_activity_specialized_uncertain_distance_supplement_v1",
        "gaia_dr3_ap_multiple_oa_v2": "gaia_dr3_ap_multiple_oa_uncertain_distance_supplement_v1",
        "gaia_dr3_ap_supp_photometric_models_v2": "gaia_dr3_ap_supp_photometric_models_uncertain_distance_supplement_v1",
        "gaia_dr3_ap_supp_spectroscopic_models_v2": "gaia_dr3_ap_supp_spectroscopic_models_uncertain_distance_supplement_v1",
        "gaia_dr3_nss_two_body_orbit_full_v2": "gaia_dr3_nss_two_body_orbit_uncertain_distance_supplement_v1",
        "gaia_dr3_variability_summary_v2": "gaia_dr3_variability_summary_uncertain_distance_supplement_v1",
        "gaia_dr3_rotation_modulation_v2": "gaia_dr3_rotation_modulation_uncertain_distance_supplement_v1",
        "gaia_dr3_allwise_best_neighbour_v1": "gaia_dr3_allwise_best_neighbour_uncertain_distance_supplement_v1",
        "gaia_dr3_tmass_best_neighbour_v1": "gaia_dr3_tmass_best_neighbour_uncertain_distance_supplement_v1",
        "gaia_dr3_hipparcos2_best_neighbour_v1": "gaia_dr3_hipparcos2_best_neighbour_uncertain_distance_supplement_v1",
        "gaia_dr3_tycho2_best_neighbour_v1": "gaia_dr3_tycho2_best_neighbour_uncertain_distance_supplement_v1",
        "gaia_dr3_ravedr6_best_neighbour_v1": "gaia_dr3_ravedr6_best_neighbour_uncertain_distance_supplement_v1",
    }
    field_contract_keys = {
        "source_id",
        "release_id",
        "endpoint",
        "table",
        "table_alias",
        "include_fields",
        "include_prefixes",
        "include_contains",
        "preserve_all_fields",
        "partition_expression",
        "max_rec",
        "unselected_field_reason",
    }
    for hard_name, supplement_name in pairs.items():
        hard = products[hard_name]
        supplement = products[supplement_name]
        assert "parallax >= 2.609272" in hard["where"]
        assert "external.gaiaedr3_distance bj" in supplement["from"]
        assert "parallax < 2.60927200 or" in supplement["where"]
        assert "bj.r_med_geo <= 383.245 or bj.r_lo_geo <= 306.601" in supplement["where"]
        for key in field_contract_keys:
            assert hard.get(key) == supplement.get(key), (hard_name, supplement_name, key)


def test_simbad_ordering_uses_unqualified_output_fields() -> None:
    program = json.loads(
        (ROOT / "config" / "evidence_lake" / "e3_acquisition_program.json").read_text()
    )
    products = {
        product["product_name"]: product
        for product in program["products"]
        if product["product_name"].startswith("simbad_gaia_envelope_")
    }
    assert {
        name: product["order_by"] for name, product in products.items()
    } == {
        "simbad_gaia_envelope_basic_supplement_v1": "oid",
        "simbad_gaia_envelope_identifier_supplement_v1": "oidref, id",
        "simbad_gaia_envelope_bibliography_supplement_v1": "oidref, oidbibref",
    }


def test_target_values_are_checksum_bounded_and_part_of_query_identity(
    tmp_path: Path,
) -> None:
    seed = tmp_path / "derived" / "targets" / "build"
    seed.mkdir(parents=True)
    artifact = seed / "missing.json"
    artifact.write_text('{"ids":[20,10,20]}\n', encoding="utf-8")
    artifact_sha256 = hashlib.sha256(artifact.read_bytes()).hexdigest()
    manifest = seed / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "build_id": "build",
                "artifacts": [
                    {"path": "missing.json", "sha256": artifact_sha256}
                ],
                "report": {"coverage": "hard_envelope_only"},
            }
        ),
        encoding="utf-8",
    )
    product = {
        "where": "quality = 1",
        "target_values": {
            "manifest_relpath": "derived/targets/build/manifest.json",
            "build_id": "build",
            "artifact": "missing.json",
            "json_path": ["ids"],
            "coverage": "hard_envelope_only",
            "field_expression": "rows.oid",
            "max_values": 10,
        },
    }
    resolved = acquire.resolve_target_values(product, tmp_path)
    assert resolved["where"] == "(quality = 1) and (rows.oid in (10,20))"
    assert resolved["target_values_lineage"]["value_count"] == 2
    assert resolved["target_values_lineage"]["artifact_sha256"] == artifact_sha256

    artifact.write_text('{"ids":[10]}\n', encoding="utf-8")
    try:
        acquire.resolve_target_values(product, tmp_path)
    except ValueError as error:
        assert "checksum mismatch" in str(error)
    else:
        raise AssertionError("modified target seed was accepted")


def test_target_values_can_be_partitioned_into_bounded_bucket_queries(
    tmp_path: Path,
) -> None:
    seed = tmp_path / "derived" / "targets" / "partitioned"
    seed.mkdir(parents=True)
    artifact = seed / "missing.json"
    artifact.write_text('{"ids":[1,2,3,9,10]}\n', encoding="utf-8")
    artifact_sha256 = hashlib.sha256(artifact.read_bytes()).hexdigest()
    manifest = seed / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "build_id": "partitioned",
                "artifacts": [
                    {"path": "missing.json", "sha256": artifact_sha256}
                ],
                "report": {"coverage": "complete"},
            }
        ),
        encoding="utf-8",
    )
    product = {
        "select": ["rows.oid"],
        "table": "test.rows",
        "where": "quality = 1",
        "partition_expression": "rows.oid",
        "buckets": 3,
        "target_values": {
            "manifest_relpath": "derived/targets/partitioned/manifest.json",
            "build_id": "partitioned",
            "artifact": "missing.json",
            "json_path": ["ids"],
            "coverage": "complete",
            "field_expression": "rows.oid",
            "max_values": 10,
            "partition_values_by_bucket": True,
        },
    }

    resolved = acquire.resolve_target_values(product, tmp_path)
    assert resolved["where"] == "quality = 1"
    assert resolved["target_values_by_bucket"] == [["3", "9"], ["1", "10"], ["2"]]
    assert resolved["target_values_lineage"] == {
        "build_id": "partitioned",
        "manifest_relpath": "derived/targets/partitioned/manifest.json",
        "artifact": "missing.json",
        "artifact_sha256": artifact_sha256,
        "coverage": "complete",
        "field_expression": "rows.oid",
        "value_count": 5,
        "values_sha256": acquire.stable_hash(["1", "10", "2", "3", "9"]),
        "partition_policy": "unsigned_integer_modulo_product_bucket_v1",
        "bucket_count": 3,
        "nonempty_bucket_count": 3,
        "min_bucket_value_count": 1,
        "max_bucket_value_count": 2,
    }
    queries = [acquire.render_query(resolved, bucket) for bucket in range(3)]
    assert "rows.oid in (3,9)" in queries[0]
    assert "rows.oid in (1,10)" in queries[1]
    assert "rows.oid in (2)" in queries[2]
    for bucket, query in enumerate(queries):
        assert f"mod(rows.oid, 3) = {bucket}" in query


def test_partitioned_target_values_make_empty_buckets_explicit(tmp_path: Path) -> None:
    seed = tmp_path / "derived" / "targets" / "sparse"
    seed.mkdir(parents=True)
    artifact = seed / "missing.json"
    artifact.write_text('{"ids":[8]}\n', encoding="utf-8")
    artifact_sha256 = hashlib.sha256(artifact.read_bytes()).hexdigest()
    (seed / "manifest.json").write_text(
        json.dumps(
            {
                "build_id": "sparse",
                "artifacts": [
                    {"path": "missing.json", "sha256": artifact_sha256}
                ],
                "report": {"coverage": "complete"},
            }
        ),
        encoding="utf-8",
    )
    product = {
        "select": ["rows.oid"],
        "table": "test.rows",
        "where": "1=1",
        "partition_expression": "rows.oid",
        "buckets": 3,
        "target_values": {
            "manifest_relpath": "derived/targets/sparse/manifest.json",
            "build_id": "sparse",
            "artifact": "missing.json",
            "json_path": ["ids"],
            "coverage": "complete",
            "field_expression": "rows.oid",
            "partition_values_by_bucket": True,
        },
    }
    resolved = acquire.resolve_target_values(product, tmp_path)
    assert "and (1=0)" in acquire.render_query(resolved, 0)
    assert "and (1=0)" in acquire.render_query(resolved, 1)
    assert "rows.oid in (8)" in acquire.render_query(resolved, 2)
    assert resolved["target_values_lineage"]["nonempty_bucket_count"] == 1
    assert resolved["target_values_lineage"]["min_bucket_value_count"] == 0


def test_untargeted_product_identity_remains_backward_compatible() -> None:
    program = {"schema_version": "spacegate.e3_acquisition_program.v1"}
    product = {
        "source_id": "test.source",
        "release_id": "r1",
        "endpoint": "https://example.test/tap/sync",
        "table": "test.rows",
        "select": ["source_id", "measurement"],
        "where": "measurement is not null",
        "partition_expression": "source_id",
        "buckets": 7,
        "max_rec": 1000,
    }
    legacy_identity = acquire.stable_hash(
        {
            "program_contract": program["schema_version"],
            "engine_version": acquire.ENGINE_VERSION,
            "source_id": product["source_id"],
            "release_id": product["release_id"],
            "endpoint": product["endpoint"],
            "table": product["table"],
            "select": product["select"],
            "from": product["table"],
            "where": product["where"],
            "partition_expression": product["partition_expression"],
            "order_by": product["partition_expression"],
            "ordered": True,
            "buckets": product["buckets"],
            "max_rec": product["max_rec"],
            "tap_mode": "sync",
            "output_format": "csv",
        }
    )[:24]
    assert acquire.product_identity(program, product) == legacy_identity
    assert acquire.product_identity(
        program, {**product, "target_values_lineage": None}
    ) == legacy_identity
    assert acquire.product_identity(
        program,
        {**product, "target_values_lineage": {"build_id": "target-build"}},
    ) != legacy_identity


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


def test_async_timeout_aborts_job_before_failing(monkeypatch, tmp_path: Path) -> None:
    opened: list[tuple[str, bytes | None]] = []

    class AsyncResponse(FakeResponse):
        def __init__(self, payload: bytes, url: str) -> None:
            super().__init__(payload)
            self.url = url

        def geturl(self) -> str:
            return self.url

    job_url = "https://example.test/tap/async/job-1"

    def fake_urlopen(request, timeout):
        opened.append((request.full_url, request.data))
        if request.full_url.endswith("/async"):
            return AsyncResponse(b"", job_url)
        if request.full_url.endswith("/phase") and request.data is not None:
            return AsyncResponse(b"", request.full_url)
        raise AssertionError(request.full_url)

    times = iter([0.0, 2.0])
    monkeypatch.setattr(acquire.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(acquire, "read_url", lambda *_args, **_kwargs: b"EXECUTING")
    monkeypatch.setattr(acquire.time, "monotonic", lambda: next(times))
    status_path = tmp_path / "job.uws.json"

    try:
        acquire.tap_async_request(
            "https://example.test/tap/sync",
            "select source_id from test.rows",
            timeout_s=1,
            read_stall_timeout_s=1,
            retries=1,
            max_rec=10,
            status_path=status_path,
        )
    except RuntimeError as error:
        assert "exceeded 1s" in str(error)
    else:
        raise AssertionError("timed-out async job was accepted")

    assert opened[-1] == (job_url + "/phase", b"PHASE=ABORT")
    status = json.loads(status_path.read_text())
    assert status["schema_version"] == "spacegate.tap_uws_lineage.v1"
    assert status["attempts"][0]["cleanup"]["status"] == "pass"


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
            },
            {
                "source_name": "retired",
                "field_disposition_report": str(product_report),
                "row_count": 99,
                "bytes_written": 999,
            },
        ],
    )
    assert report["status"] == "in_progress"
    assert report["summary"] == {
        "expected_products": 2,
        "completed_products": 1,
        "pending_products": 1,
        "retained_superseded_products": 1,
        "completed_rows": 2,
        "completed_bytes": 10,
    }
    assert report["pending"] == ["pending"]
    assert report["retained_superseded"] == ["retired"]
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


def test_progress_publish_uses_durable_merged_manifest(tmp_path: Path) -> None:
    product_report = tmp_path / "product_report.json"
    product_report.write_text(
        json.dumps(
            {
                "field_dispositions": [
                    {"column_name": "source_id"},
                    {"column_name": "measurement"},
                ]
            }
        )
    )
    program = {
        "schema_version": "spacegate.e3_acquisition_program.v1",
        "program_version": "test.1",
        "manifest_name": "test_manifest.json",
        "products": [
            {
                "product_name": "complete",
                "table": "test.rows",
                "preserve_all_fields": True,
            }
        ],
    }
    manifest = tmp_path / "reports" / "manifests" / "test_manifest.json"
    acquire.merge_manifest_rows(
        manifest,
        [
            {
                "source_name": "complete",
                "field_disposition_report": str(product_report),
                "row_count": 2,
                "bytes_written": 10,
            },
            {
                "source_name": "retired",
                "field_disposition_report": str(product_report),
                "row_count": 99,
                "bytes_written": 999,
            },
        ],
    )
    report = acquire.publish_acquisition_progress(program, tmp_path)
    assert report["status"] == "pass"
    assert report["summary"] == {
        "product_count": 1,
        "row_count": 2,
        "bytes": 10,
        "pending_product_count": 0,
        "retained_superseded_product_count": 1,
    }
    assert [row["source_name"] for row in report["products"]] == ["complete"]
    assert [
        row["source_name"] for row in report["retained_superseded_products"]
    ] == ["retired"]
    assert json.loads(
        (tmp_path / "reports" / "evidence_lake_v2" / "e3_acquisition_report.json").read_text()
    ) == report


def test_uncompressed_binary_votable_metadata(tmp_path: Path) -> None:
    path = tmp_path / "rows.vot"
    votable = from_table(Table({"source_id": [1, 2], "value": [3.5, 4.5]}))
    writeto(votable, str(path), tabledata_format="binary")
    assert acquire.response_suffix("votable/b") == ".vot"
    assert acquire.response_metadata(path.read_bytes(), "votable/b") == (
        ["source_id", "value"],
        2,
    )
