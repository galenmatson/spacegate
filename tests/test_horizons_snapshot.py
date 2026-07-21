from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from horizons_snapshot import (  # noqa: E402
    ResponseCapture,
    seed_sha256,
    sha256_file,
    tree_sha256,
    write_horizons_snapshot,
)


def test_horizons_snapshot_preserves_raw_response_and_atomic_projection(
    tmp_path: Path,
) -> None:
    collector = tmp_path / "collector.py"
    collector.write_text("# pinned collector\n", encoding="utf-8")
    targets = [
        {
            "source_pk": 1,
            "name": "Example",
            "command": "123",
            "center": "500@10",
        }
    ]
    capture = ResponseCapture(
        source_pk="1",
        object_name="Example",
        horizons_command="123",
        center_code="500@10",
        query_url="https://example.invalid/?COMMAND=123",
        query_parameters={"COMMAND": "'123'", "CENTER": "'500@10'"},
        payload=b"Target body name: Example\r\n$$SOE\r\nrow\r\n$$EOE\r\n",
    )
    row = {
        "source_pk": "1",
        "object_name": "Example",
        "horizons_response_path": capture.response_path,
        "horizons_response_sha256": capture.response_sha256,
    }
    kwargs = {
        "state_dir": tmp_path,
        "family": "sol_test",
        "table_source_name": "sol_test_objects",
        "response_source_name": "sol_test_horizons_responses",
        "parsed_filename": "objects.csv",
        "legacy_relative_path": "raw/sol_test/objects.csv",
        "manifest_filename": "sol_test_manifest.json",
        "source_version": "test_v1",
        "source_url": "https://example.invalid/",
        "retrieved_at": "2026-07-21T00:00:00Z",
        "rows": [row],
        "fieldnames": list(row),
        "captures": [capture],
        "seed_version": "seed_v1",
        "targets": targets,
        "collector_path": collector,
        "query_signature": {"epoch": "test"},
    }
    first, manifest = write_horizons_snapshot(**kwargs)
    second, repeated_manifest = write_horizons_snapshot(**kwargs)

    assert first == second
    assert manifest == repeated_manifest
    assert (first / "source" / capture.response_path).read_bytes() == capture.payload
    assert sha256_file(first / "source" / capture.response_path) == capture.response_sha256
    assert tree_sha256(first / "source") == manifest[1]["sha256"]
    assert (tmp_path / "raw/sol_test/objects.csv").read_bytes() == (
        first / "extracted/objects.csv"
    ).read_bytes()
    assert json.loads((first / "source/targets.json").read_text())["targets"] == targets
    assert (
        json.loads((first / "source/snapshot_metadata.json").read_text())[
            "operator_seed_sha256"
        ]
        == seed_sha256("seed_v1", targets)
    )
    written_manifest = json.loads(
        (tmp_path / "reports/manifests/sol_test_manifest.json").read_text()
    )
    assert [row["source_name"] for row in written_manifest] == [
        "sol_test_objects",
        "sol_test_horizons_responses",
    ]
