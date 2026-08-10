from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import fetch_gaia_dr2_identity as forward_module  # noqa: E402
import fetch_gaia_dr2_identity_reverse as reverse_module  # noqa: E402
from fetch_gaia_dr2_identity import target_ids, validate_tap_csv  # noqa: E402
from fetch_gaia_dr2_identity_reverse import (  # noqa: E402
    build_reverse_target_set,
    default_forward_chunks,
    reverse_target_ids,
)


def test_validate_tap_csv_preserves_all_official_neighbour_rows() -> None:
    payload = (
        "dr2_source_id,dr3_source_id,angular_distance,magnitude_difference,"
        "proper_motion_propagation\n"
        "10,20,0.1,0.2,true\n"
        "10,21,0.2,0.3,true\n"
    ).encode()
    assert validate_tap_csv(payload) == 2


def test_validate_tap_csv_rejects_error_documents_and_schema_drift() -> None:
    with pytest.raises(ValueError, match="error document"):
        validate_tap_csv(b"<VOTABLE><INFO name='QUERY_STATUS'>ERROR</INFO></VOTABLE>")
    with pytest.raises(ValueError, match="schema drift"):
        validate_tap_csv(b"dr2_source_id,dr3_source_id\n10,20\n")


def test_target_ids_require_unique_numeric_order(tmp_path: Path) -> None:
    target = tmp_path / "targets.csv"
    target.write_text(
        "dr2_source_id,source_families,source_family_count,source_record_count\n"
        "10,a,1,1\n20,b,1,1\n",
        encoding="utf-8",
    )
    assert target_ids(target) == ["10", "20"]
    target.write_text(
        "dr2_source_id,source_families,source_family_count,source_record_count\n"
        "20,b,1,1\n10,a,1,1\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="not unique and numerically ordered"):
        target_ids(target)


def test_reverse_target_set_is_deduplicated_and_ordered(tmp_path: Path) -> None:
    chunks = tmp_path / "chunks"
    chunks.mkdir()
    header = (
        "dr2_source_id,dr3_source_id,angular_distance,magnitude_difference,"
        "proper_motion_propagation\n"
    )
    (chunks / "part_00000.csv").write_text(
        header + "10,200,0.1,0.2,true\n11,100,0.2,0.3,false\n",
        encoding="utf-8",
    )
    (chunks / "part_00001.csv").write_text(
        header + "12,200,0.3,0.4,true\n10,200,0.1,0.2,true\n",
        encoding="utf-8",
    )
    output = tmp_path / "targets.csv"

    report = build_reverse_target_set(chunks, output)

    assert report["target_count"] == 2
    assert report["forward_pair_count"] == 4
    assert reverse_target_ids(output) == ["100", "200"]
    assert output.read_text(encoding="utf-8").splitlines() == [
        "dr3_source_id,forward_dr2_source_count,forward_pair_count",
        "100,1,1",
        "200,2,3",
    ]


def test_reverse_target_ids_require_unique_numeric_order(tmp_path: Path) -> None:
    target = tmp_path / "targets.csv"
    target.write_text(
        "dr3_source_id,forward_dr2_source_count,forward_pair_count\n"
        "200,1,1\n100,1,1\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="not unique and numerically ordered"):
        reverse_target_ids(target)


def test_reverse_collector_discovers_forward_snapshot_from_manifest(tmp_path: Path) -> None:
    chunks = tmp_path / "raw" / "gaia" / "snapshot" / "chunks"
    chunks.mkdir(parents=True)
    manifests = tmp_path / "reports" / "manifests"
    manifests.mkdir(parents=True)
    (manifests / "gaia_dr2_identity_manifest.json").write_text(
        "["
        '{"source_name":"gaia_dr2_identity_target_set","dest_path":"raw/targets.csv"},'
        '{"source_name":"gaia_dr2_neighbourhood_union",'
        '"dest_path":"raw/gaia/snapshot/chunks"}'
        "]",
        encoding="utf-8",
    )

    assert default_forward_chunks(tmp_path) == chunks


@pytest.mark.parametrize(
    ("module", "snapshot_parent", "snapshot_prefix", "builder_name"),
    [
        (
            forward_module,
            "gaia_dr2_identity",
            "target_union_",
            "build_target_set",
        ),
        (
            reverse_module,
            "gaia_dr2_identity_reverse",
            "reverse_union_",
            "build_reverse_target_set",
        ),
    ],
)
def test_existing_snapshot_removes_regenerated_staging_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    module: object,
    snapshot_parent: str,
    snapshot_prefix: str,
    builder_name: str,
) -> None:
    target_hash = "a" * 64
    final = (
        tmp_path
        / "raw"
        / snapshot_parent
        / "snapshots"
        / f"{snapshot_prefix}{target_hash[:16]}"
    )
    final.mkdir(parents=True)
    (final / "snapshot_report.json").write_text("{}", encoding="utf-8")

    def build_staging(_: Path, output: Path) -> dict[str, str]:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text("generated staging\n", encoding="utf-8")
        return {"target_set_sha256": target_hash}

    monkeypatch.setattr(module, builder_name, build_staging)
    monkeypatch.setattr(module, "publish_manifest", lambda *_: None)
    if module is forward_module:
        args = SimpleNamespace(
            state_dir=tmp_path,
            typed_report=tmp_path / "typed.json",
            chunk_size=10_000,
            workers=1,
            timeout_s=1,
            retries=1,
            max_records=1,
            targets_only=False,
        )
        staging = tmp_path / "tmp" / "gaia_dr2_identity_targets.csv"
    else:
        args = SimpleNamespace(
            state_dir=tmp_path,
            forward_chunks=tmp_path / "chunks",
            chunk_size=10_000,
            workers=1,
            timeout_s=1,
            retries=1,
            max_records=1,
            targets_only=False,
        )
        staging = tmp_path / "tmp" / "gaia_dr3_identity_targets.csv"
    monkeypatch.setattr(module, "parse_args", lambda: args)

    assert module.main() == 0
    assert not staging.exists()
