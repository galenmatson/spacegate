from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from build_gaia_uncertainty_target_seed import build  # noqa: E402


def test_target_seed_is_deterministic_and_checksum_bounded(tmp_path: Path) -> None:
    typed = tmp_path / "typed"
    typed.mkdir()
    parquet = typed / "uncertain.parquet"
    with duckdb.connect() as con:
        con.execute(
            "copy (select * from (values (30::ubigint),(10::ubigint),(20::ubigint)) "
            "t(source_id)) to ? (format parquet)",
            [str(parquet)],
        )
    parquet_sha = hashlib.sha256(parquet.read_bytes()).hexdigest()
    manifest_path = typed / "typed_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "snapshot_id": "snapshot",
                "content_sha256": "content",
                "tables": [
                    {
                        "source_name": "gaia_dr3_source_uncertain_distance_supplement_v1",
                        "parquet_path": parquet.name,
                        "sha256": parquet_sha,
                        "row_count": 3,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    first = build(manifest_path, tmp_path / "out")
    second = build(manifest_path, tmp_path / "out")
    assert first["build_id"] == second["build_id"]
    artifact = tmp_path / "out" / str(first["build_id"]) / "gaia_dr3_source_ids.json"
    assert json.loads(artifact.read_text())["source_ids"] == ["10", "20", "30"]
    assert hashlib.sha256(artifact.read_bytes()).hexdigest() == first["artifacts"][0][
        "sha256"
    ]
