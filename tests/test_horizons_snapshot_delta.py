from __future__ import annotations

import csv
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from report_horizons_snapshot_delta import compare  # noqa: E402


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_delta_separates_science_from_lineage(tmp_path: Path) -> None:
    old = tmp_path / "old.csv"
    new = tmp_path / "new.csv"
    write_csv(
        old,
        [
            {
                "source_pk": "1",
                "object_name": "Object",
                "eccentricity": "0.1",
                "retrieved_at": "old",
            }
        ],
    )
    write_csv(
        new,
        [
            {
                "source_pk": "1",
                "object_name": "Object",
                "eccentricity": "0.2",
                "retrieved_at": "new",
                "horizons_response_sha256": "a" * 64,
            }
        ],
    )
    report = compare("test", old, new)
    assert report["status"] == "pass"
    assert report["schema_delta"]["added_fields"] == [
        "horizons_response_sha256"
    ]
    assert report["scientific_delta"]["field_change_counts"] == {
        "eccentricity": 1
    }
    assert report["lineage_change_counts"] == {"retrieved_at": 1}
