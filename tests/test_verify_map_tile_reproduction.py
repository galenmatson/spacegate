from __future__ import annotations

import json
from pathlib import Path

from scripts.verify_map_tile_reproduction import compare


def write_tree(root: Path, *, generated_at: str, tile_payload: bytes = b"tile") -> None:
    root.mkdir(parents=True)
    (root / "index.json").write_text(
        json.dumps({"build_id": "build", "generated_at": generated_at, "public_radii_ly": [100]})
    )
    radius = root / "radius-100"
    (radius / "tiles").mkdir(parents=True)
    (radius / "manifest.json").write_text(
        json.dumps({"generated_at": generated_at, "manifest_sha256": "deterministic"})
    )
    (radius / "tiles" / "payload.sgtile.gz").write_bytes(tile_payload)


def test_reproduction_ignores_timestamps_but_checks_tile_bytes(tmp_path: Path) -> None:
    reference = tmp_path / "reference"
    reproduction = tmp_path / "reproduction"
    write_tree(reference, generated_at="first")
    write_tree(reproduction, generated_at="second")
    assert compare(reference, reproduction, [100])["passed"]

    (reproduction / "radius-100/tiles/payload.sgtile.gz").write_bytes(b"changed")
    report = compare(reference, reproduction, [100])
    assert not report["passed"]
    assert report["radius_results"]["100"]["differing_tiles"] == [
        "payload.sgtile.gz"
    ]
