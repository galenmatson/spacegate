from __future__ import annotations

import gzip
import json
import struct
import tempfile
import unittest
from pathlib import Path

from scripts.build_map_tiles import MAGIC, RECORD_STRUCT, cell_bounds, cell_index, encode_tile, morton3, tile_id
from scripts.verify_map_tiles import read_tile


class MapTileContractTests(unittest.TestCase):
    def test_boundary_policy_is_half_open_and_clamped(self) -> None:
        self.assertEqual(cell_index(-1024.0, 4), 0)
        self.assertEqual(cell_index(0.0, 4), 8)
        self.assertEqual(cell_index(1024.0, 4), 15)
        minimum, maximum, center = cell_bounds(4, 8, 8, 8)
        self.assertEqual(minimum, [0.0, 0.0, 0.0])
        self.assertEqual(maximum, [128.0, 128.0, 128.0])
        self.assertEqual(center, [64.0, 64.0, 64.0])

    def test_morton_and_tile_ids_are_stable(self) -> None:
        self.assertEqual(morton3(1, 0, 0, 1), 1)
        self.assertEqual(morton3(0, 1, 0, 1), 2)
        self.assertEqual(morton3(0, 0, 1, 1), 4)
        self.assertEqual(tile_id(4, 8, 8, 8), "d4-e00")

    def test_binary_tile_preserves_identity_and_cell_relative_position(self) -> None:
        row = (
            17788193, "canon:system:sol", "Sol", 0.0, 0.0, 0.0, 0.0,
            30.0, "G", 1, 8, 1, 1, 5772.0, '["G"]',
            "Sol", "Sol", "Sun",
        )
        raw, metadata = encode_tile(depth=4, x=8, y=8, z=8, rows=[row], exact=True, represented_count=1)
        self.assertEqual(raw[:8], MAGIC)
        header_length = struct.unpack_from("<I", raw, 8)[0]
        header = json.loads(raw[12:12 + header_length])
        self.assertEqual(header["emitted_count"], 1)
        record = RECORD_STRUCT.unpack_from(raw, 12 + header_length)
        self.assertEqual(record[0], 17788193)
        self.assertEqual(record[1:4], (-64.0, -64.0, -64.0))
        self.assertEqual(metadata["interest"]["planet_systems"], 1)
        self.assertEqual(gzip.decompress(gzip.compress(raw, mtime=0)), raw)

    def test_verifier_decodes_public_name_from_binary_string_table(self) -> None:
        row = (
            17788193, "canon:system:sol", "Sol", 0.0, 0.0, 0.0, 0.0,
            30.0, "G", 1, 8, 1, 1, 5772.0,
            '["G"]', "Sol", "Sol", "Sun",
        )
        raw, _ = encode_tile(
            depth=4, x=8, y=8, z=8, rows=[row], exact=True, represented_count=1,
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tile.sgtile.gz"
            path.write_bytes(gzip.compress(raw, mtime=0))
            header, names, _, badges = read_tile(path)
        self.assertEqual(header["emitted_count"], 1)
        self.assertEqual(names, {17788193: "Sol"})
        self.assertEqual(badges, {17788193: ["G", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"]})


if __name__ == "__main__":
    unittest.main()
