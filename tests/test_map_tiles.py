from __future__ import annotations

import gzip
import json
import struct
import tempfile
import unittest
from pathlib import Path

import duckdb

from scripts.build_map_tiles import (
    MAGIC,
    RECORD_STRUCT,
    cell_bounds,
    cell_index,
    encode_tile,
    morton3,
    performance_delta,
    performance_token,
    read_profile,
    tile_id,
)
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
            30.0, "G", 1, 8, 1, 1, 5772.0, ["G"], 0,
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
        self.assertEqual(record[-1], 0)
        self.assertEqual(metadata["interest"]["planet_systems"], 1)
        self.assertEqual(gzip.decompress(gzip.compress(raw, mtime=0)), raw)

    def test_verifier_decodes_public_name_from_binary_string_table(self) -> None:
        row = (
            17788193, "canon:system:sol", "Sol", 0.0, 0.0, 0.0, 0.0,
            30.0, "G", 1, 8, 1, 1, 5772.0,
            ["G"], 0, "Sol", "Sol", "Sun",
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
        self.assertEqual(badges, {17788193: ["G"]})

    def test_planet_badge_mask_is_capped_to_six_categories(self) -> None:
        row = (
            17788193, "canon:system:sol", "Sol", 0.0, 0.0, 0.0, 0.0,
            30.0, "G", 1, 8, 1, 1, 5772.0,
            ["G"], 255, "Sol", "Sol", "Sun",
        )
        raw, _ = encode_tile(
            depth=4, x=8, y=8, z=8, rows=[row], exact=True, represented_count=1,
        )
        header_length = struct.unpack_from("<I", raw, 8)[0]
        record = RECORD_STRUCT.unpack_from(raw, 12 + header_length)
        self.assertEqual(record[-1], 63)

    def test_tile_profile_prefers_build_pinned_disc_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state"
            active = state / "config/coolness_profiles/active.json"
            active.parent.mkdir(parents=True)
            active.write_text(
                json.dumps(
                    {
                        "profile_id": "later",
                        "profile_version": "2",
                        "profile_hash": "later-hash",
                    }
                )
            )
            core = root / "core.duckdb"
            disc = root / "disc.duckdb"
            con = duckdb.connect(str(core))
            con.execute("CREATE TABLE build_metadata(key VARCHAR,value VARCHAR)")
            con.execute("INSERT INTO build_metadata VALUES ('build_id','test-build')")
            con.close()
            con = duckdb.connect(str(disc))
            con.execute("CREATE TABLE build_metadata(key VARCHAR,value VARCHAR)")
            con.execute(
                "INSERT INTO build_metadata VALUES "
                "('e6_coolness_profile_id','pinned'),"
                "('e6_coolness_profile_version','1'),"
                "('e6_coolness_profile_hash','pinned-hash')"
            )
            con.execute(
                "CREATE TABLE coolness_scores(profile_id VARCHAR,profile_version VARCHAR)"
            )
            con.execute("INSERT INTO coolness_scores VALUES ('pinned','1')")
            con.close()
            con = duckdb.connect(str(core), read_only=True)
            con.execute(f"ATTACH '{disc}' AS disc_db (READ_ONLY)")
            try:
                profile = read_profile(state, con)
            finally:
                con.close()
        self.assertEqual(profile["profile_id"], "pinned")
        self.assertEqual(profile["profile_version"], "1")
        self.assertEqual(profile["profile_hash"], "pinned-hash")

    def test_performance_phase_includes_resource_and_output_accounting(self) -> None:
        token = performance_token()
        phase = performance_delta(
            "radius_100_tiles",
            token,
            output_bytes=42,
            details={"radius_ly": 100},
        )
        self.assertEqual(phase["name"], "radius_100_tiles")
        self.assertGreaterEqual(phase["wall_seconds"], 0)
        self.assertGreaterEqual(phase["cpu_seconds"], 0)
        self.assertGreater(phase["peak_rss_kib"], 0)
        self.assertEqual(phase["output_bytes"], 42)
        self.assertEqual(phase["details"], {"radius_ly": 100})


if __name__ == "__main__":
    unittest.main()
