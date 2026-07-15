from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from rebuild_side_artifacts import resolve_canonical_evidence_arm  # noqa: E402


class RebuildSideArtifactsTest(unittest.TestCase):
    def test_sliced_core_resolves_full_canonical_arm(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp)
            canonical_arm = state / "out" / "canonical-v1" / "arm.duckdb"
            canonical_arm.parent.mkdir(parents=True)
            duckdb.connect(str(canonical_arm)).close()

            sliced_core = state / "slice" / "core.duckdb"
            sliced_core.parent.mkdir(parents=True)
            con = duckdb.connect(str(sliced_core))
            con.execute("create table build_metadata(key varchar, value varchar)")
            con.executemany(
                "insert into build_metadata values (?, ?)",
                [
                    ("slice_profile_id", "core.public"),
                    ("bootstrap_source_build_id", "canonical-v1"),
                ],
            )
            con.close()

            self.assertEqual(
                resolve_canonical_evidence_arm(state=state, core_db=sliced_core),
                canonical_arm.resolve(),
            )

    def test_sliced_core_without_canonical_lineage_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp)
            sliced_core = state / "core.duckdb"
            con = duckdb.connect(str(sliced_core))
            con.execute("create table build_metadata(key varchar, value varchar)")
            con.execute("insert into build_metadata values ('slice_profile_id', 'core.public')")
            con.close()

            with self.assertRaises(SystemExit):
                resolve_canonical_evidence_arm(state=state, core_db=sliced_core)


if __name__ == "__main__":
    unittest.main()
