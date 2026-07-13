from __future__ import annotations

import unittest

import duckdb

from scripts.cook_extended_objects import normalize_designation
from scripts.extended_object_materialization import identity_token
from srv.api.app.queries import search_extended_objects


class ExtendedObjectIdentityTests(unittest.TestCase):
    def test_compact_melotte_designation_normalizes(self) -> None:
        self.assertEqual(normalize_designation("Mel022"), "Melotte 22")
        self.assertEqual(identity_token("Melotte 22"), ("melotte", "22"))

    def test_catalog_identifier_normalization(self) -> None:
        self.assertEqual(normalize_designation("NGC0045"), "NGC 45")
        self.assertEqual(normalize_designation("Sh 2 001"), "Sh 2-1")
        self.assertEqual(identity_token("Barnard 33"), ("barnard", "33"))


class ExtendedObjectSearchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.con = duckdb.connect()
        self.con.execute(
            """
            create table extended_objects(
              extended_object_id bigint, display_name varchar, object_family varchar,
              object_type varchar, map_domain varchar, dist_ly double
            )
            """
        )
        self.con.execute(
            """
            insert into extended_objects values
              (33, 'Barnard 33', 'nebula', 'dark_nebula', 'sky_only', null),
              (330, 'Barnard 330', 'nebula', 'dark_nebula', 'sky_only', null)
            """
        )
        self.con.execute(
            """
            create table extended_object_search_terms(
              extended_object_id bigint, term_norm varchar
            )
            """
        )
        self.con.execute(
            "insert into extended_object_search_terms values (33, 'barnard 33'), (330, 'barnard 330')"
        )

    def tearDown(self) -> None:
        self.con.close()

    def test_exact_match_ranks_before_prefix_match(self) -> None:
        rows = search_extended_objects(
            self.con,
            q_norm="barnard 33",
            object_family=None,
            object_type=None,
            map_domain=None,
            max_dist_ly=None,
            limit=10,
        )
        self.assertEqual([row["extended_object_id"] for row in rows], [33, 330])


if __name__ == "__main__":
    unittest.main()
