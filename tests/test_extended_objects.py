from __future__ import annotations

import unittest

import duckdb

from scripts.cook_extended_objects import normalize_designation
from scripts.extended_object_materialization import identity_token, load_relation_stars
from srv.api.app.main import _json_safe_extended_object_ids
from srv.api.app.queries import search_extended_objects
from srv.api.app.utils import normalize_extended_object_query


class ExtendedObjectIdentityTests(unittest.TestCase):
    def test_compact_melotte_designation_normalizes(self) -> None:
        self.assertEqual(normalize_designation("Mel022"), "Melotte 22")
        self.assertEqual(identity_token("Melotte 22"), ("melotte", "22"))

    def test_catalog_identifier_normalization(self) -> None:
        self.assertEqual(normalize_designation("NGC0045"), "NGC 45")
        self.assertEqual(normalize_designation("Sh 2 001"), "Sh 2-1")
        self.assertEqual(identity_token("Barnard 33"), ("barnard", "33"))

    def test_compact_catalog_search_query_normalizes(self) -> None:
        self.assertEqual(normalize_extended_object_query("M45"), "m 45")
        self.assertEqual(normalize_extended_object_query("IC4592"), "ic 4592")
        self.assertEqual(normalize_extended_object_query("LBN1113"), "lbn 1113")


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

    def test_api_serializes_64_bit_object_ids_without_javascript_rounding(self) -> None:
        object_id = 458477943266456492
        payload = _json_safe_extended_object_ids({
            "extended_object_id": object_id,
            "evidence": {"geometry": [{"extended_object_id": object_id}]},
        })
        self.assertEqual(payload["extended_object_id"], str(object_id))
        self.assertEqual(payload["evidence"]["geometry"][0]["extended_object_id"], str(object_id))


class ExtendedObjectRelationTests(unittest.TestCase):
    def test_relation_stars_are_loaded_in_one_typed_index(self) -> None:
        con = duckdb.connect()
        try:
            con.execute(
                """
                create table stars(
                  star_id bigint, system_id bigint, stable_object_key varchar,
                  hd_id bigint, parallax_mas double, parallax_error_mas double,
                  parallax_over_error double, ruwe double, dist_ly double
                )
                """
            )
            con.execute(
                "insert into stars values (1, 10, 'star:1', 145502, 7.2, 0.1, 72, 1.1, 453.0)"
            )
            rows = load_relation_stars(
                con,
                [
                    {"target_namespace": "hd", "target_value": "145502"},
                    {"target_namespace": "name", "target_value": "irrelevant"},
                ],
            )
            self.assertEqual(sorted(rows), [145502])
            self.assertEqual(rows[145502][0][0:4], (1, 10, "star:1", 145502))
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main()
