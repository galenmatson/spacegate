from __future__ import annotations

import unittest

import duckdb

from srv.api.app.planet_categories import (
    parse_planet_categories,
    planet_category_bit_sql,
    planet_category_mask,
)


class PlanetCategoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.con = duckdb.connect(":memory:")
        self.con.execute(
            """
            CREATE TABLE planets (
              radius_earth DOUBLE,
              radius_jup DOUBLE,
              mass_earth DOUBLE,
              mass_jup DOUBLE,
              eq_temp_k DOUBLE,
              insol_earth DOUBLE
            )
            """
        )

    def tearDown(self) -> None:
        self.con.close()

    def category_bit(self, values: tuple[float | None, ...]) -> int:
        self.con.execute("DELETE FROM planets")
        self.con.execute("INSERT INTO planets VALUES (?, ?, ?, ?, ?, ?)", values)
        return int(
            self.con.execute(
                f"SELECT {planet_category_bit_sql('p')} FROM planets p"
            ).fetchone()[0]
        )

    def test_radius_precedes_mass_and_ambiguous_radius_is_unclassified(self) -> None:
        self.assertEqual(self.category_bit((1.5, None, 80.0, None, 350.0, None)), 8)
        self.assertEqual(self.category_bit((3.0, None, 100.0, None, 350.0, None)), 0)
        self.assertEqual(self.category_bit((8.0, None, 2.0, None, 350.0, None)), 1)

    def test_mass_only_policy_preserves_an_unclassified_middle(self) -> None:
        self.assertEqual(self.category_bit((None, None, 10.0, None, 250.0, None)), 16)
        self.assertEqual(self.category_bit((None, None, 30.0, None, 250.0, None)), 0)
        self.assertEqual(self.category_bit((None, None, 50.0, None, 250.0, None)), 2)

    def test_insolation_is_a_temperature_fallback(self) -> None:
        self.assertEqual(self.category_bit((1.0, None, None, None, None, 1.0)), 16)
        self.assertEqual(self.category_bit((1.0, None, None, None, None, 4.0)), 8)
        self.assertEqual(self.category_bit((8.0, None, None, None, None, 0.1)), 4)

    def test_filter_categories_are_deduplicated_and_or_masked(self) -> None:
        categories = parse_planet_categories(
            "hot_jupiter, temperate_terrestrial,hot_jupiter"
        )
        self.assertEqual(categories, ["hot_jupiter", "temperate_terrestrial"])
        self.assertEqual(planet_category_mask(categories), 17)


if __name__ == "__main__":
    unittest.main()
