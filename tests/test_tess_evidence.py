from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from fetch_tess_evidence import (  # noqa: E402
    GAIA_DR3_FIELDS,
    GAIA_EXTERNAL_FIELDS,
    NEIGHBOUR_FIELDS,
    TIC_COOKED_FIELDS,
    TOI_COOKED_FIELDS,
    TOI_HISTORY_FIELDS,
    parse_tic_id,
    update_disposition_history,
)
from ingest.emit_canonical_build import remap_object_identifiers  # noqa: E402
from tess_evidence_materialization import materialize_arm, materialize_core  # noqa: E402


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


class TessEvidenceTest(unittest.TestCase):
    def test_parse_tic_id(self) -> None:
        self.assertEqual(parse_tic_id("TIC 001234"), "1234")
        self.assertEqual(parse_tic_id(1234), "1234")
        self.assertIsNone(parse_tic_id(""))

    def test_disposition_history_is_append_only_by_event(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "history.csv"
            base = {field: "" for field in TOI_COOKED_FIELDS}
            first = dict(base, source_key="TOI-1.01", tic_id="1", toi_display="TOI-1.01",
                         disposition="PC", row_updated_at="2026-01-01", source_row_hash="a")
            update_disposition_history(path, [first], observed_at="2026-01-02T00:00:00Z")
            update_disposition_history(path, [first], observed_at="2026-01-03T00:00:00Z")
            changed = dict(first, disposition="CP", row_updated_at="2026-02-01", source_row_hash="b")
            update_disposition_history(path, [changed], observed_at="2026-02-02T00:00:00Z")
            with path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual([row["disposition"] for row in rows], ["PC", "CP"])
            self.assertEqual(rows[0]["first_observed_at"], "2026-01-02T00:00:00Z")
            self.assertEqual(rows[0]["last_observed_at"], "2026-01-03T00:00:00Z")

    def test_canonical_emission_remaps_and_deduplicates_object_identifiers(self) -> None:
        con = duckdb.connect(":memory:")
        con.execute(
            """
            create table systems (system_id bigint);
            create table stars (star_id bigint);
            create table planets (planet_id bigint);
            insert into systems values (10);
            insert into stars values (100);
            insert into planets values (1000);
            create temp table original_system_to_preview (original_system_id bigint, system_id bigint);
            create temp table original_star_to_preview (original_star_id bigint, star_id bigint);
            create temp table original_planet_to_preview (original_planet_id bigint, planet_id bigint);
            insert into original_system_to_preview values (1, 10);
            insert into original_star_to_preview values (2, 100), (3, 100);
            insert into original_planet_to_preview values (4, 1000);
            create table object_identifiers (
              identifier_id bigint, target_type varchar, target_id bigint, namespace varchar,
              id_value_raw varchar, id_value_norm varchar, is_canonical boolean,
              resolution_method varchar, resolution_confidence double, source_catalog varchar,
              source_version varchar, source_pk bigint, evidence_json varchar
            );
            insert into object_identifiers values
              (1, 'system', 1, 'hip', '1', '1', true, 'fixture', 1.0, 'fixture', 'v1', 1, '{}'),
              (2, 'star', 2, 'tic', '42', '42', false, 'weak', 0.8, 'z_source', 'v1', 2, '{}'),
              (3, 'star', 3, 'tic', '42', '42', false, 'strong', 0.98, 'a_source', 'v1', 3, '{}'),
              (4, 'planet', 4, 'toi', '1.01', '1.01', false, 'fixture', 1.0, 'fixture', 'v1', 4, '{}'),
              (5, 'star', 999, 'tic', '99', '99', false, 'orphan', 1.0, 'fixture', 'v1', 5, '{}');
            """
        )
        remap_object_identifiers(con)
        self.assertEqual(
            con.execute(
                "select target_type, target_id, namespace, id_value_norm "
                "from object_identifiers order by target_type"
            ).fetchall(),
            [
                ("planet", 1000, "toi", "1.01"),
                ("star", 100, "tic", "42"),
                ("system", 10, "hip", "1"),
            ],
        )
        self.assertEqual(
            con.execute("select resolution_method from object_identifiers where namespace='tic'").fetchone()[0],
            "strong",
        )
        self.assertEqual(
            con.execute("select list(identifier_id order by identifier_id) from object_identifiers").fetchone()[0],
            [1, 2, 3],
        )
        con.close()

    def test_core_and_arm_materialization_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cooked = root / "cooked"
            manifest_path = root / "manifest.json"
            core_path = root / "core.duckdb"
            arm_path = root / "arm.duckdb"
            self._write_fixture_inputs(cooked)
            manifest_path.write_text(json.dumps([
                {"source_name": "mast_tic_targeted", "source_version": "fixture_tic",
                 "retrieved_at": "2026-01-01T00:00:00Z", "sha256": "tic-sha"},
                {"source_name": "nasa_toi", "source_version": "fixture_toi",
                 "retrieved_at": "2026-01-01T00:00:00Z", "sha256": "toi-sha"},
            ]), encoding="utf-8")
            self._create_core(core_path)

            con = duckdb.connect(str(core_path))
            before_planets = con.execute("select count(*) from planets").fetchone()[0]
            report = materialize_core(
                con, cooked_dir=cooked, manifest_path=manifest_path,
                report_path=root / "coverage.json", append_search_terms=True,
            )
            self.assertEqual(report["counts"]["accepted"], 2)
            self.assertEqual(report["counts"]["ambiguous"], 1)
            self.assertEqual(report["counts"]["excluded"], 1)
            self.assertEqual(report["counts"]["missing"], 1)
            self.assertEqual(report["counts"]["tic_identifier_collisions"], 0)
            self.assertEqual(con.execute("select count(*) from planets").fetchone()[0], before_planets)
            self.assertEqual(
                con.execute("select count(*) from aliases where alias_norm='tic 1001'").fetchone()[0], 1
            )
            self.assertEqual(
                con.execute("select count(*) from system_search_terms where term_norm='toi 1 01'").fetchone()[0], 1
            )
            self.assertEqual(
                con.execute("select count(*) from identifier_quarantine where reason='tic_split'").fetchone()[0], 1
            )
            self.assertIsNone(
                con.execute("select gaia_id from identifier_quarantine where reason='tic_split'").fetchone()[0]
            )
            con.close()

            arm = duckdb.connect(str(arm_path))
            arm.execute(f"attach '{str(core_path).replace(chr(39), chr(39) * 2)}' as core (read_only)")
            counts = materialize_arm(
                arm, cooked_dir=cooked, manifest_path=manifest_path,
                ingested_at="2026-01-02T00:00:00Z",
            )
            self.assertEqual(counts["toi_current_evidence_rows"], 3)
            self.assertEqual(counts["toi_candidate_rows"], 1)
            self.assertEqual(counts["toi_negative_evidence_rows"], 1)
            self.assertEqual(counts["toi_confirmed_known_planet_links"], 1)
            self.assertEqual(
                arm.execute("select planet_id from toi_current_evidence where disposition='CP'").fetchone()[0], 100
            )
            self.assertIsNone(
                arm.execute("select planet_id from toi_current_evidence where disposition='PC'").fetchone()[0]
            )
            arm.close()

    def _write_fixture_inputs(self, cooked: Path) -> None:
        write_csv(cooked / "target_tic_ids.csv", ["tic_id", "source_families"], [
            {"tic_id": 1001, "source_families": "nasa_toi"},
            {"tic_id": 1002, "source_families": "nasa_toi"},
            {"tic_id": 1003, "source_families": "tess_eb"},
            {"tic_id": 1004, "source_families": "nasa_planet_host"},
            {"tic_id": 1005, "source_families": "operator_seed"},
        ])
        tic_rows = []
        for tic_id, gaia_id, hip_id, disposition, distance in (
            (1001, 2001, "", "", 10),
            (1002, 2002, "", "", 20),
            (1003, 2003, "", "SPLIT", 30),
            (1004, 2004, "", "", 400),
            (1005, "", 15, "", 40),
        ):
            row = {field: "" for field in TIC_COOKED_FIELDS}
            row.update(tic_id=tic_id, tic_version="fixture", gaia_dr2_id=gaia_id,
                       hip_id=hip_id, object_type="STAR", ra_deg=10 + tic_id / 1000,
                       dec_deg=20, distance_pc=distance, disposition=disposition,
                       source_row_hash=f"tic-{tic_id}")
            tic_rows.append(row)
        write_csv(cooked / "targeted_tic.csv", TIC_COOKED_FIELDS, tic_rows)
        write_csv(cooked / "gaia_dr2_neighbourhood.csv", NEIGHBOUR_FIELDS, [
            {"dr2_source_id": 2001, "dr3_source_id": 3001, "angular_distance_arcsec": 0.01,
             "magnitude_difference": 0, "number_of_neighbours": 1, "proper_motion_propagation": "true"},
            {"dr2_source_id": 2002, "dr3_source_id": 3002, "angular_distance_arcsec": 0.01,
             "magnitude_difference": 0, "number_of_neighbours": 2, "proper_motion_propagation": "true"},
            {"dr2_source_id": 2002, "dr3_source_id": 3003, "angular_distance_arcsec": 0.02,
             "magnitude_difference": 0, "number_of_neighbours": 2, "proper_motion_propagation": "true"},
            {"dr2_source_id": 2004, "dr3_source_id": 3999, "angular_distance_arcsec": 0.01,
             "magnitude_difference": 0, "number_of_neighbours": 1, "proper_motion_propagation": "true"},
        ])
        write_csv(cooked / "gaia_dr3_targets.csv", GAIA_DR3_FIELDS, [
            {"source_id": 3001, "parallax_mas": 100},
            {"source_id": 3002, "parallax_mas": 50},
            {"source_id": 3003, "parallax_mas": 50},
            {"source_id": 3999, "parallax_mas": 2.5},
        ])
        write_csv(cooked / "gaia_external_crossmatches.csv", GAIA_EXTERNAL_FIELDS, [])
        toi_rows = []
        for source_key, tic_id, disposition, period in (
            ("TOI-1.01", 1001, "CP", 10.0),
            ("TOI-1.02", 1001, "PC", 20.0),
            ("TOI-3.01", 1003, "FP", 5.0),
        ):
            row = {field: "" for field in TOI_COOKED_FIELDS}
            row.update(source_key=source_key, tic_id=tic_id, toi=source_key[4:],
                       toi_display=source_key, disposition=disposition,
                       orbital_period_days=period, row_updated_at="2026-01-01",
                       source_row_hash=source_key)
            toi_rows.append(row)
        write_csv(cooked / "toi.csv", TOI_COOKED_FIELDS, toi_rows)
        history_rows = [
            {"source_key": row["source_key"], "tic_id": row["tic_id"],
             "toi_display": row["toi_display"], "disposition": row["disposition"],
             "effective_at": row["row_updated_at"], "release_date": "",
             "source_row_hash": row["source_row_hash"],
             "first_observed_at": "2026-01-01T00:00:00Z",
             "last_observed_at": "2026-01-01T00:00:00Z"}
            for row in toi_rows
        ]
        write_csv(cooked / "toi_disposition_history.csv", TOI_HISTORY_FIELDS, history_rows)

    def _create_core(self, path: Path) -> None:
        con = duckdb.connect(str(path))
        con.execute("""
            create table stars (
              star_id bigint, system_id bigint, gaia_id bigint, hip_id bigint,
              catalog_ids_json json, ra_deg double, dec_deg double,
              pm_ra_mas_yr double, pm_dec_mas_yr double
            );
            insert into stars values
              (1, 10, 3001, null, '{}', 11.001, 20, 1, 1),
              (2, 20, 3002, null, '{}', 11.002, 20, 1, 1),
              (3, 20, 3003, null, '{}', 11.0021, 20, 1, 1),
              (5, 50, null, 15, '{}', 11.005, 20, 1, 1);
            create table planets (planet_id bigint, system_id bigint, star_id bigint, orbital_period_days double);
            insert into planets values (100, 10, 1, 10.0001);
            create table object_identifiers (
              identifier_id bigint, target_type varchar, target_id bigint, namespace varchar,
              id_value_raw varchar, id_value_norm varchar, is_canonical boolean,
              resolution_method varchar, resolution_confidence double, source_catalog varchar,
              source_version varchar, source_pk bigint, evidence_json varchar
            );
            create table identifier_quarantine (
              quarantine_id bigint, source_catalog varchar, source_version varchar,
              source_pk bigint, gaia_id bigint, hip_id bigint, hd_id bigint,
              reason varchar, details_json varchar, created_at varchar
            );
            create table aliases (
              alias_id bigint, target_type varchar, target_id bigint, system_id bigint,
              star_id bigint, alias_raw varchar, alias_norm varchar, alias_kind varchar,
              alias_priority integer, is_primary boolean, source_catalog varchar,
              source_version varchar, source_pk bigint
            );
            create table system_search_terms (
              search_term_id bigint, system_id bigint, target_type varchar, target_id bigint,
              star_id bigint, alias_id bigint, term_raw varchar, term_norm varchar,
              term_kind varchar, term_priority integer, is_primary boolean,
              source_catalog varchar, source_version varchar, source_pk bigint
            );
        """)
        con.close()


if __name__ == "__main__":
    unittest.main()
