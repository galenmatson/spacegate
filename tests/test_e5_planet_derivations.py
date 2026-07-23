from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "compile_e5_planet_derivations",
    ROOT / "scripts/compile_e5_planet_derivations.py",
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

REPRO_SPEC = importlib.util.spec_from_file_location(
    "verify_e5_planet_derivation_reproduction",
    ROOT / "scripts/verify_e5_planet_derivation_reproduction.py",
)
REPRO = importlib.util.module_from_spec(REPRO_SPEC)
assert REPRO_SPEC.loader is not None
REPRO_SPEC.loader.exec_module(REPRO)


def test_checked_in_policy_declares_only_registered_planet_derivations() -> None:
    policy = MODULE.load_json(ROOT / "config/evidence_lake/e5_planet_derivations.json")
    selection, derivations = MODULE.validate_policy(
        ROOT / "config/evidence_lake/e5_planet_derivations.json", policy
    )
    assert selection["policy_version"] == policy["selection_policy"]["policy_version"]
    assert set(derivations) == {
        "planet_semimajor_axis_kepler",
        "planet_insolation",
        "planet_equilibrium_temperature",
    }


def test_planet_derivation_formulas_and_direct_precedence(tmp_path: Path) -> None:
    base_path = tmp_path / "base.duckdb"
    core_path = tmp_path / "core.duckdb"
    out_path = tmp_path / "out.duckdb"
    base = duckdb.connect(str(base_path))
    base.execute(
        """
        CREATE TABLE selected_facts(
          selected_fact_id VARCHAR,object_type VARCHAR,stable_object_key VARCHAR,
          system_stable_object_key VARCHAR,quantity_group VARCHAR,quantity_key VARCHAR,
          value_raw VARCHAR,normalized_value DOUBLE,normalized_unit VARCHAR,
          value_lower DOUBLE,value_upper DOUBLE,interval_semantics VARCHAR,
          fact_status VARCHAR,evidence_build_id VARCHAR,evidence_table VARCHAR,
          evidence_id VARCHAR,parameter_set_id VARCHAR,source_record_id VARCHAR,
          source_id VARCHAR,release_id VARCHAR,method VARCHAR,model VARCHAR,
          reference_raw VARCHAR,selection_decision_id VARCHAR,authority_rank INTEGER,
          authority_reason VARCHAR,policy_version VARCHAR,normalization_version VARCHAR,
          quality_json JSON,binding_id VARCHAR
        );
        CREATE TABLE selected_fact_derivations(
          derivation_id VARCHAR,output_selected_fact_id VARCHAR,stable_object_key VARCHAR,
          quantity_key VARCHAR,algorithm_key VARCHAR,algorithm_version VARCHAR,
          input_selected_fact_ids_json JSON,applicability VARCHAR,formula VARCHAR,
          assumptions_json JSON,uncertainty_method VARCHAR,confidence_tier VARCHAR,
          supersedes_json JSON,policy_version VARCHAR
        );
        """
    )
    rows = [
        ("period", "planet", "planet:one", "system:one", "planet_orbit", "orbital_period_days", 365.25, "d"),
        ("mass", "star", "star:one", "system:one", "stellar_fundamental", "mass_msun", 1.0, "Msun"),
        ("lum", "star", "star:one", "system:one", "stellar_fundamental", "luminosity_lsun", 1.0, "Lsun"),
        ("period2", "planet", "planet:direct", "system:one", "planet_orbit", "orbital_period_days", 10.0, "d"),
        ("direct", "planet", "planet:direct", "system:one", "planet_orbit", "semi_major_axis_au", 0.1, "au"),
        ("direct-insol", "planet", "planet:direct", "system:one", "planet_environment", "insol_earth", 100.0, "Searth"),
        ("direct-temp", "planet", "planet:direct", "system:one", "planet_environment", "eq_temp_k", 880.0, "K"),
    ]
    for fact_id, object_type, key, system_key, group, quantity, value, unit in rows:
        base.execute(
            """
            INSERT INTO selected_facts VALUES (
              ?,?,?,?,?,?,?,?, ?,NULL,NULL,NULL,'source_selected',
              NULL,NULL,NULL,NULL,NULL,'test','v1',NULL,NULL,NULL,NULL,10,NULL,
              'test','test',NULL,NULL
            )
            """,
            [fact_id, object_type, key, system_key, group, quantity, str(value), value, unit],
        )
    base.close()
    core = duckdb.connect(str(core_path))
    core.execute(
        """
        CREATE TABLE stars(star_id HUGEINT,stable_object_key VARCHAR);
        CREATE TABLE planets(planet_id HUGEINT,star_id HUGEINT,stable_object_key VARCHAR);
        INSERT INTO stars VALUES (1,'star:one');
        INSERT INTO planets VALUES (10,1,'planet:one'),(11,1,'planet:direct');
        """
    )
    core.close()
    con = duckdb.connect(str(out_path))
    con.execute(f"ATTACH '{base_path}' AS base (READ_ONLY)")
    con.execute(f"ATTACH '{core_path}' AS core (READ_ONLY)")
    MODULE.create_schema(con)
    policy = MODULE.load_json(ROOT / "config/evidence_lake/e5_planet_derivations.json")
    selection = MODULE.load_json(ROOT / "config/evidence_lake/e5_selection_policies.json")
    derivations = {row["derivation_key"]: row for row in selection["derivations"]}
    assert MODULE.compile_semimajor_axes(
        con, derivations["planet_semimajor_axis_kepler"], policy
    ) == 1
    assert MODULE.compile_insolation(con, derivations["planet_insolation"], policy) == 1
    assert MODULE.compile_equilibrium_temperature(
        con, derivations["planet_equilibrium_temperature"], policy
    ) == 1
    values = dict(con.execute(
        "SELECT quantity_key,normalized_value FROM selected_facts ORDER BY quantity_key"
    ).fetchall())
    assert abs(values["semi_major_axis_au"] - 1.0) < 1e-10
    assert abs(values["insol_earth"] - 1.0) < 1e-10
    assert abs(values["eq_temp_k"] - 278.5) < 1e-10
    assert MODULE.checks(con) == {
        "duplicate_object_quantities": 0,
        "derived_overrides_direct": 0,
        "derived_without_lineage": 0,
        "lineage_without_fact": 0,
        "invalid_values": 0,
        "unresolved_semimajor_applicable": 0,
        "unresolved_insolation_applicable": 0,
        "unresolved_equilibrium_applicable": 0,
    }
    con.close()


def test_reproduction_verifier_compares_logical_duckdb_tables(tmp_path: Path) -> None:
    left = tmp_path / "left.duckdb"
    right = tmp_path / "right.duckdb"
    for path in (left, right):
        con = duckdb.connect(str(path))
        con.execute("CREATE TABLE facts(id INTEGER,value VARCHAR)")
        con.execute("INSERT INTO facts VALUES (1,'same')")
        con.close()
    assert REPRO.table_delta(left, right, "facts") == {
        "left_only": 0,
        "right_only": 0,
    }
