from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_scientific_evidence as compiler  # noqa: E402
import verify_scientific_evidence_artifact as artifact_audit  # noqa: E402
import verify_scientific_evidence_reproduction as reproduction  # noqa: E402


CONTRACT_PATH = ROOT / "config" / "evidence_lake" / "e4_scientific_evidence.json"


def test_checked_in_scientific_evidence_contract_is_complete_and_valid() -> None:
    contract = compiler.load_json(CONTRACT_PATH)
    assert compiler.validate_contract(contract) == []
    assert set(contract["domain_tables"]) == compiler.DOMAIN_TABLES
    assert compiler.CITATION_LINK_BUCKET_COUNT >= 16
    nasa_adapter = contract["source_adapters"][
        "nasa_exoplanet_archive.planetary_systems"
    ]
    assert len(nasa_adapter["tables"]) == 12
    wide_adapter = contract["source_adapters"][
        "multiplicity.el_badry_2021_wide_binary"
    ]
    assert set(wide_adapter["tables"]) == {
        "el_badry_finder_code",
        "el_badry_neighbor_code",
        "el_badry_shifted_control_catalog",
        "el_badry_wide_binary_catalog",
    }
    magnetar_adapter = contract["source_adapters"]["compact.mcgill_magnetar"]
    assert len(magnetar_adapter["tables"]["TabO1"]["compact_object_parameter_sets"]) == 5
    assert set(contract["source_adapters"]["multiplicity.sb9"]["tables"]) == {
        "sb9_readme",
        "sb9_main",
        "sb9_alias",
        "sb9_orbits",
    }
    sbx_adapter = contract["source_adapters"]["multiplicity.sbx"]
    assert set(sbx_adapter["tables"]) == {
        "sbx_systems",
        "sbx_alias",
        "sbx_configurations",
        "sbx_orbits",
    }
    assert sbx_adapter["tables"]["sbx_orbits"]["orbital_solution"][
        "relation_link"
    ]["required"] is True
    msc = contract["source_adapters"]["multiplicity.msc"]
    assert set(msc["tables"]) == {
        "msc_archive_members",
        "msc_comp",
        "msc_notes",
        "msc_orb",
        "msc_readme",
        "msc_sys",
    }
    assert msc["tables"]["msc_sys"]["relation_claim"][
        "left_identifier_fields"
    ] == ["WDS", "Primary"]
    assert msc["tables"]["msc_orb"]["orbital_solution"][
        "relation_link_policy"
    ] == "opaque_source_pair_pending_e2"
    wds = contract["source_adapters"]["multiplicity.wds"]
    assert set(wds["tables"]) == {"wdsweb_format", "wdsweb_summ2"}
    assert "components" not in wds["tables"]["wdsweb_summ2"][
        "identifier_claims"
    ]
    wds_xmatch = contract["source_adapters"]["multiplicity.wds_gaia_xmatch"][
        "tables"
    ]["wds_gaia_xmatch_best"]["relation_claim"]
    assert wds_xmatch["evidence_polarity"] == "candidate"
    assert "probability_field" not in wds_xmatch
    assert wds_xmatch["confidence_statistic_field"] == "angDist"
    nss_adapter = contract["source_adapters"]["gaia.dr3.non_single_star"]
    assert set(nss_adapter["tables"]) == {
        "gaia_dr3_nss_two_body_orbit_full_v2",
        "gaia_dr3_nss_two_body_orbit_uncertain_distance_supplement_v1",
    }
    nss_orbit = nss_adapter["tables"]["gaia_dr3_nss_two_body_orbit_full_v2"][
        "orbital_solution"
    ]
    assert nss_adapter["tables"]["gaia_dr3_nss_two_body_orbit_full_v2"][
        "logical_key_fields"
    ] == ["source_id", "solution_id", "nss_solution_type"]
    assert nss_orbit["solution_key_fields"] == [
        "source_id",
        "solution_id",
        "nss_solution_type",
    ]
    assert nss_orbit["model_field"] == "nss_solution_type"
    assert "corr_vec" in nss_orbit["quality_fields"]
    assert len(nss_orbit["parameter_fields"]) == 56
    assert len(nss_orbit["quality_fields"]) == 18
    wgsn = contract["source_adapters"]["naming.iau_wgsn"]["tables"][
        "iau_wgsn_catalog_html"
    ]
    assert wgsn["identifier_claims"]["bayer_id"]["claim_scope"] == (
        "alias_system_or_component"
    )
    assert wgsn["source_citation_links"][0]["identifier_claim_field"] == (
        "proper_name"
    )
    gcvs = contract["source_adapters"]["classification.gcvs"]
    assert set(gcvs["tables"]) == {
        "gcvs_catalog",
        "gcvs_cross_identifiers",
        "gcvs_readme",
        "gcvs_references",
        "gcvs_suspected_variables",
        "gcvs_variable_type_dictionary",
    }
    assert gcvs["tables"]["gcvs_references"]["citation_catalog"][
        "aggregate_repeated_key_lines"
    ] is True
    assert gcvs["tables"]["gcvs_catalog"]["conditional_identifier_claims"][0][
        "sql_predicate"
    ] == "m_VarNum is null"
    assert gcvs["tables"]["gcvs_suspected_variables"][
        "conditional_identifier_claims"
    ][0]["sql_predicate"] == "m_NSV is null"
    clusters = contract["source_adapters"]["clusters.hunt_reffert_2024"]
    assert set(clusters["tables"]) == {
        "hunt_reffert_2024_clusters",
        "hunt_reffert_2024_crossmatch",
        "hunt_reffert_2024_members",
    }
    member_selection = clusters["tables"]["hunt_reffert_2024_members"][
        "row_selection"
    ]
    assert member_selection["cross_table_memberships"][0][
        "target_sql_predicate"
    ] == "membership_target.dist16 <= 383.245"
    ultracool = contract["source_adapters"]["ultracool.gaia_dr3_sample"]
    assert set(ultracool["tables"]) == {"table4", "table4_readme"}
    assert [
        membership["evidence_key"]
        for membership in ultracool["tables"]["table4"]["cluster_memberships"]
    ] == ["hmac_assignment", "banyan_best_hypothesis"]
    assert "membership_probability_field" not in ultracool["tables"]["table4"][
        "cluster_memberships"
    ][0]
    ultracool_sheet = contract["source_adapters"]["ultracool.ultracoolsheet"][
        "tables"
    ]["UltracoolSheet_Main"]
    assert len(ultracool_sheet["photometry_measurements"]) == 23
    assert len(ultracool_sheet["configured_domain_measurements"]) == 55
    assert ultracool_sheet["identifier_claims"]["sourceID_Gaia_DR2"][
        "namespace"
    ] == "gaia_dr2_source_id"
    assert ultracool_sheet["identifier_claims"]["sourceID_Gaia_DR3"][
        "namespace"
    ] == "gaia_dr3_source_id"
    assert "identifiers_simbad" not in ultracool_sheet["identifier_claims"]
    assert ultracool_sheet["observation_product_missing_values"] == {
        "url_simpleDB": ["null", "nan"]
    }
    apogee = contract["source_adapters"]["spectroscopy.apogee_dr17"]
    assert set(apogee["tables"]) == {
        "apogee_dr17_allstar",
        "apogee_dr17_model_grid_metadata",
        "apogee_dr17_field_versions",
    }
    apogee_allstar = apogee["tables"]["apogee_dr17_allstar"]
    assert len(apogee_allstar["scoped_stellar_parameter_sets"][0]["measurements"]) == 30
    apogee_membership = apogee_allstar["row_selection"][
        "external_membership_groups"
    ][0]
    assert apogee_membership["normalization"] == "unsigned_integer_decimal_v1"
    assert apogee_membership["targets"][0]["source_id"] == (
        "distance.gaia_edr3_bailer_jones"
    )
    assert apogee_allstar["identifier_claims"]["GAIAEDR3_SOURCE_ID"][
        "namespace"
    ] == "gaia_edr3_source_id"
    galah = contract["source_adapters"]["spectroscopy.galah_dr4"]
    assert set(galah["tables"]) == {"galah_dr4_allstar_240705"}
    galah_allstar = galah["tables"]["galah_dr4_allstar_240705"]
    assert galah_allstar["row_selection"]["cache_selected_rows"] is True
    galah_membership = galah_allstar["row_selection"][
        "external_membership_groups"
    ][0]
    assert galah_membership["match"] == "any"
    assert galah_membership["normalization"] == "unsigned_integer_decimal_v1"
    assert [target["target_table"] for target in galah_membership["targets"]] == [
        "gaia_dr3_source_envelope_v2",
        "gaia_dr3_source_uncertain_distance_supplement_v1",
    ]
    assert galah_allstar["identifier_claims"]["gaiadr3_source_id"][
        "namespace"
    ] == "gaia_dr3_source_id"
    assert [
        parameter_set["parameter_set_kind"]
        for parameter_set in galah_allstar["scoped_stellar_parameter_sets"]
    ] == [
        "galah_dr4_spectroscopic_parameters_and_abundances",
        "galah_dr4_isochrone_and_bolometric_model",
    ]
    assert len(galah_allstar["scoped_stellar_parameter_sets"][0]["measurements"]) == 36
    assert len(galah_allstar["scoped_stellar_parameter_sets"][1]["measurements"]) == 4
    assert len(galah_allstar["photometry_measurements"]) == 14
    galah_domain = galah_allstar["configured_domain_measurements"]
    assert len(galah_domain) == 17
    assert {
        measurement["quantity_key"]
        for measurement in galah_domain
        if measurement["value_field"] in {"r_lo", "r_med", "r_hi"}
    } == {
        "distance_model_lower_bound",
        "distance_model_median",
        "distance_model_upper_bound",
    }
    assert {
        measurement["quantity_key"]
        for measurement in galah_domain
        if measurement["value_field"].startswith("sb2_rv_")
    } == {
        "sb2_radial_velocity_posterior_p16",
        "sb2_radial_velocity_posterior_median",
        "sb2_radial_velocity_posterior_p84",
    }
    lamost = contract["source_adapters"]["spectroscopy.lamost_dr11"]
    assert set(lamost["tables"]) == {
        "lamost_dr11_v2_lrs_stellar",
        "lamost_dr11_v2_lrs_mstellar",
        "lamost_dr11_v2_mrs_stellar",
    }
    for lamost_table in lamost["tables"].values():
        assert lamost_table["row_selection"]["cache_selected_rows"] is True
        membership = lamost_table["row_selection"]["external_membership_groups"][0]
        assert membership["match"] == "any"
        assert membership["normalization"] == "unsigned_integer_decimal_v1"
        assert [target["target_table"] for target in membership["targets"]] == [
            "gaia_dr3_source_envelope_v2",
            "gaia_dr3_source_uncertain_distance_supplement_v1",
        ]
        assert lamost_table["identifier_claims"]["gaia_source_id"]["namespace"] == (
            "gaia_dr3_source_id"
        )
        assert lamost_table["configured_observation_products"][0][
            "retrieval_policy"
        ] == "on_demand_official_archive"
        assert lamost_table["configured_domain_storage"] == {
            "astrometry_distance_evidence": "typed_measurement_bundle_v1"
        }
        assert {
            claim["namespace"]
            for claim in lamost_table["conditional_identifier_claims"]
        } == {
            "gaia_dr3_source_id",
            "panstarrs1_object_id",
            "lamost_coordinate_source_id",
        }
    assert len(
        lamost["tables"]["lamost_dr11_v2_lrs_stellar"][
            "scoped_stellar_parameter_sets"
        ][1]["measurements"]
    ) == 5
    assert len(
        lamost["tables"]["lamost_dr11_v2_lrs_mstellar"][
            "scoped_stellar_parameter_sets"
        ][2]["measurements"]
    ) == 11
    assert [
        len(parameter_set["measurements"])
        for parameter_set in lamost["tables"]["lamost_dr11_v2_mrs_stellar"][
            "scoped_stellar_parameter_sets"
        ]
    ] == [5, 17]
    extended = contract["source_adapters"]["extended.openngc_and_nebulae"]
    assert len(extended["tables"]) == 16
    assert extended["tables"]["openngc_addendum"]["table_contract_ref"] == (
        "openngc_ngc"
    )
    cederblad_claims = extended["tables"]["cederblad_vii_231"][
        "composite_identifier_claims"
    ]
    assert [claim["sql_predicate"] for claim in cederblad_claims] == [
        "m_Ced is null",
        "m_Ced is not null",
    ]
    tess_mast = contract["source_adapters"][
        "tess.identity_and_candidate_evidence"
    ]["tables"]["mast_tic_targeted"]
    tess_claims = tess_mast["identifier_claims"]
    for relation in tess_mast["relation_claims"]:
        for side in ("left", "right"):
            field = relation[f"{side}_identifier_field"]
            assert tess_claims[field]["namespace"] == relation[
                f"{side}_identifier_namespace"
            ]
            assert tess_claims[field]["component_scope"] == relation[
                f"{side}_component_scope"
            ]


def test_contract_table_order_must_cover_each_table_exactly_once() -> None:
    contract = compiler.load_json(CONTRACT_PATH)
    adapter = contract["source_adapters"]["compact.atnf"]
    original = list(adapter["table_order"])

    adapter["table_order"] = [*original, original[0]]
    errors = compiler.validate_contract(contract)
    assert "compact.atnf.table_order contains duplicates" in errors

    adapter["table_order"] = original[:-1]
    errors = compiler.validate_contract(contract)
    assert "compact.atnf.table_order must cover every table exactly" in errors


def test_multiple_cluster_memberships_require_distinct_evidence_keys() -> None:
    contract = compiler.load_json(CONTRACT_PATH)
    table = contract["source_adapters"]["ultracool.gaia_dr3_sample"]["tables"][
        "table4"
    ]
    table["cluster_memberships"][1]["evidence_key"] = "hmac_assignment"
    errors = compiler.validate_contract(contract)
    assert (
        "ultracool.gaia_dr3_sample.table4.cluster_memberships contains duplicate "
        "evidence_key values"
    ) in errors


def test_multiple_relation_claims_require_distinct_evidence_keys() -> None:
    contract = compiler.load_json(CONTRACT_PATH)
    table = contract["source_adapters"]["multiplicity.wds_gaia_xmatch"]["tables"][
        "wds_gaia_xmatch_best"
    ]
    relation = table.pop("relation_claim")
    table["relation_claims"] = [
        {**relation, "evidence_key": "candidate"},
        {**relation, "evidence_key": "candidate"},
    ]
    errors = compiler.validate_contract(contract)
    assert (
        "multiplicity.wds_gaia_xmatch.wds_gaia_xmatch_best.relation_claims "
        "contains duplicate evidence_key values"
    ) in errors


def test_configured_measurements_reject_nonfinite_numeric_values() -> None:
    numeric = {"value_field": "value", "quantity_key": "quantity"}
    lexical = {
        "value_field": "value",
        "quantity_key": "quantity",
        "normalize_numeric": False,
    }
    assert "isfinite" in compiler.configured_measurement_predicate("value", numeric)
    assert "isfinite" not in compiler.configured_measurement_predicate("value", lexical)
    uncertainty = compiler.nullable_measurement_double_expression(
        "uncertainty", ["nan"], absolute=True, minimum_value=0
    )
    assert "isfinite" in uncertainty
    assert 'try_cast("uncertainty" as double)>=0.0' in uncertainty


def test_configured_measurements_support_asymmetric_uncertainty_fields() -> None:
    measurement = {
        "value_field": "value",
        "quantity_key": "quantity",
        "uncertainty_lower_field": "error_lower",
        "uncertainty_upper_field": "error_upper",
    }
    assert compiler.configured_domain_measurement_fields([measurement]) == {
        "value",
        "error_lower",
        "error_upper",
    }

    contract = compiler.load_json(CONTRACT_PATH)
    table = contract["source_adapters"]["tess.villanova_eb"]["tables"][
        "tess_eb_catalog"
    ]
    table["configured_domain_measurements"][0]["uncertainty_field"] = "symmetric"
    table["configured_domain_measurements"][0]["uncertainty_lower_field"] = "lower"
    errors = compiler.validate_contract(contract)
    assert any(
        "cannot combine symmetric and asymmetric uncertainty fields" in error
        for error in errors
    )

    bailer_jones = contract["source_adapters"][
        "distance.gaia_edr3_bailer_jones"
    ]["tables"]["bailer_jones_edr3_distance_envelope_v1"]
    distance_measurements = bailer_jones["configured_domain_measurements"]
    assert [row["bound_semantics"] for row in distance_measurements] == [
        "posterior_16th_84th_percentile_interval_endpoints",
        "posterior_16th_84th_percentile_interval_endpoints",
    ]


def test_configured_photometry_fields_include_lineage_and_quality() -> None:
    assert compiler.configured_photometry_fields(
        [
            {
                "value_field": "mag",
                "uncertainty_field": "mag_error",
                "bandpass_field": "band",
                "reference_field": "reference",
                "quality_fields": ["flag", "chi_square"],
            }
        ]
    ) == {"mag", "mag_error", "band", "reference", "flag", "chi_square"}


def test_configured_epoch_expression_accepts_field_or_constant() -> None:
    assert compiler.configured_epoch_expression({"epoch_field": "epoch"}) == (
        'trim(cast("epoch" as varchar))'
    )
    assert compiler.configured_epoch_expression({"epoch_raw": "J2000.0"}) == (
        "'J2000.0'"
    )


def test_scalar_evidence_accepts_explicit_units_for_schema_poor_sources() -> None:
    contract = compiler.load_json(CONTRACT_PATH)
    table = contract["source_adapters"]["tess.villanova_eb"]["tables"][
        "tess_eb_catalog"
    ]
    table["unit_overrides"] = {"period_days": "d"}
    assert compiler.validate_contract(contract) == []

    table["unit_overrides"] = {"period_days": ""}
    errors = compiler.validate_contract(contract)
    assert any("unit_overrides must be a non-empty" in error for error in errors)


def test_table_contract_reference_inherits_mapping_with_local_overrides() -> None:
    adapter = {
        "tables": {
            "base": {
                "logical_key_fields": ["oid"],
                "object_scope": "object",
                "field_profile": "basic",
                "identifier_claims": {"oid": {"namespace": "oid"}},
            },
            "supplement": {
                "table_contract_ref": "base",
                "raw_artifact_name": "supplement_raw",
                "object_scope": "supplement_object",
            },
        }
    }
    resolved = compiler.resolve_table_contract(adapter, "supplement")
    assert resolved["logical_key_fields"] == ["oid"]
    assert resolved["field_profile"] == "basic"
    assert resolved["raw_artifact_name"] == "supplement_raw"
    assert resolved["object_scope"] == "supplement_object"
    assert "table_contract_ref" not in resolved


def test_table_contract_reference_rejects_missing_and_cyclic_references() -> None:
    adapter = {
        "tables": {
            "missing": {"table_contract_ref": "absent"},
            "cycle_a": {"table_contract_ref": "cycle_b"},
            "cycle_b": {"table_contract_ref": "cycle_a"},
        }
    }
    for table_name, expected in (
        ("missing", "unknown table contract reference"),
        ("cycle_a", "cyclic table contract reference"),
    ):
        try:
            compiler.resolve_table_contract(adapter, table_name)
        except ValueError as error:
            assert expected in str(error)
        else:
            raise AssertionError(f"invalid contract reference accepted: {table_name}")


def test_source_field_metadata_reconciles_source_and_selected_output_names() -> None:
    fields = compiler.source_field_metadata(
        {
            "columns": [
                {"name": "ID", "type": "BIGINT"},
                {"name": "CMDCl2_5", "type": "DOUBLE"},
            ]
        },
        {
            "field_dispositions": [
                {"column_name": "ID", "datatype": "long", "unit": None},
                {
                    "column_name": "CMDCl2.5",
                    "datatype": "double",
                    "unit": "mag",
                },
            ],
            "selected_fields": [
                {"source_name": "ID", "output_name": "ID"},
                {"source_name": "CMDCl2.5", "output_name": "CMDCl2_5"},
            ],
        },
    )
    assert [row["column_name"] for row in fields] == ["ID", "CMDCl2_5"]
    assert fields[1]["source_column_name"] == "CMDCl2.5"
    assert fields[1]["unit"] == "mag"


def test_source_field_metadata_reconciles_case_collision_aliases() -> None:
    fields = compiler.source_field_metadata(
        {
            "columns": [
                {"name": "b_rgeo", "type": "DOUBLE"},
                {"name": "B_rgeo__source_case_2", "type": "DOUBLE"},
            ],
            "source_schema": {
                "source_schema": [
                    {"name": "b_rgeo", "unit": "pc"},
                    {
                        "name": "B_rgeo__source_case_2",
                        "source_name": "B_rgeo",
                        "unit": "pc",
                    },
                ]
            },
        },
        {
            "field_dispositions": [
                {"column_name": "b_rgeo", "datatype": "double", "unit": "pc"},
                {"column_name": "B_rgeo", "datatype": "double", "unit": "pc"},
            ]
        },
    )
    assert [row["column_name"] for row in fields] == [
        "b_rgeo",
        "B_rgeo__source_case_2",
    ]
    assert fields[1]["source_column_name"] == "B_rgeo"


def test_source_row_hash_is_not_shadowed_by_single_letter_t_column(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "orbit_with_t.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values ('WDS-1','A,B','0.0000'),"
            "('WDS-2','C,D','0.0000')) rows(WDS,System,T)) "
            f"to '{parquet}' (format parquet)"
        )
        hashes = con.execute(
            f"select sha256(to_json(source_row)) "
            f"from read_parquet('{parquet}') source_row order by WDS"
        ).fetchall()
        assert len({row[0] for row in hashes}) == 2
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'multiplicity.test', 'r1', 'orbits', 'orbit', "
            "'{}', '{}', ?, 1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [[f"record-{index}", row_hash] for index, (row_hash,) in enumerate(hashes)],
        )
        compiler.materialize_identifier_claims(
            con,
            source_id="multiplicity.test",
            release_id="r1",
            table_name="orbits",
            path=parquet,
            fields=["WDS"],
            claim_by_field={
                "WDS": {"namespace": "wds_id", "claim_scope": "system"}
            },
        )
        claims = con.execute(
            "select identifier_raw from identifier_claim_evidence order by 1"
        ).fetchall()
    assert claims == [("WDS-1",), ("WDS-2",)]


def test_cross_table_row_selection_is_bounded_and_schema_checked(
    tmp_path: Path,
) -> None:
    rows = tmp_path / "rows.parquet"
    members = tmp_path / "members.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values (1), (2), (3)) t(oidref)) "
            f"to '{rows}' (format parquet)"
        )
        con.execute(
            f"copy (select * from (values (2, false), (3, true)) t(oid, in_scope)) "
            f"to '{members}' (format parquet)"
        )
    table_contract = {
        "row_selection": {
            "sql_predicate": "oidref >= 2",
            "cross_table_memberships": [
                {
                    "local_field": "oidref",
                    "target_table": "members",
                    "target_field": "oid",
                    "target_sql_predicate": "membership_target.in_scope",
                }
            ],
        }
    }
    typed_tables = {
        "members": {
            "parquet_path": members.name,
            "columns": [{"name": "oid"}],
        }
    }
    predicate = compiler.row_selection_predicate(
        table_contract,
        typed_tables=typed_tables,
        typed_root=tmp_path,
        available_fields={"oidref"},
    )
    with duckdb.connect() as con:
        assert con.execute(
            f"select oidref from read_parquet('{rows}') source_row "
            f"where {predicate} order by 1"
        ).fetchall() == [(3,)]
    typed_tables["members"]["columns"] = [
        {"name": "different"},
        {"name": "in_scope"},
    ]
    try:
        compiler.row_selection_predicate(
            table_contract,
            typed_tables=typed_tables,
            typed_root=tmp_path,
            available_fields={"oidref"},
        )
    except ValueError as error:
        assert "target field missing" in str(error)
    else:
        raise AssertionError("missing cross-table target field was accepted")


def test_external_membership_groups_match_any_target_and_normalize_unsigned_ids(
    tmp_path: Path,
) -> None:
    rows = tmp_path / "rows.parquet"
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values ('0002'), ('3'), ('bad'), (null)) "
            f"t(source_id)) to '{rows}' (format parquet)"
        )
        con.execute(f"copy (select 2::ubigint source_id) to '{first}' (format parquet)")
        con.execute(f"copy (select '0003' source_id) to '{second}' (format parquet)")
    predicate = compiler.row_selection_predicate(
        {"row_selection": {}},
        available_fields={"source_id"},
        external_membership_groups=[
            {
                "membership_id": "nearby_gaia_v1",
                "local_field": "source_id",
                "normalization": "unsigned_integer_decimal_v1",
                "match": "any",
                "targets": [
                    {
                        "parquet_path": str(first),
                        "target_field": "source_id",
                        "target_sql_predicate": "true",
                    },
                    {
                        "parquet_path": str(second),
                        "target_field": "source_id",
                        "target_sql_predicate": "true",
                    },
                ],
            }
        ],
    )
    with duckdb.connect() as con:
        assert con.execute(
            f"select source_id from read_parquet('{rows}') source_row "
            f"where {predicate} order by 1"
        ).fetchall() == [("0002",), ("3",)]


def test_external_membership_target_is_registry_and_checksum_bound(
    tmp_path: Path,
) -> None:
    source_id = "gaia.dr3.test"
    release_id = "r1"
    raw_snapshot_id = "raw1"
    typed_snapshot_id = "typed1"
    typed_root = (
        tmp_path
        / "typed"
        / "evidence_lake_v2"
        / source_id
        / release_id
        / raw_snapshot_id
        / typed_snapshot_id
    )
    typed_root.mkdir(parents=True)
    parquet = typed_root / "members.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select 42::ubigint source_id) to '{parquet}' (format parquet)"
        )
    table_hash = compiler.file_hash(parquet)
    manifest = {
        "typed_snapshot_id": typed_snapshot_id,
        "content_sha256": "typed-content",
        "tables": [
            {
                "source_name": "members",
                "parquet_path": parquet.name,
                "sha256": table_hash,
                "columns": [{"name": "source_id"}],
            }
        ],
    }
    compiler.write_json(typed_root / "typed_manifest.json", manifest)
    target = {
        "source_id": source_id,
        "release_id": release_id,
        "raw_snapshot_id": raw_snapshot_id,
        "typed_snapshot_id": typed_snapshot_id,
        "typed_content_sha256": "typed-content",
        "target_table": "members",
        "target_field": "source_id",
        "target_table_sha256": table_hash,
    }
    registry = {source_id: {"source_id": source_id, "release_id": release_id}}
    resolved = compiler.resolve_external_membership_target(tmp_path, registry, target)
    assert resolved["parquet_path"] == str(parquet.resolve())
    assert resolved["target_table_sha256"] == table_hash

    changed = dict(target, typed_content_sha256="wrong")
    try:
        compiler.resolve_external_membership_target(tmp_path, registry, changed)
    except ValueError as error:
        assert "typed content mismatch" in str(error)
    else:
        raise AssertionError("external typed content drift was accepted")

    changed = dict(target, target_field="missing")
    try:
        compiler.resolve_external_membership_target(tmp_path, registry, changed)
    except ValueError as error:
        assert "target field missing" in str(error)
    else:
        raise AssertionError("external target field drift was accepted")


def test_cluster_context_and_probability_membership_preserve_source_records(
    tmp_path: Path,
) -> None:
    cluster_path = tmp_path / "clusters.parquet"
    member_path = tmp_path / "members.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select 7::bigint as \"ID\", 'Nearby Cluster' as \"Name\", "
            f"300.5::double as dist16) to '{cluster_path}' (format parquet)"
        )
        con.execute(
            f"copy (select 7::bigint ID, 123456789012345678::bigint GaiaDR3, "
            f"0.875::double Prob, 12::bigint HMACcl, 1.1::double Mass50) "
            f"to '{member_path}' (format parquet)"
        )
        cluster_hash = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{cluster_path}') t"
        ).fetchone()[0]
        member_hash = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{member_path}') t"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'clusters.test', 'r1', ?, ?, '{}', '{}', ?, 1, "
            "'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [
                ("cluster-record", "clusters", "open_cluster", cluster_hash),
                (
                    "member-record",
                    "members",
                    "probability_bearing_cluster_member_claim",
                    member_hash,
                ),
            ],
        )
        cluster_fields = [
            {"column_name": "Name"},
            {"column_name": "dist16"},
        ]
        consumed_cluster = compiler.materialize_cluster_context(
            con,
            source_id="clusters.test",
            release_id="r1",
            table_name="clusters",
            path=cluster_path,
            fields=cluster_fields,
            cluster_context={
                "cluster_identity_field": "ID",
                "method": "test_cluster_method",
                "model": "test_model",
                "reference_raw": "2024TEST",
                "quality_fields": ["Name"],
                "normalization_version": "source_native_v1",
            },
            available_fields={"ID", "Name", "dist16"},
        )
        member_fields = [
            {"column_name": "Prob"},
            {"column_name": "Mass50"},
        ]
        consumed_member = compiler.materialize_cluster_memberships(
            con,
            source_id="clusters.test",
            release_id="r1",
            table_name="members",
            path=member_path,
            fields=member_fields,
            cluster_membership={
                "cluster_identity_field": "ID",
                "member_identity_field": "GaiaDR3",
                "membership_probability_field": "Prob",
                "probability_semantics": "test_probability",
                "method": "test_membership_method",
                "reference_raw": "2024TEST",
            },
            available_fields={"ID", "GaiaDR3", "Prob", "HMACcl", "Mass50"},
        )
        consumed_hard_assignment = compiler.materialize_cluster_memberships(
            con,
            source_id="clusters.test",
            release_id="r1",
            table_name="members",
            path=member_path,
            fields=[{"column_name": "HMACcl"}],
            cluster_membership={
                "cluster_identity_field": "HMACcl",
                "member_identity_field": "GaiaDR3",
                "probability_semantics": "hard_assignment_without_probability",
                "method": "test_hard_assignment",
                "reference_raw": "2024TEST",
            },
            available_fields={"ID", "GaiaDR3", "Prob", "HMACcl", "Mass50"},
            evidence_key="hmac_assignment",
        )
        cluster = con.execute(
            "select cluster_identity_raw, parameter_set_raw->>'dist16', method, "
            "reference_raw from cluster_evidence"
        ).fetchone()
        members = con.execute(
            "select cluster_identity_raw, member_identity_raw, "
            "membership_probability, quality_json->>'probability_semantics', "
            "quality_json->'source_membership_record'->>'Mass50' "
            "from cluster_membership_evidence order by method"
        ).fetchall()
    assert consumed_cluster == {"ID", "Name", "dist16"}
    assert consumed_member == {"ID", "GaiaDR3", "Prob", "Mass50"}
    assert consumed_hard_assignment == {"HMACcl", "GaiaDR3"}
    assert cluster == ("7", "300.5", "test_cluster_method", "2024TEST")
    assert members == [
        (
            "12",
            "123456789012345678",
            None,
            "hard_assignment_without_probability",
            None,
        ),
        (
            "7",
            "123456789012345678",
            0.875,
            "test_probability",
            "1.1",
        ),
    ]


def test_scientific_evidence_schema_has_bounded_domain_tables() -> None:
    with duckdb.connect() as con:
        compiler.create_schema(con)
        tables = set(compiler.user_tables(con))
    assert compiler.DOMAIN_TABLES <= tables
    assert {
        "evidence_build",
        "evidence_sources",
        "source_records",
        "source_field_dispositions",
        "object_binding_outcomes",
        "identifier_normalization_rejections",
    } <= tables


def test_source_record_compilation_is_deterministic_and_accounts_duplicates(
    tmp_path: Path,
) -> None:
    raw_path = tmp_path / "raw"
    typed_path = tmp_path / "typed"
    artifact_path = raw_path / "artifacts" / "test_rows"
    tables_path = typed_path / "tables"
    artifact_path.mkdir(parents=True)
    tables_path.mkdir(parents=True)
    (artifact_path / "product_manifest.json").write_text(
        json.dumps(
            {
                "field_dispositions": [
                    {
                        "column_name": "source_id",
                        "datatype": "long",
                        "unit": None,
                        "ucd": "meta.id",
                        "description": "source identifier",
                    },
                    {
                        "column_name": "note",
                        "datatype": "char",
                        "unit": None,
                        "ucd": None,
                        "description": "source context",
                    },
                    {
                        "column_name": "disposition",
                        "datatype": "char",
                        "unit": None,
                        "ucd": None,
                        "description": "candidate disposition",
                    },
                    {
                        "column_name": "reference",
                        "datatype": "char",
                        "unit": None,
                        "ucd": None,
                        "description": "source reference",
                    },
                    {
                        "column_name": "ignored",
                        "datatype": "char",
                        "unit": None,
                        "ucd": None,
                        "description": "preserved only in source-native storage",
                    },
                    {
                        "column_name": "lineage_note",
                        "datatype": "char",
                        "unit": None,
                        "ucd": None,
                        "description": "source lineage context",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    parquet = tables_path / "test_rows.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            f"('1','alpha','PC','REF','raw-a','release-1'),"
            f"('1','alpha','PC','REF','raw-a','release-1'),"
            f"('1','beta','FP','--','raw-b','release-2')) "
            f"t(source_id,note,disposition,reference,ignored,lineage_note)) "
            f"to '{parquet}' "
            f"(format parquet, compression zstd)"
        )

    input_row = {
        "source_id": "test.catalog",
        "release_id": "r1",
        "raw_path": raw_path,
        "raw_manifest": {
            "snapshot_id": "raw1",
            "content_sha256": "raw-content",
            "artifacts": [
                {
                    "source_name": "raw_rows",
                    "artifact_path": "artifacts/test_rows",
                    "tree_sha256": "raw-tree",
                    "retrieved_at": "2026-07-19T00:00:00Z",
                }
            ],
        },
        "typed_path": typed_path,
        "typed_manifest": {
            "typed_snapshot_id": "typed1",
            "content_sha256": "typed-content",
            "tables": [
                {
                    "source_name": "test_rows",
                    "status": "typed",
                    "parquet_path": "tables/test_rows.parquet",
                    "row_count": 3,
                    "sha256": "typed-table",
                }
            ],
        },
    }
    adapter = {
        "adapter_version": "test_adapter_v1",
        "tables": {
            "test_rows": {
                "raw_artifact_name": "raw_rows",
                "logical_key_fields": ["source_id"],
                "object_scope": "object",
                "field_profile": "test",
                "lifecycle_claims": [
                    {
                        "claim_role": "test_disposition",
                        "identifier_field": "source_id",
                        "disposition_field": "disposition",
                        "context_fields": ["note"],
                    }
                ],
                "citation_catalog": {
                    "reference_key_field": "reference",
                    "citation_text_field": "reference",
                    "excluded_values": ["--"],
                },
                "source_citation_links": [
                    {
                        "identifier_claim_field": "source_id",
                        "reference_key_field": "reference",
                        "citation_role": "source_identity_reference",
                        "excluded_reference_values": ["--"],
                        "required": True,
                    }
                ],
            }
        },
    }
    contract = {
        "identifier_claims": {
            "source_id": {"namespace": "test_id", "claim_scope": "host"}
        },
        "field_profiles": {
            "test": [
                {
                    "pattern": "source_id",
                    "disposition": "identity",
                    "destination": "identifier_claim_evidence",
                    "reason": "source identity",
                },
                {
                    "pattern": "disposition",
                    "disposition": "domain",
                    "destination": "planet_lifecycle_evidence",
                    "reason": "candidate lifecycle",
                },
                {
                    "pattern": "note",
                    "disposition": "context",
                    "destination": "source_records",
                    "reason": "source context",
                },
                {
                    "pattern": "reference",
                    "disposition": "lineage",
                    "destination": "citations",
                    "reason": "source citation",
                },
                {
                    "pattern": "ignored",
                    "disposition": "exclude",
                    "destination": "source_records",
                    "reason": "retained only in source-native typed storage",
                },
                {
                    "pattern": "lineage_note",
                    "disposition": "lineage",
                    "destination": "source_records",
                    "reason": "retained release lineage",
                },
            ]
        }
    }

    snapshots = []
    for _ in range(2):
        with duckdb.connect() as con:
            compiler.create_schema(con)
            report = compiler.materialize_source(
                con, input_row, adapter, contract
            )
            records = con.execute(
                "select source_record_id, source_row_sha256, source_duplicate_count, "
                "source_context_json::varchar from source_records order by source_record_id"
            ).fetchall()
            dispositions = con.execute(
                "select source_field, source_native_field, mapping_status "
                "from source_field_dispositions "
                "order by source_field"
            ).fetchall()
            binding_count = con.execute(
                "select count(*) from object_binding_outcomes"
            ).fetchone()[0]
            binding_scopes = con.execute(
                "select distinct binding_scope from object_binding_outcomes order by 1"
            ).fetchall()
            identifier_claims = con.execute(
                "select identifier_normalized from identifier_claim_evidence "
                "order by evidence_id"
            ).fetchall()
            lifecycle = con.execute(
                "select disposition_normalized, evidence_polarity "
                "from planet_lifecycle_evidence order by disposition_normalized"
            ).fetchall()
            citation_count = con.execute(
                "select count(*) from citations"
            ).fetchone()[0]
            citation_link_count = con.execute(
                "select count(*) from evidence_citations"
            ).fetchone()[0]
        snapshots.append(records)
        assert report["source_rows"] == 3
        assert report["source_records"] == 2
        assert report["exact_duplicate_rows"] == 1
        assert sorted(row[2] for row in records) == [1, 2]
        assert {json.loads(row[3])["note"] for row in records} == {"alpha", "beta"}
        assert all(
            set(json.loads(row[3])) == {"note", "lineage_note"}
            for row in records
        )
        assert dispositions == [
            ("disposition", "disposition", "materialized"),
            ("ignored", "ignored", "excluded"),
            ("lineage_note", "lineage_note", "materialized"),
            ("note", "note", "materialized"),
            ("reference", "reference", "materialized"),
            ("source_id", "source_id", "materialized"),
        ]
        assert identifier_claims == [("1",), ("1",)]
        assert lifecycle == [
            ("CANDIDATE", "candidate"),
            ("FALSE_POSITIVE", "negative"),
        ]
        assert citation_count == 1
        assert citation_link_count == 1
        assert binding_count == 4
        assert binding_scopes == [("host",), ("object",)]
    assert snapshots[0] == snapshots[1]

    selected_adapter = json.loads(json.dumps(adapter))
    selected_adapter["tables"]["test_rows"]["row_selection"] = {
        "policy_id": "alpha_only_v1",
        "sql_predicate": "note = 'alpha'",
        "cache_selected_rows": True,
        "reason": "test selection",
    }
    with duckdb.connect() as con:
        compiler.create_schema(con)
        selected = compiler.materialize_source(
            con,
            input_row,
            selected_adapter,
            contract,
            materialization_cache_root=tmp_path / "scratch",
        )
        assert con.execute("select count(*) from source_records").fetchone()[0] == 1
    assert selected["input_source_rows"] == 3
    assert selected["source_rows"] == 2
    assert selected["excluded_by_row_selection"] == 1
    assert selected["tables"][0]["row_selection_policy"] == "alpha_only_v1"
    cache = selected["tables"][0]["selected_row_cache"]
    assert cache == {
        "enabled": True,
        "row_count": 2,
        "source_row_hash_mismatches": 0,
        "storage": "duckdb_temporary_table",
    }


def test_reproduction_comparison_uses_logical_content_not_runtime_database_bytes() -> None:
    report = {
        "build_id": "build",
        "contract_version": "contract",
        "compiler_version": "compiler",
        "compiler_sha256": "compiler-sha",
        "registry_sha256": "registry-sha",
        "runtime_versions": {"python": "3.14", "duckdb": "1.4"},
        "input_fingerprint": "inputs",
        "status": "in_progress",
        "sources": [],
        "mapping_status_counts": {"declared_pending": 1},
        "identifier_claim_counts_by_namespace": {"test_id": 1},
        "identifier_claim_counts_by_scope": {"host": 1},
        "identifier_normalization_rejections": {
            "total": 0,
            "by_source_table_field_namespace": [],
        },
        "binding_outcome_counts_by_status_and_scope": {
            "unresolved": {"host": 1, "object": 1}
        },
        "lifecycle_claim_counts": {
            "by_disposition": {"CANDIDATE": 1},
            "by_polarity": {"candidate": 1},
        },
        "relation_claim_counts": {
            "by_kind_and_polarity": {},
            "with_strict_probability": 0,
            "with_confidence_statistic": 0,
        },
        "citation_summary": {"citations": 1, "evidence_links": 1},
        "logical_content_sha256": "logical",
        "scientific_content_sha256": "scientific",
        "logical_hash_algorithm": "sha256_bucketed_multiset_v1",
        "tables": [{"table": "source_records", "row_count": 1, "logical_sha256": "a"}],
        "created_at": "2026-07-19T00:00:00Z",
        "database_sha256": "runtime-only-a",
    }
    reproduced = dict(report)
    reproduced["database_sha256"] = "runtime-only-b"
    assert reproduction.compare_reports(report, reproduced) == []
    reproduced["logical_content_sha256"] = "changed"
    assert reproduction.compare_reports(report, reproduced) == [
        "logical_content_sha256"
    ]
    reproduced = dict(report)
    reproduced["external_memberships"] = {"spectroscopy.test": {}}
    assert reproduction.compare_reports(report, reproduced) == [
        "external_memberships"
    ]
    reproduced = dict(report, scientific_content_sha256="changed")
    assert reproduction.compare_reports(report, reproduced) == [
        "scientific_content_sha256"
    ]


def test_duckdb_temporary_directory_can_use_operator_scratch(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_temporary = tmp_path / "artifact"
    artifact_temporary.mkdir()
    external_root = tmp_path / "external"
    monkeypatch.setenv("SPACEGATE_E4_TEMP_DIRECTORY", str(external_root))
    temporary, policy = compiler.create_duckdb_temporary_directory(
        artifact_temporary, "build"
    )
    assert policy == "external_operator_scratch"
    assert temporary.parent == external_root
    assert temporary.name.startswith("scientific-evidence-build.")

    monkeypatch.delenv("SPACEGATE_E4_TEMP_DIRECTORY")
    local, local_policy = compiler.create_duckdb_temporary_directory(
        artifact_temporary, "local"
    )
    assert local_policy == "artifact_family_staging"
    assert local == artifact_temporary / ".duckdb_tmp"


def test_bucketed_logical_hash_is_order_independent_and_duplicate_sensitive() -> None:
    with duckdb.connect() as con:
        con.execute("create table first_rows (id integer, value varchar)")
        con.execute("create table second_rows (id integer, value varchar)")
        con.execute("insert into first_rows values (1, 'a'), (2, 'b')")
        con.execute("insert into second_rows values (2, 'b'), (1, 'a')")
        reference = con.execute(
            """
            with row_hashes as (
              select sha256(to_json(t)) row_hash from first_rows t
            ), bucket_hashes as (
              select substr(row_hash,1,2) bucket, count(*) row_count,
                sha256(string_agg(row_hash,'' order by row_hash)) bucket_hash
              from row_hashes group by bucket
            )
            select sha256(string_agg(
              bucket || ':' || row_count || ':' || bucket_hash,
              '|' order by bucket)) from bucket_hashes
            """
        ).fetchone()[0]
        first = compiler.table_logical_report(con, "first_rows")
        second = compiler.table_logical_report(con, "second_rows")
        assert first["logical_sha256"] == reference
        assert first["logical_sha256"] == second["logical_sha256"]
        assert first["logical_hash_algorithm"] == compiler.LOGICAL_HASH_ALGORITHM
        con.execute("insert into second_rows values (1, 'a')")
        duplicated = compiler.table_logical_report(con, "second_rows")
        con.execute("create table empty_rows (id integer)")
        empty = compiler.table_logical_report(con, "empty_rows")
    assert duplicated["row_count"] == 3
    assert duplicated["logical_sha256"] != first["logical_sha256"]
    assert empty["row_count"] == 0
    assert empty["logical_sha256"] == hashlib.sha256(b"").hexdigest()


def test_streaming_logical_hash_does_not_allocate_persistent_blocks(
    tmp_path: Path,
) -> None:
    database = tmp_path / "hash.duckdb"
    scratch = tmp_path / "hash-tmp"
    scratch.mkdir()
    with duckdb.connect(str(database)) as con:
        con.execute(f"set temp_directory='{scratch}'")
        con.execute(
            "create table rows as select i id, repeat('evidence-', 20) payload "
            "from range(10000) t(i)"
        )
        con.execute("checkpoint")
        before = compiler.database_block_report(con)
        report = compiler.table_logical_report(con, "rows")
        after = compiler.database_block_report(con)
    assert report["row_count"] == 10_000
    assert after == before


def test_artifact_audit_rejects_invalid_relation_probability() -> None:
    with duckdb.connect() as con:
        compiler.create_schema(con)
        assert artifact_audit.audit_evidence(con)["status"] == "pass"
        con.execute(
            """
            insert into relation_claim_evidence (
              evidence_id, source_record_id,
              left_identity_namespace, left_identity_raw, left_component_scope,
              right_identity_namespace, right_identity_raw, right_component_scope,
              relation_kind, relation_scope, probability, probability_semantics,
              evidence_polarity
            ) values (
              'evidence', 'record', 'test', 'left', 'left', 'test', 'right', 'right',
              'candidate_test', 'pair', 2.0, 'strict probability', 'candidate'
            )
            """
        )
        report = artifact_audit.audit_evidence(con)
    assert report["status"] == "fail"
    assert report["checks"]["strict_probabilities_outside_unit_interval"] == 1


def test_artifact_audit_rejects_empty_or_orphaned_orbital_solutions() -> None:
    with duckdb.connect() as con:
        compiler.create_schema(con)
        con.execute(
            "insert into orbital_solution_evidence "
            "(evidence_id,source_record_id,relation_claim_id,solution_key) "
            "values ('orbit','record','missing-relation','solution')"
        )
        report = artifact_audit.audit_evidence(con)
    assert report["status"] == "fail"
    assert report["checks"]["empty_orbital_solution_parameter_sets"] == 1
    assert report["checks"]["orphan_orbital_solution_relations"] == 1


def test_artifact_audit_rejects_extended_object_without_geometry() -> None:
    with duckdb.connect() as con:
        compiler.create_schema(con)
        con.execute(
            "insert into extended_object_evidence "
            "(evidence_id,source_record_id,extended_kind) "
            "values ('extended','record','test')"
        )
        report = artifact_audit.audit_evidence(con)
    assert report["status"] == "fail"
    assert report["checks"]["empty_extended_object_geometry"] == 1


def test_artifact_audit_rejects_compact_object_without_parameters() -> None:
    with duckdb.connect() as con:
        compiler.create_schema(con)
        con.execute(
            "insert into compact_object_evidence "
            "(evidence_id,source_record_id,compact_kind) "
            "values ('compact','record','test')"
        )
        report = artifact_audit.audit_evidence(con)
    assert report["status"] == "fail"
    assert report["checks"]["empty_compact_object_parameter_sets"] == 1


def test_logical_key_expression_qualifies_source_id_against_lineage_alias() -> None:
    expression = compiler.logical_key_expression(["source_id"], "t")
    with duckdb.connect() as con:
        value = con.execute(
            f"select {expression} from (values ('catalog-id')) t(source_id) "
            "cross join (values ('lineage-id')) r(source_id)"
        ).fetchone()[0]
    assert json.loads(value) == {"source_id": "catalog-id"}


def test_refuted_planet_claim_is_negative_evidence() -> None:
    expression = compiler.lifecycle_polarity_expression("disposition")
    with duckdb.connect() as con:
        polarity = con.execute(
            f"select {expression} from (select 'REFUTED' disposition)"
        ).fetchone()[0]
    assert polarity == "negative"


def test_source_field_metadata_falls_back_to_source_native_fits_schema() -> None:
    fields = compiler.source_field_metadata(
        {
            "source_name": "fits_rows",
            "source_schema": {
                "source_schema": [
                    {
                        "name": "source_id1",
                        "arrow_type": "int64",
                        "source_format": "K",
                        "unit": None,
                    },
                    {
                        "name": "sep_AU",
                        "arrow_type": "double",
                        "source_format": "D",
                        "unit": "AU",
                    },
                ]
            },
        },
        {},
    )
    assert fields == [
        {
            "column_name": "source_id1",
            "datatype": "int64",
            "unit": None,
            "ucd": None,
            "description": None,
        },
        {
            "column_name": "sep_AU",
            "datatype": "double",
            "unit": "AU",
            "ucd": None,
            "description": None,
        },
    ]
    fields_with_lineage = compiler.source_field_metadata(
        {
            "source_name": "source_rows",
            "source_schema": {"source_schema": [{"name": "value", "unit": "K"}]},
            "columns": [
                {"name": "source_line_number", "type": "BIGINT"},
                {"name": "value", "type": "VARCHAR"},
                {"name": "raw_row", "type": "VARCHAR"},
            ],
        },
        {},
    )
    assert [field["column_name"] for field in fields_with_lineage] == [
        "source_line_number",
        "value",
        "raw_row",
    ]
    assert fields_with_lineage[1]["unit"] == "K"


def test_relation_claim_preserves_non_probability_statistic_and_control_polarity(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "wide_pairs.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            "(11::bigint,12::bigint,0.1::double,100::double),"
            "(21::bigint,22::bigint,4.2::double,200::double)) "
            "t(source_id1,source_id2,R_chance_align,sep_AU)) "
            f"to '{parquet}' (format parquet)"
        )
        rows = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchall()
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'test.wide', 'r1', 'control', 'control_relation', "
            "'{}', '{}', ?, 1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [
                [f"record-{index}", row_hash]
                for index, (row_hash,) in enumerate(rows)
            ],
        )
        consumed = compiler.materialize_relation_claims(
            con,
            source_id="test.wide",
            release_id="r1",
            table_name="control",
            path=parquet,
            relation_claim={
                "left_identifier_field": "source_id1",
                "left_identifier_namespace": "gaia_edr3_source_id",
                "left_component_scope": "left",
                "right_identifier_field": "source_id2",
                "right_identifier_namespace": "gaia_edr3_source_id",
                "right_component_scope": "right",
                "relation_kind": "shifted_control",
                "relation_scope": "stellar_pair_control",
                "evidence_polarity": "negative_control",
                "method": "test_kde",
                "reference_raw": "test reference",
                "confidence_statistic_field": "R_chance_align",
                "confidence_statistic_key": "chance_alignment_density_ratio",
                "confidence_statistic_unit": "dimensionless",
                "confidence_statistic_semantics": "not a strict probability",
                "quality_fields": ["sep_AU", "R_chance_align"],
            },
            available_fields={
                "source_id1",
                "source_id2",
                "R_chance_align",
                "sep_AU",
            },
        )
        evidence = con.execute(
            "select left_identity_raw, right_identity_raw, probability, "
            "confidence_statistic_value, evidence_polarity, quality_json::varchar "
            "from relation_claim_evidence order by left_identity_raw"
        ).fetchall()
    assert consumed == {
        "source_id1",
        "source_id2",
        "R_chance_align",
        "sep_AU",
    }
    assert [(row[0], row[1], row[2], row[3], row[4]) for row in evidence] == [
        ("11", "12", None, 0.1, "negative_control"),
        ("21", "22", None, 4.2, "negative_control"),
    ]
    assert all(json.loads(row[5])["sep_AU"] in {100.0, 200.0} for row in evidence)


def test_relation_claim_source_fields_do_not_collide_with_lineage_columns(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "external_crossmatch.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select 123::bigint source_id, 456::bigint external_id, "
            f"0.25::double angular_distance) to '{parquet}' (format parquet)"
        )
        row_hash = con.execute(
            f"select sha256(to_json(source_row)) from read_parquet('{parquet}') "
            "source_row"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record', 'test.external', 'r1', 'best_neighbour', 'crossmatch', "
            "'{}', '{}', ?, 1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [row_hash],
        )
        consumed = compiler.materialize_relation_claims(
            con,
            source_id="test.external",
            release_id="r1",
            table_name="best_neighbour",
            path=parquet,
            relation_claim={
                "left_identifier_field": "source_id",
                "left_identifier_namespace": "gaia_dr3_source_id",
                "left_component_scope": "gaia_source",
                "right_identifier_field": "external_id",
                "right_identifier_namespace": "external_id",
                "right_component_scope": "external_source",
                "relation_kind": "best_neighbour",
                "relation_scope": "catalog_crossmatch",
                "evidence_polarity": "candidate",
                "method": "official_crossmatch",
                "reference_raw": "test reference",
                "confidence_statistic_field": "angular_distance",
                "confidence_statistic_key": "angular_distance",
                "confidence_statistic_unit": "arcsec",
                "confidence_statistic_semantics": "source angular separation",
                "quality_fields": ["source_id", "angular_distance"],
            },
            available_fields={"source_id", "external_id", "angular_distance"},
        )
        evidence = con.execute(
            "select left_identity_raw, right_identity_raw, "
            "confidence_statistic_value, quality_json->>'source_id' "
            "from relation_claim_evidence"
        ).fetchone()
    assert consumed == {"source_id", "external_id", "angular_distance"}
    assert evidence == ("123", "456", 0.25, "123")


def test_relation_claim_predicate_retains_source_rows_but_bounds_claims(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "hierarchy.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values ('1','2'),('2',null)) t(sn,parent)) "
            f"to '{parquet}' (format parquet)"
        )
        rows = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t order by sn"
        ).fetchall()
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'test.hierarchy', 'r1', 'configurations', 'hierarchy_claim', "
            "'{}', '{}', ?, 1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [[f"record-{index}", row_hash] for index, (row_hash,) in enumerate(rows)],
        )
        compiler.materialize_relation_claims(
            con,
            source_id="test.hierarchy",
            release_id="r1",
            table_name="configurations",
            path=parquet,
            relation_claim={
                "left_identifier_field": "sn",
                "left_identifier_namespace": "source_component",
                "left_component_scope": "subsystem",
                "right_identifier_field": "parent",
                "right_identifier_namespace": "source_component",
                "right_component_scope": "parent_subsystem",
                "relation_kind": "hierarchical_parent",
                "relation_scope": "source_configuration",
                "evidence_polarity": "positive",
                "method": "source_configuration",
                "reference_raw": "source reference",
                "sql_predicate": "parent is not null",
            },
            available_fields={"sn", "parent"},
        )
        assert con.execute("select count(*) from source_records").fetchone()[0] == 2
        assert con.execute(
            "select left_identity_raw,right_identity_raw from relation_claim_evidence"
        ).fetchall() == [("1", "2")]


def test_relation_claim_supports_composite_endpoints_and_dynamic_polarity(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "msc_pairs.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            "('00001+0001','A','B','V'),"
            "('00002+0002','A','B','X')) "
            "t(WDS,\"Primary\",\"Secondary\",Type)) "
            f"to '{parquet}' (format parquet)"
        )
        rows = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t order by WDS"
        ).fetchall()
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'multiplicity.msc', 'r1', 'msc_sys', 'source_pair', "
            "'{}', '{}', ?, 1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [[f"record-{index}", row_hash] for index, (row_hash,) in enumerate(rows)],
        )
        consumed = compiler.materialize_relation_claims(
            con,
            source_id="multiplicity.msc",
            release_id="r1",
            table_name="msc_sys",
            path=parquet,
            relation_claim={
                "left_identifier_fields": ["WDS", "Primary"],
                "left_identifier_delimiter": ":",
                "left_identifier_namespace": "msc_component",
                "left_component_scope": "primary_endpoint",
                "right_identifier_fields": ["WDS", "Secondary"],
                "right_identifier_delimiter": ":",
                "right_identifier_namespace": "msc_component",
                "right_component_scope": "secondary_endpoint",
                "relation_kind": "source_binary_pair",
                "relation_scope": "source_hierarchy",
                "evidence_polarity_sql": (
                    "case when Type='X' then 'negative' else 'positive' end"
                ),
                "method": "msc_source_relation",
                "reference_raw": "source reference",
                "quality_fields": ["Type"],
            },
            available_fields={"WDS", "Primary", "Secondary", "Type"},
        )
        evidence = con.execute(
            "select left_identity_raw,right_identity_raw,evidence_polarity,"
            "quality_json->>'source_evidence_polarity' "
            "from relation_claim_evidence order by left_identity_raw"
        ).fetchall()
    assert consumed == {"WDS", "Primary", "Secondary", "Type"}
    assert evidence == [
        ("00001+0001:A", "00001+0001:B", "positive", "positive"),
        ("00002+0002:A", "00002+0002:B", "negative", "negative"),
    ]


def test_scoped_stellar_evidence_supports_dynamic_component_scope(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "msc_components.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select '00001+0001' WDS, 'Aa' Comp, 'M3V' Sp, "
            f"0.42::double Mass) to '{parquet}' (format parquet)"
        )
        row_hash = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record', 'multiplicity.msc', 'r1', 'msc_comp', 'component', "
            "'{}', '{}', ?, 1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [row_hash],
        )
        consumed = compiler.materialize_scoped_stellar_evidence(
            con,
            source_id="multiplicity.msc",
            release_id="r1",
            table_name="msc_comp",
            path=parquet,
            parameter_sets=[
                {
                    "scope_key": "source_component",
                    "component_scope_fields": ["WDS", "Comp"],
                    "component_scope_delimiter": ":",
                    "parameter_set_kind": "msc_component_physics",
                    "classification_field": "Sp",
                    "classification_scheme": "spectral_type",
                    "method": "msc_source_component",
                    "reference_raw": "2026TEST...1A",
                    "normalization_version": "msc_source_native_v1",
                    "measurements": [
                        {
                            "value_field": "Mass",
                            "quantity_key": "mass",
                            "unit_raw": "solMass",
                        }
                    ],
                }
            ],
            available_fields={"WDS", "Comp", "Sp", "Mass"},
        )
        scopes = con.execute(
            "select component_scope from stellar_parameter_sets"
        ).fetchall()
        classifications = con.execute(
            "select component_scope,classification_raw "
            "from stellar_classification_evidence"
        ).fetchall()
        references = con.execute(
            "select distinct reference_raw from stellar_parameter_evidence"
        ).fetchall()
    assert consumed == {"WDS", "Comp", "Sp", "Mass"}
    assert scopes == [("00001+0001:Aa",)]
    assert classifications == [("00001+0001:Aa", "M3V")]
    assert references == [("2026TEST...1A",)]


def test_numeric_zero_missing_semantics_reject_signed_zero_lexemes() -> None:
    predicate = compiler.missing_value_predicate(
        "value_raw", [], zero_is_missing=True
    )
    with duckdb.connect() as con:
        retained = con.execute(
            "select value_raw from (values ('0'),('0.0'),('-0.0'),('1.0')) "
            f"rows(value_raw) where {predicate} order by value_raw"
        ).fetchall()
    assert retained == [("1.0",)]


def test_configured_measurement_validity_bounds_reject_sentinels_and_bad_angles() -> None:
    predicate = compiler.configured_measurement_predicate(
        "value_raw",
        {"missing_values": ["-1"], "minimum_value": 0, "maximum_value": 359},
    )
    with duckdb.connect() as con:
        retained = con.execute(
            "select value_raw from (values ('-1'),('0'),('180'),('359'),('360'),('.') ) "
            f"rows(value_raw) where {predicate} order by try_cast(value_raw as integer)"
        ).fetchall()
    assert retained == [("0",), ("180",), ("359",)]


def test_relation_audit_accepts_source_native_primary_secondary_scopes() -> None:
    with duckdb.connect() as con:
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record', 'multiplicity.test', 'r1', 'systems', 'binary', "
            "'{}', '{}', 'row-hash', 1, 'raw', 'typed', 'raw-tree', "
            "'typed-table', timestamp '2026-07-19 00:00:00')"
        )
        con.execute(
            "insert into relation_claim_evidence ("
            "evidence_id,source_record_id,left_identity_namespace,left_identity_raw,"
            "left_component_scope,right_identity_namespace,right_identity_raw,"
            "right_component_scope,relation_kind,relation_scope,evidence_polarity) "
            "values ('relation','record','component','system:primary','primary',"
            "'component','system:secondary','secondary','binary','pair','positive')"
        )
        con.execute(
            "insert into identifier_claim_evidence "
            "(evidence_id,source_record_id,namespace,identifier_raw,"
            "identifier_normalized,claim_scope,component_scope) values "
            "('primary-claim','record','component','system:primary',"
            "'system:primary','star','primary'),"
            "('secondary-claim','record','component','system:secondary',"
            "'system:secondary','star','secondary')"
        )
        con.execute(
            "insert into object_binding_outcomes "
            "(binding_outcome_id,source_record_id,binding_status,binding_scope,"
            "component_scope,reason,provenance_json) values "
            "('primary-binding','record','unresolved','star','primary',"
            "'test unresolved','{}'),"
            "('secondary-binding','record','unresolved','star','secondary',"
            "'test unresolved','{}')"
        )
        report = artifact_audit.audit_evidence(con)
    assert report["checks"]["relation_endpoints_without_identifier_claims"] == 0
    assert report["checks"]["relation_endpoints_without_binding_scopes"] == 0
    assert report["status"] == "pass"


def test_orbital_solution_preserves_one_coherent_source_parameter_set(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "orbit.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select '00021-6817' wds_id, 'I 699AB' pair_designation, "
            "'290.0' period_raw, '1884.54' epoch_raw, 'Zir2013d' reference_code, "
            "'Orbital' model_name, '2' grade_raw) "
            f"to '{parquet}' (format parquet)"
        )
        source_row_sha256 = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record','test.orbits','r1','orbits','orbital_solution',"
            "'{}','{}',?,1,'raw','typed','raw-tree','typed-table',"
            "timestamp '2026-07-19 00:00:00')",
            [source_row_sha256],
        )
        consumed = compiler.materialize_orbital_solutions(
            con,
            source_id="test.orbits",
            release_id="r1",
            table_name="orbits",
            path=parquet,
            fields=[
                {"column_name": "period_raw"},
                {"column_name": "epoch_raw"},
                {"column_name": "reference_code"},
                {"column_name": "model_name"},
                {"column_name": "grade_raw"},
            ],
            orbital_solution={
                "solution_key_fields": [
                    "wds_id",
                    "pair_designation",
                    "reference_code",
                ],
                "parameter_fields": ["period_raw", "epoch_raw"],
                "quality_fields": ["grade_raw"],
                "epoch_field": "epoch_raw",
                "frame": "ICRS J2016.0",
                "model_field": "model_name",
                "reference_field": "reference_code",
                "method": "published_visual_orbit_solution",
                "normalization_version": "source_native_v1",
            },
            available_fields={
                "wds_id",
                "pair_designation",
                "period_raw",
                "epoch_raw",
                "reference_code",
                "model_name",
                "grade_raw",
            },
        )
        row = con.execute(
            "select relation_claim_id,solution_key,parameter_set_raw::varchar,"
            "epoch_raw,frame_raw,model,reference_raw,quality_json::varchar "
            "from orbital_solution_evidence"
        ).fetchone()
    assert consumed == {
        "wds_id",
        "pair_designation",
        "period_raw",
        "epoch_raw",
        "reference_code",
        "model_name",
        "grade_raw",
    }
    assert row[0] is None
    assert json.loads(row[1]) == {
        "wds_id": "00021-6817",
        "pair_designation": "I 699AB",
        "reference_code": "Zir2013d",
    }
    assert json.loads(row[2]) == {"period_raw": "290.0", "epoch_raw": "1884.54"}
    assert row[3:7] == ("1884.54", "ICRS J2016.0", "Orbital", "Zir2013d")
    assert json.loads(row[7]) == {"grade_raw": "2"}


def test_orbital_solution_links_exactly_one_relation_by_source_logical_key(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "orbits.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values ('1','2','5.0','reference')) "
            "t(Seq,o,Per,Ref)) "
            f"to '{parquet}' (format parquet)"
        )
        orbit_hash = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('system-record','multiplicity.test','r1','systems','binary',"
            "'{\"Seq\":\"1\"}','{}','system-hash',1,'raw','typed',"
            "'raw-tree','typed-table',timestamp '2026-07-19 00:00:00'),"
            "('orbit-record','multiplicity.test','r1','orbits','orbit',"
            "'{\"Seq\":\"1\",\"o\":\"2\"}','{}',?,1,'raw','typed',"
            "'raw-tree','typed-table',timestamp '2026-07-19 00:00:00')",
            [orbit_hash],
        )
        con.execute(
            "insert into relation_claim_evidence ("
            "evidence_id,source_record_id,left_identity_namespace,left_identity_raw,"
            "left_component_scope,right_identity_namespace,right_identity_raw,"
            "right_component_scope,relation_kind,relation_scope,evidence_polarity) "
            "values ('relation','system-record','component','system:primary','primary',"
            "'component','system:secondary','secondary','binary','pair','positive')"
        )
        compiler.materialize_orbital_solutions(
            con,
            source_id="multiplicity.test",
            release_id="r1",
            table_name="orbits",
            path=parquet,
            fields=[{"column_name": "Per"}, {"column_name": "Ref"}],
            orbital_solution={
                "solution_key_fields": ["Seq", "o"],
                "parameter_fields": ["Per"],
                "quality_fields": ["o"],
                "reference_field": "Ref",
                "relation_link": {
                    "source_table": "systems",
                    "key_fields": {"Seq": "Seq"},
                    "required": True,
                },
                "method": "source_orbit",
                "normalization_version": "source_native_v1",
            },
            available_fields={"Seq", "o", "Per", "Ref"},
        )
        linked = con.execute(
            "select relation_claim_id from orbital_solution_evidence"
        ).fetchone()[0]
    assert linked == "relation"


def test_scoped_stellar_parameter_sets_keep_binary_components_separate(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "components.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select 'M4V' spectral_1, 'none' spectral_2, 'DA2' spectral_3, "
            "'-0.65' log_mass_1, '0.01' log_mass_error_1, "
            "'-9.99' log_mass_2, '-9.99' log_mass_error_2) "
            f"to '{parquet}' (format parquet)"
        )
        source_row_sha256 = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record','test.binary','r1','components','binary_system',"
            "'{}','{}',?,1,'raw','typed','raw-tree','typed-table',"
            "timestamp '2026-07-19 00:00:00')",
            [source_row_sha256],
        )
        consumed = compiler.materialize_scoped_stellar_evidence(
            con,
            source_id="test.binary",
            release_id="r1",
            table_name="components",
            path=parquet,
            parameter_sets=[
                {
                    "component_scope": "primary",
                    "parameter_set_kind": "dynamical_component",
                    "classification_field": "spectral_1",
                    "classification_missing_values": ["none"],
                    "method": "published_binary_solution",
                    "normalization_version": "source_native_v1",
                    "measurements": [
                        {
                            "value_field": "log_mass_1",
                            "uncertainty_field": "log_mass_error_1",
                            "quantity_key": "log10_mass",
                            "unit_raw": "dex(M_sun)",
                            "missing_values": ["-9.99"],
                        }
                    ],
                },
                {
                    "component_scope": "secondary",
                    "parameter_set_kind": "dynamical_component",
                    "classification_field": "spectral_2",
                    "classification_missing_values": ["none"],
                    "method": "published_binary_solution",
                    "normalization_version": "source_native_v1",
                    "measurements": [
                        {
                            "value_field": "log_mass_2",
                            "uncertainty_field": "log_mass_error_2",
                            "quantity_key": "log10_mass",
                            "unit_raw": "dex(M_sun)",
                            "missing_values": ["-9.99"],
                        }
                    ],
                },
                {
                    "component_scope": "tertiary",
                    "parameter_set_kind": "published_component_classification",
                    "classification_field": "spectral_3",
                    "method": "published_binary_classification",
                    "normalization_version": "source_native_v1",
                    "measurements": [],
                },
            ],
            available_fields={
                "spectral_1",
                "spectral_2",
                "spectral_3",
                "log_mass_1",
                "log_mass_error_1",
                "log_mass_2",
                "log_mass_error_2",
            },
        )
        evidence = con.execute(
            "select component_scope,quantity_key,value_raw,uncertainty_lower "
            "from stellar_parameter_evidence"
        ).fetchall()
        classifications = con.execute(
            "select component_scope,classification_raw "
            "from stellar_classification_evidence"
        ).fetchall()
        parameter_sets = con.execute(
            "select component_scope from stellar_parameter_sets"
        ).fetchall()
    assert consumed == {
        "spectral_1",
        "spectral_2",
        "spectral_3",
        "log_mass_1",
        "log_mass_error_1",
        "log_mass_2",
        "log_mass_error_2",
    }
    assert evidence == [("primary", "log10_mass", "-0.65", 0.01)]
    assert classifications == [("primary", "M4V"), ("tertiary", "DA2")]
    assert parameter_sets == [("primary",)]


def test_missing_uncertainty_sentinel_does_not_become_large_uncertainty() -> None:
    expression = compiler.nullable_measurement_double_expression(
        "uncertainty", ["-9.99", "-9.9900"], absolute=True
    )
    with duckdb.connect() as con:
        rows = con.execute(
            f"select {expression} from (values ('-9.9900'),('-0.25')) t(uncertainty)"
        ).fetchall()
    assert rows == [(None,), (0.25,)]


def test_scoped_stellar_evidence_preserves_interval_endpoints(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "stellar_bounds.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select 1::bigint source_id, 10.0 measurement_value, "
            f"9.0 lower_bound, 11.0 upper_bound) "
            f"to '{parquet}' (format parquet)"
        )
        source_hash = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record','test.stellar','r1','parameters','star',"
            "'{}','{}',?,1,'raw','typed','raw-tree','typed-table',"
            "timestamp '2026-07-20 00:00:00')",
            [source_hash],
        )
        consumed = compiler.materialize_scoped_stellar_evidence(
            con,
            source_id="test.stellar",
            release_id="r1",
            table_name="parameters",
            path=parquet,
            parameter_sets=[
                {
                    "scope_key": "model",
                    "parameter_set_kind": "source_model",
                    "method": "source_pipeline",
                    "normalization_version": "source_native_v1",
                    "measurements": [
                        {
                            "value_field": "measurement_value",
                            "uncertainty_lower_field": "lower_bound",
                            "uncertainty_upper_field": "upper_bound",
                            "uncertainty_field_semantics": "interval_endpoints",
                            "bound_semantics": "posterior_interval_endpoints",
                            "quantity_key": "effective_temperature",
                            "unit_raw": "K",
                        }
                    ],
                }
            ],
            available_fields={
                "source_id",
                "measurement_value",
                "lower_bound",
                "upper_bound",
            },
        )
        row = con.execute(
            "select normalized_value,uncertainty_lower,uncertainty_upper,"
            "bound_semantics,quality_json::varchar from stellar_parameter_evidence"
        ).fetchone()
    assert consumed == {"measurement_value", "lower_bound", "upper_bound"}
    assert row[:4] == (10.0, 9.0, 11.0, "posterior_interval_endpoints")
    assert json.loads(row[4])["uncertainty_field_semantics"] == "interval_endpoints"


def test_configured_photometry_preserves_dynamic_band_reference_and_quality(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "photometry.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select '8.4' mag, 'V' band, '2024A&A...1A' reference, "
            f"'primary' component, '0.1' mag_lower, '0.2' mag_upper) "
            f"to '{parquet}' (format parquet)"
        )
        source_row_sha256 = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record','test.photometry','r1','systems','binary_system',"
            "'{}','{}',?,1,'raw','typed','raw-tree','typed-table',"
            "timestamp '2026-07-19 00:00:00')",
            [source_row_sha256],
        )
        consumed = compiler.materialize_configured_photometry(
            con,
            source_id="test.photometry",
            release_id="r1",
            table_name="systems",
            path=parquet,
            measurements=[
                {
                    "value_field": "mag",
                    "quantity_key": "apparent_magnitude",
                    "uncertainty_lower_field": "mag_lower",
                    "uncertainty_upper_field": "mag_upper",
                    "bandpass_field": "band",
                    "reference_field": "reference",
                    "quality_fields": ["component"],
                    "unit_raw": "mag",
                }
            ],
            available_fields={
                "mag",
                "mag_lower",
                "mag_upper",
                "band",
                "reference",
                "component",
            },
        )
        row = con.execute(
            "select bandpass,reference_raw,uncertainty_lower,uncertainty_upper,"
            "quality_json::varchar "
            "from photometry_extinction_evidence"
        ).fetchone()
    assert consumed == {
        "mag",
        "mag_lower",
        "mag_upper",
        "band",
        "reference",
        "component",
    }
    assert row[:2] == ("V", "2024A&A...1A")
    assert row[2:4] == (0.1, 0.2)
    quality = json.loads(row[4])
    assert quality["component"] == "primary"
    assert quality["uncertainty_lower_field"] == "mag_lower"
    assert quality["uncertainty_upper_field"] == "mag_upper"


def test_scalar_grouping_preserves_measurement_companions() -> None:
    groups = compiler.scalar_field_groups(
        [
            "pl_rade",
            "pl_radeerr1",
            "pl_radeerr2",
            "pl_radelim",
            "pl_radestr",
        ],
        {"pl_rade", "pl_radeerr1", "pl_radeerr2", "pl_radelim", "pl_radestr"},
    )
    assert groups == [
        {
            "base_field": "pl_rade",
            "auxiliary": {
                "error_upper": "pl_radeerr1",
                "error_lower": "pl_radeerr2",
                "limit": "pl_radelim",
                "formatted": "pl_radestr",
            },
        }
    ]


def test_configured_astrometry_bundle_preserves_typed_measurements_and_citations(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "astrometry.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values (10.0, 9.0, 11.0, null)) "
            f"t(parallax,parallax_lower,parallax_upper,radial_velocity)) "
            f"to '{parquet}' (format parquet)"
        )
        source_hash = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record','test.catalog','r1','astrometry','star',"
            "'{}','{}',?,1,'raw','typed','raw-tree','typed-table',"
            "timestamp '2026-07-19 00:00:00')",
            [source_hash],
        )
        measurements = [
            {
                "destination": "astrometry_distance_evidence",
                "value_field": "parallax",
                "uncertainty_lower_field": "parallax_lower",
                "uncertainty_upper_field": "parallax_upper",
                "quantity_key": "parallax",
                "unit_raw": "mas",
                "normalized_unit": "mas",
                "bound_semantics": "posterior_interval_endpoints",
                "reference_raw": "2025A&A...1A",
                "method": "source_astrometry",
                "normalization_version": "source_native_v1",
            },
            {
                "destination": "astrometry_distance_evidence",
                "value_field": "radial_velocity",
                "quantity_key": "radial_velocity",
                "unit_raw": "km/s",
                "normalized_unit": "km s-1",
                "reference_raw": "2025A&A...1A",
                "method": "source_velocity",
                "normalization_version": "source_native_v1",
            },
        ]
        consumed = compiler.materialize_configured_domain_measurements(
            con,
            source_id="test.catalog",
            release_id="r1",
            table_name="astrometry",
            path=parquet,
            measurements=measurements,
            available_fields={
                "parallax",
                "parallax_lower",
                "parallax_upper",
                "radial_velocity",
            },
            storage_modes={
                "astrometry_distance_evidence": "typed_measurement_bundle_v1"
            },
        )
        citation_summary = compiler.materialize_citations(con)
        nested = con.execute(
            "select measurement.quantity_key,measurement.value_raw,"
            "measurement.uncertainty_lower,measurement.uncertainty_upper,"
            "measurement.bound_semantics,measurement.reference_raw,"
            "measurement.quality_json::varchar "
            "from astrometry_distance_evidence_bundles b, "
            "unnest(b.measurements) as nested(measurement)"
        ).fetchall()
        flat_count = con.execute(
            "select count(*) from astrometry_distance_evidence"
        ).fetchone()[0]
        audit = artifact_audit.audit_evidence(con)
    assert consumed == {
        "parallax",
        "parallax_lower",
        "parallax_upper",
        "radial_velocity",
    }
    assert [row[:6] for row in nested] == [
        (
            "parallax",
            "10.0",
            9.0,
            11.0,
            "posterior_interval_endpoints",
            "2025A&A...1A",
        )
    ]
    quality = json.loads(nested[0][6])
    assert quality["uncertainty_field"] is None
    assert quality["uncertainty_lower_field"] == "parallax_lower"
    assert quality["uncertainty_upper_field"] == "parallax_upper"
    assert flat_count == 0
    assert citation_summary == {"citations": 1, "evidence_links": 1}
    assert audit["status"] == "pass"


def test_configured_coordinates_normalize_sexagesimal_values_with_source_context(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "coordinates.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            f"(1, 0, 0.0, '+', -30, 30, 0.0, ':', 'Hip')) "
            f"t(rah,ram,ras,sign,ded,dem,des,accuracy,reference)) "
            f"to '{parquet}' (format parquet)"
        )
        compiler.create_schema(con)
        con.execute(
            f"insert into source_records select "
            f"'record','test.catalog','r1','coordinates','star',"
            f"'{{}}','{{}}',sha256(to_json(t)),1,'raw','typed','raw-tree',"
            f"'typed-table',timestamp '2026-07-19 00:00:00' "
            f"from read_parquet('{parquet}') t"
        )
        consumed = compiler.materialize_configured_coordinate_measurements(
            con,
            source_id="test.catalog",
            release_id="r1",
            table_name="coordinates",
            path=parquet,
            measurements=[
                {
                    "quantity_key": "right_ascension",
                    "coordinate_kind": "right_ascension_hms",
                    "component_fields": ["rah", "ram", "ras"],
                    "unit_raw": "sexagesimal_hms",
                    "normalized_unit": "deg",
                    "frame_raw": "ICRS",
                    "epoch_raw": "J2000.0",
                    "quality_fields": ["accuracy"],
                    "reference_field": "reference",
                    "method": "catalog_position",
                    "normalization_version": "sexagesimal_degrees_v1",
                },
                {
                    "quantity_key": "declination",
                    "coordinate_kind": "declination_dms",
                    "component_fields": ["sign", "ded", "dem", "des"],
                    "unit_raw": "sexagesimal_dms",
                    "normalized_unit": "deg",
                    "frame_raw": "ICRS",
                    "epoch_raw": "J2000.0",
                    "quality_fields": ["accuracy"],
                    "reference_field": "reference",
                    "method": "catalog_position",
                    "normalization_version": "sexagesimal_degrees_v1",
                },
            ],
            available_fields={
                "rah", "ram", "ras", "sign", "ded", "dem", "des",
                "accuracy", "reference",
            },
        )
        evidence = con.execute(
            "select quantity_key,normalized_value,normalized_unit,frame_raw,"
            "epoch_raw,reference_raw,quality_json->>'$.normalization_valid',"
            "quality_json->>'$.embedded_degree_sign' "
            "from astrometry_distance_evidence order by quantity_key"
        ).fetchall()
    assert consumed == {
        "rah", "ram", "ras", "sign", "ded", "dem", "des", "accuracy",
        "reference",
    }
    assert evidence == [
        ("declination", -30.5, "deg", "ICRS", "J2000.0", "Hip", "true", "true"),
        ("right_ascension", 15.0, "deg", "ICRS", "J2000.0", "Hip", "true", "false"),
    ]


def test_citation_catalog_aggregates_repeated_key_lines_deterministically(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "references.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values (2,'R1','second'),(1,'R1','first')) "
            f"t(source_line_number,reference,text)) to '{parquet}' (format parquet)"
        )
        compiler.create_schema(con)
        con.execute(
            f"insert into source_records select "
            f"sha256('record|' || sha256(to_json(t))),"
            f"'test.catalog','r1','references','source_reference',"
            f"to_json(struct_pack(reference := reference)),to_json(struct_pack(text := text)),"
            f"sha256(to_json(t)),1,'raw','typed','raw-tree','typed-table',"
            f"timestamp '2026-07-19 00:00:00' from read_parquet('{parquet}') t"
        )
        consumed = compiler.materialize_citation_catalog(
            con,
            source_id="test.catalog",
            release_id="r1",
            table_name="references",
            path=parquet,
            citation_catalog={
                "reference_key_field": "reference",
                "citation_text_field": "text",
                "aggregate_repeated_key_lines": True,
                "line_order_field": "source_line_number",
                "line_separator": " ",
            },
            fields=[
                {"column_name": "reference"},
                {"column_name": "text"},
            ],
            available_fields={"source_line_number", "reference", "text"},
        )
        citation = con.execute(
            "select source_reference_key,citation_text_raw,"
            "parsed_json->'$.source_context'->>'$.line_count' from citations"
        ).fetchone()
    assert consumed == {"source_line_number", "reference", "text"}
    assert citation == ("R1", "first second", "2")


def test_composite_identifier_predicate_keeps_suffixed_identity_distinct(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "identifiers.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values ('10360',null),('10360','A')) "
            f"t(number,suffix)) to '{parquet}' (format parquet)"
        )
        compiler.create_schema(con)
        con.execute(
            f"insert into source_records select sha256('record|' || sha256(to_json(t))),"
            f"'test.catalog','r1','identifiers','object','{{}}','{{}}',"
            f"sha256(to_json(t)),1,'raw','typed','raw-tree','typed-table',"
            f"timestamp '2026-07-19 00:00:00' from read_parquet('{parquet}') t"
        )
        consumed = compiler.materialize_composite_identifier_claims(
            con,
            source_id="test.catalog",
            release_id="r1",
            table_name="identifiers",
            path=parquet,
            claims=[
                {
                    "fields": ["number"],
                    "prefix": "NSV ",
                    "namespace": "nsv_designation",
                    "claim_scope": "object",
                    "sql_predicate": "suffix is null",
                },
                {
                    "fields": ["number", "suffix"],
                    "prefix": "NSV ",
                    "namespace": "nsv_designation",
                    "claim_scope": "component",
                },
            ],
            available_fields={"number", "suffix"},
        )
        claims = con.execute(
            "select identifier_normalized,claim_scope from identifier_claim_evidence "
            "order by identifier_normalized"
        ).fetchall()
    assert consumed == {"number", "suffix"}
    assert claims == [("NSV 10360", "object"), ("NSV 10360A", "component")]


def test_configured_lexical_measurement_does_not_normalize_numeric_looking_code(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "lexical.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select '00001' reference_code) to '{parquet}' (format parquet)"
        )
        compiler.create_schema(con)
        source_hash = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        con.execute(
            "insert into source_records values "
            "('record','test.catalog','r1','lexical','object','{}','{}',?,1,"
            "'raw','typed','raw-tree','typed-table',timestamp '2026-07-19 00:00:00')",
            [source_hash],
        )
        compiler.materialize_configured_domain_measurements(
            con,
            source_id="test.catalog",
            release_id="r1",
            table_name="lexical",
            path=parquet,
            measurements=[
                {
                    "destination": "variability_activity_rotation_evidence",
                    "value_field": "reference_code",
                    "quantity_key": "chart_reference",
                    "evidence_kind": "observation_document_reference",
                    "normalize_numeric": False,
                }
            ],
            available_fields={"reference_code"},
        )
        value = con.execute(
            "select value_raw,normalized_value "
            "from variability_activity_rotation_evidence"
        ).fetchone()
    assert value == ("00001", None)


def test_configured_observation_product_reuses_archive_identity_as_locator(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "products.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select '000123' obsid, 60000 mjd, 42 snr) "
            f"to '{parquet}' (format parquet)"
        )
        compiler.create_schema(con)
        source_hash = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        con.execute(
            "insert into source_records values "
            "('record','test.catalog','r1','products','observation','{}','{}',?,1,"
            "'raw','typed','raw-tree','typed-table',timestamp '2026-07-19 00:00:00')",
            [source_hash],
        )
        consumed = compiler.materialize_configured_observation_products(
            con,
            source_id="test.catalog",
            release_id="r1",
            table_name="products",
            path=parquet,
            products=[
                {
                    "locator_field": "obsid",
                    "locator_kind": "archive_observation_id",
                    "product_kind": "spectrum",
                    "product_key_prefix": "spectrum:",
                    "retrieval_policy": "on_demand",
                    "observation_epoch_field": "mjd",
                    "processing_level": "source_release",
                    "quality_fields": ["snr"],
                }
            ],
            available_fields={"obsid", "mjd", "snr"},
        )
        row = con.execute(
            "select product_kind,product_key,product_locator,retrieval_policy,"
            "observation_epoch_raw,processing_level,quality_json->>'$.snr' "
            "from observation_product_lineage"
        ).fetchone()
    assert consumed == {"obsid", "mjd", "snr"}
    assert row == (
        "spectrum",
        "spectrum:000123",
        "000123",
        "on_demand",
        "60000",
        "source_release",
        "42",
    )


def test_planet_scalar_materialization_preserves_units_errors_bounds_and_reference(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "planet.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            f"('Planet b','Reference A',1.75,0.20,-0.10,1,'< 1.75')) "
            f"t(pl_name,pl_refname,pl_rade,pl_radeerr1,pl_radeerr2,pl_radelim,pl_radestr)) "
            f"to '{parquet}' (format parquet)"
        )
        source_row_sha256 = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record','test.catalog','r1','planet_rows','planet',"
            "'{}','{}',?,1,'raw','typed','raw-tree','typed-table',"
            "timestamp '2026-07-19 00:00:00')",
            [source_row_sha256],
        )
        fields = [
            {
                "column_name": name,
                "unit": "Rearth" if name.startswith("pl_rade") else None,
                "ucd": None,
                "description": "planet radius",
            }
            for name in (
                "pl_rade",
                "pl_radeerr1",
                "pl_radeerr2",
                "pl_radelim",
                "pl_radestr",
            )
        ]
        table_contract = {
            "planet_parameter_set": {
                "kind": "reference_specific",
                "reference_field": "pl_refname",
            },
            "signal_identifier_fields": ["pl_name"],
        }
        consumed = compiler.materialize_scalar_evidence(
            con,
            source_id="test.catalog",
            release_id="r1",
            table_name="planet_rows",
            path=parquet,
            destination="planet_parameter_evidence",
            fields=fields,
            available_fields={
                "pl_name",
                "pl_refname",
                "pl_rade",
                "pl_radeerr1",
                "pl_radeerr2",
                "pl_radelim",
                "pl_radestr",
            },
            table_contract=table_contract,
            unit_normalizations={"Rearth": "R_earth"},
        )
        compiler.materialize_parameter_sets(
            con,
            source_id="test.catalog",
            release_id="r1",
            table_name="planet_rows",
            table_contract=table_contract,
        )
        citation_summary = compiler.materialize_citations(con)
        evidence = con.execute(
            "select quantity_key,value_raw,unit_raw,normalized_value,normalized_unit,"
            "uncertainty_lower,uncertainty_upper,bound_semantics,reference_raw,"
            "quality_json->>'formatted_value_raw' from planet_parameter_evidence"
        ).fetchone()
        set_row = con.execute(
            "select parameter_set_kind,reference_raw from planet_parameter_sets"
        ).fetchone()
        citation = con.execute(
            "select source_reference_key,citation_text_raw from citations"
        ).fetchone()
    assert consumed == {
        "pl_rade",
        "pl_radeerr1",
        "pl_radeerr2",
        "pl_radelim",
        "pl_radestr",
    }
    assert evidence == (
        "nasa_exoplanet_archive.pl_rade",
        "1.75",
        "Rearth",
        1.75,
        "R_earth",
        0.1,
        0.2,
        "upper_limit",
        "Reference A",
        "< 1.75",
    )
    assert set_row == ("reference_specific", "Reference A")
    assert citation_summary == {"citations": 1, "evidence_links": 1}
    assert citation == ("Reference A", "Reference A")


def test_conditional_identifier_claims_apply_only_to_matching_rows(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "parameters.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            "('PSRJ','J1234+5678'),('P0','1.25')) "
            "t(parameter_name,value_raw)) "
            f"to '{parquet}' (format parquet)"
        )
        rows = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchall()
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'compact.test', 'r1', 'parameters', 'compact_object', "
            "'{}', '{}', ?, 1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [(f"record-{index}", row[0]) for index, row in enumerate(rows)],
        )
        consumed = compiler.materialize_conditional_identifier_claims(
            con,
            source_id="compact.test",
            release_id="r1",
            table_name="parameters",
            path=parquet,
            claims=[
                {
                    "value_field": "value_raw",
                    "namespace": "psrj",
                    "claim_scope": "compact_object",
                    "sql_predicate": "parameter_name='PSRJ'",
                }
            ],
            available_fields={"parameter_name", "value_raw"},
        )
        claims = con.execute(
            "select namespace,identifier_raw,claim_scope,quality_json->>'predicate' "
            "from identifier_claim_evidence"
        ).fetchall()
    assert consumed == {"value_raw"}
    assert claims == [("psrj", "J1234+5678", "compact_object", "parameter_name='PSRJ'")]


def test_conditional_identifier_claim_strips_prefix_before_numeric_normalization(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "aliases.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            "('GAIADR3 000123'),('GAIADR3 123A'),('HIP 7')) t(Name)) "
            f"to '{parquet}' (format parquet)"
        )
        rows = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchall()
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'multiplicity.test', 'r1', 'aliases', 'system_alias', '{}', '{}', "
            "?, 1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [(f"record-{index}", row[0]) for index, row in enumerate(rows)],
        )
        compiler.materialize_conditional_identifier_claims(
            con,
            source_id="multiplicity.test",
            release_id="r1",
            table_name="aliases",
            path=parquet,
            claims=[
                {
                    "value_field": "Name",
                    "namespace": "gaia_dr3_source_id",
                    "claim_scope": "star",
                    "sql_predicate": "Name like 'GAIADR3 %'",
                    "strip_prefix": "GAIADR3 ",
                    "normalization": "unsigned_integer_decimal_v1",
                }
            ],
            available_fields={"Name"},
        )
        claim = con.execute(
            "select identifier_raw,identifier_normalized from identifier_claim_evidence"
        ).fetchone()
        rejection = con.execute(
            "select identifier_raw,requested_namespace,normalization,reason "
            "from identifier_normalization_rejections"
        ).fetchone()
        audit = artifact_audit.audit_evidence(con)
    assert claim == ("GAIADR3 000123", "123")
    assert rejection == (
        "GAIADR3 123A",
        "gaia_dr3_source_id",
        "unsigned_integer_decimal_v1",
        "normalization did not produce a usable identifier",
    )
    assert audit["checks"]["blank_identifier_claims"] == 0
    assert audit["status"] == "pass"


def test_identifier_normalization_strips_only_trailing_hash_footnotes(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "names.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            "('AX J1818.8-1559 #'),('PSR J1846-0258 ##'),('Name#Internal')) "
            "t(Name)) "
            f"to '{parquet}' (format parquet)"
        )
        rows = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchall()
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'compact.test', 'r1', 'names', 'compact_object', '{}', '{}', ?, "
            "1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [(f"record-{index}", row[0]) for index, row in enumerate(rows)],
        )
        compiler.materialize_identifier_claims(
            con,
            source_id="compact.test",
            release_id="r1",
            table_name="names",
            path=parquet,
            fields=["Name"],
            claim_by_field={
                "Name": {
                    "namespace": "compact_name",
                    "claim_scope": "compact_object",
                    "normalization": "strip_trailing_hash_footnote_v1",
                }
            },
        )
        names = con.execute(
            "select identifier_raw,identifier_normalized "
            "from identifier_claim_evidence order by identifier_raw"
        ).fetchall()
    assert names == [
        ("AX J1818.8-1559 #", "AX J1818.8-1559"),
        ("Name#Internal", "Name#Internal"),
        ("PSR J1846-0258 ##", "PSR J1846-0258"),
    ]


def test_identifier_normalization_preserves_display_form_and_strips_literal_prefix(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "toi_names.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values ('TOI-101.01'),('101.02')) "
            f"t(toidisplay)) to '{parquet}' (format parquet)"
        )
        rows = con.execute(
            f"select sha256(to_json(source_row)) from read_parquet('{parquet}') "
            "source_row"
        ).fetchall()
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'tess.test', 'r1', 'toi', 'planet_candidate', '{}', '{}', ?, "
            "1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [(f"record-{index}", row[0]) for index, row in enumerate(rows)],
        )
        compiler.materialize_identifier_claims(
            con,
            source_id="tess.test",
            release_id="r1",
            table_name="toi",
            path=parquet,
            fields=["toidisplay"],
            claim_by_field={
                "toidisplay": {
                    "namespace": "toi_id",
                    "claim_scope": "planet_candidate",
                    "normalization": "strip_literal_prefix_v1",
                    "normalization_prefix": "TOI-",
                }
            },
        )
        claims = con.execute(
            "select identifier_raw, identifier_normalized, "
            "quality_json->>'normalization_prefix' "
            "from identifier_claim_evidence"
        ).fetchall()
        rejections = con.execute(
            "select identifier_raw from identifier_normalization_rejections"
        ).fetchall()
    assert claims == [("TOI-101.01", "101.01", "TOI-")]
    assert rejections == [("101.02",)]


def test_multiple_compact_parameter_sets_have_distinct_ids_and_predicates(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "compact.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            "('Object A','5.0','timing-ref','0.4','xray-ref'),"
            "('Object B','7.0','timing-ref',null,null)) "
            "t(Name,Period,Ref_Time,kT,Ref_Xray)) "
            f"to '{parquet}' (format parquet)"
        )
        rows = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchall()
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'compact.test', 'r1', 'compact', 'compact_object', '{}', '{}', ?, "
            "1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [(f"record-{index}", row[0]) for index, row in enumerate(rows)],
        )
        common = {
            "source_id": "compact.test",
            "release_id": "r1",
            "table_name": "compact",
            "path": parquet,
            "fields": [
                {"column_name": name}
                for name in ("Name", "Period", "Ref_Time", "kT", "Ref_Xray")
            ],
            "available_fields": {"Name", "Period", "Ref_Time", "kT", "Ref_Xray"},
        }
        timing_fields = compiler.materialize_compact_objects(
            con,
            compact_object={
                "compact_kind": "timing",
                "parameter_set_key_fields": ["Name", "Ref_Time"],
                "parameter_fields": ["Period", "Ref_Time"],
                "quality_fields": ["Name"],
                "reference_field": "Ref_Time",
                "sql_predicate": "nullif(trim(Period), '') is not null",
                "method": "source_timing",
                "normalization_version": "source_native_v1",
            },
            **common,
        )
        xray_fields = compiler.materialize_compact_objects(
            con,
            compact_object={
                "compact_kind": "xray",
                "parameter_set_key_fields": ["Name", "Ref_Xray"],
                "parameter_fields": ["kT", "Ref_Xray"],
                "quality_fields": ["Name"],
                "reference_field": "Ref_Xray",
                "sql_predicate": "nullif(trim(kT), '') is not null",
                "method": "source_xray",
                "normalization_version": "source_native_v1",
            },
            **common,
        )
        rows = con.execute(
            "select compact_kind,count(*),count(distinct evidence_id) "
            "from compact_object_evidence group by 1 order by 1"
        ).fetchall()
    assert timing_fields == {"Name", "Period", "Ref_Time"}
    assert xray_fields == {"Name", "kT", "Ref_Xray"}
    assert rows == [("timing", 2, 2), ("xray", 1, 1)]


def test_authoritative_citation_catalog_validates_compact_references(
    tmp_path: Path,
) -> None:
    citation_parquet = tmp_path / "references.parquet"
    parameter_parquet = tmp_path / "parameters.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            "('REF1','Author et al. 2025','source block','2025ApJ...1A','10.1/test','2025')) "
            "t(reference_code,citation_text,raw_block,bibcode,doi,year)) "
            f"to '{citation_parquet}' (format parquet)"
        )
        con.execute(
            f"copy (select * from (values "
            "('PSR A','P0','1.25','REF1'),"
            "('PSR B','P0','2.50','not-a-reference')) "
            "t(pulsar_name,parameter_name,value_raw,reference_raw)) "
            f"to '{parameter_parquet}' (format parquet)"
        )
        citation_sha = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{citation_parquet}') t"
        ).fetchone()[0]
        parameter_rows = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parameter_parquet}') t"
        ).fetchall()
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('citation-record', 'compact.test', 'r1', 'references', "
            "'source_reference', '{}', '{}', ?, 1, 'raw', 'typed', "
            "'raw-tree', 'typed-table', timestamp '2026-07-19 00:00:00')",
            [citation_sha],
        )
        con.executemany(
            "insert into source_records values "
            "(?, 'compact.test', 'r1', 'parameters', 'compact_object', "
            "'{}', '{}', ?, 1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [
                (f"parameter-record-{index}", row[0])
                for index, row in enumerate(parameter_rows)
            ],
        )
        citation_fields = [
            {"column_name": name}
            for name in (
                "reference_code",
                "citation_text",
                "raw_block",
                "bibcode",
                "doi",
                "year",
            )
        ]
        compiler.materialize_citation_catalog(
            con,
            source_id="compact.test",
            release_id="r1",
            table_name="references",
            path=citation_parquet,
            citation_catalog={
                "reference_key_field": "reference_code",
                "citation_text_field": "citation_text",
                "context_fields": ["raw_block"],
                "bibcode_field": "bibcode",
                "doi_field": "doi",
                "publication_year_field": "year",
            },
            fields=citation_fields,
            available_fields={
                "reference_code",
                "citation_text",
                "raw_block",
                "bibcode",
                "doi",
                "year",
            },
        )
        compiler.materialize_compact_objects(
            con,
            source_id="compact.test",
            release_id="r1",
            table_name="parameters",
            path=parameter_parquet,
            fields=[
                {"column_name": name}
                for name in (
                    "pulsar_name",
                    "parameter_name",
                    "value_raw",
                    "reference_raw",
                )
            ],
            compact_object={
                "compact_kind": "pulsar_parameter",
                "parameter_set_key_fields": ["pulsar_name", "parameter_name"],
                "parameter_fields": ["parameter_name", "value_raw", "reference_raw"],
                "quality_fields": ["pulsar_name"],
                "reference_field": "reference_raw",
                "reference_catalog_validated": True,
                "method": "source_parameter",
                "normalization_version": "source_native_v1",
            },
            available_fields={
                "pulsar_name",
                "parameter_name",
                "value_raw",
                "reference_raw",
            },
        )
        summary = compiler.materialize_citations(con)
        evidence = con.execute(
            "select quality_json->>'pulsar_name', reference_raw, "
            "parameter_set_raw->>'reference_raw' "
            "from compact_object_evidence order by 1"
        ).fetchall()
        citation = con.execute(
            "select source_reference_key,citation_text_raw,"
            "parsed_json->'source_context'->>'raw_block',bibcode,doi,publication_year "
            "from citations"
        ).fetchone()
    assert evidence == [
        ("PSR A", "REF1", "REF1"),
        ("PSR B", None, "not-a-reference"),
    ]
    assert citation == (
        "REF1",
        "Author et al. 2025",
        "source block",
        "2025ApJ...1A",
        "10.1/test",
        2025,
    )
    assert summary == {"citations": 1, "evidence_links": 1}


def test_citation_materialization_matches_key_or_text_without_duplicate_links(
    tmp_path: Path,
) -> None:
    with duckdb.connect() as con:
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record','identity.test','r1','rows','object',"
            "'{}','{}','row-hash',1,'raw','typed','raw-tree','typed-table',"
            "timestamp '2026-07-19 00:00:00')"
        )
        con.execute(
            "insert into stellar_classification_evidence values "
            "('evidence','record',null,'spectral_type','G2V','G2V',null,"
            "'source',null,'same-reference','{}')"
        )
        con.execute(
            "insert into citations values "
            "('same','identity.test','same-reference','same-reference',"
            "null,null,null,null,'{}'),"
            "('text','identity.test','different-key','same-reference',"
            "null,null,null,null,'{}')"
        )
        summary = compiler.materialize_citations(con)
        links = con.execute(
            "select citation_id from evidence_citations order by citation_id"
        ).fetchall()
    assert summary == {"citations": 2, "evidence_links": 2}
    assert links == [("same",), ("text",)]


def test_source_citation_link_attaches_catalog_reference_to_identifier_claim(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "bibliography.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select '508' oidref, '423709' oidbibref) "
            f"to '{parquet}' (format parquet)"
        )
        source_hash = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record','identity.test','r1','bibliography','object_reference',"
            "'{}','{}',?,1,'raw','typed','raw-tree','typed-table',"
            "timestamp '2026-07-19 00:00:00')",
            [source_hash],
        )
        compiler.materialize_identifier_claims(
            con,
            source_id="identity.test",
            release_id="r1",
            table_name="bibliography",
            path=parquet,
            fields=["oidref"],
            claim_by_field={
                "oidref": {
                    "namespace": "source_oid",
                    "claim_scope": "object",
                }
            },
        )
        con.execute(
            "insert into citations values "
            "('citation','identity.test','423709','Reference',null,null,null,null,'{}')"
        )
        consumed = compiler.materialize_source_citation_links(
            con,
            source_id="identity.test",
            release_id="r1",
            table_name="bibliography",
            path=parquet,
            links=[
                {
                    "identifier_claim_field": "oidref",
                    "reference_key_field": "oidbibref",
                    "citation_role": "object_bibliography",
                    "required": True,
                }
            ],
            available_fields={"oidref", "oidbibref"},
        )
        row = con.execute(
            "select evidence_table,citation_id,citation_role from evidence_citations"
        ).fetchone()
    assert consumed == {"oidref", "oidbibref"}
    assert row == ("identifier_claim_evidence", "citation", "object_bibliography")


def test_nasa_reference_fragment_parser_preserves_lineage_and_parses_ads() -> None:
    raw = (
        "<a refstr=HOLCZER_ET_AL__2016 "
        "href=https://ui.adsabs.harvard.edu/abs/2016ApJS..225....9H/abstract "
        "target=ref>Holczer et al. 2016</a>"
    )
    assert compiler.parse_reference_fragment(raw) == {
        "reference_key": "HOLCZER_ET_AL__2016",
        "display_text": "Holczer et al. 2016",
        "url": "https://ui.adsabs.harvard.edu/abs/2016ApJS..225....9H/abstract",
        "bibcode": "2016ApJS..225....9H",
        "doi": None,
        "publication_year": 2016,
    }


def test_reference_fragment_parser_recognizes_direct_ads_bibcode() -> None:
    assert compiler.parse_reference_fragment("1926PDAO....3..341H") == {
        "reference_key": None,
        "display_text": "1926PDAO....3..341H",
        "url": "https://ui.adsabs.harvard.edu/abs/1926PDAO....3..341H/abstract",
        "bibcode": "1926PDAO....3..341H",
        "doi": None,
        "publication_year": 1926,
    }
