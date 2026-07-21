from __future__ import annotations

import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_gaia_source_typed_source as gaia_audit  # noqa: E402


def test_field_roles_exhaustively_partition_gaia_source_columns() -> None:
    fields = [
        "solution_id",
        "source_id",
        "random_index",
        "ra",
        "phot_g_mean_mag",
        "radial_velocity",
        "phot_variable_flag",
        "has_rvs",
        "teff_gspphot",
    ]
    roles = gaia_audit.field_roles(fields)
    assert roles["identity"] == ["solution_id", "source_id"]
    assert roles["compiler_index"] == ["random_index"]
    assert roles["astrometric_solution"] == ["ra"]
    assert roles["photometric_solution"] == ["phot_g_mean_mag"]
    assert roles["radial_velocity_solution"] == ["radial_velocity"]
    assert roles["classification_and_membership"] == ["phot_variable_flag"]
    assert roles["observation_product_index"] == ["has_rvs"]
    assert roles["redundant_ap_projection"] == ["teff_gspphot"]
    assert roles["unclassified"] == []
    assert sorted(value for values in roles.values() for value in values) == sorted(fields)


def source_row(source_id: int, parallax: float | None) -> dict[str, object]:
    row: dict[str, object] = {field: None for field in gaia_audit.REQUIRED_FIELDS}
    row.update(
        {
            "solution_id": 1,
            "designation": f"Gaia DR3 {source_id}",
            "source_id": source_id,
            "ref_epoch": 2016.0,
            "ra": 10.0,
            "ra_error": 0.1,
            "dec": -20.0,
            "dec_error": 0.1,
            "parallax": parallax,
            "parallax_error": 0.2,
            "pmra_error": 0.3,
            "pmdec_error": 0.3,
            "ra_dec_corr": 0.0,
            "phot_g_mean_flux_error": 1.0,
            "radial_velocity_error": 1.0,
            "vbroad_error": 1.0,
            "has_xp_continuous": False,
            "has_xp_sampled": False,
            "has_rvs": False,
            "has_epoch_photometry": False,
            "has_epoch_rv": False,
        }
    )
    for index in range(gaia_audit.EXPECTED_FIELD_COUNT - len(row)):
        row[f"preserved_field_{index:03d}"] = None
    return row


def write_table(path: Path, row: dict[str, object]) -> list[dict[str, str]]:
    fields = sorted(row)
    con = duckdb.connect()
    definitions = []
    for field in fields:
        if field in {"solution_id", "source_id"}:
            kind = "bigint"
        elif field.startswith("has_"):
            kind = "boolean"
        elif field in {"designation", "phot_variable_flag", "libname_gspphot"}:
            kind = "varchar"
        elif field.startswith("preserved_field_"):
            kind = "varchar"
        else:
            kind = "double"
        definitions.append(f'"{field}" {kind}')
    con.execute("create table source(" + ",".join(definitions) + ")")
    con.execute(
        "insert into source values (" + ",".join("?" for _ in fields) + ")",
        [row[field] for field in fields],
    )
    con.execute("copy source to ? (format parquet)", [str(path)])
    columns = [
        {"name": str(name), "type": str(kind)}
        for _, name, kind, *_ in con.execute("pragma table_info('source')").fetchall()
    ]
    con.close()
    return columns


def test_gaia_source_audit_requires_disjoint_envelope_branches(tmp_path: Path) -> None:
    tables = tmp_path / "tables"
    tables.mkdir()
    hard_path = tables / "hard.parquet"
    supplement_path = tables / "supplement.parquet"
    hard_columns = write_table(hard_path, source_row(1, 3.0))
    supplement_columns = write_table(supplement_path, source_row(2, 2.0))
    assert hard_columns == supplement_columns
    manifest = {
        "source_id": gaia_audit.SOURCE_ID,
        "release_id": "test",
        "snapshot_id": "raw",
        "typed_snapshot_id": "typed",
        "content_sha256": "content",
        "tables": [
            {
                "source_name": gaia_audit.HARD_TABLE,
                "status": "typed",
                "row_count": 1,
                "parquet_path": "tables/hard.parquet",
                "columns": hard_columns,
            },
            {
                "source_name": gaia_audit.SUPPLEMENT_TABLE,
                "status": "typed",
                "row_count": 1,
                "parquet_path": "tables/supplement.parquet",
                "columns": supplement_columns,
            },
        ],
    }
    report = gaia_audit.audit(tmp_path, manifest)
    assert report["status"] == "pass"
    assert report["checks"]["cross_branch_source_id_overlap"] == 0

    write_table(supplement_path, source_row(1, 2.0))
    collided = gaia_audit.audit(tmp_path, manifest)
    assert collided["status"] == "fail"
    assert collided["checks"]["cross_branch_source_id_overlap"] == 1
