#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
from dataclasses import dataclass
from pathlib import Path

import duckdb


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def iso_utc(value: dt.datetime) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def normalize_name_sql(expr: str) -> str:
    return (
        "case when {expr} is null or trim({expr}) = '' then null else "
        "lower(trim(regexp_replace(regexp_replace({expr}, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g'))) end"
    ).format(expr=expr)


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if value and value[0] not in "\"'":
            value = value.split("#", 1)[0].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ[key] = value


def init_env(root: Path) -> None:
    for env_path in (
        Path("/etc/spacegate/spacegate.env"),
        root / ".spacegate.env",
        root / ".spacegate.local.env",
    ):
        load_env_file(env_path)


@dataclass(frozen=True)
class OverlapField:
    field_name: str
    core_column: str
    label: str


@dataclass(frozen=True)
class CatalogSpec:
    name: str
    entity_type: str
    path_parts: tuple[str, ...]
    source_sql: str
    normalized_sql: str
    sample_columns: tuple[str, ...]
    coverage_columns: tuple[str, ...]
    overlap_fields: tuple[OverlapField, ...]

    def resolve_path(self, state_dir: Path) -> Path:
        return state_dir.joinpath(*self.path_parts)


ATHYG_NORMALIZED_SQL = f"""
select
  'athyg' as catalog_name,
  'star' as entity_type,
  coalesce(
    nullif(id, ''),
    nullif(gaia, ''),
    nullif(hip, ''),
    nullif(hd, ''),
    md5(
      coalesce(nullif(proper, ''), '') || '|' ||
      coalesce(nullif(ra, ''), '') || '|' ||
      coalesce(nullif(dec, ''), '') || '|' ||
      coalesce(nullif(dist, ''), '')
    )
  ) as sample_key,
  nullif(id, '')::bigint as source_pk,
  nullif(gaia, '')::bigint as gaia_id,
  nullif(hip, '')::bigint as hip_id,
  nullif(hd, '')::bigint as hd_id,
  nullif(hr, '')::bigint as hr_id,
  nullif(gl, '') as gl_id,
  nullif(tyc, '') as tyc_id,
  nullif(hyg, '')::bigint as hyg_id,
  nullif(proper, '') as proper_name,
  nullif(bayer, '') as bayer,
  nullif(flam, '') as flam,
  nullif(con, '') as constellation,
  coalesce(
    nullif(proper, ''),
    case when nullif(bayer, '') is not null and nullif(con, '') is not null then nullif(bayer, '') || ' ' || nullif(con, '') end,
    case when nullif(flam, '') is not null and nullif(con, '') is not null then nullif(flam, '') || ' ' || nullif(con, '') end,
    case when nullif(hip, '') is not null then 'HIP ' || nullif(hip, '') end,
    case when nullif(hd, '') is not null then 'HD ' || nullif(hd, '') end,
    case when nullif(gaia, '') is not null then 'Gaia DR3 ' || nullif(gaia, '') end
  ) as object_name,
  {normalize_name_sql("coalesce(nullif(proper, ''), case when nullif(bayer, '') is not null and nullif(con, '') is not null then nullif(bayer, '') || ' ' || nullif(con, '') end, case when nullif(flam, '') is not null and nullif(con, '') is not null then nullif(flam, '') || ' ' || nullif(con, '') end, case when nullif(hip, '') is not null then 'HIP ' || nullif(hip, '') end, case when nullif(hd, '') is not null then 'HD ' || nullif(hd, '') end, case when nullif(gaia, '') is not null then 'Gaia DR3 ' || nullif(gaia, '') end)")} as object_name_norm,
  nullif(ra, '')::double as ra_deg,
  nullif(dec, '')::double as dec_deg,
  nullif(dist, '')::double as dist_pc,
  nullif(x0, '')::double as x_helio_pc,
  nullif(y0, '')::double as y_helio_pc,
  nullif(z0, '')::double as z_helio_pc,
  nullif(pm_ra, '')::double as pm_ra_mas_yr,
  nullif(pm_dec, '')::double as pm_dec_mas_yr,
  nullif(rv, '')::double as radial_velocity_kms,
  nullif(mag, '')::double as vmag,
  nullif(absmag, '')::double as absmag,
  nullif(ci, '')::double as color_index,
  nullif(spect, '') as spectral_type_raw,
  nullif(pos_src, '') as pos_src,
  nullif(dist_src, '') as dist_src,
  nullif(pm_src, '') as pm_src,
  nullif(rv_src, '') as rv_src,
  nullif(spect_src, '') as spect_src
from catalog_source
"""


NASA_NORMALIZED_SQL = f"""
select
  'nasa_exoplanet_archive' as catalog_name,
  'planet' as entity_type,
  coalesce(nullif(pl_name, ''), nullif(objectid, ''), md5(coalesce(nullif(hostname, ''), ''))) as sample_key,
  nullif(objectid, '')::bigint as source_pk,
  nullif(pl_name, '') as object_name,
  {normalize_name_sql("nullif(pl_name, '')")} as object_name_norm,
  nullif(hostname, '') as host_name_raw,
  {normalize_name_sql("nullif(hostname, '')")} as host_name_norm,
  cast(nullif(regexp_extract(coalesce(nullif(gaia_dr3_id, ''), nullif(gaia_dr2_id, ''), ''), '(\\d{{10,}})\\s*$', 1), '') as bigint) as host_gaia_id,
  cast(nullif(regexp_extract(coalesce(nullif(hip_name, ''), ''), '(\\d+)', 1), '') as bigint) as host_hip_id,
  cast(nullif(regexp_extract(coalesce(nullif(hd_name, ''), ''), '(\\d+)', 1), '') as bigint) as host_hd_id,
  nullif(discoverymethod, '') as discovery_method,
  nullif(disc_year, '')::int as disc_year,
  nullif(pl_orbper, '')::double as orbital_period_days,
  nullif(pl_orbsmax, '')::double as semi_major_axis_au,
  nullif(pl_orbeccen, '')::double as eccentricity,
  nullif(pl_orbincl, '')::double as inclination_deg,
  nullif(pl_rade, '')::double as radius_earth,
  nullif(pl_radj, '')::double as radius_jup,
  nullif(pl_masse, '')::double as mass_earth,
  nullif(pl_massj, '')::double as mass_jup,
  nullif(pl_eqt, '')::double as eq_temp_k,
  nullif(pl_insol, '')::double as insol_earth,
  nullif(sy_dist, '')::double as host_dist_pc
from catalog_source
"""


CATALOGS: dict[str, CatalogSpec] = {
    "athyg": CatalogSpec(
        name="athyg",
        entity_type="star",
        path_parts=("cooked", "athyg", "athyg.csv.gz"),
        source_sql="""
            select * from read_csv_auto(
              {path},
              compression='gzip',
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """,
        normalized_sql=ATHYG_NORMALIZED_SQL,
        sample_columns=(
            "source_pk",
            "gaia_id",
            "hip_id",
            "hd_id",
            "object_name",
            "object_name_norm",
            "ra_deg",
            "dec_deg",
            "dist_pc",
            "x_helio_pc",
            "y_helio_pc",
            "z_helio_pc",
            "pm_ra_mas_yr",
            "pm_dec_mas_yr",
            "radial_velocity_kms",
            "vmag",
            "absmag",
            "color_index",
            "spectral_type_raw",
            "pos_src",
            "dist_src",
            "pm_src",
            "rv_src",
            "spect_src",
        ),
        coverage_columns=(
            "gaia_id",
            "hip_id",
            "hd_id",
            "object_name",
            "object_name_norm",
            "ra_deg",
            "dec_deg",
            "dist_pc",
            "x_helio_pc",
            "y_helio_pc",
            "z_helio_pc",
            "pm_ra_mas_yr",
            "pm_dec_mas_yr",
            "radial_velocity_kms",
            "vmag",
            "spectral_type_raw",
        ),
        overlap_fields=(
            OverlapField("gaia_id", "gaia_id", "gaia_id"),
            OverlapField("hip_id", "hip_id", "hip_id"),
            OverlapField("hd_id", "hd_id", "hd_id"),
            OverlapField("object_name_norm", "star_name_norm", "name_norm"),
        ),
    ),
    "nasa_exoplanet_archive": CatalogSpec(
        name="nasa_exoplanet_archive",
        entity_type="planet",
        path_parts=("cooked", "nasa_exoplanet_archive", "pscomppars_clean.csv"),
        source_sql="""
            select * from read_csv_auto(
              {path},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """,
        normalized_sql=NASA_NORMALIZED_SQL,
        sample_columns=(
            "source_pk",
            "object_name",
            "object_name_norm",
            "host_name_raw",
            "host_name_norm",
            "host_gaia_id",
            "host_hip_id",
            "host_hd_id",
            "discovery_method",
            "disc_year",
            "orbital_period_days",
            "semi_major_axis_au",
            "eccentricity",
            "inclination_deg",
            "radius_earth",
            "radius_jup",
            "mass_earth",
            "mass_jup",
            "eq_temp_k",
            "insol_earth",
            "host_dist_pc",
        ),
        coverage_columns=(
            "object_name",
            "host_name_raw",
            "host_name_norm",
            "host_gaia_id",
            "host_hip_id",
            "host_hd_id",
            "orbital_period_days",
            "semi_major_axis_au",
            "eccentricity",
            "radius_earth",
            "mass_earth",
            "eq_temp_k",
            "insol_earth",
            "host_dist_pc",
        ),
        overlap_fields=(
            OverlapField("host_gaia_id", "gaia_id", "host_gaia_id"),
            OverlapField("host_hip_id", "hip_id", "host_hip_id"),
            OverlapField("host_hd_id", "hd_id", "host_hd_id"),
            OverlapField("host_name_norm", "star_name_norm", "host_name_norm"),
        ),
    ),
}


def default_state_dir(root: Path) -> Path:
    state_dir = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if state_dir:
        return Path(state_dir)
    return root / "data"


def load_catalog_view(con: duckdb.DuckDBPyConnection, spec: CatalogSpec, path: Path) -> str:
    source_sql = spec.source_sql.format(path=sql_quote(str(path)))
    con.execute(f"create or replace temp view catalog_source as {source_sql}")
    table_name = f"catalog_{spec.name}"
    con.execute(f"create or replace temp table {table_name} as {spec.normalized_sql}")
    return table_name


def prepare_core_overlap_tables(con: duckdb.DuckDBPyConnection, core_db_attached: bool) -> None:
    if not core_db_attached:
        return
    con.execute(
        """
        create or replace temp table core_key_gaia as
        select distinct gaia_id as key_value
        from core.stars
        where gaia_id is not null
        """
    )
    con.execute(
        """
        create or replace temp table core_key_hip as
        select distinct hip_id as key_value
        from core.stars
        where hip_id is not null
        """
    )
    con.execute(
        """
        create or replace temp table core_key_hd as
        select distinct hd_id as key_value
        from core.stars
        where hd_id is not null
        """
    )
    con.execute(
        """
        create or replace temp table core_key_name as
        select distinct star_name_norm as key_value
        from core.stars
        where star_name_norm is not null and trim(star_name_norm) <> ''
        """
    )


def core_overlap_table(core_column: str) -> str:
    if core_column == "gaia_id":
        return "core_key_gaia"
    if core_column == "hip_id":
        return "core_key_hip"
    if core_column == "hd_id":
        return "core_key_hd"
    if core_column == "star_name_norm":
        return "core_key_name"
    raise ValueError(f"Unsupported core overlap column: {core_column}")


def build_overlap_query(spec: CatalogSpec, view_name: str, limit: int, seed: str) -> str:
    if not spec.overlap_fields:
        return ""
    joins = []
    flag_columns = []
    score_terms = []
    for index, field in enumerate(spec.overlap_fields):
        alias = f"match_{field.label}"
        join_alias = f"j{index}"
        joins.append(
            f"left join {core_overlap_table(field.core_column)} {join_alias} "
            f"on src.{field.field_name} = {join_alias}.key_value"
        )
        flag_columns.append(
            f"case when src.{field.field_name} is not null and {join_alias}.key_value is not null then 1 else 0 end as {alias}"
        )
        score_terms.append(alias)

    score_sql = " + ".join(score_terms)
    select_cols = ", ".join(["src." + col for col in spec.sample_columns])
    match_cols = ", ".join([f"flags.match_{field.label}" for field in spec.overlap_fields])
    return f"""
        with flags as (
          select
            src.sample_key,
            {", ".join(flag_columns)}
          from {view_name} src
          {' '.join(joins)}
        ), ranked as (
          select
            src.catalog_name,
            src.entity_type,
            src.sample_key,
            {select_cols},
            {match_cols},
            ({score_sql}) as overlap_score
          from {view_name} src
          join flags using (sample_key)
          where ({score_sql}) > 0
        )
        select *
        from ranked
        order by overlap_score desc, md5(coalesce(sample_key, '') || '|' || {sql_quote(seed)}) asc
        limit {limit}
    """


def write_csv(con: duckdb.DuckDBPyConnection, query: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"copy ({query}) to {sql_quote(str(dest))} (header, delimiter ',')")


def coverage_summary(con: duckdb.DuckDBPyConnection, view_name: str, columns: tuple[str, ...]) -> dict:
    total_rows = con.execute(f"select count(*) from {view_name}").fetchone()[0]
    coverage = {}
    for column in columns:
        non_null = con.execute(
            f"select count(*) from {view_name} where {column} is not null"
        ).fetchone()[0]
        coverage[column] = {
            "non_null_rows": int(non_null),
            "coverage_pct": round((non_null / total_rows * 100.0), 2) if total_rows else 0.0,
        }
    return {"row_count": int(total_rows), "columns": coverage}


def overlap_summary(
    con: duckdb.DuckDBPyConnection, spec: CatalogSpec, view_name: str, core_db_attached: bool
) -> dict:
    if not core_db_attached or not spec.overlap_fields:
        return {"core_db_attached": core_db_attached, "matched_rows": 0, "fields": {}}

    summary = {"core_db_attached": True, "matched_rows": 0, "fields": {}}
    match_predicates = []
    for index, field in enumerate(spec.overlap_fields):
        join_alias = f"j{index}"
        predicate = f"src.{field.field_name} is not null and {join_alias}.key_value is not null"
        count = con.execute(
            f"""
            select count(*)
            from {view_name} src
            left join {core_overlap_table(field.core_column)} {join_alias}
              on src.{field.field_name} = {join_alias}.key_value
            where {predicate}
            """
        ).fetchone()[0]
        summary["fields"][field.label] = int(count)
        match_predicates.append(f"({predicate})")
    if match_predicates:
        summary["matched_rows"] = int(
            con.execute(
                f"""
                select count(*)
                from {view_name} src
                {' '.join([
                    f"left join {core_overlap_table(field.core_column)} j{idx} on src.{field.field_name} = j{idx}.key_value"
                    for idx, field in enumerate(spec.overlap_fields)
                ])}
                where {' or '.join(match_predicates)}
                """
            ).fetchone()[0]
        )
    return summary


def pick_catalogs(catalog_names: list[str], state_dir: Path) -> list[CatalogSpec]:
    if catalog_names:
        specs = []
        for name in catalog_names:
            if name not in CATALOGS:
                raise SystemExit(f"Unknown catalog: {name}")
            specs.append(CATALOGS[name])
        return specs

    available = []
    for spec in CATALOGS.values():
        if spec.resolve_path(state_dir).exists():
            available.append(spec)
    if not available:
        raise SystemExit("No built-in catalog sample sources are available locally.")
    return available


def render_markdown(run_id: str, catalog_reports: list[dict]) -> str:
    lines = [
        "# Catalog Evaluation Summary",
        "",
        f"- Run ID: `{run_id}`",
        f"- Generated at: `{iso_utc(utc_now())}`",
        "",
    ]
    for report in catalog_reports:
        lines.extend(
            [
                f"## {report['catalog']}",
                "",
                f"- Entity type: `{report['entity_type']}`",
                f"- Source path: `{report['source_path']}`",
                f"- Rows scanned: `{report['coverage']['row_count']}`",
                f"- Core-overlap rows: `{report['overlap']['matched_rows']}`",
                "",
                "### Field Coverage",
                "",
            ]
        )
        for column, stats in report["coverage"]["columns"].items():
            lines.append(
                f"- `{column}`: {stats['non_null_rows']} rows ({stats['coverage_pct']}%)"
            )
        if report["overlap"]["fields"]:
            lines.extend(["", "### Core Overlap Keys", ""])
            for key, count in report["overlap"]["fields"].items():
                lines.append(f"- `{key}`: {count} rows")
        lines.extend(
            [
                "",
                "### Sample Files",
                "",
                f"- Random sample: `{report['random_sample_path']}`",
                f"- Overlap sample: `{report['overlap_sample_path']}`",
                "",
            ]
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sample and summarize candidate catalogs for v1.2 precedence work."
    )
    parser.add_argument(
        "--catalog",
        action="append",
        default=[],
        help="Catalog name to evaluate. Defaults to all locally available built-ins.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100,
        help="Rows to emit for deterministic random and overlap samples (default: 100).",
    )
    parser.add_argument(
        "--seed",
        default="spacegate-v1-2",
        help="Deterministic sample seed.",
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="Optional explicit output directory. Defaults under $SPACEGATE_STATE_DIR/reports/catalog_eval/<run_id>/.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    init_env(root)
    state_dir = default_state_dir(root)
    run_id = utc_now().strftime("%Y-%m-%dT%H%M%SZ")
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else state_dir / "reports" / "catalog_eval" / run_id
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    core_db_path = state_dir / "served" / "current" / "core.duckdb"
    con = duckdb.connect(database=":memory:")
    core_db_attached = core_db_path.exists()
    if core_db_attached:
        con.execute(f"attach {sql_quote(str(core_db_path))} as core")
        prepare_core_overlap_tables(con, core_db_attached)

    reports = []
    for spec in pick_catalogs(args.catalog, state_dir):
        source_path = spec.resolve_path(state_dir)
        if not source_path.exists():
            raise SystemExit(f"Missing source for {spec.name}: {source_path}")

        view_name = load_catalog_view(con, spec, source_path)
        random_query = f"""
            select
              catalog_name,
              entity_type,
              sample_key,
              {", ".join(spec.sample_columns)}
            from {view_name}
            order by md5(coalesce(sample_key, '') || '|' || {sql_quote(args.seed)}) asc
            limit {args.sample_size}
        """
        overlap_query = build_overlap_query(spec, view_name, args.sample_size, args.seed)
        random_sample_path = output_dir / f"{spec.name}_random_sample.csv"
        overlap_sample_path = output_dir / f"{spec.name}_overlap_sample.csv"

        write_csv(con, random_query, random_sample_path)
        if core_db_attached and overlap_query:
            write_csv(con, overlap_query, overlap_sample_path)
        else:
            overlap_sample_path.write_text("core overlap unavailable\n")

        coverage = coverage_summary(con, view_name, spec.coverage_columns)
        overlap = overlap_summary(con, spec, view_name, core_db_attached)
        report = {
            "catalog": spec.name,
            "entity_type": spec.entity_type,
            "source_path": str(source_path),
            "coverage": coverage,
            "overlap": overlap,
            "random_sample_path": str(random_sample_path),
            "overlap_sample_path": str(overlap_sample_path),
        }
        reports.append(report)
        (output_dir / f"{spec.name}_summary.json").write_text(
            json.dumps(report, indent=2) + "\n"
        )
        con.execute(f"drop table {view_name}")
        con.execute("drop view catalog_source")

    summary = {"run_id": run_id, "catalogs": reports}
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    (output_dir / "summary.md").write_text(render_markdown(run_id, reports))
    print(str(output_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
