from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb


SB9_VERSION_FALLBACK = "sb9_2024-04-22"
SB9_URL_FALLBACK = "https://cdsarc.cds.unistra.fr/ftp/B/sb9"
MATCH_VERSION = "multiple_component_evidence_match_v1"


def _sql(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def _empty_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create table sb9_systems as select
          null::bigint as sb9_sequence, null::varchar as b1900_name,
          null::double as ra_deg, null::double as dec_deg, null::varchar as component_label,
          null::double as magnitude_primary, null::varchar as magnitude_primary_band,
          null::double as magnitude_secondary, null::varchar as magnitude_secondary_band,
          null::varchar as spectral_type_primary, null::varchar as spectral_type_secondary,
          null::varchar as source_name, null::bigint as source_line_number, null::varchar as raw_row,
          null::varchar as source_catalog, null::varchar as source_version, null::varchar as source_pk,
          null::varchar as source_row_hash, null::varchar as retrieval_checksum,
          null::varchar as retrieved_at, null::varchar as ingested_at, null::varchar as transform_version
        where false;
        create table sb9_aliases as select
          null::bigint as sb9_sequence, null::varchar as alias, null::bigint as source_line_number,
          null::varchar as raw_row, null::varchar as source_catalog, null::varchar as source_version,
          null::varchar as source_pk, null::varchar as source_row_hash, null::varchar as retrieval_checksum,
          null::varchar as retrieved_at, null::varchar as ingested_at, null::varchar as transform_version
        where false;
        create table sb9_orbits as select
          null::bigint as sb9_sequence, null::bigint as orbit_number, null::double as period_days,
          null::varchar as period_fixed_flag, null::double as period_error_days, null::double as periastron_jd,
          null::varchar as periastron_fixed_flag, null::double as periastron_error_days,
          null::varchar as periastron_epoch_flag, null::double as eccentricity,
          null::varchar as eccentricity_fixed_flag, null::double as eccentricity_error,
          null::double as omega_deg, null::varchar as omega_fixed_flag, null::double as omega_error_deg,
          null::double as semi_amplitude_primary_kms, null::varchar as semi_amplitude_primary_uncertain,
          null::varchar as semi_amplitude_primary_fixed_flag, null::double as semi_amplitude_primary_error_kms,
          null::double as semi_amplitude_secondary_kms, null::varchar as semi_amplitude_secondary_uncertain,
          null::varchar as semi_amplitude_secondary_fixed_flag, null::double as semi_amplitude_secondary_error_kms,
          null::double as systemic_velocity_kms, null::varchar as systemic_velocity_uncertain,
          null::varchar as systemic_velocity_fixed_flag, null::double as systemic_velocity_error_kms,
          null::double as rms_primary_kms, null::double as rms_secondary_kms,
          null::bigint as observation_count_primary, null::bigint as observation_count_secondary,
          null::double as orbit_grade, null::varchar as reference_bibcode, null::varchar as contributor,
          null::varchar as access_code, null::bigint as source_line_number, null::varchar as raw_row,
          null::varchar as source_catalog, null::varchar as source_version, null::varchar as source_pk,
          null::varchar as source_row_hash, null::varchar as retrieval_checksum,
          null::varchar as retrieved_at, null::varchar as ingested_at, null::varchar as transform_version
        where false
        """
    )


def materialize_arm(
    con: duckdb.DuckDBPyConnection,
    state_dir: Path,
    build_id: str,
    ingested_at: str,
    transform_version: str,
    manifest_entries: dict[str, dict[str, Any]],
    enabled: bool = True,
) -> dict[str, int]:
    cooked_dir = state_dir / "cooked" / "sb9"
    systems_path = cooked_dir / "sb9_systems.csv"
    aliases_path = cooked_dir / "sb9_aliases.csv"
    orbits_path = cooked_dir / "sb9_orbits.csv"
    required = (systems_path, aliases_path, orbits_path)
    if not enabled or not all(path.exists() for path in required):
        _empty_tables(con)
    else:
        main_manifest = manifest_entries.get("sb9_main", {})
        alias_manifest = manifest_entries.get("sb9_alias", {})
        orbit_manifest = manifest_entries.get("sb9_orbits", {})
        version = str(main_manifest.get("source_version") or SB9_VERSION_FALLBACK)

        def materialize(name: str, path: Path, manifest: dict[str, Any], pk_expr: str) -> None:
            con.execute(
                f"""
                create table {name} as
                select
                  raw.*,
                  'sb9'::varchar as source_catalog,
                  {_sql(version)}::varchar as source_version,
                  ({pk_expr})::varchar as source_pk,
                  sha256(raw.raw_row)::varchar as source_row_hash,
                  {_sql(str(manifest.get('sha256') or ''))}::varchar as retrieval_checksum,
                  {_sql(str(manifest.get('retrieved_at') or ''))}::varchar as retrieved_at,
                  {_sql(ingested_at)}::varchar as ingested_at,
                  {_sql(transform_version)}::varchar as transform_version
                from read_csv_auto(
                  {_sql(str(path))}, header=true, all_varchar=false,
                  strict_mode=false, null_padding=true
                ) raw
                """
            )

        materialize("sb9_systems", systems_path, main_manifest, "'SB9 ' || raw.sb9_sequence::varchar")
        materialize(
            "sb9_aliases",
            aliases_path,
            alias_manifest,
            "'SB9 ' || raw.sb9_sequence::varchar || ':alias:' || raw.source_line_number::varchar",
        )
        materialize(
            "sb9_orbits",
            orbits_path,
            orbit_manifest,
            "'SB9 ' || raw.sb9_sequence::varchar || ':orbit:' || raw.orbit_number::varchar",
        )

    con.execute(
        f"""
        create table multiple_component_evidence_matches as
        with refs as (
          select
            d.msc_system_detail_id,
            d.wds_id,
            d.primary_label,
            d.secondary_label,
            d.primary_component_key,
            d.secondary_component_key,
            d.source_pk as msc_source_pk,
            try_cast(regexp_extract(upper(coalesce(d.comment, '')), 'SB9_([0-9]+)', 1) as bigint) as sb9_sequence
          from msc_system_details d
          where regexp_matches(upper(coalesce(d.comment, '')), 'SB9_[0-9]+')
        ), ranked as (
          select
            r.*,
            count(*) over (partition by r.sb9_sequence) as msc_reference_count,
            s.spectral_type_primary,
            s.spectral_type_secondary,
            s.component_label as sb9_component_label,
            s.source_version,
            s.source_pk as sb9_source_pk,
            s.source_row_hash,
            s.retrieval_checksum,
            s.retrieved_at,
            p.stable_component_key is not null as primary_exists,
            q.stable_component_key is not null as secondary_exists
          from refs r
          left join sb9_systems s on s.sb9_sequence = r.sb9_sequence
          left join component_entities p on p.stable_component_key = r.primary_component_key and p.component_type = 'star'
          left join component_entities q on q.stable_component_key = r.secondary_component_key and q.component_type = 'star'
        )
        select
          row_number() over (order by sb9_sequence, wds_id, msc_system_detail_id)::bigint as evidence_match_id,
          {_sql(build_id)}::varchar as build_id,
          'sb9'::varchar as source_catalog,
          sb9_sequence::varchar as source_record_id,
          wds_id,
          primary_label,
          secondary_label,
          primary_component_key,
          secondary_component_key,
          case
            when sb9_source_pk is null then 'quarantined'
            when msc_reference_count <> 1 then 'quarantined'
            when primary_component_key is null or secondary_component_key is null then 'quarantined'
            when not primary_exists or not secondary_exists then 'quarantined'
            when nullif(trim(coalesce(spectral_type_primary, '')), '') is null
             and nullif(trim(coalesce(spectral_type_secondary, '')), '') is null then 'excluded'
            else 'accepted'
          end::varchar as match_status,
          case
            when sb9_source_pk is null then 'sb9_sequence_missing'
            when msc_reference_count <> 1 then 'ambiguous_msc_sequence_reference'
            when primary_component_key is null or secondary_component_key is null then 'unresolved_msc_endpoint_key'
            when not primary_exists or not secondary_exists then 'endpoint_absent_from_component_graph'
            when nullif(trim(coalesce(spectral_type_primary, '')), '') is null
             and nullif(trim(coalesce(spectral_type_secondary, '')), '') is null then 'no_component_spectral_type'
            else 'exact_msc_sb9_sequence_and_resolved_endpoints'
          end::varchar as reason,
          1.0::double as match_score,
          {_sql(MATCH_VERSION)}::varchar as match_version,
          json_object(
            'msc_system_detail_id', msc_system_detail_id,
            'msc_source_pk', msc_source_pk,
            'sb9_sequence', sb9_sequence,
            'sb9_component_label', sb9_component_label,
            'msc_reference_count', msc_reference_count,
            'primary_exists', primary_exists,
            'secondary_exists', secondary_exists
          )::varchar as evidence_json,
          source_version,
          sb9_source_pk as source_pk,
          source_row_hash,
          retrieval_checksum,
          retrieved_at,
          {_sql(ingested_at)}::varchar as ingested_at,
          {_sql(transform_version)}::varchar as transform_version
        from ranked
        """
    )

    core_tables = {
        str(row[0])
        for row in con.execute(
            "select table_name from information_schema.tables where table_catalog='core'"
        ).fetchall()
    }
    if "eclipsing_binaries" in core_tables:
        con.execute(
            f"""
            insert into multiple_component_evidence_matches
            with existing_max as (
              select coalesce(max(evidence_match_id), 0)::bigint as max_id
              from multiple_component_evidence_matches
            ), source_rows as (
              select
                e.eclipsing_binary_id,
                e.source_pk,
                e.object_name,
                e.system_id,
                s.wds_id,
                e.period_days,
                e.spectral_type_primary,
                e.spectral_type_secondary,
                e.source_version,
                e.source_row_hash,
                e.retrieval_checksum,
                e.retrieved_at,
                e.ingested_at,
                e.transform_version
              from core.eclipsing_binaries e
              join core.systems s on s.system_id = e.system_id
              where e.source_catalog = 'debcat'
                and e.system_id is not null
                and s.wds_id is not null
                and e.period_days is not null
                and (
                  nullif(trim(coalesce(e.spectral_type_primary, '')), '') is not null
                  or nullif(trim(coalesce(e.spectral_type_secondary, '')), '') is not null
                )
            ), candidates as (
              select
                e.*,
                d.msc_system_detail_id,
                d.primary_label,
                d.secondary_label,
                d.primary_component_key,
                d.secondary_component_key,
                abs(d.period_days - e.period_days) as period_delta_days,
                count(d.msc_system_detail_id) over (partition by e.eclipsing_binary_id) as candidate_count,
                p.stable_component_key is not null as primary_exists,
                q.stable_component_key is not null as secondary_exists
              from source_rows e
              left join msc_system_details d
                on d.wds_id = e.wds_id
               and d.period_days is not null
               and abs(d.period_days - e.period_days) <= greatest(0.01, e.period_days * 0.01)
              left join component_entities p
                on p.stable_component_key = d.primary_component_key and p.component_type = 'star'
              left join component_entities q
                on q.stable_component_key = d.secondary_component_key and q.component_type = 'star'
            ), selected as (
              select *
              from candidates
              qualify row_number() over (
                partition by eclipsing_binary_id
                order by period_delta_days nulls last, msc_system_detail_id nulls last
              ) = 1
            )
            select
              existing_max.max_id + row_number() over (order by eclipsing_binary_id)::bigint,
              {_sql(build_id)}::varchar,
              'debcat'::varchar,
              source_pk::varchar,
              wds_id,
              primary_label,
              secondary_label,
              primary_component_key,
              secondary_component_key,
              case
                when candidate_count = 0 then 'quarantined'
                when candidate_count <> 1 then 'quarantined'
                when primary_component_key is null or secondary_component_key is null then 'quarantined'
                when not primary_exists or not secondary_exists then 'quarantined'
                else 'accepted'
              end::varchar,
              case
                when candidate_count = 0 then 'no_msc_period_match'
                when candidate_count <> 1 then 'ambiguous_msc_period_match'
                when primary_component_key is null or secondary_component_key is null then 'unresolved_msc_endpoint_key'
                when not primary_exists or not secondary_exists then 'endpoint_absent_from_component_graph'
                else 'unique_system_and_period_match_with_resolved_endpoints'
              end::varchar,
              case when candidate_count = 1 then 0.98 else 0.0 end::double,
              {_sql(MATCH_VERSION)}::varchar,
              json_object(
                'eclipsing_binary_id', eclipsing_binary_id,
                'object_name', object_name,
                'system_id', system_id,
                'msc_system_detail_id', msc_system_detail_id,
                'source_period_days', period_days,
                'period_delta_days', period_delta_days,
                'candidate_count', candidate_count
              )::varchar,
              source_version,
              source_pk::varchar,
              source_row_hash,
              retrieval_checksum,
              retrieved_at,
              coalesce(ingested_at, {_sql(ingested_at)})::varchar,
              coalesce(transform_version, {_sql(transform_version)})::varchar
            from selected, existing_max
            """
        )

    con.execute(
        """
        create table multiple_component_stellar_evidence as
        with sb9_accepted as (
          select m.*, s.spectral_type_primary, s.spectral_type_secondary
          from multiple_component_evidence_matches m
          join sb9_systems s on s.sb9_sequence::varchar = m.source_record_id
          where m.match_status = 'accepted' and m.source_catalog = 'sb9'
        ), debcat_accepted as (
          select m.*, e.spectral_type_primary, e.spectral_type_secondary
          from multiple_component_evidence_matches m
          join core.eclipsing_binaries e on e.source_pk = try_cast(m.source_record_id as bigint)
          where m.match_status = 'accepted'
            and m.source_catalog = 'debcat'
            and e.source_catalog = 'debcat'
        ), accepted as (
          select * from sb9_accepted
          union all
          select * from debcat_accepted
        ), endpoints as (
          select evidence_match_id, primary_component_key as stable_component_key,
                 'primary'::varchar as component_role, spectral_type_primary as spectral_type_raw,
                 source_catalog, source_version, source_pk, source_row_hash, retrieval_checksum,
                 retrieved_at, ingested_at, transform_version
          from accepted
          union all
          select evidence_match_id, secondary_component_key, 'secondary', spectral_type_secondary,
                 source_catalog, source_version, source_pk, source_row_hash, retrieval_checksum,
                 retrieved_at, ingested_at, transform_version
          from accepted
        )
        select
          row_number() over (order by evidence_match_id, component_role)::bigint as component_evidence_id,
          evidence_match_id, stable_component_key, component_role, spectral_type_raw,
          source_catalog, source_version,
          source_pk || ':' || component_role as source_pk,
          source_row_hash, retrieval_checksum, retrieved_at, ingested_at, transform_version
        from endpoints
        where nullif(trim(coalesce(spectral_type_raw, '')), '') is not null
        """
    )

    con.execute(
        f"""
        insert into derived_stellar_classifications
        with existing_max as (
          select coalesce(max(derived_classification_id), 0)::bigint as max_id
          from derived_stellar_classifications
        ), classified as (
          select
            e.*,
            case
              when regexp_matches(e.spectral_type_raw, '^d[OBAFGKMLTY]') then upper(substr(e.spectral_type_raw, 2, 1))
              when regexp_matches(upper(e.spectral_type_raw), '^W[CNOR]') then 'WR'
              when regexp_matches(upper(e.spectral_type_raw), '^D') then 'WD'
              when regexp_matches(upper(e.spectral_type_raw), '^[OBAFGKMLTY]')
                then regexp_extract(upper(e.spectral_type_raw), '^([OBAFGKMLTY])', 1)
              else null
            end::varchar as class_token
          from multiple_component_stellar_evidence e
        )
        select
          existing_max.max_id + row_number() over (order by c.stable_component_key, c.component_role)::bigint,
          {_sql(build_id)}::varchar,
          'component'::varchar,
          cast(null as bigint),
          cast(null as bigint),
          cast(null as varchar),
          c.stable_component_key,
          'stellar_display_class'::varchar,
          c.class_token,
          'source'::varchar,
          'source_component_spectral_type_v1'::varchar,
          {_sql(MATCH_VERSION)}::varchar,
          json_object(
            'stable_component_key', c.stable_component_key,
            'component_role', c.component_role,
            'spectral_type_raw', c.spectral_type_raw,
            'evidence_match_id', c.evidence_match_id
          )::varchar,
          json_object(
            'binding_requires_exact_msc_sb9_sequence', true,
            'binding_requires_unique_sequence_reference', true,
            'binding_requires_existing_graph_endpoints', true
          )::varchar,
          false,
          false,
          0.96::double,
          'high'::varchar,
          'accepted'::varchar,
          c.source_catalog,
          c.source_version,
          c.source_pk || ':stellar_display_class',
          sha256(concat_ws('|', c.stable_component_key, c.spectral_type_raw, c.source_pk)),
          c.retrieval_checksum,
          c.retrieved_at,
          c.ingested_at,
          c.transform_version
        from classified c, existing_max
        where c.class_token is not null
        """
    )

    return {
        "sb9_systems": int(con.execute("select count(*) from sb9_systems").fetchone()[0] or 0),
        "sb9_aliases": int(con.execute("select count(*) from sb9_aliases").fetchone()[0] or 0),
        "sb9_orbits": int(con.execute("select count(*) from sb9_orbits").fetchone()[0] or 0),
        "accepted_component_matches": int(
            con.execute(
                "select count(*) from multiple_component_evidence_matches where match_status='accepted'"
            ).fetchone()[0]
            or 0
        ),
        "quarantined_component_matches": int(
            con.execute(
                "select count(*) from multiple_component_evidence_matches where match_status='quarantined'"
            ).fetchone()[0]
            or 0
        ),
        "accepted_sb9_component_matches": int(
            con.execute(
                "select count(*) from multiple_component_evidence_matches where source_catalog='sb9' and match_status='accepted'"
            ).fetchone()[0]
            or 0
        ),
        "accepted_debcat_component_matches": int(
            con.execute(
                "select count(*) from multiple_component_evidence_matches where source_catalog='debcat' and match_status='accepted'"
            ).fetchone()[0]
            or 0
        ),
        "component_spectral_evidence": int(
            con.execute("select count(*) from multiple_component_stellar_evidence").fetchone()[0] or 0
        ),
    }
