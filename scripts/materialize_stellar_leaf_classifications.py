#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


PROJECTION_VERSION = "stellar_leaf_display_classification_v1"
VALID_CLASSES = (
    "O", "B", "A", "F", "G", "K", "M", "L", "T", "Y", "WR", "WD",
    "NS", "PULSAR", "MAGNETAR", "BLACK HOLE", "UNKNOWN",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return bool(
        con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema='main' AND table_name=?",
            [table_name],
        ).fetchone()[0]
    )


def spectral_class_sql(raw: str, spectral_class: str, object_type: str) -> str:
    return f"""
      case
        when regexp_matches(lower(coalesce({raw}, '') || ' ' || coalesce({object_type}, '')), 'black[ _-]?hole') then 'BLACK HOLE'
        when regexp_matches(lower(coalesce({raw}, '') || ' ' || coalesce({object_type}, '')), 'magnetar') then 'MAGNETAR'
        when lower(coalesce({object_type}, '')) = 'pulsar' or regexp_matches(lower(coalesce({raw}, '')), 'pulsar|\\bpsr\\b') then 'PULSAR'
        when regexp_matches(lower(coalesce({raw}, '') || ' ' || coalesce({object_type}, '')), 'neutron[ _-]?star') then 'NS'
        when regexp_matches(coalesce({raw}, ''), '^d[OBAFGKMLTY]') then upper(substr({raw}, 2, 1))
        when regexp_matches(coalesce({raw}, ''), '^sd[OBAFGKMLTY]') then upper(substr({raw}, 3, 1))
        when regexp_matches(coalesce({raw}, ''), '^(esd|usd)[OBAFGKMLTY]') then upper(substr({raw}, 4, 1))
        when regexp_matches(upper(coalesce({raw}, '')), '^W[CNOR]') or regexp_matches(lower(coalesce({raw}, '')), 'wolf[ _-]?rayet') then 'WR'
        when lower(coalesce({object_type}, '')) = 'white_dwarf'
          or upper(coalesce({spectral_class}, '')) = 'D'
          or regexp_matches(coalesce({raw}, ''), '^(WD|D($|[ABCOQZX0-9]))', 'i') then 'WD'
        when upper(coalesce({spectral_class}, '')) in ('O','B','A','F','G','K','M','L','T','Y') then upper({spectral_class})
        when regexp_matches(upper(coalesce({raw}, '')), '^[OBAFGKMLTY]') then regexp_extract(upper({raw}), '^([OBAFGKMLTY])', 1)
        else null
      end
    """


def mass_class_sql(mass: str) -> str:
    return f"""
      case
        when {mass} is null or {mass} <= 0 then null
        when {mass} < 0.08 then 'L'
        when {mass} < 0.65 then 'M'
        when {mass} < 0.85 then 'K'
        when {mass} < 1.04 then 'G'
        when {mass} < 1.40 then 'F'
        when {mass} < 2.10 then 'A'
        when {mass} < 16.0 then 'B'
        else 'O'
      end
    """


def materialize(*, core_db: Path, arm_db: Path, hierarchy_db: Path, build_id: str) -> dict[str, object]:
    con = duckdb.connect(str(arm_db))
    try:
        con.execute(f"attach {sql_literal(core_db)} as core (read_only)")
        con.execute(f"attach {sql_literal(hierarchy_db)} as hierarchy (read_only)")
        con.execute("drop table if exists stellar_leaf_display_classifications")

        has_e6_display = table_exists(
            con, "e6_selected_stellar_display_classifications"
        )
        has_e6_msc = all(
            table_exists(con, table_name)
            for table_name in (
                "e6_component_msc_component_entities",
                "e6_component_msc_classification_projection",
                "e6_component_msc_stellar_parameter_projection",
            )
        )

        core_class = spectral_class_sql("s.spectral_type_raw", "s.spectral_class", "s.object_type")
        raw_primary_class = spectral_class_sql("r.spectral_type_primary", "null", "'star'")
        raw_primary_mass_class = mass_class_sql("r.mass_primary_msun")
        e6_core_candidates = ""
        legacy_core_candidates = f"""
              select l.hierarchy_node_key, 0 as evidence_rank, l.core_class_token as class_token,
                     'source'::varchar as classification_status,
                     'core_leaf_source_class_v1'::varchar as evidence_basis,
                     cast(null as varchar) as selected_fact_id,
                     l.core_source_catalog as source_catalog, cast(null as varchar) as source_version,
                     l.core_source_pk as source_pk, cast(null as varchar) as retrieval_checksum,
                     cast(null as varchar) as retrieved_at,
                     l.core_spectral_type_raw as source_value, 0.95::double as confidence_score
              from leaves l where l.core_class_token is not null
              union all
        """
        canonical_legacy_guard = ""
        if has_e6_display:
            legacy_core_candidates = ""
            canonical_legacy_guard = " and l.star_id is null"
            e6_core_candidates = """
              select l.hierarchy_node_key,
                     case d.evidence_basis
                       when 'canonical_object_type' then 0
                       when 'selected_spectral_type_optical' then 1
                       when 'selected_spectral_type_infrared' then 1
                       when 'selected_spectral_type_simbad' then 1
                       when 'stability_core_source_class_fallback' then 2
                       when 'selected_teff_visual_class_prior' then 3
                       when 'selected_bp_rp_visual_class_prior' then 4
                       when 'selected_mass_main_sequence_prior' then 5
                       else 6
                     end as evidence_rank,
                     d.classification_value as class_token,
                     d.classification_status,
                     d.evidence_basis,
                     d.selected_fact_id,
                     'evidence_lake_v2'::varchar as source_catalog,
                     d.projection_version as source_version,
                     d.selected_display_classification_id::varchar as source_pk,
                     cast(null as varchar) as retrieval_checksum,
                     cast(null as varchar) as retrieved_at,
                     d.source_value,
                     d.confidence_score
              from leaves l
              join e6_selected_stellar_display_classifications d using (star_id)
              where l.star_id is not null and d.classification_value <> 'UNKNOWN'
              union all
            """

        e6_msc_candidates = ""
        if has_e6_msc:
            e6_msc_class = spectral_class_sql(
                "cp.classification_raw", "cp.classification_normalized", "'star'"
            )
            e6_msc_candidates = f"""
              select l.hierarchy_node_key, 1 as evidence_rank,
                     {e6_msc_class}::varchar as class_token,
                     'source'::varchar as classification_status,
                     'e6_msc_component_spectral_type_v1'::varchar as evidence_basis,
                     cp.evidence_id as selected_fact_id,
                     ce.source_id as source_catalog,
                     ce.release_id as source_version,
                     cp.evidence_id as source_pk,
                     cast(null as varchar) as retrieval_checksum,
                     cast(null as varchar) as retrieved_at,
                     cp.classification_raw as source_value,
                     0.90::double as confidence_score
              from leaves l
              join e6_component_msc_component_entities ce
                on ce.canonical_system_stable_object_key=l.system_stable_object_key
               and ce.component_label_normalized=l.catalog_component_label
               and ce.binding_status='accepted'
              join e6_component_msc_classification_projection cp
                on cp.component_entity_id=ce.component_entity_id
               and cp.projection_status='eligible_for_quantity_selection'
              where {e6_msc_class} is not null
              union all
            """
            e6_msc_mass_class = mass_class_sql("sp.normalized_value")
            e6_msc_candidates += f"""
              select l.hierarchy_node_key, 5 as evidence_rank,
                     {e6_msc_mass_class}::varchar as class_token,
                     'assumed'::varchar as classification_status,
                     'e6_msc_component_mass_main_sequence_prior_v1'::varchar as evidence_basis,
                     sp.evidence_id as selected_fact_id,
                     ce.source_id as source_catalog,
                     ce.release_id as source_version,
                     sp.evidence_id as source_pk,
                     cast(null as varchar) as retrieval_checksum,
                     cast(null as varchar) as retrieved_at,
                     sp.value_raw as source_value,
                     0.35::double as confidence_score
              from leaves l
              join e6_component_msc_component_entities ce
                on ce.canonical_system_stable_object_key=l.system_stable_object_key
               and ce.component_label_normalized=l.catalog_component_label
               and ce.binding_status='accepted'
              join e6_component_msc_stellar_parameter_projection sp
                on sp.component_entity_id=ce.component_entity_id
               and sp.projection_status='eligible_for_quantity_selection'
               and sp.quantity_key='mass'
              where {e6_msc_mass_class} is not null
              union all
            """

        con.execute(
            f"""
            create table stellar_leaf_display_classifications as
            with core_leaves as (
              select
                s.system_id,
                sys.stable_object_key as system_stable_object_key,
                n.hierarchy_node_key,
                n.canonical_key,
                ('comp:star:' || n.canonical_key)::varchar as leaf_component_key,
                case
                  when sys.wds_id is not null and nullif(trim(s.component), '') is not null
                    then 'comp:msc:wds:' || sys.wds_id || ':' || lower(trim(s.component))
                  else null
                end::varchar as evidence_component_key,
                s.star_id,
                s.stable_object_key,
                s.star_name as display_name,
                case when nullif(trim(s.component), '') is not null
                  then upper(substr(trim(s.component), 1, 1)) || lower(substr(trim(s.component), 2))
                end as catalog_component_label,
                n.node_kind,
                n.source_basis as hierarchy_source_basis,
                s.spectral_type_raw as core_spectral_type_raw,
                s.spectral_class as core_spectral_class,
                s.object_type as core_object_type,
                s.source_catalog as core_source_catalog,
                cast(s.source_pk as varchar) as core_source_pk,
                {core_class}::varchar as core_class_token
              from core.stars s
              join core.systems sys using (system_id)
              join hierarchy.hierarchy_nodes n on n.canonical_key = s.stable_object_key
              where n.component_family = 'star'
                and not exists (
                  select 1
                  from hierarchy.hierarchy_edges e
                  join hierarchy.hierarchy_nodes child on child.hierarchy_node_key = e.child_node_key
                  where e.parent_node_key = n.hierarchy_node_key
                    and child.component_family = 'star'
                )
            ), inferred_leaves as (
              select
                sys.system_id,
                sys.stable_object_key as system_stable_object_key,
                n.hierarchy_node_key,
                n.canonical_key,
                ('comp:msc:wds:' || n.wds_id || ':' || split_part(n.hierarchy_node_key, ':', 5))::varchar as leaf_component_key,
                ('comp:msc:wds:' || n.wds_id || ':' || split_part(n.hierarchy_node_key, ':', 5))::varchar as evidence_component_key,
                cast(null as bigint) as star_id,
                cast(null as varchar) as stable_object_key,
                n.display_name,
                (
                  upper(substr(split_part(n.hierarchy_node_key, ':', 5), 1, 1))
                  || lower(substr(split_part(n.hierarchy_node_key, ':', 5), 2))
                )::varchar as catalog_component_label,
                n.node_kind,
                n.source_basis as hierarchy_source_basis,
                cast(null as varchar) as core_spectral_type_raw,
                cast(null as varchar) as core_spectral_class,
                cast(null as varchar) as core_object_type,
                cast(null as varchar) as core_source_catalog,
                cast(null as varchar) as core_source_pk,
                cast(null as varchar) as core_class_token
              from hierarchy.hierarchy_nodes n
              join core.systems sys on sys.wds_id = n.wds_id
              where n.node_kind = 'inferred_star_leaf'
                and n.component_family = 'star'
                and n.component_type = 'star'
                and not exists (
                  select 1 from hierarchy.hierarchy_edges e
                  where e.parent_node_key = n.hierarchy_node_key
                )
            ), leaves as (
              select * from core_leaves
              union all
              select * from inferred_leaves
            ), raw_endpoints as (
              select primary_component_key as stable_component_key, spectral_type_primary as spectral_type_raw,
                     mass_primary_msun as mass_msun, source_catalog, source_version,
                     source_pk || ':primary' as source_pk, retrieval_checksum, retrieved_at,
                     {raw_primary_class}::varchar as class_token,
                     {raw_primary_mass_class}::varchar as mass_class_token
              from msc_system_details r where primary_component_key is not null
              union all
              select secondary_component_key, spectral_type_secondary, mass_secondary_msun,
                     source_catalog, source_version, source_pk || ':secondary', retrieval_checksum, retrieved_at,
                     {spectral_class_sql('r.spectral_type_secondary', 'null', "'star'")}::varchar,
                     {mass_class_sql('r.mass_secondary_msun')}::varchar
              from msc_system_details r where secondary_component_key is not null
            ), candidates as (
              {e6_core_candidates}
              {legacy_core_candidates}
              {e6_msc_candidates}
              select l.hierarchy_node_key, 10, d.classification_value, 'source',
                     d.derivation_method, cast(null as varchar), d.source_catalog, d.source_version, d.source_pk,
                     d.retrieval_checksum, d.retrieved_at,
                     try_cast(json_extract_string(d.input_parameters_json, '$.spectral_type_raw') as varchar),
                     coalesce(d.confidence_score, 0.9)
              from leaves l join derived_stellar_classifications d
                on d.stable_component_key = l.evidence_component_key
               and d.classification_key = 'stellar_display_class'
              where d.review_status = 'accepted' and d.classification_status = 'source'
                {canonical_legacy_guard}
              union all
              select l.hierarchy_node_key, 20, r.class_token, 'derived',
                     'msc_exact_leaf_spectral_type_v1', cast(null as varchar), r.source_catalog, r.source_version,
                     r.source_pk, r.retrieval_checksum, r.retrieved_at, r.spectral_type_raw, 0.72
              from leaves l join raw_endpoints r on r.stable_component_key = l.evidence_component_key
              where r.class_token is not null {canonical_legacy_guard}
              union all
              select l.hierarchy_node_key, 30, d.classification_value, 'derived',
                     d.derivation_method, cast(null as varchar), d.source_catalog, d.source_version, d.source_pk,
                     d.retrieval_checksum, d.retrieved_at,
                     try_cast(json_extract_string(d.input_parameters_json, '$.spectral_type_raw') as varchar),
                     coalesce(d.confidence_score, 0.55)
              from leaves l join derived_stellar_classifications d
                on d.stable_component_key = l.evidence_component_key
               and d.classification_key = 'stellar_display_class'
              where d.classification_status = 'derived' {canonical_legacy_guard}
              union all
              select l.hierarchy_node_key, 40, coalesce(d.classification_value, r.mass_class_token), 'assumed',
                     coalesce(d.derivation_method, 'mass_main_sequence_prior_v1'),
                     cast(null as varchar),
                     coalesce(d.source_catalog, r.source_catalog), coalesce(d.source_version, r.source_version),
                     coalesce(d.source_pk, r.source_pk), coalesce(d.retrieval_checksum, r.retrieval_checksum),
                     coalesce(d.retrieved_at, r.retrieved_at), cast(null as varchar),
                     coalesce(d.confidence_score, 0.35)
              from leaves l
              left join derived_stellar_classifications d
                on d.stable_component_key = l.evidence_component_key
               and d.classification_key = 'stellar_display_class'
               and d.classification_status = 'assumed'
              left join raw_endpoints r on r.stable_component_key = l.evidence_component_key
              where coalesce(d.classification_value, r.mass_class_token) is not null
                {canonical_legacy_guard}
            ), ranked as (
              select c.*,
                     row_number() over (
                       partition by c.hierarchy_node_key
                       order by c.evidence_rank, c.confidence_score desc, c.source_pk nulls last, c.class_token
                     ) as choice_rank
              from candidates c
              where c.class_token in {VALID_CLASSES[:-1]}
            ), conflicts as (
              select hierarchy_node_key,
                     count(distinct class_token)::integer as distinct_candidate_class_count,
                     to_json(list(distinct class_token order by class_token))::varchar as candidate_classes_json
              from ranked group by hierarchy_node_key
            )
            select
              row_number() over (order by l.system_id, l.hierarchy_node_key)::bigint as stellar_leaf_classification_id,
              {sql_literal(build_id)}::varchar as build_id,
              l.system_id,
              l.system_stable_object_key,
              l.hierarchy_node_key,
              l.leaf_component_key,
              l.evidence_component_key,
              l.star_id,
              l.stable_object_key,
              l.display_name,
              l.catalog_component_label,
              l.node_kind,
              l.hierarchy_source_basis,
              coalesce(r.class_token, 'UNKNOWN')::varchar as classification_value,
              coalesce(r.classification_status, 'missing')::varchar as classification_status,
              coalesce(r.evidence_basis, 'no_exact_leaf_classification_evidence')::varchar as evidence_basis,
              r.selected_fact_id,
              r.source_catalog,
              r.source_version,
              r.source_pk,
              r.retrieval_checksum,
              r.retrieved_at,
              r.source_value,
              coalesce(r.confidence_score, 0.0)::double as confidence_score,
              coalesce(c.distinct_candidate_class_count, 0)::integer as distinct_candidate_class_count,
              coalesce(c.candidate_classes_json, '[]')::varchar as candidate_classes_json,
              (coalesce(c.distinct_candidate_class_count, 0) > 1)::boolean as has_classification_conflict,
              {sql_literal(PROJECTION_VERSION)}::varchar as projection_version
            from leaves l
            left join ranked r on r.hierarchy_node_key = l.hierarchy_node_key and r.choice_rank = 1
            left join conflicts c on c.hierarchy_node_key = l.hierarchy_node_key
            """
        )
        con.execute("create unique index stellar_leaf_display_hierarchy_key_uq on stellar_leaf_display_classifications(hierarchy_node_key)")
        con.execute("create index stellar_leaf_display_system_idx on stellar_leaf_display_classifications(system_id)")

        counts = dict(
            con.execute(
                """
                select classification_status, count(*)::bigint
                from stellar_leaf_display_classifications group by 1 order by 1
                """
            ).fetchall()
        )
        total = int(con.execute("select count(*) from stellar_leaf_display_classifications").fetchone()[0])
        duplicates = int(
            con.execute(
                """
                select count(*) from (
                  select hierarchy_node_key from stellar_leaf_display_classifications
                  group by 1 having count(*) <> 1
                )
                """
            ).fetchone()[0]
        )
        invalid = int(
            con.execute(
                f"""
                select count(*) from stellar_leaf_display_classifications
                where classification_value not in {VALID_CLASSES}
                   or classification_status not in ('source','derived','assumed','missing')
                """
            ).fetchone()[0]
        )
        return {
            "schema_version": PROJECTION_VERSION,
            "build_id": build_id,
            "generated_at": utc_now(),
            "rows": total,
            "by_status": counts,
            "classification_conflicts": int(
                con.execute("select count(*) from stellar_leaf_display_classifications where has_classification_conflict").fetchone()[0]
            ),
            "duplicate_leaf_keys": duplicates,
            "invalid_rows": invalid,
            "status": "pass" if total > 0 and duplicates == 0 and invalid == 0 else "fail",
        }
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize the shared stellar-leaf display classification projection.")
    parser.add_argument("--core-db", type=Path, required=True)
    parser.add_argument("--arm-db", type=Path, required=True)
    parser.add_argument("--hierarchy-db", type=Path, required=True)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--report-path", type=Path)
    args = parser.parse_args()
    for path in (args.core_db, args.arm_db, args.hierarchy_db):
        if not path.exists():
            raise SystemExit(f"Missing required database: {path}")
    report = materialize(
        core_db=args.core_db,
        arm_db=args.arm_db,
        hierarchy_db=args.hierarchy_db,
        build_id=args.build_id,
    )
    if args.report_path:
        args.report_path.parent.mkdir(parents=True, exist_ok=True)
        args.report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
