#!/usr/bin/env python3
"""Compile the release-scoped Evidence Lake v2 identity and scope graph."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from evidence_lake_registry import stable_hash
from fetch_gaia_dr2_identity import (
    required_path,
    target_source_queries,
    typed_paths,
)


DEFAULT_POLICY = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "evidence_lake"
    / "identity_graph_policy.json"
)
COMPILER_VERSION = "evidence_identity_compiler_v8"
CONSUMED_TABLE_KEYS = {
    ("gaia.dr3.dr2_neighbourhood", "gaia_dr2_identity_target_set"),
    ("gaia.dr3.dr2_neighbourhood", "gaia_dr2_neighbourhood_union"),
    ("gaia.dr3.dr2_neighbourhood_reverse", "gaia_dr3_identity_target_set"),
    ("gaia.dr3.dr2_neighbourhood_reverse", "gaia_dr2_neighbourhood_reverse_union"),
    ("nasa_exoplanet_archive.planetary_systems", "ps"),
    ("nasa_exoplanet_archive.planetary_systems", "pscomppars"),
    ("tess.identity_and_candidate_evidence", "mast_tic_targeted"),
    ("clusters.cantat_gaudin_2020", "cantat_gaudin_2020_members"),
    ("compact.gaia_edr3_white_dwarf", "gaia_edr3_white_dwarf_main"),
    ("ultracool.ultracoolsheet", "UltracoolSheet_Main"),
    ("multiplicity.msc", "msc_comp"),
    ("multiplicity.msc", "msc_sys"),
    ("multiplicity.wds", "wdsweb_summ2"),
}
TABLE_ORDER = {
    "graph_metadata": "graph_id",
    "canonical_object_nodes": "object_node_key",
    "identifier_nodes": "identifier_node_key",
    "canonical_identifier_bindings": "binding_key",
    "release_crossmatch_edges": "edge_key",
    "dr2_release_outcomes": "dr2_source_id",
    "source_record_bindings": "source_record_binding_key",
    "scope_claims": "scope_claim_key",
    "identifier_collision_diagnostics": "identifier_node_key",
    "identity_quarantine": "quarantine_key",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    )
    temp = Path(handle.name)
    try:
        with handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temp, path)
    except Exception:
        temp.unlink(missing_ok=True)
        raise


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.evidence_identity_graph_policy.v1":
        raise ValueError("unsupported identity graph policy schema")
    outcomes = policy.get("outcomes")
    if outcomes != ["accepted", "missing", "excluded", "ambiguous", "quarantined"]:
        raise ValueError("identity outcome vocabulary is incomplete or reordered")
    constraints = policy.get("production_constraints", {})
    if constraints.get("canonical_containment_mutation") is not False:
        raise ValueError("E2 policy must prohibit canonical containment mutation")
    if constraints.get("canonical_inventory_mutation") is not False:
        raise ValueError("E2 policy must prohibit canonical inventory mutation")
    reconciliation = policy.get("gaia_release_reconciliation", {})
    lineage_fields = {
        "table",
        "upstream_release_id",
        "forward_registry_source_id",
        "forward_registry_release_id",
        "reverse_registry_source_id",
        "reverse_registry_release_id",
    }
    if lineage_fields - set(reconciliation):
        raise ValueError("Gaia release policy lacks row-level source/release lineage")
    scope_kinds = set(policy.get("scope_kinds", {}))
    for family, value in policy.get("source_target_scopes", {}).items():
        if not isinstance(value, dict):
            raise ValueError(f"source target scope must be provenance-bearing: {family}")
        missing = {"scope_kind", "source_id", "release_id", "source_name"} - set(value)
        if missing:
            raise ValueError(f"source target scope lacks {sorted(missing)}: {family}")
        if value["scope_kind"] not in scope_kinds:
            raise ValueError(f"source target scope has unknown scope kind: {family}")


def classify_release_binding(
    *,
    forward_candidate_count: int,
    reverse_predecessor_count: int | None,
    pair_payload_consistent: bool | None,
    canonical_match_count: int | None,
) -> tuple[str, str]:
    """Reference implementation of the E2 target outcome state machine."""
    if forward_candidate_count == 0:
        return "missing", "no_official_dr3_neighbour"
    if forward_candidate_count > 1:
        return "ambiguous", "gaia_release_split_candidates"
    if reverse_predecessor_count is None or reverse_predecessor_count == 0:
        return "quarantined", "reverse_neighbourhood_incomplete"
    if reverse_predecessor_count > 1:
        return "ambiguous", "gaia_release_merge_candidates"
    if pair_payload_consistent is False:
        return "quarantined", "forward_reverse_pair_payload_conflict"
    if canonical_match_count is None or canonical_match_count == 0:
        return "excluded", "outside_current_canonical_backbone"
    if canonical_match_count > 1:
        return "quarantined", "canonical_gaia_dr3_collision"
    return "accepted", "unique_bidirectional_release_mapping_and_canonical_binding"


def source_path_index(report: dict[str, Any], report_path: Path) -> dict[tuple[str, str], Path]:
    paths = typed_paths(report_path)
    expected = {
        (source["source_id"], table["source_name"])
        for source in report.get("sources", [])
        for table in source.get("tables", [])
        if table.get("status") == "typed"
    }
    if expected != set(paths):
        missing = sorted(expected - set(paths))
        extra = sorted(set(paths) - expected)
        raise ValueError(f"typed report path accounting mismatch missing={missing} extra={extra}")
    return paths


def input_identity(
    policy_path: Path,
    typed_report: dict[str, Any],
    core_path: Path,
) -> dict[str, Any]:
    tables = []
    for source in typed_report.get("sources", []):
        for table in source.get("tables", []):
            if table.get("status") != "typed":
                continue
            key = (str(source["source_id"]), str(table["source_name"]))
            if key not in CONSUMED_TABLE_KEYS:
                continue
            tables.append(
                {
                    "source_id": source["source_id"],
                    "release_id": source["release_id"],
                    "snapshot_id": source.get("snapshot_id"),
                    "typed_snapshot_id": source.get("typed_snapshot_id"),
                    "source_name": table["source_name"],
                    "sha256": table["sha256"],
                    "row_count": table["row_count"],
                }
            )
    seen = {(row["source_id"], row["source_name"]) for row in tables}
    if seen != CONSUMED_TABLE_KEYS:
        missing = sorted(CONSUMED_TABLE_KEYS - seen)
        extra = sorted(seen - CONSUMED_TABLE_KEYS)
        raise ValueError(f"identity compiler input accounting mismatch missing={missing} extra={extra}")
    tables = sorted(tables, key=lambda row: (row["source_id"], row["source_name"]))
    return {
        "compiler_version": COMPILER_VERSION,
        "policy_sha256": file_sha256(policy_path),
        "typed_table_set_sha256": stable_hash(tables),
        "typed_tables": tables,
        "canonical_reference_build_id": core_path.parent.name,
        "canonical_reference_core_sha256": file_sha256(core_path),
    }


def configure_connection(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("set threads=1")
    con.execute("set preserve_insertion_order=true")
    con.execute("set enable_progress_bar=false")


def create_policy_tables(con: duckdb.DuckDBPyConnection, policy: dict[str, Any]) -> None:
    con.execute(
        """
        create temp table release_scope_mapping (
          namespace varchar,
          source_id varchar,
          release_id varchar
        )
        """
    )
    mappings = policy["canonical_identifier_release_scopes"]
    con.executemany(
        "insert into release_scope_mapping values (?, ?, ?)",
        [
            (namespace, value["source_id"], value["release_id"])
            for namespace, value in sorted(mappings.items())
        ],
    )
    lineage = policy["gaia_release_reconciliation"]
    con.execute(
        """
        create temp table gaia_release_lineage as
        select ?::varchar forward_source_id,
               ?::varchar forward_release_id,
               ?::varchar reverse_source_id,
               ?::varchar reverse_release_id,
               ?::varchar upstream_release_id,
               ?::varchar upstream_table
        """,
        [
            lineage["forward_registry_source_id"],
            lineage["forward_registry_release_id"],
            lineage["reverse_registry_source_id"],
            lineage["reverse_registry_release_id"],
            lineage["upstream_release_id"],
            lineage["table"],
        ],
    )
    con.execute(
        """
        create temp table source_scope_mapping (
          source_family varchar,
          scope_kind varchar,
          source_id varchar,
          release_id varchar,
          source_name varchar
        )
        """
    )
    con.executemany(
        "insert into source_scope_mapping values (?, ?, ?, ?, ?)",
        [
            (
                family,
                value["scope_kind"],
                value["source_id"],
                value["release_id"],
                value["source_name"],
            )
            for family, value in sorted(policy["source_target_scopes"].items())
        ],
    )


def create_canonical_tables(
    con: duckdb.DuckDBPyConnection,
    *,
    core_path: Path,
    core_build_id: str,
    fallback_source_id: str,
    fallback_release_id: str,
) -> None:
    con.execute(f"attach {sql_literal(core_path)} as core (read_only)")
    con.execute(
        f"""
        create table canonical_object_nodes as
        with objects as (
          select
            'object:' || stable_object_key as object_node_key,
            'system'::varchar as object_type,
            stable_object_key,
            cast(system_id as varchar) as canonical_row_id,
            stable_object_key as system_stable_object_key,
            system_name as display_name
          from core.systems where stable_object_key is not null
          union all
          select
            'object:' || st.stable_object_key,
            'star',
            st.stable_object_key,
            cast(st.star_id as varchar),
            sy.stable_object_key,
            st.star_name
          from core.stars st
          left join core.systems sy on sy.system_id = st.system_id
          where st.stable_object_key is not null
          union all
          select
            'object:' || p.stable_object_key,
            'planet',
            p.stable_object_key,
            cast(p.planet_id as varchar),
            sy.stable_object_key,
            p.planet_name
          from core.planets p
          left join core.systems sy on sy.system_id = p.system_id
          where p.stable_object_key is not null
        )
        select *, {sql_literal(core_build_id)}::varchar canonical_reference_build_id
        from objects order by object_node_key
        """
    )
    con.execute(
        """
        create temp table canonical_row_lookup as
        select object_type as target_type,
               try_cast(canonical_row_id as hugeint) as target_id,
               object_node_key,
               stable_object_key,
               system_stable_object_key
        from canonical_object_nodes
        """
    )
    con.execute(
        f"""
        create table canonical_identifier_bindings as
        select
          sha256(concat_ws('|', 'core_identifier', lower(trim(oi.namespace)),
                 trim(oi.id_value_norm), obj.object_node_key,
                 coalesce(oi.source_catalog, ''), coalesce(cast(oi.source_pk as varchar), ''))) as binding_key,
          'id:' || lower(trim(oi.namespace)) || ':' ||
            coalesce(map.release_id, {sql_literal(fallback_release_id)}) || ':' ||
            trim(oi.id_value_norm) as identifier_node_key,
          obj.object_node_key,
          obj.stable_object_key,
          obj.system_stable_object_key,
          lower(trim(oi.namespace)) as namespace,
          oi.id_value_raw,
          trim(oi.id_value_norm) as id_value_norm,
          coalesce(map.source_id, {sql_literal(fallback_source_id)}) as identifier_source_id,
          coalesce(map.release_id, {sql_literal(fallback_release_id)}) as identifier_release_id,
          oi.is_canonical,
          oi.resolution_method,
          cast(oi.resolution_confidence as double) as resolution_confidence,
          oi.source_catalog,
          oi.source_version,
          cast(oi.source_pk as varchar) as source_record_id,
          cast(oi.evidence_json as varchar) as evidence_json
        from core.object_identifiers oi
        join canonical_row_lookup obj
          on obj.target_type = lower(trim(oi.target_type))
         and obj.target_id = try_cast(oi.target_id as hugeint)
        left join release_scope_mapping map
          on map.namespace = lower(trim(oi.namespace))
        where nullif(trim(oi.id_value_norm), '') is not null
        order by binding_key
        """
    )


def create_release_tables(
    con: duckdb.DuckDBPyConnection,
    *,
    forward_targets: Path,
    forward_edges: Path,
    reverse_targets: Path,
    reverse_edges: Path,
    high_pm_threshold: float,
    lineage: dict[str, Any],
) -> None:
    con.execute(
        f"""
        create temp view forward_targets as
        select cast(dr2_source_id as varchar) dr2_source_id,
               source_families,
               try_cast(source_family_count as integer) source_family_count,
               try_cast(source_record_count as bigint) source_record_count
        from read_parquet({sql_literal(forward_targets)})
        """
    )
    con.execute(
        f"""
        create temp view forward_rows as
        select cast(dr2_source_id as varchar) dr2_source_id,
               cast(dr3_source_id as varchar) dr3_source_id,
               cast(angular_distance as varchar) angular_distance_raw,
               cast(magnitude_difference as varchar) magnitude_difference_raw,
               cast(proper_motion_propagation as varchar) proper_motion_propagation_raw,
               try_cast(angular_distance as double) angular_distance_arcsec,
               try_cast(magnitude_difference as double) magnitude_difference_mag,
               try_cast(proper_motion_propagation as boolean) proper_motion_propagation
        from read_parquet({sql_literal(forward_edges)})
        """
    )
    con.execute(
        f"""
        create temp view reverse_targets as
        select cast(dr3_source_id as varchar) dr3_source_id,
               try_cast(forward_dr2_source_count as integer) forward_dr2_source_count,
               try_cast(forward_pair_count as bigint) forward_pair_count
        from read_parquet({sql_literal(reverse_targets)})
        """
    )
    con.execute(
        f"""
        create temp view reverse_rows as
        select cast(dr2_source_id as varchar) dr2_source_id,
               cast(dr3_source_id as varchar) dr3_source_id,
               cast(angular_distance as varchar) angular_distance_raw,
               cast(magnitude_difference as varchar) magnitude_difference_raw,
               cast(proper_motion_propagation as varchar) proper_motion_propagation_raw,
               try_cast(angular_distance as double) angular_distance_arcsec,
               try_cast(magnitude_difference as double) magnitude_difference_mag,
               try_cast(proper_motion_propagation as boolean) proper_motion_propagation
        from read_parquet({sql_literal(reverse_edges)})
        """
    )
    con.execute(
        f"""
        create table release_crossmatch_edges as
        with f as (
          select dr2_source_id, dr3_source_id,
                 min(angular_distance_raw) angular_distance_raw,
                 min(magnitude_difference_raw) magnitude_difference_raw,
                 min(proper_motion_propagation_raw) proper_motion_propagation_raw,
                 min(angular_distance_arcsec) angular_distance_arcsec,
                 min(magnitude_difference_mag) magnitude_difference_mag,
                 bool_or(coalesce(proper_motion_propagation, false)) proper_motion_propagation,
                 count(*)::bigint forward_row_count
          from forward_rows group by dr2_source_id, dr3_source_id
        ),
        r as (
          select dr2_source_id, dr3_source_id,
                 min(angular_distance_raw) angular_distance_raw,
                 min(magnitude_difference_raw) magnitude_difference_raw,
                 min(proper_motion_propagation_raw) proper_motion_propagation_raw,
                 min(angular_distance_arcsec) angular_distance_arcsec,
                 min(magnitude_difference_mag) magnitude_difference_mag,
                 bool_or(coalesce(proper_motion_propagation, false)) proper_motion_propagation,
                 count(*)::bigint reverse_row_count
          from reverse_rows group by dr2_source_id, dr3_source_id
        ),
        paired as (
          select
            coalesce(f.dr2_source_id, r.dr2_source_id) dr2_source_id,
            coalesce(f.dr3_source_id, r.dr3_source_id) dr3_source_id,
            f.dr2_source_id is not null present_in_forward,
            r.dr2_source_id is not null present_in_reverse,
            coalesce(f.angular_distance_raw, r.angular_distance_raw) angular_distance_raw,
            coalesce(f.magnitude_difference_raw, r.magnitude_difference_raw) magnitude_difference_raw,
            coalesce(f.proper_motion_propagation_raw, r.proper_motion_propagation_raw) proper_motion_propagation_raw,
            coalesce(f.angular_distance_arcsec, r.angular_distance_arcsec) angular_distance_arcsec,
            coalesce(f.magnitude_difference_mag, r.magnitude_difference_mag) magnitude_difference_mag,
            coalesce(f.proper_motion_propagation, r.proper_motion_propagation, false) proper_motion_propagation,
            coalesce(f.forward_row_count, 0) forward_row_count,
            coalesce(r.reverse_row_count, 0) reverse_row_count,
            case
              when f.dr2_source_id is null or r.dr2_source_id is null then false
              when f.angular_distance_arcsec is distinct from r.angular_distance_arcsec then false
              when f.magnitude_difference_mag is distinct from r.magnitude_difference_mag then false
              when f.proper_motion_propagation is distinct from r.proper_motion_propagation then false
              else true
            end pair_payload_consistent
          from f full join r using (dr2_source_id, dr3_source_id)
        )
        select
          'edge:gaia-dr2-dr3:' || dr2_source_id || ':' || dr3_source_id edge_key,
          'id:gaia_dr2:dr2:' || dr2_source_id left_identifier_node_key,
          'id:gaia_dr3:dr3:' || dr3_source_id right_identifier_node_key,
          dr2_source_id,
          dr3_source_id,
          'official_release_neighbourhood_candidate'::varchar relation_type,
          present_in_forward,
          present_in_reverse,
          pair_payload_consistent,
          angular_distance_raw,
          magnitude_difference_raw,
          proper_motion_propagation_raw,
          angular_distance_arcsec,
          magnitude_difference_mag,
          proper_motion_propagation,
          forward_row_count,
          reverse_row_count,
          {sql_literal(lineage['forward_registry_source_id'])}::varchar source_id,
          {sql_literal(lineage['forward_registry_release_id'])}::varchar source_release_id,
          {sql_literal(lineage['reverse_registry_source_id'])}::varchar reverse_source_id,
          {sql_literal(lineage['reverse_registry_release_id'])}::varchar reverse_source_release_id,
          {sql_literal(lineage['upstream_release_id'])}::varchar upstream_release_id,
          {sql_literal(lineage['table'])}::varchar source_table
        from paired order by edge_key
        """
    )
    con.execute(
        f"""
        create table dr2_release_outcomes as
        with forward_stats as (
          select dr2_source_id,
                 count(distinct dr3_source_id)::integer forward_candidate_count,
                 min(dr3_source_id) filter (
                   where dr3_source_id is not null
                 ) sole_dr3_source_id,
                 bool_or(coalesce(proper_motion_propagation, false)) proper_motion_propagation_applied,
                 to_json(list(distinct dr3_source_id order by dr3_source_id)) candidate_dr3_ids_json
          from forward_rows group by dr2_source_id
        ),
        reverse_stats as (
          select dr3_source_id,
                 count(distinct dr2_source_id)::integer reverse_predecessor_count,
                 to_json(list(distinct dr2_source_id order by dr2_source_id)) predecessor_dr2_ids_json
          from reverse_rows group by dr3_source_id
        ),
        pair_stats as (
          select dr2_source_id, dr3_source_id,
                 bool_and(pair_payload_consistent) pair_payload_consistent
          from release_crossmatch_edges
          group by dr2_source_id, dr3_source_id
        ),
        core_match as (
          select cast(gaia_id as varchar) dr3_source_id,
                 count(distinct stable_object_key)::integer canonical_match_count,
                 min(stable_object_key) canonical_stable_object_key,
                 min(system_id)::varchar canonical_system_row_id,
                 max(sqrt(coalesce(pm_ra_mas_yr, 0) * coalesce(pm_ra_mas_yr, 0) +
                          coalesce(pm_dec_mas_yr, 0) * coalesce(pm_dec_mas_yr, 0))) total_proper_motion_mas_yr,
                 to_json(list(distinct stable_object_key order by stable_object_key)) canonical_keys_json
          from core.stars where gaia_id is not null
          group by gaia_id
        ),
        staged as (
          select
            t.dr2_source_id,
            t.source_families,
            t.source_family_count,
            t.source_record_count,
            coalesce(f.forward_candidate_count, 0) forward_candidate_count,
            case when coalesce(f.forward_candidate_count, 0) = 1 then f.sole_dr3_source_id end accepted_dr3_source_id,
            coalesce(f.candidate_dr3_ids_json, '[]') candidate_dr3_ids_json,
            r.reverse_predecessor_count,
            coalesce(r.predecessor_dr2_ids_json, '[]') predecessor_dr2_ids_json,
            p.pair_payload_consistent,
            coalesce(c.canonical_match_count, 0) canonical_match_count,
            c.canonical_stable_object_key,
            sy.stable_object_key canonical_system_stable_object_key,
            coalesce(c.canonical_keys_json, '[]') canonical_keys_json,
            coalesce(f.proper_motion_propagation_applied, false) proper_motion_propagation_applied,
            c.total_proper_motion_mas_yr,
            coalesce(c.total_proper_motion_mas_yr >= {high_pm_threshold}, false) high_proper_motion_guard,
            coalesce(f.proper_motion_propagation_applied, false) or
              coalesce(c.total_proper_motion_mas_yr >= {high_pm_threshold}, false) proper_motion_safeguard_applied,
            coalesce(sy.star_count, 0) > 1 component_scope_guard
          from forward_targets t
          left join forward_stats f using (dr2_source_id)
          left join reverse_stats r on r.dr3_source_id = f.sole_dr3_source_id
          left join pair_stats p
            on p.dr2_source_id = t.dr2_source_id
           and p.dr3_source_id = f.sole_dr3_source_id
          left join core_match c on c.dr3_source_id = f.sole_dr3_source_id
          left join core.systems sy
            on sy.system_id = try_cast(c.canonical_system_row_id as hugeint)
        ),
        classified as (
          select *,
            case
              when forward_candidate_count = 0 then 'missing'
              when forward_candidate_count > 1 then 'ambiguous'
              when reverse_predecessor_count is null or reverse_predecessor_count = 0 then 'quarantined'
              when reverse_predecessor_count > 1 then 'ambiguous'
              when pair_payload_consistent = false then 'quarantined'
              when canonical_match_count = 0 then 'excluded'
              when canonical_match_count > 1 then 'quarantined'
              else 'accepted'
            end outcome,
            case
              when forward_candidate_count = 0 then 'no_official_dr3_neighbour'
              when forward_candidate_count > 1 then 'gaia_release_split_candidates'
              when reverse_predecessor_count is null or reverse_predecessor_count = 0 then 'reverse_neighbourhood_incomplete'
              when reverse_predecessor_count > 1 then 'gaia_release_merge_candidates'
              when pair_payload_consistent = false then 'forward_reverse_pair_payload_conflict'
              when canonical_match_count = 0 then 'outside_current_canonical_backbone'
              when canonical_match_count > 1 then 'canonical_gaia_dr3_collision'
              else 'unique_bidirectional_release_mapping_and_canonical_binding'
            end reason
          from staged
        ),
        accepted_star_counts as (
          select canonical_stable_object_key,
                 count(*)::integer accepted_dr2_count
          from classified
          where outcome = 'accepted'
          group by canonical_stable_object_key
        ),
        accepted_system_counts as (
          select canonical_system_stable_object_key,
                 count(*)::integer accepted_dr2_count
          from classified
          where outcome = 'accepted'
          group by canonical_system_stable_object_key
        )
        select
          dr2_source_id,
          'id:gaia_dr2:dr2:' || dr2_source_id dr2_identifier_node_key,
          accepted_dr3_source_id,
          case when accepted_dr3_source_id is not null
            then 'id:gaia_dr3:dr3:' || accepted_dr3_source_id end dr3_identifier_node_key,
          source_families,
          source_family_count,
          source_record_count,
          {sql_literal(lineage['forward_registry_source_id'])}::varchar crossmatch_source_id,
          {sql_literal(lineage['forward_registry_release_id'])}::varchar crossmatch_source_release_id,
          {sql_literal(lineage['reverse_registry_source_id'])}::varchar reverse_crossmatch_source_id,
          {sql_literal(lineage['reverse_registry_release_id'])}::varchar reverse_crossmatch_source_release_id,
          {sql_literal(lineage['upstream_release_id'])}::varchar upstream_release_id,
          forward_candidate_count,
          reverse_predecessor_count,
          outcome,
          reason,
          candidate_dr3_ids_json,
          predecessor_dr2_ids_json,
          pair_payload_consistent,
          canonical_match_count,
          case when outcome = 'accepted' then canonical_stable_object_key end canonical_stable_object_key,
          case when outcome = 'accepted' then canonical_system_stable_object_key end canonical_system_stable_object_key,
          canonical_keys_json,
          proper_motion_propagation_applied,
          total_proper_motion_mas_yr,
          high_proper_motion_guard,
          proper_motion_safeguard_applied,
          component_scope_guard,
          coalesce(star_counts.accepted_dr2_count, 0) canonical_star_accepted_dr2_count,
          coalesce(system_counts.accepted_dr2_count, 0) canonical_system_accepted_dr2_count,
          outcome <> 'accepted' or coalesce(star_counts.accepted_dr2_count, 0) = 1 duplicate_system_guard,
          json_object(
            'official_table', 'gaiadr3.dr2_neighbourhood',
            'forward_registry_release_id', {sql_literal(lineage['forward_registry_release_id'])},
            'reverse_registry_release_id', {sql_literal(lineage['reverse_registry_release_id'])},
            'upstream_release_id', {sql_literal(lineage['upstream_release_id'])},
            'forward_candidate_count', forward_candidate_count,
            'reverse_predecessor_count', reverse_predecessor_count,
            'canonical_match_count', canonical_match_count,
            'candidate_dr3_ids', candidate_dr3_ids_json,
            'predecessor_dr2_ids', predecessor_dr2_ids_json
          )::varchar evidence_json
        from classified
        left join accepted_star_counts star_counts using (canonical_stable_object_key)
        left join accepted_system_counts system_counts using (canonical_system_stable_object_key)
        order by try_cast(dr2_source_id as ubigint)
        """
    )


def create_source_bindings(
    con: duckdb.DuckDBPyConnection,
    paths: dict[tuple[str, str], Path],
    *,
    core_build_id: str,
) -> None:
    family_queries = target_source_queries(paths)
    unions = [
        "select "
        f"{sql_literal(family)}::varchar source_family, cast(dr2_source_id as varchar) dr2_source_id "
        f"from ({query}) where dr2_source_id is not null and dr2_source_id > 0"
        for family, query in family_queries
    ]
    con.execute("create temp table source_target_attempts as " + " union all ".join(unions))
    con.execute(
        f"""
        create table source_record_bindings as
        with grouped as (
          select source_family, dr2_source_id, count(*)::bigint source_record_count
          from source_target_attempts group by source_family, dr2_source_id
        )
        select
          sha256(concat_ws('|', 'source_target_group', g.source_family, g.dr2_source_id)) source_record_binding_key,
          g.source_family,
          s.source_id,
          s.release_id,
          s.source_name,
          g.dr2_source_id,
          'record-group:' || g.source_family || ':gaia_dr2:' || g.dr2_source_id source_record_group_key,
          g.source_record_count,
          s.scope_kind,
          'id:gaia_dr2:dr2:' || g.dr2_source_id target_identifier_node_key,
          o.outcome,
          o.reason,
          case when o.outcome = 'accepted' then 'object:' || o.canonical_stable_object_key end canonical_object_node_key,
          case when o.outcome = 'accepted' then o.canonical_stable_object_key end canonical_stable_object_key,
          case when o.outcome = 'accepted' then o.canonical_system_stable_object_key end canonical_system_stable_object_key,
          json_object(
            'dr2_source_id', g.dr2_source_id,
            'accepted_dr3_source_id', o.accepted_dr3_source_id,
            'outcome', o.outcome,
            'reason', o.reason,
            'source_id', s.source_id,
            'release_id', s.release_id,
            'source_name', s.source_name,
            'source_record_count', g.source_record_count
          )::varchar evidence_json
        from grouped g
        join source_scope_mapping s using (source_family)
        join dr2_release_outcomes o using (dr2_source_id)
        order by source_record_binding_key
        """
    )
    msc_comp = required_path(paths, ("multiplicity.msc", "msc_comp"))
    msc_sys = required_path(paths, ("multiplicity.msc", "msc_sys"))
    wds_rows = required_path(paths, ("multiplicity.wds", "wdsweb_summ2"))
    con.execute(
        f"""
        create temp view msc_component_scope_rows as
        select source_line_number, nullif(upper(trim("WDS")), '') wds_id,
               nullif(trim("Comp"), '') component_label,
               nullif(trim("HIP"), '') hip_id,
               nullif(trim("HD"), '') hd_id,
               nullif(trim("Id"), '') source_names,
               raw_row
        from read_parquet({sql_literal(msc_comp)})
        """
    )
    con.execute(
        f"""
        create temp view msc_relation_scope_rows as
        select source_line_number, nullif(upper(trim("WDS")), '') wds_id,
               nullif(trim("Primary"), '') primary_component,
               nullif(trim("Secondary"), '') secondary_component,
               nullif(trim("Parent"), '') parent_component,
               nullif(trim("Type"), '') relation_type,
               nullif(trim("Comment"), '') source_comment,
               raw_row
        from read_parquet({sql_literal(msc_sys)})
        """
    )
    con.execute(
        f"""
        create temp view wds_component_scope_rows as
        select source_line_number,
               nullif(upper(trim("2000_coordinates")), '') wds_id,
               nullif(trim(discoverer_number), '') discoverer_number,
               nullif(trim(components), '') components,
               raw_row
        from read_parquet({sql_literal(wds_rows)})
        """
    )
    con.execute(
        """
        create temp table source_scope_identifier_nodes as
        select distinct
          'id:wds:newmsc_20260619:' || wds_id identifier_node_key,
          'wds'::varchar namespace,
          'multiplicity.msc'::varchar source_id,
          'newmsc_20260619'::varchar release_id,
          wds_id id_value_raw,
          wds_id id_value_norm,
          'source_relation_scope'::varchar node_origin,
          0 origin_priority
        from (
          select wds_id from msc_component_scope_rows
          union all
          select wds_id from msc_relation_scope_rows
        ) where wds_id is not null
        union all
        select distinct
          'id:wds:rolling_snapshot:' || wds_id,
          'wds',
          'multiplicity.wds',
          'rolling_snapshot',
          wds_id,
          wds_id,
          'source_relation_scope',
          0
        from wds_component_scope_rows where wds_id is not null
        """
    )
    con.execute(
        f"""
        create table scope_claims as
        with target_claims as (
          select
            sha256(concat_ws('|', 'scope', source_record_binding_key, scope_kind)) scope_claim_key,
            source_id,
            release_id,
            source_record_group_key source_record_key,
            scope_kind,
            case when scope_kind = 'observation_target'
              then 'source_record_targets_identifier'
              else 'source_record_describes_identifier'
            end predicate,
            target_identifier_node_key,
            canonical_object_node_key,
            outcome claim_outcome,
            evidence_json
          from source_record_bindings
        ),
        alias_claims as (
          select
            sha256(concat_ws('|', 'alias_scope', coalesce(a.source_catalog, ''), cast(a.alias_id as varchar))) scope_claim_key,
            coalesce(a.source_catalog, 'spacegate.core.alias_projection') source_id,
            coalesce(a.source_version, 'canonical_reference_build') release_id,
            'alias-row:' || cast(a.alias_id as varchar) source_record_key,
            'alias_or_public_name'::varchar scope_kind,
            'alias_labels_object'::varchar predicate,
            cast(null as varchar) target_identifier_node_key,
            obj.object_node_key canonical_object_node_key,
            case when obj.object_node_key is null then 'quarantined' else 'accepted' end claim_outcome,
            json_object(
              'alias_raw', a.alias_raw,
              'alias_norm', a.alias_norm,
              'alias_kind', a.alias_kind,
              'target_type', a.target_type,
              'target_id', cast(a.target_id as varchar)
            )::varchar evidence_json
          from core.aliases a
          left join canonical_row_lookup obj
            on obj.target_type = lower(trim(a.target_type))
           and obj.target_id = try_cast(a.target_id as hugeint)
        ),
        canonical_containment_claims as (
          select
            sha256('canonical_containment_star|' || st.stable_object_key) scope_claim_key,
            'spacegate.core.canonical_reference'::varchar source_id,
            {sql_literal(core_build_id)}::varchar release_id,
            'object:' || sy.stable_object_key source_record_key,
            'system_containment'::varchar scope_kind,
            'system_contains_star'::varchar predicate,
            cast(null as varchar) target_identifier_node_key,
            'object:' || st.stable_object_key canonical_object_node_key,
            'accepted'::varchar claim_outcome,
            json_object(
              'reference_role', 'stability_reference_not_new_authority',
              'parent_object_node_key', 'object:' || sy.stable_object_key,
              'child_object_node_key', 'object:' || st.stable_object_key,
              'component', st.component
            )::varchar evidence_json
          from core.stars st
          join core.systems sy on sy.system_id = st.system_id
          where st.stable_object_key is not null and sy.stable_object_key is not null
          union all
          select
            sha256('canonical_containment_planet|' || p.stable_object_key),
            'spacegate.core.canonical_reference',
            {sql_literal(core_build_id)},
            'object:' || sy.stable_object_key,
            'system_containment',
            'system_contains_planet',
            cast(null as varchar),
            'object:' || p.stable_object_key,
            'accepted',
            json_object(
              'reference_role', 'stability_reference_not_new_authority',
              'parent_object_node_key', 'object:' || sy.stable_object_key,
              'child_object_node_key', 'object:' || p.stable_object_key,
              'host_star_id', cast(p.star_id as varchar)
            )::varchar
          from core.planets p
          join core.systems sy on sy.system_id = p.system_id
          where p.stable_object_key is not null and sy.stable_object_key is not null
        ),
        canonical_component_claims as (
          select
            sha256('canonical_component_scope|' || st.stable_object_key) scope_claim_key,
            'spacegate.core.canonical_reference'::varchar source_id,
            {sql_literal(core_build_id)}::varchar release_id,
            'object:' || st.stable_object_key source_record_key,
            'component_or_subsystem'::varchar scope_kind,
            'component_label_scopes_object'::varchar predicate,
            cast(null as varchar) target_identifier_node_key,
            'object:' || st.stable_object_key canonical_object_node_key,
            'accepted'::varchar claim_outcome,
            json_object(
              'reference_role', 'stability_reference_not_new_authority',
              'component', st.component,
              'parent_object_node_key', 'object:' || sy.stable_object_key
            )::varchar evidence_json
          from core.stars st
          join core.systems sy on sy.system_id = st.system_id
          where st.stable_object_key is not null
            and nullif(trim(st.component), '') is not null
        ),
        msc_component_claims as (
          select
            sha256('msc_component_scope|' || cast(source_line_number as varchar)) scope_claim_key,
            'multiplicity.msc'::varchar source_id,
            'newmsc_20260619'::varchar release_id,
            'msc_comp:' || cast(source_line_number as varchar) source_record_key,
            'component_or_subsystem'::varchar scope_kind,
            'source_describes_component'::varchar predicate,
            case when wds_id is not null then 'id:wds:newmsc_20260619:' || wds_id end target_identifier_node_key,
            cast(null as varchar) canonical_object_node_key,
            'candidate'::varchar claim_outcome,
            json_object(
              'wds_id', wds_id,
              'component', component_label,
              'hip_id', hip_id,
              'hd_id', hd_id,
              'source_names', source_names,
              'raw_row', raw_row
            )::varchar evidence_json
          from msc_component_scope_rows
        ),
        msc_relation_claims as (
          select
            sha256('msc_relation_scope|' || cast(source_line_number as varchar)) scope_claim_key,
            'multiplicity.msc'::varchar source_id,
            'newmsc_20260619'::varchar release_id,
            'msc_sys:' || cast(source_line_number as varchar) source_record_key,
            'system_containment'::varchar scope_kind,
            'source_asserts_hierarchical_relation'::varchar predicate,
            case when wds_id is not null then 'id:wds:newmsc_20260619:' || wds_id end target_identifier_node_key,
            cast(null as varchar) canonical_object_node_key,
            'candidate'::varchar claim_outcome,
            json_object(
              'wds_id', wds_id,
              'primary_component', primary_component,
              'secondary_component', secondary_component,
              'parent_component', parent_component,
              'relation_type', relation_type,
              'source_comment', source_comment,
              'raw_row', raw_row
            )::varchar evidence_json
          from msc_relation_scope_rows
        ),
        wds_component_claims as (
          select
            sha256('wds_component_scope|' || cast(source_line_number as varchar)) scope_claim_key,
            'multiplicity.wds'::varchar source_id,
            'rolling_snapshot'::varchar release_id,
            'wdsweb_summ2:' || cast(source_line_number as varchar) source_record_key,
            'component_or_subsystem'::varchar scope_kind,
            'source_observes_component_pair'::varchar predicate,
            case when wds_id is not null then 'id:wds:rolling_snapshot:' || wds_id end target_identifier_node_key,
            cast(null as varchar) canonical_object_node_key,
            'candidate'::varchar claim_outcome,
            json_object(
              'wds_id', wds_id,
              'discoverer_number', discoverer_number,
              'components', components,
              'raw_row', raw_row
            )::varchar evidence_json
          from wds_component_scope_rows
        )
        select * from target_claims
        union all by name
        select * from alias_claims
        union all by name
        select * from canonical_containment_claims
        union all by name
        select * from canonical_component_claims
        union all by name
        select * from msc_component_claims
        union all by name
        select * from msc_relation_claims
        union all by name
        select * from wds_component_claims
        order by scope_claim_key
        """
    )


def create_identifier_and_diagnostic_tables(
    con: duckdb.DuckDBPyConnection,
    *,
    core_build_id: str,
) -> None:
    con.execute(
        f"""
        create table identifier_nodes as
        with candidates as (
          select identifier_node_key, namespace, identifier_source_id source_id,
                 identifier_release_id release_id, id_value_raw, id_value_norm,
                 'canonical_object_identifier'::varchar node_origin,
                 1 origin_priority
          from canonical_identifier_bindings
          union all
          select distinct
            'id:gaia_dr2:dr2:' || dr2_source_id,
            'gaia_dr2',
            'gaia.dr2.gaia_source',
            'dr2',
            dr2_source_id,
            dr2_source_id,
            'targeted_release_reconciliation',
            0
          from dr2_release_outcomes
          union all
          select distinct
            'id:gaia_dr3:dr3:' || dr3_source_id,
            'gaia_dr3',
            'gaia.dr3.gaia_source',
            'dr3',
            dr3_source_id,
            dr3_source_id,
            'official_release_neighbourhood',
            0
          from release_crossmatch_edges
          union all
          select identifier_node_key, namespace, source_id, release_id,
                 id_value_raw, id_value_norm, node_origin, origin_priority
          from source_scope_identifier_nodes
        ),
        ranked as (
          select *, row_number() over (
            partition by identifier_node_key
            order by origin_priority, source_id, release_id, id_value_raw
          ) rank
          from candidates
        )
        select identifier_node_key, namespace, source_id, release_id,
               id_value_raw, id_value_norm, node_origin,
               {sql_literal(core_build_id)}::varchar canonical_reference_build_id
        from ranked where rank = 1
        order by identifier_node_key
        """
    )
    con.execute(
        """
        create table identifier_collision_diagnostics as
        select
          identifier_node_key,
          namespace,
          count(distinct object_node_key)::bigint canonical_object_count,
          count(*)::bigint binding_row_count,
          to_json(list(distinct stable_object_key order by stable_object_key)) canonical_keys_json,
          case
            when namespace = 'wds' then 'component_scoped_shared_identifier_review'
            when namespace = 'gaia_dr3' then 'quarantine'
            else 'namespace_collision_review'
          end disposition
        from canonical_identifier_bindings
        group by identifier_node_key, namespace
        having count(distinct object_node_key) > 1
        order by identifier_node_key
        """
    )
    con.execute(
        """
        create table identity_quarantine as
        with release_quarantine as (
          select
            sha256('gaia_dr2_outcome|' || dr2_source_id) quarantine_key,
            'gaia_release_reconciliation'::varchar quarantine_kind,
            outcome,
            reason,
            dr2_identifier_node_key subject_node_key,
            candidate_dr3_ids_json candidate_nodes_json,
            evidence_json
          from dr2_release_outcomes
          where outcome in ('ambiguous', 'quarantined')
        ),
        core_quarantine as (
          select
            sha256('canonical_identifier_collision|' || identifier_node_key) quarantine_key,
            'canonical_identifier_collision'::varchar quarantine_kind,
            'quarantined'::varchar outcome,
            'canonical_gaia_dr3_collision'::varchar reason,
            identifier_node_key subject_node_key,
            canonical_keys_json candidate_nodes_json,
            json_object(
              'namespace', namespace,
              'canonical_object_count', canonical_object_count,
              'binding_row_count', binding_row_count,
              'canonical_keys', canonical_keys_json
            )::varchar evidence_json
          from identifier_collision_diagnostics
          where namespace = 'gaia_dr3'
        )
        select * from release_quarantine
        union all by name
        select * from core_quarantine
        order by quarantine_key
        """
    )


def verification_summary(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    outcome_rows = con.execute(
        """
        select outcome, count(*)::bigint row_count
        from dr2_release_outcomes group by outcome order by outcome
        """
    ).fetchall()
    checks = {
        "target_count": int(con.execute("select count(*) from forward_targets").fetchone()[0]),
        "distinct_target_count": int(
            con.execute("select count(distinct dr2_source_id) from forward_targets").fetchone()[0]
        ),
        "reverse_target_count": int(
            con.execute("select count(*) from reverse_targets").fetchone()[0]
        ),
        "distinct_forward_dr3_candidate_count": int(
            con.execute("select count(distinct dr3_source_id) from forward_rows").fetchone()[0]
        ),
        "forward_candidate_missing_reverse_target_count": int(
            con.execute(
                """
                select count(*) from (
                  select distinct dr3_source_id from forward_rows
                  except
                  select dr3_source_id from reverse_targets
                )
                """
            ).fetchone()[0]
        ),
        "reverse_target_without_forward_candidate_count": int(
            con.execute(
                """
                select count(*) from (
                  select dr3_source_id from reverse_targets
                  except
                  select distinct dr3_source_id from forward_rows
                )
                """
            ).fetchone()[0]
        ),
        "reverse_row_outside_reverse_target_count": int(
            con.execute(
                """
                select count(*) from reverse_rows r
                where not exists (
                  select 1 from reverse_targets t where t.dr3_source_id=r.dr3_source_id
                )
                """
            ).fetchone()[0]
        ),
        "reverse_target_metadata_mismatch_count": int(
            con.execute(
                """
                with actual as (
                  select dr3_source_id,
                         count(distinct dr2_source_id)::integer forward_dr2_source_count,
                         count(*)::bigint forward_pair_count
                  from forward_rows group by dr3_source_id
                )
                select count(*)
                from reverse_targets t full join actual a using (dr3_source_id)
                where t.dr3_source_id is null or a.dr3_source_id is null
                   or t.forward_dr2_source_count is distinct from a.forward_dr2_source_count
                   or t.forward_pair_count is distinct from a.forward_pair_count
                """
            ).fetchone()[0]
        ),
        "outcome_count": int(con.execute("select count(*) from dr2_release_outcomes").fetchone()[0]),
        "outcome_duplicate_count": int(
            con.execute(
                """
                select count(*) from (
                  select dr2_source_id from dr2_release_outcomes group by dr2_source_id having count(*) > 1
                )
                """
            ).fetchone()[0]
        ),
        "target_without_source_binding_count": int(
            con.execute(
                """
                select count(*) from dr2_release_outcomes o
                where not exists (
                  select 1 from source_record_bindings b
                  where b.target_identifier_node_key = o.dr2_identifier_node_key
                )
                """
            ).fetchone()[0]
        ),
        "target_source_aggregate_mismatch_count": int(
            con.execute(
                """
                with actual as (
                  select dr2_source_id,
                         count(*)::bigint source_record_count,
                         count(distinct source_family)::integer source_family_count,
                         string_agg(distinct source_family, ',' order by source_family) source_families
                  from source_target_attempts group by dr2_source_id
                )
                select count(*)
                from forward_targets t full join actual a using (dr2_source_id)
                where t.dr2_source_id is null or a.dr2_source_id is null
                   or t.source_record_count is distinct from a.source_record_count
                   or t.source_family_count is distinct from a.source_family_count
                   or t.source_families is distinct from a.source_families
                """
            ).fetchone()[0]
        ),
        "source_binding_without_target_count": int(
            con.execute(
                """
                select count(*) from source_record_bindings b
                where not exists (
                  select 1 from dr2_release_outcomes o
                  where o.dr2_identifier_node_key = b.target_identifier_node_key
                )
                """
            ).fetchone()[0]
        ),
        "source_binding_provenance_mismatch_count": int(
            con.execute(
                """
                select count(*) from source_record_bindings b
                left join source_scope_mapping s using (source_family)
                where s.source_family is null
                   or b.scope_kind is distinct from s.scope_kind
                   or b.source_id is distinct from s.source_id
                   or b.release_id is distinct from s.release_id
                   or b.source_name is distinct from s.source_name
                """
            ).fetchone()[0]
        ),
        "source_family_binding_mismatch_count": int(
            con.execute(
                """
                with expected as (
                  select source_family, dr2_source_id,
                         count(*)::bigint source_record_count
                  from source_target_attempts
                  group by source_family, dr2_source_id
                )
                select count(*)
                from expected e full join source_record_bindings b
                  using (source_family, dr2_source_id)
                where e.source_family is null or b.source_family is null
                   or e.source_record_count is distinct from b.source_record_count
                """
            ).fetchone()[0]
        ),
        "accepted_without_canonical_key_count": int(
            con.execute(
                "select count(*) from dr2_release_outcomes where outcome='accepted' and canonical_stable_object_key is null"
            ).fetchone()[0]
        ),
        "nonaccepted_with_canonical_key_count": int(
            con.execute(
                "select count(*) from dr2_release_outcomes where outcome<>'accepted' and canonical_stable_object_key is not null"
            ).fetchone()[0]
        ),
        "forward_pair_missing_reverse_count": int(
            con.execute(
                "select count(*) from release_crossmatch_edges where present_in_forward and not present_in_reverse"
            ).fetchone()[0]
        ),
        "pair_payload_conflict_count": int(
            con.execute(
                "select count(*) from release_crossmatch_edges where present_in_forward and present_in_reverse and not pair_payload_consistent"
            ).fetchone()[0]
        ),
        "release_edge_lineage_mismatch_count": int(
            con.execute(
                """
                select count(*) from release_crossmatch_edges e
                cross join gaia_release_lineage l
                where e.source_id is distinct from l.forward_source_id
                   or e.source_release_id is distinct from l.forward_release_id
                   or e.reverse_source_id is distinct from l.reverse_source_id
                   or e.reverse_source_release_id is distinct from l.reverse_release_id
                   or e.upstream_release_id is distinct from l.upstream_release_id
                   or e.source_table is distinct from l.upstream_table
                """
            ).fetchone()[0]
        ),
        "release_outcome_lineage_mismatch_count": int(
            con.execute(
                """
                select count(*) from dr2_release_outcomes o
                cross join gaia_release_lineage l
                where o.crossmatch_source_id is distinct from l.forward_source_id
                   or o.crossmatch_source_release_id is distinct from l.forward_release_id
                   or o.reverse_crossmatch_source_id is distinct from l.reverse_source_id
                   or o.reverse_crossmatch_source_release_id is distinct from l.reverse_release_id
                   or o.upstream_release_id is distinct from l.upstream_release_id
                """
            ).fetchone()[0]
        ),
        "accepted_high_proper_motion_count": int(
            con.execute(
                "select count(*) from dr2_release_outcomes where outcome='accepted' and high_proper_motion_guard"
            ).fetchone()[0]
        ),
        "accepted_proper_motion_propagation_count": int(
            con.execute(
                "select count(*) from dr2_release_outcomes where outcome='accepted' and proper_motion_propagation_applied"
            ).fetchone()[0]
        ),
        "accepted_proper_motion_safeguard_count": int(
            con.execute(
                "select count(*) from dr2_release_outcomes where outcome='accepted' and proper_motion_safeguard_applied"
            ).fetchone()[0]
        ),
        "accepted_component_scope_count": int(
            con.execute(
                "select count(*) from dr2_release_outcomes where outcome='accepted' and component_scope_guard"
            ).fetchone()[0]
        ),
        "accepted_shared_system_component_count": int(
            con.execute(
                """
                select count(*) from dr2_release_outcomes
                where outcome='accepted' and canonical_system_accepted_dr2_count > 1
                """
            ).fetchone()[0]
        ),
        "duplicate_system_guard_failure_count": int(
            con.execute(
                "select count(*) from dr2_release_outcomes where not duplicate_system_guard"
            ).fetchone()[0]
        ),
        "canonical_identifier_collision_count": int(
            con.execute("select count(*) from identifier_collision_diagnostics").fetchone()[0]
        ),
        "gaia_dr3_collision_count": int(
            con.execute(
                "select count(*) from identifier_collision_diagnostics where namespace='gaia_dr3'"
            ).fetchone()[0]
        ),
        "scope_kind_count": int(
            con.execute("select count(distinct scope_kind) from scope_claims").fetchone()[0]
        ),
        "source_relation_claim_count": int(
            con.execute(
                "select count(*) from scope_claims where source_id in ('multiplicity.msc', 'multiplicity.wds')"
            ).fetchone()[0]
        ),
        "source_relation_canonical_promotion_count": int(
            con.execute(
                """
                select count(*) from scope_claims
                where source_id in ('multiplicity.msc', 'multiplicity.wds')
                  and canonical_object_node_key is not null
                """
            ).fetchone()[0]
        ),
        "canonical_containment_claim_count": int(
            con.execute(
                """
                select count(*) from scope_claims
                where source_id='spacegate.core.canonical_reference'
                  and scope_kind='system_containment'
                """
            ).fetchone()[0]
        ),
        "scope_target_identifier_missing_count": int(
            con.execute(
                """
                select count(*) from scope_claims c
                where c.target_identifier_node_key is not null
                  and not exists (
                    select 1 from identifier_nodes i
                    where i.identifier_node_key=c.target_identifier_node_key
                  )
                """
            ).fetchone()[0]
        ),
        "scope_canonical_object_missing_count": int(
            con.execute(
                """
                select count(*) from scope_claims c
                where c.canonical_object_node_key is not null
                  and not exists (
                    select 1 from canonical_object_nodes o
                    where o.object_node_key=c.canonical_object_node_key
                  )
                """
            ).fetchone()[0]
        ),
        "canonical_object_node_duplicate_count": int(
            con.execute(
                "select count(*) from (select object_node_key from canonical_object_nodes group by 1 having count(*) > 1)"
            ).fetchone()[0]
        ),
        "identifier_node_duplicate_count": int(
            con.execute(
                "select count(*) from (select identifier_node_key from identifier_nodes group by 1 having count(*) > 1)"
            ).fetchone()[0]
        ),
        "canonical_identifier_binding_duplicate_count": int(
            con.execute(
                "select count(*) from (select binding_key from canonical_identifier_bindings group by 1 having count(*) > 1)"
            ).fetchone()[0]
        ),
        "release_edge_duplicate_count": int(
            con.execute(
                "select count(*) from (select edge_key from release_crossmatch_edges group by 1 having count(*) > 1)"
            ).fetchone()[0]
        ),
        "source_record_binding_duplicate_count": int(
            con.execute(
                "select count(*) from (select source_record_binding_key from source_record_bindings group by 1 having count(*) > 1)"
            ).fetchone()[0]
        ),
        "scope_claim_duplicate_count": int(
            con.execute(
                "select count(*) from (select scope_claim_key from scope_claims group by 1 having count(*) > 1)"
            ).fetchone()[0]
        ),
        "identity_quarantine_duplicate_count": int(
            con.execute(
                "select count(*) from (select quarantine_key from identity_quarantine group by 1 having count(*) > 1)"
            ).fetchone()[0]
        ),
        "identity_quarantine_count": int(
            con.execute("select count(*) from identity_quarantine").fetchone()[0]
        ),
        "expected_identity_quarantine_count": int(
            con.execute(
                """
                select
                  (select count(*) from dr2_release_outcomes where outcome in ('ambiguous', 'quarantined')) +
                  (select count(*) from identifier_collision_diagnostics where namespace='gaia_dr3')
                """
            ).fetchone()[0]
        ),
    }
    errors = []
    for key in (
        "outcome_duplicate_count",
        "target_without_source_binding_count",
        "source_binding_without_target_count",
        "source_binding_provenance_mismatch_count",
        "source_family_binding_mismatch_count",
        "accepted_without_canonical_key_count",
        "nonaccepted_with_canonical_key_count",
        "forward_pair_missing_reverse_count",
        "pair_payload_conflict_count",
        "release_edge_lineage_mismatch_count",
        "release_outcome_lineage_mismatch_count",
        "source_relation_canonical_promotion_count",
        "scope_target_identifier_missing_count",
        "scope_canonical_object_missing_count",
        "forward_candidate_missing_reverse_target_count",
        "reverse_target_without_forward_candidate_count",
        "reverse_row_outside_reverse_target_count",
        "reverse_target_metadata_mismatch_count",
        "target_source_aggregate_mismatch_count",
        "canonical_object_node_duplicate_count",
        "identifier_node_duplicate_count",
        "canonical_identifier_binding_duplicate_count",
        "release_edge_duplicate_count",
        "source_record_binding_duplicate_count",
        "scope_claim_duplicate_count",
        "identity_quarantine_duplicate_count",
        "duplicate_system_guard_failure_count",
    ):
        if checks[key] != 0:
            errors.append(f"{key}={checks[key]}")
    if checks["target_count"] != checks["distinct_target_count"]:
        errors.append("target universe is not unique")
    if checks["target_count"] != checks["outcome_count"]:
        errors.append("target universe is not exhaustively accounted")
    if checks["reverse_target_count"] != checks["distinct_forward_dr3_candidate_count"]:
        errors.append("reverse target universe does not equal the distinct forward DR3 candidates")
    if checks["scope_kind_count"] != 5:
        errors.append(f"scope_kind_count={checks['scope_kind_count']} expected=5")
    if checks["identity_quarantine_count"] != checks["expected_identity_quarantine_count"]:
        errors.append(
            "identity quarantine count does not match ambiguous/quarantined outcomes and Gaia collisions"
        )
    return {
        "status": "pass" if not errors else "fail",
        "errors": errors,
        "checks": checks,
        "outcomes": {str(outcome): int(count) for outcome, count in outcome_rows},
    }


def write_ordered_parquet(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
) -> dict[str, dict[str, Any]]:
    parquet_dir = output_dir / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    report: dict[str, dict[str, Any]] = {}
    for table, order_key in TABLE_ORDER.items():
        output = parquet_dir / f"{table}.parquet"
        con.execute(
            f"copy (select * from {table} order by {order_key}) to {sql_literal(output)} "
            "(format parquet, compression zstd)"
        )
        report[table] = {
            "row_count": int(con.execute(f"select count(*) from {table}").fetchone()[0]),
            "bytes": output.stat().st_size,
            "sha256": file_sha256(output),
            "path": f"parquet/{output.name}",
        }
    return report


def compile_graph(
    *,
    state_dir: Path,
    policy_path: Path,
    typed_report_path: Path,
    core_path: Path,
    output_root: Path,
    update_current: bool,
) -> dict[str, Any]:
    policy = load_json(policy_path)
    validate_policy(policy)
    typed_report = load_json(typed_report_path)
    if typed_report.get("status") != "pass":
        raise ValueError("typed source report must pass before E2 compilation")
    paths = source_path_index(typed_report, typed_report_path)
    forward_targets = required_path(
        paths, ("gaia.dr3.dr2_neighbourhood", "gaia_dr2_identity_target_set")
    )
    forward_edges = required_path(
        paths, ("gaia.dr3.dr2_neighbourhood", "gaia_dr2_neighbourhood_union")
    )
    reverse_edges = required_path(
        paths,
        ("gaia.dr3.dr2_neighbourhood_reverse", "gaia_dr2_neighbourhood_reverse_union"),
    )
    reverse_targets = required_path(
        paths,
        ("gaia.dr3.dr2_neighbourhood_reverse", "gaia_dr3_identity_target_set"),
    )
    inputs = input_identity(policy_path, typed_report, core_path)
    graph_id = stable_hash(inputs)[:24]
    final = output_root / graph_id
    final_report = final / "identity_graph_report.json"
    if final.exists():
        if not final_report.exists():
            raise ValueError(f"immutable identity graph lacks report: {final}")
        existing = load_json(final_report)
        if existing.get("graph_id") != graph_id or existing.get("inputs") != inputs:
            raise ValueError(f"immutable identity graph input mismatch: {final}")
        if update_current:
            promote_pointer(output_root, final)
        return existing

    work = output_root / f".{graph_id}.work"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    db_path = work / "identity_graph.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        configure_connection(con)
        create_policy_tables(con, policy)
        fallback = policy["fallback_identifier_scope"]
        create_canonical_tables(
            con,
            core_path=core_path,
            core_build_id=core_path.parent.name,
            fallback_source_id=fallback["source_id"],
            fallback_release_id=core_path.parent.name,
        )
        create_release_tables(
            con,
            forward_targets=forward_targets,
            forward_edges=forward_edges,
            reverse_targets=reverse_targets,
            reverse_edges=reverse_edges,
            high_pm_threshold=float(
                policy["gaia_release_reconciliation"]["high_proper_motion_mas_yr"]
            ),
            lineage=policy["gaia_release_reconciliation"],
        )
        create_source_bindings(con, paths, core_build_id=core_path.parent.name)
        create_identifier_and_diagnostic_tables(con, core_build_id=core_path.parent.name)
        verification = verification_summary(con)
        con.execute(
            """
            create table graph_metadata as
            select ?::varchar graph_id,
                   ?::varchar policy_version,
                   ?::varchar canonical_reference_build_id,
                   ?::varchar input_fingerprint,
                   'release_scoped_identity_and_scope_graph'::varchar graph_kind,
                   false canonical_inventory_mutated,
                   false canonical_containment_mutated
            """,
            [
                graph_id,
                policy["policy_version"],
                core_path.parent.name,
                stable_hash(inputs),
            ],
        )
        tables = write_ordered_parquet(con, work)
    finally:
        con.close()
    report = {
        "schema_version": "spacegate.evidence_identity_graph_report.v1",
        "graph_id": graph_id,
        "generated_at": utc_now(),
        "status": verification["status"],
        "policy_version": policy["policy_version"],
        "inputs": inputs,
        "tables": tables,
        "verification": verification,
        "artifact_path": str(final),
        "notes": [
            "The current CORE is a stability reference, not new scientific authority.",
            "Official Gaia release-neighborhood rows remain candidate edges; only unique bidirectional mappings bind automatically.",
            "Scope claims cannot mutate canonical containment or inventory in E2.",
        ],
    }
    json_write(work / "identity_graph_report.json", report)
    if verification["status"] != "pass":
        raise RuntimeError("identity graph verification failed: " + "; ".join(verification["errors"]))
    output_root.mkdir(parents=True, exist_ok=True)
    os.replace(work, final)
    if update_current:
        promote_pointer(output_root, final)
    report_path = state_dir / "reports" / "evidence_lake_v2" / "e2_identity_graph_report.json"
    json_write(report_path, report)
    return report


def promote_pointer(output_root: Path, final: Path) -> None:
    pointer = output_root / "current"
    temp = output_root / f".current.{os.getpid()}"
    temp.unlink(missing_ok=True)
    temp.symlink_to(final.name)
    os.replace(temp, pointer)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--typed-report", type=Path)
    parser.add_argument("--core-db", type=Path)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--no-current-pointer", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state_dir = args.state_dir.resolve()
    typed_report = args.typed_report or (
        state_dir / "reports" / "evidence_lake_v2" / "e2_typed_cook_report.json"
    )
    core_path = (args.core_db or (state_dir / "served" / "current" / "core.duckdb")).resolve()
    output_root = args.output_root or (
        state_dir / "derived" / "evidence_lake_v2" / "identity"
    )
    if not typed_report.exists():
        raise SystemExit(f"typed report not found: {typed_report}")
    if not core_path.exists():
        raise SystemExit(f"canonical stability reference not found: {core_path}")
    report = compile_graph(
        state_dir=state_dir,
        policy_path=args.policy.resolve(),
        typed_report_path=typed_report.resolve(),
        core_path=core_path,
        output_root=output_root.resolve(),
        update_current=not args.no_current_pointer,
    )
    print(
        f"Evidence identity graph {report['graph_id']} {report['status']} "
        f"targets={report['verification']['checks']['target_count']:,}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
