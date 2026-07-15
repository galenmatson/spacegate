#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import urllib.parse
import urllib.request
from pathlib import Path

import duckdb


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return bool(con.execute(
        "select 1 from information_schema.tables where table_schema='main' and table_name=?",
        [name],
    ).fetchone())


def build_metadata(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    if not table_exists(con, "build_metadata"):
        return {}
    columns = {
        str(row[1]) for row in con.execute("pragma table_info('build_metadata')").fetchall()
    }
    if not {"key", "value"}.issubset(columns):
        return {}
    return {
        str(key): "" if value is None else str(value)
        for key, value in con.execute("select key, value from build_metadata").fetchall()
    }


def verify_canonical_projection(
    arm: duckdb.DuckDBPyConnection,
    *,
    core_db: Path,
    canonical_arm: Path,
) -> None:
    if not canonical_arm.is_file():
        raise SystemExit(f"Canonical TESS projection source is missing: {canonical_arm}")

    arm.execute(f"attach {sql_literal(str(core_db))} as verify_core (read_only)")
    arm.execute(f"attach {sql_literal(str(canonical_arm))} as verify_canonical (read_only)")
    try:
        comparisons = {
            "tess_target_identity": (
                "select * exclude(tess_identity_id, ingested_at) from tess_target_identity",
                """
                select * exclude(tess_identity_id, ingested_at)
                from verify_canonical.tess_target_identity t
                where t.resolution_status <> 'accepted'
                   or (
                     t.star_id in (select star_id from verify_core.stars)
                     and t.system_id in (select system_id from verify_core.systems)
                   )
                """,
            ),
            "tess_missing_object_audit": (
                "select * exclude(audit_id) from tess_missing_object_audit",
                "select * exclude(audit_id) from verify_canonical.tess_missing_object_audit",
            ),
            "toi_current_evidence": (
                "select * exclude(toi_evidence_id, ingested_at) from toi_current_evidence",
                """
                select * exclude(toi_evidence_id, ingested_at)
                from verify_canonical.toi_current_evidence t
                where (t.system_id is null or t.system_id in (select system_id from verify_core.systems))
                  and (t.star_id is null or t.star_id in (select star_id from verify_core.stars))
                  and (t.planet_id is null or t.planet_id in (select planet_id from verify_core.planets))
                """,
            ),
            "toi_disposition_history": (
                "select * exclude(history_id, ingested_at) from toi_disposition_history",
                "select * exclude(history_id, ingested_at) from verify_canonical.toi_disposition_history",
            ),
        }
        failures: list[str] = []
        for table_name, (actual_sql, expected_sql) in comparisons.items():
            unexpected = int(
                arm.execute(
                    f"select count(*) from ({actual_sql} except {expected_sql})"
                ).fetchone()[0]
            )
            missing = int(
                arm.execute(
                    f"select count(*) from ({expected_sql} except {actual_sql})"
                ).fetchone()[0]
            )
            if unexpected or missing:
                failures.append(
                    f"{table_name}: unexpected={unexpected}, missing={missing}"
                )
        if failures:
            raise SystemExit(
                "Canonical TESS projection mismatch: " + "; ".join(failures)
            )
    finally:
        arm.execute("detach verify_canonical")
        arm.execute("detach verify_core")


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify TESS identity and TOI ARM evidence gates.")
    parser.add_argument("--core-db", required=True)
    parser.add_argument("--arm-db", required=True)
    parser.add_argument("--source-delta-report", default=None)
    parser.add_argument("--api-base-url", default=None)
    args = parser.parse_args()

    core_db = Path(args.core_db).resolve()
    core = duckdb.connect(str(core_db), read_only=True)
    arm = duckdb.connect(args.arm_db, read_only=True)
    required_core = {"object_identifiers", "identifier_quarantine", "aliases", "system_search_terms", "planets", "systems", "stars"}
    required_arm = {"tess_target_identity", "tess_missing_object_audit", "toi_current_evidence", "toi_disposition_history"}
    missing_core = sorted(name for name in required_core if not table_exists(core, name))
    missing_arm = sorted(name for name in required_arm if not table_exists(arm, name))
    if missing_core or missing_arm:
        raise SystemExit(f"Missing TESS verification tables: core={missing_core}, arm={missing_arm}")

    arm_metadata = build_metadata(arm)
    if arm_metadata.get("arm_tess_identity_mode") == "canonical_projection":
        source_arm = arm_metadata.get("arm_tess_identity_source_arm", "").strip()
        if not source_arm:
            raise SystemExit("Canonical TESS projection metadata is missing its source ARM")
        verify_canonical_projection(
            arm,
            core_db=core_db,
            canonical_arm=Path(source_arm),
        )

    target_count = int(arm.execute("select count(*) from tess_target_identity").fetchone()[0])
    partition_count = int(arm.execute(
        "select count(*) from tess_target_identity where resolution_status in ('accepted','missing','excluded','ambiguous','source_missing')"
    ).fetchone()[0])
    if target_count == 0 or partition_count != target_count:
        raise SystemExit(f"TIC coverage partition failed: targets={target_count}, partitioned={partition_count}")

    collisions = int(core.execute(
        """
        select count(*) from (
          select id_value_norm from object_identifiers where namespace='tic'
          group by id_value_norm having count(distinct target_id) > 1
        )
        """
    ).fetchone()[0])
    if collisions:
        raise SystemExit(f"TIC collision gate failed: {collisions}")

    accepted_count = int(arm.execute(
        "select count(*) from tess_target_identity where resolution_status='accepted'"
    ).fetchone()[0])
    unsearchable_tic_count = int(core.execute(
        """
        select count(*) from object_identifiers oi
        where oi.namespace='tic' and not exists (
          select 1 from system_search_terms t where t.term_norm='tic ' || oi.id_value_raw
        )
        """
    ).fetchone()[0])
    exact_search_count = accepted_count - unsearchable_tic_count
    if unsearchable_tic_count:
        raise SystemExit(
            f"TIC exact-search coverage failed: accepted={accepted_count}, searchable={exact_search_count}"
        )

    artifact_leaks = int(arm.execute(
        "select count(*) from tess_target_identity where resolution_status='accepted' and (tic_disposition in ('SPLIT','DUPLICATE','ARTIFACT') or duplicate_id is not null)"
    ).fetchone()[0])
    if artifact_leaks:
        raise SystemExit(f"TIC artifact quarantine gate failed: accepted_artifacts={artifact_leaks}")

    toi_count = int(arm.execute("select count(*) from toi_current_evidence").fetchone()[0])
    disposition_count = int(arm.execute(
        "select count(*) from toi_current_evidence where disposition in ('CP','KP','PC','APC','FP','FA')"
    ).fetchone()[0])
    if toi_count == 0 or disposition_count < toi_count - 10:
        raise SystemExit(f"TOI disposition coverage failed: rows={toi_count}, classified={disposition_count}")

    candidate_planet_leaks = int(arm.execute(
        "select count(*) from toi_current_evidence where disposition in ('PC','APC','FP','FA') and planet_id is not null"
    ).fetchone()[0])
    if candidate_planet_leaks:
        raise SystemExit(f"TOI candidate/negative planet-link leak: {candidate_planet_leaks}")

    duplicate_confirmed_links = int(arm.execute(
        """
        select count(*) from (
          select source_key from toi_current_evidence
          where disposition in ('CP','KP') and planet_id is not null
          group by source_key having count(distinct planet_id) > 1
        )
        """
    ).fetchone()[0])
    if duplicate_confirmed_links:
        raise SystemExit(f"Confirmed TOI duplicate planet links: {duplicate_confirmed_links}")

    host_searchable_tois = int(arm.execute(
        "select count(*) from toi_current_evidence where host_resolution_status='accepted'"
    ).fetchone()[0])
    searchable_tois = int(core.execute(
        "select count(*) from aliases a where a.alias_kind='toi_id' and exists (select 1 from system_search_terms t where t.system_id=a.system_id and t.term_norm=a.alias_norm)"
    ).fetchone()[0])
    if searchable_tois < host_searchable_tois:
        raise SystemExit(
            f"TOI exact-search coverage failed: accepted_hosts={host_searchable_tois}, searchable={searchable_tois}"
        )

    representative_counts = {
        "tess_eb_seeded": int(arm.execute(
            "select count(*) from tess_target_identity where source_families like '%tess_eb%'"
        ).fetchone()[0]),
    }
    # High-proper-motion and multiple-system focus checks run against core directly.
    high_pm = int(core.execute(
        """
        select count(*) from object_identifiers oi
        join stars s on s.star_id=oi.target_id
        join aliases a on a.target_type='star' and a.target_id=s.star_id
          and a.alias_kind='tic_id' and try_cast(replace(a.alias_raw,'TIC ','') as bigint)=try_cast(oi.id_value_raw as bigint)
        where oi.namespace='tic'
          and sqrt(coalesce(s.pm_ra_mas_yr,0)^2 + coalesce(s.pm_dec_mas_yr,0)^2) >= 500
        """
    ).fetchone()[0])
    representative_counts["high_proper_motion"] = high_pm
    representative_counts["multiple_system"] = int(core.execute(
        """
        select count(*) from object_identifiers oi
        join stars st on st.star_id=oi.target_id
        join systems sy on sy.system_id=st.system_id
        join aliases a on a.target_type='star' and a.target_id=st.star_id and a.alias_kind='tic_id'
        where oi.namespace='tic' and coalesce(sy.star_count,0) > 1
        """
    ).fetchone()[0])
    if (
        representative_counts["tess_eb_seeded"] == 0
        or representative_counts["high_proper_motion"] == 0
        or representative_counts["multiple_system"] == 0
    ):
        raise SystemExit(f"Representative TESS identity families missing: {representative_counts}")

    if args.source_delta_report:
        delta_path = Path(args.source_delta_report)
        if not delta_path.exists():
            raise SystemExit(f"Missing TESS source delta report: {delta_path}")
        delta = json.loads(delta_path.read_text(encoding="utf-8"))
        for family in ("toi", "tic"):
            family_delta = (delta.get("delta") or {}).get(family) or {}
            for key in ("added", "removed", "changed"):
                if key not in family_delta:
                    raise SystemExit(f"TESS source delta missing delta.{family}.{key}")

    if args.api_base_url:
        cases = []
        confirmed = arm.execute(
            """
            select source_key, tic_id, planet_id
            from toi_current_evidence
            where disposition in ('CP','KP') and planet_id is not null
              and host_resolution_status='accepted'
            order by source_key limit 1
            """
        ).fetchone()
        candidate = arm.execute(
            """
            select source_key, tic_id, star_id
            from toi_current_evidence
            where disposition in ('PC','APC') and star_id is not null
              and host_resolution_status='accepted'
            order by source_key limit 1
            """
        ).fetchone()
        negative = arm.execute(
            """
            select source_key, tic_id, star_id
            from toi_current_evidence
            where disposition in ('FP','FA') and star_id is not null
              and host_resolution_status='accepted'
            order by source_key limit 1
            """
        ).fetchone()
        tess_eb = arm.execute(
            """
            select tic_id, star_id
            from tess_target_identity
            where resolution_status='accepted' and source_families like '%tess_eb%'
            order by tic_id limit 1
            """
        ).fetchone()
        high_pm_tic = core.execute(
            """
            select oi.id_value_raw, s.star_id
            from object_identifiers oi
            join stars s on s.star_id=oi.target_id
            where oi.namespace='tic'
              and sqrt(coalesce(s.pm_ra_mas_yr,0)^2 + coalesce(s.pm_dec_mas_yr,0)^2) >= 500
            order by try_cast(oi.id_value_raw as bigint) limit 1
            """
        ).fetchone()
        multiple_tic = core.execute(
            """
            select oi.id_value_raw, st.star_id
            from object_identifiers oi
            join stars st on st.star_id=oi.target_id
            join systems sy on sy.system_id=st.system_id
            where oi.namespace='tic' and coalesce(sy.star_count,0) > 1
            order by try_cast(oi.id_value_raw as bigint) limit 1
            """
        ).fetchone()
        if not all((confirmed, candidate, negative, tess_eb, high_pm_tic, multiple_tic)):
            raise SystemExit("Unable to select the complete representative TESS API golden set")
        cases.extend([
            (f"TIC {confirmed[1]}", "star", None),
            (confirmed[0], "planet", int(confirmed[2])),
            (candidate[0], "star", None),
            (negative[0], "star", None),
            (f"TIC {tess_eb[0]}", "star", None),
            (f"TIC {high_pm_tic[0]}", "star", None),
            (f"TIC {multiple_tic[0]}", "star", None),
        ])
        endpoint = args.api_base_url.rstrip("/") + "/api/v1/systems/search"
        for query, expected_type, expected_planet_id in cases:
            url = endpoint + "?" + urllib.parse.urlencode({"q": query, "sort": "match", "limit": 1})
            with urllib.request.urlopen(url, timeout=30) as response:
                payload = json.load(response)
            items = payload.get("items") or []
            if not items:
                raise SystemExit(f"TESS API golden failed: {query!r} returned no rows")
            hit = items[0]
            if hit.get("matched_target_type") != expected_type:
                raise SystemExit(
                    f"TESS API golden failed: {query!r} expected {expected_type}, got {hit.get('matched_target_type')!r}"
                )
            if expected_planet_id is not None and int(hit.get("matched_planet_id") or 0) != expected_planet_id:
                raise SystemExit(
                    f"TESS API golden failed: {query!r} expected planet {expected_planet_id}, got {hit.get('matched_planet_id')!r}"
                )

    print(
        "OK: TESS evidence gates "
        f"(targets={target_count:,}, accepted={accepted_count:,}, TOIs={toi_count:,}, "
        f"searchable_TOIs={searchable_tois:,}, high_pm={high_pm:,})"
    )
    core.close()
    arm.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
