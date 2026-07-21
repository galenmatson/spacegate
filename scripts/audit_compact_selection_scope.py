#!/usr/bin/env python3
"""Audit whether compact-object evidence can bind to distinct canonical leaves."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_selection_policies.json"
DEFAULT_REPORT = Path(
    "/data/spacegate/state/reports/evidence_lake_v2/"
    "e5_compact_selection_scope_audit.json"
)
SOURCE_IDS = (
    "compact.atnf",
    "compact.mcgill_magnetar",
    "identity.simbad",
)


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def release_members(state_dir: Path, policy: dict[str, Any]) -> dict[str, dict[str, Any]]:
    manifest_path = (
        state_dir
        / "derived/evidence_lake_v2/scientific_evidence_sets"
        / str(policy["evidence_release_set_id"])
        / "manifest.json"
    )
    manifest = load_json(manifest_path)
    if manifest.get("release_set_sha256") != policy.get("evidence_release_set_sha256"):
        raise ValueError("selection policy/release-set content hash mismatch")
    result: dict[str, dict[str, Any]] = {}
    for member in manifest.get("members") or []:
        for source_id in member.get("source_ids") or []:
            if source_id in SOURCE_IDS:
                result[source_id] = member
    missing = sorted(set(SOURCE_IDS) - set(result))
    if missing:
        raise ValueError(f"release set lacks compact audit sources: {missing}")
    return result


def source_database(state_dir: Path, member: dict[str, Any]) -> Path:
    path = state_dir / str(member["artifact_path"]) / str(member["database"])
    if not path.is_file():
        raise ValueError(f"scientific evidence database missing: {path}")
    return path


def audit(*, state_dir: Path, policy_path: Path) -> dict[str, Any]:
    state_dir = state_dir.resolve()
    policy = load_json(policy_path.resolve())
    members = release_members(state_dir, policy)
    identity_db = (
        state_dir
        / "derived/evidence_lake_v2/identity"
        / str(policy["identity_graph_id"])
        / "identity_graph.duckdb"
    )
    core_db = (
        state_dir
        / "out"
        / str(policy["canonical_reference_build_id"])
        / "core.duckdb"
    )
    if not identity_db.is_file() or not core_db.is_file():
        raise ValueError("compact audit identity graph or canonical reference is missing")

    paths = {source_id: source_database(state_dir, member) for source_id, member in members.items()}
    con = duckdb.connect(
        ":memory:",
        config={"threads": "4", "memory_limit": "8GB", "preserve_insertion_order": "false"},
    )
    try:
        con.execute(f"ATTACH {sql_literal(paths['compact.atnf'])} AS atnf (READ_ONLY)")
        con.execute(
            f"ATTACH {sql_literal(paths['compact.mcgill_magnetar'])} AS mcgill (READ_ONLY)"
        )
        con.execute(f"ATTACH {sql_literal(paths['identity.simbad'])} AS simbad (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(identity_db)} AS identity (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(core_db)} AS core (READ_ONLY)")
        con.execute(
            """
            CREATE TEMP TABLE compact_source_names AS
            WITH atnf_names AS (
              SELECT DISTINCT identifier_normalized source_name,
                     lower('psr ' || trim(identifier_normalized)) bridge_name
              FROM atnf.identifier_claim_evidence
              WHERE namespace='atnf_pulsar_name'
            ), mcgill_names AS (
              SELECT DISTINCT identifier_normalized source_name,
                     lower(regexp_replace(trim(identifier_normalized), '\\s+', ' ', 'g'))
                       bridge_name
              FROM mcgill.identifier_claim_evidence
              WHERE namespace='magnetar_name'
            )
            SELECT 'compact.atnf' source_id, * FROM atnf_names
            UNION ALL
            SELECT 'compact.mcgill_magnetar' source_id, * FROM mcgill_names
            """
        )
        con.execute(
            """
            CREATE TEMP TABLE simbad_designation_to_canonical AS
            WITH designations AS (
              SELECT normalized.designation_normalized, ic.source_record_id
              FROM simbad.identifier_claim_evidence ic
              CROSS JOIN LATERAL (
                SELECT lower(regexp_replace(trim(ic.identifier_normalized), '\\s+', ' ', 'g'))
                         designation_normalized
              ) normalized
              JOIN (SELECT DISTINCT bridge_name FROM compact_source_names) wanted
                ON wanted.bridge_name=normalized.designation_normalized
              WHERE ic.namespace IN ('simbad_identifier', 'simbad_main_id')
            ), designation_oids AS (
              SELECT d.designation_normalized,
                     oid.identifier_normalized simbad_oid
              FROM designations d
              JOIN simbad.identifier_claim_evidence oid
                ON oid.source_record_id=d.source_record_id
               AND oid.namespace='simbad_oid'
            ), oid_gaia AS (
              SELECT oid.identifier_normalized simbad_oid,
                     regexp_extract(gaia.identifier_normalized, '([0-9]+)$', 1) gaia_dr3
              FROM simbad.identifier_claim_evidence oid
              JOIN simbad.identifier_claim_evidence gaia
                ON gaia.source_record_id=oid.source_record_id
               AND gaia.namespace='gaia_dr3_source_id'
              WHERE oid.namespace='simbad_oid'
            )
            SELECT DISTINCT d.designation_normalized, d.simbad_oid, g.gaia_dr3,
                   b.object_node_key, b.stable_object_key
            FROM designation_oids d
            JOIN oid_gaia g USING (simbad_oid)
            JOIN identity.canonical_identifier_bindings b
              ON b.namespace='gaia_dr3' AND b.id_value_norm=g.gaia_dr3
            """
        )
        con.execute(
            """
            CREATE TEMP TABLE compact_name_candidates AS
            SELECT n.source_id, n.source_name, n.bridge_name,
                   m.simbad_oid, m.gaia_dr3, m.object_node_key,
                   m.stable_object_key, s.star_name, s.object_type,
                   s.spectral_type_raw,
                   regexp_matches(
                     trim(coalesce(s.spectral_type_raw, '')), '^[OBAFGKM](?:[0-9.]|$)'
                   ) ordinary_stellar_class,
                   lower(coalesce(s.object_type, '')) IN
                     ('pulsar', 'magnetar', 'neutron_star') compact_object_type,
                   regexp_matches(
                     lower(coalesce(s.spectral_type_raw, '')),
                     'pulsar|magnetar|neutron'
                   ) compact_spectral_marker
            FROM compact_source_names n
            LEFT JOIN simbad_designation_to_canonical m
              ON m.designation_normalized=n.bridge_name
            LEFT JOIN core.stars s ON s.stable_object_key=m.stable_object_key
            """
        )

        source_summary: dict[str, Any] = {}
        for source_id in SOURCE_IDS[:2]:
            row = con.execute(
                """
                SELECT COUNT(*) source_name_count,
                       COUNT(*) FILTER (WHERE stable_object_key IS NOT NULL)
                         matched_name_count,
                       COUNT(DISTINCT stable_object_key) target_count,
                       COUNT(*) FILTER (
                         WHERE stable_object_key IS NOT NULL
                           AND (ordinary_stellar_class
                             OR NOT (compact_object_type OR compact_spectral_marker))
                       ) scope_conflict_count,
                       COUNT(*) FILTER (
                         WHERE stable_object_key IS NOT NULL
                           AND NOT ordinary_stellar_class
                           AND (compact_object_type OR compact_spectral_marker)
                       ) safe_target_count
                FROM compact_name_candidates WHERE source_id=?
                """,
                [source_id],
            ).fetchone()
            compact_rows = int(
                con.execute(
                    ("SELECT COUNT(*) FROM atnf.compact_object_evidence" if source_id == "compact.atnf"
                     else "SELECT COUNT(*) FROM mcgill.compact_object_evidence")
                ).fetchone()[0]
            )
            source_summary[source_id] = {
                "compact_evidence_rows": compact_rows,
                "source_name_count": int(row[0]),
                "matched_name_count": int(row[1]),
                "target_count": int(row[2]),
                "scope_conflict_count": int(row[3]),
                "safe_target_count": int(row[4]),
                "missing_name_count": int(row[0]) - int(row[1]),
            }

        diagnostics = [
            {
                "source_id": row[0],
                "source_name": row[1],
                "simbad_oid": row[2],
                "gaia_dr3": row[3],
                "stable_object_key": row[4],
                "canonical_name": row[5],
                "canonical_object_type": row[6],
                "canonical_spectral_type_raw": row[7],
                "outcome": (
                    "quarantined_component_scope_conflict"
                    if row[8] or not (row[9] or row[10])
                    else "eligible_compact_leaf"
                ),
            }
            for row in con.execute(
                """
                SELECT source_id, source_name, simbad_oid, gaia_dr3,
                       stable_object_key, star_name, object_type, spectral_type_raw,
                       ordinary_stellar_class, compact_object_type,
                       compact_spectral_marker
                FROM compact_name_candidates
                WHERE stable_object_key IS NOT NULL
                ORDER BY source_id, source_name, stable_object_key
                """
            ).fetchall()
        ]
    finally:
        con.close()

    failures: list[str] = []
    if source_summary["compact.atnf"]["safe_target_count"]:
        failures.append("ATNF has a safe compact canonical leaf and needs an E5 quantity policy")
    if source_summary["compact.mcgill_magnetar"]["safe_target_count"]:
        failures.append("McGill has a safe compact canonical leaf and needs an E5 quantity policy")
    return {
        "schema_version": "spacegate.e5_compact_selection_scope_audit.v1",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
            "+00:00", "Z"
        ),
        "status": "pass" if not failures else "action_required",
        "policy_version": policy["policy_version"],
        "evidence_release_set_id": policy["evidence_release_set_id"],
        "identity_graph_id": policy["identity_graph_id"],
        "canonical_reference_build_id": policy["canonical_reference_build_id"],
        "source_summary": source_summary,
        "matched_target_diagnostics": diagnostics,
        "selection_disposition": (
            "No compact facts may be selected onto the current canonical leaves. "
            "Preserve ATNF and McGill as evidence until E6 creates or binds distinct "
            "compact-object identities without merging an optical companion."
        ),
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=Path, default=Path("/data/spacegate/state"))
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = audit(state_dir=args.state_dir, policy_path=args.policy)
    atomic_json(args.report.resolve(), report)
    print(
        "E5 compact scope audit "
        f"{report['status']}: atnf_safe="
        f"{report['source_summary']['compact.atnf']['safe_target_count']} "
        "mcgill_safe="
        f"{report['source_summary']['compact.mcgill_magnetar']['safe_target_count']}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
