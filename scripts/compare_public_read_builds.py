#!/usr/bin/env python3
"""Compare two immutable Public Read builds and classify presentation deltas."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


INVARIANT_COUNTS = (
    "systems",
    "stars",
    "planets",
    "aliases",
    "search_terms",
    "exact_identifiers",
    "identifier_outcomes",
    "identifier_quarantine",
)
INVARIANT_HASHES = ("stars", "search_terms")
SYSTEM_PRESENTATION_FIELDS = {
    "star_count",
    "spectral_classes_json",
    "spectral_class_mask",
    "planet_category_mask",
    "hierarchy_representation",
    "scene_representation",
}
SYSTEM_LINEAGE_FIELDS = {
    "source_catalog",
    "source_version",
    "source_pk_text",
    "source_row_hash",
    "transform_version",
}
PLANET_LINEAGE_FIELDS = {
    "selected_fact_lineage_json",
    "source_catalog",
    "source_version",
    "source_row_hash",
    "transform_version",
}
CLASSIFICATION_EVIDENCE_PREFIXES = (
    "selected_msc_component_",
    "selected_sb9_component_",
    "selected_debcat_component_",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def columns(connection: sqlite3.Connection, schema: str, table: str) -> list[str]:
    return [str(row[1]) for row in connection.execute(f"PRAGMA {schema}.table_info({table})")]


def changed_count(
    connection: sqlite3.Connection,
    *,
    table: str,
    key: str,
    fields: list[str],
) -> int:
    if not fields:
        return 0
    condition = " OR ".join(
        f"b.{quote_identifier(field)} IS NOT c.{quote_identifier(field)}" for field in fields
    )
    return int(
        connection.execute(
            f"SELECT count(*) FROM main.{table} b JOIN candidate.{table} c USING({quote_identifier(key)}) WHERE {condition}"
        ).fetchone()[0]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--selected-hierarchy-ab", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--sample-limit", type=int, default=100)
    args = parser.parse_args()

    baseline = args.baseline.resolve()
    candidate = args.candidate.resolve()
    for root in (baseline, candidate):
        if not (root / "manifest.json").is_file() or not (root / "public_read.sqlite").is_file():
            raise SystemExit(f"incomplete Public Read artifact: {root}")
    baseline_manifest = load_json(baseline / "manifest.json")
    candidate_manifest = load_json(candidate / "manifest.json")
    hierarchy_ab = load_json(args.selected_hierarchy_ab.resolve())
    if hierarchy_ab.get("status") != "pass":
        raise SystemExit("selected hierarchy A/B has not passed")

    connection = sqlite3.connect(f"file:{baseline / 'public_read.sqlite'}?mode=ro", uri=True)
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=60000")
    connection.execute(
        "ATTACH DATABASE ? AS candidate", (f"file:{candidate / 'public_read.sqlite'}?mode=ro",)
    )

    count_deltas = {
        key: {
            "baseline": int((baseline_manifest.get("counts") or {}).get(key, -1)),
            "candidate": int((candidate_manifest.get("counts") or {}).get(key, -1)),
        }
        for key in INVARIANT_COUNTS
    }
    hash_deltas = {
        key: {
            "baseline": (baseline_manifest.get("logical_hashes") or {}).get(key),
            "candidate": (candidate_manifest.get("logical_hashes") or {}).get(key),
        }
        for key in INVARIANT_HASHES
    }

    removed_rows = connection.execute(
        """
        SELECT b.system_id,b.hierarchy_node_key,b.display_name,b.classification_value,
               b.classification_status,b.evidence_basis
        FROM main.stellar_badge_overlays b
        LEFT JOIN candidate.stellar_badge_overlays c
          ON c.system_id=b.system_id AND c.hierarchy_node_key IS b.hierarchy_node_key
        WHERE c.system_id IS NULL
        ORDER BY b.system_id,b.hierarchy_node_key
        """
    ).fetchall()
    added_rows = connection.execute(
        """
        SELECT c.system_id,c.hierarchy_node_key,c.stable_object_key,c.display_name,
               c.classification_value,c.classification_status,c.evidence_basis
        FROM candidate.stellar_badge_overlays c
        LEFT JOIN main.stellar_badge_overlays b
          ON b.system_id=c.system_id AND b.hierarchy_node_key IS c.hierarchy_node_key
        WHERE b.system_id IS NULL
        ORDER BY c.system_id,c.hierarchy_node_key
        """
    ).fetchall()
    removed_keys = {str(row[1]) for row in removed_rows}
    removed_system_ids = {int(row[0]) for row in removed_rows}
    expected_removed_leaf_keys = {
        str(row["hierarchy_node_key"])
        for row in hierarchy_ab.get("removed_nodes") or []
        if row.get("source_basis") == "msc_inferred_leaf"
    }
    removed_source_leaf_keys = {
        str(row[1]) for row in removed_rows if str(row[1] or "").startswith("canon:leaf:msc:")
    }
    removed_canonical_overlay_keys = removed_keys - removed_source_leaf_keys
    accepted_removed_canonical_overlay_keys = {
        key
        for key in removed_canonical_overlay_keys
        if connection.execute(
            "SELECT 1 FROM candidate.stars WHERE stable_object_key=? LIMIT 1", (key,)
        ).fetchone()
    }
    accepted_addition_keys = {
        str(row[1])
        for row in added_rows
        if str(row[1] or "").startswith("canon:star:")
        and int(row[0]) in removed_system_ids
        and connection.execute(
            "SELECT 1 FROM candidate.stars WHERE stable_object_key=? LIMIT 1", (row[2],)
        ).fetchone()
    }

    classification_rows = connection.execute(
        """
        SELECT b.system_id,b.hierarchy_node_key,b.classification_value,c.classification_value,
               b.classification_status,c.classification_status,c.evidence_basis
        FROM main.stellar_badge_overlays b JOIN candidate.stellar_badge_overlays c
          ON c.system_id=b.system_id AND c.hierarchy_node_key IS b.hierarchy_node_key
        WHERE (b.classification_value,b.classification_status)
              IS NOT (c.classification_value,c.classification_status)
        ORDER BY b.system_id,b.hierarchy_node_key
        """
    ).fetchall()
    accepted_classification_rows = [
        row
        for row in classification_rows
        if row[2] == "UNKNOWN"
        and row[4] == "missing"
        and row[5] in {"source", "assumed"}
        and any(str(row[6] or "").startswith(prefix) for prefix in CLASSIFICATION_EVIDENCE_PREFIXES)
    ]
    badge_identity_fields = [
        "leaf_component_key",
        "evidence_component_key",
        "star_id_text",
        "stable_object_key",
        "display_name",
        "catalog_component_label",
    ]
    badge_identity_mutations = changed_count(
        connection,
        table="stellar_badge_overlays",
        key="hierarchy_node_key",
        fields=badge_identity_fields,
    )

    system_fields = columns(connection, "main", "systems")
    immutable_system_fields = sorted(
        set(system_fields) - SYSTEM_PRESENTATION_FIELDS - SYSTEM_LINEAGE_FIELDS - {"system_id"}
    )
    immutable_system_change_count = changed_count(
        connection, table="systems", key="system_id", fields=immutable_system_fields
    )
    presentation_field_counts = {
        field: changed_count(connection, table="systems", key="system_id", fields=[field])
        for field in sorted(SYSTEM_PRESENTATION_FIELDS)
    }
    changed_badge_system_violation_count = int(
        connection.execute(
            """
            WITH changed_badge_systems AS (
              SELECT b.system_id
              FROM main.stellar_badge_overlays b
              LEFT JOIN candidate.stellar_badge_overlays c
                ON c.system_id=b.system_id AND c.hierarchy_node_key IS b.hierarchy_node_key
              WHERE c.system_id IS NULL
              UNION
              SELECT c.system_id
              FROM candidate.stellar_badge_overlays c
              LEFT JOIN main.stellar_badge_overlays b
                ON b.system_id=c.system_id AND b.hierarchy_node_key IS c.hierarchy_node_key
              WHERE b.system_id IS NULL
              UNION
              SELECT b.system_id
              FROM main.stellar_badge_overlays b JOIN candidate.stellar_badge_overlays c
                ON c.system_id=b.system_id AND c.hierarchy_node_key IS b.hierarchy_node_key
              WHERE (b.classification_value,b.classification_status)
                    IS NOT (c.classification_value,c.classification_status)
            )
            SELECT count(*)
            FROM main.systems b JOIN candidate.systems c USING(system_id)
            WHERE (
              b.star_count IS NOT c.star_count
              OR b.spectral_classes_json IS NOT c.spectral_classes_json
              OR b.spectral_class_mask IS NOT c.spectral_class_mask
              OR b.hierarchy_representation IS NOT c.hierarchy_representation
              OR b.scene_representation IS NOT c.scene_representation
            ) AND b.system_id NOT IN (SELECT system_id FROM changed_badge_systems)
            """
        ).fetchone()[0]
    )
    planet_mask_bit_removal_count = int(
        connection.execute(
            """
            SELECT count(*) FROM main.systems b JOIN candidate.systems c USING(system_id)
            WHERE (b.planet_category_mask & c.planet_category_mask) <> b.planet_category_mask
            """
        ).fetchone()[0]
    )
    planet_mask_transitions = {
        f"{before}->{after}": int(count)
        for before, after, count in connection.execute(
            """
            SELECT b.planet_category_mask,c.planet_category_mask,count(*)
            FROM main.systems b JOIN candidate.systems c USING(system_id)
            WHERE b.planet_category_mask IS NOT c.planet_category_mask
            GROUP BY 1,2 ORDER BY 1,2
            """
        )
    }

    planet_fields = columns(connection, "main", "planets")
    immutable_planet_fields = sorted(
        set(planet_fields) - PLANET_LINEAGE_FIELDS - {"planet_id"}
    )
    immutable_planet_change_count = changed_count(
        connection, table="planets", key="planet_id", fields=immutable_planet_fields
    )
    connection.close()

    checks = {
        "invariant_counts_unchanged": all(
            row["baseline"] == row["candidate"] for row in count_deltas.values()
        ),
        "invariant_logical_hashes_unchanged": all(
            row["baseline"] == row["candidate"] and row["baseline"] is not None
            for row in hash_deltas.values()
        ),
        "removed_source_badges_equal_reviewed_leaf_removals": (
            removed_source_leaf_keys == expected_removed_leaf_keys
        ),
        "removed_canonical_overlays_remain_base_stars": (
            accepted_removed_canonical_overlay_keys == removed_canonical_overlay_keys
        ),
        "added_badges_are_existing_canonical_stars_in_affected_systems": (
            len(accepted_addition_keys) == len(added_rows)
        ),
        "classification_changes_use_reviewed_general_policies": (
            len(accepted_classification_rows) == len(classification_rows)
        ),
        "badge_identity_fields_unchanged_for_retained_nodes": badge_identity_mutations == 0,
        "immutable_system_fields_unchanged": immutable_system_change_count == 0,
        "hierarchy_dependent_system_changes_trace_to_badges": (
            changed_badge_system_violation_count == 0
        ),
        "planet_category_bits_only_added": planet_mask_bit_removal_count == 0,
        "immutable_planet_fields_unchanged": immutable_planet_change_count == 0,
    }
    report = {
        "schema_version": "spacegate.public_read_scientific_ab.v2",
        "status": "pass" if all(checks.values()) else "fail",
        "generated_at_utc": utc_now(),
        "baseline": {"path": str(baseline), "build_id": baseline_manifest.get("build_id")},
        "candidate": {"path": str(candidate), "build_id": candidate_manifest.get("build_id")},
        "selected_hierarchy_ab": str(args.selected_hierarchy_ab.resolve()),
        "checks": checks,
        "count_comparison": count_deltas,
        "logical_hash_comparison": hash_deltas,
        "stellar_badges": {
            "removed_count": len(removed_rows),
            "reviewed_source_leaf_removal_count": len(removed_source_leaf_keys),
            "canonical_overlay_to_base_star_count": len(removed_canonical_overlay_keys),
            "added_count": len(added_rows),
            "classification_change_count": len(classification_rows),
            "classification_evidence_basis_counts": {
                basis: count
                for basis, count in sorted(
                    {
                        str(row[6]): sum(1 for item in classification_rows if item[6] == row[6])
                        for row in classification_rows
                    }.items()
                )
            },
            "removed_sample": [
                {
                    "system_id": row[0],
                    "hierarchy_node_key": row[1],
                    "display_name": row[2],
                    "classification_value": row[3],
                    "classification_status": row[4],
                    "evidence_basis": row[5],
                }
                for row in removed_rows[: args.sample_limit]
            ],
            "added": [
                {
                    "system_id": row[0],
                    "hierarchy_node_key": row[1],
                    "stable_object_key": row[2],
                    "display_name": row[3],
                    "classification_value": row[4],
                    "classification_status": row[5],
                    "evidence_basis": row[6],
                }
                for row in added_rows
            ],
        },
        "system_presentation_changes": {
            "field_counts": presentation_field_counts,
            "planet_category_mask_transitions": planet_mask_transitions,
            "planet_category_bit_removal_count": planet_mask_bit_removal_count,
            "hierarchy_dependent_change_violation_count": changed_badge_system_violation_count,
        },
        "unexpected": {
            "badge_identity_mutation_count": badge_identity_mutations,
            "classification_change_count": len(classification_rows) - len(accepted_classification_rows),
            "immutable_system_change_count": immutable_system_change_count,
            "immutable_planet_change_count": immutable_planet_change_count,
        },
    }
    atomic_json(args.report.resolve(), report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
