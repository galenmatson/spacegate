#!/usr/bin/env python3
"""Compile the active hierarchy from permanent identity and selected relations."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_selected_hierarchy.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_OUTPUT_ROOT = Path("/space/spacegate/e7-selected-hierarchy")
CANONICAL_NODE_BASES = {"canonical_system", "canonical_star", "canonical_planet"}
CANONICAL_EDGE_BASES = {
    "canonical_root_star", "canonical_host_planet", "fallback_root_planet"
}
PRUNABLE_GROUP_BASES = {"wds_msc_implied_role"}


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.e7_selected_hierarchy_policy.v1":
        raise ValueError("unsupported selected hierarchy policy")
    if set(policy.get("inputs") or {}) != {"clean_foundation", "selected_components"}:
        raise ValueError("selected hierarchy inputs are incomplete")
    expected_rules = {
        "preserve_all_canonical_nodes": True,
        "preserve_all_canonical_edges": True,
        "require_accepted_relation_for_msc_inferred_leaf": True,
        "preserve_component_case_until_role_interpretation": True,
        "uppercase_multi_component_labels_are_groups": True,
        "prune_empty_source_groups": True,
        "allow_named_object_conditions": False,
    }
    if policy.get("rules") != expected_rules:
        raise ValueError("unsafe selected hierarchy rules")
    for name, item in policy["inputs"].items():
        path = Path(str(item.get("relative_path") or ""))
        if path.is_absolute() or ".." in path.parts:
            raise ValueError(f"unbounded input path: {name}")
        if not item.get("build_id") or len(str(item.get("manifest_sha256") or "")) != 64:
            raise ValueError(f"invalid input contract: {name}")


def resolve_input(state: Path, spec: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    root = (state / spec["relative_path"]).resolve(strict=True)
    manifest_path = root / "manifest.json"
    if sha256_file(manifest_path) != spec["manifest_sha256"]:
        raise ValueError(f"manifest checksum mismatch: {root}")
    manifest = load_json(manifest_path)
    if manifest.get("build_id") != spec["build_id"]:
        raise ValueError(f"build identity mismatch: {root}")
    verification = manifest.get("verification") or {}
    if manifest.get("status") == "fail" or verification.get("status") == "fail":
        raise ValueError(f"input did not pass verification: {root}")
    return root, manifest


def registered_product(root: Path, manifest: dict[str, Any], relative: str) -> Path:
    item = (manifest.get("products") or manifest.get("files") or {}).get(relative)
    path = root / relative
    if not path.is_file() or not isinstance(item, dict):
        raise FileNotFoundError(path)
    if sha256_file(path) != item.get("sha256"):
        raise ValueError(f"input product checksum mismatch: {path}")
    return path


def leaf_label(value: str | None) -> str | None:
    """Return case-significant MSC stellar leaf labels, excluding group labels."""
    label = str(value or "").strip()
    if re.fullmatch(r"[A-Z][a-z][0-9]*", label):
        return label
    return None


def source_component_parts(key: str | None) -> tuple[str, str] | None:
    parts = str(key or "").split(":")
    if len(parts) < 2:
        return None
    label = leaf_label(parts[-1])
    return (parts[-2].lower(), label.lower()) if label else None


def compile_hierarchy(
    policy_path: Path,
    state: Path,
    output_root: Path,
    *,
    link_into_state: bool,
) -> dict[str, Any]:
    started = time.monotonic()
    policy = load_json(policy_path)
    validate_policy(policy)
    foundation_root, foundation_manifest = resolve_input(state, policy["inputs"]["clean_foundation"])
    components_root, components_manifest = resolve_input(state, policy["inputs"]["selected_components"])
    source_hierarchy = registered_product(
        foundation_root, foundation_manifest, "canonical_hierarchy.duckdb"
    )
    components_db = registered_product(
        components_root, components_manifest, "selected_components.duckdb"
    )
    input_identity = {
        name: {
            "build_id": spec["build_id"],
            "manifest_sha256": spec["manifest_sha256"],
        }
        for name, spec in policy["inputs"].items()
    }
    build_id = stable_hash({
        "policy_sha256": sha256_file(policy_path),
        "compiler_sha256": sha256_file(Path(__file__).resolve()),
        "inputs": input_identity,
    })[:24]
    final_dir = output_root / build_id
    if (final_dir / "manifest.json").is_file():
        return load_json(final_dir / "manifest.json")
    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    database = staging / "canonical_hierarchy.duckdb"
    nodes_parquet = staging / "hierarchy_nodes.parquet"
    edges_parquet = staging / "hierarchy_edges.parquet"
    try:
        con = duckdb.connect()
        con.execute("SET threads=8")
        con.execute("SET memory_limit='16GB'")
        con.execute(f"ATTACH {sql_literal(source_hierarchy)} AS base (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(components_db)} AS selected (READ_ONLY)")

        accepted_leaf_support = {
            parts
            for row in con.execute(
                "SELECT left_source_component_key,right_source_component_key "
                "FROM selected.msc_relation_evidence_projection "
                "WHERE projection_status='accepted_relation_evidence'"
            ).fetchall()
            for key in row
            if (parts := source_component_parts(key)) is not None
        }
        con.execute("CREATE TEMP TABLE accepted_msc_leaf_support(wds_id VARCHAR,label VARCHAR)")
        con.executemany(
            "INSERT INTO accepted_msc_leaf_support VALUES (?,?)",
            sorted(accepted_leaf_support),
        )
        con.execute(
            """
            CREATE TEMP TABLE removed_nodes(hierarchy_node_key VARCHAR PRIMARY KEY,reason VARCHAR);
            INSERT INTO removed_nodes
            SELECT hierarchy_node_key,'msc_leaf_without_accepted_case_valid_relation'
            FROM base.hierarchy_nodes n
            WHERE n.source_basis='msc_inferred_leaf'
              AND NOT EXISTS (
                SELECT 1 FROM accepted_msc_leaf_support support
                WHERE support.wds_id=lower(trim(n.wds_id))
                  AND support.label=lower(split_part(n.hierarchy_node_key,':',-1))
              );
            """
        )
        while True:
            inserted = int(
                con.execute(
                    """
                    WITH empty_groups AS (
                      SELECT n.hierarchy_node_key
                      FROM base.hierarchy_nodes n
                      WHERE n.source_basis IN ('wds_msc_implied_role')
                        AND n.hierarchy_node_key NOT IN (SELECT hierarchy_node_key FROM removed_nodes)
                        AND NOT EXISTS (
                          SELECT 1 FROM base.hierarchy_edges e
                          WHERE e.parent_node_key=n.hierarchy_node_key
                            AND e.child_node_key NOT IN (SELECT hierarchy_node_key FROM removed_nodes)
                        )
                    )
                    INSERT INTO removed_nodes
                    SELECT hierarchy_node_key,'empty_source_group_after_leaf_selection'
                    FROM empty_groups
                    ON CONFLICT DO NOTHING
                    RETURNING hierarchy_node_key
                    """
                ).fetchall().__len__()
            )
            if inserted == 0:
                break
        con.execute(
            """
            CREATE TEMP TABLE active_nodes AS
            SELECT * FROM base.hierarchy_nodes
            WHERE hierarchy_node_key NOT IN (SELECT hierarchy_node_key FROM removed_nodes);
            CREATE TEMP TABLE active_edges AS
            SELECT * FROM base.hierarchy_edges
            WHERE parent_node_key IN (SELECT hierarchy_node_key FROM active_nodes)
              AND child_node_key IN (SELECT hierarchy_node_key FROM active_nodes);
            """
        )
        before_node_counts = dict(con.execute(
            "SELECT source_basis,count(*) FROM base.hierarchy_nodes GROUP BY 1 ORDER BY 1"
        ).fetchall())
        after_node_counts = dict(con.execute(
            "SELECT source_basis,count(*) FROM active_nodes GROUP BY 1 ORDER BY 1"
        ).fetchall())
        before_edge_counts = dict(con.execute(
            "SELECT source_basis,count(*) FROM base.hierarchy_edges GROUP BY 1 ORDER BY 1"
        ).fetchall())
        after_edge_counts = dict(con.execute(
            "SELECT source_basis,count(*) FROM active_edges GROUP BY 1 ORDER BY 1"
        ).fetchall())
        removed_nodes = [
            {
                "hierarchy_node_key": row[0], "display_name": row[1], "wds_id": row[2],
                "source_basis": row[3], "reason": row[4],
            }
            for row in con.execute(
                """
                SELECT n.hierarchy_node_key,n.display_name,n.wds_id,n.source_basis,r.reason
                FROM removed_nodes r JOIN base.hierarchy_nodes n USING(hierarchy_node_key)
                ORDER BY n.wds_id,n.hierarchy_node_key
                """
            ).fetchall()
        ]
        for node in removed_nodes:
            if node["source_basis"] != "msc_inferred_leaf":
                node["evidence_context"] = []
                node["removal_class"] = "empty_source_group"
                continue
            suffix = node["hierarchy_node_key"].rsplit(":", 1)[-1].lower()
            evidence_context: list[dict[str, Any]] = []
            for row in con.execute(
                """
                SELECT source_record_id,left_source_component_key,
                       right_source_component_key,projection_status,
                       json_extract_string(quality_json,'$.Comment')
                FROM selected.msc_relation_evidence_projection
                WHERE lower(split_part(coalesce(left_source_component_key,''),':',-2))=?
                   OR lower(split_part(coalesce(right_source_component_key,''),':',-2))=?
                ORDER BY source_record_id
                """,
                [str(node["wds_id"] or "").lower()] * 2,
            ).fetchall():
                for endpoint_key in row[1:3]:
                    endpoint_label = str(endpoint_key or "").rsplit(":", 1)[-1]
                    if endpoint_key and endpoint_label.lower() == suffix:
                        evidence_context.append({
                            "source_record_id": row[0],
                            "endpoint_label": endpoint_label,
                            "endpoint_kind": (
                                "stellar_leaf" if leaf_label(endpoint_label) else "group_or_unresolved"
                            ),
                            "projection_status": row[3],
                            "comment": row[4],
                        })
            node["evidence_context"] = evidence_context
            statuses = {row["projection_status"] for row in evidence_context}
            accepted_group_collision = any(
                row["projection_status"] == "accepted_relation_evidence"
                and row["endpoint_kind"] != "stellar_leaf"
                for row in evidence_context
            )
            if "context_only_planetary_relation_evidence" in statuses:
                node["removal_class"] = (
                    "planetary_context_with_casefold_group_collision"
                    if accepted_group_collision
                    else "planetary_context"
                )
            elif accepted_group_collision:
                node["removal_class"] = "casefolded_group_presented_as_leaf"
            elif evidence_context:
                node["removal_class"] = "nonaccepted_relation_endpoint"
            else:
                node["removal_class"] = "no_selected_relation_endpoint"
        removed_edges = [
            {
                "hierarchy_edge_id": row[0], "parent_node_key": row[1],
                "child_node_key": row[2], "source_basis": row[3],
            }
            for row in con.execute(
                """
                SELECT hierarchy_edge_id,parent_node_key,child_node_key,source_basis
                FROM base.hierarchy_edges
                WHERE hierarchy_edge_id NOT IN (SELECT hierarchy_edge_id FROM active_edges)
                ORDER BY hierarchy_edge_id
                """
            ).fetchall()
        ]
        canonical_node_delta = int(con.execute(
            "SELECT count(*) FROM base.hierarchy_nodes WHERE source_basis IN ('canonical_system','canonical_star','canonical_planet')"
        ).fetchone()[0]) - int(con.execute(
            "SELECT count(*) FROM active_nodes WHERE source_basis IN ('canonical_system','canonical_star','canonical_planet')"
        ).fetchone()[0])
        canonical_edge_delta = int(con.execute(
            "SELECT count(*) FROM base.hierarchy_edges WHERE source_basis IN ('canonical_root_star','canonical_host_planet','fallback_root_planet')"
        ).fetchone()[0]) - int(con.execute(
            "SELECT count(*) FROM active_edges WHERE source_basis IN ('canonical_root_star','canonical_host_planet','fallback_root_planet')"
        ).fetchone()[0])
        checks = {
            "canonical_node_delta": canonical_node_delta,
            "canonical_edge_delta": canonical_edge_delta,
            "duplicate_active_nodes": int(con.execute(
                "SELECT count(*) FROM (SELECT hierarchy_node_key FROM active_nodes GROUP BY 1 HAVING count(*)>1)"
            ).fetchone()[0]),
            "duplicate_active_edges": int(con.execute(
                "SELECT count(*) FROM (SELECT hierarchy_edge_id FROM active_edges GROUP BY 1 HAVING count(*)>1)"
            ).fetchone()[0]),
            "edges_missing_parent": int(con.execute(
                "SELECT count(*) FROM active_edges e LEFT JOIN active_nodes n ON n.hierarchy_node_key=e.parent_node_key WHERE n.hierarchy_node_key IS NULL"
            ).fetchone()[0]),
            "edges_missing_child": int(con.execute(
                "SELECT count(*) FROM active_edges e LEFT JOIN active_nodes n ON n.hierarchy_node_key=e.child_node_key WHERE n.hierarchy_node_key IS NULL"
            ).fetchone()[0]),
            "unsupported_msc_leaves_remaining": int(con.execute(
                """
                SELECT count(*) FROM active_nodes n
                WHERE n.source_basis='msc_inferred_leaf'
                  AND NOT EXISTS (
                    SELECT 1 FROM accepted_msc_leaf_support support
                    WHERE support.wds_id=lower(trim(n.wds_id))
                      AND support.label=lower(split_part(n.hierarchy_node_key,':',-1))
                  )
                """
            ).fetchone()[0]),
            "empty_source_groups_remaining": int(con.execute(
                """
                SELECT count(*) FROM active_nodes n
                WHERE n.source_basis='wds_msc_implied_role'
                  AND NOT EXISTS (SELECT 1 FROM active_edges e WHERE e.parent_node_key=n.hierarchy_node_key)
                """
            ).fetchone()[0]),
        }
        if any(checks.values()):
            raise ValueError(f"selected hierarchy verification failed: {checks}")
        con.execute(
            f"COPY (SELECT * FROM active_nodes ORDER BY hierarchy_node_key) TO {sql_literal(nodes_parquet)} (FORMAT PARQUET,COMPRESSION ZSTD)"
        )
        con.execute(
            f"COPY (SELECT * FROM active_edges ORDER BY hierarchy_edge_id) TO {sql_literal(edges_parquet)} (FORMAT PARQUET,COMPRESSION ZSTD)"
        )
        con.close()

        output = duckdb.connect(str(database))
        output.execute(
            f"CREATE TABLE hierarchy_nodes AS SELECT * FROM read_parquet({sql_literal(nodes_parquet)}) ORDER BY hierarchy_node_key"
        )
        output.execute(
            f"CREATE TABLE hierarchy_edges AS SELECT * FROM read_parquet({sql_literal(edges_parquet)}) ORDER BY hierarchy_edge_id"
        )
        output.execute("CREATE TABLE build_metadata(key VARCHAR,value VARCHAR)")
        output.executemany("INSERT INTO build_metadata VALUES (?,?)", [
            ("build_id", build_id),
            ("build_kind", "e7_selected_hierarchy"),
            ("policy_version", policy["policy_version"]),
            ("foundation_build_id", policy["inputs"]["clean_foundation"]["build_id"]),
            ("selected_components_build_id", policy["inputs"]["selected_components"]["build_id"]),
        ])
        output.execute("CREATE INDEX hierarchy_nodes_key_idx ON hierarchy_nodes(hierarchy_node_key)")
        output.execute("CREATE INDEX hierarchy_edges_parent_idx ON hierarchy_edges(parent_node_key)")
        output.execute("CREATE INDEX hierarchy_edges_child_idx ON hierarchy_edges(child_node_key)")
        output.execute("CHECKPOINT")
        output.close()

        products = {
            path.name: {
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
                "determinism": "logical_tables" if path.suffix == ".duckdb" else "byte_exact",
            }
            for path in (database, nodes_parquet, edges_parquet)
        }
        report = {
            "schema_version": "spacegate.e7_selected_hierarchy_ab.v1",
            "status": "pass",
            "before": {
                "node_count": sum(before_node_counts.values()),
                "edge_count": sum(before_edge_counts.values()),
                "node_counts_by_source_basis": before_node_counts,
                "edge_counts_by_source_basis": before_edge_counts,
            },
            "after": {
                "node_count": sum(after_node_counts.values()),
                "edge_count": sum(after_edge_counts.values()),
                "node_counts_by_source_basis": after_node_counts,
                "edge_counts_by_source_basis": after_edge_counts,
            },
            "removed_node_count": len(removed_nodes),
            "removed_edge_count": len(removed_edges),
            "affected_wds_system_count": len({row["wds_id"] for row in removed_nodes if row["wds_id"]}),
            "removed_nodes": removed_nodes,
            "removed_edges": removed_edges,
            "checks": checks,
        }
        atomic_json(staging / "scientific_ab.json", report)
        products["scientific_ab.json"] = {
            "bytes": (staging / "scientific_ab.json").stat().st_size,
            "sha256": sha256_file(staging / "scientific_ab.json"),
            "determinism": "logical_content",
        }
        manifest = {
            "schema_version": "spacegate.e7_selected_hierarchy_manifest.v1",
            "status": "pass",
            "build_id": build_id,
            "generated_at_utc": utc_now(),
            "policy_version": policy["policy_version"],
            "compiler_version": policy["compiler_version"],
            "policy_sha256": sha256_file(policy_path),
            "compiler_sha256": sha256_file(Path(__file__).resolve()),
            "inputs": input_identity,
            "products": products,
            "counts": report["after"],
            "scientific_ab": {
                "status": "pass",
                "removed_node_count": len(removed_nodes),
                "removed_edge_count": len(removed_edges),
                "affected_wds_system_count": report["affected_wds_system_count"],
            },
            "verification": {"status": "pass", "checks": checks},
            "wall_seconds": round(time.monotonic() - started, 6),
        }
        atomic_json(staging / "manifest.json", manifest)
        os.replace(staging, final_dir)
        if link_into_state:
            link_root = state / "derived/evidence_lake_v2/selected_hierarchy"
            link_root.mkdir(parents=True, exist_ok=True)
            link = link_root / build_id
            if not link.exists() and not link.is_symlink():
                temporary = link_root / f".{build_id}.link"
                temporary.unlink(missing_ok=True)
                temporary.symlink_to(final_dir)
                os.replace(temporary, link)
        return manifest
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--no-state-link", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    manifest = compile_hierarchy(
        args.policy.resolve(), args.state.resolve(), args.output_root.resolve(),
        link_into_state=not args.no_state_link,
    )
    rendered = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
