#!/usr/bin/env python3
"""Compile E4 evidence shards into immutable, provenance-bearing selected facts."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import re
import resource
import shutil
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

import audit_e5_source_dispositions as source_disposition_audit


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_selection_policies.json"
DEFAULT_DISPOSITIONS = ROOT / "config/evidence_lake/e5_source_dispositions.json"
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def allocated_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for entry in path.rglob("*"):
        try:
            if entry.is_file():
                total += entry.stat().st_blocks * 512
        except FileNotFoundError:
            continue
    return total


def process_cpu_seconds() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return float(usage.ru_utime + usage.ru_stime)


class PhaseRecorder:
    """Persist incremental compiler timing and resource observations."""

    def __init__(
        self,
        *,
        build_id: str,
        compiler_version: str,
        staging: Path,
        spill: Path,
        report_path: Path,
    ) -> None:
        self.build_id = build_id
        self.compiler_version = compiler_version
        self.staging = staging
        self.spill = spill
        self.report_path = report_path
        self.phases: list[dict[str, Any]] = []
        self.active: dict[str, Any] | None = None
        self._stop: threading.Event | None = None
        self._sampler: threading.Thread | None = None
        self._peaks: dict[str, int] = {}

    def _snapshot(self, status: str) -> dict[str, Any]:
        return {
            "schema_version": "spacegate.e5_compile_performance.v1",
            "status": status,
            "build_id": self.build_id,
            "compiler_version": self.compiler_version,
            "active_phase": self.active,
            "phases": self.phases,
        }

    def _write(self, status: str = "in_progress") -> None:
        atomic_json(self.report_path, self._snapshot(status))

    def _sample(self) -> None:
        while self._stop is not None and not self._stop.wait(1.0):
            self._sample_once()

    def _sample_once(self) -> None:
        self._peaks["peak_staging_allocated_bytes"] = max(
            self._peaks.get("peak_staging_allocated_bytes", 0),
            allocated_bytes(self.staging),
        )
        self._peaks["peak_spill_allocated_bytes"] = max(
            self._peaks.get("peak_spill_allocated_bytes", 0),
            allocated_bytes(self.spill),
        )
        self._peaks["process_peak_rss_kib"] = max(
            self._peaks.get("process_peak_rss_kib", 0),
            int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
        )

    def start(self, phase: str, *, source_id: str | None = None) -> None:
        if self.active is not None:
            raise RuntimeError(f"compiler phase already active: {self.active['phase']}")
        self._peaks = {}
        self._sample_once()
        self.active = {
            "phase": phase,
            "source_id": source_id,
            "started_at": utc_now(),
            "start_monotonic": time.monotonic(),
            "start_cpu_seconds": process_cpu_seconds(),
            "start_staging_allocated_bytes": allocated_bytes(self.staging),
            "start_spill_allocated_bytes": allocated_bytes(self.spill),
        }
        self._stop = threading.Event()
        self._sampler = threading.Thread(target=self._sample, daemon=True)
        self._sampler.start()
        print(
            f"E5 phase start: {phase}"
            + (f" source={source_id}" if source_id else ""),
            flush=True,
        )
        self._write()

    def add_completed(
        self,
        phase: str,
        *,
        started_at: str,
        start_monotonic: float,
        start_cpu_seconds: float,
        details: dict[str, Any] | None = None,
    ) -> None:
        row = {
            "phase": phase,
            "source_id": None,
            "started_at": started_at,
            "finished_at": utc_now(),
            "status": "pass",
            "wall_seconds": round(time.monotonic() - start_monotonic, 6),
            "cpu_seconds": round(process_cpu_seconds() - start_cpu_seconds, 6),
            "staging_allocated_bytes": allocated_bytes(self.staging),
            "spill_allocated_bytes": allocated_bytes(self.spill),
            "peak_staging_allocated_bytes": allocated_bytes(self.staging),
            "peak_spill_allocated_bytes": allocated_bytes(self.spill),
            "process_peak_rss_kib": int(
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            ),
            "details": details or {},
        }
        self.phases.append(row)
        print(
            f"E5 phase pass: {phase} wall={row['wall_seconds']:.3f}s "
            f"cpu={row['cpu_seconds']:.3f}s",
            flush=True,
        )
        self._write()

    def finish(
        self, *, status: str = "pass", details: dict[str, Any] | None = None
    ) -> None:
        if self.active is None:
            return
        if self._stop is not None:
            self._stop.set()
        if self._sampler is not None:
            self._sampler.join()
        self._sample_once()
        finished = time.monotonic()
        cpu = process_cpu_seconds()
        row = {
            "phase": self.active["phase"],
            "source_id": self.active["source_id"],
            "started_at": self.active["started_at"],
            "finished_at": utc_now(),
            "status": status,
            "wall_seconds": round(finished - self.active["start_monotonic"], 6),
            "cpu_seconds": round(cpu - self.active["start_cpu_seconds"], 6),
            "staging_allocated_bytes": allocated_bytes(self.staging),
            "spill_allocated_bytes": allocated_bytes(self.spill),
            **self._peaks,
            "details": details or {},
        }
        self.phases.append(row)
        print(
            f"E5 phase {status}: {row['phase']} wall={row['wall_seconds']:.3f}s "
            f"cpu={row['cpu_seconds']:.3f}s",
            flush=True,
        )
        self.active = None
        self._stop = None
        self._sampler = None
        self._write("failed" if status == "fail" else "in_progress")

    def complete(self, status: str) -> dict[str, Any]:
        self.finish(status="fail" if status == "failed" else "pass")
        report = self._snapshot(status)
        atomic_json(self.report_path, report)
        return report


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def file_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


class FileHashAttestor:
    """Cache byte hashes only while an input's full stat identity is unchanged."""

    def __init__(self) -> None:
        self._cache: dict[Path, tuple[tuple[int, ...], str]] = {}
        self._lock = threading.Lock()
        self.hash_count = 0

    @staticmethod
    def signature(path: Path) -> tuple[int, ...]:
        stat = path.stat()
        return (
            int(stat.st_dev),
            int(stat.st_ino),
            int(stat.st_size),
            int(stat.st_mtime_ns),
            int(stat.st_ctime_ns),
        )

    def digest(self, path: Path) -> str:
        path = path.resolve()
        before = self.signature(path)
        with self._lock:
            cached = self._cache.get(path)
        if cached is not None and cached[0] == before:
            return cached[1]
        digest = file_sha256(path)
        after = self.signature(path)
        if before != after:
            raise ValueError(f"input changed while hashing: {path}")
        with self._lock:
            self._cache[path] = (after, digest)
            self.hash_count += 1
        return digest

    def verify(self, path: Path, expected_sha256: str) -> None:
        actual = self.digest(path)
        if actual != expected_sha256:
            raise ValueError(
                f"immutable input checksum changed: {path}:"
                f"expected={expected_sha256}:actual={actual}"
            )


def verify_e4_member_inputs(
    *,
    state_dir: Path,
    sources: list[dict[str, Any]],
    members: dict[str, dict[str, Any]],
    attestor: FileHashAttestor,
    workers: int,
) -> dict[str, int]:
    unique: dict[Path, tuple[Path, dict[str, Any]]] = {}
    for source in sources:
        member = members[str(source["source_id"])]
        artifact = (state_dir / str(member["artifact_path"])).resolve()
        database = artifact / str(member["database"])
        unique[database] = (artifact, member)

    def verify(item: tuple[Path, tuple[Path, dict[str, Any]]]) -> int:
        database, (artifact, member) = item
        attestor.verify(artifact / "manifest.json", str(member["manifest_sha256"]))
        if database.stat().st_size != int(member["database_bytes"]):
            raise ValueError(
                f"E4 member database size changed: {member['source_ids']}"
            )
        attestor.verify(database, str(member["database_sha256"]))
        return int(member["database_bytes"])

    worker_count = max(1, min(int(workers), len(unique)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        verified_bytes = sum(executor.map(verify, sorted(unique.items())))
    return {
        "members": len(unique),
        "files": len(unique) * 2,
        "database_bytes": verified_bytes,
        "workers": worker_count,
        "byte_hashes": attestor.hash_count,
    }


def stable_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def sql_identifier(value: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier in selection policy: {value!r}")
    return f'"{value}"'


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def json_sql_literal(value: Any) -> str:
    return f"{sql_literal(json.dumps(value, ensure_ascii=False, sort_keys=True))}::JSON"


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def release_set_paths(state_dir: Path, policy: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    release_set_id = str(policy["evidence_release_set_id"])
    root = state_dir / "derived/evidence_lake_v2/scientific_evidence_sets"
    manifest_path = root / release_set_id / "manifest.json"
    manifest = load_json(manifest_path)
    if manifest.get("release_set_id") != release_set_id:
        raise ValueError("selection policy/release-set identity mismatch")
    if manifest.get("release_set_sha256") != policy.get("evidence_release_set_sha256"):
        raise ValueError("selection policy/release-set content hash mismatch")
    if manifest.get("status") != "pass":
        raise ValueError("selected E4 release set is not pass")
    return manifest_path, manifest


def member_by_source(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for member in manifest.get("members") or []:
        for source_id in member.get("source_ids") or []:
            if source_id in result:
                raise ValueError(f"release set repeats source: {source_id}")
            result[source_id] = member
    return result


def validate_policy(policy: dict[str, Any], release_manifest: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.selected_fact_policy.v1":
        raise ValueError("unsupported selected-fact policy schema")
    members = member_by_source(release_manifest)
    seen_programs: set[tuple[str, str, str]] = set()
    seen_groups: set[tuple[str, str, str]] = set()
    for source in policy.get("selection_sources") or []:
        source_id = str(source.get("source_id") or "")
        object_type = str(source.get("object_type") or "")
        binding_scope = str(source.get("binding_scope") or "")
        program_key = (source_id, object_type, binding_scope)
        if not object_type or not binding_scope or program_key in seen_programs:
            raise ValueError(
                "duplicate or incomplete selection program: "
                f"{source_id}:{object_type}:{binding_scope}"
            )
        seen_programs.add(program_key)
        if source_id not in members:
            raise ValueError(f"selection source absent from E4 release set: {source_id}")
        storage = str(source.get("storage") or "eav")
        if storage not in {
            "eav", "coherent_array", "measurement_bundle", "classification",
            "identifier_claim",
        }:
            raise ValueError(f"unsupported selection storage: {source_id}:{storage}")
        if storage == "classification":
            sql_identifier(str(source["classification_evidence_table"]))
        elif storage == "identifier_claim":
            sql_identifier(
                str(source.get("identifier_claim_evidence_table") or "identifier_claim_evidence")
            )
        else:
            sql_identifier(str(source["parameter_set_table"]))
        if storage == "eav":
            sql_identifier(str(source["parameter_evidence_table"]))
        elif storage == "coherent_array":
            sql_identifier(str(source["schema_table"]))
            sql_identifier(str(source.get("values_field") or "values_json"))
        elif storage == "measurement_bundle":
            sql_identifier(str(source["bundle_table"]))
            sql_identifier(str(source.get("bundle_id_field") or "bundle_id"))
            sql_identifier(str(source.get("measurements_field") or "measurements"))
        selection_mode = str(source.get("selection_mode") or "ranked_candidates")
        if selection_mode not in {"ranked_candidates", "authoritative_direct"}:
            raise ValueError(f"unsupported selection mode: {source_id}:{selection_mode}")
        if selection_mode == "authoritative_direct" and storage not in {
            "coherent_array", "measurement_bundle"
        }:
            raise ValueError(
                f"authoritative-direct selection requires coherent arrays or measurement bundles: {source_id}"
            )
        component_scope_policy = str(
            source.get("component_scope_policy") or "require_null"
        )
        if component_scope_policy not in {
            "require_null",
            "same_record_object_identifier",
            "matching_identifier_component_scope",
        }:
            raise ValueError(
                f"unsupported component-scope policy: {source_id}:{component_scope_policy}"
            )
        allowed_claim_scopes = source.get("allowed_claim_scopes")
        if allowed_claim_scopes is not None and (
            not isinstance(allowed_claim_scopes, list)
            or not allowed_claim_scopes
            or any(not str(value).strip() for value in allowed_claim_scopes)
        ):
            raise ValueError(f"invalid allowed claim scopes: {source_id}")
        expected_outcomes = source.get("expected_binding_outcomes")
        if expected_outcomes is not None:
            allowed_statuses = {
                "accepted", "missing", "excluded", "ambiguous",
                "quarantined", "unresolved",
            }
            if (
                not isinstance(expected_outcomes, dict)
                or not set(expected_outcomes).issubset(allowed_statuses)
                or any(
                    not isinstance(value, int) or value < 0
                    for value in expected_outcomes.values()
                )
            ):
                raise ValueError(f"invalid expected binding outcomes: {source_id}")
        expected_selected_facts = source.get("expected_selected_facts")
        if expected_selected_facts is not None and (
            not isinstance(expected_selected_facts, int)
            or expected_selected_facts < 0
        ):
            raise ValueError(f"invalid expected selected facts: {source_id}")
        if source.get("require_unique_source_target") not in {None, True, False}:
            raise ValueError(f"invalid unique-source-target policy: {source_id}")
        channel_dispositions = source.get("channel_dispositions")
        if channel_dispositions is not None:
            channel_names = [
                str(row.get("channel") or "") for row in channel_dispositions
            ] if isinstance(channel_dispositions, list) else []
            if (
                not isinstance(channel_dispositions, list)
                or not channel_dispositions
                or any(
                    not isinstance(row, dict)
                    or row.get("disposition") not in {"selected", "evidence_only"}
                    or not str(row.get("channel") or "").strip()
                    or not str(row.get("reason") or "").strip()
                    for row in channel_dispositions
                )
                or len(channel_names) != len(set(channel_names))
                or "selected" not in {
                    str(row.get("disposition")) for row in channel_dispositions
                }
            ):
                raise ValueError(f"invalid source channel dispositions: {source_id}")
        applicability = source.get("applicability_context")
        if applicability is not None:
            if storage != "eav":
                raise ValueError(
                    f"applicability context currently requires EAV storage: {source_id}"
                )
            sql_identifier(str(applicability["table"]))
            sql_identifier(str(applicability.get("record_id_field") or "source_record_id"))
            sql_identifier(str(applicability.get("evidence_id_field") or "evidence_id"))
            filters = applicability.get("filters") or {}
            if not isinstance(filters, dict):
                raise ValueError(f"invalid applicability filters: {source_id}")
            for field in filters:
                sql_identifier(str(field))
            conditions = applicability.get("conditions") or []
            validate_quality_rule(
                {"quality_conditions": conditions},
                source_id=source_id,
                group_key="applicability_context",
            )
            if any(
                str(condition.get("scope"))
                not in {"applicability_parameters", "applicability_quality"}
                for condition in conditions
            ):
                raise ValueError(
                    f"applicability conditions use an unrelated scope: {source_id}"
                )
        preselection = source.get("parameter_set_preselection")
        if preselection is not None:
            if storage != "eav":
                raise ValueError(
                    f"parameter-set preselection requires EAV storage: {source_id}"
                )
            selection_key = str(preselection.get("selection_key") or "")
            required_quantities = preselection.get("required_quantities") or []
            order_quantity = str(preselection.get("order_quantity") or "")
            direction = str(preselection.get("direction") or "asc")
            if (
                not SAFE_IDENTIFIER.fullmatch(selection_key)
                or not isinstance(required_quantities, list)
                or not required_quantities
                or any(not str(value).strip() for value in required_quantities)
                or not order_quantity
                or direction not in {"asc", "desc"}
                or not str(preselection.get("reason") or "").strip()
            ):
                raise ValueError(f"invalid parameter-set preselection: {source_id}")
            expected_preselected = preselection.get("expected_selected_parameter_sets")
            if expected_preselected is not None and (
                not isinstance(expected_preselected, int)
                or expected_preselected < 0
            ):
                raise ValueError(
                    f"invalid expected parameter-set preselection count: {source_id}"
                )
        binding = source.get("binding") or {}
        if binding.get("strategy") not in {
            "canonical_identifier", "canonical_unique_name",
            "authoritative_release_equivalence",
            "canonical_identifier_consensus",
            "release_identifier_bridge",
        }:
            raise ValueError(f"unsupported selection binding strategy: {binding.get('strategy')}")
        if binding.get("strategy") == "authoritative_release_equivalence":
            equivalence = binding.get("release_equivalence") or {}
            required = {
                "source_release", "canonical_release", "relationship",
                "authority_url", "authority_statement",
            }
            if not required.issubset(equivalence) or equivalence.get("source_list_identical") is not True:
                raise ValueError(
                    f"release-equivalence binding lacks authoritative contract: {source_id}"
                )
            if binding.get("claim_namespace") == binding.get("canonical_namespace"):
                raise ValueError(
                    f"release-equivalence binding must preserve distinct namespaces: {source_id}"
                )
        if binding.get("strategy") == "canonical_identifier_consensus":
            namespaces = binding.get("identifier_namespaces") or []
            if (
                not isinstance(namespaces, list)
                or not namespaces
                or any(
                    not isinstance(row, dict)
                    or not str(row.get("claim_namespace") or "").strip()
                    or not str(row.get("canonical_namespace") or "").strip()
                    or row.get("normalization") != "unsigned_decimal"
                    for row in namespaces
                )
                or len({str(row["claim_namespace"]) for row in namespaces})
                != len(namespaces)
            ):
                raise ValueError(f"invalid consensus identifier binding: {source_id}")
        if binding.get("strategy") == "release_identifier_bridge":
            required = {
                "claim_namespace",
                "bridge_match_namespace",
                "bridge_target_namespace",
                "canonical_namespace",
            }
            if (
                not required.issubset(binding)
                or binding.get("normalization") != "unsigned_decimal"
                or binding.get("claim_namespace")
                != binding.get("bridge_match_namespace")
            ):
                raise ValueError(f"invalid release identifier bridge: {source_id}")
        source_quantities: set[str] = set()
        for group in source.get("quantity_groups") or []:
            group_key = str(group.get("group_key") or "")
            key = (source_id, object_type, group_key)
            if not group_key or key in seen_groups:
                raise ValueError(f"missing or duplicate quantity group: {key}")
            seen_groups.add(key)
            authorities = group.get("authorities") or []
            parameter_set_kinds = group.get("parameter_set_kinds")
            if parameter_set_kinds is not None and (
                storage != "coherent_array"
                or not isinstance(parameter_set_kinds, list)
                or not parameter_set_kinds
                or any(not str(value).strip() for value in parameter_set_kinds)
                or len(parameter_set_kinds) != len(set(parameter_set_kinds))
            ):
                raise ValueError(f"invalid coherent parameter-set kinds: {key}")
            ranks = [int(item["rank"]) for item in authorities]
            if not authorities or any(rank <= 0 for rank in ranks):
                raise ValueError(f"quantity group has no positive authority rules: {key}")
            if selection_mode == "authoritative_direct" and len(authorities) != 1:
                raise ValueError(f"authoritative-direct group requires one authority rule: {key}")
            for rule in authorities:
                validate_quality_rule(rule, source_id=source_id, group_key=group_key)
                if str(source.get("storage") or "eav") == "coherent_array":
                    quality_scopes = {
                        str(item.get("scope") or "")
                        for item in rule.get("quality_conditions") or []
                    }
                    if rule.get("quality_order"):
                        quality_scopes.add(str(rule["quality_order"].get("scope") or ""))
                    if "evidence_quality" in quality_scopes:
                        raise ValueError(
                            "coherent-array quality rules cannot use evidence scope: "
                            f"{source_id}:{group_key}"
                        )
            parameter_set_order = group.get("parameter_set_order")
            if parameter_set_order is not None:
                if storage != "eav":
                    raise ValueError(
                        f"parameter-set value ordering requires EAV storage: {key}"
                    )
                order_quantity = str(parameter_set_order.get("source_quantity") or "")
                direction = str(parameter_set_order.get("direction") or "asc")
                if (
                    order_quantity not in (group.get("quantities") or {})
                    or direction not in {"asc", "desc"}
                ):
                    raise ValueError(f"invalid parameter-set ordering: {key}")
            for source_quantity, selected_spec in (group.get("quantities") or {}).items():
                if source_quantity in source_quantities:
                    raise ValueError(f"source quantity appears in multiple groups: {source_id}:{source_quantity}")
                source_quantities.add(source_quantity)
                if isinstance(selected_spec, dict):
                    selected_quantity = str(selected_spec.get("quantity_key") or "")
                    if storage not in {
                        "coherent_array", "measurement_bundle", "classification",
                        "identifier_claim",
                    } or not selected_quantity:
                        raise ValueError(f"invalid structured quantity mapping: {source_id}:{source_quantity}")
                elif not str(selected_spec):
                    raise ValueError(f"blank selected quantity: {source_id}:{source_quantity}")
        if source.get("require_unique_source_target") is True and len(source_quantities) != 1:
            raise ValueError(
                f"unique-source-target policy requires exactly one source quantity: {source_id}"
            )


def validate_quality_rule(
    rule: dict[str, Any], *, source_id: str, group_key: str
) -> None:
    allowed_scopes = {
        "evidence_quality", "parameter_set_quality", "source_context",
        "applicability_parameters", "applicability_quality",
    }
    allowed_operators = {
        "eq", "ne", "gt", "gte", "lt", "lte", "bitmask_none", "not_null"
    }
    for condition in rule.get("quality_conditions") or []:
        scope = str(condition.get("scope") or "")
        operator = str(condition.get("operator") or "")
        path = str(condition.get("path") or "")
        if scope not in allowed_scopes or operator not in allowed_operators:
            raise ValueError(
                f"invalid quality condition: {source_id}:{group_key}:{scope}:{operator}"
            )
        if not path.startswith("$.") or len(path) > 256:
            raise ValueError(f"invalid quality JSON path: {source_id}:{group_key}:{path!r}")
        if operator != "not_null" and "value" not in condition:
            raise ValueError(
                f"quality condition lacks value: {source_id}:{group_key}:{path}"
            )
        if operator == "bitmask_none" and int(condition.get("value") or 0) < 0:
            raise ValueError(
                f"quality bitmask must be nonnegative: {source_id}:{group_key}:{path}"
            )
    quality_order = rule.get("quality_order")
    if quality_order is not None:
        scope = str(quality_order.get("scope") or "")
        path = str(quality_order.get("path") or "")
        direction = str(quality_order.get("direction") or "desc")
        if scope not in allowed_scopes or not path.startswith("$.") or len(path) > 256:
            raise ValueError(
                f"invalid quality order: {source_id}:{group_key}:{scope}:{path!r}"
            )
        if direction not in {"asc", "desc"}:
            raise ValueError(
                f"invalid quality order direction: {source_id}:{group_key}:{direction}"
            )


def quality_json_expression(
    scope: str, *, source_alias: str, set_alias: str, evidence_alias: str,
    applicability_alias: str = "app",
) -> str:
    if scope == "evidence_quality":
        return f"{evidence_alias}.quality_json"
    if scope == "parameter_set_quality":
        return f"{set_alias}.quality_json"
    if scope == "source_context":
        return f"{source_alias}.source_context_json"
    if scope == "applicability_parameters":
        return f"{applicability_alias}.parameter_set_raw"
    if scope == "applicability_quality":
        return f"{applicability_alias}.quality_json"
    raise ValueError(f"unsupported quality JSON scope: {scope}")


def quality_condition_sql(
    condition: dict[str, Any], *, source_alias: str, set_alias: str,
    evidence_alias: str, applicability_alias: str = "app",
) -> str:
    expression = quality_json_expression(
        str(condition["scope"]), source_alias=source_alias,
        set_alias=set_alias, evidence_alias=evidence_alias,
        applicability_alias=applicability_alias,
    )
    extracted = f"json_extract_string({expression}, {sql_literal(condition['path'])})"
    operator = str(condition["operator"])
    if operator == "not_null":
        return f"{extracted} IS NOT NULL"
    value = condition["value"]
    value_type = str(condition.get("value_type") or "number")
    if operator == "bitmask_none":
        return f"(try_cast({extracted} AS UBIGINT) & {int(value)}) = 0"
    if value_type == "string":
        left = extracted
        right = sql_literal(value)
    elif value_type == "boolean":
        left = f"try_cast({extracted} AS BOOLEAN)"
        right = "TRUE" if bool(value) else "FALSE"
    else:
        left = f"try_cast({extracted} AS DOUBLE)"
        right = repr(float(value))
    sql_operator = {
        "eq": "=", "ne": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
    }[operator]
    return f"{left} {sql_operator} {right}"


def authority_condition(
    rule: dict[str, Any], source_alias: str, set_alias: str,
    evidence_alias: str = "pe",
) -> str:
    conditions: list[str] = []
    for field, alias in (("source_table", source_alias), ("method", set_alias), ("model", set_alias), ("parameter_set_kind", set_alias)):
        if rule.get(field) is not None:
            conditions.append(f"{alias}.{sql_identifier(field)} = {sql_literal(rule[field])}")
    if rule.get("context_field") is not None:
        context_field = str(rule["context_field"])
        if not SAFE_IDENTIFIER.fullmatch(context_field):
            raise ValueError(f"unsafe source context field: {context_field!r}")
        conditions.append(
            f"json_extract_string({source_alias}.source_context_json, {sql_literal('$.' + context_field)}) "
            f"= {sql_literal(rule.get('context_value'))}"
        )
    conditions.extend(
        quality_condition_sql(
            condition, source_alias=source_alias, set_alias=set_alias,
            evidence_alias=evidence_alias,
        )
        for condition in rule.get("quality_conditions") or []
    )
    return " AND ".join(conditions) if conditions else "TRUE"


def quality_score_sql(
    rule: dict[str, Any], *, source_alias: str = "sr", set_alias: str = "ps",
    evidence_alias: str = "pe", applicability_alias: str = "app",
) -> str:
    quality_order = rule.get("quality_order")
    if quality_order is None:
        return "NULL::DOUBLE"
    expression = quality_json_expression(
        str(quality_order["scope"]), source_alias=source_alias,
        set_alias=set_alias, evidence_alias=evidence_alias,
        applicability_alias=applicability_alias,
    )
    extracted = (
        f"try_cast(json_extract_string({expression}, "
        f"{sql_literal(quality_order['path'])}) AS DOUBLE)"
    )
    return f"-({extracted})" if quality_order.get("direction", "desc") == "asc" else extracted


def authority_case(
    group: dict[str, Any], *, value: str, source_alias: str = "sr",
    set_alias: str = "ps", evidence_alias: str = "pe",
) -> str:
    clauses: list[str] = []
    for rule in group["authorities"]:
        condition = authority_condition(
            rule, source_alias, set_alias, evidence_alias
        )
        clauses.append(f"WHEN {condition} THEN {sql_literal(rule[value])}")
    return "CASE " + " ".join(clauses) + " ELSE NULL END"


def quality_score_case(
    group: dict[str, Any], *, source_alias: str = "sr",
    set_alias: str = "ps", evidence_alias: str = "pe",
) -> str:
    clauses: list[str] = []
    for rule in group["authorities"]:
        condition = authority_condition(
            rule, source_alias, set_alias, evidence_alias
        )
        clauses.append(
            f"WHEN {condition} THEN "
            f"{quality_score_sql(rule, source_alias=source_alias, set_alias=set_alias, evidence_alias=evidence_alias)}"
        )
    return "CASE " + " ".join(clauses) + " ELSE NULL END"


def quantity_values(source: dict[str, Any]) -> str:
    rows: list[str] = []
    for group in source["quantity_groups"]:
        for source_quantity, selected_spec in group["quantities"].items():
            selected_quantity = (
                selected_spec["quantity_key"] if isinstance(selected_spec, dict) else selected_spec
            )
            rows.append(
                "(" + ",".join(
                    [sql_literal(source_quantity), sql_literal(selected_quantity), sql_literal(group["group_key"])]
                ) + ")"
            )
    return ",".join(rows)


def coherent_field_specs(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
) -> dict[str, dict[str, Any]]:
    schema_table = sql_identifier(str(source["schema_table"]))
    rows = con.execute(
        f"SELECT DISTINCT schema_json::VARCHAR FROM {source_alias}.{schema_table}"
    ).fetchall()
    if not rows:
        raise ValueError(f"coherent source has no schemas: {source['source_id']}")
    requested = {
        source_quantity
        for group in source["quantity_groups"]
        for source_quantity in group["quantities"]
    }
    requested.update(
        str(spec["uncertainty_field"])
        for group in source["quantity_groups"]
        for spec in group["quantities"].values()
        if isinstance(spec, dict) and spec.get("uncertainty_field")
    )
    resolved: dict[str, dict[str, Any]] = {}
    for (schema_raw,) in rows:
        schema = json.loads(schema_raw)
        fields = {str(field["name"]): field for field in schema.get("fields") or []}
        for field_name in requested & set(fields):
            projection = {
                "position": int(fields[field_name]["position"]),
                "datatype": str(fields[field_name].get("datatype") or ""),
                "unit": str(fields[field_name].get("unit") or ""),
            }
            previous = resolved.get(field_name)
            if previous is not None and previous != projection:
                raise ValueError(
                    f"coherent schema field changed across source tables: "
                    f"{source['source_id']}:{field_name}:{previous}:{projection}"
                )
            resolved[field_name] = projection
    missing = sorted(requested - set(resolved))
    if missing:
        raise ValueError(
            f"coherent source schemas lack requested fields: "
            f"{source['source_id']}:{missing}"
        )
    return resolved


def parameter_set_kind_condition(group: dict[str, Any], set_alias: str) -> str:
    kinds = group.get("parameter_set_kinds")
    if not kinds:
        return "TRUE"
    values = ",".join(sql_literal(str(value)) for value in kinds)
    return f"{set_alias}.parameter_set_kind IN ({values})"


def prepare_applicability_context(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
) -> tuple[str, str, str, str]:
    context = source.get("applicability_context")
    if context is None:
        return "", "TRUE", "no additional applicability predicate", "NULL::VARCHAR"
    table = sql_identifier(str(context["table"]))
    record_id_field = sql_identifier(
        str(context.get("record_id_field") or "source_record_id")
    )
    evidence_id_field = sql_identifier(
        str(context.get("evidence_id_field") or "evidence_id")
    )
    filters = context.get("filters") or {}
    filter_sql = " AND ".join(
        f"{sql_identifier(str(field))}={sql_literal(value)}"
        for field, value in sorted(filters.items())
    ) or "TRUE"
    view_name = sql_identifier(f"applicability_{source_alias}")
    duplicate_count = int(
        con.execute(
            f"SELECT COUNT(*) FROM (SELECT {record_id_field},COUNT(*) n "
            f"FROM {source_alias}.{table} WHERE {filter_sql} "
            f"GROUP BY 1 HAVING COUNT(*)<>1)"
        ).fetchone()[0]
    )
    if duplicate_count:
        raise ValueError(
            f"applicability context is not one row per source record: "
            f"{source['source_id']}:{duplicate_count}"
        )
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {view_name} AS "
        f"SELECT * FROM {source_alias}.{table} WHERE {filter_sql}"
    )
    conditions = context.get("conditions") or []
    configured_conditions = " AND ".join(
        quality_condition_sql(
            condition,
            source_alias="sr",
            set_alias="ps",
            evidence_alias="pe",
            applicability_alias="app",
        )
        for condition in conditions
    ) or "TRUE"
    condition_sql = f"app.{record_id_field} IS NOT NULL AND ({configured_conditions})"
    reason = str(
        context.get("reason")
        or "source record satisfies the configured applicability predicate"
    )
    join_sql = f"LEFT JOIN {view_name} app ON app.{record_id_field}=ps.source_record_id"
    return join_sql, condition_sql, reason, f"app.{evidence_id_field}"


def authority_sql(
    source: dict[str, Any], *, source_alias: str = "sr",
    set_alias: str = "ps", evidence_alias: str = "pe",
) -> tuple[str, str, str]:
    rank_clauses: list[str] = []
    reason_clauses: list[str] = []
    quality_clauses: list[str] = []
    for group in source["quantity_groups"]:
        group_literal = sql_literal(group["group_key"])
        rank_clauses.append(
            f"WHEN q.group_key = {group_literal} THEN "
            f"{authority_case(group, value='rank', source_alias=source_alias, set_alias=set_alias, evidence_alias=evidence_alias)}"
        )
        reason_clauses.append(
            f"WHEN q.group_key = {group_literal} THEN "
            f"{authority_case(group, value='reason', source_alias=source_alias, set_alias=set_alias, evidence_alias=evidence_alias)}"
        )
        quality_clauses.append(
            f"WHEN q.group_key = {group_literal} THEN "
            f"{quality_score_case(group, source_alias=source_alias, set_alias=set_alias, evidence_alias=evidence_alias)}"
        )
    return (
        "CASE " + " ".join(rank_clauses) + " ELSE NULL END",
        "CASE " + " ".join(reason_clauses) + " ELSE NULL END",
        "CASE " + " ".join(quality_clauses) + " ELSE NULL END",
    )


def parameter_set_order_sql(source: dict[str, Any], fallback_sql: str) -> str:
    clauses: list[str] = []
    for group in source["quantity_groups"]:
        order = group.get("parameter_set_order")
        if order is None:
            continue
        source_quantity = sql_literal(str(order["source_quantity"]))
        value = (
            f"MAX(CASE WHEN pe.quantity_key={source_quantity} "
            "THEN pe.normalized_value END) OVER "
            "(PARTITION BY pe.parameter_set_id,q.group_key)"
        )
        if str(order.get("direction") or "asc") == "asc":
            value = f"-({value})"
        clauses.append(
            f"WHEN q.group_key={sql_literal(group['group_key'])} THEN {value}"
        )
    if not clauses:
        return fallback_sql
    return "CASE " + " ".join(clauses) + f" ELSE {fallback_sql} END"


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE evidence_build (
          selected_fact_build_id VARCHAR,
          policy_version VARCHAR,
          policy_sha256 VARCHAR,
          evidence_release_set_id VARCHAR,
          evidence_release_set_sha256 VARCHAR,
          identity_graph_id VARCHAR,
          identity_graph_sha256 VARCHAR,
          canonical_reference_build_id VARCHAR,
          canonical_reference_sha256 VARCHAR,
          compiler_version VARCHAR,
          compiler_sha256 VARCHAR,
          created_at TIMESTAMP,
          status VARCHAR
        );
        CREATE TABLE selection_source_accounting (
          source_id VARCHAR,
          release_id VARCHAR,
          evidence_build_id VARCHAR,
          object_type VARCHAR,
          eligible_binding_subjects BIGINT,
          accepted_current_bindings BIGINT,
          nonaccepted_binding_subjects BIGINT,
          candidate_parameter_sets BIGINT,
          selected_parameter_sets BIGINT,
          selected_facts BIGINT
        );
        CREATE TABLE evidence_object_bindings (
          binding_id VARCHAR,
          source_id VARCHAR,
          release_id VARCHAR,
          evidence_build_id VARCHAR,
          source_record_id VARCHAR,
          binding_subject_kind VARCHAR,
          binding_subject_id VARCHAR,
          binding_scope VARCHAR,
          component_scope VARCHAR,
          identifier_claim_scope VARCHAR,
          applicability_status VARCHAR,
          applicability_reason VARCHAR,
          applicability_evidence_id VARCHAR,
          object_type VARCHAR,
          canonical_object_node_key VARCHAR,
          stable_object_key VARCHAR,
          system_stable_object_key VARCHAR,
          binding_status VARCHAR,
          binding_method VARCHAR,
          binding_reason VARCHAR
        );
        CREATE TABLE source_parameter_set_preselections (
          preselection_id VARCHAR,
          source_id VARCHAR,
          release_id VARCHAR,
          evidence_build_id VARCHAR,
          source_record_id VARCHAR,
          selection_key VARCHAR,
          selected_parameter_set_id VARCHAR,
          selected_model VARCHAR,
          selected_completeness INTEGER,
          selected_uncertainty_count INTEGER,
          selected_order_value DOUBLE,
          candidate_parameter_set_count INTEGER,
          runner_up_parameter_set_id VARCHAR,
          runner_up_model VARCHAR,
          runner_up_order_value DOUBLE,
          applicability_evidence_id VARCHAR,
          selection_reason VARCHAR,
          policy_version VARCHAR
        );
        CREATE TABLE parameter_set_selection_decisions (
          decision_id VARCHAR,
          object_type VARCHAR,
          stable_object_key VARCHAR,
          system_stable_object_key VARCHAR,
          quantity_group VARCHAR,
          selected_parameter_set_id VARCHAR,
          selected_source_record_id VARCHAR,
          selected_source_id VARCHAR,
          selected_release_id VARCHAR,
          selected_evidence_build_id VARCHAR,
          authority_rank INTEGER,
          authority_reason VARCHAR,
          selection_quality_score DOUBLE,
          selected_quantity_count INTEGER,
          selected_uncertainty_count INTEGER,
          candidate_parameter_set_count INTEGER,
          runner_up_parameter_set_id VARCHAR,
          runner_up_authority_rank INTEGER,
          runner_up_quality_score DOUBLE,
          policy_version VARCHAR
        );
        CREATE TABLE selected_facts (
          selected_fact_id VARCHAR,
          object_type VARCHAR,
          stable_object_key VARCHAR,
          system_stable_object_key VARCHAR,
          quantity_group VARCHAR,
          quantity_key VARCHAR,
          value_raw VARCHAR,
          normalized_value DOUBLE,
          normalized_unit VARCHAR,
          value_lower DOUBLE,
          value_upper DOUBLE,
          interval_semantics VARCHAR,
          fact_status VARCHAR,
          evidence_build_id VARCHAR,
          evidence_table VARCHAR,
          evidence_id VARCHAR,
          parameter_set_id VARCHAR,
          source_record_id VARCHAR,
          source_id VARCHAR,
          release_id VARCHAR,
          method VARCHAR,
          model VARCHAR,
          reference_raw VARCHAR,
          selection_decision_id VARCHAR,
          authority_rank INTEGER,
          authority_reason VARCHAR,
          policy_version VARCHAR,
          normalization_version VARCHAR,
          quality_json JSON,
          binding_id VARCHAR
        );
        CREATE TABLE selected_fact_derivations (
          derivation_id VARCHAR,
          output_selected_fact_id VARCHAR,
          stable_object_key VARCHAR,
          quantity_key VARCHAR,
          algorithm_key VARCHAR,
          algorithm_version VARCHAR,
          input_selected_fact_ids_json JSON,
          applicability VARCHAR,
          formula VARCHAR,
          assumptions_json JSON,
          uncertainty_method VARCHAR,
          confidence_tier VARCHAR,
          supersedes_json JSON,
          policy_version VARCHAR
        );
        """
    )


def create_binding(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
    member: dict[str, Any],
    release_id: str,
) -> tuple[int, int]:
    source_id = str(source["source_id"])
    object_type = str(source["object_type"])
    binding = source["binding"]
    storage = str(source.get("storage") or "eav")
    component_field = source.get("component_scope_field")
    component_scope_policy = str(
        source.get("component_scope_policy") or "require_null"
    )
    set_table = (
        sql_identifier(str(source["parameter_set_table"]))
        if storage not in {"classification", "identifier_claim"}
        else None
    )
    if storage == "coherent_array":
        values_field = sql_identifier(str(source.get("values_field") or "values_json"))
        set_id_field = sql_identifier(str(source.get("set_id_field") or "evidence_id"))
        specs = coherent_field_specs(con, source=source, source_alias=source_alias)
        present_groups: list[str] = []
        for group in source["quantity_groups"]:
            group_present = " OR ".join(
                f"json_extract(ps.{values_field}, '$[{specs[name]['position']}]') IS NOT NULL"
                for name in group["quantities"]
            )
            present_groups.append(
                f"(({parameter_set_kind_condition(group, 'ps')}) "
                f"AND ({group_present}))"
            )
        present = " OR ".join(present_groups)
        component_value = (
            f"ps.{sql_identifier(str(component_field))}" if component_field else "NULL::VARCHAR"
        )
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE eligible_{source_alias} AS
            SELECT DISTINCT ps.source_record_id,
                   CASE WHEN {component_value} IS NULL
                        THEN 'source_record' ELSE 'parameter_set' END binding_subject_kind,
                   CASE WHEN {component_value} IS NULL
                        THEN ps.source_record_id ELSE cast(ps.{set_id_field} as varchar) END binding_subject_id,
                   {component_value} component_scope,
                   TRUE applicability_pass,
                   'no additional applicability predicate'::VARCHAR applicability_reason,
                   NULL::VARCHAR applicability_evidence_id
            FROM {source_alias}.{set_table} ps
            WHERE ({present})
            """
        )
    elif storage == "measurement_bundle":
        bundle_table = sql_identifier(str(source["bundle_table"]))
        measurements_field = sql_identifier(str(source.get("measurements_field") or "measurements"))
        source_quantities = sorted(
            str(quantity)
            for group in source["quantity_groups"]
            for quantity in group["quantities"]
        )
        quantity_list = ",".join(sql_literal(value) for value in source_quantities)
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE eligible_{source_alias} AS
            SELECT DISTINCT bundle.source_record_id,
                   'source_record'::VARCHAR binding_subject_kind,
                   bundle.source_record_id binding_subject_id,
                   NULL::VARCHAR component_scope,
                   TRUE applicability_pass,
                   'no additional applicability predicate'::VARCHAR applicability_reason,
                   NULL::VARCHAR applicability_evidence_id
            FROM {source_alias}.{bundle_table} bundle
            CROSS JOIN UNNEST(bundle.{measurements_field}) AS nested(measurement)
            WHERE nested.measurement.quantity_key IN ({quantity_list})
              AND (nested.measurement.normalized_value IS NOT NULL
                   OR NULLIF(TRIM(nested.measurement.value_raw), '') IS NOT NULL)
            """
        )
    elif storage == "classification":
        classification_table = sql_identifier(str(source["classification_evidence_table"]))
        quantity_rows = quantity_values(source)
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE eligible_{source_alias} AS
            WITH quantities(source_quantity, selected_quantity, group_key) AS (VALUES {quantity_rows})
            SELECT DISTINCT ce.source_record_id,
                   'classification_evidence'::VARCHAR binding_subject_kind,
                   ce.evidence_id binding_subject_id,
                   ce.component_scope,
                   TRUE applicability_pass,
                   'no additional applicability predicate'::VARCHAR applicability_reason,
                   NULL::VARCHAR applicability_evidence_id
            FROM {source_alias}.{classification_table} ce
            JOIN quantities q ON q.source_quantity = ce.classification_scheme
            WHERE NULLIF(TRIM(ce.classification_raw), '') IS NOT NULL
            """
        )
    elif storage == "identifier_claim":
        identifier_table = sql_identifier(
            str(source.get("identifier_claim_evidence_table") or "identifier_claim_evidence")
        )
        quantity_rows = quantity_values(source)
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE eligible_{source_alias} AS
            WITH quantities(source_quantity, selected_quantity, group_key) AS (VALUES {quantity_rows})
            SELECT DISTINCT ic.source_record_id,
                   'identifier_claim_evidence'::VARCHAR binding_subject_kind,
                   ic.evidence_id binding_subject_id,
                   ic.component_scope,
                   TRUE applicability_pass,
                   'no additional applicability predicate'::VARCHAR applicability_reason,
                   NULL::VARCHAR applicability_evidence_id
            FROM {source_alias}.{identifier_table} ic
            JOIN quantities q ON q.source_quantity = ic.namespace
            WHERE NULLIF(TRIM(ic.identifier_raw), '') IS NOT NULL
            """
        )
    else:
        evidence_table = sql_identifier(str(source["parameter_evidence_table"]))
        quantity_rows = quantity_values(source)
        (
            applicability_join,
            applicability_condition,
            applicability_reason,
            applicability_evidence_id,
        ) = (
            prepare_applicability_context(
                con, source=source, source_alias=source_alias
            )
        )
        component_value = (
            f"ps.{sql_identifier(str(component_field))}" if component_field else "NULL::VARCHAR"
        )
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE eligible_{source_alias} AS
            WITH quantities(source_quantity, selected_quantity, group_key) AS (VALUES {quantity_rows})
            SELECT DISTINCT pe.source_record_id,
                   CASE WHEN {component_value} IS NULL
                        THEN 'source_record' ELSE 'parameter_set' END binding_subject_kind,
                   CASE WHEN {component_value} IS NULL
                        THEN pe.source_record_id ELSE pe.parameter_set_id END binding_subject_id,
                   {component_value} component_scope,
                   coalesce(({applicability_condition}), FALSE) applicability_pass,
                   {sql_literal(applicability_reason)}::VARCHAR applicability_reason,
                   {applicability_evidence_id} applicability_evidence_id
            FROM {source_alias}.{evidence_table} pe
            JOIN quantities q ON q.source_quantity = pe.quantity_key
            JOIN {source_alias}.{set_table} ps ON ps.parameter_set_id = pe.parameter_set_id
            {applicability_join}
            WHERE (pe.normalized_value IS NOT NULL OR NULLIF(TRIM(pe.value_raw), '') IS NOT NULL)
            """
        )
    eligible_count = int(con.execute(f"SELECT COUNT(*) FROM eligible_{source_alias}").fetchone()[0])
    strategy = binding["strategy"]
    if strategy == "canonical_identifier_consensus":
        consensus_namespaces = binding["identifier_namespaces"]
        binding_claim_namespaces = [
            str(row["claim_namespace"]) for row in consensus_namespaces
        ]
    else:
        binding_claim_namespaces = [str(binding["claim_namespace"])]
    claim_namespace_predicate = (
        "ic.namespace IN ("
        + ",".join(sql_literal(value) for value in binding_claim_namespaces)
        + ")"
    )
    allowed_claim_scopes = source.get("allowed_claim_scopes") or []
    claim_scope_filter = (
        " AND ic.claim_scope IN ("
        + ",".join(sql_literal(str(value)) for value in allowed_claim_scopes)
        + ")"
        if allowed_claim_scopes
        else ""
    )
    if component_scope_policy == "matching_identifier_component_scope":
        component_claim_filter = (
            " AND (e.component_scope IS NULL OR ic.component_scope = e.component_scope)"
        )
    else:
        component_claim_filter = ""
    scope_candidate_filter = (
        " AND e.applicability_pass AND e.component_scope IS NULL"
        if component_scope_policy == "require_null"
        else " AND e.applicability_pass"
    )
    if strategy in {"canonical_identifier", "authoritative_release_equivalence"}:
        normalization = binding.get("normalization")
        if normalization != "unsigned_decimal":
            raise ValueError(f"unsupported canonical identifier normalization: {normalization}")
        normalized = "regexp_extract(ic.identifier_normalized, '([0-9]+)$', 1)"
        canonical_namespace = sql_literal(binding["canonical_namespace"])
        if strategy == "authoritative_release_equivalence":
            equivalence = binding["release_equivalence"]
            binding_method = (
                "authoritative_release_equivalence:"
                f"{equivalence['source_release']}->{equivalence['canonical_release']}"
            )
        else:
            binding_method = "canonical_identifier_graph"
        candidate_sql = f"""
          SELECT e.source_record_id, e.binding_subject_kind, e.binding_subject_id,
                 b.object_node_key, b.stable_object_key, b.system_stable_object_key,
                 o.object_type target_object_type, ic.claim_scope identifier_claim_scope,
                 {sql_literal(binding_method)} AS binding_method,
                 0::BIGINT bridge_target_claim_count
          FROM eligible_{source_alias} e
          JOIN {source_alias}.identifier_claim_evidence ic
            ON ic.source_record_id = e.source_record_id
            AND ic.namespace = {sql_literal(binding['claim_namespace'])}
            {claim_scope_filter} {component_claim_filter}
          JOIN identity.canonical_identifier_bindings b
            ON b.namespace = {canonical_namespace} AND b.id_value_norm = {normalized}
          JOIN identity.canonical_object_nodes o ON o.object_node_key = b.object_node_key
          WHERE TRUE {scope_candidate_filter}
        """
    elif strategy == "canonical_identifier_consensus":
        namespace_rows = ",".join(
            "(" + ",".join(
                [
                    sql_literal(str(row["claim_namespace"])),
                    sql_literal(str(row["canonical_namespace"])),
                ]
            ) + ")"
            for row in consensus_namespaces
        )
        candidate_sql = f"""
          WITH namespace_map(claim_namespace, canonical_namespace) AS (
            VALUES {namespace_rows}
          )
          SELECT e.source_record_id, e.binding_subject_kind, e.binding_subject_id,
                 b.object_node_key, b.stable_object_key, b.system_stable_object_key,
                 o.object_type target_object_type, ic.claim_scope identifier_claim_scope,
                 'canonical_identifier_consensus' AS binding_method,
                 0::BIGINT bridge_target_claim_count
          FROM eligible_{source_alias} e
          JOIN {source_alias}.identifier_claim_evidence ic
            ON ic.source_record_id = e.source_record_id
            {claim_scope_filter} {component_claim_filter}
          JOIN namespace_map m ON m.claim_namespace = ic.namespace
          JOIN identity.canonical_identifier_bindings b
            ON b.namespace = m.canonical_namespace
           AND b.id_value_norm = regexp_extract(ic.identifier_normalized, '([0-9]+)$', 1)
          JOIN identity.canonical_object_nodes o ON o.object_node_key = b.object_node_key
          WHERE TRUE {scope_candidate_filter}
        """
    elif strategy == "release_identifier_bridge":
        candidate_sql = f"""
          WITH bridge_target_stats AS (
            SELECT bridge_match.identifier_normalized bridge_match_identifier,
                   COUNT(DISTINCT bridge_target.identifier_normalized)::BIGINT
                     bridge_target_claim_count
            FROM {source_alias}.identifier_claim_evidence bridge_match
            JOIN {source_alias}.identifier_claim_evidence bridge_target
              ON bridge_target.source_record_id = bridge_match.source_record_id
             AND bridge_target.namespace = {sql_literal(binding['bridge_target_namespace'])}
            WHERE bridge_match.namespace = {sql_literal(binding['bridge_match_namespace'])}
            GROUP BY bridge_match.identifier_normalized
          )
          SELECT e.source_record_id, e.binding_subject_kind, e.binding_subject_id,
                 b.object_node_key, b.stable_object_key, b.system_stable_object_key,
                 o.object_type target_object_type,
                 source_claim.claim_scope identifier_claim_scope,
                 'release_identifier_bridge' AS binding_method,
                 bridge_stats.bridge_target_claim_count
          FROM eligible_{source_alias} e
          JOIN {source_alias}.identifier_claim_evidence source_claim
            ON source_claim.source_record_id = e.source_record_id
           AND source_claim.namespace = {sql_literal(binding['claim_namespace'])}
          JOIN {source_alias}.identifier_claim_evidence bridge_match
            ON bridge_match.namespace = {sql_literal(binding['bridge_match_namespace'])}
           AND bridge_match.identifier_normalized = source_claim.identifier_normalized
          JOIN {source_alias}.identifier_claim_evidence bridge_target
            ON bridge_target.source_record_id = bridge_match.source_record_id
           AND bridge_target.namespace = {sql_literal(binding['bridge_target_namespace'])}
          JOIN bridge_target_stats bridge_stats
            ON bridge_stats.bridge_match_identifier = source_claim.identifier_normalized
          JOIN identity.canonical_identifier_bindings b
            ON b.namespace = {sql_literal(binding['canonical_namespace'])}
           AND b.id_value_norm = regexp_extract(
                 bridge_target.identifier_normalized, '([0-9]+)$', 1
               )
          JOIN identity.canonical_object_nodes o ON o.object_node_key = b.object_node_key
          WHERE TRUE {scope_candidate_filter}
        """
    else:
        if binding.get("normalization") != "spacegate_public_name_v1":
            raise ValueError("unsupported canonical name normalization")
        canonical_table = sql_identifier(str(binding["canonical_table"]))
        canonical_name_field = sql_identifier(str(binding["canonical_name_field"]))
        candidate_sql = f"""
          WITH canonical_names AS (
            SELECT {canonical_name_field} AS name_norm, stable_object_key,
                   system_id, COUNT(*) OVER (PARTITION BY {canonical_name_field}) AS name_count
            FROM core.{canonical_table}
          )
          SELECT e.source_record_id, e.binding_subject_kind, e.binding_subject_id,
                 o.object_node_key, n.stable_object_key, o.system_stable_object_key,
                 o.object_type target_object_type, ic.claim_scope identifier_claim_scope,
                 'canonical_unique_name' AS binding_method,
                 0::BIGINT bridge_target_claim_count
          FROM eligible_{source_alias} e
          JOIN {source_alias}.identifier_claim_evidence ic
            ON ic.source_record_id = e.source_record_id
            AND ic.namespace = {sql_literal(binding['claim_namespace'])}
            {claim_scope_filter} {component_claim_filter}
          JOIN canonical_names n
            ON n.name_count = 1 AND n.name_norm = TRIM(regexp_replace(lower(ic.identifier_normalized), '[^a-z0-9]+', ' ', 'g'))
          JOIN identity.canonical_object_nodes o ON o.stable_object_key = n.stable_object_key
          WHERE TRUE {scope_candidate_filter}
        """
    con.execute(f"CREATE OR REPLACE TEMP TABLE binding_candidates_{source_alias} AS {candidate_sql}")
    if strategy == "release_identifier_bridge":
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE binding_bridge_stats_{source_alias} AS
            SELECT e.binding_subject_kind, e.binding_subject_id,
                   COUNT(DISTINCT bridge_target.identifier_normalized)::BIGINT
                     bridge_target_claim_count
            FROM eligible_{source_alias} e
            JOIN {source_alias}.identifier_claim_evidence source_claim
              ON source_claim.source_record_id = e.source_record_id
             AND source_claim.namespace = {sql_literal(binding['claim_namespace'])}
            JOIN {source_alias}.identifier_claim_evidence bridge_match
              ON bridge_match.namespace = {sql_literal(binding['bridge_match_namespace'])}
             AND bridge_match.identifier_normalized = source_claim.identifier_normalized
            JOIN {source_alias}.identifier_claim_evidence bridge_target
              ON bridge_target.source_record_id = bridge_match.source_record_id
             AND bridge_target.namespace = {sql_literal(binding['bridge_target_namespace'])}
            GROUP BY e.binding_subject_kind, e.binding_subject_id
            """
        )
        bridge_stats_join = (
            f"LEFT JOIN binding_bridge_stats_{source_alias} bs "
            "USING (binding_subject_kind, binding_subject_id)"
        )
        bridge_target_count_sql = (
            "coalesce(bs.bridge_target_claim_count, "
            "r.bridge_target_claim_count, 0)"
        )
    else:
        bridge_stats_join = ""
        bridge_target_count_sql = "coalesce(r.bridge_target_claim_count, 0)"
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE binding_claim_stats_{source_alias} AS
        SELECT e.binding_subject_kind, e.binding_subject_id,
               COUNT(ic.source_record_id)::BIGINT claim_count,
               MIN(ic.claim_scope) identifier_claim_scope
        FROM eligible_{source_alias} e
        LEFT JOIN {source_alias}.identifier_claim_evidence ic
          ON ic.source_record_id=e.source_record_id AND {claim_namespace_predicate}
          {claim_scope_filter} {component_claim_filter}
        GROUP BY e.binding_subject_kind, e.binding_subject_id
        """
    )
    if strategy == "authoritative_release_equivalence":
        configured_method = (
            "authoritative_release_equivalence:"
            f"{binding['release_equivalence']['source_release']}"
            f"->{binding['release_equivalence']['canonical_release']}"
        )
        accepted_reason = (
            "authoritative identical release source list; unique current canonical target"
        )
    elif strategy == "canonical_identifier":
        configured_method = "canonical_identifier_graph"
        accepted_reason = "unique current canonical target"
    elif strategy == "canonical_identifier_consensus":
        configured_method = "canonical_identifier_consensus"
        accepted_reason = "all matched source identifiers converge on one current canonical target"
    elif strategy == "release_identifier_bridge":
        configured_method = "release_identifier_bridge"
        accepted_reason = "unique same-release bridge target and unique current canonical target"
    else:
        configured_method = "canonical_unique_name"
        accepted_reason = "unique normalized current canonical name"
    con.execute(
        f"""
        INSERT INTO evidence_object_bindings
        WITH resolved AS (
          SELECT binding_subject_kind, binding_subject_id,
                 MIN(object_node_key) FILTER (
                   WHERE target_object_type={sql_literal(object_type)}
                 ) AS object_node_key,
                 MIN(stable_object_key) FILTER (
                   WHERE target_object_type={sql_literal(object_type)}
                 ) AS stable_object_key,
                 MIN(system_stable_object_key) FILTER (
                   WHERE target_object_type={sql_literal(object_type)}
                 ) AS system_stable_object_key,
                 MIN(binding_method) AS binding_method,
                 MIN(identifier_claim_scope) identifier_claim_scope,
                 MAX(bridge_target_claim_count)::BIGINT bridge_target_claim_count,
                 COUNT(DISTINCT stable_object_key) AS target_count,
                 COUNT(DISTINCT stable_object_key) FILTER (
                   WHERE target_object_type={sql_literal(object_type)}
                 ) AS compatible_target_count
          FROM binding_candidates_{source_alias}
          GROUP BY binding_subject_kind, binding_subject_id
        )
        SELECT sha256(concat_ws('|', {sql_literal(source_id)}, e.binding_subject_kind,
                                    e.binding_subject_id, {sql_literal(object_type)})),
               {sql_literal(source_id)}, {sql_literal(release_id)}, {sql_literal(member['build_id'])},
               e.source_record_id, e.binding_subject_kind, e.binding_subject_id,
               {sql_literal(source['binding_scope'])}, e.component_scope,
               coalesce(r.identifier_claim_scope, cs.identifier_claim_scope),
               CASE WHEN e.applicability_pass THEN 'applicable' ELSE 'inapplicable' END,
               e.applicability_reason,
               e.applicability_evidence_id,
               {sql_literal(object_type)},
               CASE WHEN r.target_count = 1 AND r.compatible_target_count = 1
                          AND {bridge_target_count_sql} <= 1
                    THEN r.object_node_key END,
               CASE WHEN r.target_count = 1 AND r.compatible_target_count = 1
                          AND {bridge_target_count_sql} <= 1
                    THEN r.stable_object_key END,
               CASE WHEN r.target_count = 1 AND r.compatible_target_count = 1
                          AND {bridge_target_count_sql} <= 1
                    THEN r.system_stable_object_key END,
               CASE
                 WHEN NOT e.applicability_pass THEN 'excluded'
                 WHEN e.component_scope IS NOT NULL
                  AND {sql_literal(component_scope_policy)} = 'require_null' THEN 'unresolved'
                 WHEN e.component_scope IS NOT NULL AND cs.claim_count = 0
                   THEN 'unresolved'
                 WHEN {bridge_target_count_sql} > 1 THEN 'ambiguous'
                 WHEN r.target_count = 1 AND r.compatible_target_count = 1 THEN 'accepted'
                 WHEN r.target_count > 1 THEN 'ambiguous'
                 WHEN r.target_count = 1 AND r.compatible_target_count = 0 THEN 'excluded'
                 ELSE 'missing'
               END,
               COALESCE(r.binding_method, {sql_literal(configured_method)}),
               CASE
                 WHEN NOT e.applicability_pass
                   THEN 'source evidence fails the configured applicability predicate: '
                        || e.applicability_reason
                 WHEN e.component_scope IS NOT NULL
                  AND {sql_literal(component_scope_policy)} = 'require_null'
                   THEN 'component scope requires an explicit compatible binding policy'
                 WHEN {bridge_target_count_sql} > 1
                   THEN 'same-release bridge has multiple target identifier claims'
                 WHEN r.target_count = 1 AND r.compatible_target_count = 1 THEN {sql_literal(accepted_reason)}
                 WHEN r.target_count > 1 THEN 'multiple current canonical targets'
                 WHEN r.target_count = 1 AND r.compatible_target_count = 0
                   THEN 'unique identifier target has an incompatible canonical object type'
                 WHEN cs.claim_count = 0 AND e.component_scope IS NOT NULL
                   THEN 'no scope-compatible source identifier claim'
                 WHEN cs.claim_count = 0 THEN 'no source identifier claim'
                 ELSE 'source identifier absent from current canonical graph'
               END
        FROM eligible_{source_alias} e
        LEFT JOIN resolved r USING (binding_subject_kind, binding_subject_id)
        LEFT JOIN binding_claim_stats_{source_alias} cs
          USING (binding_subject_kind, binding_subject_id)
        {bridge_stats_join}
        """
    )
    if source.get("require_unique_source_target") is True:
        con.execute(
            """
            UPDATE evidence_object_bindings AS binding
            SET canonical_object_node_key=NULL,
                stable_object_key=NULL,
                system_stable_object_key=NULL,
                binding_status='ambiguous',
                binding_reason='multiple source subjects for this quantity converge on one canonical target'
            WHERE binding.source_id=?
              AND binding.binding_status='accepted'
              AND (binding.object_type, binding.stable_object_key) IN (
                SELECT object_type, stable_object_key
                FROM evidence_object_bindings
                WHERE source_id=? AND binding_status='accepted'
                GROUP BY object_type, stable_object_key
                HAVING COUNT(*) > 1
              )
            """,
            [source_id, source_id],
        )
    accepted = int(
        con.execute(
            "SELECT COUNT(*) FROM evidence_object_bindings "
            "WHERE source_id = ? AND object_type = ? "
            "AND binding_status = 'accepted'",
            [source_id, object_type],
        ).fetchone()[0]
    )
    return eligible_count, accepted


def create_parameter_set_preselection(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
    member: dict[str, Any],
    release_id: str,
) -> int:
    preselection = source.get("parameter_set_preselection")
    if preselection is None:
        return 0
    set_table = sql_identifier(str(source["parameter_set_table"]))
    evidence_table = sql_identifier(str(source["parameter_evidence_table"]))
    selection_key = str(preselection["selection_key"])
    required = sorted(str(value) for value in preselection["required_quantities"])
    required_sql = ",".join(sql_literal(value) for value in required)
    order_quantity = str(preselection["order_quantity"])
    direction = str(preselection.get("direction") or "asc")
    order_direction = "ASC" if direction == "asc" else "DESC"
    minimum_completeness = int(
        preselection.get("minimum_required_quantities") or len(required)
    )
    if minimum_completeness < 1 or minimum_completeness > len(required):
        raise ValueError(
            f"invalid preselection completeness floor: {source['source_id']}"
        )
    temp_name = sql_identifier(f"preselected_{source_alias}")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {temp_name} AS
        WITH set_stats AS (
          SELECT ps.source_record_id, ps.parameter_set_id, ps.model,
                 COUNT(DISTINCT pe.quantity_key) FILTER (
                   WHERE pe.quantity_key IN ({required_sql})
                     AND (pe.normalized_value IS NOT NULL
                       OR NULLIF(TRIM(pe.value_raw), '') IS NOT NULL)
                 )::INTEGER completeness,
                 COUNT(DISTINCT pe.quantity_key) FILTER (
                   WHERE pe.quantity_key IN ({required_sql})
                     AND (pe.uncertainty_lower IS NOT NULL
                       OR pe.uncertainty_upper IS NOT NULL)
                 )::INTEGER uncertainty_count,
                 MAX(pe.normalized_value) FILTER (
                   WHERE pe.quantity_key={sql_literal(order_quantity)}
                 ) order_value,
                 MIN(b.applicability_evidence_id) applicability_evidence_id
          FROM {source_alias}.{set_table} ps
          JOIN {source_alias}.{evidence_table} pe
            ON pe.parameter_set_id=ps.parameter_set_id
          JOIN evidence_object_bindings b
            ON b.source_id={sql_literal(source['source_id'])}
           AND b.object_type={sql_literal(source['object_type'])}
           AND b.binding_subject_kind='source_record'
           AND b.source_record_id=ps.source_record_id
           AND b.binding_status='accepted'
          WHERE pe.quantity_key IN ({required_sql},{sql_literal(order_quantity)})
          GROUP BY ps.source_record_id,ps.parameter_set_id,ps.model
        ), eligible AS (
          SELECT * FROM set_stats
          WHERE completeness >= {minimum_completeness} AND order_value IS NOT NULL
        ), ranked AS (
          SELECT *,
                 ROW_NUMBER() OVER (
                   PARTITION BY source_record_id
                   ORDER BY completeness DESC,uncertainty_count DESC,
                            order_value {order_direction} NULLS LAST,
                            model,parameter_set_id
                 ) selection_rank,
                 COUNT(*) OVER (PARTITION BY source_record_id)::INTEGER candidate_count,
                 LEAD(parameter_set_id) OVER (
                   PARTITION BY source_record_id
                   ORDER BY completeness DESC,uncertainty_count DESC,
                            order_value {order_direction} NULLS LAST,
                            model,parameter_set_id
                 ) runner_up_parameter_set_id,
                 LEAD(model) OVER (
                   PARTITION BY source_record_id
                   ORDER BY completeness DESC,uncertainty_count DESC,
                            order_value {order_direction} NULLS LAST,
                            model,parameter_set_id
                 ) runner_up_model,
                 LEAD(order_value) OVER (
                   PARTITION BY source_record_id
                   ORDER BY completeness DESC,uncertainty_count DESC,
                            order_value {order_direction} NULLS LAST,
                            model,parameter_set_id
                 ) runner_up_order_value
          FROM eligible
        )
        SELECT * FROM ranked WHERE selection_rank=1
        """
    )
    con.execute(
        f"""
        INSERT INTO source_parameter_set_preselections
        SELECT sha256(concat_ws('|',{sql_literal(source['source_id'])},
                                source_record_id,{sql_literal(selection_key)},
                                parameter_set_id,{sql_literal(source['_policy_version'])})),
               {sql_literal(source['source_id'])}, {sql_literal(release_id)},
               {sql_literal(member['build_id'])}, source_record_id,
               {sql_literal(selection_key)}, parameter_set_id, model,
               completeness, uncertainty_count, order_value, candidate_count,
               runner_up_parameter_set_id, runner_up_model, runner_up_order_value,
               applicability_evidence_id, {sql_literal(preselection['reason'])},
               {sql_literal(source['_policy_version'])}
        FROM {temp_name}
        """
    )
    return int(con.execute(f"SELECT COUNT(*) FROM {temp_name}").fetchone()[0])


def insert_candidates(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
    member: dict[str, Any],
    release_id: str,
) -> None:
    if str(source.get("storage") or "eav") == "classification":
        insert_classification_candidates(
            con,
            source=source,
            source_alias=source_alias,
            member=member,
            release_id=release_id,
        )
        return
    if str(source.get("storage") or "eav") == "identifier_claim":
        insert_identifier_claim_candidates(
            con,
            source=source,
            source_alias=source_alias,
            member=member,
            release_id=release_id,
        )
        return
    if str(source.get("storage") or "eav") == "measurement_bundle":
        insert_measurement_bundle_direct(
            con,
            source=source,
            source_alias=source_alias,
            member=member,
            release_id=release_id,
        )
        return
    if str(source.get("selection_mode") or "ranked_candidates") == "authoritative_direct":
        insert_coherent_direct(
            con,
            source=source,
            source_alias=source_alias,
            member=member,
            release_id=release_id,
        )
        return
    if str(source.get("storage") or "eav") == "coherent_array":
        insert_coherent_candidates(
            con,
            source=source,
            source_alias=source_alias,
            member=member,
            release_id=release_id,
        )
        return
    set_table = sql_identifier(str(source["parameter_set_table"]))
    evidence_table = sql_identifier(str(source["parameter_evidence_table"]))
    quantity_rows = quantity_values(source)
    rank_sql, reason_sql, quality_sql = authority_sql(source)
    quality_sql = parameter_set_order_sql(source, quality_sql)
    component_filter = ""
    if source.get("component_scope_field"):
        component_filter = f"AND ps.{sql_identifier(str(source['component_scope_field']))} IS NULL"
    preselection_join = ""
    if source.get("parameter_set_preselection") is not None:
        preselection_join = (
            f"JOIN {sql_identifier(f'preselected_{source_alias}')} pre "
            "ON pre.parameter_set_id=pe.parameter_set_id"
        )
    con.execute(
        f"""
        INSERT INTO fact_candidates
        WITH quantities(source_quantity, selected_quantity, group_key) AS (VALUES {quantity_rows}),
        candidates AS (
          SELECT b.object_type, b.stable_object_key, b.system_stable_object_key,
                 b.binding_id,
                 q.group_key, q.selected_quantity AS quantity_key,
                 pe.value_raw, pe.normalized_value, pe.normalized_unit,
                 pe.uncertainty_lower, pe.uncertainty_upper, pe.bound_semantics,
                 pe.evidence_id, pe.parameter_set_id, pe.source_record_id,
                 ps.method, ps.model, pe.reference_raw,
                 pe.normalization_version, pe.quality_json,
                 {rank_sql} AS authority_rank,
                 {reason_sql} AS authority_reason,
                 {quality_sql} AS selection_quality_score
          FROM {source_alias}.{evidence_table} pe
          JOIN quantities q ON q.source_quantity = pe.quantity_key
          JOIN {source_alias}.{set_table} ps ON ps.parameter_set_id = pe.parameter_set_id
          {preselection_join}
          JOIN {source_alias}.source_records sr ON sr.source_record_id = pe.source_record_id
          JOIN evidence_object_bindings b
            ON b.source_id = {sql_literal(source['source_id'])}
           AND b.object_type = {sql_literal(source['object_type'])}
           AND b.binding_subject_kind = 'source_record'
           AND b.source_record_id = pe.source_record_id
           AND b.binding_status = 'accepted'
          WHERE (pe.normalized_value IS NOT NULL OR NULLIF(TRIM(pe.value_raw), '') IS NOT NULL)
            {component_filter}
        )
        SELECT object_type, stable_object_key, system_stable_object_key,
               group_key, quantity_key, value_raw, normalized_value, normalized_unit,
               uncertainty_lower, uncertainty_upper, bound_semantics,
               {sql_literal(member['build_id'])}, {sql_literal(evidence_table.strip(chr(34)))},
               evidence_id, parameter_set_id, source_record_id,
               {sql_literal(source['source_id'])}, {sql_literal(release_id)},
               method, model, reference_raw, authority_rank, authority_reason,
               selection_quality_score, normalization_version, quality_json,
               binding_id
        FROM candidates WHERE authority_rank IS NOT NULL
        """
    )


def insert_classification_candidates(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
    member: dict[str, Any],
    release_id: str,
) -> None:
    table_name = str(source["classification_evidence_table"])
    evidence_table = sql_identifier(table_name)
    quantity_rows = quantity_values(source)
    rank_sql, reason_sql, quality_sql = authority_sql(
        source, source_alias="sr", set_alias="ce", evidence_alias="ce"
    )
    con.execute(
        f"""
        INSERT INTO fact_candidates
        WITH quantities(source_quantity, selected_quantity, group_key) AS (
          VALUES {quantity_rows}
        ), candidates AS (
          SELECT b.object_type, b.stable_object_key, b.system_stable_object_key,
                 b.binding_id,
                 q.group_key, q.selected_quantity quantity_key,
                 ce.classification_raw value_raw, NULL::DOUBLE normalized_value,
                 NULL::VARCHAR normalized_unit, NULL::DOUBLE uncertainty_lower,
                 NULL::DOUBLE uncertainty_upper, NULL::VARCHAR bound_semantics,
                 ce.evidence_id, ce.evidence_id parameter_set_id,
                 ce.source_record_id, ce.method, ce.model, ce.reference_raw,
                 'source_native_classification_v1'::VARCHAR normalization_version,
                 ce.quality_json,
                 {rank_sql} authority_rank,
                 {reason_sql} authority_reason,
                 {quality_sql} selection_quality_score
          FROM {source_alias}.{evidence_table} ce
          JOIN quantities q ON q.source_quantity=ce.classification_scheme
          JOIN {source_alias}.source_records sr
            ON sr.source_record_id=ce.source_record_id
          JOIN evidence_object_bindings b
            ON b.source_id={sql_literal(source['source_id'])}
           AND b.object_type={sql_literal(source['object_type'])}
           AND b.binding_subject_kind='classification_evidence'
           AND b.binding_subject_id=ce.evidence_id
           AND b.binding_status='accepted'
          WHERE NULLIF(TRIM(ce.classification_raw), '') IS NOT NULL
        )
        SELECT object_type, stable_object_key, system_stable_object_key,
               group_key, quantity_key, value_raw, normalized_value,
               normalized_unit, uncertainty_lower, uncertainty_upper,
               bound_semantics, {sql_literal(member['build_id'])},
               {sql_literal(table_name)}, evidence_id, parameter_set_id,
               source_record_id, {sql_literal(source['source_id'])},
               {sql_literal(release_id)}, method, model, reference_raw,
               authority_rank, authority_reason, selection_quality_score,
               normalization_version, quality_json, binding_id
        FROM candidates WHERE authority_rank IS NOT NULL
        """
    )


def insert_identifier_claim_candidates(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
    member: dict[str, Any],
    release_id: str,
) -> None:
    table_name = str(
        source.get("identifier_claim_evidence_table") or "identifier_claim_evidence"
    )
    evidence_table = sql_identifier(table_name)
    quantity_rows = quantity_values(source)
    rank_sql, reason_sql, quality_sql = authority_sql(
        source, source_alias="sr", set_alias="ic", evidence_alias="ic"
    )
    claim_method = sql_literal(
        str(source.get("identifier_claim_method") or "source_identifier_claim")
    )
    con.execute(
        f"""
        INSERT INTO fact_candidates
        WITH quantities(source_quantity, selected_quantity, group_key) AS (
          VALUES {quantity_rows}
        ), candidates AS (
          SELECT b.object_type, b.stable_object_key, b.system_stable_object_key,
                 b.binding_id,
                 q.group_key, q.selected_quantity quantity_key,
                 ic.identifier_raw value_raw, NULL::DOUBLE normalized_value,
                 NULL::VARCHAR normalized_unit, NULL::DOUBLE uncertainty_lower,
                 NULL::DOUBLE uncertainty_upper, NULL::VARCHAR bound_semantics,
                 ic.evidence_id, ic.evidence_id parameter_set_id,
                 ic.source_record_id, {claim_method}::VARCHAR AS "method",
                 NULL::VARCHAR AS "model", ic.reference_raw,
                 'source_identifier_claim_v1'::VARCHAR normalization_version,
                 ic.quality_json,
                 {rank_sql} authority_rank,
                 {reason_sql} authority_reason,
                 {quality_sql} selection_quality_score
          FROM {source_alias}.{evidence_table} ic
          JOIN quantities q ON q.source_quantity=ic.namespace
          JOIN {source_alias}.source_records sr
            ON sr.source_record_id=ic.source_record_id
          JOIN evidence_object_bindings b
            ON b.source_id={sql_literal(source['source_id'])}
           AND b.object_type={sql_literal(source['object_type'])}
           AND b.binding_subject_kind='identifier_claim_evidence'
           AND b.binding_subject_id=ic.evidence_id
           AND b.binding_status='accepted'
          WHERE NULLIF(TRIM(ic.identifier_raw), '') IS NOT NULL
        )
        SELECT object_type, stable_object_key, system_stable_object_key,
               group_key, quantity_key, value_raw, normalized_value,
               normalized_unit, uncertainty_lower, uncertainty_upper,
               bound_semantics, {sql_literal(member['build_id'])},
               {sql_literal(table_name)}, evidence_id, parameter_set_id,
               source_record_id, {sql_literal(source['source_id'])},
               {sql_literal(release_id)}, method, model, reference_raw,
               authority_rank, authority_reason, selection_quality_score,
               normalization_version, quality_json, binding_id
        FROM candidates WHERE authority_rank IS NOT NULL
        """
    )


def insert_measurement_bundle_direct(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
    member: dict[str, Any],
    release_id: str,
) -> None:
    if str(source.get("selection_mode") or "") != "authoritative_direct":
        raise ValueError(f"measurement bundles require authoritative-direct selection: {source['source_id']}")
    table_name = str(source["bundle_table"])
    bundle_table = sql_identifier(table_name)
    bundle_id_field = sql_identifier(str(source.get("bundle_id_field") or "bundle_id"))
    measurements_field = sql_identifier(str(source.get("measurements_field") or "measurements"))
    source_id = sql_literal(source["source_id"])
    object_type = sql_literal(source["object_type"])
    release = sql_literal(release_id)
    evidence_build = sql_literal(member["build_id"])
    policy_version = sql_literal(source["_policy_version"])

    duplicate_sets = int(
        con.execute(
            f"""
            SELECT COALESCE(SUM(n-1), 0) FROM (
              SELECT b.stable_object_key, COUNT(*) n
              FROM {source_alias}.{bundle_table} bundle
              JOIN evidence_object_bindings b
                ON b.source_id={source_id}
               AND b.object_type={object_type}
               AND b.binding_subject_kind='source_record'
               AND b.source_record_id=bundle.source_record_id
               AND b.binding_status='accepted'
              GROUP BY b.stable_object_key HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    )
    if duplicate_sets:
        raise ValueError(
            f"measurement-bundle source has duplicate object bundles: {source['source_id']}:{duplicate_sets}"
        )

    quantity_rows: list[str] = []
    decision_selects: list[str] = []
    for group in source["quantity_groups"]:
        rule = group["authorities"][0]
        unsupported_rule_fields = set(rule) - {"rank", "reason"}
        if unsupported_rule_fields:
            raise ValueError(
                f"measurement-bundle direct authority must be unconditional: "
                f"{source['source_id']}:{group['group_key']}:{sorted(unsupported_rule_fields)}"
            )
        group_key = sql_literal(group["group_key"])
        source_quantities = [str(value) for value in group["quantities"]]
        quantity_list = ",".join(sql_literal(value) for value in source_quantities)
        decision_selects.append(
            "SELECT "
            f"sha256(concat_ws('|', b.object_type, b.stable_object_key, {group_key}, "
            f"bundle.{bundle_id_field}, {policy_version})), "
            "b.object_type, b.stable_object_key, b.system_stable_object_key, "
            f"{group_key}, bundle.{bundle_id_field}, bundle.source_record_id, "
            f"{source_id}, {release}, {evidence_build}, {int(rule['rank'])}, "
            f"{sql_literal(rule['reason'])}, "
            "COUNT(DISTINCT nested.measurement.quantity_key)::INTEGER, "
            "COUNT(DISTINCT CASE WHEN nested.measurement.uncertainty_lower IS NOT NULL "
            "OR nested.measurement.uncertainty_upper IS NOT NULL "
            "THEN nested.measurement.quantity_key END)::INTEGER, "
            f"NULL, 1, NULL, NULL, NULL, {policy_version} "
            f"FROM {source_alias}.{bundle_table} bundle "
            "JOIN evidence_object_bindings b "
            f"ON b.source_id={source_id} AND b.object_type={object_type} "
            "AND b.binding_subject_kind='source_record' "
            "AND b.source_record_id=bundle.source_record_id "
            "AND b.binding_status='accepted' "
            f"CROSS JOIN UNNEST(bundle.{measurements_field}) AS nested(measurement) "
            f"WHERE nested.measurement.quantity_key IN ({quantity_list}) "
            "AND (nested.measurement.normalized_value IS NOT NULL "
            "OR NULLIF(TRIM(nested.measurement.value_raw), '') IS NOT NULL) "
            "GROUP BY b.object_type, b.stable_object_key, b.system_stable_object_key, "
            f"bundle.{bundle_id_field}, bundle.source_record_id"
        )
        for source_quantity, selected_spec in group["quantities"].items():
            if not isinstance(selected_spec, dict):
                selected_spec = {"quantity_key": str(selected_spec)}
            quantity_rows.append(
                "(" + ",".join(
                    [
                        sql_literal(source_quantity),
                        sql_literal(selected_spec["quantity_key"]),
                        sql_literal(group["group_key"]),
                    ]
                ) + ")"
            )
    con.execute(
        "INSERT INTO parameter_set_selection_decisions " + " UNION ALL ".join(decision_selects)
    )
    con.execute(
        f"""
        INSERT INTO selected_facts
        WITH quantities(source_quantity, selected_quantity, quantity_group) AS (
          VALUES {','.join(quantity_rows)}
        )
        SELECT sha256(concat_ws('|', b.object_type, b.stable_object_key,
                                q.selected_quantity, nested.measurement.evidence_id,
                                {policy_version})),
               b.object_type, b.stable_object_key, b.system_stable_object_key,
               q.quantity_group, q.selected_quantity,
               nested.measurement.value_raw,
               nested.measurement.normalized_value,
               nested.measurement.normalized_unit,
               CASE
                 WHEN lower(coalesce(nested.measurement.bound_semantics, '')) LIKE '%endpoint%'
                   THEN nested.measurement.uncertainty_lower
                 WHEN nested.measurement.normalized_value IS NOT NULL
                  AND nested.measurement.uncertainty_lower IS NOT NULL
                   THEN nested.measurement.normalized_value
                        - abs(nested.measurement.uncertainty_lower)
               END,
               CASE
                 WHEN lower(coalesce(nested.measurement.bound_semantics, '')) LIKE '%endpoint%'
                   THEN nested.measurement.uncertainty_upper
                 WHEN nested.measurement.normalized_value IS NOT NULL
                  AND nested.measurement.uncertainty_upper IS NOT NULL
                   THEN nested.measurement.normalized_value
                        + abs(nested.measurement.uncertainty_upper)
               END,
               nested.measurement.bound_semantics,
               'source_selected', {evidence_build}, {sql_literal(table_name)},
               nested.measurement.evidence_id, bundle.{bundle_id_field},
               bundle.source_record_id, {source_id}, {release},
               nested.measurement.method, nested.measurement.model,
               nested.measurement.reference_raw,
               d.decision_id, d.authority_rank, d.authority_reason,
               {policy_version}, nested.measurement.normalization_version,
               nested.measurement.quality_json, b.binding_id
        FROM {source_alias}.{bundle_table} bundle
        JOIN evidence_object_bindings b
          ON b.source_id={source_id}
         AND b.object_type={object_type}
         AND b.binding_subject_kind='source_record'
         AND b.source_record_id=bundle.source_record_id
         AND b.binding_status='accepted'
        CROSS JOIN UNNEST(bundle.{measurements_field}) AS nested(measurement)
        JOIN quantities q ON q.source_quantity=nested.measurement.quantity_key
        JOIN parameter_set_selection_decisions d
          ON d.object_type=b.object_type
         AND d.stable_object_key=b.stable_object_key
         AND d.quantity_group=q.quantity_group
         AND d.selected_parameter_set_id=bundle.{bundle_id_field}
        WHERE nested.measurement.normalized_value IS NOT NULL
           OR NULLIF(TRIM(nested.measurement.value_raw), '') IS NOT NULL
        """
    )


def insert_coherent_direct(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
    member: dict[str, Any],
    release_id: str,
) -> None:
    set_table_name = str(source["parameter_set_table"])
    set_table = sql_identifier(set_table_name)
    set_id_field = sql_identifier(str(source.get("set_id_field") or "evidence_id"))
    values_field = sql_identifier(str(source.get("values_field") or "values_json"))
    specs = coherent_field_specs(con, source=source, source_alias=source_alias)
    policy_version = sql_literal(source["_policy_version"])
    source_id = sql_literal(source["source_id"])
    object_type = sql_literal(source["object_type"])
    release = sql_literal(release_id)
    evidence_build = sql_literal(member["build_id"])

    configured_kinds = {
        str(kind)
        for group in source["quantity_groups"]
        for kind in group.get("parameter_set_kinds") or []
    }
    if configured_kinds:
        kind_values = ",".join(sql_literal(value) for value in sorted(configured_kinds))
        duplicate_expression = (
            "SELECT COALESCE(SUM(n - 1), 0) FROM ("
            "SELECT COUNT(*) n "
            f"FROM {source_alias}.{set_table} ps "
            "JOIN evidence_object_bindings b "
            f"ON b.source_id={source_id} AND b.object_type={object_type} "
            "AND b.binding_subject_kind='source_record' "
            "AND b.source_record_id=ps.source_record_id "
            "AND b.binding_status='accepted' "
            f"WHERE ps.parameter_set_kind IN ({kind_values}) "
            "GROUP BY b.stable_object_key, ps.parameter_set_kind HAVING COUNT(*) > 1)"
        )
    else:
        duplicate_expression = (
            "SELECT COUNT(*) - COUNT(DISTINCT b.stable_object_key) "
            f"FROM {source_alias}.{set_table} ps "
            "JOIN evidence_object_bindings b "
            f"ON b.source_id={source_id} AND b.object_type={object_type} "
            "AND b.binding_subject_kind='source_record' "
            "AND b.source_record_id=ps.source_record_id "
            "AND b.binding_status='accepted'"
        )
    duplicate_sets = int(
        con.execute(duplicate_expression).fetchone()[0]
    )
    if duplicate_sets:
        raise ValueError(
            f"authoritative-direct source has duplicate object parameter sets: "
            f"{source['source_id']}:{duplicate_sets}"
        )

    decision_selects: list[str] = []
    value_rows: list[str] = []
    for group in source["quantity_groups"]:
        rule = group["authorities"][0]
        condition = authority_condition(rule, "sr", "ps")
        kind_condition = parameter_set_kind_condition(group, "ps")
        selection_quality_score = quality_score_sql(
            rule, source_alias="sr", set_alias="ps"
        )
        present_terms: list[str] = []
        uncertainty_terms: list[str] = []
        for source_field, selected_spec in group["quantities"].items():
            if not isinstance(selected_spec, dict):
                selected_spec = {"quantity_key": str(selected_spec)}
            spec = specs[source_field]
            raw = f"json_extract_string(ps.{values_field}, '$[{spec['position']}]')"
            numeric = bool(selected_spec.get("numeric", True))
            normalized = f"try_cast({raw} AS DOUBLE)" if numeric else "NULL::DOUBLE"
            present_terms.append(f"CASE WHEN {raw} IS NOT NULL THEN 1 ELSE 0 END")
            uncertainty_field = selected_spec.get("uncertainty_field")
            if uncertainty_field:
                uncertainty_spec = specs[str(uncertainty_field)]
                uncertainty = (
                    f"try_cast(json_extract_string(ps.{values_field}, "
                    f"'$[{uncertainty_spec['position']}]') AS DOUBLE)"
                )
                uncertainty_lower = uncertainty
                uncertainty_upper = uncertainty
                bound_semantics = sql_literal("symmetric_error")
                uncertainty_terms.append(
                    f"CASE WHEN {raw} IS NOT NULL AND {uncertainty} IS NOT NULL THEN 1 ELSE 0 END"
                )
            else:
                uncertainty_lower = "NULL::DOUBLE"
                uncertainty_upper = "NULL::DOUBLE"
                bound_semantics = "NULL::VARCHAR"
            value_rows.append(
                "(" + ",".join(
                    [
                        sql_literal(group["group_key"]),
                        sql_literal(selected_spec["quantity_key"]),
                        raw,
                        normalized,
                        sql_literal(selected_spec.get("unit", spec["unit"]) or None),
                        uncertainty_lower,
                        uncertainty_upper,
                        bound_semantics,
                        kind_condition,
                    ]
                ) + ")"
            )
        quantity_count = " + ".join(present_terms)
        uncertainty_count = " + ".join(uncertainty_terms) if uncertainty_terms else "0"
        group_key = sql_literal(group["group_key"])
        decision_selects.append(
            "SELECT "
            f"sha256(concat_ws('|', b.object_type, b.stable_object_key, {group_key}, "
            f"ps.{set_id_field}, {policy_version})), "
            "b.object_type, b.stable_object_key, b.system_stable_object_key, "
            f"{group_key}, ps.{set_id_field}, ps.source_record_id, {source_id}, "
            f"{release}, {evidence_build}, {int(rule['rank'])}, {sql_literal(rule['reason'])}, "
            f"{selection_quality_score}, "
            f"({quantity_count})::INTEGER, ({uncertainty_count})::INTEGER, 1, "
            f"NULL, NULL, NULL, {policy_version} "
            f"FROM {source_alias}.{set_table} ps "
            f"JOIN {source_alias}.source_records sr ON sr.source_record_id=ps.source_record_id "
            "JOIN evidence_object_bindings b "
            f"ON b.source_id={source_id} AND b.object_type={object_type} "
            "AND b.binding_subject_kind='source_record' "
            "AND b.source_record_id=ps.source_record_id "
            f"AND b.binding_status='accepted' "
            f"WHERE ({condition}) AND ({kind_condition})"
        )
    con.execute(
        "INSERT INTO parameter_set_selection_decisions " + " UNION ALL ".join(decision_selects)
    )

    con.execute(
        f"""
        INSERT INTO selected_facts
        SELECT sha256(concat_ws('|', b.object_type, b.stable_object_key, v.quantity_key,
                                ps.{set_id_field}, {policy_version})),
               b.object_type, b.stable_object_key, b.system_stable_object_key,
               v.quantity_group, v.quantity_key, v.value_raw, v.normalized_value,
               v.normalized_unit,
               CASE WHEN v.normalized_value IS NOT NULL AND v.uncertainty_lower IS NOT NULL
                    THEN v.normalized_value - abs(v.uncertainty_lower) END,
               CASE WHEN v.normalized_value IS NOT NULL AND v.uncertainty_upper IS NOT NULL
                    THEN v.normalized_value + abs(v.uncertainty_upper) END,
               v.bound_semantics, 'source_selected', {evidence_build},
               {sql_literal(set_table_name)}, ps.{set_id_field}, ps.{set_id_field},
               ps.source_record_id, {source_id}, {release}, ps.method, ps.model,
               ps.reference_raw,
               sha256(concat_ws('|', b.object_type, b.stable_object_key,
                                v.quantity_group, ps.{set_id_field}, {policy_version})),
               d.authority_rank, d.authority_reason, {policy_version},
               ps.normalization_version, ps.quality_json, b.binding_id
        FROM {source_alias}.{set_table} ps
        JOIN evidence_object_bindings b
          ON b.source_id={source_id}
         AND b.object_type={object_type}
         AND b.binding_subject_kind='source_record'
         AND b.source_record_id=ps.source_record_id
         AND b.binding_status='accepted'
        CROSS JOIN LATERAL (
          VALUES {','.join(value_rows)}
        ) v(quantity_group, quantity_key, value_raw, normalized_value,
            normalized_unit, uncertainty_lower, uncertainty_upper, bound_semantics,
            applicable_parameter_set_kind)
        JOIN parameter_set_selection_decisions d
          ON d.object_type=b.object_type
         AND d.stable_object_key=b.stable_object_key
         AND d.quantity_group=v.quantity_group
         AND d.selected_parameter_set_id=ps.{set_id_field}
        WHERE v.applicable_parameter_set_kind AND v.value_raw IS NOT NULL
        """
    )


def insert_coherent_candidates(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
    member: dict[str, Any],
    release_id: str,
) -> None:
    set_table_name = str(source["parameter_set_table"])
    set_table = sql_identifier(set_table_name)
    set_id_field = sql_identifier(str(source.get("set_id_field") or "evidence_id"))
    values_field = sql_identifier(str(source.get("values_field") or "values_json"))
    specs = coherent_field_specs(con, source=source, source_alias=source_alias)
    rows: list[str] = []
    for group in source["quantity_groups"]:
        group_key = str(group["group_key"])
        for source_field, selected_spec in group["quantities"].items():
            if not isinstance(selected_spec, dict):
                selected_spec = {"quantity_key": str(selected_spec)}
            spec = specs[source_field]
            raw = f"json_extract_string(ps.{values_field}, '$[{spec['position']}]')"
            numeric = bool(selected_spec.get("numeric", True))
            normalized = f"try_cast({raw} AS DOUBLE)" if numeric else "NULL::DOUBLE"
            uncertainty_field = selected_spec.get("uncertainty_field")
            if uncertainty_field:
                uncertainty_spec = specs[str(uncertainty_field)]
                uncertainty = (
                    f"try_cast(json_extract_string(ps.{values_field}, "
                    f"'$[{uncertainty_spec['position']}]') AS DOUBLE)"
                )
                uncertainty_lower = uncertainty
                uncertainty_upper = uncertainty
                bound_semantics = sql_literal("symmetric_error")
            else:
                uncertainty_lower = "NULL::DOUBLE"
                uncertainty_upper = "NULL::DOUBLE"
                bound_semantics = "NULL::VARCHAR"
            rank_clauses = [
                f"WHEN {authority_condition(rule, 'sr', 'ps')} THEN {int(rule['rank'])}"
                for rule in group["authorities"]
            ]
            reason_clauses = [
                f"WHEN {authority_condition(rule, 'sr', 'ps')} THEN {sql_literal(rule['reason'])}"
                for rule in group["authorities"]
            ]
            quality_clauses = [
                f"WHEN {authority_condition(rule, 'sr', 'ps')} "
                f"THEN {quality_score_sql(rule, source_alias='sr', set_alias='ps')}"
                for rule in group["authorities"]
            ]
            rows.append(
                "SELECT "
                "b.object_type, b.stable_object_key, b.system_stable_object_key, "
                f"{sql_literal(group_key)} AS quantity_group, "
                f"{sql_literal(selected_spec['quantity_key'])} AS quantity_key, "
                f"{raw} AS value_raw, {normalized} AS normalized_value, "
                f"{sql_literal(selected_spec.get('unit', spec['unit']) or None)} AS normalized_unit, "
                f"{uncertainty_lower} AS uncertainty_lower, "
                f"{uncertainty_upper} AS uncertainty_upper, "
                f"{bound_semantics} AS bound_semantics, "
                f"{sql_literal(member['build_id'])} AS evidence_build_id, "
                f"{sql_literal(set_table_name)} AS evidence_table, "
                f"ps.{set_id_field} AS evidence_id, ps.{set_id_field} AS parameter_set_id, "
                "ps.source_record_id, "
                f"{sql_literal(source['source_id'])} AS source_id, "
                f"{sql_literal(release_id)} AS release_id, "
                "ps.method, ps.model, ps.reference_raw, "
                f"CASE {' '.join(rank_clauses)} ELSE NULL END AS authority_rank, "
                f"CASE {' '.join(reason_clauses)} ELSE NULL END AS authority_reason, "
                f"CASE {' '.join(quality_clauses)} ELSE NULL END AS selection_quality_score, "
                "ps.normalization_version, ps.quality_json, b.binding_id "
                f"FROM {source_alias}.{set_table} ps "
                f"JOIN {source_alias}.source_records sr ON sr.source_record_id=ps.source_record_id "
                "JOIN evidence_object_bindings b "
                f"ON b.source_id={sql_literal(source['source_id'])} "
                f"AND b.object_type={sql_literal(source['object_type'])} "
                "AND b.binding_subject_kind='source_record' "
                "AND b.source_record_id=ps.source_record_id AND b.binding_status='accepted' "
                f"WHERE {raw} IS NOT NULL"
            )
    con.execute("INSERT INTO fact_candidates " + " UNION ALL ".join(rows))


def create_candidate_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP TABLE fact_candidates (
          object_type VARCHAR, stable_object_key VARCHAR, system_stable_object_key VARCHAR,
          quantity_group VARCHAR, quantity_key VARCHAR, value_raw VARCHAR,
          normalized_value DOUBLE, normalized_unit VARCHAR,
          uncertainty_lower DOUBLE, uncertainty_upper DOUBLE, bound_semantics VARCHAR,
          evidence_build_id VARCHAR, evidence_table VARCHAR, evidence_id VARCHAR,
          parameter_set_id VARCHAR, source_record_id VARCHAR, source_id VARCHAR,
          release_id VARCHAR, method VARCHAR, model VARCHAR, reference_raw VARCHAR,
          authority_rank INTEGER, authority_reason VARCHAR,
          selection_quality_score DOUBLE,
          normalization_version VARCHAR, quality_json JSON, binding_id VARCHAR
        );
        """
    )


def select_parameter_sets(con: duckdb.DuckDBPyConnection, policy_version: str) -> None:
    con.execute(
        f"""
        CREATE TEMP TABLE candidate_sets AS
        SELECT object_type, stable_object_key, system_stable_object_key, quantity_group,
               parameter_set_id, source_record_id, source_id, release_id, evidence_build_id,
               MIN(authority_rank)::INTEGER AS authority_rank,
               MIN(authority_reason) AS authority_reason,
               MAX(selection_quality_score) AS selection_quality_score,
               COUNT(DISTINCT quantity_key)::INTEGER AS quantity_count,
               COUNT(DISTINCT CASE WHEN uncertainty_lower IS NOT NULL OR uncertainty_upper IS NOT NULL THEN quantity_key END)::INTEGER AS uncertainty_count,
               COUNT(DISTINCT CASE WHEN NULLIF(TRIM(reference_raw), '') IS NOT NULL THEN quantity_key END)::INTEGER AS reference_count
        FROM fact_candidates
        GROUP BY ALL;

        CREATE TEMP TABLE ranked_sets AS
        SELECT *,
               ROW_NUMBER() OVER (
                 PARTITION BY object_type, stable_object_key, quantity_group
                 ORDER BY authority_rank, quantity_count DESC, uncertainty_count DESC,
                          reference_count DESC, selection_quality_score DESC NULLS LAST,
                          parameter_set_id
               ) AS selection_rank,
               COUNT(*) OVER (PARTITION BY object_type, stable_object_key, quantity_group)::INTEGER AS candidate_count,
               LEAD(parameter_set_id) OVER (
                 PARTITION BY object_type, stable_object_key, quantity_group
                 ORDER BY authority_rank, quantity_count DESC, uncertainty_count DESC,
                          reference_count DESC, selection_quality_score DESC NULLS LAST,
                          parameter_set_id
               ) AS runner_up_parameter_set_id,
               LEAD(authority_rank) OVER (
                 PARTITION BY object_type, stable_object_key, quantity_group
                 ORDER BY authority_rank, quantity_count DESC, uncertainty_count DESC,
                          reference_count DESC, selection_quality_score DESC NULLS LAST,
                          parameter_set_id
               )::INTEGER AS runner_up_authority_rank
               ,LEAD(selection_quality_score) OVER (
                 PARTITION BY object_type, stable_object_key, quantity_group
                 ORDER BY authority_rank, quantity_count DESC, uncertainty_count DESC,
                          reference_count DESC, selection_quality_score DESC NULLS LAST,
                          parameter_set_id
               ) AS runner_up_quality_score
        FROM candidate_sets;

        INSERT INTO parameter_set_selection_decisions
        SELECT sha256(concat_ws('|', object_type, stable_object_key, quantity_group,
                                parameter_set_id, {sql_literal(policy_version)})),
               object_type, stable_object_key, system_stable_object_key, quantity_group,
               parameter_set_id, source_record_id, source_id, release_id,
               evidence_build_id, authority_rank, authority_reason,
               selection_quality_score,
               quantity_count, uncertainty_count, candidate_count,
               runner_up_parameter_set_id, runner_up_authority_rank,
               runner_up_quality_score,
               {sql_literal(policy_version)}
        FROM ranked_sets WHERE selection_rank = 1;

        INSERT INTO selected_facts
        SELECT sha256(concat_ws('|', c.object_type, c.stable_object_key, c.quantity_key,
                                c.evidence_id, {sql_literal(policy_version)})),
               c.object_type, c.stable_object_key, c.system_stable_object_key,
               c.quantity_group, c.quantity_key, c.value_raw, c.normalized_value,
               c.normalized_unit,
               CASE
                 WHEN lower(coalesce(c.bound_semantics, '')) LIKE '%endpoint%' THEN c.uncertainty_lower
                 WHEN c.normalized_value IS NOT NULL AND c.uncertainty_lower IS NOT NULL
                   THEN c.normalized_value - abs(c.uncertainty_lower)
                 ELSE NULL
               END AS value_lower,
               CASE
                 WHEN lower(coalesce(c.bound_semantics, '')) LIKE '%endpoint%' THEN c.uncertainty_upper
                 WHEN c.normalized_value IS NOT NULL AND c.uncertainty_upper IS NOT NULL
                   THEN c.normalized_value + abs(c.uncertainty_upper)
                 ELSE NULL
               END AS value_upper,
               c.bound_semantics, 'source_selected', c.evidence_build_id,
               c.evidence_table, c.evidence_id, c.parameter_set_id,
               c.source_record_id, c.source_id, c.release_id, c.method, c.model,
               c.reference_raw, d.decision_id, c.authority_rank,
               c.authority_reason, {sql_literal(policy_version)},
               c.normalization_version, c.quality_json, c.binding_id
        FROM fact_candidates c
        JOIN parameter_set_selection_decisions d
          ON d.object_type = c.object_type
         AND d.stable_object_key = c.stable_object_key
         AND d.quantity_group = c.quantity_group
         AND d.selected_parameter_set_id = c.parameter_set_id;
        """
    )


def derive_stellar_luminosity(
    con: duckdb.DuckDBPyConnection, policy: dict[str, Any]
) -> None:
    derivation = next(
        item for item in policy["derivations"]
        if item["derivation_key"] == "stellar_luminosity_stefan_boltzmann"
    )
    policy_version = str(policy["policy_version"])
    key = str(derivation["derivation_key"])
    version = str(derivation["version"])
    con.execute(
        f"""
        CREATE TEMP TABLE luminosity_derivations AS
        WITH radius AS (
          SELECT * FROM selected_facts
          WHERE object_type = 'star' AND quantity_key = 'radius_rsun'
        ), temperature AS (
          SELECT * FROM selected_facts
          WHERE object_type = 'star' AND quantity_key = 'teff_k'
        ), candidates AS (
          SELECT r.stable_object_key, r.system_stable_object_key,
                 r.selected_fact_id AS radius_fact_id,
                 t.selected_fact_id AS temperature_fact_id,
                 r.normalized_value * r.normalized_value
                   * pow(t.normalized_value / 5772.0, 4.0) AS value,
                 CASE WHEN r.value_lower > 0 AND t.value_lower > 0
                   THEN r.value_lower * r.value_lower * pow(t.value_lower / 5772.0, 4.0)
                   ELSE NULL END AS value_lower,
                 CASE WHEN r.value_upper > 0 AND t.value_upper > 0
                   THEN r.value_upper * r.value_upper * pow(t.value_upper / 5772.0, 4.0)
                   ELSE NULL END AS value_upper
          FROM radius r JOIN temperature t USING (object_type, stable_object_key)
          LEFT JOIN selected_facts direct
            ON direct.object_type = 'star'
           AND direct.stable_object_key = r.stable_object_key
           AND direct.quantity_key = 'luminosity_lsun'
          WHERE direct.selected_fact_id IS NULL
            AND r.normalized_value > 0 AND t.normalized_value > 0
        )
        SELECT sha256(concat_ws('|', stable_object_key, {sql_literal(key)}, {sql_literal(version)})) AS derivation_id,
               sha256(concat_ws('|', 'star', stable_object_key, 'luminosity_lsun',
                                {sql_literal(key)}, {sql_literal(version)}, {sql_literal(policy_version)})) AS selected_fact_id,
               *
        FROM candidates;

        INSERT INTO selected_facts
        SELECT selected_fact_id, 'star', stable_object_key, system_stable_object_key,
               'stellar_fundamental', 'luminosity_lsun', cast(value AS VARCHAR),
               value, 'solLum', value_lower, value_upper,
               'propagated_selected_interval_endpoints', 'derived',
               NULL, NULL, NULL, NULL, NULL, 'spacegate.derivation',
               {sql_literal(version)}, {sql_literal(key)}, NULL, NULL, NULL, NULL,
               NULL, {sql_literal(policy_version)}, {sql_literal(version)},
               json_object('solar_effective_temperature_k', 5772.0), NULL
        FROM luminosity_derivations;

        INSERT INTO selected_fact_derivations
        SELECT derivation_id, selected_fact_id, stable_object_key, 'luminosity_lsun',
               {sql_literal(key)}, {sql_literal(version)},
               to_json([radius_fact_id, temperature_fact_id]),
               {sql_literal(derivation['applicability'])}, {sql_literal(derivation['formula'])},
               json_object('solar_effective_temperature_k', 5772.0),
               {sql_literal(derivation['uncertainty'])}, 'medium',
               {json_sql_literal(derivation['supersedes'])},
               {sql_literal(policy_version)}
        FROM luminosity_derivations;
        """
    )


def selection_quality_integrity_checks(
    con: duckdb.DuckDBPyConnection,
    policy: dict[str, Any],
    recorder: PhaseRecorder | None = None,
) -> dict[str, int]:
    result: dict[str, int] = {}
    for source in policy.get("selection_sources") or []:
        source_id = str(source["source_id"])
        for group in source.get("quantity_groups") or []:
            group_key = str(group["group_key"])
            for authority in group.get("authorities") or []:
                if not authority.get("quality_order"):
                    continue
                rank = int(authority["rank"])
                reason = str(authority["reason"])
                name = (
                    "competing_parameter_sets_without_selection_quality_"
                    f"{source_id}_{group_key}_{rank}"
                )
                if recorder is not None:
                    recorder.start(f"integrity_check.{name}")
                try:
                    count = int(
                        con.execute(
                            "SELECT COUNT(*) FROM parameter_set_selection_decisions "
                            "WHERE selected_source_id=? AND quantity_group=? "
                            "AND authority_rank=? AND authority_reason=? "
                            "AND selection_quality_score IS NULL "
                            "AND candidate_parameter_set_count>1 "
                            "AND runner_up_authority_rank=authority_rank",
                            [source_id, group_key, rank, reason],
                        ).fetchone()[0]
                        or 0
                    )
                except Exception:
                    if recorder is not None:
                        recorder.finish(status="fail")
                    raise
                result[name] = count
                if recorder is not None:
                    recorder.finish(details={"failure_count": count})
    return result


def verify_keys(
    con: duckdb.DuckDBPyConnection,
    recorder: PhaseRecorder | None = None,
    policy: dict[str, Any] | None = None,
) -> dict[str, int]:
    checks = {
        "duplicate_binding_ids": "SELECT COUNT(*) - COUNT(DISTINCT binding_id) FROM evidence_object_bindings",
        "duplicate_preselection_ids": "SELECT COUNT(*) - COUNT(DISTINCT preselection_id) FROM source_parameter_set_preselections",
        "duplicate_source_preselection_keys": "SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) n FROM source_parameter_set_preselections GROUP BY source_id,source_record_id,selection_key HAVING COUNT(*)>1)",
        "invalid_binding_statuses": "SELECT COUNT(*) FROM evidence_object_bindings WHERE binding_status NOT IN ('accepted', 'missing', 'excluded', 'ambiguous', 'quarantined', 'unresolved')",
        "bindings_without_subjects": "SELECT COUNT(*) FROM evidence_object_bindings WHERE binding_subject_kind IS NULL OR binding_subject_id IS NULL",
        "invalid_applicability_statuses": "SELECT COUNT(*) FROM evidence_object_bindings WHERE applicability_status NOT IN ('applicable','inapplicable')",
        "accepted_inapplicable_bindings": "SELECT COUNT(*) FROM evidence_object_bindings WHERE binding_status='accepted' AND applicability_status<>'applicable'",
        "accepted_bindings_without_targets": "SELECT COUNT(*) FROM evidence_object_bindings WHERE binding_status='accepted' AND (canonical_object_node_key IS NULL OR stable_object_key IS NULL)",
        "unresolved_bindings_with_targets": "SELECT COUNT(*) FROM evidence_object_bindings WHERE binding_status<>'accepted' AND (canonical_object_node_key IS NOT NULL OR stable_object_key IS NOT NULL OR system_stable_object_key IS NOT NULL)",
        "duplicate_decision_ids": "SELECT COUNT(*) - COUNT(DISTINCT decision_id) FROM parameter_set_selection_decisions",
        "duplicate_selected_fact_ids": "SELECT COUNT(*) - COUNT(DISTINCT selected_fact_id) FROM selected_facts",
        "duplicate_object_quantities": "SELECT COALESCE(SUM(n - 1), 0) FROM (SELECT COUNT(*) n FROM selected_facts GROUP BY object_type, stable_object_key, quantity_key HAVING COUNT(*) > 1)",
        "selected_source_facts_without_evidence": "SELECT COUNT(*) FROM selected_facts WHERE fact_status='source_selected' AND (evidence_build_id IS NULL OR evidence_id IS NULL OR parameter_set_id IS NULL)",
        "selected_source_facts_without_binding_id": "SELECT COUNT(*) FROM selected_facts WHERE fact_status='source_selected' AND binding_id IS NULL",
        "selected_source_facts_without_accepted_subject_binding": "SELECT COUNT(*) FROM selected_facts f WHERE f.fact_status='source_selected' AND NOT EXISTS (SELECT 1 FROM evidence_object_bindings b WHERE b.binding_id=f.binding_id AND b.source_id=f.source_id AND b.object_type=f.object_type AND b.binding_status='accepted')",
        "selected_source_facts_outside_required_preselection": "SELECT COUNT(*) FROM selected_facts f WHERE f.fact_status='source_selected' AND f.source_id IN (SELECT DISTINCT source_id FROM source_parameter_set_preselections) AND NOT EXISTS (SELECT 1 FROM source_parameter_set_preselections p WHERE p.source_id=f.source_id AND p.source_record_id=f.source_record_id AND p.selected_parameter_set_id=f.parameter_set_id)",
        "derived_facts_without_derivation": "SELECT COUNT(*) FROM selected_facts f LEFT JOIN selected_fact_derivations d ON d.output_selected_fact_id=f.selected_fact_id WHERE f.fact_status='derived' AND d.derivation_id IS NULL",
        "lower_authority_winner": "SELECT COUNT(*) FROM parameter_set_selection_decisions WHERE runner_up_authority_rank IS NOT NULL AND runner_up_authority_rank < authority_rank",
    }
    result: dict[str, int] = {}
    for name, sql in checks.items():
        if recorder is not None:
            recorder.start(f"integrity_check.{name}")
        try:
            count = int(con.execute(sql).fetchone()[0] or 0)
        except Exception:
            if recorder is not None:
                recorder.finish(status="fail")
            raise
        result[name] = count
        if recorder is not None:
            recorder.finish(details={"failure_count": count})
    if policy is not None:
        result.update(selection_quality_integrity_checks(con, policy, recorder))
    failing = {name: count for name, count in result.items() if count}
    if failing:
        raise ValueError(f"selected-fact integrity checks failed: {failing}")
    return result


def table_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    return {
        table: int(con.execute(f"SELECT COUNT(*) FROM {sql_identifier(table)}").fetchone()[0])
        for table in [
            "selection_source_accounting", "evidence_object_bindings",
            "source_parameter_set_preselections",
            "parameter_set_selection_decisions", "selected_facts",
            "selected_fact_derivations",
        ]
    }


def safe_partition_name(value: str) -> str:
    if not re.fullmatch(r"[a-z0-9_]+", value):
        raise ValueError(f"unsafe selected-fact partition key: {value!r}")
    return value


def compile_selected_facts(
    *,
    state_dir: Path,
    policy_path: Path,
    dispositions_path: Path | None = None,
    artifact_root: Path | None = None,
    report_path: Path | None = None,
    memory_limit: str = "32GB",
    threads: int = 8,
    temp_directory: Path | None = None,
) -> dict[str, Any]:
    preflight_started_at = utc_now()
    preflight_start_monotonic = time.monotonic()
    preflight_start_cpu = process_cpu_seconds()
    state_dir = state_dir.resolve()
    policy_path = policy_path.resolve()
    dispositions_path = (
        dispositions_path or policy_path.with_name(DEFAULT_DISPOSITIONS.name)
    ).resolve()
    policy = load_json(policy_path)
    release_manifest_path, release_manifest = release_set_paths(state_dir, policy)
    validate_policy(policy, release_manifest)
    dispositions = load_json(dispositions_path)
    disposition_audit = source_disposition_audit.audit(
        release_manifest, policy, dispositions
    )
    if disposition_audit["status"] == "fail":
        raise ValueError(
            "E5 source disposition audit failed: "
            + json.dumps(disposition_audit["checks"], sort_keys=True)
        )
    members = member_by_source(release_manifest)
    input_attestor = FileHashAttestor()

    identity_dir = state_dir / "derived/evidence_lake_v2/identity" / str(policy["identity_graph_id"])
    identity_db = identity_dir / "identity_graph.duckdb"
    core_dir = state_dir / "out" / str(policy["canonical_reference_build_id"])
    core_db = core_dir / "core.duckdb"
    if not identity_db.is_file() or not core_db.is_file():
        raise ValueError("selection identity graph or canonical reference database is missing")
    identity_sha = file_sha256(identity_db)
    core_sha = file_sha256(core_db)
    compiler_sha = file_sha256(Path(__file__).resolve())
    policy_sha = file_sha256(policy_path)
    dispositions_sha = file_sha256(dispositions_path)
    disposition_audit_compiler_sha = file_sha256(
        Path(source_disposition_audit.__file__).resolve()
    )
    inputs = {
        "policy_sha256": policy_sha,
        "source_disposition_sha256": dispositions_sha,
        "source_disposition_version": dispositions["disposition_version"],
        "source_disposition_audit_compiler_sha256": disposition_audit_compiler_sha,
        "evidence_release_set_id": release_manifest["release_set_id"],
        "evidence_release_set_sha256": release_manifest["release_set_sha256"],
        "identity_graph_id": policy["identity_graph_id"],
        "identity_graph_sha256": identity_sha,
        "canonical_reference_build_id": policy["canonical_reference_build_id"],
        "canonical_reference_sha256": core_sha,
        "compiler_sha256": compiler_sha,
        "duckdb_version": duckdb.__version__,
    }
    build_sha = stable_sha256(inputs)
    build_id = build_sha[:24]
    artifact_root = (
        artifact_root or state_dir / "derived/evidence_lake_v2/selected_facts"
    ).resolve()
    final_dir = artifact_root / build_id
    final_manifest = final_dir / "manifest.json"
    if final_manifest.is_file():
        manifest = load_json(final_manifest)
        if manifest.get("build_sha256") != build_sha:
            raise ValueError(f"immutable selected-fact build collision: {build_id}")
        if report_path:
            atomic_json(report_path, manifest["report"])
        return manifest["report"]

    artifact_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=artifact_root))
    database = staging / "selected_facts.duckdb"
    temp_directory = (temp_directory or Path("/mnt/space/spacegate/e5-selection-spill")).resolve()
    temp_directory.mkdir(parents=True, exist_ok=True)
    timing_report_path = (
        report_path.with_name(f"{report_path.stem}_timing.json")
        if report_path is not None
        else state_dir / "reports/evidence_lake_v2"
        / f"e5_selected_fact_{build_id}_timing.json"
    )
    recorder = PhaseRecorder(
        build_id=build_id,
        compiler_version=str(policy["compiler_version"]),
        staging=staging,
        spill=temp_directory,
        report_path=timing_report_path,
    )
    recorder.add_completed(
        "preflight",
        started_at=preflight_started_at,
        start_monotonic=preflight_start_monotonic,
        start_cpu_seconds=preflight_start_cpu,
        details={
            "selection_sources": len(policy["selection_sources"]),
            "source_disposition_status": disposition_audit["status"],
        },
    )
    recorder.start("database_open")
    try:
        con = duckdb.connect(
            str(database),
            config={
                "memory_limit": memory_limit,
                "threads": str(max(1, threads)),
                "temp_directory": str(temp_directory),
                "preserve_insertion_order": "false",
            },
        )
    except Exception:
        recorder.finish(status="fail")
        recorder.complete("failed")
        raise
    recorder.finish(
        details={"memory_limit": memory_limit, "threads": max(1, threads)}
    )
    try:
        recorder.start("schema_and_inputs")
        create_schema(con)
        create_candidate_table(con)
        con.execute(f"ATTACH {sql_literal(str(identity_db))} AS identity (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(str(core_db))} AS core (READ_ONLY)")
        recorder.finish()
        recorder.start("immutable_e4_input_verification")
        verified_inputs = verify_e4_member_inputs(
            state_dir=state_dir,
            sources=list(policy["selection_sources"]),
            members=members,
            attestor=input_attestor,
            workers=min(4, max(1, threads)),
        )
        recorder.finish(details=verified_inputs)
        source_runtime: list[tuple[dict[str, Any], str, dict[str, Any], str, int, int]] = []
        for index, configured_source in enumerate(policy["selection_sources"]):
            source = dict(configured_source)
            source["_policy_version"] = str(policy["policy_version"])
            source_id = str(source["source_id"])
            member = members[source_id]
            alias = f"e4_{index}"
            artifact_path = state_dir / str(member["artifact_path"])
            db_path = artifact_path / str(member["database"])
            recorder.start("source_prepare", source_id=source_id)
            con.execute(f"ATTACH {sql_literal(str(db_path))} AS {sql_identifier(alias)} (READ_ONLY)")
            release_id = str(member["release_ids"][source_id])
            recorder.finish(
                details={
                    "evidence_build_id": member["build_id"],
                    "database_bytes": int(member["database_bytes"]),
                }
            )
            recorder.start("source_binding", source_id=source_id)
            eligible, accepted = create_binding(
                con,
                source=source,
                source_alias=alias,
                member=member,
                release_id=release_id,
            )
            actual_outcomes = {
                str(status): int(count)
                for status, count in con.execute(
                    "SELECT binding_status,COUNT(*) FROM evidence_object_bindings "
                    "WHERE source_id=? AND object_type=? GROUP BY 1 ORDER BY 1",
                    [source_id, source["object_type"]],
                ).fetchall()
            }
            expected_outcomes = source.get("expected_binding_outcomes")
            if expected_outcomes is not None and actual_outcomes != expected_outcomes:
                raise ValueError(
                    f"selection source binding outcomes changed: {source_id}:"
                    f"expected={expected_outcomes}:actual={actual_outcomes}"
                )
            if eligible < int(source.get("minimum_eligible_records") or 1):
                raise ValueError(
                    f"selection source eligible-record floor failed: "
                    f"{source_id}:{eligible}<{source.get('minimum_eligible_records', 1)}"
                )
            if accepted < int(source.get("minimum_accepted_bindings") or 1):
                raise ValueError(
                    f"selection source accepted-binding floor failed: "
                    f"{source_id}:{accepted}<{source.get('minimum_accepted_bindings', 1)}"
                )
            recorder.finish(
                details={
                    "eligible_binding_subjects": eligible,
                    "accepted_bindings": accepted,
                    "binding_outcomes": actual_outcomes,
                }
            )
            recorder.start("source_preselection", source_id=source_id)
            preselected = create_parameter_set_preselection(
                con,
                source=source,
                source_alias=alias,
                member=member,
                release_id=release_id,
            )
            if source.get("parameter_set_preselection") is not None:
                minimum_preselected = int(
                    source["parameter_set_preselection"].get(
                        "minimum_selected_parameter_sets", 1
                    )
                )
                if preselected < minimum_preselected:
                    raise ValueError(
                        f"source parameter-set preselection floor failed: "
                        f"{source_id}:{preselected}<{minimum_preselected}"
                    )
                expected_preselected = source["parameter_set_preselection"].get(
                    "expected_selected_parameter_sets"
                )
                if (
                    expected_preselected is not None
                    and preselected != int(expected_preselected)
                ):
                    raise ValueError(
                        f"source parameter-set preselection count changed: "
                        f"{source_id}:expected={expected_preselected}:actual={preselected}"
                    )
            recorder.finish(details={"selected_parameter_sets": preselected})
            before_fact_candidates = int(
                con.execute("SELECT COUNT(*) FROM fact_candidates").fetchone()[0]
            )
            before_selected_facts = int(
                con.execute("SELECT COUNT(*) FROM selected_facts").fetchone()[0]
            )
            before_decisions = int(
                con.execute(
                    "SELECT COUNT(*) FROM parameter_set_selection_decisions"
                ).fetchone()[0]
            )
            recorder.start("source_candidate_insertion", source_id=source_id)
            insert_candidates(
                con,
                source=source,
                source_alias=alias,
                member=member,
                release_id=release_id,
            )
            recorder.finish(
                details={
                    "fact_candidates_added": int(
                        con.execute("SELECT COUNT(*) FROM fact_candidates").fetchone()[0]
                    ) - before_fact_candidates,
                    "direct_selected_facts_added": int(
                        con.execute("SELECT COUNT(*) FROM selected_facts").fetchone()[0]
                    ) - before_selected_facts,
                    "direct_decisions_added": int(
                        con.execute(
                            "SELECT COUNT(*) FROM parameter_set_selection_decisions"
                        ).fetchone()[0]
                    ) - before_decisions,
                }
            )
            source_runtime.append((source, alias, member, release_id, eligible, accepted))

        recorder.start("global_parameter_set_selection")
        select_parameter_sets(con, str(policy["policy_version"]))
        recorder.finish(
            details={
                "selection_decisions": int(
                    con.execute(
                        "SELECT COUNT(*) FROM parameter_set_selection_decisions"
                    ).fetchone()[0]
                ),
                "source_selected_facts": int(
                    con.execute(
                        "SELECT COUNT(*) FROM selected_facts "
                        "WHERE fact_status='source_selected'"
                    ).fetchone()[0]
                ),
            }
        )
        recorder.start("derivations")
        derive_stellar_luminosity(con, policy)
        recorder.finish(
            details={
                "derived_facts": int(
                    con.execute(
                        "SELECT COUNT(*) FROM selected_facts WHERE fact_status='derived'"
                    ).fetchone()[0]
                )
            }
        )

        recorder.start("source_accounting")
        for source, _alias, member, release_id, eligible, accepted in source_runtime:
            source_id = str(source["source_id"])
            if str(source.get("selection_mode") or "ranked_candidates") == "authoritative_direct":
                candidate_sets = int(
                    con.execute(
                        "SELECT COUNT(*) FROM parameter_set_selection_decisions "
                        "WHERE selected_source_id=? AND object_type=?",
                        [source_id, source["object_type"]],
                    ).fetchone()[0]
                )
            else:
                candidate_sets = int(
                    con.execute(
                        "SELECT COUNT(*) FROM candidate_sets "
                        "WHERE source_id=? AND object_type=?",
                        [source_id, source["object_type"]],
                    ).fetchone()[0]
                )
            selected_sets = int(
                con.execute(
                    "SELECT COUNT(*) FROM parameter_set_selection_decisions "
                    "WHERE selected_source_id=? AND object_type=?",
                    [source_id, source["object_type"]],
                ).fetchone()[0]
            )
            selected = int(
                con.execute(
                    "SELECT COUNT(*) FROM selected_facts WHERE source_id=? "
                    "AND object_type=? AND fact_status='source_selected'",
                    [source_id, source["object_type"]],
                ).fetchone()[0]
            )
            if selected < int(source.get("minimum_selected_facts") or 1):
                raise ValueError(
                    f"selection source selected-fact floor failed: "
                    f"{source_id}:{selected}<{source.get('minimum_selected_facts', 1)}"
                )
            if (
                source.get("expected_selected_facts") is not None
                and selected != int(source["expected_selected_facts"])
            ):
                raise ValueError(
                    f"selection source selected-fact count changed: {source_id}:"
                    f"expected={source['expected_selected_facts']}:actual={selected}"
                )
            con.execute(
                "INSERT INTO selection_source_accounting VALUES (?,?,?,?,?,?,?,?,?,?)",
                [
                    source_id, release_id, member["build_id"], source["object_type"],
                    eligible, accepted, eligible - accepted, candidate_sets,
                    selected_sets, selected,
                ],
            )
        recorder.finish(details={"sources": len(source_runtime)})

        checks = verify_keys(con, recorder, policy)
        con.execute(
            "INSERT INTO evidence_build VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                build_id, policy["policy_version"], policy_sha,
                release_manifest["release_set_id"], release_manifest["release_set_sha256"],
                policy["identity_graph_id"], identity_sha,
                policy["canonical_reference_build_id"], core_sha,
                policy["compiler_version"], compiler_sha, utc_now(), "pass",
            ],
        )
        recorder.start("checkpoint")
        con.execute("CHECKPOINT")
        recorder.finish()
        recorder.start("summary_accounting")
        counts = table_counts(con)
        binding_outcomes: dict[str, dict[str, int]] = {}
        binding_outcomes_by_object_type: dict[str, dict[str, dict[str, int]]] = {}
        for source_id, binding_status, row_count in con.execute(
            "SELECT source_id, binding_status, COUNT(*) "
            "FROM evidence_object_bindings GROUP BY 1,2 ORDER BY 1,2"
        ).fetchall():
            binding_outcomes.setdefault(str(source_id), {})[str(binding_status)] = int(
                row_count
            )
        for source_id, object_type, binding_status, row_count in con.execute(
            "SELECT source_id, object_type, binding_status, COUNT(*) "
            "FROM evidence_object_bindings GROUP BY 1,2,3 ORDER BY 1,2,3"
        ).fetchall():
            binding_outcomes_by_object_type.setdefault(str(source_id), {}).setdefault(
                str(object_type), {}
            )[str(binding_status)] = int(row_count)
        recorder.finish(
            details={
                "table_counts": counts,
                "binding_outcomes": binding_outcomes,
                "binding_outcomes_by_object_type": binding_outcomes_by_object_type,
            }
        )
        fact_exports: dict[str, int] = {}
        decision_exports: dict[str, int] = {}
        recorder.start("selected_fact_exports")
        for quantity_key, row_count in con.execute(
            "SELECT quantity_key, COUNT(*) FROM selected_facts "
            "GROUP BY quantity_key ORDER BY quantity_key"
        ).fetchall():
            partition = safe_partition_name(str(quantity_key))
            output = staging / f"selected_facts__{partition}.parquet"
            fact_exports[output.name] = int(row_count)
            con.execute(
                "COPY (SELECT * FROM selected_facts "
                f"WHERE quantity_key={sql_literal(quantity_key)} ORDER BY selected_fact_id) "
                f"TO {sql_literal(str(output))} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
            )
        recorder.finish(
            details={
                "partitions": len(fact_exports),
                "rows": sum(fact_exports.values()),
            }
        )
        recorder.start("selection_decision_exports")
        for quantity_group, row_count in con.execute(
            "SELECT quantity_group, COUNT(*) FROM parameter_set_selection_decisions "
            "GROUP BY quantity_group ORDER BY quantity_group"
        ).fetchall():
            partition = safe_partition_name(str(quantity_group))
            output = staging / f"selection_decisions__{partition}.parquet"
            decision_exports[output.name] = int(row_count)
            con.execute(
                "COPY (SELECT * FROM parameter_set_selection_decisions "
                f"WHERE quantity_group={sql_literal(quantity_group)} ORDER BY decision_id) "
                f"TO {sql_literal(str(output))} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
            )
        recorder.finish(
            details={
                "partitions": len(decision_exports),
                "rows": sum(decision_exports.values()),
            }
        )
        recorder.start("auxiliary_exports")
        for table, order_key in [
            ("selected_fact_derivations", "derivation_id"),
            ("selection_source_accounting", "source_id"),
            ("source_parameter_set_preselections", "preselection_id"),
        ]:
            output = staging / f"{table}.parquet"
            con.execute(
                f"COPY (SELECT * FROM {sql_identifier(table)} ORDER BY {sql_identifier(order_key)}) "
                f"TO {sql_literal(str(output))} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
            )
        recorder.finish(details={"tables": 3})
    except Exception:
        recorder.finish(status="fail")
        recorder.complete("failed")
        con.close()
        shutil.rmtree(staging, ignore_errors=True)
        raise
    else:
        con.close()

    expected_exports = {**fact_exports, **decision_exports}
    recorder.start("export_validation")
    missing_exports = sorted(name for name in expected_exports if not (staging / name).is_file())
    if missing_exports:
        recorder.finish(status="fail", details={"missing_exports": missing_exports})
        recorder.complete("failed")
        shutil.rmtree(staging, ignore_errors=True)
        raise ValueError(f"selected-fact partition exports missing: {missing_exports}")
    export_con = duckdb.connect(":memory:")
    try:
        export_counts = {
            name: int(
                export_con.execute(
                    f"SELECT COUNT(*) FROM read_parquet({sql_literal(str(staging / name))})"
                ).fetchone()[0]
            )
            for name in sorted(expected_exports)
        }
    finally:
        export_con.close()
    mismatched_exports = {
        name: {"expected": expected_exports[name], "actual": export_counts[name]}
        for name in expected_exports
        if expected_exports[name] != export_counts[name]
    }
    if mismatched_exports:
        recorder.finish(
            status="fail", details={"mismatched_exports": mismatched_exports}
        )
        recorder.complete("failed")
        shutil.rmtree(staging, ignore_errors=True)
        raise ValueError(f"selected-fact partition row accounting failed: {mismatched_exports}")
    recorder.finish(
        details={
            "files": len(expected_exports),
            "rows": sum(expected_exports.values()),
        }
    )

    recorder.start("artifact_hashing")
    files = {
        path.name: {"bytes": path.stat().st_size, "sha256": file_sha256(path)}
        for path in sorted(staging.iterdir()) if path.is_file()
    }
    logical_sha = stable_sha256({name: value["sha256"] for name, value in files.items() if name.endswith(".parquet")})
    recorder.finish(
        details={"files": len(files), "logical_content_sha256": logical_sha}
    )
    report = {
        "schema_version": "spacegate.selected_fact_compile_report.v1",
        "status": "pass",
        "build_id": build_id,
        "build_sha256": build_sha,
        "policy_version": policy["policy_version"],
        "source_disposition_version": dispositions["disposition_version"],
        "source_disposition_status": disposition_audit["status"],
        "source_disposition_blockers": disposition_audit["checks"]["blocking_sources"],
        "evidence_release_set_id": release_manifest["release_set_id"],
        "identity_graph_id": policy["identity_graph_id"],
        "canonical_reference_build_id": policy["canonical_reference_build_id"],
        "table_counts": counts,
        "binding_outcomes": binding_outcomes,
        "binding_outcomes_by_object_type": binding_outcomes_by_object_type,
        "integrity_checks": checks,
        "logical_content_sha256": logical_sha,
        "partition_exports": {
            "selected_facts": fact_exports,
            "selection_decisions": decision_exports,
        },
        "files": files,
        "performance_report": str(timing_report_path),
        "performance_phase_count_before_promotion": len(recorder.phases),
    }
    manifest = {
        "schema_version": "spacegate.selected_fact_artifact.v1",
        "build_id": build_id,
        "build_sha256": build_sha,
        "inputs": inputs,
        "report": report,
    }
    recorder.start("manifest_write")
    atomic_json(staging / "manifest.json", manifest)
    recorder.finish()
    recorder.start("artifact_promotion")
    os.replace(staging, final_dir)
    recorder.finish(details={"artifact_path": str(final_dir)})
    recorder.start("current_pointer_promotion")
    current_temp = artifact_root / f".current.{os.getpid()}.tmp"
    current_temp.unlink(missing_ok=True)
    current_temp.symlink_to(build_id)
    os.replace(current_temp, artifact_root / "current")
    recorder.finish(details={"current_target": build_id})
    performance = recorder.complete("pass")
    report["performance"] = performance
    if report_path:
        atomic_json(report_path, report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("/data/spacegate/state"))
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--dispositions", type=Path, default=DEFAULT_DISPOSITIONS)
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--memory-limit", default="32GB")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--temp-directory", type=Path)
    args = parser.parse_args()
    report = compile_selected_facts(
        state_dir=args.state_dir,
        policy_path=args.policy,
        dispositions_path=args.dispositions,
        artifact_root=args.artifact_root,
        report_path=args.report,
        memory_limit=args.memory_limit,
        threads=args.threads,
        temp_directory=args.temp_directory,
    )
    print(
        f"E5 selected facts {report['build_id']} pass: "
        f"facts={report['table_counts']['selected_facts']} "
        f"decisions={report['table_counts']['parameter_set_selection_decisions']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
