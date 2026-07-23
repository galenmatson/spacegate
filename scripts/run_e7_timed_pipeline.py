#!/usr/bin/env python3
"""Run the pinned E7 clean checkpoint with per-stage resource accounting."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config/evidence_lake/e7_timed_pipeline.json"
DEFAULT_RUN_ROOT = Path("/mnt/space/spacegate/e7-build-runs")
DEFAULT_REPORT_ROOT = Path("/data/spacegate/state/reports/evidence_lake_v2/e7_build_runs")
FORBIDDEN_COMMAND_FRAGMENTS = (
    "antiproton",
    "deploy_",
    "docker compose",
    "git push",
    "promote",
    "proton:",
    "ssh ",
)
TIME_PATTERNS = {
    "user_seconds": re.compile(r"^User time \(seconds\):\s*(.+)$"),
    "system_seconds": re.compile(r"^System time \(seconds\):\s*(.+)$"),
    "max_rss_kib": re.compile(r"^Maximum resident set size \(kbytes\):\s*(\d+)$"),
    "filesystem_input_blocks": re.compile(r"^File system inputs:\s*(\d+)$"),
    "filesystem_output_blocks": re.compile(r"^File system outputs:\s*(\d+)$"),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def repository_state() -> dict[str, Any]:
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()
    dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain"], cwd=ROOT, check=True, capture_output=True, text=True
        ).stdout.strip()
    )
    return {"revision": revision, "dirty": dirty}


def read_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def write_object_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def validate_config(config: dict[str, Any]) -> None:
    if config.get("schema_version") != "spacegate.e7_timed_pipeline.v1":
        raise ValueError("unsupported E7 timed-pipeline schema")
    stages = config.get("stages")
    if not isinstance(stages, list) or not stages:
        raise ValueError("pipeline must contain stages")
    artifact_roots = {
        "default": config.get("artifact_root"),
        **(config.get("artifact_roots") or {}),
    }
    if not all(
        isinstance(name, str) and isinstance(path, str) and Path(path).is_absolute()
        for name, path in artifact_roots.items()
        if path is not None
    ):
        raise ValueError("artifact roots must be named absolute paths")
    seen: set[str] = set()
    for stage in stages:
        if not isinstance(stage, dict):
            raise ValueError("pipeline stage must be an object")
        stage_id = stage.get("stage_id")
        if not isinstance(stage_id, str) or not re.fullmatch(r"[a-z0-9_]+", stage_id):
            raise ValueError(f"invalid stage_id: {stage_id!r}")
        if stage_id in seen:
            raise ValueError(f"duplicate stage_id: {stage_id}")
        seen.add(stage_id)
        if stage.get("kind") not in {"compiler", "verification"}:
            raise ValueError(f"invalid stage kind: {stage_id}")
        command = stage.get("command")
        if not isinstance(command, list) or not command or not all(isinstance(item, str) for item in command):
            raise ValueError(f"invalid command: {stage_id}")
        rendered = " ".join(command).lower()
        if any(fragment in rendered for fragment in FORBIDDEN_COMMAND_FRAGMENTS):
            raise ValueError(f"unsafe command in timed pipeline: {stage_id}")
        artifact = stage.get("artifact")
        if artifact is not None and (Path(artifact).is_absolute() or ".." in Path(artifact).parts):
            raise ValueError(f"artifact must be relative and bounded: {stage_id}")
        root_name = stage.get("artifact_root", "default")
        if artifact is not None and root_name not in artifact_roots:
            raise ValueError(f"unknown artifact root for stage {stage_id}: {root_name}")


def parse_gnu_time(path: Path) -> dict[str, int | float]:
    parsed: dict[str, int | float] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        for key, pattern in TIME_PATTERNS.items():
            match = pattern.match(line)
            if match:
                parsed[key] = int(match.group(1)) if key not in {"user_seconds", "system_seconds"} else float(match.group(1))
    missing = sorted(set(TIME_PATTERNS) - set(parsed))
    if missing:
        raise ValueError(f"GNU time output missing fields: {', '.join(missing)}")
    parsed["cpu_seconds"] = round(float(parsed["user_seconds"]) + float(parsed["system_seconds"]), 6)
    return parsed


def manifest_product_bytes(artifact: Path) -> int | None:
    manifest_path = artifact / "manifest.json"
    if not manifest_path.is_file():
        return None
    manifest = read_object(manifest_path)
    products = manifest.get("products") or manifest.get("deterministic_files")
    if isinstance(products, dict):
        sizes = [item.get("bytes") for item in products.values() if isinstance(item, dict)]
        if sizes and all(isinstance(value, int) for value in sizes):
            return sum(sizes)
    return None


def command_report_path(command: list[str]) -> Path | None:
    try:
        index = command.index("--report")
    except ValueError:
        return None
    if index + 1 >= len(command):
        raise ValueError("--report requires a path")
    return Path(command[index + 1])


def validate_stage_result(stage: dict[str, Any], command: list[str]) -> dict[str, Any]:
    expected_build_id = stage.get("expected_build_id")
    if not expected_build_id:
        return {}
    report_path = command_report_path(command)
    if report_path is None or not report_path.is_file():
        return {"result_report": str(report_path) if report_path else None, "result_error": "missing_report"}
    result_report = read_object(report_path)
    errors: dict[str, Any] = {}
    if result_report.get("build_id") != expected_build_id:
        errors["build_id"] = {"expected": expected_build_id, "actual": result_report.get("build_id")}
    if result_report.get("status") != "pass":
        errors["scientific_status"] = result_report.get("status")
    return {"result_report": str(report_path), "result_validation_errors": errors}


def render_command(command: list[str], values: dict[str, str]) -> list[str]:
    try:
        return [item.format_map(values) for item in command]
    except KeyError as exc:
        raise ValueError(f"unknown command template field: {exc.args[0]}") from exc


def command_values(config: dict[str, Any], run_report_dir: Path) -> dict[str, str]:
    # Do not resolve the venv interpreter: it is normally a symlink, and resolving
    # it bypasses pyvenv.cfg and silently runs without the repository environment.
    return {
        "python": str(ROOT / ".venv/bin/python"),
        "state_dir": str(Path(config["state_dir"]).resolve()),
        "artifact_root": str(Path(config["artifact_root"]).resolve()),
        "run_report_dir": str(run_report_dir),
    }


def acquire_lock(run_root: Path):
    run_root.mkdir(parents=True, exist_ok=True)
    handle = (run_root / ".pipeline.lock").open("w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise RuntimeError("another E7 timed pipeline is already running") from exc
    return handle


def run_pipeline(config: dict[str, Any], mode: str, run_root: Path, report_root: Path) -> tuple[dict[str, Any], Path]:
    validate_config(config)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"_{os.getpid()}"
    run_dir = run_root / run_id
    log_dir = run_dir / "logs"
    run_report_dir = run_dir / "reports"
    log_dir.mkdir(parents=True)
    run_report_dir.mkdir(parents=True)
    report_path = report_root / f"{run_id}.json"
    latest_path = report_root / "latest.json"
    artifact_roots = {
        "default": Path(config["artifact_root"]).resolve(),
        **{
            name: Path(path).resolve()
            for name, path in (config.get("artifact_roots") or {}).items()
        },
    }
    values = command_values(config, run_report_dir)
    report: dict[str, Any] = {
        "schema_version": "spacegate.e7_timed_pipeline_report.v1",
        "pipeline_id": config["pipeline_id"],
        "config_sha256": canonical_sha256(config),
        "host": socket.gethostname(),
        "repository": repository_state(),
        "run_id": run_id,
        "mode": mode,
        "started_at": utc_now(),
        "finished_at": None,
        "status": "running",
        "run_dir": str(run_dir),
        "stages": [],
        "totals": {},
    }
    write_object_atomic(report_path, report)

    total_started = time.monotonic()
    failed = False
    for stage in config["stages"]:
        artifact_root = artifact_roots[stage.get("artifact_root", "default")]
        artifact = artifact_root / stage["artifact"] if stage.get("artifact") else None
        artifact_present = bool(artifact and (artifact / "manifest.json").is_file())
        if mode == "verify" and stage["kind"] == "compiler":
            command = render_command(stage["command"], values)
            result = {
                "stage_id": stage["stage_id"],
                "kind": stage["kind"],
                "status": "skipped",
                "cache_state": "attested_reuse_pending_verifier" if artifact_present else "missing_artifact",
                "expected_build_id": stage.get("expected_build_id"),
                "artifact": str(artifact) if artifact else None,
                "declared_output_bytes": manifest_product_bytes(artifact) if artifact_present and artifact else None,
                "command": command,
            }
            report["stages"].append(result)
            if not artifact_present:
                failed = True
                report["status"] = "fail"
                write_object_atomic(report_path, report)
                break
            write_object_atomic(report_path, report)
            continue

        command = render_command(stage["command"], values)
        stdout_path = log_dir / f"{stage['stage_id']}.stdout.log"
        stderr_path = log_dir / f"{stage['stage_id']}.stderr.log"
        time_path = log_dir / f"{stage['stage_id']}.time.txt"
        started = utc_now()
        wall_started = time.monotonic()
        with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open("w", encoding="utf-8") as stderr:
            completed = subprocess.run(
                ["/usr/bin/time", "-v", "-o", str(time_path), "--", *command],
                cwd=ROOT,
                stdout=stdout,
                stderr=stderr,
                check=False,
            )
        timing = parse_gnu_time(time_path)
        result_validation = validate_stage_result(stage, command) if completed.returncode == 0 else {}
        validation_errors = result_validation.get("result_validation_errors") or (
            {"report": result_validation.get("result_error")} if result_validation.get("result_error") else {}
        )
        stage_passed = completed.returncode == 0 and not validation_errors
        result = {
            "stage_id": stage["stage_id"],
            "kind": stage["kind"],
            "status": "pass" if stage_passed else "fail",
            "exit_code": completed.returncode,
            "started_at": started,
            "finished_at": utc_now(),
            "wall_seconds": round(time.monotonic() - wall_started, 6),
            **timing,
            "cache_state": "artifact_present_before" if artifact_present else "cold_artifact_path",
            "expected_build_id": stage.get("expected_build_id"),
            "artifact": str(artifact) if artifact else None,
            "declared_output_bytes": manifest_product_bytes(artifact) if artifact and artifact.is_dir() else None,
            "stdout_log": str(stdout_path),
            "stderr_log": str(stderr_path),
            "time_log": str(time_path),
            "command": command,
            **result_validation,
        }
        report["stages"].append(result)
        write_object_atomic(report_path, report)
        if not stage_passed:
            failed = True
            break

    measured = [stage for stage in report["stages"] if "wall_seconds" in stage]
    report["finished_at"] = utc_now()
    report["status"] = "fail" if failed else "pass"
    report["totals"] = {
        "pipeline_wall_seconds": round(time.monotonic() - total_started, 6),
        "measured_stage_wall_seconds": round(sum(float(stage["wall_seconds"]) for stage in measured), 6),
        "measured_stage_cpu_seconds": round(sum(float(stage["cpu_seconds"]) for stage in measured), 6),
        "maximum_stage_rss_kib": max((int(stage["max_rss_kib"]) for stage in measured), default=0),
        "filesystem_input_blocks": sum(int(stage["filesystem_input_blocks"]) for stage in measured),
        "filesystem_output_blocks": sum(int(stage["filesystem_output_blocks"]) for stage in measured),
    }
    write_object_atomic(report_path, report)
    write_object_atomic(latest_path, report)
    return report, report_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--mode", choices=("verify", "full"), default="verify")
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    args = parser.parse_args()
    config = read_object(args.config.resolve())
    lock = acquire_lock(args.run_root.resolve())
    try:
        report, report_path = run_pipeline(
            config,
            args.mode,
            args.run_root.resolve(),
            args.report_root.resolve(),
        )
    finally:
        lock.close()
    print(f"E7 timed pipeline {report['status']}: {report_path}")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
