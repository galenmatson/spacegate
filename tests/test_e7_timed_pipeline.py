from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "run_e7_timed_pipeline", ROOT / "scripts/run_e7_timed_pipeline.py"
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_checked_in_pipeline_is_valid_and_has_paired_clean_stages() -> None:
    config = MODULE.read_object(ROOT / "config/evidence_lake/e7_timed_pipeline.json")
    MODULE.validate_config(config)
    stage_ids = {stage["stage_id"] for stage in config["stages"]}
    for domain in (
        "foundation",
        "science",
        "wise",
        "clusters",
        "extended_objects",
        "runtime_core",
    ):
        assert f"clean_{domain}_compile" in stage_ids
        assert f"clean_{domain}_verify" in stage_ids


def test_pipeline_rejects_deployment_and_unbounded_artifacts() -> None:
    base = {
        "schema_version": "spacegate.e7_timed_pipeline.v1",
        "stages": [{"stage_id": "bad", "kind": "compiler", "command": ["deploy_antiproton.sh"]}],
    }
    with pytest.raises(ValueError, match="unsafe command"):
        MODULE.validate_config(base)
    base["stages"][0]["command"] = ["true"]
    base["stages"][0]["artifact"] = "../escape"
    with pytest.raises(ValueError, match="bounded"):
        MODULE.validate_config(base)


def test_pipeline_supports_named_bounded_artifact_roots(tmp_path: Path) -> None:
    config = {
        "schema_version": "spacegate.e7_timed_pipeline.v1",
        "artifact_root": str(tmp_path / "bulk"),
        "artifact_roots": {"data": str(tmp_path / "data")},
        "stages": [
            {
                "stage_id": "data_artifact",
                "kind": "compiler",
                "artifact_root": "data",
                "artifact": "family/build",
                "command": ["true"],
            }
        ],
    }
    MODULE.validate_config(config)
    config["stages"][0]["artifact_root"] = "missing"
    with pytest.raises(ValueError, match="unknown artifact root"):
        MODULE.validate_config(config)


def test_parse_gnu_time_fields(tmp_path: Path) -> None:
    timing = tmp_path / "time.txt"
    timing.write_text(
        "\n".join(
            (
                "User time (seconds): 2.25",
                "System time (seconds): 0.75",
                "Maximum resident set size (kbytes): 4096",
                "File system inputs: 12",
                "File system outputs: 34",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    parsed = MODULE.parse_gnu_time(timing)
    assert parsed == {
        "user_seconds": 2.25,
        "system_seconds": 0.75,
        "max_rss_kib": 4096,
        "filesystem_input_blocks": 12,
        "filesystem_output_blocks": 34,
        "cpu_seconds": 3.0,
    }


def test_command_values_preserve_venv_interpreter_path(tmp_path: Path) -> None:
    config = {"state_dir": str(tmp_path / "state"), "artifact_root": str(tmp_path / "artifacts")}
    values = MODULE.command_values(config, tmp_path / "reports")
    assert values["python"] == str(ROOT / ".venv/bin/python")


def test_stage_result_must_match_pinned_build(tmp_path: Path) -> None:
    report = tmp_path / "result.json"
    report.write_text('{"build_id":"wrong","status":"pass"}\n', encoding="utf-8")
    stage = {"expected_build_id": "expected"}
    result = MODULE.validate_stage_result(stage, ["tool", "--report", str(report)])
    assert result["result_validation_errors"] == {
        "build_id": {"expected": "expected", "actual": "wrong"}
    }


def test_cluster_deterministic_files_contribute_output_bytes(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact"
    artifact.mkdir()
    (artifact / "manifest.json").write_text(
        '{"deterministic_files":{"a":{"bytes":12},"b":{"bytes":34}}}\n',
        encoding="utf-8",
    )
    assert MODULE.manifest_product_bytes(artifact) == 46
