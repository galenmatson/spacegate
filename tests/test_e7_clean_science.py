from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_e7_clean_science as compiler  # noqa: E402


def test_checked_in_clean_science_policy_is_fail_closed() -> None:
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    compiler.validate_policy(policy)
    assert policy["rules"]["open_stability_databases"] is False
    assert policy["rules"]["copy_stability_scientific_values"] is False
    assert policy["rules"]["allow_core_classification_fallback"] is False


def test_policy_rejects_stability_fallback() -> None:
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    policy["rules"]["allow_core_classification_fallback"] = True
    with pytest.raises(ValueError, match="unsafe E7 clean science rules"):
        compiler.validate_policy(policy)


def test_failed_phase_is_written_incrementally(tmp_path: Path) -> None:
    trace = tmp_path / "trace.json"
    timings = compiler.Timings(trace)

    def fail() -> None:
        raise RuntimeError("test failure")

    with pytest.raises(RuntimeError, match="test failure"):
        timings.run("failure_probe", fail)
    payload = json.loads(trace.read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    assert payload["timing"]["phases"][-1]["phase"] == "failure_probe"
    assert payload["timing"]["phases"][-1]["status"] == "fail"
