from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts/run_runtime_capacity_staircase.py"
SPEC = importlib.util.spec_from_file_location(
    "run_runtime_capacity_staircase", MODULE_PATH
)
assert SPEC is not None and SPEC.loader is not None
STAIRCASE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = STAIRCASE
SPEC.loader.exec_module(STAIRCASE)


def test_parse_steps_accepts_unique_increasing_values() -> None:
    assert STAIRCASE.parse_steps("1,2,4,8") == [1, 2, 4, 8]


@pytest.mark.parametrize("raw", ["", "0,1", "2,1", "1,1"])
def test_parse_steps_rejects_unsafe_sequences(raw: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        STAIRCASE.parse_steps(raw)
