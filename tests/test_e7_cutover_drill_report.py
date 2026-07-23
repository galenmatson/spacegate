from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/report_e7_cutover_drill.py"
SPEC = importlib.util.spec_from_file_location("report_e7_cutover_drill", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_elapsed_seconds_accepts_gnu_time_shapes() -> None:
    assert MODULE.elapsed_seconds("4.25") == 4.25
    assert MODULE.elapsed_seconds("2:42.34") == 162.34
    assert MODULE.elapsed_seconds("1:02:03") == 3723.0


def test_parse_gnu_time_extracts_required_metrics(tmp_path: Path) -> None:
    path = tmp_path / "step.time"
    path.write_text(
        "\n".join(
            [
                '\tCommand being timed: "example --flag"',
                "\tUser time (seconds): 1.25",
                "\tSystem time (seconds): 0.50",
                "\tElapsed (wall clock) time (h:mm:ss or m:ss): 2:42.00",
                "\tMaximum resident set size (kbytes): 1024",
                "\tFile system inputs: 8",
                "\tFile system outputs: 16",
                "\tExit status: 0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    result = MODULE.parse_gnu_time(path)
    assert result == {
        "path": str(path),
        "command": "example --flag",
        "wall_seconds": 162.0,
        "user_cpu_seconds": 1.25,
        "system_cpu_seconds": 0.5,
        "peak_rss_kib": 1024,
        "filesystem_inputs": 8,
        "filesystem_outputs": 16,
        "exit_status": 0,
    }
