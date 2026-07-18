from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "prune_state_retention.sh"


def test_retention_recognizes_minute_and_second_build_ids(tmp_path: Path) -> None:
    state = tmp_path / "state"
    out = state / "out"
    reports = state / "reports"
    served = state / "served"
    out.mkdir(parents=True)
    reports.mkdir()
    served.mkdir()

    minute_old = out / "20260715T1200Z_old"
    second_old = out / "20260715T120001Z_old_seconds"
    active = out / "20260716T1410Z_active"
    named_workspace = out / "20260715T_named_workspace"
    for path in (minute_old, second_old, active, named_workspace):
        path.mkdir()
        (path / "payload").write_bytes(b"x")
    (served / "current").symlink_to(Path("../out") / active.name)

    result = subprocess.run(
        [
            str(SCRIPT),
            "--state-dir",
            str(state),
            "--keep-builds",
            "0",
            "--keep-reports",
            "0",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert str(minute_old) in result.stdout
    assert str(second_old) in result.stdout
    assert str(active) not in result.stdout
    assert str(named_workspace) not in result.stdout


def test_retention_preserves_explicit_and_file_listed_builds(tmp_path: Path) -> None:
    state = tmp_path / "state"
    out = state / "out"
    reports = state / "reports"
    served = state / "served"
    out.mkdir(parents=True)
    reports.mkdir()
    served.mkdir()

    removable = "20260715T1200Z_removable"
    explicit = "20260715T1201Z_published"
    listed = "20260715T1202Z_rollback"
    for build_id in (removable, explicit, listed):
        (out / build_id).mkdir()
        (reports / build_id).mkdir()
    protect_file = tmp_path / "protected.txt"
    protect_file.write_text(f"# retained rollback\n{listed}\n", encoding="utf-8")

    result = subprocess.run(
        [
            str(SCRIPT),
            "--state-dir",
            str(state),
            "--keep-builds",
            "0",
            "--keep-reports",
            "0",
            "--protect-build",
            explicit,
            "--protect-file",
            str(protect_file),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert str(out / removable) in result.stdout
    assert str(reports / removable) in result.stdout
    assert str(out / explicit) not in result.stdout
    assert str(out / listed) not in result.stdout
    assert f"Explicit protected builds (2):" in result.stdout


def test_retention_rejects_invalid_protected_build_id(tmp_path: Path) -> None:
    result = subprocess.run(
        [str(SCRIPT), "--state-dir", str(tmp_path), "--protect-build", "../escape"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert "Invalid protected build ID" in result.stderr
