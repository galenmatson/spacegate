from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import prune_published_downloads as retention  # noqa: E402


def test_plan_retains_current_and_newest_rollback_archives(tmp_path: Path) -> None:
    dl_root = tmp_path / "dl"
    db_root = dl_root / "db"
    reports_root = dl_root / "reports"
    db_root.mkdir(parents=True)
    reports_root.mkdir()
    archives = []
    for index in range(5):
        archive = db_root / f"build-{index}.7z"
        archive.write_bytes(str(index).encode())
        archive.touch()
        archives.append(archive)
        report = reports_root / f"build-{index}"
        report.mkdir()
        (report / "report.json").write_text("{}\n")
    (dl_root / "current").symlink_to(Path("db") / archives[-1].name)
    plan = retention.build_plan(dl_root, keep_archives=3)
    retained = {Path(value).name for value in plan["retained_archives"]}
    candidate_paths = {Path(row["path"]).name for row in plan["candidates"]}
    assert retained == {"build-2.7z", "build-3.7z", "build-4.7z"}
    assert candidate_paths == {
        "build-0.7z",
        "build-1.7z",
        "build-0",
        "build-1",
    }


def test_plan_fails_when_current_pointer_escapes_archive_root(tmp_path: Path) -> None:
    dl_root = tmp_path / "dl"
    (dl_root / "db").mkdir(parents=True)
    outside = tmp_path / "outside.7z"
    outside.write_bytes(b"outside")
    (dl_root / "current").symlink_to(outside)
    try:
        retention.build_plan(dl_root, keep_archives=3)
    except ValueError as error:
        assert "escapes" in str(error)
    else:
        raise AssertionError("unsafe current pointer was accepted")
