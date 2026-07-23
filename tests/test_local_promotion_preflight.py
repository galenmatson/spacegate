from __future__ import annotations

import importlib.util
import json
import os
import subprocess
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "preflight_local_promotion", ROOT / "scripts/preflight_local_promotion.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def make_build(state: Path, build_id: str) -> Path:
    build = state / "out" / build_id
    build.mkdir(parents=True)
    for name in ("core", "arm", "disc"):
        con = duckdb.connect(str(build / f"{name}.duckdb"))
        con.execute("CREATE TABLE build_metadata(key VARCHAR,value VARCHAR)")
        con.execute("INSERT INTO build_metadata VALUES ('build_id',?)", [build_id])
        con.close()
    (build / "canonical_hierarchy.duckdb").write_bytes(b"fixture")
    (build / "parquet").mkdir()
    (build / "parquet/core.parquet").write_bytes(b"fixture")
    (build / "map_tiles").mkdir()
    (build / "map_tiles/index.json").write_text(
        json.dumps({"build_id": build_id}), encoding="utf-8"
    )
    return build


def test_preflight_is_read_only_and_requires_bounded_rollback(tmp_path: Path) -> None:
    state = tmp_path / "state"
    candidate = make_build(state, "candidate")
    rollback = make_build(state, "rollback")
    served = state / "served"
    served.mkdir()
    (served / "current").symlink_to(os.path.relpath(rollback, served))
    before = (served / "current").readlink()

    report = MODULE.preflight(state, candidate.name)

    assert report["status"] == "pass"
    assert report["mutations_performed"] is False
    assert report["rollback"]["target"] == str(rollback.resolve())
    assert (served / "current").readlink() == before


def test_preflight_rejects_build_path_escape(tmp_path: Path) -> None:
    try:
        MODULE.bounded_build(tmp_path, "../outside")
    except ValueError as exc:
        assert "bounded path component" in str(exc)
    else:
        raise AssertionError("path escape was accepted")


def test_promote_script_uses_atomic_pointer_replacement() -> None:
    source = (ROOT / "scripts/promote_build.sh").read_text(encoding="utf-8")
    assert "os.replace(temporary, link_path)" in source
    assert "ln -sfn" not in source
    assert source.index("score_coolness.sh") < source.index("set_current_symlink \"$build_dir\"")
    assert "refusing to score immutable selected-fact build" in source


def test_immutable_build_promotion_defaults_to_no_scoring(tmp_path: Path) -> None:
    state = tmp_path / "state"
    candidate = make_build(state, "candidate")
    con = duckdb.connect(str(candidate / "core.duckdb"))
    con.execute(
        "INSERT INTO build_metadata VALUES "
        "('build_kind','e7_clean_runtime_core'),"
        "('scientific_values_from_selected_facts_only','1')"
    )
    con.close()
    env = {
        **os.environ,
        "SPACEGATE_STATE_DIR": str(state),
        "SPACEGATE_AUTO_SCORE_COOLNESS": "",
        "SPACEGATE_PROMOTE_ENFORCE_PROFILE_SLO": "0",
    }
    completed = subprocess.run(
        [str(ROOT / "scripts/promote_build.sh"), candidate.name],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "Skipping auto coolness scoring" in completed.stdout
    assert (state / "served/current").resolve() == candidate.resolve()
