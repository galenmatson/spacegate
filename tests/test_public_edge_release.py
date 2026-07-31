from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
import os
import shutil
import sqlite3
import subprocess
import tarfile
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "public_edge_release.py"
SPEC = importlib.util.spec_from_file_location("public_edge_release", SCRIPT)
assert SPEC and SPEC.loader
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def make_public_read(path: Path, build_id: str) -> dict[str, object]:
    con = sqlite3.connect(path)
    try:
        con.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT)")
        con.execute("INSERT INTO metadata VALUES('build_id',?)", [build_id])
        con.commit()
    finally:
        con.close()
    return {
        "schema_version": "spacegate.public_read_manifest.v1",
        "build_id": build_id,
        "status": "pass",
        "sample_limit": None,
        "projection_schema_version": "spacegate.public_read.v2",
        "search_schema_version": "spacegate.search.v2",
        "artifact": {
            "path": path.name,
            "bytes": path.stat().st_size,
            "sha256": sha256(path),
        },
    }


def make_scenes(path: Path, build_id: str) -> dict[str, object]:
    scene = gzip.compress(
        json.dumps({"materialization": {"build_id": build_id}}).encode(),
        mtime=0,
    )
    entry = {
        "system_id": 7,
        "path": "scenes/system_7.json.gz",
        "bytes": len(scene),
        "sha256": hashlib.sha256(scene).hexdigest(),
    }
    embedded = {
        "schema_version": "spacegate.simulation_scene_frozen_set.v1",
        "build_id": build_id,
        "required_count": 1,
        "scene_count": 1,
        "entries": [entry],
    }
    with tarfile.open(path, "w:gz") as archive:
        manifest_path = path.parent / "embedded.json"
        manifest_path.write_text(json.dumps(embedded), encoding="utf-8")
        scene_path = path.parent / "system_7.json.gz"
        scene_path.write_bytes(scene)
        archive.add(manifest_path, arcname="manifest.json")
        archive.add(scene_path, arcname=entry["path"])
    return {
        **embedded,
        "verification": {"status": "pass"},
        "archive": {
            "path": path.name,
            "bytes": path.stat().st_size,
            "sha256": sha256(path),
        },
    }


def make_smart_tags(path: Path, build_id: str) -> tuple[dict[str, object], Path]:
    root = path.parent / "smart-tags"
    root.mkdir()
    artifact_names = {
        "database": "smart_tags.sqlite",
        "assignments": "assignments.parquet",
        "source_contributions": "source_contributions.parquet",
        "registry": "registry.json",
        "coverage": "coverage.json",
        "quarantine": "quarantine.json",
        "proposal_accounting": "proposal_accounting.json",
        "source_accounting": "source_accounting.json",
        "timings": "timings.json",
    }
    artifacts = {}
    for key, filename in artifact_names.items():
        target = root / filename
        target.write_text(f"{key}:{build_id}\n", encoding="utf-8")
        artifacts[key] = {
            "path": filename,
            "bytes": target.stat().st_size,
            "sha256": sha256(target),
        }
    manifest = {
        "schema_version": "spacegate.smart_tags_manifest.v2",
        "tag_schema_version": "spacegate.smart_tags.v2",
        "assignment_schema_version": "spacegate.smart_tag_assignments.v2",
        "source_summary_schema_version": "spacegate.smart_tag_source_summary.v3",
        "source_contribution_schema_version": "spacegate.smart_tag_source_contributions.v1",
        "compiler_version": "spacegate.smart_tags_compiler.v2.3",
        "status": "pass",
        "build_id": build_id,
        "registry_hash": hashlib.sha256(b"registry").hexdigest(),
        "sample_limit": None,
        "counts": {"tag_assignments": 3},
        "artifacts": artifacts,
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with tarfile.open(path, "w:gz") as archive:
        for member in sorted(root.iterdir()):
            archive.add(member, arcname=member.name)
    return manifest, manifest_path


def make_fixture(tmp_path: Path) -> tuple[Path, Path, str]:
    if shutil.which("7z") is None:
        pytest.skip("7z is required")
    build_id = "test_public_edge_build"
    source = tmp_path / "source"
    build = source / build_id
    for relative in (
        "core.duckdb",
        "arm.duckdb",
        "canonical_hierarchy.duckdb",
        "disc.duckdb",
        "map_tiles/index.json",
    ):
        path = build / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(relative, encoding="utf-8")
    archive = source / f"{build_id}.7z"
    subprocess.run(
        ["7z", "a", "-t7z", str(archive), build.name],
        cwd=source,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    public = source / "public_read.sqlite"
    public_manifest = make_public_read(public, build_id)
    public_manifest_path = source / "public-read-manifest.json"
    public_manifest_path.write_text(json.dumps(public_manifest), encoding="utf-8")
    scenes = source / "simulation_scenes.tar.gz"
    scene_manifest = make_scenes(scenes, build_id)
    scene_manifest_path = source / "scene-manifest.json"
    scene_manifest_path.write_text(json.dumps(scene_manifest), encoding="utf-8")
    smart_tags = source / "smart_tags.tar.gz"
    smart_tag_manifest, smart_tag_manifest_path = make_smart_tags(
        smart_tags, build_id
    )
    manifest = source / "release.json"
    args = type(
        "Args",
        (),
        {
            "build_id": build_id,
            "build_dir": build,
            "scientific_archive": archive,
            "public_read": public,
            "public_read_manifest": public_manifest_path,
            "simulation_scenes": scenes,
            "simulation_scene_manifest": scene_manifest_path,
            "smart_tags": smart_tags,
            "smart_tag_manifest": smart_tag_manifest_path,
            "output": manifest,
        },
    )()
    release.command_create(args)
    return manifest, source, build_id


def stage_args(manifest: Path, state: Path, incoming: Path) -> object:
    return type(
        "Args",
        (),
        {"manifest": manifest, "state_dir": state, "incoming_dir": incoming},
    )()


def test_release_stages_activates_and_rolls_back(tmp_path: Path) -> None:
    manifest, source, build_id = make_fixture(tmp_path)
    state = tmp_path / "state"
    incoming = state / "incoming"
    incoming.mkdir(parents=True)
    previous = state / "out" / "previous"
    previous.mkdir(parents=True)
    (previous / "core.duckdb").write_text("previous", encoding="utf-8")
    (state / "served").mkdir()
    (state / "served" / "current").symlink_to(
        os.path.relpath(previous, state / "served")
    )
    value = release.validate_release(release.load_json(manifest))
    for role, spec in value["artifacts"].items():
        shutil.copy2(Path(spec["source_path"]), incoming / spec["transfer_filename"])

    args = stage_args(manifest, state, incoming)
    release.command_stage_scientific(args)
    release.command_stage_public_read(args)
    release.command_stage_scenes(args)
    release.command_stage_smart_tags(args)
    verified = release.command_verify_installed(args)
    assert verified["status"] == "pass"
    assert verified["scene_count"] == 1
    scene_cache = state / "cache" / "simulation_scenes" / build_id
    assert scene_cache.stat().st_mode & 0o777 == 0o755
    assert (scene_cache / "manifest.json").stat().st_mode & 0o777 == 0o644
    assert (scene_cache / "system_7.json.gz").stat().st_mode & 0o777 == 0o644
    smart_tag_target = (
        state
        / "derived"
        / "smart_tags"
        / build_id
        / value["smart_tag_manifest"]["registry_hash"]
    )
    assert (smart_tag_target / "smart_tags.sqlite").is_file()
    assert not (smart_tag_target.parent / "current").exists()

    activated = release.command_activate(
        type("Args", (), {"manifest": manifest, "state_dir": state})()
    )
    assert activated["previous_target"] == str(previous.resolve())
    assert (state / "served" / "current").resolve().name == build_id
    assert (smart_tag_target.parent / "current").resolve() == smart_tag_target

    rolled_back = release.command_rollback(
        type("Args", (), {"build_id": build_id, "state_dir": state})()
    )
    assert rolled_back["status"] == "pass"
    assert (state / "served" / "current").resolve() == previous.resolve()
    assert not (smart_tag_target.parent / "current").exists()


def test_release_rejects_path_escape(tmp_path: Path) -> None:
    manifest, _, _ = make_fixture(tmp_path)
    value = release.load_json(manifest)
    value["artifacts"]["public_read"]["transfer_filename"] = "../escape.sqlite"
    with pytest.raises(ValueError, match="unsafe transfer filename"):
        release.validate_release(value)


def test_release_rejects_old_smart_tag_source_summary_schema(tmp_path: Path) -> None:
    manifest, _, _ = make_fixture(tmp_path)
    value = release.load_json(manifest)
    value["smart_tag_manifest"]["source_summary_schema_version"] = (
        "spacegate.smart_tag_source_summary.v2"
    )
    with pytest.raises(ValueError, match="smart-tag manifest is incompatible"):
        release.validate_release(value)


def test_release_rejects_build_identity_mismatch(tmp_path: Path) -> None:
    manifest, source, _ = make_fixture(tmp_path)
    state = tmp_path / "state"
    incoming = state / "incoming"
    incoming.mkdir(parents=True)
    value = release.validate_release(release.load_json(manifest))
    spec = value["artifacts"]["public_read"]
    bad = source / "wrong.sqlite"
    make_public_read(bad, "wrong_build")
    incoming_file = incoming / spec["transfer_filename"]
    shutil.copy2(bad, incoming_file)
    spec["bytes"] = incoming_file.stat().st_size
    spec["sha256"] = sha256(incoming_file)
    value["public_read_manifest"]["artifact"]["bytes"] = spec["bytes"]
    value["public_read_manifest"]["artifact"]["sha256"] = spec["sha256"]
    bad_manifest = source / "bad-release.json"
    bad_manifest.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValueError, match="build identity mismatch"):
        release.command_stage_public_read(
            stage_args(bad_manifest, state, incoming)
        )


def test_runtime_env_configuration_is_bounded_and_verifiable(
    tmp_path: Path,
) -> None:
    manifest, _, _ = make_fixture(tmp_path)
    env = tmp_path / ".spacegate.local.env"
    env.write_text(
        "SPACEGATE_SESSION_SECRET=preserved\n"
        "SPACEGATE_API_DUCKDB_THREADS=4\n",
        encoding="utf-8",
    )
    configured = release.command_configure_runtime_env(
        type("Args", (), {"manifest": manifest, "env_file": env})()
    )
    assert configured["status"] == "pass"
    assert "SPACEGATE_SESSION_SECRET=preserved" in env.read_text()
    assert "SPACEGATE_API_DUCKDB_THREADS=1" in env.read_text()
    assert "SPACEGATE_SMART_TAGS_REQUIRED=1" in env.read_text()
    verified = release.command_verify_runtime_env(
        type("Args", (), {"manifest": manifest, "env_file": [env]})()
    )
    assert verified["status"] == "pass"


def test_verify_rejects_runtime_inaccessible_scene_cache(tmp_path: Path) -> None:
    manifest, source, build_id = make_fixture(tmp_path)
    state = tmp_path / "state"
    incoming = state / "incoming"
    incoming.mkdir(parents=True)
    value = release.validate_release(release.load_json(manifest))
    for role, spec in value["artifacts"].items():
        shutil.copy2(Path(spec["source_path"]), incoming / spec["transfer_filename"])

    args = stage_args(manifest, state, incoming)
    release.command_stage_scientific(args)
    release.command_stage_public_read(args)
    release.command_stage_scenes(args)
    release.command_stage_smart_tags(args)
    scene_cache = state / "cache" / "simulation_scenes" / build_id
    scene_cache.chmod(0o700)

    with pytest.raises(ValueError, match="not runtime-traversable"):
        release.command_verify_installed(args)
