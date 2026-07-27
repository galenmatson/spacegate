#!/usr/bin/env python3
"""Package one verified Smart Tag artifact as a deterministic release archive."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import tarfile
import tempfile
from pathlib import Path
from typing import Any


EXPECTED_SCHEMA = "spacegate.smart_tags_manifest.v2"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_manifest(artifact: Path) -> dict[str, Any]:
    manifest = json.loads((artifact / "manifest.json").read_text(encoding="utf-8"))
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != EXPECTED_SCHEMA
        or manifest.get("status") != "pass"
        or manifest.get("sample_limit") is not None
    ):
        raise ValueError("only a verified full Smart Tag v2 artifact can be packaged")
    return manifest


def package(artifact: Path, output: Path) -> dict[str, Any]:
    artifact = artifact.resolve(strict=True)
    manifest = load_manifest(artifact)
    members = {"manifest.json": artifact / "manifest.json"}
    for key, spec in (manifest.get("artifacts") or {}).items():
        relative = Path(str(spec.get("path") or ""))
        if relative.is_absolute() or len(relative.parts) != 1 or ".." in relative.parts:
            raise ValueError(f"unsafe Smart Tag artifact path for {key}")
        path = (artifact / relative).resolve(strict=True)
        if not path.is_relative_to(artifact) or not path.is_file():
            raise ValueError(f"missing Smart Tag artifact for {key}")
        if (
            path.stat().st_size != spec.get("bytes")
            or sha256_file(path) != spec.get("sha256")
        ):
            raise ValueError(f"Smart Tag artifact checksum mismatch for {key}")
        members[relative.as_posix()] = path
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=output.parent, prefix=f".{output.name}.", delete=False
    ) as handle:
        temporary = Path(handle.name)
    try:
        with temporary.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as zipped:
                with tarfile.open(fileobj=zipped, mode="w") as archive:
                    for name in sorted(members):
                        content = members[name].read_bytes()
                        info = tarfile.TarInfo(name=name)
                        info.size = len(content)
                        info.mode = 0o644
                        info.uid = 0
                        info.gid = 0
                        info.uname = ""
                        info.gname = ""
                        info.mtime = 0
                        archive.addfile(info, io.BytesIO(content))
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return {
        "status": "pass",
        "build_id": manifest["build_id"],
        "registry_hash": manifest["registry_hash"],
        "archive": str(output),
        "bytes": output.stat().st_size,
        "sha256": sha256_file(output),
        "member_count": len(members),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = package(args.artifact, args.output)
    except (OSError, ValueError, json.JSONDecodeError, tarfile.TarError) as exc:
        raise SystemExit(f"Smart Tag packaging failed: {exc}") from exc
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
