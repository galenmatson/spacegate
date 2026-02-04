#!/usr/bin/env python3
import argparse
import datetime as dt
import json
from pathlib import Path


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def default_build_dir() -> Path:
    root = Path(__file__).resolve().parents[1]
    return (root / "served" / "current").resolve()


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate packs_manifest.json")
    parser.add_argument("--build-dir", default=str(default_build_dir()))
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    build_dir = Path(args.build_dir).resolve()
    if not build_dir.exists():
        raise SystemExit(f"Build dir not found: {build_dir}")

    build_id = build_dir.name
    packs_dir = build_dir / "packs"
    packs = []
    if packs_dir.exists():
        for entry in sorted(packs_dir.iterdir()):
            if not entry.is_dir():
                continue
            packs.append(
                {
                    "name": entry.name,
                    "artifact_path": f"packs/{entry.name}",
                    "format": "parquet",
                    "schema_version": "v1",
                }
            )

    manifest = {
        "schema_version": "v1",
        "build_id": build_id,
        "generated_at": utc_now(),
        "packs": packs,
    }

    output_path = Path(args.output) if args.output else build_dir / "packs_manifest.json"
    output_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
