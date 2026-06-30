#!/usr/bin/env python3
import argparse
from typing import Any

import requests


def get_json(base_url: str, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
    response = requests.get(f"{base_url}{path}", params=params, timeout=20)
    if response.status_code != 200:
        raise AssertionError(f"{path} returned {response.status_code}: {response.text[:500]}")
    payload = response.json()
    if not isinstance(payload, dict):
        raise AssertionError(f"{path} returned non-object JSON")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify the served build exposes deterministic snapshot fallback artifacts."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/api/v1")
    parser.add_argument("--map-limit", type=int, default=20)
    parser.add_argument("--min-map-snapshots", type=int, default=1)
    args = parser.parse_args()

    map_payload = get_json(args.base_url, "/map/systems", params={"limit": args.map_limit})
    items = map_payload.get("items") or []
    snapshot_items = [item for item in items if item.get("has_snapshot")]
    if len(snapshot_items) < args.min_map_snapshots:
        raise AssertionError(
            f"expected at least {args.min_map_snapshots} map snapshot(s), got {len(snapshot_items)} "
            f"from {len(items)} returned systems"
        )

    checked = []
    for item in snapshot_items[: min(3, len(snapshot_items))]:
        system_id = item.get("system_id")
        detail = get_json(args.base_url, f"/systems/{system_id}")
        snapshot = (detail.get("system") or {}).get("snapshot")
        if not isinstance(snapshot, dict) or not snapshot.get("url"):
            raise AssertionError(f"system {system_id} advertises has_snapshot but detail has no snapshot URL")
        asset_url = f"{args.base_url.rsplit('/api/v1', 1)[0]}{snapshot['url']}"
        response = requests.get(asset_url, timeout=20)
        if response.status_code != 200:
            raise AssertionError(f"snapshot asset {asset_url} returned {response.status_code}")
        content_type = response.headers.get("content-type", "")
        if "image/svg" not in content_type and not response.text.lstrip().startswith("<svg"):
            raise AssertionError(f"snapshot asset {asset_url} is not SVG-like: {content_type}")
        checked.append((system_id, snapshot.get("artifact_path") or snapshot.get("url")))

    print("Snapshot fallback verification passed:")
    print(f"- map snapshots: {len(snapshot_items)}/{len(items)} returned systems")
    for system_id, artifact in checked:
        print(f"- system {system_id}: {artifact}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
