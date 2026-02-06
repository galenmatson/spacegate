#!/usr/bin/env python3
import sys
from typing import Any, Dict

import requests


def require_keys(obj: Dict[str, Any], keys, label: str):
    missing = [key for key in keys if key not in obj]
    if missing:
        raise AssertionError(f"{label} missing keys: {missing}")


def main():
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000/api/v1"

    health = requests.get(f"{base_url}/health", timeout=10)
    health.raise_for_status()
    health_json = health.json()
    require_keys(health_json, ["status", "build_id", "db_path", "time_utc"], "health")

    search = requests.get(
        f"{base_url}/systems/search",
        params={"q": "sol", "limit": 1},
        timeout=10,
    )
    search.raise_for_status()
    search_json = search.json()
    require_keys(search_json, ["items", "has_more", "next_cursor"], "search")
    if not search_json["items"]:
        raise AssertionError("search returned no items")

    first = search_json["items"][0]
    require_keys(first, ["system_id", "system_name", "provenance"], "search item")

    detail = requests.get(
        f"{base_url}/systems/{first['system_id']}",
        timeout=10,
    )
    detail.raise_for_status()
    detail_json = detail.json()
    require_keys(detail_json, ["system", "stars", "planets"], "detail")
    require_keys(detail_json["system"], ["system_id", "system_name", "provenance"], "detail.system")

    print("Integration test passed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Integration test failed: {exc}")
        sys.exit(1)
