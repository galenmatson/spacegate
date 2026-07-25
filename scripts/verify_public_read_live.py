#!/usr/bin/env python3
"""Verify the promoted public-read contracts through the live API."""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_BASE_URL = "http://127.0.0.1:8000/api/v1"


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def request_json(
    base_url: str,
    path: str,
    params: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any], dict[str, str]]:
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": "spacegate-public-read-verifier/1",
        },
    )
    try:
        response = urllib.request.urlopen(request, timeout=60)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        payload = response.read()
        headers = {key.lower(): value for key, value in response.headers.items()}
        if headers.get("content-encoding", "").lower() == "gzip" or payload.startswith(b"\x1f\x8b"):
            payload = gzip.decompress(payload)
        body = json.loads(payload.decode("utf-8")) if payload else {}
        return response.status, body, headers


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    started = utc_now()
    failures: list[str] = []
    checks: list[dict[str, Any]] = []

    def check(name: str, fn) -> None:
        try:
            result = fn()
            checks.append({"name": name, "status": "pass", **(result or {})})
        except Exception as exc:  # noqa: BLE001 - a release verifier must report every failure
            failures.append(f"{name}: {exc}")
            checks.append({"name": name, "status": "fail", "error": str(exc)})

    status, initial_health, _ = request_json(args.base_url, "health")
    require(status == 200, f"initial health returned {status}")
    build_id = initial_health.get("build_id")
    initial_stats = initial_health.get("public_read_runtime") or {}

    def search_case(
        query: str,
        *,
        match_status: str | None = None,
        resolution_status: str | None = None,
        display_name: str | None = None,
        match_resolution: str | None = None,
    ) -> dict[str, Any]:
        status_code, payload, _ = request_json(
            args.base_url,
            "systems/search",
            {"q": query, "sort": "match", "limit": 5},
        )
        require(status_code == 200, f"{query}: HTTP {status_code}")
        resolution = payload.get("query_resolution")
        items = payload.get("items") or []
        if match_status is not None:
            require(isinstance(resolution, dict), f"{query}: missing resolution")
            require(resolution.get("match_status") == match_status, f"{query}: {resolution}")
            require(
                resolution.get("resolution_status") == resolution_status,
                f"{query}: {resolution}",
            )
            if match_status == "exact_no_match":
                require(not items, f"{query}: exact no-match returned unrelated items")
        if display_name is not None:
            require(items, f"{query}: no results")
            require(items[0].get("display_name") == display_name, f"{query}: {items[0]}")
        if match_resolution is not None:
            require(items[0].get("match_resolution") == match_resolution, f"{query}: {items[0]}")
        require(payload.get("read_backend") == "public_read_v2", f"{query}: wrong backend")
        return {
            "query": query,
            "item_count": len(items),
            "system_id": items[0].get("system_id") if items else None,
            "query_resolution": resolution,
            "match_resolution": items[0].get("match_resolution") if items else None,
        }

    check(
        "accepted_tic",
        lambda: search_case(
            "TIC 307210830",
            match_status="exact_match",
            resolution_status="accepted",
        ),
    )
    check(
        "accepted_toi",
        lambda: search_case(
            "TOI-700.01",
            match_status="exact_match",
            resolution_status="accepted",
        ),
    )
    check(
        "deferred_tic",
        lambda: search_case(
            "TIC 150320610",
            match_status="exact_no_match",
            resolution_status="missing",
        ),
    )
    check(
        "deferred_toi",
        lambda: search_case(
            "TOI-6725.01",
            match_status="exact_no_match",
            resolution_status="missing",
        ),
    )
    check(
        "exact_name",
        lambda: search_case("Castor", display_name="Castor", match_resolution="exact"),
    )
    check(
        "bounded_fuzzy_name",
        lambda: search_case("Castpr", display_name="Castor", match_resolution="fuzzy"),
    )

    singleton_id = 17786544
    complex_id = 17784468

    def endpoint_case(
        path: str,
        *,
        expected_backend: str | None = None,
        expected_cache: str | None = None,
        expected_scene_tier: str | None = None,
    ) -> dict[str, Any]:
        status_code, payload, headers = request_json(args.base_url, path)
        require(status_code == 200, f"{path}: HTTP {status_code}")
        if expected_backend is not None:
            require(payload.get("read_backend") == expected_backend, f"{path}: {payload.get('read_backend')}")
        cache = headers.get("x-spacegate-simulation-scene-cache")
        if expected_cache is not None:
            require(cache == expected_cache, f"{path}: cache={cache}")
        if expected_scene_tier is not None:
            require(payload.get("scene_tier") == expected_scene_tier, f"{path}: tier={payload.get('scene_tier')}")
        return {
            "path": path,
            "read_backend": payload.get("read_backend"),
            "scene_tier": payload.get("scene_tier"),
            "cache": cache,
            "content_encoding": headers.get("content-encoding"),
        }

    check(
        "singleton_summary",
        lambda: endpoint_case(
            f"systems/{singleton_id}/summary",
            expected_backend="public_read_v2",
        ),
    )
    check(
        "singleton_detail",
        lambda: endpoint_case(
            f"systems/{singleton_id}",
            expected_backend="public_read_v2_singleton",
        ),
    )
    check(
        "singleton_scene_seed",
        lambda: endpoint_case(
            f"systems/{singleton_id}/scene-seed",
            expected_backend="public_read_v2",
        ),
    )
    check(
        "singleton_simulation_scene",
        lambda: endpoint_case(
            f"systems/{singleton_id}/simulation-scene",
            expected_cache="singleton-seed",
            expected_scene_tier="singleton_seed",
        ),
    )
    check(
        "complex_detail_bundle",
        lambda: endpoint_case(
            f"systems/{complex_id}",
            expected_backend="public_read_v2_bundle",
        ),
    )
    check(
        "complex_hierarchy_bundle",
        lambda: endpoint_case(
            f"systems/{complex_id}/hierarchy",
            expected_backend="public_read_v2_bundle",
        ),
    )

    def prebuilt_scene_case() -> dict[str, Any]:
        status_code, payload, headers = request_json(
            args.base_url,
            f"systems/{complex_id}/simulation-scene",
        )
        require(status_code == 200, f"prebuilt scene HTTP {status_code}")
        require(
            headers.get("x-spacegate-simulation-scene-cache") == "prebuilt",
            f"prebuilt scene cache={headers.get('x-spacegate-simulation-scene-cache')}",
        )
        tree = (payload.get("render_scene") or {}).get("simulation_tree") or {}
        diagnostics = tree.get("diagnostics") or {}
        require(int(diagnostics.get("nested_orbit_count") or 0) >= 1, "prebuilt scene lost nested orbit")
        return {
            "path": f"systems/{complex_id}/simulation-scene",
            "cache": "prebuilt",
            "content_encoding": headers.get("content-encoding"),
            "nested_orbit_count": diagnostics.get("nested_orbit_count"),
        }

    check("complex_prebuilt_scene", prebuilt_scene_case)

    status, final_health, _ = request_json(args.base_url, "health")
    require(status == 200, f"final health returned {status}")
    final_stats = final_health.get("public_read_runtime") or {}
    stat_delta = {
        key: int(final_stats.get(key, 0)) - int(initial_stats.get(key, 0))
        for key in sorted(set(initial_stats) | set(final_stats))
    }
    check(
        "zero_compatibility_fallbacks",
        lambda: (
            require(
                stat_delta.get("compatibility_fallbacks", 0) == 0,
                f"compatibility fallback delta={stat_delta.get('compatibility_fallbacks')}",
            ),
            require(
                stat_delta.get("incompatible_artifacts", 0) == 0,
                f"incompatible artifact delta={stat_delta.get('incompatible_artifacts')}",
            ),
            {"counter_delta": stat_delta},
        )[-1],
    )

    report = {
        "schema_version": "spacegate.public_read_live_verification.v1",
        "status": "pass" if not failures else "fail",
        "started_at_utc": started,
        "completed_at_utc": utc_now(),
        "base_url": args.base_url,
        "build_id": build_id,
        "checks": checks,
        "failures": failures,
        "initial_runtime_stats": initial_stats,
        "final_runtime_stats": final_stats,
        "runtime_stat_delta": stat_delta,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
