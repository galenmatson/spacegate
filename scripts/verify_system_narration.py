#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, Iterable, List

import requests


REQUIRED_BLOCKS = {
    "what_you_are_looking_at",
    "why_this_system_matters",
    "infrared_view",
    "what_we_know",
    "what_remains_uncertain",
    "further_exploration",
}

DEFAULT_QUERIES = [
    "Tau Ceti",
    "TRAPPIST-1",
    "Alpha Centauri",
    "Epsilon Eridani",
    "Sirius",
    "55 Cnc",
    "Luhman 16",
    "WISE 0855",
]


def api_url(base_url: str, path: str) -> str:
    base = base_url.rstrip("/")
    if base.endswith("/api/v1"):
        return f"{base}/{path.lstrip('/')}"
    return f"{base}/api/v1/{path.lstrip('/')}"


def get_json(base_url: str, path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    response = requests.get(api_url(base_url, path), params=params or {}, timeout=30)
    response.raise_for_status()
    return response.json()


def first_system_id_for_query(base_url: str, query: str) -> int:
    payload = get_json(base_url, "/systems/search", {"q": query, "limit": 5})
    rows = payload.get("items") or payload.get("results") or []
    if not rows:
        raise AssertionError(f"No search result for {query!r}")
    system_id = rows[0].get("system_id")
    if system_id is None:
        raise AssertionError(f"Search result for {query!r} missing system_id")
    return int(system_id)


def verify_system(base_url: str, query: str) -> List[str]:
    failures: List[str] = []
    system_id = first_system_id_for_query(base_url, query)
    detail = get_json(base_url, f"/systems/{system_id}")
    system_name = (detail.get("system") or {}).get("display_name") or (detail.get("system") or {}).get("system_name") or query
    blocks = detail.get("narrative_blocks") or []
    if not isinstance(blocks, list) or not blocks:
        return [f"{query}: {system_name} returned no narrative_blocks"]
    kinds = {str(block.get("block_kind") or "") for block in blocks}
    missing = REQUIRED_BLOCKS - kinds
    if missing:
        failures.append(f"{query}: {system_name} missing blocks {sorted(missing)}")
    for block in blocks:
        body = str(block.get("body_text") or block.get("body_markdown") or "").strip()
        title = str(block.get("title") or "").strip()
        method = str(block.get("generation_method") or "").strip()
        version = str(block.get("generator_version") or "").strip()
        if not title:
            failures.append(f"{query}: block {block.get('block_kind')} missing title")
        if len(body) < 80:
            failures.append(f"{query}: block {block.get('block_kind')} body too short")
        if not method or not version:
            failures.append(f"{query}: block {block.get('block_kind')} missing generator metadata")
        if block.get("block_kind") == "infrared_view" and "artist impression" not in body.lower():
            failures.append(f"{query}: infrared block does not state survey imagery is not an artist impression")
    return failures


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify deterministic system narration API blocks.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="API base URL, with or without /api/v1")
    parser.add_argument("--query", action="append", default=[], help="Specific golden query to verify; repeatable")
    args = parser.parse_args(list(argv) if argv is not None else None)

    failures: List[str] = []
    queries = args.query or DEFAULT_QUERIES
    for query in queries:
        try:
            failures.extend(verify_system(args.base_url, query))
        except Exception as exc:
            failures.append(f"{query}: {exc}")
    if failures:
        print("System narration verification failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print(f"System narration verification passed for {len(queries)} systems.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
