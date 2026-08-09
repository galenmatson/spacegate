#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import ssl
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def request(url: str, *, headers: dict[str, str] | None = None, insecure: bool = False) -> tuple[int, dict[str, str], bytes, float]:
    context = ssl._create_unverified_context() if insecure else None
    started = time.monotonic()
    try:
        with urlopen(Request(url, headers=headers or {}), timeout=90, context=context) as response:
            return response.status, dict(response.headers.items()), response.read(), time.monotonic() - started
    except HTTPError as exc:
        return exc.code, dict(exc.headers.items()), exc.read(), time.monotonic() - started


def header(headers: dict[str, str], name: str) -> str | None:
    target = name.lower()
    return next((value for key, value in headers.items() if key.lower() == target), None)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify WISE cache hits and browser validators through the public API contract.")
    parser.add_argument("--base-url", default="https://10.0.0.12")
    parser.add_argument("--system-id", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--insecure", action="store_true")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")
    metadata_url = f"{base}/api/v1/systems/{args.system_id}/infrared?size_arcmin=8"

    first_status, first_headers, first_body, first_seconds = request(metadata_url, insecure=args.insecure)
    if first_status != 200:
        raise SystemExit(f"metadata request failed: HTTP {first_status}")
    metadata = json.loads(first_body)
    preview_url = f"{base}{metadata['preview_url']}"
    preview_status, preview_headers, preview_body, preview_seconds = request(preview_url, insecure=args.insecure)
    if preview_status != 200 or not preview_body.startswith(b"\x89PNG\r\n\x1a\n"):
        raise SystemExit(f"preview request failed: HTTP {preview_status}")
    etag = header(preview_headers, "etag")
    if not etag:
        raise SystemExit("preview response has no ETag")
    validator_status, validator_headers, validator_body, validator_seconds = request(
        preview_url,
        headers={"If-None-Match": etag},
        insecure=args.insecure,
    )
    if validator_status != 304 or validator_body:
        raise SystemExit(f"validator request expected empty 304, got HTTP {validator_status}")

    report = {
        "schema_version": "survey_image_cache_verification_v1",
        "base_url": base,
        "system_id": args.system_id,
        "metadata": {
            "status": first_status,
            "seconds": round(first_seconds, 6),
            "cache_status": metadata.get("cache_status"),
            "cache": metadata.get("cache"),
        },
        "preview": {
            "status": preview_status,
            "seconds": round(preview_seconds, 6),
            "bytes": len(preview_body),
            "etag": etag,
            "cache_control": header(preview_headers, "cache-control"),
            "cache_status": header(preview_headers, "x-spacegate-wise-cache"),
        },
        "validator": {
            "status": validator_status,
            "seconds": round(validator_seconds, 6),
            "bytes": len(validator_body),
            "etag": header(validator_headers, "etag"),
        },
        "passed": True,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
