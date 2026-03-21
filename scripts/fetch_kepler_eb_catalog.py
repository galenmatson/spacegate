#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import http.cookiejar
import json
import os
import re
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path

INDEX_URL = "https://keplerebs.villanova.edu/"
USER_AGENT = "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"
SOURCE_VERSION = "third_revision_2019-08-08"


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"{ts} {msg}", flush=True)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fetch_catalog_csv() -> bytes:
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

    get_req = urllib.request.Request(INDEX_URL, headers={"User-Agent": USER_AGENT})
    with opener.open(get_req, timeout=120) as response:
        html = response.read().decode("utf-8", errors="replace")

    token_match = re.search(
        r"name='csrfmiddlewaretoken'\s+value='([^']+)'", html, flags=re.IGNORECASE
    )
    if not token_match:
        raise RuntimeError("Unable to locate Kepler EB CSRF token on index page")
    csrf = token_match.group(1)

    payload = urllib.parse.urlencode(
        {"csrfmiddlewaretoken": csrf, "format": "file_csv"}
    ).encode("utf-8")
    post_req = urllib.request.Request(
        INDEX_URL,
        data=payload,
        headers={
            "User-Agent": USER_AGENT,
            "Referer": INDEX_URL,
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    with opener.open(post_req, timeout=180) as response:
        body = response.read()

    text = body.decode("utf-8", errors="replace")
    if "#KIC,period" not in text:
        snippet = text[:300].replace("\n", "\\n")
        raise RuntimeError(
            "Kepler EB payload validation failed (missing expected CSV header). "
            f"Payload head: {snippet}"
        )
    return body


def write_manifest(manifest_path: Path, out_path: Path, row_count: int) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    entries = [
        {
            "source_name": "kepler_eb_catalog",
            "source_version": SOURCE_VERSION,
            "url": INDEX_URL,
            "dest_path": str(out_path),
            "retrieved_at": ts,
            "checked_at": ts,
            "sha256": sha256_file(out_path),
            "bytes_written": out_path.stat().st_size,
            "row_count": row_count,
        }
    ]
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(entries, indent=2) + "\n", encoding="utf-8")


def count_rows(csv_bytes: bytes) -> int:
    count = 0
    for line in csv_bytes.decode("utf-8", errors="replace").splitlines():
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("kic,") or line.lower().startswith("#kic,"):
            continue
        if "," in line:
            count += 1
    return count


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch Kepler Eclipsing Binary Catalog CSV export."
    )
    parser.add_argument("--state-dir", default=None)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    env_state = args.state_dir or os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if env_state:
        state_dir = Path(env_state)
    else:
        shared_state = Path("/data/spacegate/data")
        state_dir = shared_state if shared_state.exists() else (root / "data")
    raw_path = state_dir / "raw" / "kepler_eb" / "kepler_eb_catalog.csv"
    manifest_path = state_dir / "reports" / "manifests" / "kepler_eb_manifest.json"

    log("Kepler EB fetch start")
    payload = fetch_catalog_csv()
    row_count = count_rows(payload)

    raw_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb",
        delete=False,
        dir=str(raw_path.parent),
        prefix=raw_path.name + ".tmp.",
    ) as tmp_file:
        tmp_path = Path(tmp_file.name)
        tmp_file.write(payload)
    tmp_path.replace(raw_path)

    write_manifest(manifest_path, raw_path, row_count)
    log(
        "Kepler EB fetch complete "
        f"(rows={row_count:,}, bytes={raw_path.stat().st_size:,}, manifest={manifest_path})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
