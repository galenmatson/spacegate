#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import os
import re
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path

SEARCH_URL = "https://tessebs.villanova.edu/search_results"
USER_AGENT = "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"
SOURCE_VERSION = "search_results_in_catalog_v1"
DEFAULT_TIMEOUT_S = 90
DEFAULT_RETRIES = 5

QUERY_COLUMNS = [
    "in_catalog",
    "tic__tess_id",
    "sectors",
    "tic__ra",
    "tic__dec",
    "tic__Tmag",
    "tic__glon",
    "tic__glat",
    "tic__pmra",
    "tic__pmdec",
    "bjd0",
    "bjd0_uncert",
    "period",
    "period_uncert",
    "morph_coeff",
    "tic__teff",
    "tic__logg",
    "tic__abun",
    "source",
    "flags",
]

RAW_FIELDS = [
    "tic_id",
    "in_catalog",
    "sectors",
    "ra_deg",
    "dec_deg",
    "tmag",
    "glon_deg",
    "glat_deg",
    "pm_ra_mas_yr",
    "pm_dec_mas_yr",
    "bjd0",
    "bjd0_error",
    "period_days",
    "period_error_days",
    "morphology",
    "teff_k",
    "logg_cgs",
    "metallicity_dex",
    "source",
    "flags",
]

HEADER_FIELD_MAP = {
    "in catalog": "in_catalog",
    "tess id": "tic_id",
    "sectors": "sectors",
    "r a": "ra_deg",
    "dec": "dec_deg",
    "tmag": "tmag",
    "gal longitude": "glon_deg",
    "gal latitude": "glat_deg",
    "pm in r a mas yr": "pm_ra_mas_yr",
    "pm in dec mas yr": "pm_dec_mas_yr",
    "t0 bjd": "bjd0",
    "sigma t0 bjd": "bjd0_error",
    "p0 days": "period_days",
    "sigma p0 days": "period_error_days",
    "morphology": "morphology",
    "teff": "teff_k",
    "log g": "logg_cgs",
    "m h": "metallicity_dex",
    "source": "source",
    "flags": "flags",
}


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"{ts} {msg}", flush=True)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def clean_html_text(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value, flags=re.IGNORECASE | re.DOTALL)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_header(value: str) -> str:
    text = clean_html_text(value)
    text = text.replace("σ", "sigma")
    text = text.replace("Σ", "sigma")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    return text


def truthy_text(value: str) -> str:
    text = str(value or "").strip().lower()
    if text in {"true", "1", "t", "y", "yes"}:
        return "true"
    if text in {"false", "0", "f", "n", "no"}:
        return "false"
    return ""


def build_query(page: int, *, in_catalog_only: bool) -> str:
    params: list[tuple[str, str]] = [
        ("order_by", "tic__tess_id"),
        ("then_order_by", "none"),
        ("display_format", "html"),
        ("page", str(page)),
    ]
    if in_catalog_only:
        params.append(("incat_only", "1"))
    for column in QUERY_COLUMNS:
        params.append(("c", column))
    return SEARCH_URL + "?" + urllib.parse.urlencode(params)


def fetch_text(url: str, *, timeout_s: int, retries: int) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                payload = response.read()
            text = payload.decode("utf-8", errors="replace")
            if "<html" not in text[:200].lower():
                raise RuntimeError("Unexpected payload while fetching TESS EB HTML page")
            return text
        except Exception as exc:  # pragma: no cover - network error path
            last_exc = exc
            if attempt >= retries:
                break
            sleep_s = min(2**attempt, 30)
            log(
                f"TESS EB retry {attempt}/{retries - 1} failed: "
                f"{type(exc).__name__}: {exc}; sleeping {sleep_s}s"
            )
            time.sleep(sleep_s)
    raise RuntimeError(f"TESS EB fetch failed after {retries} attempts: {last_exc}")


def extract_total_pages(html_text: str) -> int:
    match = re.search(r"Page\s+\d+\s+of\s+(\d+)", html_text, flags=re.IGNORECASE)
    if not match:
        return 1
    try:
        pages = int(match.group(1))
    except ValueError:
        return 1
    return max(pages, 1)


def extract_data_table(html_text: str) -> tuple[list[str], list[list[str]]]:
    for table_match in re.finditer(r"<table[^>]*>.*?</table>", html_text, flags=re.IGNORECASE | re.DOTALL):
        table_html = table_match.group(0)
        header_html = re.findall(r"<th[^>]*>(.*?)</th>", table_html, flags=re.IGNORECASE | re.DOTALL)
        headers = [normalize_header(item) for item in header_html]
        if "tess id" not in headers:
            continue
        rows: list[list[str]] = []
        for row_html in re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, flags=re.IGNORECASE | re.DOTALL):
            cells_html = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.IGNORECASE | re.DOTALL)
            if not cells_html:
                continue
            cells = [clean_html_text(item) for item in cells_html]
            if len(cells) < len(headers):
                cells.extend([""] * (len(headers) - len(cells)))
            elif len(cells) > len(headers):
                cells = cells[: len(headers)]
            rows.append(cells)
        if headers and rows:
            return headers, rows
    raise RuntimeError("Unable to locate TESS EB data table in search_results page")


def rows_from_page(html_text: str) -> list[dict[str, str]]:
    headers, rows = extract_data_table(html_text)
    field_by_idx: dict[int, str] = {}
    for idx, header in enumerate(headers):
        field_name = HEADER_FIELD_MAP.get(header)
        if field_name:
            field_by_idx[idx] = field_name

    if "tic_id" not in field_by_idx.values():
        raise RuntimeError("TESS EB table parsing failed: missing TESS ID column")

    parsed_rows: list[dict[str, str]] = []
    for row in rows:
        item = {field: "" for field in RAW_FIELDS}
        for idx, cell in enumerate(row):
            field_name = field_by_idx.get(idx)
            if not field_name:
                continue
            item[field_name] = cell
        tic_digits = re.sub(r"[^0-9]", "", item.get("tic_id", ""))
        if not tic_digits:
            continue
        item["tic_id"] = tic_digits
        item["in_catalog"] = truthy_text(item.get("in_catalog", ""))
        parsed_rows.append(item)
    return parsed_rows


def write_manifest(
    manifest_path: Path,
    out_path: Path,
    *,
    row_count: int,
    page_count: int,
    in_catalog_only: bool,
) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    entries = [
        {
            "source_name": "tess_eb_catalog",
            "source_version": SOURCE_VERSION,
            "url": SEARCH_URL,
            "dest_path": str(out_path),
            "retrieved_at": ts,
            "checked_at": ts,
            "sha256": sha256_file(out_path),
            "bytes_written": out_path.stat().st_size,
            "row_count": row_count,
            "page_count": page_count,
            "in_catalog_only": bool(in_catalog_only),
        }
    ]
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(entries, indent=2) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch TESS Eclipsing Binary search export via paginated HTML table."
    )
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--timeout-s", type=int, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--max-pages", type=int, default=0, help="Optional cap for testing; 0 means all pages.")
    parser.add_argument(
        "--include-not-in-catalog",
        action="store_true",
        help="Include non-catalog rows by omitting incat_only=1 query flag.",
    )
    return parser.parse_args()


def resolve_state_dir(root: Path, cli_state_dir: str | None) -> Path:
    env_state = cli_state_dir or os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if env_state:
        return Path(env_state)
    shared_state = Path("/data/spacegate/data")
    if shared_state.exists():
        return shared_state
    return root / "data"


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    state_dir = resolve_state_dir(root, args.state_dir)
    raw_path = state_dir / "raw" / "tess_eb" / "tess_eb_catalog.csv"
    manifest_path = state_dir / "reports" / "manifests" / "tess_eb_manifest.json"
    in_catalog_only = not bool(args.include_not_in_catalog)

    log(
        "TESS EB fetch start "
        f"(in_catalog_only={'1' if in_catalog_only else '0'}, max_pages={args.max_pages or 'all'})"
    )
    first_url = build_query(1, in_catalog_only=in_catalog_only)
    first_page = fetch_text(first_url, timeout_s=args.timeout_s, retries=args.retries)
    total_pages = extract_total_pages(first_page)
    if args.max_pages and args.max_pages > 0:
        total_pages = min(total_pages, args.max_pages)

    all_rows: list[dict[str, str]] = []
    page_rows = rows_from_page(first_page)
    all_rows.extend(page_rows)
    log(f"tess_eb: page 1/{total_pages} rows={len(page_rows):,}")

    for page in range(2, total_pages + 1):
        page_text = fetch_text(
            build_query(page, in_catalog_only=in_catalog_only),
            timeout_s=args.timeout_s,
            retries=args.retries,
        )
        page_rows = rows_from_page(page_text)
        all_rows.extend(page_rows)
        if page % 10 == 0 or page == total_pages:
            log(f"tess_eb: page {page}/{total_pages} rows={len(page_rows):,} total={len(all_rows):,}")

    raw_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        newline="",
        encoding="utf-8",
        delete=False,
        dir=str(raw_path.parent),
        prefix=raw_path.name + ".tmp.",
    ) as tmp_file:
        tmp_path = Path(tmp_file.name)
        writer = csv.DictWriter(tmp_file, fieldnames=RAW_FIELDS)
        writer.writeheader()
        writer.writerows(all_rows)
    tmp_path.replace(raw_path)

    write_manifest(
        manifest_path,
        raw_path,
        row_count=len(all_rows),
        page_count=total_pages,
        in_catalog_only=in_catalog_only,
    )
    log(
        "TESS EB fetch complete "
        f"(rows={len(all_rows):,}, bytes={raw_path.stat().st_size:,}, pages={total_pages}, manifest={manifest_path})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
