from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import json
import math
import os
import time
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import numpy as np
from PIL import Image

try:
    from astropy.io import fits
except Exception:  # pragma: no cover - exercised only in minimal dependency envs
    fits = None


SIA_URL = "https://irsa.ipac.caltech.edu/SIA"
WISE_ATTRIBUTION = "NASA/IPAC Infrared Science Archive (IRSA), WISE/AllWISE"
DEFAULT_CACHE_LIMIT_BYTES = 4 * 1024 * 1024 * 1024
DEFAULT_CUTOUT_ARCMIN = 8.0
WISE_BAND_ORDER = ("W1", "W2", "W3")


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def cache_limit_bytes() -> int:
    raw = str(os.getenv("SPACEGATE_WISE_IMAGE_CACHE_LIMIT_BYTES") or "").strip()
    if raw:
        try:
            return max(32 * 1024 * 1024, int(raw))
        except Exception:
            pass
    return DEFAULT_CACHE_LIMIT_BYTES


def cache_root(state_dir: Path) -> Path:
    configured = str(os.getenv("SPACEGATE_WISE_IMAGE_CACHE_DIR") or "").strip()
    if configured:
        return Path(configured)
    prefer_bulk = str(os.getenv("SPACEGATE_WISE_IMAGE_CACHE_PREFER_BULK") or "").strip().lower()
    bulk_root = Path("/mnt/space/spacegate")
    if prefer_bulk in {"1", "true", "yes", "on"} and bulk_root.exists():
        return bulk_root / "cache" / "wise_images"
    return state_dir / "cache" / "wise_images"


def product_key(system_id: int, ra_deg: float, dec_deg: float, size_arcmin: float) -> str:
    payload = f"{int(system_id)}:{ra_deg:.7f}:{dec_deg:.7f}:{size_arcmin:.3f}:allwise_w1w2w3"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def product_paths(root: Path, key: str) -> Dict[str, Path]:
    shard = root / key[:2] / key[2:4]
    return {
        "dir": shard,
        "metadata": shard / f"{key}.json",
        "png": shard / f"{key}.png",
    }


def _json_url(url: str, *, timeout_s: float = 30.0) -> str:
    request = Request(url, headers={"User-Agent": "Spacegate WISE image cache"})
    with urlopen(request, timeout=timeout_s) as response:
        return response.read().decode("utf-8", "replace")


def query_sia_products(ra_deg: float, dec_deg: float, radius_deg: float = 0.02) -> List[Dict[str, Any]]:
    params = {
        "COLLECTION": "wise_allwise",
        "POS": f"circle {ra_deg:.8f} {dec_deg:.8f} {radius_deg:.6f}",
        "RESPONSEFORMAT": "CSV",
        "MAXREC": "500",
    }
    text = _json_url(f"{SIA_URL}?{urlencode(params)}")
    return list(csv.DictReader(io.StringIO(text)))


def choose_band_products(products: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    chosen: Dict[str, Dict[str, Any]] = {}
    for row in products:
        band = str(row.get("energy_bandpassname") or "").strip().upper()
        if band not in WISE_BAND_ORDER:
            continue
        if str(row.get("dataproduct_subtype") or "").strip().lower() != "science":
            continue
        access_url = str(row.get("access_url") or "").strip()
        if "/ibe/data/" not in access_url or not access_url.endswith((".fits", ".fits.gz")):
            continue
        current = chosen.get(band)
        score = 0
        if "-int-" in access_url:
            score += 10
        if access_url.endswith(".fits"):
            score += 1
        if current is None or score > int(current.get("_spacegate_score") or 0):
            next_row = dict(row)
            next_row["_spacegate_score"] = score
            chosen[band] = next_row
    return chosen


def _cutout_url(access_url: str, ra_deg: float, dec_deg: float, size_arcmin: float) -> str:
    separator = "&" if "?" in access_url else "?"
    query = urlencode({
        "center": f"{ra_deg:.8f},{dec_deg:.8f}deg",
        "size": f"{size_arcmin:.3f}arcmin",
        "gzip": "false",
    })
    return f"{access_url}{separator}{query}"


def _download_cutout(access_url: str, ra_deg: float, dec_deg: float, size_arcmin: float) -> bytes:
    url = _cutout_url(access_url, ra_deg, dec_deg, size_arcmin)
    request = Request(url, headers={"User-Agent": "Spacegate WISE image cache"})
    with urlopen(request, timeout=60.0) as response:
        return response.read()


def _scale_array(data: np.ndarray) -> np.ndarray:
    arr = np.asarray(data, dtype=np.float32)
    arr = np.where(np.isfinite(arr), arr, np.nan)
    if not np.isfinite(arr).any():
        return np.zeros(arr.shape, dtype=np.float32)
    low, high = np.nanpercentile(arr, [2.0, 99.4])
    if not math.isfinite(float(low)) or not math.isfinite(float(high)) or high <= low:
        low = float(np.nanmin(arr))
        high = float(np.nanmax(arr))
    if high <= low:
        return np.zeros(arr.shape, dtype=np.float32)
    norm = np.clip((arr - low) / (high - low), 0.0, 1.0)
    scaled = np.arcsinh(norm * 9.0) / np.arcsinh(9.0)
    return np.where(np.isfinite(scaled), scaled, 0.0).astype(np.float32)


def _fits_image_data(payload: bytes) -> np.ndarray:
    if fits is None:
        raise RuntimeError("astropy is not installed; WISE FITS preview rendering is unavailable")
    with fits.open(io.BytesIO(payload), memmap=False) as hdus:
        for hdu in hdus:
            data = getattr(hdu, "data", None)
            if data is not None and np.asarray(data).ndim >= 2:
                arr = np.asarray(data)
                if arr.ndim > 2:
                    arr = arr[0]
                return arr.astype(np.float32)
    raise RuntimeError("No image data found in WISE FITS cutout")


def _write_rgb_preview(
    *,
    png_path: Path,
    band_products: Dict[str, Dict[str, Any]],
    ra_deg: float,
    dec_deg: float,
    size_arcmin: float,
) -> Dict[str, Any]:
    arrays: Dict[str, np.ndarray] = {}
    for band, row in band_products.items():
        payload = _download_cutout(str(row["access_url"]), ra_deg, dec_deg, size_arcmin)
        arrays[band] = _scale_array(_fits_image_data(payload))
    if not arrays:
        raise RuntimeError("No WISE band cutouts available")
    shape = next(iter(arrays.values())).shape
    red = arrays.get("W3")
    green = arrays.get("W2")
    blue = arrays.get("W1")
    if red is None:
        red = green if green is not None else np.zeros(shape, dtype=np.float32)
    if green is None:
        green = blue if blue is not None else red
    if blue is None:
        blue = green if green is not None else red
    rgb = np.stack([red, green, blue], axis=-1)
    rgb = np.clip(rgb * 255.0, 0, 255).astype(np.uint8)
    image = Image.fromarray(rgb)
    png_path.parent.mkdir(parents=True, exist_ok=True)
    image.save(png_path)
    return {
        "width": int(image.width),
        "height": int(image.height),
        "bands_rendered": [band for band in WISE_BAND_ORDER if band in arrays],
    }


def _cache_size(root: Path) -> int:
    if not root.exists():
        return 0
    total = 0
    for path in root.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except OSError:
            continue
    return total


def enforce_cache_limit(root: Path, limit_bytes: int | None = None) -> Dict[str, Any]:
    limit = int(limit_bytes or cache_limit_bytes())
    root.mkdir(parents=True, exist_ok=True)
    files = []
    total = 0
    for path in root.rglob("*"):
        try:
            if path.is_file():
                stat = path.stat()
                total += stat.st_size
                files.append((stat.st_mtime, stat.st_size, path))
        except OSError:
            continue
    removed = 0
    removed_bytes = 0
    if total > limit:
        for _, size, path in sorted(files):
            if total <= limit:
                break
            try:
                path.unlink()
                removed += 1
                removed_bytes += size
                total -= size
            except OSError:
                continue
    return {
        "limit_bytes": limit,
        "total_bytes": total,
        "removed_files": removed,
        "removed_bytes": removed_bytes,
    }


def ensure_wise_metadata(
    *,
    state_dir: Path,
    system: Dict[str, Any],
    size_arcmin: float = DEFAULT_CUTOUT_ARCMIN,
) -> Dict[str, Any]:
    ra = float(system.get("ra_deg"))
    dec = float(system.get("dec_deg"))
    system_id = int(system.get("system_id"))
    root = cache_root(state_dir)
    key = product_key(system_id, ra, dec, size_arcmin)
    paths = product_paths(root, key)
    if paths["metadata"].exists():
        try:
            metadata = json.loads(paths["metadata"].read_text(encoding="utf-8"))
            metadata["cache_status"] = "metadata_hit"
            return metadata
        except Exception:
            pass
    products = query_sia_products(ra, dec)
    band_products = choose_band_products(products)
    metadata = {
        "schema_version": "wise_image_product_v1",
        "system_id": system_id,
        "stable_object_key": system.get("stable_object_key"),
        "display_name": system.get("display_name") or system.get("system_name"),
        "center_ra_deg": ra,
        "center_dec_deg": dec,
        "cutout_size_arcmin": size_arcmin,
        "collection": "wise_allwise",
        "source_catalog": "irsa_wise_allwise",
        "source_version": "AllWISE Atlas Images",
        "bands": {
            band: {
                "access_url": row.get("access_url"),
                "obs_id": row.get("obs_id"),
                "s_pixel_scale": row.get("s_pixel_scale"),
                "s_resolution": row.get("s_resolution"),
                "source_url": row.get("access_url"),
            }
            for band, row in band_products.items()
        },
        "available_bands": sorted(band_products.keys()),
        "attribution": WISE_ATTRIBUTION,
        "retrieved_at": utc_now(),
        "cache_key": key,
        "preview_available": paths["png"].exists(),
        "cache_status": "metadata_miss",
        "policy": "IRSA WISE imagery is observational survey imagery, not an artist impression.",
    }
    paths["dir"].mkdir(parents=True, exist_ok=True)
    paths["metadata"].write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    enforce_cache_limit(root)
    return metadata


def ensure_wise_preview(
    *,
    state_dir: Path,
    system: Dict[str, Any],
    size_arcmin: float = DEFAULT_CUTOUT_ARCMIN,
) -> tuple[Path, Dict[str, Any]]:
    metadata = ensure_wise_metadata(state_dir=state_dir, system=system, size_arcmin=size_arcmin)
    root = cache_root(state_dir)
    key = str(metadata["cache_key"])
    paths = product_paths(root, key)
    if not paths["png"].exists():
        band_products = {
            band: {"access_url": info.get("access_url")}
            for band, info in (metadata.get("bands") or {}).items()
            if info.get("access_url")
        }
        if not band_products:
            raise RuntimeError("No WISE image products are available for this system.")
        render_info = _write_rgb_preview(
            png_path=paths["png"],
            band_products=band_products,
            ra_deg=float(metadata["center_ra_deg"]),
            dec_deg=float(metadata["center_dec_deg"]),
            size_arcmin=float(metadata["cutout_size_arcmin"]),
        )
        metadata["preview_available"] = True
        metadata["preview_generated_at"] = utc_now()
        metadata["render_info"] = render_info
        metadata["cache_status"] = "preview_generated"
        paths["metadata"].write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
        enforce_cache_limit(root)
    else:
        try:
            paths["png"].touch()
            paths["metadata"].touch()
        except OSError:
            pass
        metadata["preview_available"] = True
        metadata["cache_status"] = "preview_hit"
    return paths["png"], metadata
