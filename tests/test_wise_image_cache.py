from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "srv/api"))

from app import survey_image_cache, wise_images  # noqa: E402


def _system() -> dict:
    return {
        "system_id": 42,
        "stable_object_key": "system:test:42",
        "display_name": "Test System",
        "ra_deg": 12.5,
        "dec_deg": -4.25,
    }


def _products() -> list[dict]:
    return [{
        "energy_bandpassname": "W1",
        "dataproduct_subtype": "science",
        "access_url": "https://irsa.ipac.caltech.edu/ibe/data/test-int-1.fits",
        "obs_id": "test-w1",
    }]


def test_metadata_requests_for_the_same_product_are_coalesced(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SPACEGATE_WISE_IMAGE_CACHE_DIR", str(tmp_path / "cache"))
    calls = 0
    guard = threading.Lock()

    def query(*_args, **_kwargs):
        nonlocal calls
        with guard:
            calls += 1
        time.sleep(0.08)
        return _products()

    monkeypatch.setattr(wise_images, "query_sia_products", query)
    with ThreadPoolExecutor(max_workers=6) as pool:
        payloads = list(pool.map(
            lambda _: wise_images.ensure_wise_metadata(state_dir=tmp_path, system=_system()),
            range(6),
        ))

    assert calls == 1
    assert {payload["cache_key"] for payload in payloads} == {payloads[0]["cache_key"]}
    assert any(payload["cache_status"] == "metadata_coalesced_hit" for payload in payloads[1:])


def test_recent_provider_failure_uses_bounded_negative_cache(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SPACEGATE_WISE_IMAGE_CACHE_DIR", str(tmp_path / "cache"))
    calls = 0

    def fail(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise TimeoutError("provider timeout")

    monkeypatch.setattr(wise_images, "query_sia_products", fail)
    with pytest.raises(TimeoutError):
        wise_images.ensure_wise_metadata(state_dir=tmp_path, system=_system())
    with pytest.raises(survey_image_cache.NegativeCacheHit):
        wise_images.ensure_wise_metadata(state_dir=tmp_path, system=_system())
    assert calls == 1


def test_cache_report_exposes_provider_budget_and_metrics(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SPACEGATE_WISE_IMAGE_CACHE_DIR", str(tmp_path / "cache"))
    root = wise_images.cache_root(tmp_path)
    root.mkdir(parents=True)
    (root / "sample.bin").write_bytes(b"spacegate")
    report = wise_images.enforce_cache_limit(root, limit_bytes=64 * 1024 * 1024)
    assert report["schema_version"] == "survey_image_cache_status_v1"
    assert report["provider"] == "irsa_wise_allwise"
    assert report["total_bytes"] >= len(b"spacegate")
    assert isinstance(report["metrics"], dict)


def test_oldest_first_eviction_preserves_in_progress_temporary_files(tmp_path) -> None:
    oldest = tmp_path / "old.bin"
    newest = tmp_path / "new.bin"
    temporary = tmp_path / ".preview.png.42.tmp"
    oldest.write_bytes(b"a" * 8)
    time.sleep(0.01)
    newest.write_bytes(b"b" * 8)
    temporary.write_bytes(b"in-progress")

    report = survey_image_cache.enforce_oldest_first(tmp_path, 8)

    assert not oldest.exists()
    assert newest.exists()
    assert temporary.exists()
    assert report["removed_files"] == 1


def test_remote_fetch_retries_once_and_records_remote_work(monkeypatch) -> None:
    provider = "test_retry_provider"
    attempts = 0

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b"image"

    def open_request(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TimeoutError("first attempt")
        return Response()

    monkeypatch.setattr(survey_image_cache, "urlopen", open_request)
    payload = survey_image_cache.fetch_bytes(
        "https://example.invalid/image",
        provider=provider,
        user_agent="Spacegate test",
        timeout_seconds=1,
        attempts=2,
        min_interval_seconds=0,
    )

    assert payload == b"image"
    assert attempts == 2
    report = survey_image_cache.metrics(provider)
    assert report["remote_requests"] == 2
    assert report["remote_retries"] == 1
    assert report["remote_failures"] == 1


def test_remote_fetch_enforces_provider_concurrency(monkeypatch) -> None:
    provider = "test_concurrency_provider"
    active = 0
    maximum_active = 0
    guard = threading.Lock()

    class Response:
        def __enter__(self):
            nonlocal active, maximum_active
            with guard:
                active += 1
                maximum_active = max(maximum_active, active)
            time.sleep(0.04)
            return self

        def __exit__(self, *_args):
            nonlocal active
            with guard:
                active -= 1
            return False

        def read(self):
            return b"image"

    monkeypatch.setattr(survey_image_cache, "urlopen", lambda *_args, **_kwargs: Response())
    with ThreadPoolExecutor(max_workers=5) as pool:
        payloads = list(pool.map(
            lambda _: survey_image_cache.fetch_bytes(
                "https://example.invalid/image",
                provider=provider,
                user_agent="Spacegate test",
                timeout_seconds=1,
                attempts=1,
                max_concurrency=2,
                min_interval_seconds=0,
            ),
            range(5),
        ))

    assert payloads == [b"image"] * 5
    assert maximum_active == 2
