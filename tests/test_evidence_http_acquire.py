from __future__ import annotations

import hashlib
import json
import sys
from email.message import Message
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import evidence_http_acquire as acquire  # noqa: E402


class FakeResponse:
    def __init__(self, payload: bytes, status: int) -> None:
        self._payload = payload
        self.status = status
        self.headers = Message()
        self.headers["Content-Length"] = str(len(payload))
        self.headers["Content-Type"] = "application/octet-stream"

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def read(self, _size: int = -1) -> bytes:
        payload, self._payload = self._payload, b""
        return payload


def test_http_acquisition_resumes_verifies_and_reuses_immutable_snapshot(
    tmp_path: Path, monkeypatch
) -> None:
    payload = b"release-scoped-source-bytes\n"
    product = {
        "source_name": "test_rows",
        "source_id": "test.catalog",
        "release_id": "r1",
        "url": "https://example.test/rows.dat",
        "filename": "rows.dat",
        "expected_bytes": len(payload),
        "expected_checksum": "md5:" + hashlib.md5(payload).hexdigest(),  # noqa: S324
    }
    snapshot_id = acquire.product_id(product)
    partial = (
        tmp_path
        / "tmp"
        / "evidence_lake_v2_http"
        / f"{snapshot_id}.rows.dat.partial"
    )
    partial.parent.mkdir(parents=True)
    partial.write_bytes(payload[:8])
    calls = []

    def fake_urlopen(request, timeout):
        calls.append((request.headers.get("Range"), timeout))
        assert request.headers["Range"] == "bytes=8-"
        return FakeResponse(payload[8:], 206)

    monkeypatch.setattr(acquire.urllib.request, "urlopen", fake_urlopen)
    first = acquire.acquire_product(
        product,
        state_dir=tmp_path,
        timeout_s=10,
        read_stall_timeout_s=3,
        retries=1,
    )
    root = Path(first["dest_path"])
    assert (root / "rows.dat").read_bytes() == payload
    manifest = json.loads((root / "product_manifest.json").read_text())
    assert manifest["resumed_from_bytes"] == 8
    assert manifest["expected_checksum_status"] == "match"
    assert first["sha256"] == hashlib.sha256(payload).hexdigest()
    assert manifest["read_stall_timeout_s"] == 3
    assert calls == [("bytes=8-", 3)]

    second = acquire.acquire_product(
        product, state_dir=tmp_path, timeout_s=10, retries=1
    )
    assert second == first
    assert len(calls) == 1
