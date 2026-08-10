"""Shared host-side Spacegate storage paths.

Runtime containers receive their bulk path through Compose. Host-side compiler
and verification scripts use this module so Photon storage changes do not
require editing dozens of independent defaults.
"""

from __future__ import annotations

import os
from pathlib import Path


DEFAULT_BULK_ROOT = Path("/space/spacegate")
LEGACY_BULK_ROOT = Path("/mnt/space/spacegate")
DEFAULT_COLD_ARCHIVE_ROOT = Path("/mnt/proton/spacegate-archive/v1")


def bulk_root() -> Path:
    configured = os.environ.get("SPACEGATE_BULK_DIR", "").strip()
    return Path(configured or DEFAULT_BULK_ROOT).expanduser()


def bulk_path(*parts: str) -> Path:
    return bulk_root().joinpath(*parts)


def cold_archive_root() -> Path:
    configured = os.environ.get("SPACEGATE_COLD_ARCHIVE_DIR", "").strip()
    return Path(configured or DEFAULT_COLD_ARCHIVE_ROOT).expanduser()
