from __future__ import annotations

import os
from typing import Final


DEFAULT_UMASK: Final[int] = 0o002


def configured_umask() -> int:
    raw = os.getenv("SPACEGATE_UMASK", "0002").strip()
    if not raw:
        return DEFAULT_UMASK
    try:
        value = int(raw, 8)
    except ValueError:
        return DEFAULT_UMASK
    if value < 0 or value > 0o777:
        return DEFAULT_UMASK
    return value


def apply_configured_umask() -> int:
    return os.umask(configured_umask())
