from __future__ import annotations

import re
from typing import Any, Optional


_STELLAR_CLASSES = "OBAFGKMLTY"
_LOWERCASE_LUMINOSITY_PREFIX = re.compile(r"^(?:d|sd|esd|usd)([OBAFGKMLTY])")
_WHITE_DWARF_PREFIX = re.compile(r"^(?:WD|D(?:$|[ABCOQZX0-9]))", flags=re.I)
_WOLF_RAYET_PREFIX = re.compile(r"^W[CNOR]", flags=re.I)


def spectral_type_indicates_white_dwarf(value: Any) -> bool:
    text = str(value or "").strip()
    if not text or _LOWERCASE_LUMINOSITY_PREFIX.match(text):
        return False
    return bool(_WHITE_DWARF_PREFIX.match(text))


def spectral_class_from_type(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    luminosity_match = _LOWERCASE_LUMINOSITY_PREFIX.match(text)
    if luminosity_match:
        return luminosity_match.group(1).upper()
    if spectral_type_indicates_white_dwarf(text):
        return "D"
    if _WOLF_RAYET_PREFIX.match(text):
        return "WR"
    upper = text.upper()
    if upper[:1] in _STELLAR_CLASSES:
        return upper[:1]
    match = re.search(rf"[^A-Z]([{_STELLAR_CLASSES}])", upper)
    return match.group(1) if match else None
