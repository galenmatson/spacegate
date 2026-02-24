import base64
import json
import re
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional


SAFE_CURSOR_MAX_LEN = 512


def normalize_query_text(text: str) -> str:
    if text is None:
        return ""
    lowered = text.strip().lower()
    # Normalize similar to ingestion: lowercase, strip punctuation, collapse whitespace
    cleaned = re.sub(r"[^a-z0-9]+", " ", lowered)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def parse_identifier_query(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    numeric = re.match(r"^(\d+)$", text)
    if numeric:
        # Plain long integers are overwhelmingly Gaia source IDs.
        if len(numeric.group(1)) >= 10:
            return {"kind": "gaia", "value": int(numeric.group(1))}
        return None
    match = re.match(r"^(hd|hip|gaia)(?:\\s+dr\\d+)?\\s*(\\d+)$", text)
    if not match:
        return None
    kind = match.group(1)
    value = int(match.group(2))
    return {"kind": kind, "value": value}


def parse_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    lowered = value.strip().lower()
    if lowered in {"1", "true", "t", "yes", "y"}:
        return True
    if lowered in {"0", "false", "f", "no", "n"}:
        return False
    return None


def parse_spectral_classes(value: Optional[str]) -> List[str]:
    if not value:
        return []
    if isinstance(value, str):
        raw_items = value.split(",")
    else:
        raw_items = list(value)
    classes = []
    for item in raw_items:
        if not item:
            continue
        cls = item.strip().upper()
        if not cls:
            continue
        classes.append(cls)
    return classes


def encode_cursor(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_cursor(cursor: str) -> Dict[str, Any]:
    if not cursor:
        return {}
    if len(cursor) > SAFE_CURSOR_MAX_LEN:
        raise ValueError("cursor_too_long")
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii"))
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise ValueError("invalid_cursor") from exc
    if not isinstance(payload, dict):
        raise ValueError("invalid_cursor")
    return payload


def normalize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value


def row_to_dict(columns: Iterable[str], row: Iterable[Any]) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for key, value in zip(columns, row):
        data[key] = normalize_value(value)
    return data
