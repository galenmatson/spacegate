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


def normalize_extended_object_query(text: str) -> str:
    normalized = normalize_query_text(text)
    match = re.fullmatch(
        r"(ngc|ic|m|mel|melotte|lbn|ldn|vdb|barnard|b)\s*0*(\d+)([a-z]?)",
        normalized,
    )
    if not match:
        return normalized
    prefix = {
        "mel": "melotte",
        "b": "barnard",
    }.get(match.group(1), match.group(1))
    suffix = match.group(3) or ""
    return f"{prefix} {int(match.group(2))}{suffix}"


def parse_identifier_query(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    numeric = re.match(r"^(\d+)$", text)
    if numeric:
        # Plain long integers are overwhelmingly Gaia source IDs.
        if len(numeric.group(1)) >= 10:
            return {"kind": "gaia", "value": int(numeric.group(1))}
        # For shorter numeric IDs, match common catalog IDs (HIP/HD).
        return {"kind": "catalog_numeric", "value": int(numeric.group(1))}
    match = re.match(r"^(hd|hip|gaia)(?:\s+dr\d+)?\s*(\d+)$", text)
    if not match:
        return None
    kind = match.group(1)
    value = int(match.group(2))
    return {"kind": kind, "value": value}


def parse_tess_identifier_query(text: str) -> Optional[Dict[str, Any]]:
    """Parse exact-like TIC/TOI input without turning malformed IDs into names."""
    raw = str(text or "").strip()
    prefix = re.match(r"^(tic|toi)(?=$|[\s._-]|\d)", raw, flags=re.IGNORECASE)
    if not prefix:
        return None

    namespace = prefix.group(1).lower()
    if namespace == "tic":
        match = re.fullmatch(r"tic[\s._-]*(\d+)", raw, flags=re.IGNORECASE)
        if not match or len(match.group(1)) > 19:
            return {
                "namespace": namespace,
                "valid": False,
                "raw": raw,
                "reason": "malformed_identifier",
            }
        value = int(match.group(1))
        if value <= 0 or value > 9_223_372_036_854_775_807:
            return {
                "namespace": namespace,
                "valid": False,
                "raw": raw,
                "reason": "identifier_out_of_range",
            }
        return {
            "namespace": namespace,
            "valid": True,
            "raw": raw,
            "value": value,
            "identifier": f"TIC {value}",
            "term_norm": f"tic {value}",
        }

    match = re.fullmatch(
        r"toi[\s_-]*(\d{1,9})(?:[.\s_-]+(\d{1,2}))?",
        raw,
        flags=re.IGNORECASE,
    )
    if not match:
        return {
            "namespace": namespace,
            "valid": False,
            "raw": raw,
            "reason": "malformed_identifier",
        }
    host_number = int(match.group(1))
    component_text = match.group(2)
    if host_number <= 0 or (component_text is not None and int(component_text) <= 0):
        return {
            "namespace": namespace,
            "valid": False,
            "raw": raw,
            "reason": "identifier_out_of_range",
        }
    component = int(component_text) if component_text is not None else None
    identifier = f"TOI-{host_number}"
    term_norm = f"toi {host_number}"
    toi_value = str(host_number)
    if component is not None:
        identifier = f"{identifier}.{component:02d}"
        term_norm = f"{term_norm} {component:02d}"
        toi_value = f"{host_number}.{component:02d}"
    return {
        "namespace": namespace,
        "valid": True,
        "raw": raw,
        "host_number": host_number,
        "component": component,
        "value": toi_value,
        "identifier": identifier,
        "term_norm": term_norm,
    }


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
