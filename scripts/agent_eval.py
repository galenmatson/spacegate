#!/usr/bin/env python3
"""Run Spacegate agent golden-case evaluations.

This harness is intentionally separate from production arm/disc artifacts. It
loads versioned eval cases, optionally calls an OpenAI-compatible model, scores
the output against deterministic expectations, and writes reproducible reports.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CASES_DIR = ROOT / "evals" / "spacegate_agent" / "cases"
DEFAULT_REPORT_DIR = ROOT / "reports" / "agent_eval"
DEFAULT_BASE_URL = "http://127.0.0.1:8000/v1"
DEFAULT_MODEL = "local-70b"
DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"
DEFAULT_GOOGLE_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"
DEFAULT_ENV_FILE = Path("/etc/spacegate/spacegate.env")
PROMPT_VERSION = "agent_eval_v1"
HARNESS_VERSION = "agent_eval.py:2026-06-15"
ANOMALY_TYPES = {
    "catalog_conflict",
    "source_conflict",
    "identity_or_host_ambiguity",
    "schema_gap",
    "stale_consensus",
    "derived_plausibility_failure",
    "interesting_hypothesis",
    "needs_human_review",
    "unsupported_claim",
    "multi_model_measurement",
    "observational_limitation",
}


@dataclass(frozen=True)
class CaseRef:
    suite_id: str
    suite_version: str
    path: Path
    case: dict[str, Any]


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_base_url(url: str) -> str:
    value = (url or "").strip().rstrip("/")
    if not value:
        value = DEFAULT_BASE_URL
    if not value.endswith("/v1"):
        value = f"{value}/v1"
    return value


def load_env_file(path: Path, *, required: bool) -> None:
    try:
        path.stat()
    except FileNotFoundError:
        if required:
            raise SystemExit(f"required env file does not exist: {path}")
        return
    except PermissionError as exc:
        if required:
            raise SystemExit(f"cannot access required env file {path}: {exc}") from exc
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except PermissionError as exc:
        if required:
            raise SystemExit(f"cannot read required env file {path}: {exc}") from exc
        return
    for line_no, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped.removeprefix("export ").strip()
        if "=" not in stripped:
            raise SystemExit(f"{path}:{line_no} expected KEY=value")
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise SystemExit(f"{path}:{line_no} empty environment key")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        os.environ.setdefault(key, value)


def load_case_refs(cases_dir: Path) -> list[CaseRef]:
    refs: list[CaseRef] = []
    for path in sorted(cases_dir.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        suite_id = str(payload.get("suite_id") or path.stem)
        suite_version = str(payload.get("suite_version") or "unknown")
        cases = payload.get("cases")
        if not isinstance(cases, list):
            raise SystemExit(f"{path} must contain a cases array")
        for case in cases:
            if not isinstance(case, dict) or not case.get("case_id"):
                raise SystemExit(f"{path} contains a case without case_id")
            refs.append(CaseRef(suite_id=suite_id, suite_version=suite_version, path=path, case=case))
    return refs


def select_cases(refs: list[CaseRef], case_ids: set[str], roles: set[str]) -> list[CaseRef]:
    selected = []
    for ref in refs:
        case = ref.case
        if case_ids and case["case_id"] not in case_ids:
            continue
        if roles and not roles.intersection(set(case.get("roles", []))):
            continue
        selected.append(ref)
    return selected


def validate_case(case: dict[str, Any], path: Path) -> list[str]:
    errors: list[str] = []
    case_id = case.get("case_id", "<missing>")
    for key in ("case_id", "title", "roles", "input", "expected"):
        if key not in case:
            errors.append(f"{path}:{case_id} missing {key}")
    if not isinstance(case.get("roles"), list) or not case.get("roles"):
        errors.append(f"{path}:{case_id} roles must be a non-empty list")
    input_payload = case.get("input", {})
    if not isinstance(input_payload, dict) or not input_payload.get("task") or not input_payload.get("source_excerpt"):
        errors.append(f"{path}:{case_id} input must include task and source_excerpt")
    expected = case.get("expected", {})
    if not isinstance(expected, dict):
        errors.append(f"{path}:{case_id} expected must be an object")
        return errors
    claims = expected.get("claims", [])
    anomalies = expected.get("anomalies", [])
    if not isinstance(claims, list):
        errors.append(f"{path}:{case_id} expected.claims must be a list")
    if not isinstance(anomalies, list):
        errors.append(f"{path}:{case_id} expected.anomalies must be a list")
    for claim in claims if isinstance(claims, list) else []:
        for key in ("claim_id", "subject", "predicate", "value", "qualifier", "status"):
            if key not in claim:
                errors.append(f"{path}:{case_id}:{claim.get('claim_id', '<missing>')} missing claim field {key}")
    for anomaly in anomalies if isinstance(anomalies, list) else []:
        anomaly_type = anomaly.get("anomaly_type")
        if anomaly_type not in ANOMALY_TYPES:
            errors.append(f"{path}:{case_id} unknown anomaly_type {anomaly_type!r}")
        for key in ("severity", "subject", "summary_contains"):
            if key not in anomaly:
                errors.append(f"{path}:{case_id}:{anomaly_type} missing anomaly field {key}")
    return errors


def build_messages(case: dict[str, Any], role: str | None) -> list[dict[str, str]]:
    canonical_predicates = [
        "planet.mass_mearth",
        "planet.minimum_mass_mearth",
        "planet.radius_rearth",
        "planet.density_g_cm3",
        "planet.equilibrium_temp_k",
        "planet.semi_major_axis_au",
        "planet.orbital_period_days",
        "planet.lifecycle_status",
        "planet.host_star",
        "planet.atmosphere_note",
        "planet.atmospheric_composition",
        "planet.conservative_hz_membership",
        "planet.tidal_locking_likelihood",
        "star.teff_k",
        "star.mass_msun",
        "star.radius_rsun",
        "star.luminosity_log10_lsun",
        "star.age_gyr",
        "star.metallicity_feh",
        "star.rotation_period_days",
        "star.logg_cgs",
        "star.classification",
        "identity.authority_record",
        "naming.alias",
        "structure.children",
        "orbit_relation.relation_kind",
        "system.non_detection_constraint",
    ]
    canonical_units = ["Mearth", "Rearth", "Msun", "Rsun", "AU", "days", "K", "dex", "solar"]
    expected_shape = {
        "claims": [
            {
                "claim_id": "short_stable_id",
                "subject": "object or relation name",
                "predicate": "claim family and field",
                "value": "string, number, boolean, or null",
                "unit": "unit string or null",
                "qualifier": "measured | upper_limit_3sigma | m_sin_i | schema_gap | ...",
                "status": "accepted | rejected | deferred",
                "supporting_citation_ids": ["citation_id"],
                "reasoning_summary": "brief evidence-grounded rationale",
            }
        ],
        "anomalies": [
            {
                "anomaly_type": "catalog_conflict | source_conflict | identity_or_host_ambiguity | schema_gap | stale_consensus | derived_plausibility_failure | interesting_hypothesis | needs_human_review | unsupported_claim | multi_model_measurement | observational_limitation",
                "severity": "low | medium | high",
                "subject": "object or relation name",
                "summary": "short quarantined finding",
                "recommended_next_action": "review | deterministic_check | source_followup | frontier_escalation | discard",
            }
        ],
        "verdict": {
            "case_status": "pass | partial | fail | abstain",
            "summary": "brief claim-level outcome",
        },
    }
    input_payload = case["input"]
    source_refs = case.get("source_refs", [])
    role_text = role or ",".join(case.get("roles", []))
    system = (
        "You are a Spacegate astronomical evidence agent. Extract only claims supported by the supplied "
        "source excerpt and current state. Do not invent facts. Preserve uncertainty, upper/lower limits, "
        "minimum-mass semantics, subject binding, and contradictions. Produce strict JSON only. Do not "
        "wrap output in markdown. Quarantine surprising or novel findings as anomalies; never promote them "
        "to canonical truth."
    )
    user_payload = {
        "role_under_test": role_text,
        "case_id": case["case_id"],
        "task": input_payload["task"],
        "canonical_predicate_examples": canonical_predicates,
        "canonical_unit_examples": canonical_units,
        "source_refs": source_refs,
        "source_excerpt": input_payload["source_excerpt"],
        "current_state": input_payload.get("current_state", {}),
        "required_output_shape": expected_shape,
    }
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": canonical_json(user_payload)},
    ]


def request_json(
    base_url: str,
    api_key: str,
    payload: dict[str, Any],
    timeout_s: int,
) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{base_url}/chat/completions",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    if api_key:
        request.add_header("Authorization", f"Bearer {api_key}")
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            return json.loads(response.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:1200]
        raise RuntimeError(f"HTTP {exc.code}: {body}") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError(f"request failed: {exc}") from exc


def request_openai_chat(
    *,
    base_url: str,
    api_key: str,
    model: str,
    messages: list[dict[str, str]],
    temperature: float,
    max_tokens: int,
    timeout_s: int,
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    request_payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "response_format": {"type": "json_object"},
    }
    is_openai_frontier = base_url == normalize_base_url(DEFAULT_OPENAI_BASE_URL)
    if not is_openai_frontier:
        request_payload["temperature"] = temperature
    if is_openai_frontier:
        request_payload["max_completion_tokens"] = max_tokens
    else:
        request_payload["max_tokens"] = max_tokens
    response = request_json(base_url, api_key, request_payload, timeout_s)
    return response, extract_message_content(response), request_payload


def request_google_generate_content(
    *,
    base_url: str,
    api_key: str,
    model: str,
    messages: list[dict[str, str]],
    temperature: float,
    max_tokens: int,
    timeout_s: int,
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY or SPACEGATE_GOOGLE_API_KEY is required for provider=google")
    if len(messages) != 2:
        raise RuntimeError("Gemini request builder expects one system and one user message")
    system_text = messages[0]["content"]
    user_text = messages[1]["content"]
    request_payload: dict[str, Any] = {
        "systemInstruction": {"parts": [{"text": system_text}]},
        "contents": [{"role": "user", "parts": [{"text": user_text}]}],
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": max_tokens,
            "responseMimeType": "application/json",
        },
    }
    data = json.dumps(request_payload).encode("utf-8")
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/models/{model}:generateContent",
        data=data,
        headers={"Content-Type": "application/json", "x-goog-api-key": api_key},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:1200]
        raise RuntimeError(f"HTTP {exc.code}: {body}") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError(f"request failed: {exc}") from exc

    candidates = payload.get("candidates")
    content = ""
    if isinstance(candidates, list) and candidates:
        first = candidates[0] if isinstance(candidates[0], dict) else {}
        candidate_content = first.get("content") if isinstance(first, dict) else {}
        parts = candidate_content.get("parts") if isinstance(candidate_content, dict) else []
        if isinstance(parts, list):
            content = "".join(str(part.get("text", "")) for part in parts if isinstance(part, dict))
    return payload, content, request_payload


def resolve_provider_config(args: argparse.Namespace) -> tuple[str, str, str, str]:
    provider = args.provider
    if provider == "frontier":
        provider = os.getenv("SPACEGATE_FRONTIER_DEFAULT_PROVIDER", "openai").strip().lower() or "openai"
    if provider not in {"local", "openai", "google"}:
        raise SystemExit(f"Unsupported provider: {provider}")

    if args.model:
        model = args.model
    elif provider == "openai":
        model = os.getenv("SPACEGATE_FRONTIER_OPENAI_MODEL", DEFAULT_MODEL)
    elif provider == "google":
        model = os.getenv("SPACEGATE_FRONTIER_GOOGLE_MODEL", DEFAULT_MODEL)
    else:
        model = os.getenv("SPACEGATE_LLM_MODEL", DEFAULT_MODEL)

    if args.base_url:
        base_url = args.base_url
    elif provider == "openai":
        base_url = os.getenv("SPACEGATE_OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL)
    elif provider == "google":
        base_url = os.getenv("SPACEGATE_GOOGLE_BASE_URL", DEFAULT_GOOGLE_BASE_URL)
    else:
        base_url = os.getenv("SPACEGATE_LLM_BASE_URL", DEFAULT_BASE_URL)

    if provider in {"local", "openai"}:
        base_url = normalize_base_url(base_url)
    else:
        base_url = base_url.rstrip("/")

    if provider == "openai":
        api_key = os.getenv("SPACEGATE_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY", "")
    elif provider == "google":
        api_key = os.getenv("SPACEGATE_GOOGLE_API_KEY") or os.getenv("GOOGLE_API_KEY", "")
    else:
        api_key = os.getenv("SPACEGATE_LLM_API_KEY", "")
    if provider in {"openai", "google"} and not api_key:
        expected = "OPENAI_API_KEY or SPACEGATE_OPENAI_API_KEY" if provider == "openai" else "GOOGLE_API_KEY or SPACEGATE_GOOGLE_API_KEY"
        raise SystemExit(f"{expected} is required for provider={provider}")
    return provider, model, base_url, api_key


def extract_message_content(response: dict[str, Any]) -> str:
    choices = response.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    return content if isinstance(content, str) else ""


def provider_usage(provider: str, response: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(response, dict):
        return {}
    if provider in {"local", "openai"}:
        usage = response.get("usage", {})
        return usage if isinstance(usage, dict) else {}
    if provider == "google":
        usage = response.get("usageMetadata", {})
        if not isinstance(usage, dict):
            return {}
        normalized = {
            "prompt_tokens": usage.get("promptTokenCount"),
            "completion_tokens": usage.get("candidatesTokenCount"),
            "thinking_tokens": usage.get("thoughtsTokenCount"),
            "total_tokens": usage.get("totalTokenCount"),
        }
        return {key: value for key, value in normalized.items() if value is not None}
    return {}


def provider_finish_metadata(provider: str, response: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(response, dict):
        return {}
    if provider in {"local", "openai"}:
        choices = response.get("choices")
        if isinstance(choices, list) and choices and isinstance(choices[0], dict):
            finish_reason = choices[0].get("finish_reason")
            return {"finish_reason": finish_reason} if finish_reason is not None else {}
    if provider == "google":
        metadata: dict[str, Any] = {}
        candidates = response.get("candidates")
        if isinstance(candidates, list) and candidates and isinstance(candidates[0], dict):
            candidate = candidates[0]
            if candidate.get("finishReason") is not None:
                metadata["finish_reason"] = candidate.get("finishReason")
            if candidate.get("safetyRatings") is not None:
                metadata["safety_ratings"] = candidate.get("safetyRatings")
        prompt_feedback = response.get("promptFeedback")
        if isinstance(prompt_feedback, dict) and prompt_feedback.get("blockReason") is not None:
            metadata["prompt_block_reason"] = prompt_feedback.get("blockReason")
        return metadata
    return {}


def parse_model_json(content: str) -> tuple[dict[str, Any] | None, str | None]:
    stripped = content.strip()
    if not stripped:
        return None, "empty model content"
    candidates = [stripped]
    fenced = re.search(r"```(?:json)?\s*(.*?)```", stripped, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        candidates.insert(0, fenced.group(1).strip())
    first = stripped.find("{")
    last = stripped.rfind("}")
    if first >= 0 and last > first:
        candidates.append(stripped[first : last + 1])
    for candidate in candidates:
        try:
            payload = json.loads(candidate)
        except ValueError:
            continue
        if isinstance(payload, dict):
            return payload, None
    return None, "model content did not contain a JSON object"


def normalize_scalar(value: Any) -> Any:
    if isinstance(value, str):
        return " ".join(value.strip().lower().split())
    return value


def parse_numeric(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        match = re.search(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?", value)
        if match:
            try:
                return float(match.group(0))
            except ValueError:
                return None
    return None


def values_match(expected: Any, actual: Any, tolerance: float | None) -> bool:
    if isinstance(expected, bool):
        return isinstance(actual, bool) and actual is expected
    if isinstance(expected, (int, float)) and not isinstance(expected, bool):
        actual_num = parse_numeric(actual)
        if actual_num is None:
            return False
        tol = 0.0 if tolerance is None else float(tolerance)
        return abs(float(expected) - actual_num) <= tol
    return normalize_scalar(expected) == normalize_scalar(actual)


def subject_matches(expected: dict[str, Any], actual_subject: Any) -> bool:
    actual = normalize_scalar(actual_subject)
    subjects = [expected.get("subject")]
    aliases = expected.get("subject_aliases", [])
    if isinstance(aliases, list):
        subjects.extend(aliases)
    return any(normalize_scalar(subject) == actual for subject in subjects)


def anomaly_type_matches(expected: dict[str, Any], actual_type: Any) -> bool:
    actual = normalize_scalar(actual_type)
    types = [expected.get("anomaly_type")]
    aliases = expected.get("anomaly_type_aliases", [])
    if isinstance(aliases, list):
        types.extend(aliases)
    return any(normalize_scalar(anomaly_type) == actual for anomaly_type in types)


def claim_key(claim: dict[str, Any]) -> tuple[str, str]:
    return (str(claim.get("subject", "")).lower(), str(claim.get("predicate", "")).lower())


def find_actual_claim(expected: dict[str, Any], actual_claims: list[dict[str, Any]]) -> dict[str, Any] | None:
    expected_id = str(expected.get("claim_id", ""))
    for actual in actual_claims:
        if str(actual.get("claim_id", "")) == expected_id:
            return actual
    candidates = []
    key = claim_key(expected)
    for actual in actual_claims:
        if claim_key(actual) == key:
            candidates.append(actual)
    expected_predicate = normalize_scalar(expected.get("predicate"))
    for actual in actual_claims:
        if normalize_scalar(actual.get("predicate")) == expected_predicate and subject_matches(expected, actual.get("subject")):
            candidates.append(actual)
    if not candidates:
        return None

    def candidate_score(actual: dict[str, Any]) -> tuple[int, int, int, int]:
        return (
            int(values_match(expected.get("value"), actual.get("value"), expected.get("tolerance"))),
            int(values_match(expected.get("qualifier"), actual.get("qualifier"), None)),
            int(values_match(expected.get("status"), actual.get("status"), None)),
            int(values_match(expected.get("unit"), actual.get("unit"), None)),
        )

    return max(candidates, key=candidate_score)


def score_claim(expected: dict[str, Any], actual: dict[str, Any] | None) -> tuple[float, list[str]]:
    if actual is None:
        return 0.0, [f"missing claim {expected.get('claim_id')}"]
    checks = [
        ("predicate", expected.get("predicate"), actual.get("predicate"), None),
        ("value", expected.get("value"), actual.get("value"), expected.get("tolerance")),
        ("unit", expected.get("unit"), actual.get("unit"), None),
        ("qualifier", expected.get("qualifier"), actual.get("qualifier"), None),
        ("status", expected.get("status"), actual.get("status"), None),
    ]
    passed = 1 if subject_matches(expected, actual.get("subject")) else 0
    notes: list[str] = []
    if not subject_matches(expected, actual.get("subject")):
        notes.append(f"{expected.get('claim_id')} field subject expected {expected.get('subject')!r} got {actual.get('subject')!r}")
    for field, expected_value, actual_value, tolerance in checks:
        if values_match(expected_value, actual_value, tolerance):
            passed += 1
        else:
            notes.append(
                f"{expected.get('claim_id')} field {field} expected {expected_value!r} got {actual_value!r}"
            )
    return passed / (len(checks) + 1), notes


def find_actual_anomaly(expected: dict[str, Any], actual_anomalies: list[dict[str, Any]]) -> dict[str, Any] | None:
    for actual in actual_anomalies:
        if not anomaly_type_matches(expected, actual.get("anomaly_type")):
            continue
        if subject_matches(expected, actual.get("subject")):
            return actual
    return None


def score_anomaly(expected: dict[str, Any], actual: dict[str, Any] | None) -> tuple[float, list[str]]:
    if actual is None:
        return 0.0, [f"missing anomaly {expected.get('anomaly_type')} for {expected.get('subject')}"]
    checks = [
        ("severity", expected.get("severity"), actual.get("severity")),
    ]
    passed = int(subject_matches(expected, actual.get("subject"))) + int(
        anomaly_type_matches(expected, actual.get("anomaly_type"))
    )
    notes: list[str] = []
    if not subject_matches(expected, actual.get("subject")):
        notes.append(
            f"anomaly {expected.get('anomaly_type')} field subject expected {expected.get('subject')!r} got {actual.get('subject')!r}"
        )
    if not anomaly_type_matches(expected, actual.get("anomaly_type")):
        notes.append(
            f"anomaly {expected.get('anomaly_type')} field anomaly_type expected {expected.get('anomaly_type')!r} got {actual.get('anomaly_type')!r}"
        )
    for field, expected_value, actual_value in checks:
        if values_match(expected_value, actual_value, None):
            passed += 1
        else:
            notes.append(
                f"anomaly {expected.get('anomaly_type')} field {field} expected {expected_value!r} got {actual_value!r}"
            )
    summary_need = str(expected.get("summary_contains", "")).lower()
    summary = str(actual.get("summary", "")).lower()
    if summary_need and summary_need not in summary:
        notes.append(f"anomaly summary missing {summary_need!r}")
    else:
        passed += 1
    return passed / 4, notes


def score_output(case: dict[str, Any], output: dict[str, Any] | None, parse_error: str | None) -> dict[str, Any]:
    expected = case["expected"]
    if output is None:
        return {
            "score": 0.0,
            "schema_valid": False,
            "parse_error": parse_error,
            "claim_score": 0.0,
            "anomaly_score": 0.0,
            "notes": [parse_error or "missing output"],
        }
    claims = output.get("claims", [])
    anomalies = output.get("anomalies", [])
    schema_valid = isinstance(claims, list) and isinstance(anomalies, list)
    if not schema_valid:
        return {
            "score": 0.0,
            "schema_valid": False,
            "parse_error": "output must contain claims and anomalies arrays",
            "claim_score": 0.0,
            "anomaly_score": 0.0,
            "notes": ["output must contain claims and anomalies arrays"],
        }
    actual_claims = [claim for claim in claims if isinstance(claim, dict)]
    actual_anomalies = [anomaly for anomaly in anomalies if isinstance(anomaly, dict)]

    notes: list[str] = []
    claim_scores = []
    for expected_claim in expected.get("claims", []):
        score, claim_notes = score_claim(expected_claim, find_actual_claim(expected_claim, actual_claims))
        claim_scores.append(score)
        notes.extend(claim_notes)
    anomaly_scores = []
    for expected_anomaly in expected.get("anomalies", []):
        score, anomaly_notes = score_anomaly(
            expected_anomaly, find_actual_anomaly(expected_anomaly, actual_anomalies)
        )
        anomaly_scores.append(score)
        notes.extend(anomaly_notes)

    expected_claim_count = len(expected.get("claims", []))
    extra_claims = max(0, len(actual_claims) - expected_claim_count)
    hallucination_penalty = min(0.15, extra_claims * 0.03)

    claim_score = sum(claim_scores) / len(claim_scores) if claim_scores else 1.0
    anomaly_score = sum(anomaly_scores) / len(anomaly_scores) if anomaly_scores else 1.0
    total = (claim_score * 0.75) + (anomaly_score * 0.20) + (0.05 if schema_valid else 0.0)
    total = max(0.0, total - hallucination_penalty)
    if extra_claims:
        notes.append(f"extra claims emitted: {extra_claims}")
    return {
        "score": round(total, 4),
        "schema_valid": schema_valid,
        "parse_error": None,
        "claim_score": round(claim_score, 4),
        "anomaly_score": round(anomaly_score, 4),
        "extra_claims": extra_claims,
        "notes": notes,
    }


def expected_as_oracle(case: dict[str, Any]) -> dict[str, Any]:
    expected = case["expected"]
    return {
        "claims": [
            {
                "claim_id": claim["claim_id"],
                "subject": claim["subject"],
                "predicate": claim["predicate"],
                "value": claim["value"],
                "unit": claim.get("unit"),
                "qualifier": claim["qualifier"],
                "status": claim["status"],
                "supporting_citation_ids": [
                    source["citation_id"] for source in case.get("source_refs", []) if source.get("citation_id")
                ],
                "reasoning_summary": "Oracle output copied from expected golden case.",
            }
            for claim in expected.get("claims", [])
        ],
        "anomalies": [
            {
                "anomaly_type": anomaly["anomaly_type"],
                "severity": anomaly["severity"],
                "subject": anomaly["subject"],
                "summary": anomaly.get("summary_contains", ""),
                "recommended_next_action": "review",
            }
            for anomaly in expected.get("anomalies", [])
        ],
        "verdict": {"case_status": "pass", "summary": "Oracle output."},
    }


def run_case(
    ref: CaseRef,
    *,
    provider: str,
    model: str,
    base_url: str,
    api_key: str,
    role: str | None,
    temperature: float,
    max_tokens: int,
    timeout_s: int,
    oracle: bool,
) -> dict[str, Any]:
    case = ref.case
    messages = build_messages(case, role)
    request_payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    started = time.monotonic()
    raw_response: dict[str, Any] | None = None
    content = ""
    error = None
    if oracle:
        parsed_output = expected_as_oracle(case)
        parse_error = None
    else:
        try:
            if provider in {"local", "openai"}:
                raw_response, content, request_payload = request_openai_chat(
                    base_url=base_url,
                    api_key=api_key,
                    model=model,
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    timeout_s=timeout_s,
                )
            elif provider == "google":
                raw_response, content, request_payload = request_google_generate_content(
                    base_url=base_url,
                    api_key=api_key,
                    model=model,
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    timeout_s=timeout_s,
                )
            else:
                raise RuntimeError(f"unsupported provider {provider}")
            parsed_output, parse_error = parse_model_json(content)
        except RuntimeError as exc:
            parsed_output = None
            parse_error = str(exc)
            error = str(exc)
    elapsed_s = time.monotonic() - started
    score = score_output(case, parsed_output, parse_error)
    usage = provider_usage(provider, raw_response)
    finish_metadata = provider_finish_metadata(provider, raw_response)
    result = {
        "case_id": case["case_id"],
        "title": case["title"],
        "suite_id": ref.suite_id,
        "suite_version": ref.suite_version,
        "case_path": str(ref.path.relative_to(ROOT)),
        "roles": case.get("roles", []),
        "role_under_test": role,
        "provider": provider,
        "model_id": model,
        "base_url": base_url,
        "prompt_version": PROMPT_VERSION,
        "harness_version": HARNESS_VERSION,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "elapsed_s": round(elapsed_s, 3),
        "usage": usage,
        "finish_metadata": finish_metadata,
        "case_hash": sha256_text(canonical_json(case)),
        "request_hash": sha256_text(canonical_json(request_payload)),
        "output_hash": sha256_text(canonical_json(parsed_output) if parsed_output is not None else content),
        "score": score,
        "output": parsed_output,
        "raw_content": content if parsed_output is None else None,
        "error": error,
    }
    return result


def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        return {"case_count": 0, "mean_score": 0.0}
    scores = [float(result["score"]["score"]) for result in results]
    schema_valid = sum(1 for result in results if result["score"].get("schema_valid"))
    anomaly_items = []
    for result in results:
        output = result.get("output") or {}
        for anomaly in output.get("anomalies", []) if isinstance(output, dict) else []:
            if isinstance(anomaly, dict):
                anomaly_items.append(
                    {
                        "case_id": result["case_id"],
                        "model_id": result["model_id"],
                        "anomaly_type": anomaly.get("anomaly_type"),
                        "severity": anomaly.get("severity"),
                        "subject": anomaly.get("subject"),
                        "summary": anomaly.get("summary"),
                        "recommended_next_action": anomaly.get("recommended_next_action"),
                        "status": "quarantined",
                    }
                )
    return {
        "case_count": len(results),
        "mean_score": round(sum(scores) / len(scores), 4),
        "min_score": round(min(scores), 4),
        "max_score": round(max(scores), 4),
        "schema_valid_count": schema_valid,
        "schema_valid_rate": round(schema_valid / len(results), 4),
        "anomaly_count": len(anomaly_items),
        "anomaly_inbox": anomaly_items,
    }


def write_reports(report_dir: Path, run_payload: dict[str, Any]) -> tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    json_path = report_dir / f"agent_eval_{stamp}.json"
    md_path = report_dir / f"agent_eval_{stamp}.md"
    json_path.write_text(json.dumps(run_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def table_cell(value: str) -> str:
        return " ".join(value.replace("|", "\\|").split())

    lines = [
        "# Spacegate Agent Eval Report",
        "",
        f"- Run ID: `{run_payload['run_id']}`",
        f"- Created: `{run_payload['created_at']}`",
        f"- Provider: `{run_payload['provider']}`",
        f"- Model: `{run_payload['model_id']}`",
        f"- Base URL: `{run_payload['base_url']}`",
        f"- Cases: `{run_payload['summary']['case_count']}`",
        f"- Mean score: `{run_payload['summary']['mean_score']}`",
        f"- Schema valid: `{run_payload['summary']['schema_valid_count']}/{run_payload['summary']['case_count']}`",
        f"- Quarantined anomalies: `{run_payload['summary']['anomaly_count']}`",
        "",
        "## Cases",
        "",
        "| Case | Score | Claim | Anomaly | Notes |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for result in run_payload["results"]:
        score = result["score"]
        notes = "; ".join(score.get("notes", [])[:3])
        if len(score.get("notes", [])) > 3:
            notes += "; ..."
        notes = table_cell(notes or "ok")
        lines.append(
            f"| `{result['case_id']}` | {score['score']:.4f} | {score['claim_score']:.4f} | "
            f"{score['anomaly_score']:.4f} | {notes} |"
        )
    if run_payload["summary"].get("anomaly_inbox"):
        lines.extend(["", "## Anomaly Inbox", ""])
        for anomaly in run_payload["summary"]["anomaly_inbox"]:
            lines.append(
                f"- `{anomaly['severity']}` `{anomaly['anomaly_type']}` in `{anomaly['case_id']}` "
                f"for `{anomaly['subject']}`: {anomaly.get('summary') or ''}"
            )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def cmd_list(args: argparse.Namespace) -> int:
    refs = load_case_refs(args.cases_dir)
    for ref in refs:
        case = ref.case
        roles = ",".join(case.get("roles", []))
        print(f"{case['case_id']}\t{roles}\t{case['title']}")
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    refs = load_case_refs(args.cases_dir)
    errors: list[str] = []
    ids: set[str] = set()
    for ref in refs:
        case_id = ref.case["case_id"]
        if case_id in ids:
            errors.append(f"duplicate case_id {case_id}")
        ids.add(case_id)
        errors.extend(validate_case(ref.case, ref.path))
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print(f"Validated {len(refs)} cases.")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    if args.env_file:
        env_required = args.provider in {"openai", "google", "frontier"} and not args.oracle
        load_env_file(args.env_file, required=env_required)
    refs = load_case_refs(args.cases_dir)
    selected = select_cases(refs, set(args.case_id or []), set(args.role or []))
    if not selected:
        raise SystemExit("No cases selected.")
    provider, model, base_url, api_key = resolve_provider_config(args)
    results = []
    for ref in selected:
        print(f"Running {ref.case['case_id']} via {provider}:{model}...", file=sys.stderr)
        result = run_case(
            ref,
            provider=provider,
            model=model,
            base_url=base_url,
            api_key=api_key,
            role=",".join(args.role) if args.role else None,
            temperature=args.temperature,
            max_tokens=args.max_tokens,
            timeout_s=args.timeout,
            oracle=args.oracle,
        )
        results.append(result)
        print(f"  score={result['score']['score']:.4f}", file=sys.stderr)
    run_payload = {
        "run_id": sha256_text(f"{utc_now()}:{provider}:{model}:{len(results)}")[:16],
        "created_at": utc_now(),
        "provider": provider,
        "model_id": model,
        "base_url": base_url,
        "prompt_version": PROMPT_VERSION,
        "harness_version": HARNESS_VERSION,
        "oracle": args.oracle,
        "summary": summarize(results),
        "results": results,
    }
    json_path, md_path = write_reports(args.report_dir, run_payload)
    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Spacegate agent golden-case evaluations.")
    parser.add_argument("--cases-dir", type=Path, default=DEFAULT_CASES_DIR)
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List available cases.")
    list_parser.set_defaults(func=cmd_list)

    validate_parser = subparsers.add_parser("validate", help="Validate case files.")
    validate_parser.set_defaults(func=cmd_validate)

    run_parser = subparsers.add_parser("run", help="Run selected cases.")
    run_parser.add_argument("--case-id", action="append", help="Case id to run. May be repeated.")
    run_parser.add_argument("--role", action="append", help="Run cases matching role. May be repeated.")
    run_parser.add_argument(
        "--provider",
        choices=["local", "openai", "google", "frontier"],
        default="local",
        help="Inference provider. frontier uses SPACEGATE_FRONTIER_DEFAULT_PROVIDER.",
    )
    run_parser.add_argument("--model", default=None, help="Override provider default model.")
    run_parser.add_argument("--base-url", default=None, help="Override provider default base URL.")
    run_parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help="Optional KEY=value environment file. Defaults to /etc/spacegate/spacegate.env when present.",
    )
    run_parser.add_argument("--temperature", type=float, default=float(os.getenv("SPACEGATE_LLM_TEMPERATURE", "0.0")))
    default_max_tokens = os.getenv("SPACEGATE_FRONTIER_MAX_TOKENS") or os.getenv("SPACEGATE_LLM_MAX_TOKENS", "3000")
    run_parser.add_argument("--max-tokens", type=int, default=int(default_max_tokens))
    run_parser.add_argument("--timeout", type=int, default=int(os.getenv("SPACEGATE_LLM_TIMEOUT_S", "120")))
    run_parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    run_parser.add_argument(
        "--oracle",
        action="store_true",
        help="Use golden expected output instead of calling a model. Useful for harness smoke tests.",
    )
    run_parser.set_defaults(func=cmd_run)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
