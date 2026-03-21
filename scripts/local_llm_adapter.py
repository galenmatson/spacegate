#!/usr/bin/env python3
"""Tiny OpenAI-compatible local LLM adapter for Spacegate workflows.

Default target is LM Studio, but any OpenAI-compatible endpoint should work.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import requests


DEFAULT_BASE_URL = "http://127.0.0.1:1234/v1"
DEFAULT_MODEL = "openai/gpt-oss-20b"
DEFAULT_TIMEOUT_S = 90
DEFAULT_MAX_TOKENS = 256
DEFAULT_TEMPERATURE = 0.1
DEFAULT_MAX_INPUT_CHARS = 12000


def _normalize_base_url(url: str) -> str:
    value = (url or "").strip().rstrip("/")
    if not value:
        value = DEFAULT_BASE_URL
    if not value.endswith("/v1"):
        value = f"{value}/v1"
    return value


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise SystemExit(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise SystemExit(f"{name} must be a float, got {raw!r}") from exc


def _build_session() -> requests.Session:
    session = requests.Session()
    api_key = os.getenv("SPACEGATE_LLM_API_KEY", "").strip()
    if api_key:
        session.headers.update({"Authorization": f"Bearer {api_key}"})
    return session


def _read_prompt(args: argparse.Namespace) -> str:
    if bool(args.prompt) == bool(args.prompt_file):
        raise SystemExit("Provide exactly one of --prompt or --prompt-file.")
    if args.prompt:
        return args.prompt.strip()
    text = Path(args.prompt_file).read_text(encoding="utf-8")
    return text.strip()


def _request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout_s: int,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        if method == "GET":
            response = session.get(url, timeout=timeout_s)
        else:
            response = session.post(url, json=payload, timeout=timeout_s)
    except requests.RequestException as exc:
        raise SystemExit(f"request failed: {exc}") from exc

    if response.status_code >= 400:
        body = response.text[:800]
        raise SystemExit(f"request failed with {response.status_code}: {body}")

    try:
        return response.json()
    except ValueError as exc:
        raise SystemExit("endpoint returned non-JSON response") from exc


def cmd_models(args: argparse.Namespace) -> int:
    base_url = _normalize_base_url(args.base_url)
    timeout_s = args.timeout
    session = _build_session()
    payload = _request_json(session, "GET", f"{base_url}/models", timeout_s=timeout_s)

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    data = payload.get("data", [])
    if not isinstance(data, list):
        raise SystemExit("unexpected /models response shape")
    for row in data:
        if isinstance(row, dict) and row.get("id"):
            print(row["id"])
    return 0


def cmd_chat(args: argparse.Namespace) -> int:
    base_url = _normalize_base_url(args.base_url)
    model = args.model
    timeout_s = args.timeout
    max_input_chars = args.max_input_chars
    prompt = _read_prompt(args)

    if len(prompt) > max_input_chars:
        raise SystemExit(
            f"prompt has {len(prompt)} chars which exceeds --max-input-chars={max_input_chars}. "
            "Increase limit explicitly if intended."
        )

    messages: list[dict[str, str]] = []
    if args.system:
        messages.append({"role": "system", "content": args.system})
    messages.append({"role": "user", "content": prompt})

    request_payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": args.temperature,
        "max_tokens": args.max_tokens,
    }
    if args.seed is not None:
        request_payload["seed"] = args.seed

    session = _build_session()
    payload = _request_json(
        session,
        "POST",
        f"{base_url}/chat/completions",
        timeout_s=timeout_s,
        payload=request_payload,
    )

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        content = ""
        choices = payload.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0]
            if isinstance(first, dict):
                message = first.get("message")
                if isinstance(message, dict):
                    content = str(message.get("content") or "")
        sys.stdout.write(content)
        if content and not content.endswith("\n"):
            sys.stdout.write("\n")

    if args.show_usage:
        usage = payload.get("usage", {})
        if isinstance(usage, dict) and usage:
            prompt_tokens = usage.get("prompt_tokens")
            completion_tokens = usage.get("completion_tokens")
            total_tokens = usage.get("total_tokens")
            print(
                (
                    f"[usage] prompt={prompt_tokens} completion={completion_tokens} total={total_tokens} "
                    f"model={model} base_url={base_url}"
                ),
                file=sys.stderr,
            )

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Local LLM adapter for OpenAI-compatible endpoints (LM Studio, etc.)."
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("SPACEGATE_LLM_BASE_URL", DEFAULT_BASE_URL),
        help="OpenAI-compatible base URL (default from SPACEGATE_LLM_BASE_URL).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=_env_int("SPACEGATE_LLM_TIMEOUT_S", DEFAULT_TIMEOUT_S),
        help="HTTP timeout in seconds.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    models_parser = subparsers.add_parser("models", help="List available models.")
    models_parser.add_argument("--json", action="store_true", help="Emit raw JSON.")
    models_parser.set_defaults(func=cmd_models)

    chat_parser = subparsers.add_parser("chat", help="Run a single chat completion.")
    chat_parser.add_argument(
        "--model",
        default=os.getenv("SPACEGATE_LLM_MODEL", DEFAULT_MODEL),
        help="Model ID (default from SPACEGATE_LLM_MODEL).",
    )
    chat_parser.add_argument("--system", default="", help="Optional system message.")
    chat_parser.add_argument("--prompt", help="User prompt text.")
    chat_parser.add_argument("--prompt-file", help="Read prompt from a UTF-8 text file.")
    chat_parser.add_argument(
        "--temperature",
        type=float,
        default=_env_float("SPACEGATE_LLM_TEMPERATURE", DEFAULT_TEMPERATURE),
        help="Sampling temperature.",
    )
    chat_parser.add_argument(
        "--max-tokens",
        type=int,
        default=_env_int("SPACEGATE_LLM_MAX_TOKENS", DEFAULT_MAX_TOKENS),
        help="Completion token cap.",
    )
    chat_parser.add_argument(
        "--max-input-chars",
        type=int,
        default=_env_int("SPACEGATE_LLM_MAX_INPUT_CHARS", DEFAULT_MAX_INPUT_CHARS),
        help="Hard prompt-size guardrail for budget control.",
    )
    chat_parser.add_argument("--seed", type=int, default=None, help="Optional RNG seed.")
    chat_parser.add_argument("--show-usage", action="store_true", help="Print token usage to stderr.")
    chat_parser.add_argument("--json", action="store_true", help="Emit raw JSON instead of message content.")
    chat_parser.set_defaults(func=cmd_chat)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
