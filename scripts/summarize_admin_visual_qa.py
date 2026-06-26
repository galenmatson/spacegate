#!/usr/bin/env python3
"""Print a concise summary for a Spacegate Admin Playwright visual QA run."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as exc:
        return {"status": "invalid_json", "path": str(path), "error": str(exc)}


def count_playwright_failures(report: dict[str, Any] | None) -> tuple[int, int]:
    if not report:
        return (0, 0)
    stats = report.get("stats") or {}
    unexpected = int(stats.get("unexpected") or 0)
    flaky = int(stats.get("flaky") or 0)
    return (unexpected, flaky)


def format_issue_counts(summary: dict[str, Any]) -> str:
    console_events = summary.get("consoleEvents") or []
    request_failures = summary.get("requestFailures") or []
    page_errors = summary.get("pageErrors") or []
    parts = [
        f"console={len(console_events)}",
        f"network={len(request_failures)}",
        f"page_errors={len(page_errors)}",
    ]
    if summary.get("status") == "captured":
        overflow = [
            screen.get("key") or screen.get("label") or "unknown"
            for screen in summary.get("screens") or []
            if (screen.get("metrics") or {}).get("horizontalOverflow")
        ]
        parts.append(f"overflow={len(overflow)}")
        if overflow:
            parts.append(f"overflow_screens={','.join(overflow)}")
    return " ".join(parts)


def print_captured_details(summary: dict[str, Any]) -> None:
    for screen in summary.get("screens") or []:
        metrics = screen.get("metrics") or {}
        flags: list[str] = []
        if metrics.get("horizontalOverflow"):
            flags.append("horizontal-overflow")
        danger_count = int(metrics.get("dangerBadgeCount") or 0)
        warning_count = int(metrics.get("warningBadgeCount") or 0)
        if danger_count:
            flags.append(f"danger={danger_count}")
        if warning_count:
            flags.append(f"warn={warning_count}")
        suffix = f" [{' '.join(flags)}]" if flags else ""
        screenshot = screen.get("screenshot") or ""
        print(
            f"    - {screen.get('label') or screen.get('key')}: "
            f"panels={metrics.get('panelCount', 0)} "
            f"kpis={metrics.get('kpiCount', 0)} "
            f"height={metrics.get('bodyScrollHeight', 0)}{suffix}"
        )
        if screenshot:
            print(f"      screenshot: {screenshot}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize Admin v2 Playwright visual QA artifacts."
    )
    parser.add_argument("run_dir", help="Visual QA run directory.")
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    captures_dir = run_dir / "captures"
    playwright_report = load_json(run_dir / "playwright-report.json")
    unexpected, flaky = count_playwright_failures(playwright_report)

    print("Admin visual QA summary")
    print(f"  run: {run_dir}")
    print(f"  html: {run_dir / 'html' / 'index.html'}")
    if playwright_report:
        print(f"  playwright: unexpected={unexpected} flaky={flaky}")

    summary_paths = sorted(captures_dir.glob("*/visual-summary.json"))
    if not summary_paths:
        print("  captures: none")
        print("  next: check Playwright output and trace artifacts for startup failures.")
        return 0

    statuses: set[str] = set()
    for summary_path in summary_paths:
        viewport = summary_path.parent.name
        summary = load_json(summary_path) or {}
        status = str(summary.get("status") or "unknown")
        statuses.add(status)
        print(f"  {viewport}: {status} {format_issue_counts(summary)}")
        print(f"    requested: {summary.get('requestedUrl') or ''}")
        print(f"    final: {summary.get('finalUrl') or ''}")
        screenshot = summary.get("screenshot")
        if screenshot:
            print(f"    screenshot: {screenshot}")
        if status == "captured":
            print_captured_details(summary)
        elif status == "auth_required":
            print("    next: create storage state with scripts/create_admin_storage_state.sh")
        elif status == "wrong_app":
            print("    next: check SPACEGATE_ADMIN_VISUAL_BASE_URL and host routing")

    if statuses == {"auth_required"}:
        print(
            "  result: auth gate captured only; authenticated screen QA needs "
            "SPACEGATE_ADMIN_STORAGE_STATE."
        )
    elif "captured" in statuses:
        print("  result: authenticated Admin screen captures are available.")
    else:
        print("  result: visual QA did not capture Admin screens.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
