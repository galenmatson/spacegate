#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
import re
from datetime import datetime, timezone
from pathlib import Path


WDS_PATTERN = re.compile(r"(?<![0-9])[0-9]{5}[+-][0-9]{4}(?![0-9])")
CONCRETE_KEY_PATTERN = re.compile(
    r"(?:canon:)?(?:system|star|planet):(?:wds|gaia|hip|hd|accepted_supplement):[^'\"|\s,)]+"
)
SQL_OBJECT_COMPARISON_PATTERN = re.compile(
    r"(?<!['A-Za-z0-9_])(?:system_name(?:_norm)?|star_name(?:_norm)?|object_name(?:_norm)?|stable_object_key|"
    r"wds_id|gaia_id|hip_id|hd_id|source_pk)\s*=\s*'([^']+)'",
    re.IGNORECASE,
)
DOMAIN_AUTHORITY_LITERALS = {
    "earth", "moon", "pluto", "charon", "sol", "sun",
    "system:sol", "star:sol:sun",
}
OBJECT_NAMES = (
    "Alpha Centauri",
    "AR Cassiopeiae",
    "Castor",
    "L 134-80",
    "Nu Sco",
    "Nu Scorpii",
    "Proxima Centauri",
    "Sirius",
    "TRAPPIST-1",
    "W Ursae Majoris",
    "YY Gem",
    "16 Cyg",
    "70 Oph",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def candidate_files(root: Path) -> list[Path]:
    paths = [root / "scripts" / "ingest_core.py", root / "scripts" / "build_arm.py"]
    paths.extend(sorted((root / "scripts" / "ingest").glob("*.py")))
    return [path for path in paths if path.exists()]


def extracted_literals(value: str) -> set[str]:
    found = set(WDS_PATTERN.findall(value))
    found.update(
        token for token in CONCRETE_KEY_PATTERN.findall(value)
        if any(character.isdigit() for character in token)
    )
    found.update(SQL_OBJECT_COMPARISON_PATTERN.findall(value))
    for name in OBJECT_NAMES:
        if re.search(rf"(?<![0-9A-Za-z]){re.escape(name)}(?![0-9A-Za-z])", value, re.IGNORECASE):
            found.add(name)
    return found


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Detect object-specific literals in production science transforms."
    )
    parser.add_argument("--root", default=None)
    parser.add_argument("--report", default=None)
    args = parser.parse_args()

    root = Path(args.root).resolve() if args.root else Path(__file__).resolve().parents[1]
    policy_path = root / "config" / "science_transform_literal_allowlist.json"
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    allowed = {
        (str(entry["path"]), str(entry["literal"])): str(entry["reason"])
        for entry in policy.get("entries", [])
    }

    findings: list[dict[str, object]] = []
    for path in candidate_files(root):
        relative = str(path.relative_to(root))
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=relative)
        seen: set[tuple[int, str]] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                for literal in sorted(extracted_literals(node.value)):
                    if (
                        literal.lower() in DOMAIN_AUTHORITY_LITERALS
                        or literal.startswith(("system:sol", "star:sol", "planet:sol"))
                    ):
                        continue
                    finding_key = (int(getattr(node, "lineno", 0)), literal)
                    if finding_key in seen:
                        continue
                    seen.add(finding_key)
                    reason = allowed.get((relative, literal))
                    findings.append(
                        {
                            "path": relative,
                            "line": int(getattr(node, "lineno", 0)),
                            "literal": literal,
                            "status": "allowed" if reason else "unexpected",
                            "allow_reason": reason,
                        }
                    )
            if isinstance(node, ast.Compare):
                left = ast.unparse(node.left).lower()
                identity_field = any(
                    token in left
                    for token in (
                        "system_name", "star_name", "object_name", "stable_object_key",
                        "wds_id", "gaia_id", "hip_id", "hd_id", "source_pk",
                    )
                )
                if not identity_field:
                    continue
                for comparator in node.comparators:
                    if not isinstance(comparator, ast.Constant) or not isinstance(comparator.value, str):
                        continue
                    literal = comparator.value.strip()
                    if not literal or literal.lower() in DOMAIN_AUTHORITY_LITERALS:
                        continue
                    finding_key = (int(getattr(node, "lineno", 0)), literal)
                    if finding_key in seen:
                        continue
                    seen.add(finding_key)
                    reason = allowed.get((relative, literal))
                    findings.append(
                        {
                            "path": relative,
                            "line": int(getattr(node, "lineno", 0)),
                            "literal": literal,
                            "status": "allowed" if reason else "unexpected",
                            "allow_reason": reason,
                        }
                    )

    # Executable object supplement files are forbidden. Deferred adjudication
    # records are intentionally data for review and are not consumed by builds.
    forbidden_configs = [root / "config" / "core_accepted_supplements.json"]
    for path in forbidden_configs:
        if path.exists():
            findings.append(
                {
                    "path": str(path.relative_to(root)),
                    "line": 1,
                    "literal": path.name,
                    "status": "unexpected",
                    "allow_reason": None,
                }
            )

    unexpected = [row for row in findings if row["status"] == "unexpected"]
    report = {
        "generated_at": utc_now(),
        "policy_version": policy.get("version"),
        "scanned_files": [str(path.relative_to(root)) for path in candidate_files(root)],
        "finding_count": len(findings),
        "allowed_count": len(findings) - len(unexpected),
        "unexpected_count": len(unexpected),
        "status": "pass" if not unexpected else "fail",
        "findings": findings,
        "limitations": [
            "Static literal detection complements, but does not replace, review of data-dependent special cases.",
            "Goldens, tests, fixtures, reports, and operator acquisition seeds may name objects without changing science outputs."
        ],
    }
    report_path = (
        Path(args.report)
        if args.report
        else root / "reports" / "science_transform_exception_audit.json"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(report_path)
    if unexpected:
        for row in unexpected:
            print(f"unexpected {row['path']}:{row['line']}: {row['literal']}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
