from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import verify_e7_cutover_plan as cutover  # noqa: E402


def test_checked_in_e7_cutover_and_dr4_plans_pass() -> None:
    report = cutover.verify(ROOT, cutover.DEFAULT_LEGACY, cutover.DEFAULT_DR4)

    assert report["status"] == "pass"
    assert report["failing_checks"] == []
    assert len(report["checks"]) == 19


def test_dr4_plan_rejects_interchangeable_release_ids(tmp_path: Path) -> None:
    plan = json.loads(cutover.DEFAULT_DR4.read_text(encoding="utf-8"))
    plan["identity_contract"]["gaia_dr3_and_dr4_ids_interchangeable"] = True
    changed = tmp_path / "gaia_dr4_adapter_plan.json"
    changed.write_text(json.dumps(plan), encoding="utf-8")

    report = cutover.verify(ROOT, cutover.DEFAULT_LEGACY, changed)

    assert report["status"] == "fail"
    assert [item["name"] for item in report["failing_checks"]] == [
        "dr3_dr4_not_interchangeable"
    ]
