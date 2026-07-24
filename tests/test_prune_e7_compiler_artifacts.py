from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from prune_e7_compiler_artifacts import PUBLIC_BUILD


def test_public_build_pattern_accepts_standard_and_profiled_names() -> None:
    build_id = "24cb15211f430a37f199f462"

    assert PUBLIC_BUILD.fullmatch(f"e7_{build_id}_public").group(1) == build_id
    assert PUBLIC_BUILD.fullmatch(f"e7_{build_id}_full_public").group(1) == build_id
    assert PUBLIC_BUILD.fullmatch(f"e7_{build_id}_1000ly-exact_public").group(1) == build_id


def test_public_build_pattern_rejects_unbounded_names() -> None:
    assert PUBLIC_BUILD.fullmatch("e7_not-a-build_full_public") is None
    assert PUBLIC_BUILD.fullmatch("prefix_e7_24cb15211f430a37f199f462_public") is None
    assert PUBLIC_BUILD.fullmatch("e7_24cb15211f430a37f199f462_public.tmp") is None
