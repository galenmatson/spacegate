#!/usr/bin/env python3
"""Independently verify and reproduce E7 stellar orbit selection."""

from __future__ import annotations

import argparse
import json
import resource
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

import compile_e7_stellar_orbit_runtime as compiler


DEFAULT_SCRATCH = Path("/mnt/space/spacegate")
PRODUCTS = {
    "selected_stellar_orbit_relations.parquet": "relations",
    "selected_stellar_orbit_solutions.parquet": "solutions",
    "deferred_stellar_orbit_context.parquet": "deferred",
}


def audit(policy_path: Path, state: Path, manifest_path: Path) -> dict[str, Any]:
    started=time.monotonic();policy=compiler.load_object(policy_path);compiler.validate_policy(policy)
    manifest=compiler.load_object(manifest_path)
    checks={
        "manifest_status_pass":manifest.get("status")=="pass",
        "manifest_schema":manifest.get("schema_version")=="spacegate.e7_stellar_orbit_runtime_manifest.v2",
        "stability_databases_not_opened":manifest.get("stability_databases_opened")==[],
        "policy_sha256_match":manifest.get("policy_sha256")==compiler.file_sha256(policy_path),
        "compiler_sha256_match":manifest.get("compiler_sha256")==compiler.file_sha256(Path(compiler.__file__).resolve()),
    }
    database,_=compiler.resolve_input(policy,state);paths={}
    for filename,table in PRODUCTS.items():
        path=manifest_path.parent/filename;product=(manifest.get("products") or {}).get(filename) or {};paths[table]=path;prefix=filename.removesuffix(".parquet")
        checks[f"{prefix}_exists"]=path.is_file();checks[f"{prefix}_bytes_match"]=path.is_file() and path.stat().st_size==product.get("bytes");checks[f"{prefix}_sha256_match"]=path.is_file() and compiler.file_sha256(path)==product.get("sha256");checks[f"{prefix}_byte_exact"]=product.get("determinism")=="byte_exact"
    con=duckdb.connect();metrics={}
    try:
        for table,path in paths.items():con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet({compiler.sql_literal(path)})")
        con.execute(f"ATTACH {compiler.sql_literal(database)} AS selected (READ_ONLY)")
        counts={
            "selected_relations":int(con.execute("SELECT count(*) FROM relations").fetchone()[0]),
            "selected_solutions":int(con.execute("SELECT count(*) FROM solutions").fetchone()[0]),
            "msc_solutions":int(con.execute("SELECT count(*) FROM solutions WHERE source_kind IN ('msc_orb','msc_sys')").fetchone()[0]),
            "orb6_solutions":int(con.execute("SELECT count(*) FROM solutions WHERE source_kind='orb6'").fetchone()[0]),
            "sb9_solutions":int(con.execute("SELECT count(*) FROM solutions WHERE source_kind='sb9'").fetchone()[0]),
            "debcat_solutions":int(con.execute("SELECT count(*) FROM solutions WHERE source_kind='debcat'").fetchone()[0]),
            "preferred_simulation_solutions":int(con.execute("SELECT count(*) FROM solutions WHERE selection_role='preferred_simulation'").fetchone()[0]),
            "deferred_sbx_solutions":int(con.execute("SELECT solution_count FROM deferred WHERE source_kind='sbx'").fetchone()[0]),
            "deferred_gaia_nss_solutions":int(con.execute("SELECT solution_count FROM deferred WHERE source_kind='gaia_nss'").fetchone()[0]),
            "deferred_tess_eb_solutions":int(con.execute("SELECT solution_count FROM deferred WHERE source_kind='tess_eb'").fetchone()[0]),
            "canonical_containment_promotions":int(con.execute("SELECT (SELECT count(*) FROM relations WHERE canonical_containment)+(SELECT count(*) FROM solutions WHERE canonical_containment)").fetchone()[0]),
        }
        expected={key:int(value) for key,value in policy["acceptance"].items()}
        scalars={
            "acceptance_count_delta":sum(abs(counts.get(key,-1)-value) for key,value in expected.items()),
            "manifest_count_delta":sum(abs(counts.get(key,-1)-int(value)) for key,value in (manifest.get("verification",{}).get("counts") or {}).items()),
            "duplicate_relations":int(con.execute("SELECT count(*) FROM (SELECT relation_id FROM relations GROUP BY 1 HAVING count(*)<>1)").fetchone()[0]),
            "duplicate_solutions":int(con.execute("SELECT count(*) FROM (SELECT selected_orbit_solution_id FROM solutions GROUP BY 1 HAVING count(*)<>1)").fetchone()[0]),
            "orphan_solutions":int(con.execute("SELECT count(*) FROM solutions s LEFT JOIN relations r USING(relation_id) WHERE r.relation_id IS NULL").fetchone()[0]),
            "multiple_preferences":int(con.execute("SELECT count(*) FROM (SELECT relation_id FROM solutions WHERE selection_role='preferred_simulation' GROUP BY 1 HAVING count(*)<>1)").fetchone()[0]),
            "incomplete_preferences":int(con.execute("SELECT count(*) FROM solutions WHERE selection_role='preferred_simulation' AND NOT simulation_complete").fetchone()[0]),
            "nonphysical_visual_preferences":int(con.execute("SELECT count(*) FROM solutions WHERE selection_role='preferred_simulation' AND (period_days<=0 OR semi_major_axis_arcsec<=0 OR eccentricity<0 OR eccentricity>=1)").fetchone()[0]),
            "preference_pointer_delta":abs(int(con.execute("SELECT count(*) FROM relations WHERE preferred_simulation_solution_id IS NOT NULL").fetchone()[0])-counts["preferred_simulation_solutions"]),
            "missing_relation_evidence":int(con.execute("SELECT count(*) FROM relations r LEFT JOIN selected.msc_relation_evidence_projection e ON e.projected_relation_id=r.relation_id WHERE e.projected_relation_id IS NULL").fetchone()[0]),
            "missing_solution_evidence":int(con.execute("SELECT count(*) FROM solutions s WHERE NOT EXISTS (SELECT 1 FROM selected.msc_orbital_solution_projection e WHERE e.evidence_id=s.evidence_id) AND NOT EXISTS (SELECT 1 FROM selected.orb6_orbital_solution_projection e WHERE e.evidence_id=s.evidence_id) AND NOT EXISTS (SELECT 1 FROM selected.sb9_orbital_solution_projection e WHERE e.evidence_id=s.evidence_id) AND NOT EXISTS (SELECT 1 FROM selected.debcat_orbital_solution_projection e WHERE e.evidence_id=s.evidence_id)").fetchone()[0]),
            "missing_lineage":int(con.execute("SELECT count(*) FROM solutions WHERE source_id IS NULL OR release_id IS NULL OR evidence_id IS NULL OR parameter_set_raw IS NULL").fetchone()[0]),
            "context_preferred":int(con.execute("SELECT count(*) FROM solutions WHERE selection_role='preferred_simulation' AND source_kind NOT IN ('orb6','msc_orb')").fetchone()[0]),
        }
        checks.update({key:value==0 for key,value in scalars.items()});metrics={"counts":counts,"scalar_checks":scalars}
    finally:con.close()
    failures=sorted(key for key,value in checks.items() if not value)
    return {"schema_version":"spacegate.e7_stellar_orbit_runtime_verification.v1","generated_at":datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),"build_id":manifest.get("build_id"),"status":"pass" if not failures else "fail","checks":checks,"failing_checks":failures,"metrics":metrics,"wall_seconds":round(time.monotonic()-started,6)}


def reproduce(policy_path: Path,state: Path,manifest_path: Path,scratch_parent: Path) -> dict[str,Any]:
    started=time.monotonic();cpu_started=time.process_time();reference=compiler.load_object(manifest_path);scratch=Path(tempfile.mkdtemp(prefix="e7-stellar-orbit-reproduction-",dir=scratch_parent))
    try:
        rebuilt=compiler.compile_runtime(policy_path,state,scratch/"artifacts",link_into_state=False);rebuilt_manifest=scratch/"artifacts"/rebuilt["build_id"]/"manifest.json";independent=audit(policy_path,state,rebuilt_manifest)
        checks={"build_id_match":rebuilt.get("build_id")==reference.get("build_id"),"input_match":rebuilt.get("input")==reference.get("input"),"products_match":rebuilt.get("products")==reference.get("products"),"verification_match":rebuilt.get("verification")==reference.get("verification"),"independent_verification_pass":independent.get("status")=="pass"}
    finally:shutil.rmtree(scratch,ignore_errors=True)
    checks["scratch_removed"]=not scratch.exists();failures=sorted(key for key,value in checks.items() if not value);usage=resource.getrusage(resource.RUSAGE_SELF)
    return {"schema_version":"spacegate.e7_stellar_orbit_runtime_reproduction.v1","generated_at":datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),"build_id":reference.get("build_id"),"status":"pass" if not failures else "fail","checks":checks,"failing_checks":failures,"rebuild_performance":rebuilt.get("performance"),"total_timing":{"wall_seconds":round(time.monotonic()-started,6),"cpu_seconds":round(time.process_time()-cpu_started,6),"peak_rss_kib":int(usage.ru_maxrss)}}


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--policy",type=Path,default=compiler.DEFAULT_POLICY);parser.add_argument("--state-dir",type=Path,default=compiler.DEFAULT_STATE);parser.add_argument("--manifest",type=Path,required=True);parser.add_argument("--reproduce",action="store_true");parser.add_argument("--scratch-parent",type=Path,default=DEFAULT_SCRATCH);parser.add_argument("--report",type=Path);args=parser.parse_args();report=reproduce(args.policy.resolve(),args.state_dir.resolve(),args.manifest.resolve(),args.scratch_parent.resolve()) if args.reproduce else audit(args.policy.resolve(),args.state_dir.resolve(),args.manifest.resolve());rendered=json.dumps(report,indent=2,sort_keys=True)+"\n"
    if args.report:args.report.parent.mkdir(parents=True,exist_ok=True);args.report.write_text(rendered,encoding="utf-8")
    print(rendered,end="");return 0 if report["status"]=="pass" else 1


if __name__=="__main__":raise SystemExit(main())
