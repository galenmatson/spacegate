#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi

PYTHON_BIN=""
if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
else
  echo "Error: python3 not found." >&2
  exit 1
fi

exec "$PYTHON_BIN" - "$ROOT_DIR" <<'PY'
import json
import os
import shutil
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

root = Path(sys.argv[1]).resolve()
state_dir = Path(os.environ.get("SPACEGATE_STATE_DIR") or os.environ.get("SPACEGATE_DATA_DIR") or root / "data").resolve()
report_dir = state_dir / "reports" / "bootstrap_audit"
report_dir.mkdir(parents=True, exist_ok=True)
report_path = report_dir / f"photon_bootstrap_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"


def run(cmd: list[str], timeout: int = 10) -> dict:
    try:
        proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout, check=False)
        return {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "stdout": proc.stdout.strip(),
            "stderr": proc.stderr.strip(),
        }
    except Exception as exc:
        return {"ok": False, "returncode": None, "stdout": "", "stderr": str(exc)}


def command_check(cmd: str, version_args: list[str] | None = None) -> dict:
    path = shutil.which(cmd)
    result = {"present": bool(path), "path": path}
    if path and version_args:
        result["version"] = run([path, *version_args], timeout=15)
    return result


def import_check(py: Path, modules: list[str]) -> dict:
    if not py.exists():
        return {"ok": False, "python": str(py), "error": "missing python executable"}
    code = "\n".join([f"import {name}" for name in modules]) + "\nprint('ok')\n"
    result = run([str(py), "-c", code], timeout=30)
    return {"ok": result["ok"], "python": str(py), "modules": modules, "result": result}


def path_check(path: Path, kind: str = "any") -> dict:
    exists = path.exists()
    ok = exists
    if kind == "dir":
        ok = path.is_dir()
    elif kind == "file":
        ok = path.is_file()
    return {"ok": ok, "path": str(path), "exists": exists, "kind": kind}


def disk_summary(path: Path) -> dict:
    usage = shutil.disk_usage(path if path.exists() else path.parent)
    return {
        "path": str(path),
        "total_gib": round(usage.total / (1024**3), 1),
        "used_gib": round(usage.used / (1024**3), 1),
        "free_gib": round(usage.free / (1024**3), 1),
    }


commands = {
    "python3": command_check("python3", ["--version"]),
    "pip3": command_check("pip3", ["--version"]),
    "git": command_check("git", ["--version"]),
    "curl": command_check("curl", ["--version"]),
    "aria2c": command_check("aria2c", ["--version"]),
    "gzip": command_check("gzip", ["--version"]),
    "7z": command_check("7z"),
    "node": command_check("node", ["--version"]),
    "npm": command_check("npm", ["--version"]),
    "duckdb": command_check("duckdb", ["--version"]),
    "docker": command_check("docker", ["--version"]),
    "nvidia-smi": command_check("nvidia-smi", ["--query-gpu=index,name,memory.total,driver_version", "--format=csv,noheader"]),
}

state_paths = {
    name: path_check(state_dir / name, "dir")
    for name in ["raw", "cooked", "out", "reports", "served", "cache", "logs"]
}

checks = {
    "root_venv_imports": import_check(root / ".venv" / "bin" / "python", ["duckdb", "pandas", "pyarrow", "polars"]),
    "api_venv_imports": import_check(root / "srv" / "api" / ".venv" / "bin" / "python", ["fastapi", "uvicorn", "duckdb"]),
    "web_node_modules": path_check(root / "srv" / "web" / "node_modules", "dir"),
    "web_dist_index": path_check(root / "srv" / "web" / "dist" / "index.html", "file"),
    "docker_info": run(["docker", "info", "--format", "DockerRootDir={{.DockerRootDir}} Runtimes={{json .Runtimes}}"], timeout=20)
    if shutil.which("docker")
    else {"ok": False, "stderr": "docker missing"},
}

preflight = run([str(root / "scripts" / "preflight_full_refresh.sh")], timeout=30)

required_commands = ["python3", "pip3", "git", "curl", "aria2c", "gzip", "7z", "node", "npm", "duckdb"]
failed = []
for cmd in required_commands:
    if not commands[cmd]["present"]:
        failed.append(f"missing command: {cmd}")
for name, item in state_paths.items():
    if not item["ok"]:
        failed.append(f"missing state path: {name}")
for name in ["root_venv_imports", "api_venv_imports", "web_node_modules", "web_dist_index"]:
    if not checks[name]["ok"]:
        failed.append(f"failed check: {name}")
if not preflight["ok"]:
    failed.append("full refresh preflight failed")

payload = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "host": socket.gethostname(),
    "root_dir": str(root),
    "state_dir": str(state_dir),
    "environment": {
        key: os.environ.get(key)
        for key in [
            "SPACEGATE_STATE_DIR",
            "SPACEGATE_DATA_DIR",
            "SPACEGATE_CACHE_DIR",
            "SPACEGATE_LOG_DIR",
            "SPACEGATE_DL_ROOT",
            "SPACEGATE_DUCKDB_MEMORY_LIMIT",
            "SPACEGATE_DUCKDB_THREADS",
            "SPACEGATE_ENABLE_GAIA_BACKBONE",
            "SPACEGATE_ENABLE_GAIA_CLASSPROB",
            "SPACEGATE_ENABLE_GAIA_NSS",
            "SPACEGATE_ENABLE_MSC",
            "SPACEGATE_ENABLE_SBX",
            "SPACEGATE_ENABLE_WDS_GAIA_XMATCH",
        ]
    },
    "disk": disk_summary(state_dir),
    "commands": commands,
    "state_paths": state_paths,
    "checks": checks,
    "preflight_full_refresh": preflight,
    "status": "ok" if not failed else "fail",
    "failures": failed,
}

report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

print(f"bootstrap_audit_status={payload['status']}")
print(f"bootstrap_audit_report={report_path}")
if failed:
    for item in failed:
        print(f"failure={item}")
    raise SystemExit(1)
PY
