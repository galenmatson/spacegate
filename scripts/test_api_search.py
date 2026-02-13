#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from typing import Optional

import duckdb
import requests

ROOT_DIR = Path(__file__).resolve().parents[1]
STATE_DIR = Path(os.getenv("SPACEGATE_STATE_DIR") or ROOT_DIR / "data")
DEFAULT_DB_PATH = STATE_DIR / "served" / "current" / "core.duckdb"
DB_PATH = os.getenv("SPACEGATE_DB_PATH", str(DEFAULT_DB_PATH))


def find_system_by_identifier(kind: str, value: int) -> Optional[int]:
    con = duckdb.connect(DB_PATH, read_only=True)
    column = {"hd": "hd_id", "hip": "hip_id", "gaia": "gaia_id"}[kind]
    row = con.execute(
        f"SELECT system_id FROM systems WHERE {column} = ? LIMIT 1", [value]
    ).fetchone()
    if row:
        return int(row[0])
    row = con.execute(
        f"SELECT s.system_id FROM systems s JOIN stars st ON st.system_id = s.system_id "
        f"WHERE st.{column} = ? LIMIT 1",
        [value],
    ).fetchone()
    if row:
        return int(row[0])
    return None


def find_system_by_name_fragment(fragment: str) -> Optional[int]:
    con = duckdb.connect(DB_PATH, read_only=True)
    row = con.execute(
        "SELECT system_id FROM systems WHERE system_name_norm LIKE ? LIMIT 1",
        [f"%{fragment}%"],
    ).fetchone()
    return int(row[0]) if row else None


def require(condition: bool, message: str):
    if not condition:
        raise AssertionError(message)


def main():
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000/api/v1"

    tau_results = requests.get(
        f"{base_url}/systems/search", params={"q": "Tau", "limit": 5}, timeout=10
    ).json()
    require(tau_results["items"], "searching 'Tau' returned no results")

    target_hd = find_system_by_identifier("hd", 10700)
    if target_hd is not None:
        hd_results = requests.get(
            f"{base_url}/systems/search", params={"q": "HD 10700", "limit": 5}, timeout=10
        ).json()
        ids = {item["system_id"] for item in hd_results["items"]}
        require(target_hd in ids, "HD 10700 did not return Tau Ceti system")
    else:
        print("Skipping HD 10700 test: ID not found in DB")

    target_hip = find_system_by_identifier("hip", 8102)
    if target_hip is not None:
        hip_results = requests.get(
            f"{base_url}/systems/search", params={"q": "HIP 8102", "limit": 5}, timeout=10
        ).json()
        ids = {item["system_id"] for item in hip_results["items"]}
        require(target_hip in ids, "HIP 8102 did not return Tau Ceti system")
    else:
        print("Skipping HIP 8102 test: ID not found in DB")

    alpha_id = find_system_by_name_fragment("alpha centauri")
    if alpha_id is not None:
        alpha_results = requests.get(
            f"{base_url}/systems/search", params={"q": "alpha cen", "limit": 5}, timeout=10
        ).json()
        ids = {item["system_id"] for item in alpha_results["items"]}
        require(alpha_id in ids, "token-and search did not find Alpha Centauri")
    else:
        print("Skipping alpha cen test: Alpha Centauri not found in DB")

    print("Search tests passed.")


if __name__ == "__main__":
    main()
