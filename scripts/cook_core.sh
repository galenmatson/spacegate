#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi
STATE_DIR="${SPACEGATE_STATE_DIR:-${SPACEGATE_DATA_DIR:-$ROOT_DIR/data}}"
RAW_DIR="$STATE_DIR/raw"
COOKED_DIR="$STATE_DIR/cooked"
LOG_DIR="${SPACEGATE_LOG_DIR:-$STATE_DIR/logs}"
PYTHON_BIN=""

ATHYG_PART1="${ATHYG_PART1:-$RAW_DIR/athyg/athyg_v33-1.csv.gz}"
ATHYG_PART2="${ATHYG_PART2:-$RAW_DIR/athyg/athyg_v33-2.csv.gz}"
NASA_RAW="$RAW_DIR/nasa_exoplanet_archive/pscomppars.csv"
NASA_PS_RAW="$RAW_DIR/nasa_exoplanet_archive/ps.csv"
WDS_RAW="$RAW_DIR/wds/wdsweb_summ2.txt"
MSC_RAW="$RAW_DIR/msc/newmsc-20260619.tar.gz"
ORB6_RAW="$RAW_DIR/orb6/orb6orbits.sql"
DEBCAT_RAW="$RAW_DIR/debcat/debs.dat"
KEPLER_EB_RAW="$RAW_DIR/kepler_eb/kepler_eb_catalog.csv"
GAIA_BACKBONE_RAW="$RAW_DIR/gaia_backbone/gaia_dr3_backbone.csv"
GAIA_NSS_NON_SINGLE_RAW="$RAW_DIR/gaia_nss/gaia_dr3_non_single_star.csv"
GAIA_NSS_TWO_BODY_RAW="$RAW_DIR/gaia_nss/gaia_dr3_nss_two_body_orbit.csv"
WDS_GAIA_XMATCH_RAW="$RAW_DIR/wds_gaia_xmatch/wds_gaia_best.csv"
SBX_SYSTEMS_RAW="$RAW_DIR/sbx/sbx_systems.csv"
SBX_ALIAS_RAW="$RAW_DIR/sbx/sbx_alias.csv"
SBX_CONFIG_RAW="$RAW_DIR/sbx/sbx_configurations.csv"
SBX_ORBITS_RAW="$RAW_DIR/sbx/sbx_orbits.csv"
SB9_README_RAW="$RAW_DIR/sb9/ReadMe"
SB9_MAIN_RAW="$RAW_DIR/sb9/main.dat"
SB9_ALIAS_RAW="$RAW_DIR/sb9/alias.dat"
SB9_ORBITS_RAW="$RAW_DIR/sb9/orbits.dat"
EXOPLANET_EU_RAW="$RAW_DIR/exoplanet_eu/catalog.csv"
OEC_RAW="$RAW_DIR/open_exoplanet_catalogue/open_exoplanet_catalogue.tar.gz"
HWC_RAW="$RAW_DIR/hwc/hwc.csv"
SOL_AUTHORITY_RAW="$RAW_DIR/sol_authority/sol_system_objects.csv"
SOL_ARTIFICIAL_RAW="$RAW_DIR/sol_artificial/sol_artificial_objects.csv"

COOKED_ATHYG_DIR="$COOKED_DIR/athyg"
COOKED_NASA_DIR="$COOKED_DIR/nasa_exoplanet_archive"
COOKED_GAIA_BACKBONE_DIR="$COOKED_DIR/gaia_backbone"
COOKED_SOL_AUTHORITY_DIR="$COOKED_DIR/sol_authority"
COOKED_SOL_ARTIFICIAL_DIR="$COOKED_DIR/sol_artificial"
COOKED_ATHYG="$COOKED_ATHYG_DIR/athyg.csv.gz"
COOKED_NASA="$COOKED_NASA_DIR/pscomppars_clean.csv"
COOKED_NASA_PS="$COOKED_NASA_DIR/ps_clean.csv"
COOKED_GAIA_BACKBONE="$COOKED_GAIA_BACKBONE_DIR/gaia_dr3_backbone.csv"
COOKED_SOL_AUTHORITY="$COOKED_SOL_AUTHORITY_DIR/sol_system_objects.csv"
COOKED_SOL_ARTIFICIAL="$COOKED_SOL_ARTIFICIAL_DIR/sol_artificial_objects.csv"

LOG_FILE="$LOG_DIR/cook_core.log"

log() {
  local msg="$1"
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  printf '%s %s\n' "$ts" "$msg" | tee -a "$LOG_FILE"
}

require_command() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: required command '$cmd' not found." >&2
    exit 1
  fi
}

is_lfs_pointer() {
  local path="$1"
  [[ -f "$path" ]] || return 1
  head -c 256 "$path" 2>/dev/null | grep -aq "version https://git-lfs.github.com/spec/v1"
}

ensure_inputs() {
  local enable_gaia_backbone="${SPACEGATE_ENABLE_GAIA_BACKBONE:-0}"
  local enable_msc="${SPACEGATE_ENABLE_MSC:-1}"
  local enable_gaia_nss="${SPACEGATE_ENABLE_GAIA_NSS:-1}"
  local enable_sbx="${SPACEGATE_ENABLE_SBX:-1}"
  local enable_sb9="${SPACEGATE_ENABLE_SB9:-1}"
  local enable_wds_gaia_xmatch="${SPACEGATE_ENABLE_WDS_GAIA_XMATCH:-1}"
  local enable_sol_authority="${SPACEGATE_ENABLE_SOL_AUTHORITY:-1}"
  local enable_sol_artificial="${SPACEGATE_ENABLE_SOL_ARTIFICIAL:-1}"
  local enable_eclipsing_catalogs="${SPACEGATE_ENABLE_ECLIPSING_CATALOGS:-1}"
  local enable_kepler_eb="${SPACEGATE_ENABLE_KEPLER_EB:-0}"
  local missing=0
  if [[ "$enable_msc" == "0" ]]; then
    echo "Error: MSC is mandatory for default science ingest (SPACEGATE_ENABLE_MSC=0 is not supported)." >&2
    exit 1
  fi
  if [[ "$enable_gaia_backbone" != "1" ]]; then
    if [[ ! -f "$ATHYG_PART1" ]]; then
      echo "Missing: $ATHYG_PART1" >&2
      missing=1
    fi
    if [[ ! -f "$ATHYG_PART2" ]]; then
      echo "Missing: $ATHYG_PART2" >&2
      missing=1
    fi
  fi
  if [[ ! -f "$NASA_RAW" ]]; then
    echo "Missing: $NASA_RAW" >&2
    missing=1
  fi
  if [[ ! -f "$WDS_RAW" ]]; then
    echo "Missing: $WDS_RAW" >&2
    missing=1
  fi
  if [[ ! -f "$ORB6_RAW" ]]; then
    echo "Missing: $ORB6_RAW" >&2
    missing=1
  fi
  if [[ ! -f "$MSC_RAW" ]]; then
    echo "Missing: $MSC_RAW" >&2
    missing=1
  fi
  if [[ "$enable_gaia_backbone" == "1" && ! -f "$GAIA_BACKBONE_RAW" ]]; then
    echo "Missing: $GAIA_BACKBONE_RAW" >&2
    missing=1
  fi
  if [[ "$enable_gaia_nss" != "0" ]]; then
    if [[ ! -f "$GAIA_NSS_NON_SINGLE_RAW" ]]; then
      echo "Missing: $GAIA_NSS_NON_SINGLE_RAW" >&2
      missing=1
    fi
    if [[ ! -f "$GAIA_NSS_TWO_BODY_RAW" ]]; then
      echo "Missing: $GAIA_NSS_TWO_BODY_RAW" >&2
      missing=1
    fi
  fi
  if [[ "$enable_sbx" != "0" ]]; then
    if [[ ! -f "$SBX_SYSTEMS_RAW" ]]; then
      echo "Missing: $SBX_SYSTEMS_RAW" >&2
      missing=1
    fi
    if [[ ! -f "$SBX_ALIAS_RAW" ]]; then
      echo "Missing: $SBX_ALIAS_RAW" >&2
      missing=1
    fi
    if [[ ! -f "$SBX_CONFIG_RAW" ]]; then
      echo "Missing: $SBX_CONFIG_RAW" >&2
      missing=1
    fi
    if [[ ! -f "$SBX_ORBITS_RAW" ]]; then
      echo "Missing: $SBX_ORBITS_RAW" >&2
      missing=1
    fi
  fi
  if [[ "$enable_sb9" != "0" ]]; then
    for path in "$SB9_README_RAW" "$SB9_MAIN_RAW" "$SB9_ALIAS_RAW" "$SB9_ORBITS_RAW"; do
      if [[ ! -f "$path" ]]; then
        echo "Missing: $path" >&2
        missing=1
      fi
    done
  fi
  if [[ "$enable_wds_gaia_xmatch" == "1" ]]; then
    if [[ ! -f "$WDS_GAIA_XMATCH_RAW" ]]; then
      echo "Missing: $WDS_GAIA_XMATCH_RAW" >&2
      missing=1
    fi
  fi
  if [[ "$enable_sol_authority" != "0" && ! -f "$SOL_AUTHORITY_RAW" ]]; then
    echo "Missing: $SOL_AUTHORITY_RAW" >&2
    missing=1
  fi
  if [[ "$enable_sol_artificial" != "0" && ! -f "$SOL_ARTIFICIAL_RAW" ]]; then
    echo "Missing: $SOL_ARTIFICIAL_RAW" >&2
    missing=1
  fi
  if [[ "$enable_eclipsing_catalogs" != "0" ]]; then
    if [[ ! -f "$DEBCAT_RAW" ]]; then
      echo "Missing: $DEBCAT_RAW" >&2
      missing=1
    fi
    if [[ "$enable_kepler_eb" != "0" && ! -f "$KEPLER_EB_RAW" ]]; then
      echo "Missing: $KEPLER_EB_RAW" >&2
      missing=1
    fi
  fi
  if [[ "${SPACEGATE_ENABLE_EXOPLANET_LIFECYCLE_CATALOGS:-0}" == "1" ]]; then
    if [[ ! -f "$EXOPLANET_EU_RAW" ]]; then
      echo "Missing: $EXOPLANET_EU_RAW" >&2
      missing=1
    fi
    if [[ ! -f "$OEC_RAW" ]]; then
      echo "Missing: $OEC_RAW" >&2
      missing=1
    fi
    if [[ ! -f "$HWC_RAW" ]]; then
      echo "Missing: $HWC_RAW" >&2
      missing=1
    fi
  fi
  if [[ $missing -eq 0 && "$enable_gaia_backbone" != "1" ]]; then
    if is_lfs_pointer "$ATHYG_PART1" || is_lfs_pointer "$ATHYG_PART2"; then
      echo "AT-HYG files are Git LFS pointers. Re-run scripts/download_core.sh to fetch the real data." >&2
      exit 1
    fi
  fi
  if [[ $missing -ne 0 ]]; then
    exit 1
  fi
}

cook_gaia_backbone() {
  mkdir -p "$COOKED_GAIA_BACKBONE_DIR"

  local tmp_dir
  tmp_dir="$(mktemp -d)"

  local tmp_out="$tmp_dir/gaia_dr3_backbone.csv"

  log "Cook: Gaia backbone normalize"

  "$PYTHON_BIN" - "$GAIA_BACKBONE_RAW" "$tmp_out" <<'PY'
import sys

in_path = sys.argv[1]
out_path = sys.argv[2]

bom = b"\xef\xbb\xbf"

with open(in_path, "rb") as f, open(out_path, "wb") as out:
    first = f.read(3)
    if first != bom:
        f.seek(0)
    prev_cr = False
    while True:
        chunk = f.read(1024 * 1024)
        if not chunk:
            break
        if prev_cr:
            if chunk.startswith(b"\n"):
                chunk = chunk[1:]
            else:
                out.write(b"\n")
            prev_cr = False
        if chunk.endswith(b"\r"):
            prev_cr = True
            chunk = chunk[:-1]
        chunk = chunk.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        out.write(chunk)
    if prev_cr:
        out.write(b"\n")
PY

  mv "$tmp_out" "$COOKED_GAIA_BACKBONE"
  rm -rf "$tmp_dir"
}

cook_athyg() {
  mkdir -p "$COOKED_ATHYG_DIR"

  local tmp_dir
  tmp_dir="$(mktemp -d)"

  local tmp_concat="$tmp_dir/athyg.csv"
  local tmp_out="$tmp_dir/athyg.csv.gz"

  log "Cook: AT-HYG concat"

  local header1_file="$tmp_dir/athyg_header1.txt"
  local header2_file="$tmp_dir/athyg_header2.txt"
  "$PYTHON_BIN" - "$ATHYG_PART1" "$header1_file" <<'PY'
import gzip
import sys

in_path = sys.argv[1]
out_path = sys.argv[2]

with gzip.open(in_path, "rb") as f, open(out_path, "wb") as out:
    out.write(f.readline())
PY
  "$PYTHON_BIN" - "$ATHYG_PART2" "$header2_file" <<'PY'
import gzip
import sys

in_path = sys.argv[1]
out_path = sys.argv[2]

with gzip.open(in_path, "rb") as f, open(out_path, "wb") as out:
    out.write(f.readline())
PY

  gzip -dc "$ATHYG_PART1" > "$tmp_concat"
  if cmp -s "$header1_file" "$header2_file"; then
    gzip -dc "$ATHYG_PART2" | tail -n +2 >> "$tmp_concat"
  else
    gzip -dc "$ATHYG_PART2" >> "$tmp_concat"
  fi

  gzip -n -c "$tmp_concat" > "$tmp_out"
  mv "$tmp_out" "$COOKED_ATHYG"

  rm -rf "$tmp_dir"
}

cook_nasa() {
  mkdir -p "$COOKED_NASA_DIR"

  local tmp_dir
  tmp_dir="$(mktemp -d)"

  local tmp_out="$tmp_dir/pscomppars_clean.csv"
  local tmp_ps_out="$tmp_dir/ps_clean.csv"

  log "Cook: NASA Exoplanet Archive pscomppars normalize"

  "$PYTHON_BIN" - "$NASA_RAW" "$tmp_out" <<'PY'
import sys
in_path = sys.argv[1]
out_path = sys.argv[2]

bom = b"\xef\xbb\xbf"

with open(in_path, "rb") as f, open(out_path, "wb") as out:
    first = f.read(3)
    if first != bom:
        f.seek(0)
    prev_cr = False
    while True:
        chunk = f.read(1024 * 1024)
        if not chunk:
            break
        if prev_cr:
            if chunk.startswith(b"\n"):
                chunk = chunk[1:]
            else:
                out.write(b"\n")
            prev_cr = False
        if chunk.endswith(b"\r"):
            prev_cr = True
            chunk = chunk[:-1]
        chunk = chunk.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        out.write(chunk)
    if prev_cr:
        out.write(b"\n")
PY

  mv "$tmp_out" "$COOKED_NASA"
  if [[ -f "$NASA_PS_RAW" ]]; then
    log "Cook: NASA Exoplanet Archive ps normalize"
    "$PYTHON_BIN" - "$NASA_PS_RAW" "$tmp_ps_out" <<'PY'
import sys
in_path = sys.argv[1]
out_path = sys.argv[2]

bom = b"\xef\xbb\xbf"

with open(in_path, "rb") as f, open(out_path, "wb") as out:
    first = f.read(3)
    if first != bom:
        f.seek(0)
    prev_cr = False
    while True:
        chunk = f.read(1024 * 1024)
        if not chunk:
            break
        if prev_cr:
            if chunk.startswith(b"\n"):
                chunk = chunk[1:]
            else:
                out.write(b"\n")
            prev_cr = False
        if chunk.endswith(b"\r"):
            prev_cr = True
            chunk = chunk[:-1]
        chunk = chunk.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        out.write(chunk)
    if prev_cr:
        out.write(b"\n")
PY
    mv "$tmp_ps_out" "$COOKED_NASA_PS"
  else
    log "Cook: skip NASA Exoplanet Archive ps (optional raw file missing)"
  fi
  rm -rf "$tmp_dir"
}

cook_sol_authority() {
  mkdir -p "$COOKED_SOL_AUTHORITY_DIR"

  local tmp_dir
  tmp_dir="$(mktemp -d)"

  local tmp_out="$tmp_dir/sol_system_objects.csv"

  log "Cook: Sol authority normalize"

  "$PYTHON_BIN" - "$SOL_AUTHORITY_RAW" "$tmp_out" <<'PY'
import sys

in_path = sys.argv[1]
out_path = sys.argv[2]

bom = b"\xef\xbb\xbf"

with open(in_path, "rb") as f, open(out_path, "wb") as out:
    first = f.read(3)
    if first != bom:
        f.seek(0)
    prev_cr = False
    while True:
        chunk = f.read(1024 * 1024)
        if not chunk:
            break
        if prev_cr:
            if chunk.startswith(b"\n"):
                chunk = chunk[1:]
            else:
                out.write(b"\n")
            prev_cr = False
        if chunk.endswith(b"\r"):
            prev_cr = True
            chunk = chunk[:-1]
        chunk = chunk.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        out.write(chunk)
    if prev_cr:
        out.write(b"\n")
PY

  mv "$tmp_out" "$COOKED_SOL_AUTHORITY"
  rm -rf "$tmp_dir"
}

cook_sol_artificial() {
  mkdir -p "$COOKED_SOL_ARTIFICIAL_DIR"

  local tmp_dir
  tmp_dir="$(mktemp -d)"

  local tmp_out="$tmp_dir/sol_artificial_objects.csv"

  log "Cook: Sol artificial normalize"

  "$PYTHON_BIN" - "$SOL_ARTIFICIAL_RAW" "$tmp_out" <<'PY'
import sys

in_path = sys.argv[1]
out_path = sys.argv[2]

bom = b"\xef\xbb\xbf"

with open(in_path, "rb") as f, open(out_path, "wb") as out:
    first = f.read(3)
    if first != bom:
        f.seek(0)
    prev_cr = False
    while True:
        chunk = f.read(1024 * 1024)
        if not chunk:
            break
        if prev_cr:
            if chunk.startswith(b"\n"):
                chunk = chunk[1:]
            else:
                out.write(b"\n")
            prev_cr = False
        if chunk.endswith(b"\r"):
            prev_cr = True
            chunk = chunk[:-1]
        chunk = chunk.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        out.write(chunk)
    if prev_cr:
        out.write(b"\n")
PY

  mv "$tmp_out" "$COOKED_SOL_ARTIFICIAL"
  rm -rf "$tmp_dir"
}

main() {
  require_command gzip
  if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
    PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
  else
    echo "Error: required command 'python3' (or 'python') not found." >&2
    exit 1
  fi

  mkdir -p "$COOKED_DIR" "$LOG_DIR"

  ensure_inputs

  log "Cook core begin"
  if [[ "${SPACEGATE_ENABLE_GAIA_BACKBONE:-0}" == "1" ]]; then
    cook_gaia_backbone
    if [[ -f "$ATHYG_PART1" && -f "$ATHYG_PART2" ]]; then
      cook_athyg
    else
      log "Cook: skip AT-HYG (Gaia backbone mode active and AT-HYG inputs absent)"
    fi
  else
    cook_athyg
  fi
  cook_nasa
  if [[ "${SPACEGATE_ENABLE_SOL_AUTHORITY:-1}" != "0" ]]; then
    cook_sol_authority
  else
    log "Cook: skip Sol authority (SPACEGATE_ENABLE_SOL_AUTHORITY=0)"
  fi
  if [[ "${SPACEGATE_ENABLE_SOL_ARTIFICIAL:-1}" != "0" ]]; then
    cook_sol_artificial
  else
    log "Cook: skip Sol artificial (SPACEGATE_ENABLE_SOL_ARTIFICIAL=0)"
  fi
  log "Cook: multiplicity catalogs (WDS/MSC/ORB6/Gaia NSS/SBX[/WDS-Gaia XMatch])"
  "$PYTHON_BIN" "$ROOT_DIR/scripts/cook_multiplicity.py"
  log "Cook: compact/superstellar/eclipsing support catalogs"
  "$PYTHON_BIN" "$ROOT_DIR/scripts/cook_science_catalogs.py"
  if [[ "${SPACEGATE_ENABLE_EXOPLANET_LIFECYCLE_CATALOGS:-0}" == "1" ]]; then
    log "Cook: exoplanet lifecycle support catalogs"
    "$PYTHON_BIN" "$ROOT_DIR/scripts/cook_exoplanet_lifecycle.py"
  else
    log "Cook: skip exoplanet lifecycle support catalogs (SPACEGATE_ENABLE_EXOPLANET_LIFECYCLE_CATALOGS=0)"
  fi
  if ! "$PYTHON_BIN" "$ROOT_DIR/scripts/update_catalog_pipeline_report.py" --stage cook >/dev/null 2>&1; then
    log "Warning: failed to update catalog pipeline report (cook stage)."
  fi
  log "Cook core complete"
  echo "Next: scripts/ingest_core.sh to build the core database."
}

main "$@"
