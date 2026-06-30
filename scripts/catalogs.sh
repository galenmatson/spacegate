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
LOG_DIR="${SPACEGATE_LOG_DIR:-$STATE_DIR/logs}"
MANIFEST_DIR="$STATE_DIR/reports/manifests"
CONFIG_ENV="$ROOT_DIR/configs/catalog_urls.env"

mkdir -p "$RAW_DIR" "$LOG_DIR" "$MANIFEST_DIR"

if [[ -f "$CONFIG_ENV" ]]; then
  # shellcheck disable=SC1090
  source "$CONFIG_ENV"
fi

ATHYG_BASE_URL_DEFAULT="https://codeberg.org/astronexus/athyg/raw/branch/main/data"
ATHYG_PART1_URL="${ATHYG_PART1_URL:-$ATHYG_BASE_URL_DEFAULT/athyg_v33-1.csv.gz}"
ATHYG_PART2_URL="${ATHYG_PART2_URL:-$ATHYG_BASE_URL_DEFAULT/athyg_v33-2.csv.gz}"
PUBLIC_BASE_URL="${SPACEGATE_PUBLIC_BASE_URL:-https://spacegates.org}"
PUBLIC_BASE_URL="${PUBLIC_BASE_URL%/}"

NASA_EXOPLANET_URL="${NASA_EXOPLANET_URL:-https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query=select+*+from+pscomppars&format=csv}"
NASA_EXOPLANET_PS_URL="${NASA_EXOPLANET_PS_URL:-https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query=select+*+from+ps&format=csv}"
EXOPLANET_EU_URL="${EXOPLANET_EU_URL:-https://www.exoplanet.eu/catalog/csv/}"
OPEN_EXOPLANET_CATALOGUE_URL="${OPEN_EXOPLANET_CATALOGUE_URL:-https://codeload.github.com/OpenExoplanetCatalogue/open_exoplanet_catalogue/tar.gz/refs/heads/master}"
HWC_URL="${HWC_URL:-https://www.hpcf.upr.edu/~abel/phl/hwc/data/hwc.csv}"

WDS_URL="${WDS_URL:-https://astro.gsu.edu/wds/wdsweb_summ2.txt}"
MSC_UPSTREAM_URL="${MSC_UPSTREAM_URL:-https://www.ctio.noirlab.edu/~atokovin/stars/newmsc-20260619.tar.gz}"
MSC_MIRROR_URL="${SPACEGATE_MSC_MIRROR_URL:-${PUBLIC_BASE_URL}/dl/catalogs/current/raw/msc/newmsc-20260619.tar.gz}"
MSC_URL="${MSC_URL:-${SPACEGATE_MSC_MIRROR_URL:-$MSC_UPSTREAM_URL}}"
MSC_HTTP_URL="${MSC_HTTP_URL:-http://www.ctio.noirlab.edu/~atokovin/stars/newmsc-20260619.tar.gz}"
ORB6_URL="${ORB6_URL:-https://crf.usno.navy.mil/data_products/WDS/orb6/orb6orbits.sql}"
DEBCAT_URL="${DEBCAT_URL:-https://www.astro.keele.ac.uk/jkt/debcat/debs.dat}"
CLUSTERS_URL="${CLUSTERS_URL:-https://cdsarc.cds.unistra.fr/ftp/J/A+A/640/A1/table1.dat}"
CLUSTERS_MEMBERS_URL="${CLUSTERS_MEMBERS_URL:-https://cdsarc.cds.unistra.fr/ftp/J/A+A/640/A1/nodup.dat.gz}"
VSX_URL="${VSX_URL:-https://cdsarc.cds.unistra.fr/ftp/B/vsx/vsx.dat}"
SNR_URL="${SNR_URL:-https://www.mrao.cam.ac.uk/surveys/snrs/snrs.data.html}"
ATNF_URL="${ATNF_URL:-https://www.atnf.csiro.au/research/pulsar/psrcat/downloads/psrcat_pkg.tar.gz}"
MAGNETAR_URL="${MAGNETAR_URL:-https://www.physics.mcgill.ca/~pulsar/magnetar/TabO1.csv}"
ULTRACOOLSHEET_URL="${ULTRACOOLSHEET_URL:-https://docs.google.com/spreadsheets/d/1i98ft8g5mzPp2DNno0kcz4B9nzMxdpyz5UquAVhz-U8/gviz/tq?tqx=out:csv&sheet=Main}"
GAIA_UCD_URL="${GAIA_UCD_URL:-https://cdsarc.cds.unistra.fr/ftp/J/A+A/669/A139/table4.dat}"
WHITE_DWARF_URL="${WHITE_DWARF_URL:-https://warwick.ac.uk/fac/sci/physics/research/astro/research/catalogues/gaiaedr3_wd_main.fits.gz}"
DWARFARCHIVES_URL="${DWARFARCHIVES_URL:-http://dwarfarchives.org/}"
CATWISE_BASE_URL="${CATWISE_BASE_URL:-https://irsa.ipac.caltech.edu/data/WISE/CatWISE/2020/catwise_2020.html}"

LOG_FILE="$LOG_DIR/catalogs.log"

log() {
  local msg="$1"
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  printf '%s %s\n' "$ts" "$msg" | tee -a "$LOG_FILE"
}

usage() {
  cat <<'USAGE'
Usage:
  scripts/catalogs.sh [--core] [--all] [--catalog <name>] [--list] [--non-interactive] [--overwrite]

Examples:
  scripts/catalogs.sh --core
  scripts/catalogs.sh --catalog athyg --catalog nasa_exoplanet_archive
  scripts/catalogs.sh --all

Notes:
  - For CatWISE full tiles, provide a URL list file via CATWISE_TILES_LIST
    or configs/catwise_full_tiles.txt (one URL per line).
  - Set SPACEGATE_ENABLE_EXOPLANET_LIFECYCLE_CATALOGS=1 to include
    exoplanet.eu, OEC, and HWC fetch targets in --core mode.
  - DwarfArchives download requires SPACEGATE_ENABLE_DWARFARCHIVES=1.
  - MSC insecure HTTP fallback is opt-in: SPACEGATE_MSC_ALLOW_INSECURE_HTTP=1.
    Integrity pin is required in HTTP mode: SPACEGATE_MSC_SHA256=<known sha256>.
  - Override any catalog URL via configs/catalog_urls.env.
  - --overwrite skips prompts and replaces existing files.
USAGE
}

require_command() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: required command '$cmd' not found." >&2
    exit 1
  fi
}

json_escape() {
  local s="$1"
  s=${s//\\/\\\\}
  s=${s//"/\\"}
  s=${s//$'\n'/\\n}
  printf '%s' "$s"
}

sha256_file() {
  local path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
  else
    echo "" >&2
    echo "Error: sha256sum or shasum not found." >&2
    exit 1
  fi
}

stat_bytes() {
  local path="$1"
  if stat -c %s "$path" >/dev/null 2>&1; then
    stat -c %s "$path"
  else
    stat -f %z "$path"
  fi
}

get_content_length() {
  local url="$1"
  local length
  length="$(curl -sIL "$url" | awk -F': ' 'tolower($1) == "content-length" {print $2}' | tr -d '\r' | tail -n 1)"
  if [[ "$length" =~ ^[0-9]+$ ]]; then
    printf '%s' "$length"
  else
    printf ''
  fi
}

is_lfs_pointer() {
  local path="$1"
  [[ -f "$path" ]] || return 1
  head -c 256 "$path" 2>/dev/null | grep -aq "version https://git-lfs.github.com/spec/v1"
}

parse_lfs_oid() {
  local path="$1"
  awk '/^oid sha256:/ {print $2}' "$path" | awk -F: '{print $2}'
}

parse_lfs_size() {
  local path="$1"
  awk '/^size / {print $2}' "$path"
}

lfs_url_from_source() {
  local source_url="$1"
  local oid="$2"
  if [[ "$source_url" =~ ^https://codeberg.org/([^/]+)/([^/]+)/raw/ ]]; then
    printf 'https://codeberg.org/%s/%s.git/info/lfs/objects/%s' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "$oid"
    return 0
  fi
  return 1
}

catalog_titles() {
  cat <<'LIST'
core|Core (AT-HYG + NASA Exoplanets + WDS/MSC/ORB6 support; Gaia NSS fetched by download_core.sh)
athyg|AT-HYG stellar catalog
nasa_exoplanet_archive|NASA Exoplanet Archive (pscomppars + ps)
exoplanet_eu|Exoplanet.eu catalog export (status layer)
open_exoplanet_catalogue|Open Exoplanet Catalogue (tarball)
hwc|Habitable Worlds Catalog (full CSV)
wds|Washington Double Star Catalog (WDS)
msc|Multiple Star Catalog (MSC)
orb6|Sixth Catalog of Orbits of Visual Binary Stars (ORB6)
debcat|DEBCat detached eclipsing binaries
clusters|Gaia DR2 clusters (Cantat-Gaudin 2020)
vsx|AAVSO Variable Star Index (VSX)
snr|Green's Galactic SNRs
atnf|ATNF pulsar catalog
magnetar|McGill magnetar catalog
ultracoolsheet|UltracoolSheet (UCDs)
gaia_ucd|Gaia DR3 ultracool dwarf sample
white_dwarf|Gaia EDR3 white dwarf catalog
dwarfarchives|DwarfArchives.org (conditional)
catwise_full|CatWISE2020 full tiles (very large)
LIST
}

catalog_manifest() {
  local catalog="$1"
  case "$catalog" in
    athyg|nasa_exoplanet_archive)
      printf '%s' "$MANIFEST_DIR/core_manifest.json"
      ;;
    *)
      printf '%s' "$MANIFEST_DIR/${catalog}_manifest.json"
      ;;
  esac
}

catalog_label() {
  local catalog="$1"
  local label
  label="$(catalog_titles | awk -F'|' -v key="$catalog" '$1 == key {print $2}')"
  if [[ -n "$label" ]]; then
    printf '%s' "$label"
  else
    printf '%s' "$catalog"
  fi
}

catalog_sources() {
  local catalog="$1"
  case "$catalog" in
    athyg)
      printf '%s\n' "athyg|athyg_v33-1|$ATHYG_PART1_URL|raw/athyg/athyg_v33-1.csv.gz"
      printf '%s\n' "athyg|athyg_v33-2|$ATHYG_PART2_URL|raw/athyg/athyg_v33-2.csv.gz"
      ;;
    nasa_exoplanet_archive)
      printf '%s\n' "nasa_exoplanet_archive|pscomppars|$NASA_EXOPLANET_URL|raw/nasa_exoplanet_archive/pscomppars.csv"
      printf '%s\n' "nasa_exoplanet_archive|ps|$NASA_EXOPLANET_PS_URL|raw/nasa_exoplanet_archive/ps.csv"
      ;;
    exoplanet_eu)
      printf '%s\n' "exoplanet_eu|catalog_csv|$EXOPLANET_EU_URL|raw/exoplanet_eu/catalog.csv"
      ;;
    open_exoplanet_catalogue)
      printf '%s\n' "open_exoplanet_catalogue|catalog_tarball|$OPEN_EXOPLANET_CATALOGUE_URL|raw/open_exoplanet_catalogue/open_exoplanet_catalogue.tar.gz"
      ;;
    hwc)
      printf '%s\n' "hwc|hwc_full_csv|$HWC_URL|raw/hwc/hwc.csv"
      ;;
    wds)
      printf '%s\n' "wds|wdsweb_summ2|$WDS_URL|raw/wds/wdsweb_summ2.txt"
      ;;
    msc)
      printf '%s\n' "msc|newmsc_20260619|$MSC_URL|raw/msc/newmsc-20260619.tar.gz"
      ;;
    orb6)
      printf '%s\n' "orb6|orb6orbits|$ORB6_URL|raw/orb6/orb6orbits.sql"
      ;;
    debcat)
      printf '%s\n' "debcat|debs_dat|$DEBCAT_URL|raw/debcat/debs.dat"
      ;;
    clusters)
      printf '%s\n' "clusters|cantat_gaudin_2020_table1|$CLUSTERS_URL|raw/clusters/table1.dat"
      printf '%s\n' "clusters|cantat_gaudin_2020_members|$CLUSTERS_MEMBERS_URL|raw/clusters/nodup.dat.gz"
      ;;
    vsx)
      printf '%s\n' "vsx|vsx_dat|$VSX_URL|raw/vsx/vsx.dat"
      ;;
    snr)
      printf '%s\n' "snr|snrs_data_html|$SNR_URL|raw/snr/snrs.data.html"
      ;;
    atnf)
      printf '%s\n' "atnf|psrcat_pkg|$ATNF_URL|raw/atnf/psrcat_pkg.tar.gz"
      ;;
    magnetar)
      printf '%s\n' "magnetar|TabO1|$MAGNETAR_URL|raw/magnetar/TabO1.csv"
      ;;
    ultracoolsheet)
      printf '%s\n' "ultracoolsheet|UltracoolSheet_Main|$ULTRACOOLSHEET_URL|raw/ultracoolsheet/ultracoolsheet_main.csv"
      ;;
    gaia_ucd)
      printf '%s\n' "gaia_ucd|table4|$GAIA_UCD_URL|raw/gaia_ucd/table4.dat"
      ;;
    white_dwarf)
      printf '%s\n' "white_dwarf|gaiaedr3_wd_main|$WHITE_DWARF_URL|raw/white_dwarf/gaiaedr3_wd_main.fits.gz"
      ;;
    dwarfarchives)
      if [[ "${SPACEGATE_ENABLE_DWARFARCHIVES:-}" != "1" ]]; then
        log "Skip dwarfarchives: set SPACEGATE_ENABLE_DWARFARCHIVES=1 to enable."
        return 0
      fi
      printf '%s\n' "dwarfarchives|dwarfarchives_data|$DWARFARCHIVES_URL|raw/dwarfarchives/dwarfarchives.data"
      ;;
    catwise_full)
      local list_file
      list_file="${CATWISE_TILES_LIST:-$ROOT_DIR/configs/catwise_full_tiles.txt}"
      if [[ ! -f "$list_file" ]]; then
        echo "Error: CatWISE list file not found at $list_file" >&2
        echo "Provide CATWISE_TILES_LIST or create configs/catwise_full_tiles.txt" >&2
        exit 1
      fi
      while IFS= read -r url; do
        [[ -z "$url" ]] && continue
        [[ "$url" =~ ^# ]] && continue
        local filename
        filename="$(basename "$url")"
        local tile_dir
        tile_dir="$(basename "$(dirname "$url")")"
        local dest
        if [[ -n "$tile_dir" && "$tile_dir" != "/" ]]; then
          dest="raw/catwise_full/$tile_dir/$filename"
        else
          dest="raw/catwise_full/$filename"
        fi
        printf '%s\n' "catwise_full|$filename|$url|$dest"
      done < "$list_file"
      ;;
    *)
      echo "Error: unknown catalog '$catalog'" >&2
      exit 1
      ;;
  esac
}

dedupe_catalogs() {
  local -a items=("$@")
  local -A seen=()
  local -a out=()
  local item
  for item in "${items[@]}"; do
    if [[ -n "${item}" && -z "${seen[$item]:-}" ]]; then
      seen[$item]=1
      out+=("$item")
    fi
  done
  printf '%s\n' "${out[@]}"
}

interactive_select() {
  local -a menu=()
  while IFS='|' read -r key label; do
    menu+=("$key|$label")
  done < <(catalog_titles)

  echo "Select catalogs to download or update:"
  local i=1
  local -a keys=()
  for entry in "${menu[@]}"; do
    local key=${entry%%|*}
    local label=${entry#*|}
    printf '  %2d) %s\n' "$i" "$label"
    keys+=("$key")
    i=$((i + 1))
  done
  echo "  a) all"

  local selection
  read -r -p "Enter selections (e.g., 1 3 5 or a): " selection
  if [[ -z "$selection" ]]; then
    echo "No selection provided." >&2
    exit 1
  fi

  if [[ "$selection" =~ ^[aA]$ ]]; then
    printf '%s\n' "${keys[@]}"
    return 0
  fi

  local -a chosen=()
  for token in $selection; do
    if [[ "$token" =~ ^[0-9]+$ ]]; then
      local idx=$((token - 1))
      if [[ $idx -ge 0 && $idx -lt ${#keys[@]} ]]; then
        chosen+=("${keys[$idx]}")
      else
        echo "Invalid selection: $token" >&2
        exit 1
      fi
    else
      echo "Invalid selection token: $token" >&2
      exit 1
    fi
  done

  printf '%s\n' "${chosen[@]}"
}

expand_catalogs() {
  local -a input=("$@")
  local -a expanded=()
  local item
  for item in "${input[@]}"; do
    case "$item" in
      core)
        expanded+=("athyg" "nasa_exoplanet_archive" "wds" "msc" "orb6" "debcat")
        if [[ "${SPACEGATE_ENABLE_EXOPLANET_LIFECYCLE_CATALOGS:-0}" == "1" ]]; then
          expanded+=("exoplanet_eu" "open_exoplanet_catalogue" "hwc")
        fi
        ;;
      *)
        expanded+=("$item")
        ;;
    esac
  done
  dedupe_catalogs "${expanded[@]}"
}

main() {
  local -a selected=()
  local non_interactive=0
  local overwrite_all=0

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --list)
        catalog_titles
        exit 0
        ;;
      --catalog)
        selected+=("$2")
        shift 2
        ;;
      --core)
        selected+=("core")
        shift 1
        ;;
      --all)
        selected+=("all")
        shift 1
        ;;
      --non-interactive)
        non_interactive=1
        shift 1
        ;;
      --overwrite)
        overwrite_all=1
        shift 1
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "Unknown argument: $1" >&2
        usage
        exit 1
        ;;
    esac
  done

  if [[ ${#selected[@]} -eq 0 && $non_interactive -eq 0 ]]; then
    mapfile -t selected < <(interactive_select)
  fi

  if [[ ${#selected[@]} -eq 0 ]]; then
    echo "No catalogs selected." >&2
    exit 1
  fi

  if printf '%s\n' "${selected[@]}" | grep -qi '^all$'; then
    mapfile -t selected < <(catalog_titles | awk -F'|' '{print $1}')
  fi

  mapfile -t selected < <(expand_catalogs "${selected[@]}")

  require_command aria2c
  require_command curl

  log "Catalog download begin: ${selected[*]}"

  local tmp_input
  tmp_input="$(mktemp)"
  local -a sources=()
  local -A skip_dest=()
  local -A resolved_url_by_dest=()
  local catalog

  for catalog in "${selected[@]}"; do
    local -a cat_sources=()
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      cat_sources+=("$line")
      sources+=("$line")
    done < <(catalog_sources "$catalog")
    if [[ ${#cat_sources[@]} -gt 0 ]]; then
      log "Queue: $(catalog_label "$catalog") (${#cat_sources[@]} file(s))"
    fi
  done

  if [[ ${#sources[@]} -eq 0 ]]; then
    log "No sources to download."
    rm -f "$tmp_input"
    exit 0
  fi

  local entry
  for entry in "${sources[@]}"; do
    local source_name rest url dest
    rest="${entry#*|}"
    source_name="${rest%%|*}"
    url="${entry#*|*|}"
    url="${url%%|*}"
    dest="${entry##*|}"

    if [[ "$source_name" == "newmsc_20260619" ]]; then
      if [[ "$url" =~ ^http:// ]]; then
        if [[ "${SPACEGATE_MSC_ALLOW_INSECURE_HTTP:-0}" != "1" ]]; then
          echo "Error: MSC URL is insecure HTTP but SPACEGATE_MSC_ALLOW_INSECURE_HTTP is not enabled." >&2
          echo "Set SPACEGATE_MSC_ALLOW_INSECURE_HTTP=1 and SPACEGATE_MSC_SHA256=<expected_sha256> to proceed." >&2
          exit 1
        fi
        if [[ -z "${SPACEGATE_MSC_SHA256:-}" ]]; then
          echo "Error: MSC insecure HTTP mode requires SPACEGATE_MSC_SHA256." >&2
          exit 1
        fi
      elif [[ "${SPACEGATE_MSC_ALLOW_INSECURE_HTTP:-0}" == "1" ]]; then
        local tls_check_timeout
        tls_check_timeout="${SPACEGATE_MSC_TLS_CHECK_TIMEOUT_S:-15}"
        if [[ "${SPACEGATE_MSC_FORCE_HTTP:-0}" == "1" ]]; then
          if [[ "$MSC_HTTP_URL" != http://* ]]; then
            echo "Error: MSC_HTTP_URL must use http:// when SPACEGATE_MSC_FORCE_HTTP=1." >&2
            exit 1
          fi
          if [[ -z "${SPACEGATE_MSC_SHA256:-}" ]]; then
            echo "Error: SPACEGATE_MSC_SHA256 is required when forcing MSC HTTP mode." >&2
            exit 1
          fi
          url="$MSC_HTTP_URL"
          log "Warning: forcing MSC download over insecure HTTP (SPACEGATE_MSC_FORCE_HTTP=1)."
        elif ! curl -fsSI --max-time "$tls_check_timeout" "$url" >/dev/null 2>&1; then
          if [[ "$MSC_HTTP_URL" != http://* ]]; then
            echo "Error: MSC_HTTP_URL must use http:// for insecure fallback." >&2
            exit 1
          fi
          if [[ -z "${SPACEGATE_MSC_SHA256:-}" ]]; then
            echo "Error: SPACEGATE_MSC_SHA256 is required for MSC HTTPS->HTTP fallback mode." >&2
            exit 1
          fi
          url="$MSC_HTTP_URL"
          log "Warning: MSC HTTPS preflight failed; falling back to insecure HTTP."
        fi
      fi
    fi
    resolved_url_by_dest["$dest"]="$url"

    local dest_abs="$STATE_DIR/$dest"
    local dest_dir
    dest_dir="$(dirname "$dest_abs")"
    mkdir -p "$dest_dir"

    if [[ -f "$dest_abs" ]] && is_lfs_pointer "$dest_abs"; then
      log "Remove LFS pointer before download: $dest"
      rm -f "$dest_abs"
    elif [[ -f "$dest_abs" ]]; then
      if [[ $overwrite_all -eq 1 ]]; then
        log "Overwrite existing file: $dest"
        rm -f "$dest_abs"
      elif [[ $non_interactive -eq 1 ]]; then
        log "Skip existing file (use --overwrite to replace): $dest"
        skip_dest["$dest"]=1
        continue
      else
        local answer
        read -r -p "File exists: $dest. Overwrite? (Y/n): " answer
        if [[ -z "$answer" || "$answer" =~ ^[Yy]$ ]]; then
          log "Overwrite existing file: $dest"
          rm -f "$dest_abs"
        else
          log "Skip existing file: $dest"
          skip_dest["$dest"]=1
          continue
        fi
      fi
    fi

    printf '%s\n  dir=%s\n  out=%s\n' "$url" "$dest_dir" "$(basename "$dest_abs")" >> "$tmp_input"
  done

  aria2c \
    --continue=true \
    --max-connection-per-server=8 \
    --split=8 \
    --min-split-size=1M \
    --file-allocation=none \
    --conditional-get=true \
    --auto-file-renaming=false \
    --allow-overwrite=true \
    -i "$tmp_input"

  rm -f "$tmp_input"

  local -A manifest_temp=()
  local size_ok=1
  local total_bytes=0
  local total_files=${#sources[@]}

  for entry in "${sources[@]}"; do
    local catalog_name source_name url dest
    catalog_name="${entry%%|*}"
    local rest="${entry#*|}"
    source_name="${rest%%|*}"
    rest="${rest#*|}"
    url="${rest%%|*}"
    dest="${entry##*|}"
    if [[ -n "${resolved_url_by_dest[$dest]:-}" ]]; then
      url="${resolved_url_by_dest[$dest]}"
    fi

    local dest_abs="$STATE_DIR/$dest"
    if [[ ! -f "$dest_abs" ]]; then
      log "Error: missing downloaded file $dest"
      size_ok=0
      continue
    fi

    local expected_bytes=""
    if [[ -z "${skip_dest[$dest]:-}" ]] && is_lfs_pointer "$dest_abs"; then
      local oid size
      oid="$(parse_lfs_oid "$dest_abs")"
      size="$(parse_lfs_size "$dest_abs")"
      if [[ -z "$oid" ]]; then
        log "Error: failed to parse LFS oid for $dest"
        size_ok=0
        continue
      fi
      local lfs_url
      lfs_url="$(lfs_url_from_source "$url" "$oid" || true)"
      if [[ -z "$lfs_url" ]]; then
        log "Error: unable to resolve LFS URL for $dest"
        size_ok=0
        continue
      fi
      log "Resolve LFS pointer for $dest"
      rm -f "$dest_abs"
      if ! curl -L --fail --retry 3 --retry-delay 2 "$lfs_url" -o "$dest_abs"; then
        log "Error: LFS download failed for $dest"
        size_ok=0
        continue
      fi
      url="$lfs_url"
      expected_bytes="$size"
    elif [[ -z "${skip_dest[$dest]:-}" ]]; then
      expected_bytes="$(get_content_length "$url")"
    fi

    if is_lfs_pointer "$dest_abs"; then
      log "Error: LFS pointer still present for $dest (download incomplete)"
      size_ok=0
      continue
    fi

    local bytes_written
    bytes_written="$(stat_bytes "$dest_abs")"
    total_bytes=$((total_bytes + bytes_written))

    if [[ -n "$expected_bytes" && "$expected_bytes" != "$bytes_written" ]]; then
      log "Error: size mismatch for $dest (expected $expected_bytes, got $bytes_written)"
      size_ok=0
    fi

    local sha
    sha="$(sha256_file "$dest_abs")"
    if [[ "$source_name" == "newmsc_20260619" && "$url" =~ ^http:// ]]; then
      if [[ "${SPACEGATE_MSC_ALLOW_INSECURE_HTTP:-0}" != "1" ]]; then
        log "Error: MSC was downloaded over insecure HTTP without explicit opt-in."
        size_ok=0
      elif [[ -z "${SPACEGATE_MSC_SHA256:-}" ]]; then
        log "Error: MSC insecure HTTP mode requires SPACEGATE_MSC_SHA256."
        size_ok=0
      else
        local expected_sha actual_sha
        expected_sha="$(printf '%s' "${SPACEGATE_MSC_SHA256}" | tr '[:upper:]' '[:lower:]')"
        actual_sha="$(printf '%s' "$sha" | tr '[:upper:]' '[:lower:]')"
        if [[ "$expected_sha" != "$actual_sha" ]]; then
          log "Error: MSC SHA256 mismatch for insecure HTTP download (expected $expected_sha, got $actual_sha)."
          size_ok=0
        else
          log "MSC insecure HTTP download verified against SPACEGATE_MSC_SHA256."
        fi
      fi
    fi

    local ts
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

    local manifest
    manifest="$(catalog_manifest "$catalog_name")"
    local temp
    temp="${manifest_temp[$manifest]:-}"
    if [[ -z "$temp" ]]; then
      temp="$(mktemp)"
      manifest_temp[$manifest]="$temp"
    fi

    local entry_json
    entry_json=$(printf '{"source_name":"%s","url":"%s","dest_path":"%s","retrieved_at":"%s","checked_at":"%s","sha256":"%s","bytes_written":%s}' \
      "$(json_escape "$source_name")" \
      "$(json_escape "$url")" \
      "$(json_escape "$dest")" \
      "$ts" \
      "$ts" \
      "$sha" \
      "$bytes_written")

    printf '%s\n' "$entry_json" >> "$temp"
  done

  local manifest
  for manifest in "${!manifest_temp[@]}"; do
    local temp
    temp="${manifest_temp[$manifest]}"
    mkdir -p "$(dirname "$manifest")"
    {
      echo "["
      awk 'NR>1{print ","} {print $0}' "$temp"
      echo "]"
    } > "$manifest"
    rm -f "$temp"
  done

  if [[ $size_ok -ne 1 ]]; then
    log "Catalog download completed with errors."
    exit 1
  fi

  log "Catalog download complete. Total files: $total_files. Total bytes: $total_bytes"
}

main "$@"
