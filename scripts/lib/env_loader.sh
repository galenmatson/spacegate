#!/usr/bin/env bash
# shellcheck shell=bash

# Load dotenv-style files without overriding variables already present
# in the current environment (CLI/env prefix always wins).
spacegate_load_env_defaults() {
  local root_dir="$1"
  local -a env_files=()
  local file line key value

  if [[ -n "${SPACEGATE_ENV_FILE:-}" ]]; then
    env_files+=("$SPACEGATE_ENV_FILE")
  fi
  env_files+=(
    "$root_dir/.spacegate.env"
    "$root_dir/.spacegate.local.env"
    "/etc/spacegate/spacegate.env"
  )

  for file in "${env_files[@]}"; do
    [[ -f "$file" ]] || continue
    while IFS= read -r line || [[ -n "$line" ]]; do
      # trim leading/trailing whitespace
      line="${line#"${line%%[![:space:]]*}"}"
      line="${line%"${line##*[![:space:]]}"}"
      [[ -z "$line" || "$line" == \#* ]] && continue

      line="${line#export }"
      [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
      key="${line%%=*}"
      value="${line#*=}"

      # Keep explicit env values.
      [[ -n "${!key+x}" ]] && continue

      # For unquoted values, allow inline comments.
      if [[ "$value" != \"*\" && "$value" != \'*\' ]]; then
        value="${value%%#*}"
        value="${value%"${value##*[![:space:]]}"}"
      fi

      # Remove wrapping quotes for common dotenv forms.
      if [[ "$value" =~ ^\".*\"$ ]]; then
        value="${value:1:${#value}-2}"
      elif [[ "$value" =~ ^\'.*\'$ ]]; then
        value="${value:1:${#value}-2}"
      fi

      export "$key=$value"
    done < "$file"
  done
}
