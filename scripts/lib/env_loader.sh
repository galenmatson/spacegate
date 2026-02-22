#!/usr/bin/env bash
# shellcheck shell=bash

spacegate_trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

spacegate_load_env_defaults() {
  local root_dir="$1"
  local -a env_files=()
  local file line key value
  local -A process_keys=()
  local -A loaded_keys=()

  # Low -> high precedence (later files override earlier file values).
  # Existing process env always wins over file values.
  env_files+=(
    "/etc/spacegate/spacegate.env"
    "$root_dir/.spacegate.env"
    "$root_dir/.spacegate.local.env"
  )
  if [[ -n "${SPACEGATE_ENV_FILE:-}" ]]; then
    env_files+=("$SPACEGATE_ENV_FILE")
  fi

  for file in "${env_files[@]}"; do
    [[ -f "$file" ]] || continue
    while IFS= read -r line || [[ -n "$line" ]]; do
      line="$(spacegate_trim "$line")"
      [[ -z "$line" || "$line" == \#* ]] && continue

      if [[ "$line" =~ ^export[[:space:]]+ ]]; then
        line="${line#export}"
        line="$(spacegate_trim "$line")"
      fi

      if [[ ! "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*=(.*)$ ]]; then
        continue
      fi
      key="${BASH_REMATCH[1]}"
      value="${BASH_REMATCH[2]}"

      # Snapshot process-provided values; do not override them.
      if [[ -z "${process_keys[$key]+x}" && -z "${loaded_keys[$key]+x}" && -n "${!key+x}" ]]; then
        process_keys["$key"]=1
      fi
      if [[ -n "${process_keys[$key]+x}" ]]; then
        continue
      fi

      value="$(spacegate_trim "$value")"

      # For unquoted values, allow inline comments.
      if [[ "$value" != \"*\" && "$value" != \'*\' ]]; then
        value="${value%%#*}"
        value="$(spacegate_trim "$value")"
      fi

      # Remove wrapping quotes for common dotenv forms.
      if [[ "$value" =~ ^\".*\"$ ]]; then
        value="${value:1:${#value}-2}"
      elif [[ "$value" =~ ^\'.*\'$ ]]; then
        value="${value:1:${#value}-2}"
      fi

      export "$key=$value"
      loaded_keys["$key"]=1
    done < "$file"
  done
}

spacegate_normalize_env_paths() {
  local root_dir="$1"
  local state_dir="${SPACEGATE_STATE_DIR:-}"
  local data_dir="${SPACEGATE_DATA_DIR:-}"

  if [[ -z "$state_dir" && -n "$data_dir" ]]; then
    state_dir="$data_dir"
  fi
  if [[ -z "$state_dir" ]]; then
    state_dir="$root_dir/data"
  fi

  if [[ -z "$data_dir" ]]; then
    data_dir="$state_dir"
  fi

  export SPACEGATE_STATE_DIR="$state_dir"
  export SPACEGATE_DATA_DIR="$data_dir"

  if [[ -z "${SPACEGATE_CACHE_DIR:-}" ]]; then
    export SPACEGATE_CACHE_DIR="$SPACEGATE_STATE_DIR/cache"
  fi
  if [[ -z "${SPACEGATE_LOG_DIR:-}" ]]; then
    export SPACEGATE_LOG_DIR="$SPACEGATE_STATE_DIR/logs"
  fi
  if [[ -z "${SPACEGATE_CONFIG_DIR:-}" ]]; then
    export SPACEGATE_CONFIG_DIR="$root_dir/configs"
  fi
}

spacegate_init_env() {
  local root_dir="$1"
  spacegate_load_env_defaults "$root_dir"
  spacegate_normalize_env_paths "$root_dir"
}
