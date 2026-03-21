#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi

SAMPLE_SIZE="${SPACEGATE_CATALOG_EVAL_SAMPLE_SIZE:-100}"
SEED="${SPACEGATE_CATALOG_EVAL_SEED:-spacegate-v1-2}"

log() {
  local msg="$1"
  printf '%s %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$msg"
}

duration_s() {
  local start="$1"
  local end="$2"
  echo $((end - start))
}

main() {
  local run_start run_end step_start step_end report_dir
  run_start="$(date +%s)"

  log "Catalog contribution evaluation begin (sample_size=$SAMPLE_SIZE seed=$SEED)"

  step_start="$(date +%s)"
  log "Step 1/2: fetching deterministic evaluation samples (includes SBX sample)"
  "$ROOT_DIR/scripts/fetch_catalog_samples.sh" \
    --sample-size "$SAMPLE_SIZE" \
    --seed "$SEED" \
    --catalog gaia_dr3_sample \
    --catalog gaia_dr3_non_single_sample \
    --catalog gaia_dr3_nss_two_body_sample \
    --catalog wds \
    --catalog msc \
    --catalog orb6 \
    --catalog sbx_sample
  step_end="$(date +%s)"
  log "Step 1/2 complete ($(duration_s "$step_start" "$step_end")s)"

  step_start="$(date +%s)"
  log "Step 2/2: running catalog coverage/linkage/contribution scoring"
  report_dir="$("$ROOT_DIR/scripts/catalog_eval.sh" \
    --sample-size "$SAMPLE_SIZE" \
    --seed "$SEED" \
    --catalog gaia_dr3_sample \
    --catalog gaia_dr3_non_single_sample \
    --catalog gaia_dr3_nss_two_body_sample \
    --catalog athyg \
    --catalog nasa_exoplanet_archive \
    --catalog wds \
    --catalog msc \
    --catalog orb6 \
    --catalog sbx_sample \
    --catalog debcat \
    --catalog kepler_eb)"
  step_end="$(date +%s)"
  log "Step 2/2 complete ($(duration_s "$step_start" "$step_end")s)"

  run_end="$(date +%s)"
  log "Catalog contribution evaluation complete ($(duration_s "$run_start" "$run_end")s total)"
  log "Report directory: $report_dir"
  log "Review: $report_dir/summary.md and $report_dir/summary.json"
}

main "$@"
