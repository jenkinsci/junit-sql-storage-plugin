#!/usr/bin/env bash
# Fires many concurrent builds of a load-test job created by create-load-job.sh, each with a unique
# RUN_ID parameter so Jenkins does not coalesce rapid-fire build requests into a single queue item
# (observed behavior: triggering the same parameterless job many times quickly can silently merge
# most of the requests into one build). Then waits for the queue to drain and all builds to finish.
#
# Usage:
#   ./scripts/benchmark/trigger-concurrent-builds.sh [job-name] [build-count]
#
# Env vars: see lib.sh. CONCURRENCY caps how many trigger requests are in flight at once
# (default: build-count, i.e. fire them all at once).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./lib.sh

JOB_NAME="${1:-$LOAD_JOB_NAME}"
BUILD_COUNT="${2:-20}"
CONCURRENCY="${CONCURRENCY:-$BUILD_COUNT}"
POLL_INTERVAL="${POLL_INTERVAL:-2}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-1800}"

bench_require curl

crumb="$(bench_crumb_header)"
crumb_args=()
[[ -n "$crumb" ]] && crumb_args=(-H "$crumb")

bench_log "Triggering ${BUILD_COUNT} builds of '${JOB_NAME}' at ${JENKINS_URL} (concurrency ${CONCURRENCY})"

trigger_one() {
    local run_id="$1"
    bench_curl -o /dev/null -s "${crumb_args[@]}" \
        --data-urlencode "RUN_ID=${run_id}" \
        "${JENKINS_URL}/job/${JOB_NAME}/buildWithParameters"
}

start_epoch=$(date +%s)
running=0
for ((i = 1; i <= BUILD_COUNT; i++)); do
    trigger_one "bench-$(date +%s%N)-${i}" &
    running=$((running + 1))
    if ((running >= CONCURRENCY)); then
        wait
        running=0
    fi
done
wait
bench_log "All ${BUILD_COUNT} trigger requests sent in $(( $(date +%s) - start_epoch ))s"

bench_log "Waiting for queue to drain..."
while true; do
    queue_len=$(bench_curl -s "${JENKINS_URL}/queue/api/json" \
        | bench_json_query '.items | length' 'len(d["items"])')
    if [[ "$queue_len" == "0" ]]; then
        break
    fi
    elapsed=$(( $(date +%s) - start_epoch ))
    if ((elapsed > TIMEOUT_SECONDS)); then
        bench_log "Timed out after ${TIMEOUT_SECONDS}s waiting for queue to drain (still ${queue_len} queued)"
        exit 1
    fi
    bench_log "  ${queue_len} item(s) still queued (${elapsed}s elapsed)"
    sleep "$POLL_INTERVAL"
done

bench_log "Waiting for running builds of '${JOB_NAME}' to finish..."
while true; do
    building=$(bench_curl -s "${JENKINS_URL}/job/${JOB_NAME}/api/json?tree=builds[building]" \
        | bench_json_query '[.builds[] | select(.building == true)] | length' \
                            'sum(1 for b in d["builds"] if b["building"])')
    if [[ "$building" == "0" ]]; then
        break
    fi
    elapsed=$(( $(date +%s) - start_epoch ))
    if ((elapsed > TIMEOUT_SECONDS)); then
        bench_log "Timed out after ${TIMEOUT_SECONDS}s waiting for builds to finish (still ${building} building)"
        exit 1
    fi
    bench_log "  ${building} build(s) still running (${elapsed}s elapsed)"
    sleep "$POLL_INTERVAL"
done

bench_log "Done: ${BUILD_COUNT} builds triggered and completed in $(( $(date +%s) - start_epoch ))s total"
