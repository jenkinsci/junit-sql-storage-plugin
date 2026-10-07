#!/usr/bin/env bash
# Deletes the load-test job created by create-load-job.sh.
#
# Usage:
#   ./scripts/benchmark/delete-load-job.sh [job-name]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./lib.sh

JOB_NAME="${1:-$LOAD_JOB_NAME}"

bench_require curl

crumb="$(bench_crumb_header)"
crumb_args=()
[[ -n "$crumb" ]] && crumb_args=(-H "$crumb")

bench_log "Deleting job '${JOB_NAME}' at ${JENKINS_URL}"
status=$(bench_curl -o /dev/null -w '%{http_code}' "${crumb_args[@]}" -X POST "${JENKINS_URL}/job/${JOB_NAME}/doDelete")

if [[ "$status" != "200" && "$status" != "302" ]]; then
    bench_log "Failed to delete job (HTTP ${status}); it may not exist"
    exit 1
fi

bench_log "Deleted '${JOB_NAME}'"
