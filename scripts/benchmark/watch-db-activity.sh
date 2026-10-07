#!/usr/bin/env bash
# Samples database connection/lock activity at a fixed interval, useful for watching connection-pool
# exhaustion or lock contention while scripts/benchmark/trigger-concurrent-builds.sh is generating
# concurrent publish load in another terminal.
#
# Works against either the local docker-compose database or a remote one -- see lib.sh for the
# DB_EXEC_MODE/DB_HOST/DB_PORT/DB_USER/DB_NAME/DB_PASSWORD env vars. No host is ever hardcoded here.
#
# Usage:
#   ./scripts/benchmark/watch-db-activity.sh [duration-seconds] [interval-seconds]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./lib.sh

DURATION_SECONDS="${1:-120}"
INTERVAL_SECONDS="${2:-2}"

bench_log "Watching ${DB_ENGINE} activity for ${DURATION_SECONDS}s (every ${INTERVAL_SECONDS}s), mode=${DB_EXEC_MODE}"

end_epoch=$(( $(date +%s) + DURATION_SECONDS ))
sample=0
while (( $(date +%s) < end_epoch )); do
    sample=$((sample + 1))
    echo "--- sample ${sample} @ $(date '+%H:%M:%S') ---"
    case "$DB_ENGINE" in
        postgres)
            bench_run_sql "
                SELECT count(*) AS total_connections,
                       count(*) FILTER (WHERE state = 'active') AS active,
                       count(*) FILTER (WHERE wait_event_type = 'Lock') AS waiting_on_lock
                FROM pg_stat_activity
                WHERE datname IS NOT NULL;"
            bench_run_sql "
                SELECT pid, state, wait_event_type, wait_event, now() - query_start AS running_for, left(query, 80) AS query
                FROM pg_stat_activity
                WHERE state = 'active' AND query NOT ILIKE '%pg_stat_activity%'
                ORDER BY query_start
                LIMIT 10;"
            ;;
        mysql)
            bench_run_sql "SHOW STATUS LIKE 'Threads_connected';"
            bench_run_sql "SELECT * FROM performance_schema.data_locks LIMIT 10;"
            ;;
    esac
    sleep "$INTERVAL_SECONDS"
done

bench_log "Finished watching after ${sample} sample(s)"
