#!/usr/bin/env bash
# Shared helpers for the scripts/benchmark/*.sh load-testing tools. Not meant to be run directly.
#
# Every setting here is read from an environment variable with a localhost-only default, so these
# scripts never need a hardcoded host/IP: point them at a remote Jenkins/database by exporting the
# variables below before running a script, e.g.:
#
#   export JENKINS_URL=http://jenkins.example.internal:8080
#   export JENKINS_USER=admin
#   export JENKINS_API_TOKEN=xxxx
#   ./scripts/benchmark/create-load-job.sh
#
# Jenkins connection settings.
JENKINS_URL="${JENKINS_URL:-http://localhost:8080}"
JENKINS_USER="${JENKINS_USER:-}"
JENKINS_API_TOKEN="${JENKINS_API_TOKEN:-}"

# Database connection settings, used only by watch-db-activity.sh.
# DB_EXEC_MODE selects how `psql`/`mysql` are invoked:
#   "compose" (default) - run through `docker compose exec`, i.e. the local dev stack from
#                          docker-compose.yaml; only works against a locally running compose project.
#   "direct"             - run `psql`/`mysql` directly against DB_HOST/DB_PORT/DB_USER/DB_NAME,
#                          which is what you want for a remote database. DB_PASSWORD (Postgres, via
#                          PGPASSWORD) or MYSQL_PWD (MySQL) may also be set; neither is ever printed
#                          or logged by these scripts.
DB_EXEC_MODE="${DB_EXEC_MODE:-compose}"
DB_ENGINE="${DB_ENGINE:-postgres}"
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-}"
DB_USER="${DB_USER:-postgres}"
DB_NAME="${DB_NAME:-}"

LOAD_JOB_NAME="${LOAD_JOB_NAME:-bench-concurrent-publish}"

bench_log() {
    echo "[$(date '+%H:%M:%S')] $*" >&2
}

bench_require() {
    command -v "$1" >/dev/null 2>&1 || {
        bench_log "Required command '$1' not found on PATH"
        exit 1
    }
}

# curl wrapper that authenticates (if JENKINS_USER/JENKINS_API_TOKEN are set) and shares a cookie
# jar across calls so the CSRF crumb fetched by bench_crumb_header can be reused for POSTs.
_BENCH_COOKIE_JAR="$(mktemp -t bench-jenkins-cookies.XXXXXX)"
trap 'rm -f "$_BENCH_COOKIE_JAR"' EXIT

bench_curl() {
    local auth=()
    if [[ -n "$JENKINS_USER" ]]; then
        auth=(-u "${JENKINS_USER}:${JENKINS_API_TOKEN}")
    fi
    # -g (globoff) disables curl's own `[]`/`{}` URL globbing, which would otherwise misparse the
    # `tree=builds[building]` style query parameters used elsewhere in these scripts.
    curl -sSg -b "$_BENCH_COOKIE_JAR" -c "$_BENCH_COOKIE_JAR" "${auth[@]}" "$@"
}

# Reads JSON from stdin and prints the result of a query, preferring `jq` (jq_filter) and falling
# back to `python3` (python_expr, evaluated with the parsed document bound to `d`) since a remote
# host is not guaranteed to have jq installed but very likely has python3. Avoids fragile
# grep/sed JSON "parsing", which breaks on nested arrays (e.g. Jenkins queue items contain their
# own nested "causes":[...] arrays).
bench_json_query() {
    local jq_filter="$1" python_expr="$2"
    if command -v jq >/dev/null 2>&1; then
        jq -r "$jq_filter"
    elif command -v python3 >/dev/null 2>&1; then
        python3 -c "import json, sys; d = json.load(sys.stdin); print(${python_expr})"
    else
        bench_log "Neither jq nor python3 is available on PATH to parse Jenkins API responses"
        exit 1
    fi
}

# Fetches a CSRF crumb header (as a single "Name: value" string suitable for curl -H) bound to the
# shared cookie jar above. Prints nothing and returns success if the instance has crumb issuance
# disabled, so this is always safe to call before a POST.
bench_crumb_header() {
    local crumb
    crumb="$(bench_curl -s "${JENKINS_URL}/crumbIssuer/api/json" 2>/dev/null \
        | sed -n 's/.*"crumbRequestField":"\([^"]*\)".*"crumb":"\([^"]*\)".*/\1: \2/p')"
    if [[ -z "$crumb" ]]; then
        # Field order can be swapped depending on Jenkins version; try the other order too.
        crumb="$(bench_curl -s "${JENKINS_URL}/crumbIssuer/api/json" 2>/dev/null \
            | sed -n 's/.*"crumb":"\([^"]*\)".*"crumbRequestField":"\([^"]*\)".*/\2: \1/p')"
    fi
    echo "$crumb"
}

# Runs a SQL statement (first argument) against the configured database and prints the result.
# Routes through either `docker compose exec` (local dev stack) or a direct client invocation
# (remote database), selected by DB_EXEC_MODE, so none of these scripts ever hardcode a host.
bench_run_sql() {
    local sql="$1"
    case "$DB_ENGINE" in
        postgres)
            if [[ "$DB_EXEC_MODE" == "compose" ]]; then
                docker compose exec -T db psql -U "${DB_USER}" -d "${DB_NAME:-postgres}" -c "$sql"
            else
                bench_require psql
                PGPASSWORD="${DB_PASSWORD:-${PGPASSWORD:-}}" \
                    psql -h "$DB_HOST" -p "${DB_PORT:-5432}" -U "$DB_USER" -d "${DB_NAME:-postgres}" -c "$sql"
            fi
            ;;
        mysql)
            if [[ "$DB_EXEC_MODE" == "compose" ]]; then
                docker compose exec -T db mysql -u "${DB_USER}" -e "$sql" "${DB_NAME:-jenkins}"
            else
                bench_require mysql
                MYSQL_PWD="${DB_PASSWORD:-${MYSQL_PWD:-}}" \
                    mysql -h "$DB_HOST" -P "${DB_PORT:-3306}" -u "$DB_USER" -e "$sql" "${DB_NAME:-jenkins}"
            fi
            ;;
        *)
            bench_log "Unknown DB_ENGINE '$DB_ENGINE' (expected postgres or mysql)"
            exit 1
            ;;
    esac
}
