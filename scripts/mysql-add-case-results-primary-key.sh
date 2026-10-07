#!/usr/bin/env bash
# Standalone, operator-run script to pre-apply the same `caseResults` primary-key/clustering
# change the plugin's own Flyway migration (V2026_10_09_1200__case-results-primary-key.sql) makes
# automatically on startup -- intended for installs with a very large existing `caseResults` table
# on MySQL, where that automatic migration's blocking ALTER TABLE rebuild (benchmarked at roughly
# 24 seconds per million existing rows -- about 6m24s for 16,000,000 rows -- and scaling with table
# size) is an unacceptable, unplanned startup delay the next time Jenkins is restarted.
#
# Run this BEFORE upgrading the plugin, at a time of the operator's own choosing, so the Flyway
# migration in the upgraded plugin finds the work already done and becomes a no-op.
#
# WHY NOT JUST LET THE AUTOMATIC MIGRATION RUN: on MySQL, "ALTER TABLE ... ADD PRIMARY KEY" always
# rebuilds the whole table (InnoDB online DDL: "Rebuilds table: Yes; Permits Concurrent DML: No"),
# so it blocks all reads/writes to caseResults for its full duration. On an 80M+ row production
# table that could be many tens of minutes of Jenkins being unable to read or publish any test
# results. This script instead builds the new table alongside the old one (so Jenkins keeps
# running normally for the bulk copy, which is the slow part), and only needs a short write-paused
# window for the final catch-up + swap.
#
# WHAT THIS DOES NOT DO: this script does not try to cleverly track "rows written during the bulk
# copy" using the existing `timestamp` column as a precise commit-order watermark -- a timestamp is
# not a unique row identity or a strict commit-ordering guarantee (duplicate timestamps at one
# second resolution are possible under concurrent batch commits, and it says nothing about
# in-flight transactions that commit slightly out of order). Instead it uses `timestamp` only as a
# coarse, deliberately-overlapping filter to narrow the catch-up pass, and then an authoritative
# COUNT(*) comparison between the old and new table to decide whether it is safe to proceed -- it
# refuses to swap if the counts do not match exactly, rather than silently completing an
# incomplete migration.
#
# USAGE:
#   mysql-add-case-results-primary-key.sh prepare   --host H --user U --password P --database D
#   # ... run normally for as long as needed; caseResults_new now exists and is being populated ...
#   # When ready for the brief write-paused maintenance window (stop Jenkins, or at minimum pause
#   # all jobs/agents that publish test results, and confirm no publish is in flight):
#   mysql-add-case-results-primary-key.sh finalize  --host H --user U --password P --database D
#
# "prepare" is safe to re-run if interrupted (DROP TABLE caseResults_new yourself first if you want
# to restart it from scratch; otherwise re-running "prepare" simply re-copies, which is wasteful
# but not unsafe, since the old table is never modified by this script until "finalize").
#
# "finalize" REQUIRES that no new rows are being written to caseResults for its duration. It does
# NOT pause writes for you -- stop/quiesce Jenkins publishing yourself first (e.g. put Jenkins in
# quiet-down mode and wait for running builds with test publishing steps to finish, or stop the
# Jenkins service outright). Running "finalize" while writes are still happening can lose rows.
set -euo pipefail

usage() {
    echo "Usage: $0 {prepare|finalize} --host HOST --user USER --password PASSWORD --database DB [--port PORT]" >&2
    exit 1
}

MODE="${1:-}"
[[ "$MODE" == "prepare" || "$MODE" == "finalize" ]] || usage
shift || true

HOST=""
PORT="3306"
USER=""
PASSWORD=""
DATABASE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host) HOST="$2"; shift 2 ;;
        --port) PORT="$2"; shift 2 ;;
        --user) USER="$2"; shift 2 ;;
        --password) PASSWORD="$2"; shift 2 ;;
        --database) DATABASE="$2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; usage ;;
    esac
done

[[ -n "$HOST" && -n "$USER" && -n "$DATABASE" ]] || usage

MYSQL=(mysql --host="$HOST" --port="$PORT" --user="$USER" --database="$DATABASE" --batch --silent)
if [[ -n "$PASSWORD" ]]; then
    export MYSQL_PWD="$PASSWORD"
fi

run_sql() {
    "${MYSQL[@]}" -e "$1"
}

if [[ "$MODE" == "prepare" ]]; then
    echo "==> Checking preconditions"
    null_key_rows=$(run_sql "SELECT COUNT(*) FROM caseResults WHERE job IS NULL OR build IS NULL;")
    if [[ "$null_key_rows" != "0" ]]; then
        echo "ERROR: $null_key_rows row(s) in caseResults have a NULL job and/or build value." >&2
        echo "The new primary key (job, build, id) requires job and build to be NOT NULL, so MySQL" >&2
        echo "will reject the bulk copy for these rows. Inspect and fix/remove them first, e.g.:" >&2
        echo "  SELECT * FROM caseResults WHERE job IS NULL OR build IS NULL LIMIT 20;" >&2
        exit 1
    fi

    existing_new=$(run_sql "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'caseResults_new';")
    if [[ "$existing_new" != "0" ]]; then
        echo "caseResults_new already exists -- assuming an earlier 'prepare' run; re-copying any rows not yet present is NOT done automatically here." >&2
        echo "If you want to start over, first run: DROP TABLE caseResults_new; then re-run 'prepare'." >&2
        exit 1
    fi

    echo "==> Recording start watermark (used only as a coarse, overlapping catch-up filter in 'finalize', never as the sole correctness check)"
    run_sql "DROP TABLE IF EXISTS caseResultsMigrationWatermark;
              CREATE TABLE caseResultsMigrationWatermark (started_at TIMESTAMP NOT NULL);
              INSERT INTO caseResultsMigrationWatermark (started_at) SELECT NOW() - INTERVAL 1 HOUR;"

    echo "==> Creating caseResults_new with the same columns/indexes as caseResults, plus the clustering primary key"
    run_sql "CREATE TABLE caseResults_new LIKE caseResults;"
    run_sql "ALTER TABLE caseResults_new ADD COLUMN id BIGINT NOT NULL AUTO_INCREMENT FIRST, ADD KEY id_key (id);"
    run_sql "ALTER TABLE caseResults_new ADD PRIMARY KEY (job, build, id);"
    # job_and_build_index is redundant once rows are clustered by (job, build, id); dropping it on
    # the new table (rather than carrying it over from "LIKE caseResults") avoids paying its
    # write/storage overhead from the very first row copied in.
    has_index=$(run_sql "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'caseResults_new' AND index_name = 'job_and_build_index';")
    if [[ "$has_index" != "0" ]]; then
        run_sql "DROP INDEX job_and_build_index ON caseResults_new;"
    fi

    echo "==> Bulk copying existing rows, ordered by (job, build) so the new table is written in its final clustered physical order"
    echo "    This is the slow part and scales with table size; caseResults remains fully readable/writable by Jenkins throughout."
    # MySQL's default session isolation level, REPEATABLE READ, requires InnoDB to take shared
    # next-key locks on every row an INSERT ... SELECT reads, held for the whole statement's
    # duration -- for a multi-hour bulk copy over 80M+ rows, that would block concurrent publishers
    # (INSERTs) and deletes against the *old* caseResults table for the whole copy, defeating the
    # purpose of doing the slow copy online. READ COMMITTED only takes (and releases per-statement)
    # record locks for rows it actually modifies, not plain consistent-read source rows, so it does
    # not hold this statement's locks across the whole copy. The isolation level is session-scoped
    # and must be set in the same session as (so before) the INSERT, which is why both are issued in
    # one run_sql call here rather than two.
    run_sql "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
             INSERT INTO caseResults_new (job, build, suite, package, className, testName, stdout, stderr, stacktrace, errorDetails, skipped, duration, timestamp)
             SELECT job, build, suite, package, className, testName, stdout, stderr, stacktrace, errorDetails, skipped, duration, timestamp
             FROM caseResults ORDER BY job, build;"

    copied=$(run_sql "SELECT COUNT(*) FROM caseResults_new;")
    echo "==> Bulk copy complete: $copied rows copied into caseResults_new."
    echo "==> Jenkins can keep running normally. When ready, pause/stop Jenkins test-result publishing and run: $0 finalize ..."
fi

if [[ "$MODE" == "finalize" ]]; then
    existing_new=$(run_sql "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'caseResults_new';")
    if [[ "$existing_new" == "0" ]]; then
        echo "caseResults_new does not exist -- run '$0 prepare ...' first." >&2
        exit 1
    fi

    echo "==> IMPORTANT: this assumes you have already stopped/paused all test-result publishing to caseResults."
    echo "    If builds are still publishing test results right now, stop here, pause them, and re-run."
    read -r -p "Type 'yes' to confirm no writes are happening and continue: " confirm
    if [[ "$confirm" != "yes" ]]; then
        echo "Aborted." >&2
        exit 1
    fi

    null_key_rows=$(run_sql "SELECT COUNT(*) FROM caseResults WHERE job IS NULL OR build IS NULL;")
    if [[ "$null_key_rows" != "0" ]]; then
        echo "ERROR: $null_key_rows row(s) in caseResults have a NULL job and/or build value." >&2
        echo "Fix/remove them first (see 'prepare' step's precondition check); refusing to continue." >&2
        exit 1
    fi

    watermark=$(run_sql "SELECT started_at FROM caseResultsMigrationWatermark LIMIT 1;")
    echo "==> Catching up rows written since prepare's start watermark ($watermark), using a wide overlapping filter"
    # The anti-join compares every column that identifies a row's content (there is still no
    # existing row id to compare against); duplicate catch-up rows are prevented by matching the
    # full row content -- including stdout/stderr/stacktrace/errorDetails/skipped, not just the
    # identity+timing columns -- so a late-arriving row that shares identity, timestamp, and
    # duration with an already-copied row but differs in its failure/output payload (e.g. a test
    # that was re-run with the same timestamp precision but a different error) is not mistaken for
    # a duplicate and skipped. A small number of false negatives (genuinely new, byte-identical
    # duplicate rows within the same second) are possible in theory; the row-count validation below
    # is the authoritative safety net, not this filter.
    run_sql "INSERT INTO caseResults_new (job, build, suite, package, className, testName, stdout, stderr, stacktrace, errorDetails, skipped, duration, timestamp)
             SELECT t.job, t.build, t.suite, t.package, t.className, t.testName, t.stdout, t.stderr, t.stacktrace, t.errorDetails, t.skipped, t.duration, t.timestamp
             FROM caseResults t
             WHERE t.timestamp >= (SELECT started_at FROM caseResultsMigrationWatermark LIMIT 1)
               AND NOT EXISTS (
                   SELECT 1 FROM caseResults_new n
                   WHERE n.job = t.job AND n.build = t.build AND n.suite <=> t.suite
                     AND n.package <=> t.package AND n.className <=> t.className AND n.testName <=> t.testName
                     AND n.timestamp = t.timestamp AND n.duration <=> t.duration
                     AND n.stdout <=> t.stdout AND n.stderr <=> t.stderr AND n.stacktrace <=> t.stacktrace
                     AND n.errorDetails <=> t.errorDetails AND n.skipped <=> t.skipped
               );"

    old_count=$(run_sql "SELECT COUNT(*) FROM caseResults;")
    new_count=$(run_sql "SELECT COUNT(*) FROM caseResults_new;")
    echo "==> caseResults has $old_count rows, caseResults_new has $new_count rows."
    if [[ "$old_count" != "$new_count" ]]; then
        echo "ERROR: row counts do not match. Refusing to swap tables." >&2
        echo "This usually means writes were still happening during finalize, or the catch-up filter missed rows." >&2
        echo "Re-run finalize (it is safe to re-run), or investigate manually before proceeding." >&2
        exit 1
    fi

    echo "==> Row counts match. Swapping tables."
    run_sql "RENAME TABLE caseResults TO caseResults_old, caseResults_new TO caseResults;"
    run_sql "DROP TABLE IF EXISTS caseResultsMigrationWatermark;"
    echo "==> Done. The old table is preserved as caseResults_old -- verify Jenkins is healthy, then"
    echo "    'DROP TABLE caseResults_old;' yourself once satisfied, to reclaim its disk space."
    echo "==> You can now upgrade the plugin; its Flyway migration will find the id column and"
    echo "    primary key already present and skip re-applying them."
fi
