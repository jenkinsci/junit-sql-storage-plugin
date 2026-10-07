# Database performance TODO

This tracks follow-up work from evaluating production feedback in
[issue #535](https://github.com/jenkinsci/junit-sql-storage-plugin/issues/535) ("Future performance
enhancements", reporting a MySQL install with ~84M rows / 34GB data + 6.4GB indexes in `caseResults`)
against what was already fixed in [PR #536](https://github.com/jenkinsci/junit-sql-storage-plugin/pull/536)
("Performance enhancements, fix connection leak"), and what this session additionally validated/fixed.

All findings below were empirically validated against a synthetic 85,000,000-row MySQL 8.0.46 dataset shaped
to match the issue's reported production distribution (1 job × 22 builds × ~295K rows, 25 jobs × 20 builds ×
150K rows, 1 job × 10,000 builds × 350 rows, 100 small jobs × 10 builds × 10 rows), built in an isolated
Docker benchmark environment, not just reasoned about from EXPLAIN output.

## Root causes from issue #535, and status

| # | Root cause | Status | Notes |
|---|------------|--------|-------|
| 1 | No PK/clustering on `caseResults`, so `job`/`build` locality is accidental (heap table) | **Fixed** (follow-up session) | Re-benchmarked with a corrected, more realistic methodology: a 16,000,000-row MySQL table built with true 2000-row chunk-level round-robin interleaving across 21 concurrent jobs (matching `MAX_DB_BATCH_SIZE`), rather than the earlier job-by-job sequential load, which under-represented real fragmentation. Measured via cold-cache physical page reads (`Innodb_buffer_pool_reads`, more reliable than wall-clock on fast SSD-backed storage): clustering on `(job, build, id)` cut physical reads for a single build's full-row fetch from 3107 to 1622 — a **~48% reduction** — at a **~6% write-throughput cost** under realistic interleaved concurrent writes (58.0s vs 61.5s for 4.2M rows). This supersedes the earlier ~30-36% estimate. Implemented as idempotent Flyway migrations for both MySQL (`V2026_10_09_1200__case-results-primary-key.sql`, using guarded `PREPARE`/`EXECUTE` so it's a no-op if pre-applied) and PostgreSQL (`DO $$ ... END $$` block) — see `docs/add-case-results-primary-key.md`. Also drops the now-redundant `job_and_build_index` (superseded by the new PK's leading columns). PostgreSQL gets the PK for row-identity purposes but, being a heap table, does not get the same automatic clustering read speedup MySQL does. |
| 2 | Missing index to support "failed since" lookups (`computeFailedSinceRun`), causing full per-job scans | **Fixed** (this session) | Added `failed_since_index` on `(job, classname, testname, build)` — MySQL migration uses prefix lengths `job(100), className(150), testName(150)` to stay under InnoDB's 3072-byte max key length; Postgres migration indexes full columns (no prefix-length syntax needed/available there). Validated on the 85M-row dataset: before, `EXPLAIN` showed `rows≈11.9M` and the query took ~17.5s (matching the issue's own reported `rows=12,727,948`); after, `rows=1` and the query takes ~85ms. Index build itself took 12m17s on 85M rows (one-time migration cost to plan around on very large installs). |
| 3 | Trend/duration/history pages recompute aggregates over all of `caseResults` on every request | **Already fixed by PR #536** (validated this session) | PR #536 added the persisted `caseResultsSummary` table, maintained incrementally at publish time, with a one-time backfill migration. Benchmarked on the 85M-row dataset: old raw `GROUP BY` aggregate for the largest job took 33.3s; reading the persisted summary took 0.46s (~72x speedup). The one-time backfill `INSERT ... SELECT ... GROUP BY` itself took 6m25s on 85M rows, directly reproducing the issue's reported "~2 minutes" full-table aggregate cost at smaller scale — a real, one-time migration cost to be aware of on upgrade, but it only runs once. |
| 4 | Write path not transactional / not provably batched (summary + rows could be inconsistent; batch-rewrite driver flags undocumented) | **Fixed** (this session) | `doPublish()` now wraps each flushed chunk (the `caseResults` batch insert + its `caseResultsSummary` upsert) in a single transaction (`connection.setAutoCommit(false)` / commit per chunk / rollback on failure), mirroring the existing pattern in `deleteRun()`/`deleteJob()`. Also narrowed `upsertSummary()`'s exception handling: it previously treated *any* `SQLException` from the insert as an assumed duplicate-key race and retried as an `UPDATE`; it now only does that when `SQLState` starts with `23` (integrity-constraint-violation class), rethrowing anything else so real failures are no longer silently masked. Documented `rewriteBatchedStatements=true` (MySQL)/`reWriteBatchedInserts=true` (PostgreSQL) JDBC URL properties in the README, since the plugin's own batching (`addBatch`/`executeBatch`) doesn't by itself guarantee the driver sends a single rewritten multi-row `INSERT` without these flags. |
| 5 | Migration risk: large installs can't safely run a blocking `ALTER TABLE` to add a clustering PK | **Fixed** (follow-up session) | Addressed by the same migration as #1 plus a standalone pre-upgrade script (`scripts/mysql-add-case-results-primary-key.sh`), implementing the issue's own proposed approach: build a new table alongside the old one, bulk-copy ordered by `(job, build)` while Jenkins keeps running, then a brief write-paused catch-up + `COUNT(*)`-validated `RENAME TABLE` swap. Validated end-to-end (happy path, simulated late writes during the copy window, and NULL `job`/`build` precondition rejection) against a local MySQL 8.0.46 container. The automatic Flyway migration remains idempotent, so operators who pre-apply the script hit a no-op on upgrade. |
| 6 | No orphan-row cleanup maintenance action (rows left behind by jobs/builds deleted outside Jenkins, e.g. direct DB manipulation, or a missed cleanup listener invocation) | **Not started** | `TestResultCleanupListener` already deletes `caseResults`/`caseResultsSummary` rows on normal build/job deletion through Jenkins; this is specifically about a maintenance action to find/remove rows with no corresponding Jenkins job/build (e.g. after manual DB edits or historical gaps before the cleanup listener existed). Still open. |

## Remaining work (not done this session)

- [ ] Orphan-row cleanup maintenance action (dry-run-first, admin-gated) — root cause #6 above.
- [ ] Consider whether `ResultsEntry.loadSummaryFromDB()`'s stacktrace-only-failure predicates should also
      read from a persisted summary path, distinct from the trend-summary predicates already backed by
      `caseResultsSummary` (kept separate deliberately; revisit if it becomes a measured bottleneck).
- [ ] No standalone pre-upgrade script exists for PostgreSQL (its default migration path is comparatively
      cheap relative to MySQL's forced full-table rebuild, so it was judged lower priority); very large
      Postgres installs wanting to avoid any startup delay can adapt the MySQL script's pattern manually.

## Benchmark environment used for validation

A disposable MySQL 8.0.46 Docker environment (capped at 8GiB RAM / 1.5 vCPU) was built on a remote host to
generate and query an 85,000,000-row synthetic `caseResults` table shaped to match the issue's reported
production numbers, using a server-side stored procedure for fast bulk generation (~100K rows/sec). This let
every root cause above be validated with real before/after timings at production scale rather than purely
from `EXPLAIN` output or code inspection. The benchmark environment was torn down after validation completed;
see git history/session notes for the exact schema/procedure used if this needs to be reproduced.
