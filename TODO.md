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
| 1 | No PK/clustering on `caseResults`, so `job`/`build` locality is accidental (heap table) | **Not fixed, documented as future manual work** (see below) | Measured ~30-36% speedup from clustering on `(job, build, id)` in this synthetic test (load 150K-row build: 308ms→196ms; full-job scan: 18.68s→12.88s). Likely an *understatement* of real production benefit, since this synthetic dataset was generated job-by-job (sequentially), unlike real interleaved concurrent-job production writes, so the "unclustered" baseline here was artificially well-clustered already. Not applied as an automatic Flyway migration: adding an `AUTO_INCREMENT`/clustering PK to an 80M+ row live table requires an online copy-and-swap (`pt-online-schema-change`/`gh-ost`-style), which this plugin does not currently orchestrate, and which carries real production risk if done naively. **Recommendation**: document as an optional, operator-triggered maintenance script/runbook rather than a silent Flyway migration. |
| 2 | Missing index to support "failed since" lookups (`computeFailedSinceRun`), causing full per-job scans | **Fixed** (this session) | Added `failed_since_index` on `(job, classname, testname, build)` — MySQL migration uses prefix lengths `job(100), className(150), testName(150)` to stay under InnoDB's 3072-byte max key length; Postgres migration indexes full columns (no prefix-length syntax needed/available there). Validated on the 85M-row dataset: before, `EXPLAIN` showed `rows≈11.9M` and the query took ~17.5s (matching the issue's own reported `rows=12,727,948`); after, `rows=1` and the query takes ~85ms. Index build itself took 12m17s on 85M rows (one-time migration cost to plan around on very large installs). |
| 3 | Trend/duration/history pages recompute aggregates over all of `caseResults` on every request | **Already fixed by PR #536** (validated this session) | PR #536 added the persisted `caseResultsSummary` table, maintained incrementally at publish time, with a one-time backfill migration. Benchmarked on the 85M-row dataset: old raw `GROUP BY` aggregate for the largest job took 33.3s; reading the persisted summary took 0.46s (~72x speedup). The one-time backfill `INSERT ... SELECT ... GROUP BY` itself took 6m25s on 85M rows, directly reproducing the issue's reported "~2 minutes" full-table aggregate cost at smaller scale — a real, one-time migration cost to be aware of on upgrade, but it only runs once. |
| 4 | Write path not transactional / not provably batched (summary + rows could be inconsistent; batch-rewrite driver flags undocumented) | **Fixed** (this session) | `doPublish()` now wraps each flushed chunk (the `caseResults` batch insert + its `caseResultsSummary` upsert) in a single transaction (`connection.setAutoCommit(false)` / commit per chunk / rollback on failure), mirroring the existing pattern in `deleteRun()`/`deleteJob()`. Also narrowed `upsertSummary()`'s exception handling: it previously treated *any* `SQLException` from the insert as an assumed duplicate-key race and retried as an `UPDATE`; it now only does that when `SQLState` starts with `23` (integrity-constraint-violation class), rethrowing anything else so real failures are no longer silently masked. Documented `rewriteBatchedStatements=true` (MySQL)/`reWriteBatchedInserts=true` (PostgreSQL) JDBC URL properties in the README, since the plugin's own batching (`addBatch`/`executeBatch`) doesn't by itself guarantee the driver sends a single rewritten multi-row `INSERT` without these flags. |
| 5 | Migration risk: large installs can't safely run a blocking `ALTER TABLE` to add a clustering PK | **Documented, not automated** | See #1 above — same underlying concern. The issue's own suggested approach (index on `timestamp`, copy build-by-build, catch up, `RENAME TABLE` under `LOCK TABLES`) is sound but out of scope for an automatic Flyway migration; left as a documented manual/operator path. |
| 6 | No orphan-row cleanup maintenance action (rows left behind by jobs/builds deleted outside Jenkins, e.g. direct DB manipulation, or a missed cleanup listener invocation) | **Not started** | `TestResultCleanupListener` already deletes `caseResults`/`caseResultsSummary` rows on normal build/job deletion through Jenkins; this is specifically about a maintenance action to find/remove rows with no corresponding Jenkins job/build (e.g. after manual DB edits or historical gaps before the cleanup listener existed). Still open. |

## Remaining work (not done this session)

- [ ] Orphan-row cleanup maintenance action (dry-run-first, admin-gated) — root cause #6 above.
- [ ] Optional operator-triggered clustering migration (copy-and-swap script per the issue's proposal) for
      installs that want `caseResults` physically clustered by `(job, build)` — root causes #1/#5 above.
      Not an automatic Flyway migration given the blocking-`ALTER TABLE` risk on very large existing tables.
- [ ] Consider whether `ResultsEntry.loadSummaryFromDB()`'s stacktrace-only-failure predicates should also
      read from a persisted summary path, distinct from the trend-summary predicates already backed by
      `caseResultsSummary` (kept separate deliberately; revisit if it becomes a measured bottleneck).

## Benchmark environment used for validation

A disposable MySQL 8.0.46 Docker environment (capped at 8GiB RAM / 1.5 vCPU) was built on a remote host to
generate and query an 85,000,000-row synthetic `caseResults` table shaped to match the issue's reported
production numbers, using a server-side stored procedure for fast bulk generation (~100K rows/sec). This let
every root cause above be validated with real before/after timings at production scale rather than purely
from `EXPLAIN` output or code inspection. The benchmark environment was torn down after validation completed;
see git history/session notes for the exact schema/procedure used if this needs to be reproduced.
