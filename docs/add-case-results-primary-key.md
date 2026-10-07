# Adding a primary key / clustering to `caseResults`

## What changed and why

`caseResults` has never had a primary key. On MySQL that means InnoDB clusters the table on an
internal, invisible row id reflecting **insertion order**, not `(job, build)`. On a production
instance where many jobs publish test results concurrently, that means a single build's rows end
up physically scattered across the table rather than stored together.

This matters because every read path that loads "one build's rows" (`loadCaseResultRows`/
`getSuite` in `DatabaseTestResultStorage`, history pages, trend/summary backfill, etc.) has to pay
for that scatter as extra random page reads.

As of this change, the plugin's Flyway migrations add a surrogate `id` column and make
`(job, build, id)` the primary key on both supported databases:

- **MySQL**: this makes InnoDB physically cluster rows by `(job, build, id)`, so a build's rows
  become contiguous on disk. Benchmarked against a 16,000,000-row table built with the same
  interleaved-chunk write pattern real concurrent publishers produce, clustering reduced physical
  page reads for a single build's full-row fetch by roughly **48%**, at a roughly **6%**
  write-throughput cost under concurrent interleaved writes.
- **PostgreSQL**: `caseResults` remains a heap table — adding a primary key here does **not**
  physically reorder rows the way MySQL's clustering primary key does (only a one-off, non-maintained
  `CLUSTER` command would, and it is not run automatically). The primary key still adds real
  value on Postgres: a stable per-row identity, needed for correct, resumable orphan-row cleanup
  and other future maintenance tooling that targets individual rows. Do not expect the same
  read-I/O speedup on Postgres that MySQL gets.

## The automatic migration's cost on an existing large table

On MySQL, `ALTER TABLE ... ADD PRIMARY KEY` always rebuilds the whole table (InnoDB online DDL:
"Rebuilds table: Yes; Permits Concurrent DML: No"), so the Flyway migration
(`V2026_10_09_1200__case-results-primary-key.sql`) blocks all reads/writes to `caseResults` for its
full duration the next time Jenkins starts up with the upgraded plugin.

Measured cost: roughly **24 seconds per million existing rows** (about 6m24s for 16,000,000 rows)
on commodity SSD-backed storage. This scales with table size, so an 80M+ row production table could
mean a startup delay of an hour or more.

On PostgreSQL, adding a primary key is comparatively cheap (it builds a new unique btree index over
existing rows, without MySQL's forced full-table rebuild), but it is still not free on a very large
table, and Jenkins will be unable to serve `caseResults` reads/writes while it runs.

## Recommended approach for large installs

If your `caseResults` table is large enough that an unplanned multi-minute-or-longer startup delay
is unacceptable, run the standalone script **before** upgrading the plugin, at a time of your
choosing:

```
scripts/mysql-add-case-results-primary-key.sh prepare  --host <host> --user <user> --password <password> --database <db>
# ... runs for as long as it takes; Jenkins keeps working normally throughout ...
# When ready, pause/stop Jenkins test-result publishing (quiet-down + wait for in-flight builds'
# publish steps to finish, or stop Jenkins outright), then:
scripts/mysql-add-case-results-primary-key.sh finalize --host <host> --user <user> --password <password> --database <db>
```

`prepare` builds a new `caseResults_new` table with the target `(job, build, id)` primary key and
bulk-copies all existing rows into it, ordered by `(job, build)`, while the original `caseResults`
table remains fully readable and writable — this is the slow part, and it does not block Jenkins.

`finalize` requires that you have actually paused/stopped publishing first; it does not do this for
you. It then:

1. Catches up any rows written to `caseResults` since `prepare` started, using a coarse,
   deliberately-overlapping filter on the existing `timestamp` column (there is no existing row id
   to use as a precise watermark, and a timestamp alone is not a reliable commit-order or uniqueness
   guarantee — duplicate per-second timestamps are possible under concurrent batch commits).
2. Compares `COUNT(*)` between the old and new table as the authoritative safety check, and
   **refuses to proceed if they don't match** — rather than silently completing an incomplete
   migration. If this happens, it is safe to re-run `finalize`.
3. Atomically swaps the tables with `RENAME TABLE`, preserving the original table as
   `caseResults_old` so you can verify Jenkins is healthy before manually dropping it.

Once this has been applied, upgrading the plugin finds the `id` column and primary key already
present, and its own Flyway migration becomes a no-op.

There is currently no equivalent standalone script for PostgreSQL, since its default migration path
is comparatively cheap relative to MySQL's forced full-table rebuild; very large Postgres installs
that still want to avoid any startup delay can adapt the same create-new-table/bulk-copy/swap
pattern manually.

## Precondition: `job`/`build` must not be `NULL`

The new primary key requires `job` and `build` to be `NOT NULL`. The standalone script checks for
and refuses to proceed if it finds existing rows with a `NULL` `job` or `build` value — inspect and
fix/remove such rows first (`SELECT * FROM caseResults WHERE job IS NULL OR build IS NULL;`).
