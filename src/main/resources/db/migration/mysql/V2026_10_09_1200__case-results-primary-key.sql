-- Adds a surrogate id column and makes (job, build, id) the InnoDB clustering primary key for
-- caseResults.
--
-- Why: caseResults has never had a primary key, so InnoDB clusters it on an internal, invisible
-- row id that reflects insertion order. Production installs publish many jobs concurrently, so
-- rows for any one build end up physically scattered across the table rather than contiguous.
-- Every read path that fetches "one build's rows" (loadCaseResultRows/getSuite, history, trend
-- backfill, etc.) pays for that scatter as extra random page reads. Benchmarking against a 16M-row
-- table built with the same interleaved-chunk write pattern real concurrent publishers produce
-- (see TODO.md) showed clustering on (job, build, id) cuts physical page reads for a single
-- build's full-row fetch by roughly half (~48%), at a roughly 6% write-throughput cost under
-- concurrent interleaved writes -- a clear net win for the plugin's read-heavy workload.
--
-- Once rows are clustered by (job, build, id), job_and_build_index(job, build) is redundant: any
-- "WHERE job = ? AND build = ?" query can be served directly from the clustering primary key
-- itself (which starts with job, build) with no secondary-index lookup at all, so the old index is
-- dropped to remove its write/storage overhead.
--
-- Cost and idempotency: for MySQL, "ADD PRIMARY KEY" always rebuilds the whole table (InnoDB
-- online DDL "Rebuilds table: Yes; Permits Concurrent DML: No"), so this migration blocks writes
-- to caseResults for its duration -- roughly 24 seconds per million existing rows in the same
-- benchmark (about 6m24s for 16M rows on commodity SSD storage). Installs with a very large
-- existing caseResults table should run the standalone pre-upgrade script documented in
-- docs/add-case-results-primary-key.md *before* upgrading, so this migration finds the column/key
-- already present and becomes a no-op (the information_schema checks below make every step safe
-- to re-run or skip).
SET @needs_id := (SELECT COUNT(*) = 0
                   FROM information_schema.columns
                   WHERE table_schema = DATABASE()
                     AND table_name = 'caseResults'
                     AND column_name = 'id');
SET @add_id_sql := IF(@needs_id,
                       'ALTER TABLE caseResults ADD COLUMN id BIGINT NOT NULL AUTO_INCREMENT FIRST, ADD KEY id_key (id)',
                       'DO 0');
PREPARE add_id_stmt FROM @add_id_sql;
EXECUTE add_id_stmt;
DEALLOCATE PREPARE add_id_stmt;

SET @needs_pk := (SELECT COUNT(*) = 0
                   FROM information_schema.table_constraints
                   WHERE table_schema = DATABASE()
                     AND table_name = 'caseResults'
                     AND constraint_type = 'PRIMARY KEY');
SET @add_pk_sql := IF(@needs_pk,
                       'ALTER TABLE caseResults ADD PRIMARY KEY (job, build, id)',
                       'DO 0');
PREPARE add_pk_stmt FROM @add_pk_sql;
EXECUTE add_pk_stmt;
DEALLOCATE PREPARE add_pk_stmt;

SET @has_job_and_build_index := (SELECT COUNT(*) > 0
                                  FROM information_schema.statistics
                                  WHERE table_schema = DATABASE()
                                    AND table_name = 'caseResults'
                                    AND index_name = 'job_and_build_index');
SET @drop_index_sql := IF(@has_job_and_build_index,
                           'DROP INDEX job_and_build_index ON caseResults',
                           'DO 0');
PREPARE drop_index_stmt FROM @drop_index_sql;
EXECUTE drop_index_stmt;
DEALLOCATE PREPARE drop_index_stmt;
