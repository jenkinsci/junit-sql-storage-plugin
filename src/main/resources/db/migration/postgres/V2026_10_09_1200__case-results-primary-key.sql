-- Adds a surrogate id column and a (job, build, id) primary key to caseresults.
--
-- Why: caseresults has never had a primary key. On MySQL this means InnoDB clusters the table on
-- an internal, invisible row id reflecting insertion order, so a single build's rows are scattered
-- across the table once multiple jobs publish concurrently; adding a primary key on
-- (job, build, id) there lets InnoDB physically cluster rows by build, measured to cut physical
-- page reads for a single build's full-row fetch by roughly half (see the MySQL migration with the
-- same name and TODO.md for the benchmark).
--
-- PostgreSQL is a heap table: adding a primary key here does NOT physically reorder existing or
-- future rows the way MySQL's clustering primary key does (only a one-off, non-maintained CLUSTER
-- command would do that, and it is not run automatically here). So this migration's benefit on
-- PostgreSQL is uniqueness/identity -- a stable per-row id, needed for correct, resumable orphan
-- cleanup and future maintenance tooling -- rather than the same read-I/O win MySQL gets. It is
-- still worth applying for that reason, and for schema parity between the two supported engines.
--
-- job_and_build_index(job, build) becomes redundant once the primary key (job, build, id) exists:
-- any planner that would have used job_and_build_index can equally use the primary key's own
-- index, since job, build are its leading columns, so the superseded index is dropped to remove
-- its write/storage overhead without losing query coverage.
--
-- Idempotent: every step below is guarded so this migration is a no-op on an install where an
-- operator has already applied the equivalent standalone script before upgrading (see
-- docs/add-case-results-primary-key.md), which is the recommended approach for a very large
-- existing table since PostgreSQL still has to build a new unique index over all existing rows
-- here (cheaper than MySQL's full-table rebuild, but not free).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = current_schema() AND table_name = 'caseresults' AND column_name = 'id') THEN
        ALTER TABLE caseresults ADD COLUMN id BIGSERIAL;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints
                   WHERE table_schema = current_schema() AND table_name = 'caseresults' AND constraint_type = 'PRIMARY KEY') THEN
        ALTER TABLE caseresults ADD PRIMARY KEY (job, build, id);
    END IF;

    IF EXISTS (SELECT 1 FROM pg_indexes
               WHERE schemaname = current_schema() AND tablename = 'caseresults' AND indexname = 'job_and_build_index') THEN
        DROP INDEX job_and_build_index;
    END IF;
END $$;
