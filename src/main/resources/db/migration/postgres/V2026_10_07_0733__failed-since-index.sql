-- Supports computeFailedSinceRun's two point lookups (last-passing-build-before /
-- first-failing-build-after a given test), which filter by
-- "job = ? AND build </> ? AND suite = ? AND package = ? AND classname = ? AND testname = ?
-- AND errordetails IS [NOT] NULL ORDER BY build [ASC|DESC] LIMIT 1". Without this index, those
-- queries only have job_and_build_index to work with, so Postgres has to scan/filter every row of
-- the job (all builds) to find the handful matching one test's identity -- this gets very
-- expensive for jobs with large history (tens of millions of rows for a single job in reported
-- production installs).
--
-- Unlike MySQL (which supports indexing a length-limited *prefix* of a column, letting the MySQL
-- version of this migration index the full job/classname/testname values directly without risking
-- an oversized index entry), PostgreSQL btree indexes have a hard per-entry size limit (roughly
-- 2704 bytes with the default 8KiB page size) and no equivalent prefix-index syntax. The supported
-- column lengths here (job varchar(255), classname varchar(255), testname varchar(500)) can hold
-- values whose combined UTF-8 encoding exceeds that limit (multibyte characters can be up to 4
-- bytes each in UTF-8), which would make a plain "(job, classname, testname, build)" index fail
-- outright on long/multibyte values -- not just during this migration's initial index build over
-- existing rows, but on every future insert of such a row thereafter.
--
-- Instead, this indexes a fixed-size (32 character) MD5 hash of (job, classname, testname),
-- computed in a stored generated column so it only has to be computed once per row (at
-- insert/update time), not on every query. The hash is always a bounded size regardless of how
-- long or how multibyte the underlying identity values are, so it can never hit the index
-- row-size limit. The application still applies job/classname/testname as exact-value residual
-- filters on top of the hash match, so a hash collision between two different tests cannot
-- produce an incorrect match (see computeFailedSinceRun in DatabaseTestResultStorage, which
-- queries by this hash on PostgreSQL and keeps the original equality filters as residual checks).
ALTER TABLE caseresults ADD COLUMN testidentityhash varchar(32)
    GENERATED ALWAYS AS (md5(coalesce(job, '') || chr(1) || coalesce(classname, '') || chr(1) || coalesce(testname, ''))) STORED;

CREATE INDEX failed_since_index ON caseresults(testidentityhash, build);

-- job_index only ever helped bare "WHERE job = ?" lookups (job deletion); job_and_build_index
-- already serves that (and every other) query just as well since job is its leading column, so
-- job_index was pure write/storage overhead with no read benefit.
DROP INDEX job_index;
