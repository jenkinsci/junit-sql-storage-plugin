-- Supports computeFailedSinceRun's two point lookups (last-passing-build-before /
-- first-failing-build-after a given test), which filter by
-- "job = ? AND build </> ? AND suite = ? AND package = ? AND classname = ? AND testname = ?
-- AND errordetails IS [NOT] NULL ORDER BY build [ASC|DESC] LIMIT 1". Without this index, those
-- queries only have job_and_build_index to work with, so Postgres has to scan/filter every row of
-- the job (all builds) to find the handful matching one test's identity -- this gets very
-- expensive for jobs with large history (tens of millions of rows for a single job in reported
-- production installs). Indexing on (job, classname, testname, build) lets the planner narrow to
-- the rows for one test across all builds directly, then use the index's own build ordering to
-- satisfy "ORDER BY build ... LIMIT 1" without a separate sort; suite/package/errordetails remain
-- residual filters, which is fine since job+classname+testname alone is already near-unique per
-- row in practice.
CREATE INDEX failed_since_index ON caseresults(job, classname, testname, build);

-- job_index only ever helped bare "WHERE job = ?" lookups (job deletion); job_and_build_index
-- already serves that (and every other) query just as well since job is its leading column, so
-- job_index was pure write/storage overhead with no read benefit.
DROP INDEX job_index;
