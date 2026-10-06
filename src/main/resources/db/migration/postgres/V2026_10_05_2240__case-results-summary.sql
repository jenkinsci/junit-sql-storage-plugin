-- Persisted per-build aggregate, maintained incrementally at publish time (see
-- RemotePublisherImpl#doPublish / #upsertSummary in DatabaseTestResultStorage), so that
-- trend/duration/history/count queries can read one small row per build instead of aggregating
-- every row in caseresults for the job on every request. This matters most for jobs with very
-- large history (hundreds to thousands of builds), where the previous GROUP BY-over-caseresults
-- queries had to scan/aggregate the whole job's row set (or, for getHistorySummary's
-- LIMIT/OFFSET pagination, everything up to the current page) on every page load.
CREATE TABLE caseResultsSummary(
    job varchar(255) NOT NULL,
    build int NOT NULL,
    passCount int NOT NULL,
    failCount int NOT NULL,
    skipCount int NOT NULL,
    duration double precision NOT NULL,
    PRIMARY KEY (job, build)
);

-- One-time backfill so existing installs' history/trend pages benefit immediately rather than
-- only for builds published after this migration runs. This is a single aggregate pass over
-- caseresults, grouped by (job, build); it is read-only against caseresults and, on a large
-- existing table, its cost/duration scales with the existing row count, same as the queries it is
-- replacing used to cost on every single page load.
INSERT INTO caseResultsSummary (job, build, passCount, failCount, skipCount, duration)
SELECT job,
       build,
       sum(case when errorDetails is null and skipped is null then 1 else 0 end) as passCount,
       sum(case when errorDetails is not null then 1 else 0 end) as failCount,
       sum(case when skipped is not null then 1 else 0 end) as skipCount,
       sum(duration) as duration
FROM caseResults
GROUP BY job, build;
