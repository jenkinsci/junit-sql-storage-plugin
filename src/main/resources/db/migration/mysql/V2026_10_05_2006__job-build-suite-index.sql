-- Supports narrow, suite-scoped lookups of a single build's cases (used e.g. by CaseResult's
-- previous-result/failure-age walk across historical builds, see getSuite(String) in
-- DatabaseTestResultStorage) without having to heap-fetch every row of the build first: without
-- this index, "... WHERE job = ? AND build = ? AND suite = ?" still has to read every row matching
-- job/build from the job_and_build_index before filtering by suite, which costs nearly as much as
-- loading the whole build.
CREATE INDEX job_build_and_suite_index ON caseResults(job, build, suite);
