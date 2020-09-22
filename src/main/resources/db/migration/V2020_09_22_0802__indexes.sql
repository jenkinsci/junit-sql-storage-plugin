CREATE INDEX job_index ON caseresults(job);
CREATE INDEX job_and_build_index ON caseresults(job, build);

-- possibly worth adding indexes on the package and on the suite but let's see how this goes
-- any feedback or index contribution welcome
-- and we can remove them if they aren't useful
