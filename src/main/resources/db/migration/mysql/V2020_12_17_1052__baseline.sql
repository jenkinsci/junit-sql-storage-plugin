CREATE TABLE caseResults
(
    job          varchar(255),
    build        int,
    suite        varchar(255),
    package      varchar(255),
    className    varchar(255),
    testName     varchar(500),
    stdout       TEXT(100000),
    stderr       TEXT(100000),
    stacktrace   TEXT(100000),
    errorDetails TEXT(100000),
    skipped      varchar(1000),
    duration     float,
    timestamp    timestamp NOT NULL default NOW()
);

CREATE
INDEX job_index ON caseResults(job);

CREATE
INDEX job_and_build_index ON caseResults(job, build);
