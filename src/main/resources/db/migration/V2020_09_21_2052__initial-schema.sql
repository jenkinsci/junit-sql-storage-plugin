CREATE TABLE caseResults(
    job varchar(255), 
    build int, 
    suite varchar(255), 
    package varchar(255), 
    className varchar(255), 
    testName varchar(255), 
    errorDetails varchar(255), 
    skipped varchar(255), 
    duration numeric
);
