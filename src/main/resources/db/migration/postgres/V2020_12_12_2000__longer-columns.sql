ALTER TABLE caseresults
    ALTER COLUMN errorDetails type varchar(100000);
ALTER TABLE caseresults
    ALTER COLUMN testName type varchar(500);
ALTER TABLE caseresults
    ALTER COLUMN skipped type varchar(1000);
