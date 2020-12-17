ALTER TABLE caseresults
    ADD COLUMN stdout varchar(100000);
ALTER TABLE caseresults
    ADD COLUMN stderr varchar(100000);
ALTER TABLE caseresults
    ADD COLUMN stacktrace varchar(100000);
