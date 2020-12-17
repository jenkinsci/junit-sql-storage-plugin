ALTER TABLE caseresults
    ADD COLUMN timestamp timestamp NOT NULL DEFAULT NOW()