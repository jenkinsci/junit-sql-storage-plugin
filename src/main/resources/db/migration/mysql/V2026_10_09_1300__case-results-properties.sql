-- Adds a column to persist JUnit test case properties (see JUnitParser's "keepProperties" option,
-- https://github.com/jenkinsci/junit-plugin/pull/546), so that CaseResult#getProperties() survives
-- a round trip through the SQL storage backend instead of always coming back empty.
--
-- Stored as a JSON object (property name -> value), matching how the other free-form text columns
-- (stdout/stderr/stacktrace) are sized, and nullable since most builds won't have any properties.
ALTER TABLE caseResults
    ADD COLUMN properties TEXT(100000);
