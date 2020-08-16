package io.jenkins.plugins.junit.postgresql;

import hudson.Util;
import hudson.model.TaskListener;
import hudson.tasks.junit.CaseResult;
import hudson.tasks.junit.SuiteResult;
import hudson.tasks.junit.TestResult;
import hudson.tasks.junit.storage.TestResultStorage;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import static io.jenkins.plugins.junit.postgresql.PostgresqlPluggableStorage.CASE_RESULTS_TABLE;

class PostgresRemotePublisher implements TestResultStorage.RemotePublisher {
    private final String job;
    private final int build;
    // TODO keep the same supplier and thus Connection open across builds, so long as the database config remains unchanged
    private final ConnectionSupplier connectionSupplier;

    PostgresRemotePublisher(String job, int build) {
        this.job = job;
        this.build = build;
        connectionSupplier = new RemoteConnectionSupplier();
    }

    @Override
    public void publish(TestResult result, TaskListener listener) throws IOException {
        try {
            Connection connection = connectionSupplier.connection();
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO " + CASE_RESULTS_TABLE + " (job, build, suite, package, className, testName, errorDetails, skipped) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
                int count = 0;
                for (SuiteResult suiteResult : result.getSuites()) {
                    for (CaseResult caseResult : suiteResult.getCases()) {
                        statement.setString(1, job);
                        statement.setInt(2, build);
                        statement.setString(3, suiteResult.getName());
                        statement.setString(4, caseResult.getPackageName());
                        statement.setString(5, caseResult.getClassName());
                        statement.setString(6, caseResult.getName());
                        String errorDetails = caseResult.getErrorDetails();
                        if (errorDetails != null) {
                            statement.setString(7, errorDetails);
                        } else {
                            statement.setNull(7, Types.VARCHAR);
                        }
                        if (caseResult.isSkipped()) {
                            statement.setString(8, Util.fixNull(caseResult.getSkippedMessage()));
                        } else {
                            statement.setNull(8, Types.VARCHAR);
                        }
                        statement.executeUpdate();
                        count++;
                    }
                }
                listener.getLogger().printf("Saved %d test cases into database.%n", count);
            }
        } catch (SQLException x) {
            throw new IOException(x);
        }
    }


}
