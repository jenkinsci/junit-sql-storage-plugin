package io.jenkins.plugins.junit.postgresql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.jenkinsci.plugins.database.Database;
import org.jenkinsci.plugins.database.GlobalDatabaseConfiguration;
import org.jenkinsci.plugins.database.postgresql.PostgreSQLDatabase;

import static io.jenkins.plugins.junit.postgresql.PostgresqlPluggableStorage.CASE_RESULTS_TABLE;

class LocalConnectionSupplier extends ConnectionSupplier {

    @Override
    protected PostgreSQLDatabase database() {
        return (PostgreSQLDatabase) GlobalDatabaseConfiguration.get().getDatabase();
    }

    @Override
    protected void initialize(Connection connection) throws SQLException {
        boolean exists = false;
        try (ResultSet rs = connection.getMetaData().getTables(null, null, CASE_RESULTS_TABLE, new String[]{"TABLE"})) {
            while (rs.next()) {
                if (rs.getString("TABLE_NAME").equalsIgnoreCase(CASE_RESULTS_TABLE)) {
                    exists = true;
                    break;
                }
            }
        }
        if (!exists) {
            try (Statement statement = connection.createStatement()) {
                // TODO this and joined tables: errorStackTrace, stdout, stderr, duration, nodeId, enclosingBlocks, enclosingBlockNames, etc.
                statement.execute("CREATE TABLE IF NOT EXISTS " + CASE_RESULTS_TABLE + "(job varchar(255), build int, suite varchar(255), package varchar(255), className varchar(255), testName varchar(255), errorDetails varchar(255), skipped varchar(255))");
                // TODO indices
            }
        }
    }

}
