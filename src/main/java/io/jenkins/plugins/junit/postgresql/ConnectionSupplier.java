package io.jenkins.plugins.junit.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
import org.jenkinsci.plugins.database.Database;
import org.jenkinsci.plugins.database.postgresql.PostgreSQLDatabase;

abstract class ConnectionSupplier { // TODO AutoCloseable

    private transient Connection connection;

    protected abstract PostgreSQLDatabase database();

    protected void initialize(Connection connection) throws SQLException {
    }

    synchronized Connection connection() throws SQLException {
        if (connection == null) {
            Connection _connection = database().getDataSource().getConnection();
            initialize(_connection);
            connection = _connection;
        }
        return connection;
    }

}
