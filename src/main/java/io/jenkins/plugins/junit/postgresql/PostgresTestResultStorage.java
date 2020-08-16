package io.jenkins.plugins.junit.postgresql;

import hudson.Extension;
import hudson.model.Run;
import hudson.tasks.junit.storage.TestResultImpl;
import hudson.tasks.junit.storage.TestResultStorage;
import java.io.IOException;
import java.sql.SQLException;
import org.kohsuke.accmod.Restricted;
import org.kohsuke.accmod.restrictions.NoExternalUse;

@Extension
@Restricted(NoExternalUse.class)
public class PostgresTestResultStorage implements TestResultStorage {

    private final ConnectionSupplier connectionSupplier = new LocalConnectionSupplier();

    @Override
    public RemotePublisher createRemotePublisher(Run<?, ?> build) throws IOException {
        try {
            connectionSupplier.connection(); // make sure we start a local server and create table first
        } catch (SQLException x) {
            throw new IOException(x);
        }
        return new PostgresRemotePublisher(build.getParent().getFullName(), build.getNumber());
    }

    @Override
    public TestResultImpl load(String job, int buildNumber) {
        return new PostgresqlPluggableStorage(job, buildNumber);
    }
}
