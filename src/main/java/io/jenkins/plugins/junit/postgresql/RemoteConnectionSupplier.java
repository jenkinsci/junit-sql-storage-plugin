package io.jenkins.plugins.junit.postgresql;

import com.thoughtworks.xstream.XStream;
import org.jenkinsci.plugins.database.Database;
import org.jenkinsci.plugins.database.GlobalDatabaseConfiguration;
import org.jenkinsci.plugins.database.postgresql.PostgreSQLDatabase;
import org.jenkinsci.remoting.SerializableOnlyOverRemoting;

public class RemoteConnectionSupplier extends ConnectionSupplier implements SerializableOnlyOverRemoting {

    private static final XStream XSTREAM = new XStream();
    private final String databaseXml;

    RemoteConnectionSupplier() {
        databaseXml = XSTREAM.toXML(GlobalDatabaseConfiguration.get().getDatabase());
    }

    @Override
    protected PostgreSQLDatabase database() {
        return (PostgreSQLDatabase) XSTREAM.fromXML(databaseXml);
    }

}
