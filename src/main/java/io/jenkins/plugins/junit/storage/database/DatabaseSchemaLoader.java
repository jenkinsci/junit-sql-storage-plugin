package io.jenkins.plugins.junit.storage.database;

import hudson.init.Initializer;
import io.jenkins.plugins.junit.storage.JunitTestResultStorageConfiguration;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.jenkinsci.plugins.database.Database;
import org.jenkinsci.plugins.database.GlobalDatabaseConfiguration;
import org.kohsuke.accmod.Restricted;
import org.kohsuke.accmod.restrictions.NoExternalUse;

import static hudson.init.InitMilestone.SYSTEM_CONFIG_ADAPTED;

@Restricted(NoExternalUse.class)
public class DatabaseSchemaLoader {
    
    static boolean MIGRATED; 
    
    @Initializer(after = SYSTEM_CONFIG_ADAPTED)
    public static void migrateSchema() throws SQLException {
        JunitTestResultStorageConfiguration configuration = JunitTestResultStorageConfiguration.get();
        if (configuration.getStorage() instanceof DatabaseTestResultStorage) {
            DatabaseTestResultStorage storage = (DatabaseTestResultStorage) configuration.getStorage();
            DataSource dataSource = storage.getConnectionSupplier().database().getDataSource();

            Database database = GlobalDatabaseConfiguration.get().getDatabase();

            assert database != null;
            String databaseDriverName = database.getClass().getName();
            String schemaLocation = "postgres";
            if (databaseDriverName.contains("mysql")) {
                schemaLocation = "mysql";
            }
            Flyway flyway = Flyway
                    .configure(DatabaseSchemaLoader.class.getClassLoader())
                    .baselineOnMigrate(true)
                    .table("junit_flyway_schema_history")
                    .dataSource(dataSource)
                    .locations("db/migration/" + schemaLocation)
                    .load();
            flyway.migrate();
            MIGRATED = true;
        }
    }
}
