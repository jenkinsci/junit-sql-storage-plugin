package io.jenkins.plugins.junit.storage.database;

import hudson.init.Initializer;
import io.jenkins.plugins.junit.storage.JunitTestResultStorageConfiguration;
import org.flywaydb.core.Flyway;
import org.jenkinsci.plugins.database.Database;
import org.jenkinsci.plugins.database.GlobalDatabaseConfiguration;
import org.kohsuke.accmod.Restricted;
import org.kohsuke.accmod.restrictions.NoExternalUse;

import javax.sql.DataSource;
import java.util.logging.Level;
import java.util.logging.Logger;

import static hudson.init.InitMilestone.SYSTEM_CONFIG_ADAPTED;

@Restricted(NoExternalUse.class)
public class DatabaseSchemaLoader {

    private static final Logger LOGGER = Logger.getLogger(DatabaseSchemaLoader.class.getName());

    static boolean MIGRATED; 
    
    @Initializer(after = SYSTEM_CONFIG_ADAPTED)
    public static void migrateSchema() {
        JunitTestResultStorageConfiguration configuration = JunitTestResultStorageConfiguration.get();
        if (configuration.getStorage() instanceof DatabaseTestResultStorage) {
            try {
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
            } catch (Exception e) {
                // TODO add admin monitor
                LOGGER.log(Level.SEVERE, "Error migrating database, correct this error before using the junit plugin", e);
            }
        }
    }
}
