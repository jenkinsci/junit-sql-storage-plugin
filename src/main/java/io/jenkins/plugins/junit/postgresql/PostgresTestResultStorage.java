package io.jenkins.plugins.junit.postgresql;

import hudson.Extension;
import hudson.Util;
import hudson.model.Job;
import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.tasks.junit.CaseResult;
import hudson.tasks.junit.ClassResult;
import hudson.tasks.junit.PackageResult;
import hudson.tasks.junit.SuiteResult;
import hudson.tasks.junit.TestResult;
import hudson.tasks.junit.TestResultSummary;
import hudson.tasks.junit.TrendTestResultSummary;
import hudson.tasks.junit.storage.TestResultImpl;
import hudson.tasks.junit.storage.TestResultStorage;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import jenkins.model.Jenkins;
import org.jenkinsci.plugins.database.Database;
import org.jenkinsci.plugins.database.GlobalDatabaseConfiguration;
import org.jenkinsci.remoting.SerializableOnlyOverRemoting;

import static java.util.Collections.emptyList;

@Extension
public class PostgresTestResultStorage implements TestResultStorage {

    static final String CASE_RESULTS_TABLE = "caseResults";

    private final ConnectionSupplier connectionSupplier = new LocalConnectionSupplier();

    @Override public RemotePublisher createRemotePublisher(Run<?, ?> build) throws IOException {
        try {
            connectionSupplier.connection(); // make sure we start a local server and create table first
        } catch (SQLException x) {
            throw new IOException(x);
        }
        return new RemotePublisherImpl(build.getParent().getFullName(), build.getNumber());
    }

    @FunctionalInterface
    private interface Querier<T> {
        T run(Connection connection) throws SQLException;
    }
    @Override public TestResultImpl load(String job, int build) {
        return new TestResultImpl() {
            private <T> T query(Querier<T> querier, T dflt) {
                try {
                    Connection connection = connectionSupplier.connection();
                    return querier.run(connection);
                } catch (SQLException x) {
                    throw new RuntimeException(x);
                }
            }
            private int getCaseCount(String and) {
                return query(connection -> {
                    try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM " + CASE_RESULTS_TABLE + " WHERE job = ? AND build = ?" + and)) {
                        statement.setString(1, job);
                        statement.setInt(2, build);
                        try (ResultSet result = statement.executeQuery()) {
                            result.next();
                            int anInt = result.getInt(1);
                            return anInt;
                        }
                    }
                }, 0);
            }

            private List<CaseResult> retrieveCaseResult(String whereCondition) {
                return query(connection -> {
                    try (PreparedStatement statement = connection.prepareStatement("SELECT suite, package, testname, classname, errordetails, skipped FROM " + PostgresTestResultStorage.CASE_RESULTS_TABLE + " WHERE job = ? AND build = ? AND " + whereCondition)) {
                        statement.setString(1, job);
                        statement.setInt(2, build);
                        try (ResultSet result = statement.executeQuery()) {

                            List<CaseResult> results = new ArrayList<>();
                            while (result.next()) {
                                String testName = result.getString("testname");
                                String packageName = result.getString("package");
                                String errorDetails = result.getString("errordetails");
                                String suite = result.getString("suite");
                                String className = result.getString("classname");
                                String skipped = result.getString("skipped");

                                SuiteResult suiteResult = new SuiteResult(suite, null, null, null);
                                suiteResult.setParent(new TestResult(this));
                                CaseResult caseResult = new CaseResult(suiteResult, className, testName, errorDetails, skipped);
                                caseResult.setClass(new ClassResult(new PackageResult(new TestResult(this), packageName), className));
                                results.add(caseResult);
                            }
                            return results;
                        }
                    }
                }, emptyList());
            }

            @Override
            public List<PackageResult> getAllPackageResults() {
                return query(connection -> {
                    try (PreparedStatement statement = connection.prepareStatement("SELECT DISTINCT package FROM " + PostgresTestResultStorage.CASE_RESULTS_TABLE + " WHERE job = ? AND build = ?")) {
                        statement.setString(1, job);
                        statement.setInt(2, build);
                        try (ResultSet result = statement.executeQuery()) {

                            List<PackageResult> results = new ArrayList<>();
                            while (result.next()) {
                                String packageName = result.getString("package");

                                results.add(new PackageResult(new TestResult(this), packageName));
                            }
                            return results;
                        }
                    }
                }, emptyList());
            }

            @Override
            public List<TrendTestResultSummary> getTrendTestResultSummary() {
                return query(connection -> {
                    try (PreparedStatement statement = connection.prepareStatement("SELECT build, sum(case when errorDetails is not null then 1 else 0 end) as failCount, sum(case when skipped is not null then 1 else 0 end) as skipCount, sum(case when errorDetails is null and skipped is null then 1 else 0 end) as passCount FROM " +  PostgresTestResultStorage.CASE_RESULTS_TABLE +  " WHERE job = ? group by build;")) {
                        statement.setString(1, job);
                        try (ResultSet result = statement.executeQuery()) {

                            List<TrendTestResultSummary> trendTestResultSummaries = new ArrayList<>();
                            while (result.next()) {
                                int buildNumber = result.getInt("build");
                                int passed = result.getInt("passCount");
                                int failed = result.getInt("failCount");
                                int skipped = result.getInt("skipCount");
                                int total = passed + failed + skipped;

                                trendTestResultSummaries.add(new TrendTestResultSummary(buildNumber, new TestResultSummary(failed, skipped, passed, total)));
                            }
                            return trendTestResultSummaries;
                        }
                    }
                }, emptyList());
            }

            @Override
            public PackageResult getPackageResult(String packageName) {
                return new PackageResult(new TestResult(this), packageName);
            }

            @Override
            public ClassResult getClassResult(String name) {
                return query(connection -> {
                    try (PreparedStatement statement = connection.prepareStatement("SELECT package, classname FROM " + PostgresTestResultStorage.CASE_RESULTS_TABLE + " WHERE job = ? AND build = ? AND classname = ?")) {
                        statement.setString(1, job);
                        statement.setInt(2, build);
                        statement.setString(3, name);
                        try (ResultSet result = statement.executeQuery()) {

                            if (result.next()) {
                                String packageName = result.getString("package");
                                String className = result.getString("classname");

                                PackageResult packageResult = new PackageResult(new TestResult(this), packageName);
                                return new ClassResult(packageResult, className);
                            }
                            return null;
                        }
                    }
                }, null);

            }

            @Override
            public Run<?, ?> getFailedSinceRun(CaseResult caseResult) {
                return query(connection -> {
                    int lastPassingBuildNumber;
                    Job<?, ?> theJob = Objects.requireNonNull(Jenkins.get().getItemByFullName(job, Job.class));
                    try (PreparedStatement statement = connection.prepareStatement(
                            "SELECT build " +
                                    "FROM " + PostgresTestResultStorage.CASE_RESULTS_TABLE + " " +
                                    "WHERE job = ? " +
                                    "AND build < ? " +
                                    "AND suite = ? " +
                                    "AND package = ? " +
                                    "AND classname = ? " +
                                    "AND testname = ? " +
                                    "AND errordetails IS NULL " +
                                    "ORDER BY BUILD DESC " +
                                    "LIMIT 1"
                    )) {
                        statement.setString(1, job);
                        statement.setInt(2, build);
                        statement.setString(3, caseResult.getSuiteResult().getName());
                        statement.setString(4, caseResult.getPackageName());
                        statement.setString(5, caseResult.getClassName());
                        statement.setString(6, caseResult.getName());
                        try (ResultSet result = statement.executeQuery()) {
                            boolean hasPassed = result.next();
                            if (!hasPassed) {
                                return theJob.getBuildByNumber(1);
                            }

                            lastPassingBuildNumber = result.getInt("build");
                        }
                    }
                    try (PreparedStatement statement = connection.prepareStatement(
                            "SELECT build " +
                                    "FROM " + PostgresTestResultStorage.CASE_RESULTS_TABLE + " " +
                                    "WHERE job = ? " +
                                    "AND build > ? " +
                                    "AND suite = ? " +
                                    "AND package = ? " +
                                    "AND classname = ? " +
                                    "AND testname = ? " +
                                    "AND errordetails is NOT NULL " +
                                    "ORDER BY BUILD ASC " +
                                    "LIMIT 1"
                    )
                    ) {
                        statement.setString(1, job);
                        statement.setInt(2, lastPassingBuildNumber);
                        statement.setString(3, caseResult.getSuiteResult().getName());
                        statement.setString(4, caseResult.getPackageName());
                        statement.setString(5, caseResult.getClassName());
                        statement.setString(6, caseResult.getName());

                        try (ResultSet result = statement.executeQuery()) {
                            result.next();

                            int firstFailingBuildAfterPassing = result.getInt("build");
                            return theJob.getBuildByNumber(firstFailingBuildAfterPassing);
                        }
                    }
                }, null);

            }

            @Override
            public List<CaseResult> getFailedTestsByPackage(String packageName) {
                return getByPackage(packageName, "AND errorDetails IS NOT NULL");
            }

            private List<CaseResult> getByPackage(String packageName, String filter) {
                return query(connection -> {
                    try (PreparedStatement statement = connection.prepareStatement("SELECT suite, testname, classname, errordetails, skipped FROM " + PostgresTestResultStorage.CASE_RESULTS_TABLE + " WHERE job = ? AND build = ? AND package = ? " + filter)) {
                        statement.setString(1, job);
                        statement.setInt(2, build);
                        statement.setString(3, packageName);
                        try (ResultSet result = statement.executeQuery()) {

                            List<CaseResult> results = new ArrayList<>();
                            while (result.next()) {
                                String testName = result.getString("testname");
                                String errorDetails = result.getString("errordetails");
                                String suite = result.getString("suite");
                                String className = result.getString("classname");
                                String skipped = result.getString("skipped");

                                SuiteResult suiteResult = new SuiteResult(suite, null, null, null);
                                suiteResult.setParent(new TestResult(this));
                                results.add(new CaseResult(suiteResult, className, testName, errorDetails, skipped));
                            }
                            return results;
                        }
                    }
                }, emptyList());
            }


            private List<CaseResult> getCaseResults(String column) {
                return retrieveCaseResult(column + " IS NOT NULL");
            }

            @Override
            public CaseResult getCaseResult(String testName) {
                return query(connection -> {
                    try (PreparedStatement statement = connection.prepareStatement("SELECT suite, testname, package, classname, errordetails, skipped FROM " + PostgresTestResultStorage.CASE_RESULTS_TABLE + " WHERE job = ? AND build = ? AND testname = ?")) {
                        statement.setString(1, job);
                        statement.setInt(2, build);
                        statement.setString(3, testName);
                        try (ResultSet result = statement.executeQuery()) {

                            CaseResult caseResult = null;
                            if (result.next()) {
                                String resultTestName = result.getString("testname");
                                String errorDetails = result.getString("errordetails");
                                String packageName = result.getString("package");
                                String suite = result.getString("suite");
                                String className = result.getString("classname");
                                String skipped = result.getString("skipped");

                                SuiteResult suiteResult = new SuiteResult(suite, null, null, null);
                                suiteResult.setParent(new TestResult(this));
                                caseResult = new CaseResult(suiteResult, className, resultTestName, errorDetails, skipped);
                                caseResult.setClass(new ClassResult(new PackageResult(new TestResult(this), packageName), className));
                            }
                            return caseResult;
                        }
                    }
                }, null);


            }

            @Override public int getFailCount() {
                int caseCount = getCaseCount(" AND errorDetails IS NOT NULL");
                return caseCount;
            }
            @Override public int getSkipCount() {
                int caseCount = getCaseCount(" AND skipped IS NOT NULL");
                return caseCount;
            }
            @Override public int getPassCount() {
                int caseCount = getCaseCount(" AND errorDetails IS NULL AND skipped IS NULL");
                return caseCount;
            }
            @Override public int getTotalCount() {
                int caseCount = getCaseCount("");
                return caseCount;
            }

            @Override
            public List<CaseResult> getFailedTests() {
                List<CaseResult> errordetails = getCaseResults("errordetails");
                return errordetails;
            }

            @Override
            public List<CaseResult> getSkippedTests() {
                List<CaseResult> errordetails = getCaseResults("skipped");
                return errordetails;
            }

            @Override
            public List<CaseResult> getSkippedTestsByPackage(String packageName) {
                return getByPackage(packageName, "AND skipped IS NOT NULL");
            }

            @Override
            public List<CaseResult> getPassedTests() {
                List<CaseResult> errordetails = retrieveCaseResult("errordetails IS NULL AND skipped IS NULL");
                return errordetails;
            }

            @Override
            public List<CaseResult> getPassedTestsByPackage(String packageName) {
                return getByPackage(packageName, "AND errordetails IS NULL AND skipped IS NULL");
            }

            @Override public TestResult getResultByNodes(List<String> nodeIds) {
                return new TestResult(this); // TODO
            }
        };
    }

    private static class RemotePublisherImpl implements RemotePublisher {

        private final String job;
        private final int build;
        // TODO keep the same supplier and thus Connection open across builds, so long as the database config remains unchanged
        private final ConnectionSupplier connectionSupplier;

        RemotePublisherImpl(String job, int build) {
            this.job = job;
            this.build = build;
            connectionSupplier = new RemoteConnectionSupplier();
        }

        @Override public void publish(TestResult result, TaskListener listener) throws IOException {
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

    static abstract class ConnectionSupplier { // TODO AutoCloseable

        private transient Connection connection;

        protected abstract Database database();

        protected void initialize(Connection connection) throws SQLException {}

        synchronized Connection connection() throws SQLException {
            if (connection == null) {
                Connection _connection = database().getDataSource().getConnection();
                initialize(_connection);
                connection = _connection;
            }
            return connection;
        }

    }

    static class LocalConnectionSupplier extends ConnectionSupplier {

        @Override protected Database database() {
            return GlobalDatabaseConfiguration.get().getDatabase();
        }

        @Override protected void initialize(Connection connection) throws SQLException {
            boolean exists = false;
            try (ResultSet rs = connection.getMetaData().getTables(null, null, CASE_RESULTS_TABLE, new String[] {"TABLE"})) {
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

    /**
     * Ensures a {@link org.jenkinsci.plugins.database.postgresql.PostgreSQLDatabase} configuration can be sent to an agent.
     */
    static class RemoteConnectionSupplier extends ConnectionSupplier implements SerializableOnlyOverRemoting {

//        private static final XStream XSTREAM = new XStream();
//        private final String databaseXml;
//
//        RemoteConnectionSupplier() {
//            databaseXml = XSTREAM.toXML(GlobalDatabaseConfiguration.get().getDatabase());
//        }
//
//        @Override
//        protected PostgreSQLDatabase database() {
//            return (PostgreSQLDatabase) XSTREAM.fromXML(databaseXml);
//        }
//        
        private final Database database;

        RemoteConnectionSupplier() {
            database = GlobalDatabaseConfiguration.get().getDatabase();
        }

        @Override protected Database database() {
            return database;
        }
    }

}

