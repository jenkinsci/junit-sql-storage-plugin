package io.jenkins.plugins.junit.storage.database;

import edu.umd.cs.findbugs.annotations.CheckForNull;
import edu.umd.cs.findbugs.annotations.NonNull;
import hudson.Extension;
import hudson.Util;
import hudson.model.Job;
import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.tasks.junit.CaseResult;
import hudson.tasks.junit.ClassResult;
import hudson.tasks.junit.PackageResult;
import hudson.tasks.junit.SuiteResult;
import hudson.tasks.junit.TestDurationResultSummary;
import hudson.tasks.junit.TestResult;
import hudson.tasks.junit.TestResultSummary;
import hudson.tasks.junit.TrendTestResultSummary;
import hudson.tasks.junit.HistoryTestResultSummary;
import io.jenkins.plugins.junit.storage.JunitTestResultStorage;
import io.jenkins.plugins.junit.storage.JunitTestResultStorageDescriptor;
import io.jenkins.plugins.junit.storage.TestResultImpl;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

import jenkins.model.Jenkins;
import org.apache.commons.lang3.StringUtils;
import org.jenkinsci.Symbol;
import org.jenkinsci.plugins.database.Database;
import org.jenkinsci.plugins.database.GlobalDatabaseConfiguration;
import org.jenkinsci.remoting.SerializableOnlyOverRemoting;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.DataBoundSetter;

@Extension
public class DatabaseTestResultStorage extends JunitTestResultStorage {

    static final int MAX_JOB_LENGTH = 255;
    static final int MAX_SUITE_LENGTH = 255;
    static final int MAX_PACKAGE_LENGTH = 255;
    static final int MAX_CLASSNAME_LENGTH =255;
    static final int MAX_TEST_NAME_LENGTH =500;
    static final int MAX_STDOUT_LENGTH =100000;
    static final int MAX_STDERR_LENGTH =100000;
    static final int MAX_STACK_TRACE_LENGTH =100000;
    static final int MAX_ERROR_DETAILS_LENGTH =100000;
    static final int MAX_SKIPPED_LENGTH =1000;

    private transient ConnectionSupplier connectionSupplier;

    private boolean skipCleanupRunsOnDeletion;

    @DataBoundConstructor
    public DatabaseTestResultStorage() {}

    public ConnectionSupplier getConnectionSupplier() {
        if (connectionSupplier == null) {
            connectionSupplier = new LocalConnectionSupplier();
        }
        return connectionSupplier;
    }

    public boolean isSkipCleanupRunsOnDeletion() {
        return skipCleanupRunsOnDeletion;
    }

    @DataBoundSetter
    public void setSkipCleanupRunsOnDeletion(boolean skipCleanupRunsOnDeletion) {
        this.skipCleanupRunsOnDeletion = skipCleanupRunsOnDeletion;
    }

    @Override public RemotePublisher createRemotePublisher(Run<?, ?> build) throws IOException {
        try {
            getConnectionSupplier().connection(); // make sure we start a local server and create table first
        } catch (SQLException x) {
            throw new IOException(x);
        }
        return new RemotePublisherImpl(build.getParent().getFullName(), build.getNumber());
    }

    @Extension
    @Symbol("database")
    public static class DescriptorImpl extends JunitTestResultStorageDescriptor {

        @NonNull
        @Override
        public String getDisplayName() {
            return Messages.DatabaseTestResultStorage_displayName();
        }
    }

    @FunctionalInterface
    private interface Querier<T> {
        T run(Connection connection) throws SQLException;
    }
    @Override public TestResultImpl load(String job, int build) {
        return new TestResultStorage(job, build);
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
                try (Connection connection = connectionSupplier.connection(); PreparedStatement statement = connection.prepareStatement("INSERT INTO caseResults (job, build, suite, package, className, testName, errorDetails, skipped, duration, stdout, stderr, stacktrace) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                    int count = 0;
                    for (SuiteResult suiteResult : result.getSuites()) {
                        for (CaseResult caseResult : suiteResult.getCases()) {
                            statement.setString(1, StringUtils.truncate(job, MAX_JOB_LENGTH));
                            statement.setInt(2, build);
                            statement.setString(3, StringUtils.truncate(suiteResult.getName(), MAX_SUITE_LENGTH));
                            statement.setString(4, StringUtils.truncate(caseResult.getPackageName(), MAX_PACKAGE_LENGTH));
                            statement.setString(5, StringUtils.truncate(caseResult.getClassName(), MAX_CLASSNAME_LENGTH));
                            statement.setString(6, StringUtils.truncate(caseResult.getName(), MAX_TEST_NAME_LENGTH));
                            String errorDetails = caseResult.getErrorDetails();
                            if (errorDetails != null) {
                                errorDetails = StringUtils.truncate(errorDetails, MAX_ERROR_DETAILS_LENGTH);
                                statement.setString(7, errorDetails);
                            } else {
                                statement.setNull(7, Types.VARCHAR);
                            }
                            if (caseResult.isSkipped()) {
                                statement.setString(8, StringUtils.truncate(Util.fixNull(caseResult.getSkippedMessage()), MAX_SKIPPED_LENGTH));
                            } else {
                                statement.setNull(8, Types.VARCHAR);
                            }
                            statement.setFloat(9, caseResult.getDuration());
                            if (StringUtils.isNotEmpty(caseResult.getStdout())) {
                                statement.setString(10, StringUtils.truncate(caseResult.getStdout(), MAX_STDOUT_LENGTH));
                            } else {
                                statement.setNull(10, Types.VARCHAR);
                            }
                            if (StringUtils.isNotEmpty(caseResult.getStderr())) {
                                statement.setString(11, StringUtils.truncate(caseResult.getStderr(), MAX_STDERR_LENGTH));
                            } else {
                                statement.setNull(11, Types.VARCHAR);
                            }
                            if (StringUtils.isNotEmpty(caseResult.getErrorStackTrace())) {
                                statement.setString(12, StringUtils.truncate(caseResult.getErrorStackTrace(), MAX_STACK_TRACE_LENGTH));
                            } else {
                                statement.setNull(12, Types.VARCHAR);
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

    static abstract class ConnectionSupplier implements AutoCloseable {

        private transient Connection connection;

        protected abstract Database database();

        protected void initialize(Connection connection) throws SQLException {}

        synchronized Connection connection() throws SQLException {
            if (connection == null || connection.isClosed()) {
                Connection _connection = database().getDataSource().getConnection();
                initialize(_connection);
                connection = _connection;
            }
            return connection;
        }

        @Override
        public void close() {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            connection = null;
        }

    }

    static class LocalConnectionSupplier extends ConnectionSupplier {

        @Override protected Database database() {
            return GlobalDatabaseConfiguration.get().getDatabase();
        }

        @Override protected void initialize(Connection connection) throws SQLException {
            if (!DatabaseSchemaLoader.MIGRATED) {
                DatabaseSchemaLoader.migrateSchema();
            }
        }

    }

    /**
     * Ensures a Database configuration can be sent to an agent.
     */
    static class RemoteConnectionSupplier extends ConnectionSupplier implements SerializableOnlyOverRemoting {

        private final Database database;

        RemoteConnectionSupplier() {
            database = GlobalDatabaseConfiguration.get().getDatabase();
        }

        @Override protected Database database() {
            return database;
        }
    }

    public final class TestResultStorage implements TestResultImpl {
        private final String job;
        private final int build;

        public TestResultStorage(String job, int build) {
            this.job = job;
            this.build = build;
        }

        private <T> T query(Querier<T> querier) {
            try {
                // TODO move to try-with-resources, whenever I try close this I get (multiple queries needed):
                // org.postgresql.util.PSQLException: This statement has been closed.
                Connection connection = getConnectionSupplier().connection();
                return querier.run(connection);
            } catch (SQLException x) {
                throw new RuntimeException(x);
            }
        }

        private int getCaseCount(String and) {
            return query(connection -> {
                try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM caseResults WHERE job = ? AND build = ?" + and)) {
                    statement.setString(1, job);
                    statement.setInt(2, build);
                    try (ResultSet result = statement.executeQuery()) {
                        result.next();
                        int anInt = result.getInt(1);
                        return anInt;
                    }
                }
            });
        }

        private List<CaseResult> retrieveCaseResult(String whereCondition) {
            return query(connection -> {
                try (PreparedStatement statement = connection.prepareStatement("SELECT suite, package, testname, classname, errordetails, skipped, duration, stdout, stderr, stacktrace FROM caseResults WHERE job = ? AND build = ? AND " + whereCondition)) {
                    statement.setString(1, job);
                    statement.setInt(2, build);
                    try (ResultSet result = statement.executeQuery()) {

                        List<CaseResult> results = new ArrayList<>();
                        Map<String, ClassResult> classResults = new HashMap<>();
                        TestResult parent = new TestResult(this);
                        while (result.next()) {
                            String testName = result.getString("testname");
                            String packageName = result.getString("package");
                            String errorDetails = result.getString("errordetails");
                            String suite = result.getString("suite");
                            String className = result.getString("classname");
                            String skipped = result.getString("skipped");
                            String stdout = result.getString("stdout");
                            String stderr = result.getString("stderr");
                            String stacktrace = result.getString("stacktrace");
                            float duration = result.getFloat("duration");

                            SuiteResult suiteResult = new SuiteResult(suite, null, null, null);
                            suiteResult.setParent(parent);
                            CaseResult caseResult = new CaseResult(suiteResult, className, testName, errorDetails, skipped, duration, stdout, stderr, stacktrace);
                            ClassResult classResult = classResults.get(className);
                            if (classResult == null) {
                                classResult = new ClassResult(new PackageResult(new TestResult(this), packageName), className);
                            }
                            classResult.add(caseResult);
                            caseResult.setClass(classResult);
                            classResults.put(className, classResult);
                            results.add(caseResult);
                        }
                        classResults.values().forEach(ClassResult::tally);
                        return results;
                    }
                }
            });
        }


        public Void deleteRun() {
            return query(connection -> {
                try (PreparedStatement statement = connection.prepareStatement(
                        "DELETE FROM caseResults WHERE job = ? AND build = ?"
                )) {
                    statement.setString(1, job);
                    statement.setInt(2, build);

                    statement.execute();
                }
                return null;
            });
        }

        public Void deleteJob() {
            return query(connection -> {
                try (PreparedStatement statement = connection.prepareStatement(
                        "DELETE FROM caseResults WHERE job = ?"
                )) {
                    statement.setString(1, job);

                    statement.execute();
                }
                return null;
            });
        }

        @Override
        public List<PackageResult> getAllPackageResults() {
            return query(connection -> {
                try (PreparedStatement statement = connection.prepareStatement("SELECT suite, testname, package, classname, errordetails, skipped, duration, stdout, stderr, stacktrace FROM caseResults WHERE job = ? AND build = ?")) {
                    statement.setString(1, job);
                    statement.setInt(2, build);
                    try (ResultSet result = statement.executeQuery()) {

                        Map<String, PackageResult> results = new HashMap<>();
                        Map<String, ClassResult> classResults = new HashMap<>();
                        TestResult parent = new TestResult(this);
                        while (result.next()) {
                            String testName = result.getString("testname");
                            String packageName = result.getString("package");
                            String errorDetails = result.getString("errordetails");
                            String suite = result.getString("suite");
                            String className = result.getString("classname");
                            String skipped = result.getString("skipped");
                            String stdout = result.getString("stdout");
                            String stderr = result.getString("stderr");
                            String stacktrace = result.getString("stacktrace");
                            float duration = result.getFloat("duration");

                            SuiteResult suiteResult = new SuiteResult(suite, null, null, null);
                            suiteResult.setParent(parent);
                            CaseResult caseResult = new CaseResult(suiteResult, className, testName, errorDetails, skipped, duration, stdout, stderr, stacktrace);
                            PackageResult packageResult = results.get(packageName);
                            if (packageResult == null) {
                                packageResult = new PackageResult(parent, packageName);
                            }
                            ClassResult classResult = classResults.get(className);
                            if (classResult == null) {
                                classResult = new ClassResult(new PackageResult(parent, packageName), className);
                            }
                            caseResult.setClass(classResult);
                            classResult.add(caseResult);

                            classResults.put(className, classResult);
                            packageResult.add(caseResult);

                            results.put(packageName, packageResult);
                        }
                        classResults.values().forEach(ClassResult::tally);
                        final List<PackageResult> resultList = new ArrayList<>(results.values());
                        resultList.forEach((PackageResult::tally));
                        resultList.sort(Comparator.comparing(PackageResult::getName, String::compareTo));
                        return resultList;
                    }
                }
            });
        }

        @Override
        public List<TrendTestResultSummary> getTrendTestResultSummary() {
            return query(connection -> {
                try (PreparedStatement statement = connection.prepareStatement("SELECT build, sum(case when errorDetails is not null then 1 else 0 end) as failCount, sum(case when skipped is not null then 1 else 0 end) as skipCount, sum(case when errorDetails is null and skipped is null then 1 else 0 end) as passCount FROM caseResults" +  " WHERE job = ? group by build order by build;")) {
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
            });
        }

        @Override
        public List<TestDurationResultSummary> getTestDurationResultSummary() {
            return query(connection -> {
                try (PreparedStatement statement = connection.prepareStatement("SELECT build, sum(duration) as duration FROM caseResults" +  " WHERE job = ? group by build order by build;")) {
                    statement.setString(1, job);
                    try (ResultSet result = statement.executeQuery()) {

                        List<TestDurationResultSummary> testDurationResultSummaries = new ArrayList<>();
                        while (result.next()) {
                            int buildNumber = result.getInt("build");
                            int duration = result.getInt("duration");

                            testDurationResultSummaries.add(new TestDurationResultSummary(buildNumber, duration));
                        }
                        return testDurationResultSummaries;
                    }
                }
            });
        }

        public List<HistoryTestResultSummary> getHistorySummary(int offset) {
            return query(connection -> {
                try (PreparedStatement statement = connection.prepareStatement(
                        "SELECT build, sum(duration) as duration, sum(case when errorDetails is not null then 1 else 0 end) as failCount, sum(case when skipped is not null then 1 else 0 end) as skipCount, sum(case when errorDetails is null and skipped is null then 1 else 0 end) as passCount FROM caseResults WHERE job = ? GROUP BY build ORDER BY build DESC LIMIT 25 OFFSET ?;"
                )) {
                    statement.setString(1, job);
                    statement.setInt(2, offset);
                    try (ResultSet result = statement.executeQuery()) {

                        List<HistoryTestResultSummary> historyTestResultSummaries = new ArrayList<>();
                        while (result.next()) {
                            int buildNumber = result.getInt("build");
                            int duration = result.getInt("duration");
                            int passed = result.getInt("passCount");
                            int failed = result.getInt("failCount");
                            int skipped = result.getInt("skipCount");

                            Job<?, ?> theJob = Jenkins.get().getItemByFullName(getJobName(), Job.class);
                            if (theJob != null) {
                                Run<?, ?> run = theJob.getBuildByNumber(buildNumber);
                                historyTestResultSummaries.add(new HistoryTestResultSummary(run, duration, failed, skipped, passed));
                            }
                        }
                        return historyTestResultSummaries;
                    }
                }
            });
        }

        @Override
        public int getCountOfBuildsWithTestResults() {
            return query(connection -> {
                try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(DISTINCT build) as count FROM caseResults WHERE job = ?;")) {
                    statement.setString(1, job);
                    try (ResultSet result = statement.executeQuery()) {
                        result.next();
                        int count = result.getInt("count");
                        return count;
                    }
                }
            });
        }

        @Override
        public PackageResult getPackageResult(String packageName) {
            return query(connection -> {
                try (PreparedStatement statement = connection.prepareStatement("SELECT suite, testname, classname, errordetails, skipped, duration, stdout, stderr, stacktrace FROM caseResults WHERE job = ? AND build = ? AND package = ?")) {
                    statement.setString(1, job);
                    statement.setInt(2, build);
                    statement.setString(3, packageName);
                    try (ResultSet result = statement.executeQuery()) {

                        PackageResult packageResult = new PackageResult(new TestResult(this), packageName);
                        Map<String, ClassResult> classResults = new HashMap<>();
                        while (result.next()) {
                            String testName = result.getString("testname");
                            String errorDetails = result.getString("errordetails");
                            String suite = result.getString("suite");
                            String className = result.getString("classname");
                            String skipped = result.getString("skipped");
                            String stdout = result.getString("stdout");
                            String stderr = result.getString("stderr");
                            String stacktrace = result.getString("stacktrace");
                            float duration = result.getFloat("duration");

                            SuiteResult suiteResult = new SuiteResult(suite, null, null, null);
                            suiteResult.setParent(new TestResult(this));
                            CaseResult caseResult = new CaseResult(suiteResult, className, testName, errorDetails, skipped, duration, stdout, stderr, stacktrace);

                            ClassResult classResult = classResults.get(className);
                            if (classResult == null) {
                                classResult = new ClassResult(packageResult, className);
                            }
                            classResult.add(caseResult);
                            classResults.put(className, classResult);
                            caseResult.setClass(classResult);

                            packageResult.add(caseResult);
                        }
                        classResults.values().forEach(ClassResult::tally);
                        packageResult.tally();
                        return packageResult;
                    }
                }
            });

        }

        @Override
        public Run<?, ?> getFailedSinceRun(CaseResult caseResult) {
            return query(connection -> {
                int lastPassingBuildNumber;
                Job<?, ?> theJob = Objects.requireNonNull(Jenkins.get().getItemByFullName(job, Job.class));
                try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT build " +
                        "FROM caseResults " +
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
                        "FROM caseResults " +
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
            });

        }

        @Override
        public String getJobName() {
            return job;
        }

        @Override
        public int getBuild() {
            return build;
        }

        @Override
        public List<CaseResult> getFailedTestsByPackage(String packageName) {
            return getByPackage(packageName, "AND errorDetails IS NOT NULL");
        }

        private List<CaseResult> getByPackage(String packageName, String filter) {
            return query(connection -> {
                try (PreparedStatement statement = connection.prepareStatement("SELECT suite, testname, classname, errordetails, duration, skipped, stdout, stderr, stacktrace FROM caseResults WHERE job = ? AND build = ? AND package = ? " + filter)) {
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
                            String stdout = result.getString("stdout");
                            String stderr = result.getString("stderr");
                            String stacktrace = result.getString("stacktrace");
                            float duration = result.getFloat("duration");

                            SuiteResult suiteResult = new SuiteResult(suite, null, null, null);
                            suiteResult.setParent(new TestResult(this));
                            CaseResult caseResult = new CaseResult(suiteResult, className, testName, errorDetails, skipped, duration, stdout, stderr, stacktrace);

                            PackageResult packageResult = new PackageResult(new TestResult(this), packageName);
                            ClassResult classResult = new ClassResult(packageResult, className);
                            classResult.add(caseResult);
                            packageResult.add(caseResult);
                            packageResult.tally();
                            caseResult.tally();
                            caseResult.setClass(classResult);

                            results.add(caseResult);
                        }
                        return results;
                    }
                }
            });
        }


        private List<CaseResult> getCaseResults(String column) {
            return retrieveCaseResult(column + " IS NOT NULL");
        }

        @Override
        public SuiteResult getSuite(String name) {
            return query(connection -> {
                try (PreparedStatement statement = connection.prepareStatement("SELECT testname, package, classname, errordetails, skipped, duration, stdout, stderr, stacktrace FROM caseResults WHERE job = ? AND build = ? AND suite = ?")) {
                    statement.setString(1, job);
                    statement.setInt(2, build);
                    statement.setString(3, name);
                    try (ResultSet result = statement.executeQuery()) {
                        SuiteResult suiteResult = new SuiteResult(name, null, null, null);
                        TestResult parent = new TestResult(this);
                        while (result.next()) {
                            String resultTestName = result.getString("testname");
                            String errorDetails = result.getString("errordetails");
                            String packageName = result.getString("package");
                            String className = result.getString("classname");
                            String skipped = result.getString("skipped");
                            String stdout = result.getString("stdout");
                            String stderr = result.getString("stderr");
                            String stacktrace = result.getString("stacktrace");
                            float duration = result.getFloat("duration");

                            suiteResult.setParent(parent);
                            CaseResult caseResult = new CaseResult(suiteResult, className, resultTestName, errorDetails, skipped, duration, stdout, stderr, stacktrace);
                            final PackageResult packageResult = new PackageResult(parent, packageName);
                            packageResult.add(caseResult);
                            ClassResult classResult = new ClassResult(packageResult, className);
                            classResult.add(caseResult);
                            caseResult.setClass(classResult);
                            suiteResult.addCase(caseResult);
                        }
                        return suiteResult;
                    }
                }
            });

        }

        @Override
        public Collection<SuiteResult> getSuites() {
            return query(connection -> {
                try (PreparedStatement statement = connection.prepareStatement("SELECT suite, testname, package, classname, errordetails, skipped, duration, stdout, stderr, stacktrace FROM caseResults WHERE job = ? AND build = ? ORDER BY suite")) {
                    statement.setString(1, job);
                    statement.setInt(2, build);
                    List<SuiteResult> suiteResults = new ArrayList<SuiteResult>();
                    try (ResultSet result = statement.executeQuery()) {
                        TestResult parent = new TestResult(this);
                        SuiteResult suiteResult = null;
                        boolean isFirstRow = true;
                        while (result.next()) {
                            String thisSuiteName = result.getString("suite");
                            if (isFirstRow || !StringUtils.equals(suiteResult.getName(), thisSuiteName)) {
                                suiteResult = new SuiteResult(thisSuiteName, null, null, null);
                                suiteResults.add(suiteResult);
                                isFirstRow = false;
                            }
                            String resultTestName = result.getString("testname");
                            String errorDetails = result.getString("errordetails");
                            String packageName = result.getString("package");
                            String className = result.getString("classname");
                            String skipped = result.getString("skipped");
                            String stdout = result.getString("stdout");
                            String stderr = result.getString("stderr");
                            String stacktrace = result.getString("stacktrace");
                            float duration = result.getFloat("duration");

                            suiteResult.setParent(parent);
                            CaseResult caseResult = new CaseResult(suiteResult, className, resultTestName, errorDetails, skipped, duration, stdout, stderr, stacktrace);
                            final PackageResult packageResult = new PackageResult(parent, packageName);
                            packageResult.add(caseResult);
                            ClassResult classResult = new ClassResult(packageResult, className);
                            classResult.add(caseResult);
                            caseResult.setClass(classResult);
                            suiteResult.addCase(caseResult);
                        }
                        return suiteResults;
                    }
                }
            });
        }

        @Override
        public float getTotalTestDuration() {
            return query(connection -> {
                try (PreparedStatement statement = connection.prepareStatement("SELECT sum(duration) as duration FROM caseResults" +  " WHERE job = ? and build = ?;")) {
                    statement.setString(1, job);
                    statement.setInt(2, build);
                    try (ResultSet result = statement.executeQuery()) {
                        if (result.next()) {
                            return result.getFloat("duration");
                        }
                        return 0f;
                    }
                }
            });
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

        @Override
        @CheckForNull
        public TestResult getPreviousResult() {
            return query(connection -> {
                try (PreparedStatement statement = connection.prepareStatement("SELECT build FROM caseResults WHERE job = ? AND build < ? ORDER BY build DESC LIMIT 1")) {
                    statement.setString(1, job);
                    statement.setInt(2, build);
                    try (ResultSet result = statement.executeQuery()) {

                        if (result.next()) {
                            int previousBuild = result.getInt("build");

                            return new TestResult(load(job, previousBuild));
                        }
                        return null;
                    }
                }
            });
        }

        @NonNull
        @Override
        public TestResult getResultByNodes(@NonNull List<String> nodeIds) {
            return new TestResult(this); // TODO
        }
    }
}
