package io.jenkins.plugins.junit.storage.database;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import edu.umd.cs.findbugs.annotations.CheckForNull;
import edu.umd.cs.findbugs.annotations.NonNull;
import hudson.Extension;
import hudson.Util;
import hudson.model.Job;
import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.tasks.junit.CaseResult;
import hudson.tasks.junit.ClassResult;
import hudson.tasks.junit.HistoryTestResultSummary;
import hudson.tasks.junit.PackageResult;
import hudson.tasks.junit.SuiteResult;
import hudson.tasks.junit.TestDurationResultSummary;
import hudson.tasks.junit.TestResult;
import hudson.tasks.junit.TestResultSummary;
import hudson.tasks.junit.TrendTestResultSummary;
import io.jenkins.plugins.junit.storage.JunitTestResultStorage;
import io.jenkins.plugins.junit.storage.JunitTestResultStorageDescriptor;
import io.jenkins.plugins.junit.storage.TestResultImpl;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.jdbc.datasource.JdbcTelemetry;
import io.opentelemetry.instrumentation.jdbc.datasource.OpenTelemetryDataSource;
import io.opentelemetry.semconv.incubating.DbIncubatingAttributes;
import jenkins.model.Jenkins;
import org.apache.commons.lang3.StringUtils;
import org.jenkinsci.Symbol;
import org.jenkinsci.plugins.database.Database;
import org.jenkinsci.plugins.database.GlobalDatabaseConfiguration;
import org.jenkinsci.remoting.SerializableOnlyOverRemoting;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.DataBoundSetter;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@Extension
public class DatabaseTestResultStorage extends JunitTestResultStorage {

    public static final Logger log = Logger.getLogger(DatabaseTestResultStorage.class.getName());

    static final int MAX_JOB_LENGTH = 255;
    static final int MAX_SUITE_LENGTH = 255;
    static final int MAX_PACKAGE_LENGTH = 255;
    static final int MAX_CLASSNAME_LENGTH = 255;
    static final int MAX_TEST_NAME_LENGTH = 500;
    static final int MAX_STDOUT_LENGTH = 100000;
    static final int MAX_STDERR_LENGTH = 100000;
    static final int MAX_STACK_TRACE_LENGTH = 100000;
    static final int MAX_ERROR_DETAILS_LENGTH = 100000;
    static final int MAX_SKIPPED_LENGTH = 1000;
    /** The maximum size of a batch to store to the database, used when publishing */
    static final int MAX_DB_BATCH_SIZE = 2000;

    private static final Cache<String, List<CaseResult>> caseResultsCache = Caffeine.newBuilder()
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .maximumSize(100)
            .removalListener((String key, List<CaseResult> ignore, RemovalCause cause) ->
                    log.config(String.format("Key '%s' removed from caseResultsCache because (%s)", key, cause)))
            .build();
    private static final Cache<String, List<PackageResult>> packageResultsCache = Caffeine.newBuilder()
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .maximumSize(100)
            .removalListener((String key, List<PackageResult> ignore, RemovalCause cause) ->
                    log.config(String.format("Key '%s' removed from packageResultsCache because (%s)", key, cause)))
            .build();

    transient ConnectionSupplier  connectionSupplier;

    private boolean skipCleanupRunsOnDeletion;

    @DataBoundConstructor
    public DatabaseTestResultStorage() {}

    public ConnectionSupplier  getConnectionSupplier() {
        if (connectionSupplier == null) {
            log.config("getConnectionSupplier() -> initializing and returning a new LocalConnectionSupplier");
            connectionSupplier = new LocalConnectionSupplier();
        }
        log.fine("getConnectionSupplier() -> returning cached connectionSupplier");
        return connectionSupplier;
    }

    public boolean isSkipCleanupRunsOnDeletion() {
        return skipCleanupRunsOnDeletion;
    }

    @DataBoundSetter
    public void setSkipCleanupRunsOnDeletion(boolean skipCleanupRunsOnDeletion) {
        this.skipCleanupRunsOnDeletion = skipCleanupRunsOnDeletion;
    }

    @Override
    public RemotePublisher createRemotePublisher(Run<?, ?> build) throws IOException {
        try {
            log.config("createRemotePublisher() -> calling getConnectionSupplier().connection() for build "
                    + build.getParent().getFullName() + " #" + build.getNumber());
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

    @Override
    public TestResultImpl load(String job, int build) {
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

        @Override
        public void publish(TestResult result, TaskListener listener) throws IOException {
            var publishSpan = createSpan("DatabaseTestResultStorage.RemotePublisherImpl.publish");
            var sql = "INSERT INTO caseResults (job, "
                    + "build, suite, package, className, testName, errorDetails, skipped, duration, stdout, "
                    + "stderr, stacktrace) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (Connection connection = connectionSupplier.connection();
                    PreparedStatement statement = connection.prepareStatement(sql);
                    Scope ignore = publishSpan.makeCurrent()) {
                addSqlAttribute(publishSpan, sql);
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
                            statement.setString(8, StringUtils.truncate(Util.fixNull(caseResult.getSkippedMessage()),
                                    MAX_SKIPPED_LENGTH));
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
                            statement.setString(12,
                                    StringUtils.truncate(caseResult.getErrorStackTrace(), MAX_STACK_TRACE_LENGTH));
                        } else {
                            statement.setNull(12, Types.VARCHAR);
                        }
                        statement.addBatch();
                        count++;
                        if (count % MAX_DB_BATCH_SIZE == 0) {
                            log.config(String.format("Inserting %d test cases for '%s #%d'.", MAX_DB_BATCH_SIZE, job, build));
                            var batchSpan = getTracer()
                                    .spanBuilder("sql batch insert caseResults")
                                    .startSpan();
                            try(Scope _ignore = batchSpan.makeCurrent()) {
                                statement.executeBatch();
                                batchSpan.setAttribute("batchSize", MAX_DB_BATCH_SIZE);
                                statement.clearBatch();
                            } finally {
                                batchSpan.end();
                            }
                        }
                    }
                }
                if (count % MAX_DB_BATCH_SIZE != 0) {
                    var batchSpan = getTracer()
                            .spanBuilder("sql batch insert caseResults")
                            .startSpan();
                    try(Scope _ignore = batchSpan.makeCurrent()) {
                        int[] updateCounts = statement.executeBatch();
                        int numberOfItemsStored = updateCounts.length;
                        log.config(String.format("Inserted final %d test cases for '%s #%d'.",
                                numberOfItemsStored, job, build));
                        batchSpan.setAttribute("batchSize", numberOfItemsStored);
                    } finally {
                        batchSpan.end();
                    }
                }
                log.info(String.format("Saved %d test cases into database for '%s #%d'.", count, job, build));
            } catch (SQLException x) {
                throw new IOException(x);
            } finally {
                publishSpan.end();
            }
        }
    }

    public static abstract class ConnectionSupplier implements AutoCloseable {

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

    static class OpenTelemetryDatabase extends Database {
        private final Database database;

        OpenTelemetryDatabase(Database database) {
            this.database = database;
        }

        @Override
        public DataSource getDataSource() throws SQLException {
            DataSource dataSource = database.getDataSource();
            if (! (dataSource instanceof OpenTelemetryDataSource)) {
                dataSource = JdbcTelemetry.create(GlobalOpenTelemetry.get()).wrap(dataSource);
            }
            return dataSource;
        }
    }

    static class LocalConnectionSupplier extends ConnectionSupplier {

        @Override
        protected Database database() {
            return new OpenTelemetryDatabase(GlobalDatabaseConfiguration.get().getDatabase());
        }

        @Override
        protected void initialize(Connection connection) throws SQLException {
            withSpan("DatabaseTestResultStorage.LocalConnectionSupplier.initialize", () -> {
                if (!DatabaseSchemaLoader.MIGRATED) {
                    DatabaseSchemaLoader.migrateSchema();
                }
                return null;
            });
        }
    }

    /** Ensures a Database configuration can be sent to an agent. */
    static class RemoteConnectionSupplier extends ConnectionSupplier implements SerializableOnlyOverRemoting {

        private final Database database;

        RemoteConnectionSupplier() {
            database = GlobalDatabaseConfiguration.get().getDatabase();
        }

        @Override protected Database database() {
            return new OpenTelemetryDatabase(database);
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

        List<CaseResult> getCaseResults() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getCaseResults", span -> {
                var cacheKey = getCacheKey();
                if (caseResultsCache.asMap().containsKey(cacheKey)) {
                    log.fine(String.format("Loading case results from cache for '%s'.", cacheKey));
                }
                List<CaseResult> caseResults = caseResultsCache.get(cacheKey, key -> loadCaseResultsFromDB(span));
                if (caseResults.isEmpty()) {
                    log.fine(String.format("Case results are empty for job %s so invalidating cache.", cacheKey));
                    caseResultsCache.invalidate(cacheKey);
                }
                var run = getRun();
                if (run != null && run.isBuilding()) {
                    log.fine(String.format("Build '%s' is still running so invalidating case results cache.", cacheKey));
                    caseResultsCache.invalidate(cacheKey);
                }
                return caseResults;
            });
        }

        private String getCacheKey() {
            return job + " #" + build;
        }

        private List<CaseResult> loadCaseResultsFromDB(Span parentSpan) {
           return withSpan("DatabaseTestResultStorage.TestResultStorage.loadCaseResultsFromDB", span ->
                    query(connection -> {
                        List<CaseResult> results = new ArrayList<>();
                        var sql = "SELECT suite, package, "
                                + "testname, classname, errordetails, skipped, duration, stdout, stderr, stacktrace "
                                + "FROM caseResults WHERE job = ? AND build = ?";
                        addSqlAttribute(span, sql);
                        try (var preparedStatement = connection.prepareStatement(sql)) {
                            preparedStatement.setString(1, job);
                            preparedStatement.setInt(2, build);
                            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                                Map<String, ClassResult> classResults = new HashMap<>();
                                TestResult parent = new TestResult(this);
                                while (resultSet.next()) {
                                    String packageName = resultSet.getString("package");
                                    String className = resultSet.getString("classname");
                                    String testName = resultSet.getString("testname");
                                    String errorDetails = resultSet.getString("errordetails");
                                    String suite = resultSet.getString("suite");
                                    String skipped = resultSet.getString("skipped");
                                    String stdout = resultSet.getString("stdout");
                                    String stderr = resultSet.getString("stderr");
                                    String stacktrace = resultSet.getString("stacktrace");
                                    float duration = resultSet.getFloat("duration");
                                    SuiteResult suiteResult = new SuiteResult(suite, null, null, null);
                                    suiteResult.setParent(parent);
                                    CaseResult caseResult =
                                            new CaseResult(suiteResult, className, testName, errorDetails,
                                                    skipped, duration, stdout, stderr, stacktrace);
                                    ClassResult classResult = classResults.get(className);
                                    if (classResult == null) {
                                        classResult =
                                                new ClassResult(new PackageResult(new TestResult(this), packageName),
                                                        className);
                                    }
                                    classResult.add(caseResult);
                                    caseResult.setClass(classResult);
                                    classResults.put(className, classResult);
                                    results.add(caseResult);
                                }
                                classResults.values().forEach(ClassResult::tally);
                            }
                        }
                        log.info(String.format("Loaded %d test cases from database for '%s #%d'.", results.size(), job,
                                build));
                        return results;
                    }));
        }

        void deleteRun() {
            withSpan("DatabaseTestResultStorage.TestResultStorage.deleteRun", span -> {
                log.info(String.format("Deleting test results and purging the cache for job %s #%d", job, build));
                var cacheKey = getCacheKey();
                caseResultsCache.invalidate(cacheKey);
                packageResultsCache.invalidate(cacheKey);
                return query(connection -> {
                    var sql = "DELETE FROM caseResults WHERE job = ? AND build = ?";
                    addSqlAttribute(span, sql);
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setString(1, job);
                        span.setAttribute("job", job);
                        statement.setInt(2, build);
                        span.setAttribute("build", build);
                        statement.execute();
                    }
                    return null;
                });
            });
        }

        private void invalidateCachesForJob(String jobName) {
            // invalidate all cache entries that start with the job name
            caseResultsCache.asMap().keySet().stream()
                    .filter(key -> key.startsWith(jobName))
                    .forEach(caseResultsCache::invalidate);
            packageResultsCache.asMap().keySet().stream()
                    .filter(key -> key.startsWith(jobName))
                    .forEach(packageResultsCache::invalidate);
        }


        void deleteJob() {
            withSpan("DatabaseTestResultStorage.TestResultStorage.deleteJob", span -> {
                log.info(String.format("Deleting test results and purging the cache for job %s", job));
                invalidateCachesForJob(job);
                return query(connection -> {
                    var sql = "DELETE FROM caseResults WHERE job = ?";
                    addSqlAttribute(span, sql);
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setString(1, job);
                        span.setAttribute("job", job);
                        statement.execute();
                    }
                    return null;
                });
            });
        }

        @Override
        public List<PackageResult> getAllPackageResults() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getAllPackageResults", span -> {
                var cacheKey = getCacheKey();
                if (packageResultsCache.asMap().containsKey(cacheKey)) {
                    log.fine(String.format("Loading package results from cache for '%s'.", cacheKey));
                }
                List<PackageResult> packageResults = packageResultsCache.get(cacheKey, k -> loadPackageResults());
                if (packageResults.isEmpty()) {
                    log.fine(String.format("Package results are empty for '%s' so invalidating cache.", cacheKey));
                    packageResultsCache.invalidate(cacheKey);
                }
                var run = getRun();
                if (run != null && run.isBuilding()) {
                    log.fine(String.format("Build '%s' is still running so invalidating package results cache.",
                            cacheKey));
                    packageResultsCache.invalidate(cacheKey);
                }
                return packageResults;
            });
        }

        private List<PackageResult> loadPackageResults() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.loadPackageResults", span -> {
                Map<String, PackageResult> mapOfPackageResults = new TreeMap<>();
                TestResult testResult = new TestResult(this);
                getCaseResults().forEach(caseResult -> {
                    String packageName = caseResult.getPackageName();
                    PackageResult packageResult = mapOfPackageResults.computeIfAbsent(packageName,
                            name -> new PackageResult(testResult, name));
                    packageResult.add(caseResult);
                });
                mapOfPackageResults.values().forEach(PackageResult::tally);
                log.info(String.format("Loaded %d package results from case results for '%s #%d'.",
                        mapOfPackageResults.size(), job, build));
                span.setAttribute("numPackages", mapOfPackageResults.size());
                return new ArrayList<>(mapOfPackageResults.values());
            });
        }

        @Override
        public List<TrendTestResultSummary> getTrendTestResultSummary() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getTrendTestResultSummary", span ->
                query(connection -> {
                    var sql = "SELECT build, "
                            + "sum(case when errorDetails is not null then 1 else 0 end) as failCount, "
                            + "sum(case when skipped is not null then 1 else 0 end) as skipCount, "
                            + "sum(case when errorDetails is null and skipped is null then 1 else 0 end) as passCount "
                            + "FROM caseResults WHERE job = ? group by build order by build;";
                    addSqlAttribute(span, sql);
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setString(1, job);
                        try (ResultSet result = statement.executeQuery()) {

                            List<TrendTestResultSummary> trendTestResultSummaries = new ArrayList<>();
                            while (result.next()) {
                                int buildNumber = result.getInt("build");
                                int passed = result.getInt("passCount");
                                int failed = result.getInt("failCount");
                                int skipped = result.getInt("skipCount");
                                int total = passed + failed + skipped;

                                trendTestResultSummaries.add(new TrendTestResultSummary(buildNumber,
                                        new TestResultSummary(failed, skipped, passed, total)));
                            }
                            return trendTestResultSummaries;
                        }
                    }
                }));
        }

        @Override
        public List<TestDurationResultSummary> getTestDurationResultSummary() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getTestDurationResultSummary", span -> query(connection -> {
                var sql = "SELECT build, sum(duration) as duration "
                        + "FROM caseResults "
                        + "WHERE job = ? group by build order by build;";
                addSqlAttribute(span, sql);
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, job);
                    try (ResultSet result = statement.executeQuery()) {

                        List<TestDurationResultSummary> testDurationResultSummaries = new ArrayList<>();
                        while (result.next()) {
                            int buildNumber = result.getInt("build");
                            int duration = result.getInt("duration");

                            testDurationResultSummaries.add(
                                    new TestDurationResultSummary(buildNumber, duration));
                        }
                        return testDurationResultSummaries;
                    }
                }
            }));
        }

        public List<HistoryTestResultSummary> getHistorySummary(int offset) {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getHistorySummary", span -> {
                span.setAttribute("offset", offset);
                return query(connection -> {
                    var sql = "SELECT build, "
                            + "sum(duration) as duration, "
                            + "sum(case when errorDetails is not null then 1 else 0 end) as failCount, "
                            + "sum(case when skipped is not null then 1 else 0 end) as skipCount, "
                            + "sum(case when errorDetails is null and skipped is null then 1 else 0 end) as passCount "
                            + "FROM caseResults "
                            + "WHERE job = ? GROUP BY build ORDER BY build DESC LIMIT 25 OFFSET ?;";
                    addSqlAttribute(span, sql);
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setString(1, job);
                        span.setAttribute("job", job);
                        statement.setInt(2, offset);
                        span.setAttribute("offset", offset);
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
                                    historyTestResultSummaries.add(
                                            new HistoryTestResultSummary(run, duration, failed, skipped, passed));
                                }
                            }
                            return historyTestResultSummaries;
                        }
                    }
                });
            });
        }

        @Override
        public int getCountOfBuildsWithTestResults() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getCountOfBuildsWithTestResults", span ->
                    query(connection -> {
                        var sql = "SELECT COUNT(DISTINCT build) as count FROM caseResults WHERE job = ?;";
                        addSqlAttribute(span, sql);
                        try (PreparedStatement statement = connection.prepareStatement(sql)) {
                            statement.setString(1, job);
                            span.setAttribute("job", job);
                            try (ResultSet result = statement.executeQuery()) {
                                result.next();
                                return result.getInt("count");
                            }
                        }
                    })
            );
        }

        @Override
        public PackageResult getPackageResult( String packageName) {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getPackageResult", span -> {
                addPackageAttribute(span, packageName);
                return getAllPackageResults().stream()
                        .filter(packageResult -> packageResult.getName().equals(packageName))
                        .findFirst()
                        .orElse(null);
            });
        }

        @Override
        public Run<?, ?> getFailedSinceRun(CaseResult caseResult) {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getFailedSinceRun", span -> {
                span.setAttribute("build", build);
                span.setAttribute("job", job);
                return query(connection -> {
                    var spanPassingBuild = createSpan("lastPassingBuild");
                    int lastPassingBuildNumber;
                    Job<?, ?> theJob = Objects.requireNonNull(Jenkins.get().getItemByFullName(job, Job.class));
                    String sqlPassingBuild = "SELECT build " +
                            "FROM caseResults " +
                            "WHERE job = ? " +
                            "AND build < ? " +
                            "AND suite = ? " +
                            "AND package = ? " +
                            "AND classname = ? " +
                            "AND testname = ? " +
                            "AND errordetails IS NULL " +
                            "ORDER BY BUILD DESC " +
                            "LIMIT 1";
                    addSqlAttribute(spanPassingBuild, sqlPassingBuild);
                    try (PreparedStatement statement = connection.prepareStatement(sqlPassingBuild);
                         Scope ignore = spanPassingBuild.makeCurrent()) {
                        addCaseResultToStatement(caseResult, build, statement);
                        try (ResultSet result = statement.executeQuery()) {
                            boolean hasPassed = result.next();
                            if (!hasPassed) {
                                return theJob.getBuildByNumber(1);
                            }
                            lastPassingBuildNumber = result.getInt("build");
                        }
                    } finally {
                        spanPassingBuild.end();
                    }
                    var spanFailingBuild = createSpan("lastFailingBuild");
                    String sqlFailedBuild = "SELECT build " +
                            "FROM caseResults " +
                            "WHERE job = ? " +
                            "AND build > ? " +
                            "AND suite = ? " +
                            "AND package = ? " +
                            "AND classname = ? " +
                            "AND testname = ? " +
                            "AND errordetails is NOT NULL " +
                            "ORDER BY BUILD ASC " +
                            "LIMIT 1";
                    addSqlAttribute(spanFailingBuild, sqlFailedBuild);
                    try (PreparedStatement statement = connection.prepareStatement(sqlFailedBuild);
                         Scope ignore = spanFailingBuild.makeCurrent()) {
                        addCaseResultToStatement(caseResult, lastPassingBuildNumber, statement);
                        try (ResultSet result = statement.executeQuery()) {
                            result.next();
                            int firstFailingBuildAfterPassing = result.getInt("build");
                            return theJob.getBuildByNumber(firstFailingBuildAfterPassing);
                        }
                    } finally {
                        spanFailingBuild.end();
                    }
                });
            });
        }

        private void addCaseResultToStatement(CaseResult caseResult, int build, PreparedStatement preparedStatement)
                throws SQLException {
            preparedStatement.setString(1, job);
            preparedStatement.setInt(2, build);
            preparedStatement.setString(3, caseResult.getSuiteResult().getName());
            preparedStatement.setString(4, caseResult.getPackageName());
            preparedStatement.setString(5, caseResult.getClassName());
            preparedStatement.setString(6, caseResult.getName());
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
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getFailedTestsByPackage", span -> {
                addPackageAttribute(span, packageName);
                return getCaseResults().stream()
                        .filter(caseResult -> caseResult.getPackageName().equals(packageName))
                        .filter(caseResult -> caseResult.getErrorDetails() != null)
                        .collect(Collectors.toList());
            });
        }

        @Override
        public SuiteResult getSuite(String suiteName) {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getSuite", span -> {
                span.setAttribute("suite", suiteName);
                log.fine(String.format("Getting suite result for suite %s from case results.", suiteName));
                SuiteResult suiteResult = new SuiteResult(suiteName, null, null, null);
                getCaseResults().stream()
                        .filter(caseResult -> caseResult.getSuiteResult().getName().equals(suiteName))
                        .forEach(caseResult -> {
                            TestResult testResult = new TestResult(this);
                            String packageName = caseResult.getPackageName();
                            String className = caseResult.getClassName();
                            populateSuiteResult(caseResult, testResult, className, packageName, suiteResult);
                        });
                return suiteResult;
            });
        }


        @Override
        public Collection<SuiteResult> getSuites() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getSuites", span -> {
                log.fine("Getting suite results from case results.");
                Map<String, SuiteResult> mapOfSuites = new TreeMap<>();
                getCaseResults().forEach(caseResult -> {
                    String suiteName = caseResult.getSuiteResult().getName();
                    SuiteResult suiteResult = mapOfSuites.computeIfAbsent(suiteName,
                            name -> new SuiteResult(name, null, null, null));
                    TestResult testResult = new TestResult(this);
                    String packageName = caseResult.getPackageName();
                    String className = caseResult.getClassName();
                    populateSuiteResult(caseResult, testResult, className, packageName, suiteResult);
                    mapOfSuites.putIfAbsent(suiteName, suiteResult);
                });
                return mapOfSuites.values();
            });
        }

        private void populateSuiteResult(CaseResult caseResult, TestResult testResult, String className, String packageName,
                SuiteResult suiteResult) {
            final PackageResult packageResult = new PackageResult(testResult, packageName);
            packageResult.add(caseResult);
            ClassResult classResult = new ClassResult(packageResult, className);
            classResult.add(caseResult);
            caseResult.setClass(classResult);
            suiteResult.addCase(caseResult);
        }

        @Override
        public float getTotalTestDuration() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getTotalTestDuration", span ->
                    getCaseResults().stream()
                    .map(CaseResult::getDuration)
                    .reduce(Float::sum)
                    .orElse(0f));
        }

        @Override
        public int getFailCount() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getFailCount", span ->
                    (int) getCaseResults().stream()
                    .filter(CaseResult::isFailed)
                    .count());
        }

        @Override
        public int getSkipCount() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getSkipCount", span ->
                    (int) getCaseResults().stream()
                            .filter(CaseResult::isSkipped)
                            .count());
        }

        @Override
        public int getPassCount() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getPassCount", span ->
                    (int) getCaseResults().stream()
                            .filter(CaseResult::isPassed)
                            .count());
        }

        @Override
        public int getTotalCount() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getTotalCount", span ->
                    getCaseResults().size());
        }

        @Override
        public List<CaseResult> getFailedTests() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getFailedTests", span ->
                    getCaseResults().stream()
                            .filter(caseResult -> caseResult.getErrorDetails() != null)
                            .collect(Collectors.toList()));
        }

        @Override
        public List<CaseResult> getSkippedTests() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getSkippedTests", span ->
                    getCaseResults().stream()
                            .filter(CaseResult::isSkipped)
                            .collect(Collectors.toList()));
        }

        @Override
        public List<CaseResult> getSkippedTestsByPackage(String packageName) {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getSkippedTestsByPackage", span -> {
                addPackageAttribute(span, packageName);
                return getCaseResults().stream()
                        .filter(caseResult -> caseResult.getPackageName().equals(packageName))
                        .filter(CaseResult::isSkipped)
                        .collect(Collectors.toList());
            });
        }

        @Override
        public List<CaseResult> getPassedTests() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getPassedTests", span ->
                    getCaseResults().stream()
                            .filter(caseResult -> !caseResult.isSkipped())
                            .filter(caseResult -> caseResult.getErrorDetails() == null)
                            .collect(Collectors.toList()));
        }

        @Override
        public List<CaseResult> getPassedTestsByPackage(String packageName) {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getPassedTestsByPackage", span -> {
                addPackageAttribute(span, packageName);
                return getCaseResults().stream()
                        .filter(caseResult -> caseResult.getPackageName().equals(packageName))
                        .filter(caseResult -> !caseResult.isSkipped())
                        .filter(caseResult -> caseResult.getErrorDetails() == null)
                        .collect(Collectors.toList());
            });
        }

        @Override
        @CheckForNull
        public TestResult getPreviousResult() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getPreviousResult", span -> {
                var sql = "SELECT build FROM caseResults WHERE job = ? AND build < ? ORDER BY build DESC LIMIT 1";
                addSqlAttribute(span, sql);
                return query(connection -> {
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
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
            });
        }

        @NonNull
        @Override
        public TestResult getResultByNodes(@NonNull List<String> nodeIds) {
            return new TestResult(this); // TODO
        }
    }

    private static Span createSpan(String spanName) {
        Span span = getTracer()
                .spanBuilder(spanName)
                .startSpan();
        span.setAttribute("java.package", DatabaseTestResultStorage.class.getPackageName());
        return span;
    }

    public static void withSpan(String spanName, Supplier<?> supplier) {
        var span = createSpan(spanName);
        try(Scope ignore = span.makeCurrent()) {
            supplier.get();
        } finally {
            span.end();
        }
    }

    public static <T> T withSpan(String spanName, Function<Span, T> function) {
        Span span = createSpan(spanName);
        try(Scope ignore = span.makeCurrent()) {
            return function.apply(span);
        } finally {
            span.end();
        }
    }

    private static void addSqlAttribute(Span span, String sql) {
        span.setAttribute(DbIncubatingAttributes.DB_STATEMENT, sql);
    }

    private static void addPackageAttribute(Span span, String packageName) {
        span.setAttribute("package", packageName);
    }

    private static Tracer getTracer() {
        return GlobalOpenTelemetry.getTracer("io.jenkins.plugins.junit.storage.database");
    }
}
