package io.jenkins.plugins.junit.storage.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
import hudson.remoting.Channel;
import hudson.tasks.junit.CaseResult;
import hudson.tasks.junit.ClassResult;
import hudson.tasks.junit.HistoryTestResultSummary;
import hudson.tasks.junit.PackageResult;
import hudson.tasks.junit.SuiteResult;
import hudson.tasks.junit.TestDurationResultSummary;
import hudson.tasks.junit.TestResult;
import hudson.tasks.junit.TestResultSummary;
import hudson.tasks.junit.TrendTestResultSummary;
import hudson.tasks.test.AbstractTestResultAction;
import hudson.util.Secret;
import io.jenkins.plugins.junit.storage.JunitTestResultStorage;
import io.jenkins.plugins.junit.storage.JunitTestResultStorageDescriptor;
import io.jenkins.plugins.junit.storage.TestResultImpl;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jenkins.model.Jenkins;
import jenkins.security.SlaveToMasterCallable;
import jenkins.util.SystemProperties;
import org.apache.commons.lang3.StringUtils;
import org.jenkinsci.Symbol;
import org.jenkinsci.plugins.database.Database;
import org.jenkinsci.plugins.database.GlobalDatabaseConfiguration;
import org.jenkinsci.remoting.SerializableOnlyOverRemoting;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.DataBoundSetter;

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

    /**
     * Number of failing {@link CaseResult}s whose "failed since" build is looked up in a single SQL
     * round trip (see {@code computeFailedSinceBatch}), rather than one round trip per failing case.
     * A test-report page lists every failing case and queries each one's "failed since" build, so
     * without batching, a build with hundreds of failing cases issued hundreds of sequential queries;
     * each query itself is index-backed and fast, but with a real (non-localhost) database connection
     * the per-round-trip network latency dominates and multiplies directly by the failing case count.
     * Batching trades a larger single query (one {@code UNION ALL} branch per case, each an exact copy
     * of the original single-case subquery) for a bounded number of round trips. Kept well under
     * typical driver/database parameter-count and query-size limits.
     */
    static final int FAILED_SINCE_BATCH_SIZE = 200;

    /**
     * Same rationale and shape as {@link #FAILED_SINCE_BATCH_SIZE}, but for
     * {@link TestResultImpl#getPreviousCaseResult(CaseResult)}: a test-report page calls
     * {@link CaseResult#getPreviousResult()} once per case of the current build (not just failing
     * ones), so batching matters even more here.
     */
    static final int PREVIOUS_CASE_RESULT_BATCH_SIZE = 200;

    /**
     * Mirrors {@code hudson.tasks.junit.CaseResult#PREVIOUS_TEST_RESULT_BACKTRACK_BUILDS_MAX}: the
     * number of historical builds {@link CaseResult#getPreviousResult()} is willing to consider when
     * looking for a case with the same identity. Read independently (same system property key) rather
     * than referencing that package-private field directly, since it lives in a different package in
     * the junit-plugin module.
     */
    static final int PREVIOUS_CASE_RESULT_BACKTRACK_BUILDS_MAX = Integer.getInteger(
            "hudson.tasks.junit.History$HistoryTableResult.PREVIOUS_TEST_RESULT_BACKTRACK_BUILDS_MAX", 25);

    /**
     * Upper bound on the total number of {@link CaseResult}s kept resident across all cached builds
     * at once, used as the {@link Caffeine#maximumWeight} for {@link #resultsCache}. Entries are
     * weighed by case count (a proxy for memory footprint, since per-case payloads such as stdout/
     * stderr/stack traces dominate) rather than by entry count, so that e.g. a handful of very large
     * builds (as can happen when several running builds with big suites are viewed around the same
     * time) cannot together exceed a bounded heap budget. Overridable (system property, in number of
     * test cases) for unusually large or small deployments/heaps; defaults conservatively since a
     * single case can carry up to several hundred KB of stdout/stderr/stack trace text.
     * <p>Note this bound must comfortably exceed the size of a single large build, since computing
     * test "age"/"failed since" ({@link CaseResult#getPreviousResult()}) walks up to
     * {@code PREVIOUS_TEST_RESULT_BACKTRACK_BUILDS_MAX} (25 by default) historical builds, each of
     * which needs its own case list resident at the same time as the current build's; setting this
     * too close to (or below) the size of one large build causes repeated evict-and-reload thrashing
     * between the current and historical builds' entries, which is far slower than either bound.
     */
    private static final long MAX_CACHED_CASE_RESULTS =
            Long.getLong(DatabaseTestResultStorage.class.getName() + ".maxCachedCaseResults", 500_000L);

    /**
     * A single cache of per-build results, keyed by exact job/build identity.
     * <p>Entries use a fixed expiry from creation (not sliding on access) as a safety net: normal
     * freshness is provided by explicit {@link #invalidate(String, int)} calls after publishing,
     * deletion, and build completion, so this expiry only matters if an invalidation is ever missed.
     * It intentionally comfortably exceeds how long even a very large build's page can take to render
     * (including the per-case "failed since"/age lookups below), since an entry expiring mid-request
     * would otherwise force an identical, equally slow reload on every subsequent request as well. An
     * entry lazily and independently memoizes the full case list, the derived package list, the
     * SQL-computed summary (counts/duration), the previous build lookup, and per-case "failed since"
     * results, so that repeated accessors sharing a build only trigger one load of each kind per cache
     * generation, including for running or empty-result builds.
     */
    private static final Cache<CacheKey, ResultsEntry> resultsCache = Caffeine.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .maximumWeight(MAX_CACHED_CASE_RESULTS)
            .weigher((CacheKey key, ResultsEntry entry) -> entry.weight())
            .removalListener((CacheKey key, ResultsEntry ignore, RemovalCause cause) ->
                    log.config(String.format("Key '%s' removed from resultsCache because (%s)", key, cause)))
            .build();

    /** Invalidates the cached results for one build, e.g. after a publisher finishes writing to it. */
    static void invalidate(String job, int build) {
        resultsCache.invalidate(new CacheKey(job, build));
    }

    /** Invalidates the cached results for every build of a job, e.g. when the job itself is deleted. */
    static void invalidateJob(String job) {
        resultsCache.asMap().keySet().stream()
                .filter(key -> key.job.equals(job))
                .forEach(resultsCache::invalidate);
    }

    private static final class CacheKey {
        private final String job;
        private final int build;

        CacheKey(String job, int build) {
            this.job = job;
            this.build = build;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof CacheKey)) {
                return false;
            }
            CacheKey other = (CacheKey) o;
            return build == other.build && job.equals(other.job);
        }

        @Override
        public int hashCode() {
            return Objects.hash(job, build);
        }

        @Override
        public String toString() {
            return job + " #" + build;
        }
    }

    /** Counts and total duration for one build, computed by a single SQL aggregate query. */
    private static final class BuildSummary {
        private final int total;
        private final int passed;
        private final int failed;
        private final int skipped;
        private final float duration;

        BuildSummary(int total, int passed, int failed, int skipped, float duration) {
            this.total = total;
            this.passed = passed;
            this.failed = failed;
            this.skipped = skipped;
            this.duration = duration;
        }
    }

    /** Callable sent from an agent back to the controller to invalidate the controller-side cache. */
    private static final class InvalidateCacheCallable extends SlaveToMasterCallable<Void, RuntimeException> {
        private static final long serialVersionUID = 1L;
        private final String job;
        private final int build;

        InvalidateCacheCallable(String job, int build) {
            this.job = job;
            this.build = build;
        }

        @Override
        public Void call() {
            DatabaseTestResultStorage.invalidate(job, build);
            return null;
        }
    }

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
            // Borrowed purely to make sure a local server is started and the table/schema exists
            // before publishing begins; each call to connection() returns a fresh pooled connection,
            // so it must be closed and returned to the pool immediately.
            try (Connection ignored = getConnectionSupplier().connection()) {
                // no-op; opening the connection above triggers ConnectionSupplier.initialize()
            }
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

    /** Binds parameters onto an already-scoped {@link PreparedStatement}; see {@code loadCaseResultRows}. */
    @FunctionalInterface
    private interface SqlBinder {
        void bind(PreparedStatement statement) throws SQLException;
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
            IOException publishFailure = null;
            try {
                doPublish(result);
            } catch (IOException x) {
                publishFailure = x;
            }
            // Always invalidate, even on a partial-write failure, so stale cached results from before
            // this call are not served while the real database state is now different (or still unknown).
            try {
                invalidateCache();
            } catch (IOException x) {
                if (publishFailure != null) {
                    publishFailure.addSuppressed(x);
                } else {
                    publishFailure = x;
                }
            }
            if (publishFailure != null) {
                throw publishFailure;
            }
        }

        private void invalidateCache() throws IOException {
            Channel channel = Channel.current();
            if (channel != null) {
                // Running on an agent: call back to the controller, where the cache actually lives.
                try {
                    channel.call(new InvalidateCacheCallable(job, build));
                } catch (InterruptedException x) {
                    Thread.currentThread().interrupt();
                    throw new IOException(x);
                }
            } else {
                // Running on the controller itself (e.g. a build on the built-in node).
                DatabaseTestResultStorage.invalidate(job, build);
            }
        }

        private void doPublish(TestResult result) throws IOException {
            var publishSpan = createSpan("DatabaseTestResultStorage.RemotePublisherImpl.publish");
            var sql = "INSERT INTO caseResults (job, "
                    + "build, suite, package, className, testName, errorDetails, skipped, duration, stdout, "
                    + "stderr, stacktrace) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (Connection connection = connectionSupplier.connection();
                    PreparedStatement statement = connection.prepareStatement(sql);
                    Scope ignore = publishSpan.makeCurrent()) {
                addSqlAttribute(publishSpan, sql);
                // Flush each chunk (the caseResults batch insert plus its caseResultsSummary delta) in
                // a single transaction, mirroring deleteRun()/deleteJob(): without this, a crash or
                // error between executeBatch() and upsertSummary() (or a mid-transaction failure in
                // upsertSummary() itself) could leave caseResultsSummary permanently inconsistent with
                // what was actually committed to caseResults, since they were previously two independent
                // auto-committed statements with no rollback tying them together.
                boolean originalAutoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                try {
                    doPublishWithinTransaction(connection, statement, publishSpan, result);
                } catch (SQLException | RuntimeException x) {
                    try {
                        connection.rollback();
                    } catch (SQLException ignored) {
                        // Best-effort: the original exception below is what actually gets reported; a
                        // failure to roll back (e.g. connection already broken) shouldn't mask it.
                    }
                    if (x instanceof SQLException) {
                        throw new IOException(x);
                    }
                    throw (RuntimeException) x;
                } finally {
                    connection.setAutoCommit(originalAutoCommit);
                }
            } catch (SQLException x) {
                throw new IOException(x);
            } finally {
                publishSpan.end();
            }
        }

        private void doPublishWithinTransaction(Connection connection, PreparedStatement statement, Span publishSpan,
                TestResult result) throws SQLException {
            int count = 0;
            // Aggregate counts/duration for whichever batch chunk is currently unflushed, so that
            // caseResultsSummary (see #upsertSummary) can be updated with exactly the rows that
            // were actually just committed to caseResults in each flush below, including on a
            // partial-batch failure partway through a large publish.
            int chunkPassCount = 0;
            int chunkFailCount = 0;
            int chunkSkipCount = 0;
            double chunkDuration = 0;
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
                    // Match the independent errorDetails IS NOT NULL / skipped IS NOT NULL predicates used
                    // by the migration backfill (see V2026_10_05_2240__case-results-summary.sql) and the
                    // original per-row aggregate queries, rather than treating them as mutually exclusive,
                    // so summaries are identical regardless of whether a build was backfilled or published
                    // after this upgrade.
                    boolean isSkipped = caseResult.isSkipped();
                    if (errorDetails != null) {
                        chunkFailCount++;
                    }
                    if (isSkipped) {
                        chunkSkipCount++;
                    }
                    if (errorDetails == null && !isSkipped) {
                        chunkPassCount++;
                    }
                    chunkDuration += caseResult.getDuration();
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
                        upsertSummary(connection, publishSpan, chunkPassCount, chunkFailCount, chunkSkipCount,
                                chunkDuration);
                        connection.commit();
                        chunkPassCount = 0;
                        chunkFailCount = 0;
                        chunkSkipCount = 0;
                        chunkDuration = 0;
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
                upsertSummary(connection, publishSpan, chunkPassCount, chunkFailCount, chunkSkipCount,
                        chunkDuration);
                connection.commit();
            }
            log.info(String.format("Saved %d test cases into database for '%s #%d'.", count, job, build));
        }

        /**
         * Incrementally maintains {@code caseResultsSummary}, the persisted per-build aggregate read
         * by {@code getTrendTestResultSummary}/{@code getTestDurationResultSummary}/
         * {@code getHistorySummary}/{@code getCountOfBuildsWithTestResults}, so those no longer need
         * to aggregate every {@code caseResults} row for a job on every call.
         * <p>Called once per flushed batch chunk (not once per whole publish), with only the counts
         * for the rows in that chunk, so that a partial-batch failure partway through a large publish
         * leaves the summary row consistent with whatever was actually committed to
         * {@code caseResults} rather than silently out of sync. A build can also be published more
         * than once (e.g. multiple {@code junit} steps, or parallel stages each publishing a subset of
         * results), so this adds to any existing row rather than replacing it.
         * <p>Uses a single atomic dialect-specific upsert statement ({@code ON CONFLICT ... DO UPDATE}
         * on PostgreSQL, {@code ON DUPLICATE KEY UPDATE} on MySQL) rather than a portable
         * update-then-insert-then-retry pattern. That pattern has a real deadlock hazard under MySQL's
         * default REPEATABLE READ isolation: an UPDATE matching no row takes a gap lock on the
         * (job, build) key range, so two concurrent first publishes for the same brand-new build can
         * each acquire that lock and then deadlock on their following INSERT. The deadlock victim gets
         * SQLState 40001, which is not an integrity-constraint violation (class "23"), so it was not
         * recoverable by retrying as an update and instead had to propagate out, rolling back the
         * whole chunk's transaction (including the caseResults rows already batched in it). A single
         * atomic upsert statement has no separate insert-after-failed-update window, so this deadlock
         * shape cannot occur for either supported database.
         */
        private void upsertSummary(Connection connection, Span span, int passCount, int failCount, int skipCount,
                double duration) throws SQLException {
            if (passCount == 0 && failCount == 0 && skipCount == 0) {
                return;
            }
            boolean postgres = isPostgres(connection);
            var upsertSql = postgres
                    ? "INSERT INTO caseResultsSummary (job, build, passCount, failCount, skipCount, duration) "
                            + "VALUES (?, ?, ?, ?, ?, ?) "
                            + "ON CONFLICT (job, build) DO UPDATE SET "
                            + "passCount = caseResultsSummary.passCount + EXCLUDED.passCount, "
                            + "failCount = caseResultsSummary.failCount + EXCLUDED.failCount, "
                            + "skipCount = caseResultsSummary.skipCount + EXCLUDED.skipCount, "
                            + "duration = caseResultsSummary.duration + EXCLUDED.duration"
                    : "INSERT INTO caseResultsSummary (job, build, passCount, failCount, skipCount, duration) "
                            + "VALUES (?, ?, ?, ?, ?, ?) "
                            + "ON DUPLICATE KEY UPDATE "
                            + "passCount = passCount + VALUES(passCount), "
                            + "failCount = failCount + VALUES(failCount), "
                            + "skipCount = skipCount + VALUES(skipCount), "
                            + "duration = duration + VALUES(duration)";
            addSqlAttribute(span, upsertSql);
            try (PreparedStatement upsert = connection.prepareStatement(upsertSql)) {
                upsert.setString(1, StringUtils.truncate(job, MAX_JOB_LENGTH));
                upsert.setInt(2, build);
                upsert.setInt(3, passCount);
                upsert.setInt(4, failCount);
                upsert.setInt(5, skipCount);
                upsert.setDouble(6, duration);
                upsert.executeUpdate();
            }
        }
    }

    public static abstract class ConnectionSupplier implements AutoCloseable {

        protected abstract Database database();

        protected void initialize(Connection connection) throws SQLException {}

        /**
         * Returns a fresh connection borrowed from {@link Database#getDataSource()}'s pool.
         *
         * <p>Each call borrows a new (pooled, so normally already-established) connection rather than
         * sharing one cached instance: since a {@link Connection} cannot safely be used by more than
         * one thread at a time, a single shared connection would serialize all read traffic onto it
         * regardless of how many connections the underlying pool actually has available&mdash;a cheap,
         * unrelated query could queue for seconds behind an expensive one just because both happened to
         * go through the same connection. Returning a fresh connection per call instead lets
         * independent callers (every concurrent job/build/test-report page view, history/trend query,
         * etc.) run truly concurrently, up to the pool's configured size. Callers are expected to close
         * what they get, typically via try-with-resources.
         */
        Connection connection() throws SQLException {
            Connection _connection = database().getDataSource().getConnection();
            try {
                initialize(_connection);
            } catch (SQLException | RuntimeException e) {
                // initialize() failed after a connection was already borrowed from the pool: close it
                // here (returning it to the pool) since the caller never receives it and therefore has
                // no way to close it themselves, then rethrow so the original failure is preserved.
                try {
                    _connection.close();
                } catch (SQLException closeException) {
                    e.addSuppressed(closeException);
                }
                throw e;
            }
            return _connection;
        }

        @Override
        public void close() {
            // No-op: each caller is responsible for closing (returning to the pool) whatever
            // connection it borrowed from connection().
        }
    }

    static class LocalConnectionSupplier extends ConnectionSupplier {

        @Override
        protected Database database() {
            return GlobalDatabaseConfiguration.get().getDatabase();
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

        /**
         * Maximum idle connections kept by the agent-side connection pool, see
         * {@link RemoteDatabaseCache#prepareForAgent}. Read from the agent JVM's system properties.
         *
         * <p>An agent only borrows connections to publish test results, a few times per build, so an
         * idle pooled connection is almost never reused there. Kept idle (the default, 8, inherited
         * from {@link Database#getDataSource()}'s pool), it holds a database connection for the whole
         * life of the agent JVM. Worse, when an agent machine disappears without closing its sockets
         * (a terminated cloud or spot/preemptible VM), the database keeps those connections open
         * until its own idle timeout or TCP keepalive notices, which by default takes hours. With many
         * short-lived agents this exhausts {@code max_connections} on the database for every other
         * client. {@code maxTotal} still caps how many connections an agent can hold at once.
         *
         * <p>Not applied on the controller: a build on the built-in node publishes through the
         * controller's own shared pool, which serves every test result page and must keep its idle
         * connections.
         */
        static final int AGENT_MAX_IDLE = SystemProperties.getInteger(
                DatabaseTestResultStorage.class.getName() + ".agentMaxIdle", 0);

        private final Database database;

        RemoteConnectionSupplier() {
            database = GlobalDatabaseConfiguration.get().getDatabase();
        }

        /**
         * Returns the agent-local canonical {@link Database} for this configuration rather than the
         * just-deserialized {@code database} field directly.
         *
         * <p>{@link Database#getDataSource} lazily creates and caches a brand-new connection pool (up
         * to 8 JDBC connections by default) in a {@code transient} field the first time it is called
         * on a given {@link Database} <em>instance</em>. Since this object arrives on the agent via
         * Java serialization (see {@link SerializableOnlyOverRemoting}), every remote publish produces
         * a freshly deserialized {@link Database} instance with no transient state: calling
         * {@code getDataSource()} on it directly would spin up a brand-new connection pool per publish,
         * which is never closed (nothing ever calls the underlying {@code BasicDataSource.close()};
         * {@link Database} exposes no close hook at all), permanently leaking one pool per build. Over
         * the lifetime of a busy controller this would eventually exhaust the database's
         * {@code max_connections}.
         *
         * <p>Routing through {@link RemoteDatabaseCache} instead reuses the same {@link Database}
         * (and therefore the same bounded connection pool) for every publish from a given agent JVM
         * that shares the same connection settings, so the agent-side pool size stays capped at the
         * datasource's configured maximum no matter how many builds that agent ever runs.
         */
        @Override protected Database database() {
            return RemoteDatabaseCache.canonicalize(database);
        }
    }

    /**
     * Caches one {@link Database} (and thus one connection pool) per distinct connection
     * configuration, per agent JVM, so repeated remote publishes reuse pooled connections instead of
     * each permanently leaking a brand-new pool. See {@link RemoteConnectionSupplier#database()}.
     */
    static final class RemoteDatabaseCache {

        private static final ConcurrentHashMap<String, Database> CACHE = new ConcurrentHashMap<>();

        private RemoteDatabaseCache() {}

        static Database canonicalize(Database database) {
            String key = keyFor(database);
            // Unknown/custom Database implementations without a recognizable key are returned as-is:
            // no caching, but no change in (correct, if pool-leaking) behavior either.
            if (key == null) {
                return database;
            }
            // On an agent the pool is created from this cached instance, so any connection property
            // added here applies to every publish from the agent. Not on the controller, where this
            // is the global configuration object itself.
            return CACHE.computeIfAbsent(key,
                    k -> Jenkins.getInstanceOrNull() == null ? prepareForAgent(database) : database);
        }

        /**
         * Tunes a copy of {@code database} for agent-side use: enables the MySQL batch rewrite
         * (see {@link #withMySqlBatchRewrite}) and caps the idle connections its pool keeps open
         * (see {@link RemoteConnectionSupplier#AGENT_MAX_IDLE}) once, since the result is cached per
         * agent JVM by {@link #canonicalize}.
         */
        static Database prepareForAgent(Database database) {
            Database agentDatabase = withMySqlBatchRewrite(database);
            try {
                agentDatabase.setMaxIdleConnections(RemoteConnectionSupplier.AGENT_MAX_IDLE);
            } catch (SQLException | RuntimeException e) {
                log.log(Level.FINE, "Cannot limit idle connections; leaving them as they are", e);
            }
            return agentDatabase;
        }

        private static final String MYSQL_DATABASE_CLASS = "org.jenkinsci.plugins.database.mysql.MySQLDatabase";
        private static final String MYSQL_BATCH_REWRITE = "rewriteBatchedStatements";

        /**
         * A copy of a MySQL database configuration with Connector/J's {@code rewriteBatchedStatements}
         * enabled, unless the configuration already sets it either way; any other database as is.
         *
         * <p>Publishing inserts test cases in JDBC batches. Without this property Connector/J still
         * sends every statement of a batch to the server separately, one round trip each, which makes
         * publishing large test reports slow; with it each batch becomes a single multi-row
         * {@code INSERT}. {@code AbstractRemoteDatabase.properties} is final, hence the copy, made
         * through the same public constructor the configuration UI uses.
         */
        static Database withMySqlBatchRewrite(Database database) {
            if (!MYSQL_DATABASE_CLASS.equals(database.getClass().getName())) {
                return database;
            }
            var remote = (org.jenkinsci.plugins.database.AbstractRemoteDatabase) database;
            try {
                for (Object name : Util.loadProperties(Util.fixNull(remote.properties)).keySet()) {
                    if (MYSQL_BATCH_REWRITE.equalsIgnoreCase(name.toString())) {
                        return database;
                    }
                }
                String properties = Util.fixNull(remote.properties).strip();
                properties = (properties.isEmpty() ? "" : properties + "\n") + MYSQL_BATCH_REWRITE + "=true";
                var copy = (org.jenkinsci.plugins.database.AbstractRemoteDatabase) database.getClass()
                        .getConstructor(String.class, String.class, String.class, Secret.class, String.class)
                        .newInstance(remote.hostname, remote.database, remote.username, remote.password, properties);
                copy.setValidationQuery(remote.getValidationQuery());
                return copy;
            } catch (IOException | ReflectiveOperationException | RuntimeException e) {
                log.log(Level.FINE, "Cannot enable " + MYSQL_BATCH_REWRITE + "; using the configuration as is", e);
                return database;
            }
        }

        @CheckForNull
        private static String keyFor(Database database) {
            if (!(database instanceof org.jenkinsci.plugins.database.AbstractRemoteDatabase)) {
                return null;
            }
            org.jenkinsci.plugins.database.AbstractRemoteDatabase remote =
                    (org.jenkinsci.plugins.database.AbstractRemoteDatabase) database;
            // Use the Secret's encrypted representation (stable per-value, but not reversible to
            // plaintext) rather than the decrypted password, so the password never lives
            // indefinitely in plaintext as part of this static cache key (e.g. in heap dumps).
            return String.join("\u0000",
                    database.getClass().getName(),
                    String.valueOf(remote.hostname),
                    String.valueOf(remote.database),
                    String.valueOf(remote.username),
                    remote.password == null ? "null" : remote.password.getEncryptedValue(),
                    String.valueOf(remote.properties),
                    String.valueOf(remote.getValidationQuery()));
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
            // Each call borrows its own connection from the pool and returns it when done, so
            // concurrent reads (e.g. several test-report pages rendering at once) run on genuinely
            // separate connections instead of queueing behind one another; see the Javadoc on
            // ConnectionSupplier.connection() for the rationale.
            try (Connection connection = getConnectionSupplier().connection()) {
                return querier.run(connection);
            } catch (SQLException x) {
                throw new RuntimeException(x);
            }
        }

        List<CaseResult> getCaseResults() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getCaseResults",
                    span -> getEntry().getCaseResults(span));
        }

        private ResultsEntry getEntry() {
            CacheKey key = new CacheKey(job, build);
            return resultsCache.get(key, k -> new ResultsEntry(k, job, build));
        }

        void deleteRun() {
            withSpan("DatabaseTestResultStorage.TestResultStorage.deleteRun", span -> {
                log.info(String.format("Deleting test results and purging the cache for job %s #%d", job, build));
                invalidate(job, build);
                try {
                    return query(connection -> {
                        // Perform both deletes in a single transaction so that, with autocommit
                        // disabled, a failure in either delete rolls back the other instead of
                        // leaving a permanent orphan caseResultsSummary row (which would otherwise
                        // keep a deleted run visible in trend/history/count queries).
                        boolean originalAutoCommit = connection.getAutoCommit();
                        connection.setAutoCommit(false);
                        try {
                            var sql = "DELETE FROM caseResults WHERE job = ? AND build = ?";
                            addSqlAttribute(span, sql);
                            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                                statement.setString(1, job);
                                span.setAttribute("job", job);
                                statement.setInt(2, build);
                                span.setAttribute("build", build);
                                statement.execute();
                            }
                            var summarySql = "DELETE FROM caseResultsSummary WHERE job = ? AND build = ?";
                            addSqlAttribute(span, summarySql);
                            try (PreparedStatement statement = connection.prepareStatement(summarySql)) {
                                statement.setString(1, job);
                                statement.setInt(2, build);
                                statement.execute();
                            }
                            connection.commit();
                        } catch (SQLException | RuntimeException e) {
                            connection.rollback();
                            throw e;
                        } finally {
                            connection.setAutoCommit(originalAutoCommit);
                        }
                        return null;
                    });
                } finally {
                    // Invalidate again in case a concurrent read populated the cache from rows that
                    // have now been deleted, between the delete starting and finishing.
                    invalidate(job, build);
                }
            });
        }

        void deleteJob() {
            withSpan("DatabaseTestResultStorage.TestResultStorage.deleteJob", span -> {
                log.info(String.format("Deleting test results and purging the cache for job %s", job));
                invalidateJob(job);
                try {
                    return query(connection -> {
                        // See deleteRun(): same atomicity rationale applies to the job-wide deletes.
                        boolean originalAutoCommit = connection.getAutoCommit();
                        connection.setAutoCommit(false);
                        try {
                            var sql = "DELETE FROM caseResults WHERE job = ?";
                            addSqlAttribute(span, sql);
                            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                                statement.setString(1, job);
                                span.setAttribute("job", job);
                                statement.execute();
                            }
                            var summarySql = "DELETE FROM caseResultsSummary WHERE job = ?";
                            addSqlAttribute(span, summarySql);
                            try (PreparedStatement statement = connection.prepareStatement(summarySql)) {
                                statement.setString(1, job);
                                statement.execute();
                            }
                            connection.commit();
                        } catch (SQLException | RuntimeException e) {
                            connection.rollback();
                            throw e;
                        } finally {
                            connection.setAutoCommit(originalAutoCommit);
                        }
                        return null;
                    });
                } finally {
                    invalidateJob(job);
                }
            });
        }

        @Override
        public List<PackageResult> getAllPackageResults() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getAllPackageResults",
                    span -> getEntry().getPackageResults(span));
        }

        @Override
        public List<TrendTestResultSummary> getTrendTestResultSummary() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getTrendTestResultSummary", span ->
                query(connection -> {
                    // Reads the persisted per-build summary (maintained incrementally at publish time,
                    // see RemotePublisherImpl#upsertSummary) instead of aggregating every caseResults
                    // row for the job on every call, which used to cost proportionally to the job's
                    // total historical row count rather than its number of builds.
                    var sql = "SELECT build, passCount, failCount, skipCount "
                            + "FROM caseResultsSummary WHERE job = ? order by build;";
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
                // See getTrendTestResultSummary: reads the persisted summary instead of re-summing
                // every row for the job.
                var sql = "SELECT build, duration "
                        + "FROM caseResultsSummary "
                        + "WHERE job = ? order by build;";
                addSqlAttribute(span, sql);
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, job);
                    try (ResultSet result = statement.executeQuery()) {

                        List<TestDurationResultSummary> testDurationResultSummaries = new ArrayList<>();
                        while (result.next()) {
                            int buildNumber = result.getInt("build");
                            float duration = result.getFloat("duration");

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
                    // Reads the persisted summary (one row per build) instead of a GROUP BY over every
                    // caseResults row for the job. The previous query's LIMIT/OFFSET pagination still
                    // had to aggregate every row up to the current offset on every page load, so this
                    // matters increasingly as a job accumulates more history, not just more cases.
                    var sql = "SELECT build, duration, passCount, failCount, skipCount "
                            + "FROM caseResultsSummary "
                            + "WHERE job = ? ORDER BY build DESC LIMIT 25 OFFSET ?;";
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
                                float duration = result.getFloat("duration");
                                int passed = result.getInt("passCount");
                                int failed = result.getInt("failCount");
                                int skipped = result.getInt("skipCount");

                                Job<?, ?> theJob = Jenkins.get().getItemByFullName(getJobName(), Job.class);
                                if (theJob != null) {
                                    Run<?, ?> run = theJob.getBuildByNumber(buildNumber);
                                    // A row can exist here for a build whose publish was interrupted
                                    // before completion (caseResultsSummary is maintained incrementally
                                    // per flushed batch, not only once the whole build/publish finishes),
                                    // so the Run may have no AbstractTestResultAction attached yet.
                                    // HistoryTestResultSummary#getUrl() assumes that action is always
                                    // present and NPEs otherwise, which breaks the whole job's history
                                    // chart rather than just this one row - so skip such builds here.
                                    if (run != null && run.getAction(AbstractTestResultAction.class) != null) {
                                        historyTestResultSummaries.add(
                                                new HistoryTestResultSummary(run, duration, failed, skipped, passed));
                                    }
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
                        // Each build has exactly one row in caseResultsSummary, so a plain count
                        // replaces a COUNT(DISTINCT build) scan over every caseResults row for the job.
                        var sql = "SELECT COUNT(*) as count FROM caseResultsSummary WHERE job = ?;";
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
                ResultsEntry entry = getEntry();
                // Jelly views (e.g. the failed-tests list on the build/test-report pages) call this
                // once per failing case shown on the page, so on the very first call for this build,
                // eagerly compute "failed since" for every currently-failing case in as few round
                // trips as possible (see computeFailedSinceBatch), rather than one pair of queries per
                // case. Each individual query is index-backed and cheap on its own (a few
                // milliseconds), but hundreds of sequential round trips to a non-localhost database
                // multiply directly by per-round-trip network latency and can dominate page load time.
                ensureFailedSinceBatchComputed(span, entry);
                String cacheKey = failedSinceCacheKey(caseResult);
                Run<?, ?> cached = entry.failedSinceRunByTest.get(cacheKey);
                if (cached != null) {
                    return cached;
                }
                // Not covered by the batch (e.g. called for a currently-passing case, or a case not
                // present in this build's loaded case list): fall back to the original single-case
                // lookup, still memoized per test identity for this build.
                return entry.getFailedSinceRun(cacheKey, () -> computeFailedSinceRun(span, caseResult));
            });
        }

        private String failedSinceCacheKey(CaseResult caseResult) {
            return caseResult.getSuiteResult().getName() + '\0' + caseResult.getPackageName()
                    + '\0' + caseResult.getClassName() + '\0' + caseResult.getName();
        }

        /**
         * Runs {@link #computeFailedSinceBatch(Span, ResultsEntry)} at most once per {@link ResultsEntry}
         * (i.e. once per build per cache generation), guarded the same way as the other lazily-computed
         * fields on {@link ResultsEntry}.
         */
        private void ensureFailedSinceBatchComputed(Span span, ResultsEntry entry) {
            if (entry.failedSinceBatchComputed) {
                return;
            }
            synchronized (entry.failedSinceBatchLock) {
                if (entry.failedSinceBatchComputed) {
                    return;
                }
                computeFailedSinceBatch(span, entry);
                entry.failedSinceBatchComputed = true;
            }
        }

        /**
         * Computes "failed since" for every currently-failing case of this build in a bounded number
         * of SQL round trips (see {@link #FAILED_SINCE_BATCH_SIZE}), storing each result directly into
         * {@link ResultsEntry#failedSinceRunByTest} keyed the same way as {@link #getFailedSinceRun}.
         * Reuses the already-loaded case list (no extra query to enumerate failing cases, since the
         * Jelly views that call {@link #getFailedSinceRun} always first call {@link #getFailedTests()}
         * or otherwise already have the full case list loaded for this build).
         */
        private void computeFailedSinceBatch(Span span, ResultsEntry entry) {
            List<CaseResult> failing = entry.getCaseResults(span).stream()
                    .filter(caseResult -> caseResult.getErrorDetails() != null)
                    .collect(Collectors.toList());
            for (int start = 0; start < failing.size(); start += FAILED_SINCE_BATCH_SIZE) {
                List<CaseResult> chunk = failing.subList(start,
                        Math.min(start + FAILED_SINCE_BATCH_SIZE, failing.size()));
                computeFailedSinceBatchChunk(entry, chunk);
            }
        }

        private void computeFailedSinceBatchChunk(ResultsEntry entry, List<CaseResult> chunk) {
            query(connection -> {
                boolean postgres = isPostgres(connection);
                // See computeFailedSinceRun for why this predicate (and its bound job/classname/testname
                // parameters) is only needed on PostgreSQL.
                String identityHashPredicate = postgres
                        ? "AND testidentityhash = md5(coalesce(?, '') || chr(1) || coalesce(?, '') || chr(1) || coalesce(?, '')) "
                        : "";

                // Step 1: last passing build before this one, for every failing case in the chunk, in
                // one query built from one UNION ALL branch per case -- each branch is an exact copy of
                // the single-case query in computeFailedSinceRun, just labelled with its chunk index so
                // results can be matched back to the case they belong to.
                StringBuilder passingSql = new StringBuilder();
                for (int i = 0; i < chunk.size(); i++) {
                    if (i > 0) {
                        passingSql.append(" UNION ALL ");
                    }
                    passingSql.append("SELECT ").append(i).append(" AS idx, (SELECT build FROM caseResults ")
                            .append("WHERE job = ? AND build < ? AND suite = ? AND package = ? ")
                            .append("AND classname = ? AND testname = ? ")
                            .append(identityHashPredicate)
                            .append("AND errordetails IS NULL ORDER BY build DESC LIMIT 1) AS lastpassing");
                }
                var spanPassingBuild = createSpan("lastPassingBuildBatch");
                addSqlAttribute(spanPassingBuild, passingSql.toString());
                spanPassingBuild.setAttribute("batchSize", chunk.size());
                Map<Integer, Integer> lastPassingByIdx = new HashMap<>();
                try (PreparedStatement statement = connection.prepareStatement(passingSql.toString());
                     Scope ignore = spanPassingBuild.makeCurrent()) {
                    int paramIndex = 1;
                    for (CaseResult caseResult : chunk) {
                        paramIndex = bindFailedSinceParams(caseResult, build, statement, paramIndex, postgres);
                    }
                    try (ResultSet result = statement.executeQuery()) {
                        while (result.next()) {
                            int lastPassing = result.getInt("lastpassing");
                            if (!result.wasNull()) {
                                lastPassingByIdx.put(result.getInt("idx"), lastPassing);
                            }
                        }
                    }
                } finally {
                    spanPassingBuild.end();
                }

                Job<?, ?> theJob = Objects.requireNonNull(Jenkins.get().getItemByFullName(job, Job.class));

                // Cases that never passed before this build failed since their own earliest recorded
                // occurrence (step 2a, batched below) -- not unconditionally build #1, since a test
                // can be introduced, already failing, partway through the job's history. Everything
                // else needs the first failing build strictly after its own last passing build (step 2b).
                List<Integer> neverPassed = new ArrayList<>();
                List<Integer> needsFailingLookup = new ArrayList<>();
                for (int i = 0; i < chunk.size(); i++) {
                    if (!lastPassingByIdx.containsKey(i)) {
                        neverPassed.add(i);
                    } else {
                        needsFailingLookup.add(i);
                    }
                }

                if (!neverPassed.isEmpty()) {
                    StringBuilder earliestSql = new StringBuilder();
                    for (int j = 0; j < neverPassed.size(); j++) {
                        if (j > 0) {
                            earliestSql.append(" UNION ALL ");
                        }
                        earliestSql.append("SELECT ").append(j).append(" AS idx, (SELECT MIN(build) FROM caseResults ")
                                .append("WHERE job = ? AND suite = ? AND package = ? AND classname = ? AND testname = ? ")
                                .append(identityHashPredicate)
                                .append(") AS earliestbuild");
                    }
                    var spanEarliestBuild = createSpan("earliestBuildBatch");
                    addSqlAttribute(spanEarliestBuild, earliestSql.toString());
                    spanEarliestBuild.setAttribute("batchSize", neverPassed.size());
                    try (PreparedStatement statement = connection.prepareStatement(earliestSql.toString());
                         Scope ignore = spanEarliestBuild.makeCurrent()) {
                        int paramIndex = 1;
                        for (int idx : neverPassed) {
                            CaseResult caseResult = chunk.get(idx);
                            statement.setString(paramIndex++, job);
                            statement.setString(paramIndex++, caseResult.getSuiteResult().getName());
                            statement.setString(paramIndex++, caseResult.getPackageName());
                            statement.setString(paramIndex++, caseResult.getClassName());
                            statement.setString(paramIndex++, caseResult.getName());
                            if (postgres) {
                                statement.setString(paramIndex++, job);
                                statement.setString(paramIndex++, caseResult.getClassName());
                                statement.setString(paramIndex++, caseResult.getName());
                            }
                        }
                        try (ResultSet result = statement.executeQuery()) {
                            while (result.next()) {
                                int pos = result.getInt("idx");
                                int earliest = result.getInt("earliestbuild");
                                // MIN(build) should never be null -- this build itself always matches
                                // its own identity -- but fall back to the current build defensively.
                                int resolved = result.wasNull() ? build : earliest;
                                CaseResult caseResult = chunk.get(neverPassed.get(pos));
                                Run<?, ?> run = theJob.getBuildByNumber(resolved);
                                // getBuildByNumber can return null for a discarded build; only cache
                                // a hit, since ConcurrentHashMap#putIfAbsent rejects null values.
                                if (run != null) {
                                    entry.failedSinceRunByTest.putIfAbsent(failedSinceCacheKey(caseResult), run);
                                }
                            }
                        }
                    } finally {
                        spanEarliestBuild.end();
                    }
                }

                if (!needsFailingLookup.isEmpty()) {
                    StringBuilder failingSql = new StringBuilder();
                    for (int j = 0; j < needsFailingLookup.size(); j++) {
                        if (j > 0) {
                            failingSql.append(" UNION ALL ");
                        }
                        failingSql.append("SELECT ").append(j).append(" AS idx, (SELECT build FROM caseResults ")
                                .append("WHERE job = ? AND build > ? AND suite = ? AND package = ? ")
                                .append("AND classname = ? AND testname = ? ")
                                .append(identityHashPredicate)
                                .append("AND errordetails IS NOT NULL ORDER BY build ASC LIMIT 1) AS firstfailing");
                    }
                    var spanFailingBuild = createSpan("firstFailingBuildBatch");
                    addSqlAttribute(spanFailingBuild, failingSql.toString());
                    spanFailingBuild.setAttribute("batchSize", needsFailingLookup.size());
                    Map<Integer, Integer> firstFailingByPos = new HashMap<>();
                    try (PreparedStatement statement = connection.prepareStatement(failingSql.toString());
                         Scope ignore = spanFailingBuild.makeCurrent()) {
                        int paramIndex = 1;
                        for (int idx : needsFailingLookup) {
                            paramIndex = bindFailedSinceParams(
                                    chunk.get(idx), lastPassingByIdx.get(idx), statement, paramIndex, postgres);
                        }
                        try (ResultSet result = statement.executeQuery()) {
                            while (result.next()) {
                                firstFailingByPos.put(result.getInt("idx"), result.getInt("firstfailing"));
                            }
                        }
                    } finally {
                        spanFailingBuild.end();
                    }
                    for (int j = 0; j < needsFailingLookup.size(); j++) {
                        int idx = needsFailingLookup.get(j);
                        CaseResult caseResult = chunk.get(idx);
                        Integer firstFailingBuild = firstFailingByPos.get(j);
                        // firstFailingBuild should always be present (the current, currently-failing
                        // build itself always satisfies "build > lastPassing AND errordetails IS NOT
                        // NULL"); fall back to the current build rather than failing the whole page if
                        // that invariant is ever violated (e.g. unexpected concurrent data changes).
                        Run<?, ?> run = theJob.getBuildByNumber(firstFailingBuild != null ? firstFailingBuild : build);
                        // getBuildByNumber can return null for a discarded build; only cache a hit,
                        // since ConcurrentHashMap#putIfAbsent rejects null values.
                        if (run != null) {
                            entry.failedSinceRunByTest.putIfAbsent(failedSinceCacheKey(caseResult), run);
                        }
                    }
                }
                return null;
            });
        }

        private int bindFailedSinceParams(CaseResult caseResult, int build, PreparedStatement preparedStatement,
                int paramIndex, boolean postgres) throws SQLException {
            preparedStatement.setString(paramIndex++, job);
            preparedStatement.setInt(paramIndex++, build);
            preparedStatement.setString(paramIndex++, caseResult.getSuiteResult().getName());
            preparedStatement.setString(paramIndex++, caseResult.getPackageName());
            preparedStatement.setString(paramIndex++, caseResult.getClassName());
            preparedStatement.setString(paramIndex++, caseResult.getName());
            if (postgres) {
                preparedStatement.setString(paramIndex++, job);
                preparedStatement.setString(paramIndex++, caseResult.getClassName());
                preparedStatement.setString(paramIndex++, caseResult.getName());
            }
            return paramIndex;
        }

        @Override
        public boolean supportsPreviousCaseResultLookup() {
            return true;
        }

        @Override
        public Optional<CaseResult> getPreviousCaseResult(CaseResult current) {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getPreviousCaseResult", span -> {
                span.setAttribute("build", build);
                span.setAttribute("job", job);
                ResultsEntry entry = getEntry();
                // CaseResult#getPreviousResult() is called once per case shown on a test-report page
                // (not just failing ones) to compute per-case "age"/regression info, and its default
                // implementation walks up to PREVIOUS_TEST_RESULT_BACKTRACK_BUILDS_MAX historical
                // builds per case, each needing its own suite-scoped query -- for a build with
                // thousands of cases this previously multiplied into tens of thousands of suite loads
                // for a single page render. Eagerly resolve every case's previous result for this
                // build up front in a bounded number of round trips instead (see
                // computePreviousCaseResultBatch), the same technique used for "failed since" above.
                ensurePreviousCaseResultBatchComputed(span, entry);
                // Absence here means "the batch determined there is no previous result" (the normal
                // case for most tests, since the batch is seeded from this build's own full case
                // list), not "not yet computed" -- so this never needs the single-case fallback that
                // getFailedSinceRun uses, as long as current belongs to this build's case list.
                return Optional.ofNullable(entry.previousCaseResultByTest.get(previousCaseResultCacheKey(current)));
            });
        }

        private String previousCaseResultCacheKey(CaseResult caseResult) {
            return caseResult.getSuiteResult().getName() + '\0' + caseResult.getPackageName()
                    + '\0' + caseResult.getClassName() + '\0' + caseResult.getName();
        }

        /**
         * Runs {@link #computePreviousCaseResultBatch(Span, ResultsEntry)} at most once per
         * {@link ResultsEntry}, guarded the same way as {@link #ensureFailedSinceBatchComputed}.
         */
        private void ensurePreviousCaseResultBatchComputed(Span span, ResultsEntry entry) {
            if (entry.previousCaseResultBatchComputed) {
                return;
            }
            synchronized (entry.previousCaseResultBatchLock) {
                if (entry.previousCaseResultBatchComputed) {
                    return;
                }
                computePreviousCaseResultBatch(span, entry);
                entry.previousCaseResultBatchComputed = true;
            }
        }

        /**
         * Resolves "previous result" for every case of this build in a bounded number of round trips:
         * one query to find the (at most {@link #PREVIOUS_CASE_RESULT_BACKTRACK_BUILDS_MAX}) candidate
         * historical builds, then one {@code UNION ALL}-based identity-match query per
         * {@link #PREVIOUS_CASE_RESULT_BATCH_SIZE}-sized chunk of cases to find which of those builds
         * (if any) contains a matching case for each -- instead of one suite load per historical build
         * visited per case.
         */
        private void computePreviousCaseResultBatch(Span span, ResultsEntry entry) {
            List<CaseResult> cases = entry.getCaseResults(span);
            if (cases.isEmpty()) {
                return;
            }
            // Builds already present in caseResults are exactly the builds that published results, so
            // the lowest of the most recent PREVIOUS_CASE_RESULT_BACKTRACK_BUILDS_MAX distinct builds
            // below this one, used as a numeric lower bound, covers precisely the same set of builds
            // as an explicit IN-list would -- without needing to bind one parameter per candidate.
            List<Integer> candidateBuilds = getPreviousBuildNumbers(PREVIOUS_CASE_RESULT_BACKTRACK_BUILDS_MAX);
            if (candidateBuilds.isEmpty()) {
                return;
            }
            int minCandidateBuild = Collections.min(candidateBuilds);
            for (int start = 0; start < cases.size(); start += PREVIOUS_CASE_RESULT_BATCH_SIZE) {
                List<CaseResult> chunk = cases.subList(start,
                        Math.min(start + PREVIOUS_CASE_RESULT_BATCH_SIZE, cases.size()));
                computePreviousCaseResultBatchChunk(entry, chunk, minCandidateBuild);
            }
        }

        /** The at most {@code limit} most recent distinct build numbers below this build in {@code caseResults}. */
        private List<Integer> getPreviousBuildNumbers(int limit) {
            return query(connection -> {
                var sql =
                        "SELECT DISTINCT build FROM caseResults WHERE job = ? AND build < ? ORDER BY build DESC LIMIT ?";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, job);
                    statement.setInt(2, build);
                    statement.setInt(3, limit);
                    try (ResultSet result = statement.executeQuery()) {
                        List<Integer> builds = new ArrayList<>();
                        while (result.next()) {
                            builds.add(result.getInt("build"));
                        }
                        return builds;
                    }
                }
            });
        }

        private void computePreviousCaseResultBatchChunk(ResultsEntry entry, List<CaseResult> chunk,
                int minCandidateBuild) {
            Map<Integer, Integer> foundBuildByIdx = query(connection -> {
                boolean postgres = isPostgres(connection);
                // See computeFailedSinceRun for why this predicate is only needed on PostgreSQL.
                String identityHashPredicate = postgres
                        ? "AND testidentityhash = md5(coalesce(?, '') || chr(1) || coalesce(?, '') || chr(1) || coalesce(?, '')) "
                        : "";
                StringBuilder sql = new StringBuilder();
                for (int i = 0; i < chunk.size(); i++) {
                    if (i > 0) {
                        sql.append(" UNION ALL ");
                    }
                    sql.append("SELECT ").append(i).append(" AS idx, (SELECT MAX(build) FROM caseResults ")
                            .append("WHERE job = ? AND build >= ? AND build < ? AND suite = ? AND package = ? ")
                            .append("AND classname = ? AND testname = ? ")
                            .append(identityHashPredicate)
                            .append(") AS foundbuild");
                }
                Map<Integer, Integer> result = new HashMap<>();
                var spanBatch = createSpan("previousCaseResultBatch");
                addSqlAttribute(spanBatch, sql.toString());
                spanBatch.setAttribute("batchSize", chunk.size());
                try (PreparedStatement statement = connection.prepareStatement(sql.toString());
                     Scope ignore = spanBatch.makeCurrent()) {
                    int paramIndex = 1;
                    for (CaseResult caseResult : chunk) {
                        paramIndex = bindPreviousCaseResultParams(
                                caseResult, minCandidateBuild, build, statement, paramIndex, postgres);
                    }
                    try (ResultSet resultSet = statement.executeQuery()) {
                        while (resultSet.next()) {
                            int found = resultSet.getInt("foundbuild");
                            if (!resultSet.wasNull()) {
                                result.put(resultSet.getInt("idx"), found);
                            }
                        }
                    }
                } finally {
                    spanBatch.end();
                }
                return result;
            });

            for (int i = 0; i < chunk.size(); i++) {
                Integer foundBuild = foundBuildByIdx.get(i);
                if (foundBuild == null) {
                    continue;
                }
                CaseResult caseResult = chunk.get(i);
                CaseResult previous = materializePreviousCaseResult(foundBuild, caseResult);
                if (previous != null) {
                    entry.previousCaseResultByTest.put(previousCaseResultCacheKey(caseResult), previous);
                }
            }
        }

        private int bindPreviousCaseResultParams(CaseResult caseResult, int minBuildInclusive, int maxBuildExclusive,
                PreparedStatement preparedStatement, int paramIndex, boolean postgres) throws SQLException {
            preparedStatement.setString(paramIndex++, job);
            preparedStatement.setInt(paramIndex++, minBuildInclusive);
            preparedStatement.setInt(paramIndex++, maxBuildExclusive);
            preparedStatement.setString(paramIndex++, caseResult.getSuiteResult().getName());
            preparedStatement.setString(paramIndex++, caseResult.getPackageName());
            preparedStatement.setString(paramIndex++, caseResult.getClassName());
            preparedStatement.setString(paramIndex++, caseResult.getName());
            if (postgres) {
                preparedStatement.setString(paramIndex++, job);
                preparedStatement.setString(paramIndex++, caseResult.getClassName());
                preparedStatement.setString(paramIndex++, caseResult.getName());
            }
            return paramIndex;
        }

        /**
         * Builds the actual {@link CaseResult} for a historical build already known (via
         * {@link #computePreviousCaseResultBatchChunk}) to contain a matching case, by loading just
         * that one suite (memoized per build/suite the same way {@link #getSuite(String)} always is,
         * so repeated calls for the same historical build+suite across many cases of the current
         * build only load it once) and looking the specific case up by its transformed display name,
         * exactly as the default {@code CaseResult#getPreviousResult()} walk would have.
         */
        private CaseResult materializePreviousCaseResult(int foundBuild, CaseResult caseResult) {
            SuiteResult suite =
                    new TestResultStorage(job, foundBuild).getSuite(caseResult.getSuiteResult().getName());
            return suite == null ? null : suite.getCase(caseResult.getTransformedFullDisplayName());
        }

        private Run<?, ?> computeFailedSinceRun(Span span, CaseResult caseResult) {
            return query(connection -> {
                var spanPassingBuild = createSpan("lastPassingBuild");
                int lastPassingBuildNumber;
                Job<?, ?> theJob = Objects.requireNonNull(Jenkins.get().getItemByFullName(job, Job.class));
                // On PostgreSQL, failed_since_index is keyed on a bounded-size hash of
                // (job, classname, testname) rather than those columns directly, since PostgreSQL btree
                // indexes have a hard per-entry size limit that long/multibyte values in those columns
                // can exceed (see V2026_10_07_0733__failed-since-index.sql in db/migration/postgres).
                // MySQL supports indexing a length-limited column prefix instead, so its equivalent
                // index can cover the full columns directly and does not need this. The original
                // job/classname/testname equality filters are kept in both cases as exact-value
                // residual checks, so a hash collision between two different tests cannot produce an
                // incorrect match.
                boolean postgres = isPostgres(connection);
                // Computed via Postgres's own md5(...) expression over bound parameters, with the
                // exact same expression the testidentityhash generated column uses (see
                // V2026_10_07_0733__failed-since-index.sql), rather than hashing client-side in Java.
                // Postgres's md5(text) hashes bytes in the database's server_encoding, not
                // necessarily UTF-8 (e.g. a LATIN1 database), so a client-side MD5 computed over UTF-8
                // bytes would not reliably match the generated column's value on such databases;
                // computing both sides with the same SQL expression avoids any encoding assumption.
                String identityHashPredicate = postgres
                        ? "AND testidentityhash = md5(coalesce(?, '') || chr(1) || coalesce(?, '') || chr(1) || coalesce(?, '')) "
                        : "";
                String sqlPassingBuild = "SELECT build " +
                        "FROM caseResults " +
                        "WHERE job = ? " +
                        "AND build < ? " +
                        "AND suite = ? " +
                        "AND package = ? " +
                        "AND classname = ? " +
                        "AND testname = ? " +
                        identityHashPredicate +
                        "AND errordetails IS NULL " +
                        "ORDER BY BUILD DESC " +
                        "LIMIT 1";
                addSqlAttribute(spanPassingBuild, sqlPassingBuild);
                try (PreparedStatement statement = connection.prepareStatement(sqlPassingBuild);
                     Scope ignore = spanPassingBuild.makeCurrent()) {
                    addCaseResultToStatement(caseResult, build, statement, postgres);
                    try (ResultSet result = statement.executeQuery()) {
                        boolean hasPassed = result.next();
                        if (!hasPassed) {
                            // Never passed (in the data retained so far): failed since the earliest
                            // build that actually contains this test identity, not build #1 -- the
                            // test may have been introduced, e.g., in build #100, in which case that
                            // is the correct "failed since" build, matching what the build-by-build
                            // walk in CaseResult.getPreviousResult() would have found.
                            return theJob.getBuildByNumber(earliestBuildForIdentity(connection, caseResult, postgres));
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
                        identityHashPredicate +
                        "AND errordetails is NOT NULL " +
                        "ORDER BY BUILD ASC " +
                        "LIMIT 1";
                addSqlAttribute(spanFailingBuild, sqlFailedBuild);
                try (PreparedStatement statement = connection.prepareStatement(sqlFailedBuild);
                     Scope ignore = spanFailingBuild.makeCurrent()) {
                    addCaseResultToStatement(caseResult, lastPassingBuildNumber, statement, postgres);
                    try (ResultSet result = statement.executeQuery()) {
                        result.next();
                        int firstFailingBuildAfterPassing = result.getInt("build");
                        return theJob.getBuildByNumber(firstFailingBuildAfterPassing);
                    }
                } finally {
                    spanFailingBuild.end();
                }
            });
        }

        private void addCaseResultToStatement(CaseResult caseResult, int build, PreparedStatement preparedStatement,
                boolean postgres) throws SQLException {
            preparedStatement.setString(1, job);
            preparedStatement.setInt(2, build);
            preparedStatement.setString(3, caseResult.getSuiteResult().getName());
            preparedStatement.setString(4, caseResult.getPackageName());
            preparedStatement.setString(5, caseResult.getClassName());
            preparedStatement.setString(6, caseResult.getName());
            if (postgres) {
                preparedStatement.setString(7, job);
                preparedStatement.setString(8, caseResult.getClassName());
                preparedStatement.setString(9, caseResult.getName());
            }
        }

        /**
         * Finds the earliest build that contains this exact test identity (any status), for a test
         * that has no earlier passing build recorded. Since it never passed, that earliest occurrence
         * is itself the first failing build -- including when the test was introduced partway through
         * the job's history (e.g. first appearing, already failing, in build #100), rather than
         * unconditionally blaming build #1 as if the test had always existed.
         */
        private int earliestBuildForIdentity(Connection connection, CaseResult caseResult, boolean postgres)
                throws SQLException {
            String identityHashPredicate = postgres
                    ? "AND testidentityhash = md5(coalesce(?, '') || chr(1) || coalesce(?, '') || chr(1) || coalesce(?, '')) "
                    : "";
            String sql = "SELECT MIN(build) AS build FROM caseResults " +
                    "WHERE job = ? AND suite = ? AND package = ? AND classname = ? AND testname = ? " +
                    identityHashPredicate;
            var span = createSpan("earliestBuildForIdentity");
            addSqlAttribute(span, sql);
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 Scope ignore = span.makeCurrent()) {
                int paramIndex = 1;
                statement.setString(paramIndex++, job);
                statement.setString(paramIndex++, caseResult.getSuiteResult().getName());
                statement.setString(paramIndex++, caseResult.getPackageName());
                statement.setString(paramIndex++, caseResult.getClassName());
                statement.setString(paramIndex++, caseResult.getName());
                if (postgres) {
                    statement.setString(paramIndex++, job);
                    statement.setString(paramIndex++, caseResult.getClassName());
                    statement.setString(paramIndex++, caseResult.getName());
                }
                try (ResultSet result = statement.executeQuery()) {
                    result.next();
                    int earliest = result.getInt("build");
                    // MIN(build) should never be null here -- this build itself always matches its own
                    // identity -- but fall back to the current build rather than propagating a bogus 0
                    // if that invariant is ever violated (e.g. unexpected concurrent data changes).
                    return result.wasNull() ? build : earliest;
                }
            } finally {
                span.end();
            }
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
                log.fine(() -> String.format("Getting suite result for suite %s from case results.", suiteName));
                // Memoized per build: CaseResult#getPreviousResult() looks up the previous build's suite
                // once per case of the current build (e.g. TestResult#getFixedCount()/#getRegressionCount()
                // on the build's test report page walk every case). Rebuilding the suite on every call made
                // one page render cost the sum over cases of their suite size -- for a build with 300k cases
                // in suites of up to several thousand cases, hundreds of millions of objects, which never
                // finished. A built suite is the same for every caller until the entry is invalidated.
                ResultsEntry entry = getEntry();
                SuiteResult cached = entry.suiteResultsByName.get(suiteName);
                if (cached != null) {
                    return cached;
                }
                SuiteResult suiteResult = new SuiteResult(suiteName, null, null, null);
                // Looked up by suite name (not iterated), e.g. by CaseResult#getPreviousResult() walking
                // up to PREVIOUS_TEST_RESULT_BACKTRACK_BUILDS_MAX historical builds per failing test to
                // compute "age"/"failed since". A single suite is typically a small fraction of a large
                // build's cases (one of possibly thousands of packages/suites), so prefer any case list or
                // suite index already resident for this build (e.g. because its own full report was
                // already rendered, which needs every suite anyway) and otherwise issue a narrow,
                // suite-scoped query -- instead of hydrating every other suite's cases (and their stdout/
                // stderr/stacktrace text) purely to resolve one historical suite lookup.
                entry.getCasesForSuite(span, suiteName)
                        .forEach(caseResult -> {
                            TestResult testResult = new TestResult(this);
                            String packageName = caseResult.getPackageName();
                            String className = caseResult.getClassName();
                            populateSuiteResult(caseResult, testResult, className, packageName, suiteResult);
                        });
                // Built outside the map so that the database query above does not run under a map lock;
                // a concurrent caller that built the same suite first wins and both return its instance.
                SuiteResult existing = entry.suiteResultsByName.putIfAbsent(suiteName, suiteResult);
                return existing != null ? existing : suiteResult;
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
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getTotalTestDuration",
                    span -> getEntry().getSummary(span).duration);
        }

        @Override
        public int getFailCount() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getFailCount",
                    span -> getEntry().getSummary(span).failed);
        }

        @Override
        public int getSkipCount() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getSkipCount",
                    span -> getEntry().getSummary(span).skipped);
        }

        @Override
        public int getPassCount() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getPassCount",
                    span -> getEntry().getSummary(span).passed);
        }

        @Override
        public int getTotalCount() {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getTotalCount",
                    span -> getEntry().getSummary(span).total);
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
            return withSpan("DatabaseTestResultStorage.TestResultStorage.getPreviousResult",
                    span -> getEntry().getPreviousResult(span).orElse(null));
        }

        @NonNull
        @Override
        public TestResult getResultByNodes(@NonNull List<String> nodeIds) {
            return new TestResult(this); // TODO
        }
    }

    /**
     * Holds the lazily-computed, memoized results for one build: the full case list, the package
     * list derived from it, and the SQL-aggregated summary (counts/duration). Each is computed at
     * most once per cache generation, regardless of how many {@link TestResultStorage} instances or
     * accessor calls request it (e.g. the several {@code getXCount()} calls JUnit makes per build).
     * A new generation is created by {@link #invalidate(String, int)}/{@link #invalidateJob(String)}
     * evicting this entry from {@link #resultsCache}, or by the fixed one-minute expiry.
     */
    private final class ResultsEntry {
        private final CacheKey key;
        private final String job;
        private final int build;
        /** Used only to build {@link TestResult} parents; any instance for this job/build will do. */
        private final TestResultStorage self;

        private volatile List<CaseResult> caseResults;
        private final Object caseResultsLock = new Object();

        private volatile List<PackageResult> packageResults;
        private final Object packageResultsLock = new Object();

        private volatile BuildSummary summary;
        private final Object summaryLock = new Object();

        /**
         * Case results grouped by suite name, memoized once the full case list has actually been loaded
         * (by {@link #getCaseResults(Span)} or indirectly via {@link #getPackageResults(Span)}/
         * {@link #getSummary(Span)}} triggering it), so that once a build's full case list is resident
         * anyway, repeated single-suite lookups (see {@link #getCasesForSuite(Span, String)}) are O(1)
         * map lookups instead of an O(n) scan of every case in the build for each lookup.
         */
        private volatile Map<String, List<CaseResult>> casesBySuiteName;
        private final Object casesBySuiteNameLock = new Object();

        /**
         * Narrow, suite-scoped case lists loaded directly from the database without hydrating the rest
         * of the build, keyed by suite name. Used by {@link #getCasesForSuite(Span, String)} only when
         * the full case list for this build has <em>not</em> been loaded; populated independently per
         * suite, so distinct suites looked up before any full load can each be loaded (and cached) with
         * at most one query, without forcing a full-build load. Once the full case list is loaded,
         * {@link #casesBySuiteName} becomes authoritative instead and this map is no longer consulted
         * (existing entries are simply left in place rather than removed, since the full load already
         * carries a strictly larger weight).
         */
        private final Map<String, List<CaseResult>> partialCasesBySuiteName = new ConcurrentHashMap<>();

        /**
         * Running total of case counts loaded via {@link #partialCasesBySuiteName}, used by
         * {@link #weight()} as a lower-bound memory proxy before (or absent) a full case-list load. Not
         * decremented if a suite is ever reloaded (it isn't, since {@link ConcurrentHashMap#computeIfAbsent}
         * loads each suite name at most once), so this can only grow, matching the "never under-count
         * retained memory" contract that {@link #weight()} relies on for cache eviction.
         */
        private final AtomicInteger partialWeight = new AtomicInteger();

        /**
         * Suites built by {@link TestResultStorage#getSuite(String)}, keyed by suite name, so that each
         * suite of this build is assembled at most once per cache generation. Holds the same
         * {@link CaseResult} instances as the case lists above, so it adds no case rows to the weight.
         */
        private final Map<String, SuiteResult> suiteResultsByName = new ConcurrentHashMap<>();

        /**
         * Memoized result of looking up the previous build's {@link TestResult}, so that repeated
         * calls (e.g. one per {@link CaseResult#getPreviousResult()} invocation, which can happen once
         * per case in a build while walking up to {@code PREVIOUS_TEST_RESULT_BACKTRACK_BUILDS_MAX}
         * builds of history) issue a single "find the previous build" SQL query per cache generation
         * instead of one query per case. Wrapped in {@link Optional} so that "not yet computed"
         * ({@code null} field) is distinguishable from "computed, and there is no previous build"
         * ({@link Optional#empty()}).
         */
        private volatile Optional<TestResult> previousResult;
        private final Object previousResultLock = new Object();

        /**
         * Memoizes {@link #getFailedSinceRun(String, Supplier)} results per test identity within this
         * build, so that Jelly views listing many failing cases (each of which looks up its own "failed
         * since" build) issue at most one pair of SQL queries per unique failing test per cache
         * generation, instead of repeating them on every render of the same build's page.
         */
        private final Map<String, Run<?, ?>> failedSinceRunByTest = new ConcurrentHashMap<>();

        /**
         * Guards {@code computeFailedSinceBatch}: set once the batched "failed since" computation has
         * been attempted for this build (successfully or not), so it runs at most once per cache
         * generation rather than once per failing case.
         */
        private volatile boolean failedSinceBatchComputed = false;
        private final Object failedSinceBatchLock = new Object();

        /**
         * Memoizes {@link TestResultStorage#getPreviousCaseResult(CaseResult)} results per test
         * identity within this build, analogous to {@link #failedSinceRunByTest}. Absence after
         * {@link #previousCaseResultBatchComputed} is set means "determined there is no previous
         * result", not "not yet computed".
         */
        private final Map<String, CaseResult> previousCaseResultByTest = new ConcurrentHashMap<>();

        /**
         * Guards {@code computePreviousCaseResultBatch}: set once the batched "previous case result"
         * computation has been attempted for this build, so it runs at most once per cache generation
         * rather than once per case.
         */
        private volatile boolean previousCaseResultBatchComputed = false;
        private final Object previousCaseResultBatchLock = new Object();

        /** Cache weight (approximately the case count); 1 until the case list is actually loaded. */
        private volatile int weight = 1;

        ResultsEntry(CacheKey key, String job, int build) {
            this.key = key;
            this.job = job;
            this.build = build;
            this.self = new TestResultStorage(job, build);
        }

        int weight() {
            // The full case list, once loaded, is authoritative and strictly a superset of whatever
            // partial/suite-scoped loading happened before it, so prefer it when present.
            if (caseResults != null) {
                return weight;
            }
            return Math.max(1, partialWeight.get());
        }

        private <T> T query(Querier<T> querier) {
            // See TestResultStorage.query(): borrow/return a connection per call rather than sharing
            // one cached connection across every concurrent caller.
            try (Connection connection = getConnectionSupplier().connection()) {
                return querier.run(connection);
            } catch (SQLException x) {
                throw new RuntimeException(x);
            }
        }

        List<CaseResult> getCaseResults(Span span) {
            List<CaseResult> local = caseResults;
            if (local != null) {
                return local;
            }
            synchronized (caseResultsLock) {
                local = caseResults;
                if (local == null) {
                    local = loadCaseResultsFromDB(span);
                    caseResults = local;
                    // The entry was inserted into resultsCache with a placeholder weight of 1 before
                    // any data was loaded (its actual size wasn't known yet); now that the case list
                    // is loaded, update the weight and re-insert so Caffeine re-weighs this entry and
                    // evicts older entries if the cache-wide case-count budget is now exceeded.
                    weight = Math.max(1, local.size());
                    resultsCache.put(key, this);
                }
                return local;
            }
        }

        Map<String, List<CaseResult>> getCasesBySuiteName(Span span) {
            Map<String, List<CaseResult>> local = casesBySuiteName;
            if (local != null) {
                return local;
            }
            synchronized (casesBySuiteNameLock) {
                local = casesBySuiteName;
                if (local == null) {
                    local = getCaseResults(span).stream()
                            .collect(Collectors.groupingBy(caseResult -> caseResult.getSuiteResult().getName()));
                    casesBySuiteName = local;
                }
                return local;
            }
        }

        /**
         * Above this many distinct suites requested narrowly for the same not-yet-fully-loaded build,
         * fall back to a single full-build load instead of continuing to issue one query per suite.
         * Bounds the worst case for builds whose failing tests are spread across many suites (e.g. a
         * historical build walked once per failing test in a large current build): a pathological
         * fan-out of per-suite queries is capped at this many round trips before paying once for a full
         * load, rather than growing unbounded with the number of distinct suites ever requested.
         */
        private static final int MAX_PARTIAL_SUITES_BEFORE_FULL_LOAD = 25;

        /**
         * Returns the cases belonging to one suite, preferring whatever is already resident (the full
         * case list/suite index, if this build's full report has already been loaded for some other
         * reason) and otherwise issuing a single suite-scoped SQL query rather than hydrating every
         * other suite's cases (and their stdout/stderr/stacktrace text) purely to resolve one suite.
         * This is the dominant lookup used by {@link CaseResult#getPreviousResult()} walking historical
         * builds: it visits one suite of one build at a time, so paying for a full-build load there would
         * multiply the cost of history/age computation by however many historical builds are walked.
         * See {@link #MAX_PARTIAL_SUITES_BEFORE_FULL_LOAD} for the fallback once too many distinct
         * suites have been requested this way for the same build.
         */
        List<CaseResult> getCasesForSuite(Span span, String suiteName) {
            Map<String, List<CaseResult>> bySuiteName = casesBySuiteName;
            if (bySuiteName != null) {
                return bySuiteName.getOrDefault(suiteName, Collections.emptyList());
            }
            if (caseResults != null) {
                // The full case list is resident but the suite index hasn't been built yet; build it now
                // (also serving any other suite lookups against this entry) instead of querying per-suite.
                return getCasesBySuiteName(span).getOrDefault(suiteName, Collections.emptyList());
            }
            if (partialCasesBySuiteName.containsKey(suiteName)
                    || partialCasesBySuiteName.size() < MAX_PARTIAL_SUITES_BEFORE_FULL_LOAD) {
                List<CaseResult> loaded = partialCasesBySuiteName.computeIfAbsent(suiteName, name -> {
                    List<CaseResult> rows = loadCasesForSuiteFromDB(span, name);
                    partialWeight.addAndGet(Math.max(1, rows.size()));
                    // Re-insert so Caffeine re-weighs this entry against the cache-wide case-count
                    // budget, mirroring what a full load does in getCaseResults(Span); without this,
                    // memory held by narrow suite loads (which can still carry large stdout/stderr/
                    // stacktrace text) would not count against the cache's weight-based eviction at all.
                    resultsCache.put(key, this);
                    return rows;
                });
                // Return the just-loaded (or already-partial) result even if this call was the one that
                // reached MAX_PARTIAL_SUITES_BEFORE_FULL_LOAD: the threshold governs how many *further*
                // distinct suites trigger a full-build fallback (handled by the outer condition on the
                // next call), not whether the suite that reached it gets served narrowly. Falling through
                // to a full load here as well would run both the narrow query and the full query for the
                // same request, contrary to the documented "above 25" threshold.
                return loaded;
            }
            return getCasesBySuiteName(span).getOrDefault(suiteName, Collections.emptyList());
        }

        Optional<TestResult> getPreviousResult(Span span) {
            Optional<TestResult> local = previousResult;
            if (local != null) {
                return local;
            }
            synchronized (previousResultLock) {
                local = previousResult;
                if (local == null) {
                    local = loadPreviousResultFromDB(span);
                    previousResult = local;
                }
                return local;
            }
        }

        Run<?, ?> getFailedSinceRun(String testKey, Supplier<Run<?, ?>> computer) {
            return failedSinceRunByTest.computeIfAbsent(testKey, k -> computer.get());
        }

        private Optional<TestResult> loadPreviousResultFromDB(Span parentSpan) {
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
                                return Optional.of(new TestResult(load(job, previousBuild)));
                            }
                            return Optional.empty();
                        }
                    }
                });
            });
        }

        private List<CaseResult> loadCaseResultsFromDB(Span parentSpan) {
            var sql = "SELECT suite, package, "
                    + "testname, classname, errordetails, skipped, duration, stdout, stderr, stacktrace "
                    + "FROM caseResults WHERE job = ? AND build = ?";
            return loadCaseResultRows("DatabaseTestResultStorage.TestResultStorage.loadCaseResultsFromDB", sql,
                    statement -> {
                        statement.setString(1, job);
                        statement.setInt(2, build);
                    });
        }

        /**
         * Narrow counterpart of {@link #loadCaseResultsFromDB(Span)}, scoped to a single suite so that
         * {@link #getCasesForSuite(Span, String)} need not hydrate every other suite in the build (and
         * their stdout/stderr/stacktrace text) to resolve one historical suite lookup.
         */
        private List<CaseResult> loadCasesForSuiteFromDB(Span parentSpan, String suiteName) {
            var sql = "SELECT suite, package, "
                    + "testname, classname, errordetails, skipped, duration, stdout, stderr, stacktrace "
                    + "FROM caseResults WHERE job = ? AND build = ? AND suite = ?";
            return loadCaseResultRows("DatabaseTestResultStorage.TestResultStorage.loadCasesForSuiteFromDB", sql,
                    statement -> {
                        statement.setString(1, job);
                        statement.setInt(2, build);
                        statement.setString(3, suiteName);
                    });
        }

        /**
         * Shared row-mapping logic for both a full build load and a single-suite load: runs the given
         * already-scoped query and maps each row into a {@link CaseResult}, wiring up {@link ClassResult}/
         * {@link PackageResult} parents exactly as the full load always has.
         */
        private List<CaseResult> loadCaseResultRows(String spanName, String sql, SqlBinder binder) {
            return withSpan(spanName, span ->
                    query(connection -> {
                        List<CaseResult> results = new ArrayList<>();
                        addSqlAttribute(span, sql);
                        try (var preparedStatement = connection.prepareStatement(sql)) {
                            binder.bind(preparedStatement);
                            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                                Map<String, ClassResult> classResults = new HashMap<>();
                                TestResult parent = new TestResult(self);
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
                                                new ClassResult(new PackageResult(new TestResult(self), packageName),
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
                        log.fine(String.format("Loaded %d test cases from database for '%s #%d'.", results.size(), job,
                                build));
                        return results;
                    }));
        }

        List<PackageResult> getPackageResults(Span span) {
            List<PackageResult> local = packageResults;
            if (local != null) {
                return local;
            }
            synchronized (packageResultsLock) {
                local = packageResults;
                if (local == null) {
                    local = loadPackageResults(span);
                    packageResults = local;
                }
                return local;
            }
        }

        private List<PackageResult> loadPackageResults(Span parentSpan) {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.loadPackageResults", span -> {
                Map<String, PackageResult> mapOfPackageResults = new TreeMap<>();
                TestResult testResult = new TestResult(self);
                getCaseResults(span).forEach(caseResult -> {
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

        BuildSummary getSummary(Span span) {
            BuildSummary local = summary;
            if (local != null) {
                return local;
            }
            synchronized (summaryLock) {
                local = summary;
                if (local == null) {
                    local = loadSummaryFromDB(span);
                    summary = local;
                }
                return local;
            }
        }

        private BuildSummary loadSummaryFromDB(Span parentSpan) {
            return withSpan("DatabaseTestResultStorage.TestResultStorage.loadSummaryFromDB", span ->
                    query(connection -> {
                        // skipped takes precedence over fail/pass, matching CaseResult#isSkipped/#isFailed;
                        // a case fails when not skipped and either errordetails or stacktrace is set, matching
                        // CaseResult#isPassed (and #isFailed = !isPassed && !isSkipped).
                        var sql = "SELECT COUNT(*) AS total, "
                                + "COALESCE(SUM(CASE WHEN skipped IS NOT NULL THEN 1 ELSE 0 END), 0) AS skipcount, "
                                + "COALESCE(SUM(CASE WHEN skipped IS NULL AND (errordetails IS NOT NULL OR stacktrace IS NOT NULL) "
                                + "THEN 1 ELSE 0 END), 0) AS failcount, "
                                + "COALESCE(SUM(CASE WHEN skipped IS NULL AND errordetails IS NULL AND stacktrace IS NULL "
                                + "THEN 1 ELSE 0 END), 0) AS passcount, "
                                + "COALESCE(SUM(duration), 0) AS totalduration "
                                + "FROM caseResults WHERE job = ? AND build = ?";
                        addSqlAttribute(span, sql);
                        try (PreparedStatement statement = connection.prepareStatement(sql)) {
                            statement.setString(1, job);
                            statement.setInt(2, build);
                            try (ResultSet result = statement.executeQuery()) {
                                result.next();
                                int total = result.getInt("total");
                                int skipped = result.getInt("skipcount");
                                int failed = result.getInt("failcount");
                                int passed = result.getInt("passcount");
                                float duration = result.getFloat("totalduration");
                                log.info(String.format(
                                        "Loaded summary (total=%d passed=%d failed=%d skipped=%d) for '%s #%d'.",
                                        total, passed, failed, skipped, job, build));
                                return new BuildSummary(total, passed, failed, skipped, duration);
                            }
                        }
                    }));
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
        span.setAttribute("sql", sql);
    }

    private static void addPackageAttribute(Span span, String packageName) {
        span.setAttribute("package", packageName);
    }

    private static Tracer getTracer() {
        return GlobalOpenTelemetry.getTracer("io.jenkins.plugins.junit.storage.database");
    }

    /**
     * Whether the given connection is to PostgreSQL rather than MySQL, the two databases this
     * plugin supports. Used to select the PostgreSQL-specific {@code testidentityhash}-based
     * variant of the "failed since" lookup query (see {@code computeFailedSinceRun}), which exists
     * because PostgreSQL's btree index entry size limit (unlike MySQL's prefix-length indexes)
     * cannot safely index the full job/classname/testname columns directly.
     */
    private static boolean isPostgres(Connection connection) throws SQLException {
        return "PostgreSQL".equals(connection.getMetaData().getDatabaseProductName());
    }
}
