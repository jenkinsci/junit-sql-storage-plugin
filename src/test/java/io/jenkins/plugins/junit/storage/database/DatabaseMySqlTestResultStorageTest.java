package io.jenkins.plugins.junit.storage.database;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import hudson.Util;
import hudson.tasks.junit.TestResult;
import hudson.util.Secret;
import hudson.util.StreamTaskListener;
import io.jenkins.plugins.junit.storage.JunitTestResultStorage.RemotePublisher;
import io.jenkins.plugins.junit.storage.JunitTestResultStorageConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.tools.ant.DirectoryScanner;
import org.jenkinsci.plugins.database.GlobalDatabaseConfiguration;
import org.jenkinsci.plugins.database.mysql.MySQLDatabase;
import org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition;
import org.jenkinsci.plugins.workflow.job.WorkflowJob;
import org.jenkinsci.plugins.workflow.job.WorkflowRun;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.jvnet.hudson.test.JenkinsRule;
import org.jvnet.hudson.test.junit.jupiter.WithJenkins;
import org.testcontainers.containers.MySQLContainer;

import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MySQL-specific coverage for {@link DatabaseTestResultStorage}'s batch-publish/summary-upsert
 * path. The rest of the integration suite ({@link DatabaseTestResultStorageTest}) runs against
 * PostgreSQL only, so it never exercises the MySQL {@code ON DUPLICATE KEY UPDATE} upsert branch
 * of {@code RemotePublisherImpl#upsertSummary}, nor the concurrent-first-publish scenario that
 * motivated replacing the old update-then-insert-then-retry pattern (see
 * https://github.com/jenkinsci/junit-sql-storage-plugin/pull/539#pullrequestreview-5440392172,
 * finding "Add MySQL concurrent upsert and rollback integration coverage").
 */
@WithJenkins
class DatabaseMySqlTestResultStorageTest {

    private static final String TEST_IMAGE = "mysql:8.0.46";

    private JenkinsRule jenkinsRule;

    @BeforeEach
    void setUp(JenkinsRule rule) {
        jenkinsRule = rule;
    }

    @Test
    void concurrentFirstPublish_bothSucceedWithConsistentSummary() throws Exception {
        // Given: two publishers racing to insert the very first caseResultsSummary row for the
        // same brand-new (job, build) -- the scenario that used to deadlock under MySQL's default
        // REPEATABLE READ isolation with the old update-then-insert-then-retry upsert (an UPDATE
        // matching no row takes a gap lock on the key range, so two concurrent first publishes can
        // each hold that lock and then deadlock on their own following INSERT). The single atomic
        // "INSERT ... ON DUPLICATE KEY UPDATE" statement has no such window.
        try (MySQLContainer<?> mysql = new MySQLContainer<>(TEST_IMAGE)) {
            setupPlugin(mysql);

            var workflowJob = jenkinsRule.createProject(WorkflowJob.class, "concurrent-first-publish");
            workflowJob.setDefinition(new CpsFlowDefinition("echo 'noop'", true));
            WorkflowRun workflowRun = jenkinsRule.buildAndAssertSuccess(workflowJob);

            DatabaseTestResultStorage storage = new DatabaseTestResultStorage();
            JunitTestResultStorageConfiguration.get().setStorage(storage);
            RemotePublisher publisherA = storage.createRemotePublisher(workflowRun);
            RemotePublisher publisherB = storage.createRemotePublisher(workflowRun);

            TestResult resultA = buildTestResult("Bulk", "passA", 10, 0);
            TestResult resultB = buildTestResult("Bulk", "passB", 0, 5);

            CountDownLatch startLine = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                var listener = StreamTaskListener.fromStdout();
                Future<?> futureA = pool.submit(() -> {
                    awaitUninterruptibly(startLine);
                    publisherA.publish(resultA, listener);
                    return null;
                });
                Future<?> futureB = pool.submit(() -> {
                    awaitUninterruptibly(startLine);
                    publisherB.publish(resultB, listener);
                    return null;
                });
                startLine.countDown();

                // When: both publishers race to upsert the summary row for the same (job, build).
                // Then: neither throws (no deadlock/unretried failure propagates).
                futureA.get(2, TimeUnit.MINUTES);
                futureB.get(2, TimeUnit.MINUTES);
            } finally {
                pool.shutdownNow();
            }

            // And: the persisted summary reflects both publishers' contributions exactly, with no
            // lost update from the concurrent upserts.
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource()
                    .getConnection()) {
                try (PreparedStatement count = connection.prepareStatement(
                        "SELECT COUNT(*) FROM caseResults WHERE job = ? AND build = ?")) {
                    count.setString(1, workflowJob.getFullName());
                    count.setInt(2, workflowRun.getNumber());
                    try (ResultSet result = count.executeQuery()) {
                        assertTrue(result.next());
                        assertThat(result.getInt(1), is(15));
                    }
                }
                try (PreparedStatement summary = connection.prepareStatement(
                        "SELECT passCount, failCount FROM caseResultsSummary WHERE job = ? AND build = ?")) {
                    summary.setString(1, workflowJob.getFullName());
                    summary.setInt(2, workflowRun.getNumber());
                    try (ResultSet result = summary.executeQuery()) {
                        assertTrue(result.next());
                        assertThat(result.getInt("passCount"), is(10));
                        assertThat(result.getInt("failCount"), is(5));
                    }
                }
            }
        }
    }

    @Test
    void publish_summaryFailureAfterBatchInsert_rollsBackChunkButKeepsEarlierChunk() throws Exception {
        // Given: a build with more than MAX_DB_BATCH_SIZE (2000) cases, published as two chunks --
        // the first chunk (2000 passing cases) flushed/committed successfully, the second chunk
        // (500 failing cases) engineered to fail during its caseResultsSummary upsert (via MySQL
        // triggers that signal an error whenever a row's failCount becomes positive) after its
        // caseResults batch insert has already run in the same open transaction. MySQL equivalent
        // of DatabaseTestResultStorageTest#publish_summaryFailureAfterBatchInsert_rollsBackChunkButKeepsEarlierChunk;
        // regression test for
        // https://github.com/jenkinsci/junit-sql-storage-plugin/pull/539#pullrequestreview-5440392172.
        // Binary logging is enabled by default in the mysql:8 image, and MySQL refuses to let a
        // non-SUPER user create a trigger/function while it is enabled (to stop replication from
        // silently diverging) unless log_bin_trust_function_creators is set.
        try (MySQLContainer<?> mysql = new MySQLContainer<>(TEST_IMAGE)
                .withCommand("--log-bin-trust-function-creators=1")) {
            setupPlugin(mysql);

            var workflowJob = jenkinsRule.createProject(WorkflowJob.class, "bulk-publish-mysql");
            workflowJob.setDefinition(new CpsFlowDefinition("echo 'noop'", true));
            WorkflowRun workflowRun = jenkinsRule.buildAndAssertSuccess(workflowJob);

            int passingCases = DatabaseTestResultStorage.MAX_DB_BATCH_SIZE;
            int failingCases = 500;
            TestResult testResult = buildTestResult("Bulk", "case", passingCases, failingCases);

            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource()
                    .getConnection()) {
                // Installed before publishing so the first chunk's upsert (all passing, never
                // setting failCount above zero) succeeds normally, and only the second (failing)
                // chunk's upsert trips it. MySQL triggers cannot combine INSERT and UPDATE in one
                // definition like PostgreSQL's "BEFORE INSERT OR UPDATE", so two are created.
                try (var statement = connection.createStatement()) {
                    statement.execute(
                            "CREATE TRIGGER fail_on_positive_failcount_ins BEFORE INSERT ON caseResultsSummary "
                                    + "FOR EACH ROW BEGIN IF NEW.failCount > 0 THEN "
                                    + "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'injected failure for test'; "
                                    + "END IF; END");
                    statement.execute(
                            "CREATE TRIGGER fail_on_positive_failcount_upd BEFORE UPDATE ON caseResultsSummary "
                                    + "FOR EACH ROW BEGIN IF NEW.failCount > 0 THEN "
                                    + "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'injected failure for test'; "
                                    + "END IF; END");
                }
            }

            DatabaseTestResultStorage storage = new DatabaseTestResultStorage();
            JunitTestResultStorageConfiguration.get().setStorage(storage);
            var publisher = storage.createRemotePublisher(workflowRun);
            var listener = StreamTaskListener.fromStdout();
            // When: publishing fails (the trigger rejects the second chunk's summary upsert).
            assertThrows(IOException.class, () -> publisher.publish(testResult, listener));

            // Then / And: verify persisted state directly against the database.
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource()
                    .getConnection()) {
                try (PreparedStatement count = connection.prepareStatement(
                        "SELECT COUNT(*) FROM caseResults WHERE job = ? AND build = ?")) {
                    count.setString(1, workflowJob.getFullName());
                    count.setInt(2, workflowRun.getNumber());
                    try (ResultSet result = count.executeQuery()) {
                        assertTrue(result.next());
                        // Then: only the first, successfully committed chunk's rows (2000 passing
                        // cases) are present; the second chunk's 500 failing-case rows were rolled
                        // back along with the trigger-induced summary failure.
                        assertThat(result.getInt(1), is(passingCases));
                    }
                }
                try (PreparedStatement summary = connection.prepareStatement(
                        "SELECT passCount, failCount FROM caseResultsSummary WHERE job = ? AND build = ?")) {
                    summary.setString(1, workflowJob.getFullName());
                    summary.setInt(2, workflowRun.getNumber());
                    try (ResultSet result = summary.executeQuery()) {
                        assertTrue(result.next());
                        // And: the summary row reflects only the first chunk too.
                        assertThat(result.getInt("passCount"), is(passingCases));
                        assertThat(result.getInt("failCount"), is(0));
                    }
                }
            }
        }
    }

    private static TestResult buildTestResult(String className, String namePrefix, int numPass, int numFail)
            throws IOException {
        StringBuilder xml = new StringBuilder("<testsuite name='bulk'>");
        for (int i = 0; i < numPass; i++) {
            xml.append("<testcase classname='").append(className).append("' name='").append(namePrefix)
                    .append("pass").append(i).append("' time='0.01'/>");
        }
        for (int i = 0; i < numFail; i++) {
            xml.append("<testcase classname='").append(className).append("' name='").append(namePrefix)
                    .append("fail").append(i).append("' time='0.01'><error message='boom'/></testcase>");
        }
        xml.append("</testsuite>");
        File reportsDir = Files.createTempDirectory("bulk-publish-mysql-reports").toFile();
        FileUtils.writeStringToFile(new File(reportsDir, "x.xml"), xml.toString(), StandardCharsets.UTF_8);
        DirectoryScanner directoryScanner = Util.createFileSet(reportsDir, "*.xml").getDirectoryScanner();
        TestResult testResult = new TestResult(System.currentTimeMillis(), directoryScanner, false, false, null, false);
        testResult.tally();
        return testResult;
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void setupPlugin(MySQLContainer<?> mysql) {
        mysql.start();

        MySQLDatabase database = new MySQLDatabase(mysql.getHost() + ":" + mysql.getMappedPort(3306),
                mysql.getDatabaseName(), mysql.getUsername(), Secret.fromString(mysql.getPassword()),
                "allowMultiQueries=true");
        database.setValidationQuery("SELECT 1");
        GlobalDatabaseConfiguration.get().setDatabase(database);
        JunitTestResultStorageConfiguration.get().setStorage(new DatabaseTestResultStorage());
        DatabaseSchemaLoader.migrateSchema();
    }
}
