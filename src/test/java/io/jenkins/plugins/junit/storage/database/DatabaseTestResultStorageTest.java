package io.jenkins.plugins.junit.storage.database;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.google.common.collect.ImmutableSet;
import hudson.Util;
import hudson.model.Label;
import hudson.model.Result;
import hudson.slaves.DumbSlave;
import hudson.tasks.junit.CaseResult;
import hudson.tasks.junit.HistoryTestResultSummary;
import hudson.tasks.junit.PackageResult;
import hudson.tasks.junit.SuiteResult;
import hudson.tasks.junit.TestDurationResultSummary;
import hudson.tasks.junit.TestResult;
import hudson.tasks.junit.TestResultAction;
import hudson.tasks.junit.TestResultSummary;
import hudson.tasks.junit.TrendTestResultSummary;
import hudson.util.Secret;
import hudson.util.StreamTaskListener;
import io.jenkins.plugins.junit.storage.JunitTestResultStorageConfiguration;
import io.jenkins.plugins.junit.storage.TestResultImpl;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.commons.io.FileUtils;
import org.apache.tools.ant.DirectoryScanner;
import org.jenkinsci.plugins.database.GlobalDatabaseConfiguration;
import org.jenkinsci.plugins.database.postgresql.PostgreSQLDatabase;
import org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition;
import org.jenkinsci.plugins.workflow.job.WorkflowJob;
import org.jenkinsci.plugins.workflow.job.WorkflowRun;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.jvnet.hudson.test.JenkinsRule;
import org.jvnet.hudson.test.LogRecorder;
import org.jvnet.hudson.test.junit.jupiter.WithJenkins;
import org.mockito.Mockito;
import org.mockito.exceptions.base.MockitoInitializationException;
import org.testcontainers.containers.PostgreSQLContainer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import static io.jenkins.plugins.junit.storage.database.DatabaseTestResultStorage.MAX_ERROR_DETAILS_LENGTH;
import static io.jenkins.plugins.junit.storage.database.DatabaseTestResultStorage.MAX_CLASSNAME_LENGTH;
import static io.jenkins.plugins.junit.storage.database.DatabaseTestResultStorage.MAX_SUITE_LENGTH;
import static io.jenkins.plugins.junit.storage.database.DatabaseTestResultStorage.MAX_TEST_NAME_LENGTH;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@WithJenkins
class DatabaseTestResultStorageTest {

    private static final String TEST_IMAGE = "postgres:16-alpine";
    private static final String BUILD_PIPELINE =
            """
                    node('remote') {
                        writeFile file: 'x.xml', text: '''<testsuite name='sweet' time='200.0'>
                            <testcase classname='Klazz' name='test1' time='198.0'><error message='failure'/></testcase>
                            <testcase classname='Klazz' name='test2' time='2.0'/>
                            <testcase classname='other.Klazz' name='test3'><skipped message='Not actually run.'/></testcase>
                        </testsuite>'''
                        def s = junit 'x.xml'
                        echo(/summary: fail=$s.failCount skip=$s.skipCount pass=$s.passCount total=$s.totalCount/)
                        writeFile file: 'x.xml', text: '''<testsuite name='supersweet'>
                            <testcase classname='another.Klazz' name='test1'><error message='another failure'/></testcase>
                        </testsuite>'''
                        s = junit 'x.xml'
                        echo(/next summary: fail=$s.failCount skip=$s.skipCount pass=$s.passCount total=$s.totalCount/)
                    }
                    """;

    private JenkinsRule jenkinsRule;

    private final LogRecorder logging = new LogRecorder().record(DatabaseTestResultStorage.class.getName(), Level.INFO);

    @BeforeEach
    void setUp(JenkinsRule rule) {
        jenkinsRule = rule;
    }

    @Test
    void smokes() throws Exception {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(TEST_IMAGE)) {
            setupPlugin(postgres);

            jenkinsRule.createOnlineSlave(Label.get("remote"));
            var workflowJob = jenkinsRule.createProject(WorkflowJob.class, "unit-tests");
            workflowJob.setDefinition(new CpsFlowDefinition(BUILD_PIPELINE, true));
            var workflowRun = Objects.requireNonNull(workflowJob.scheduleBuild2(0), "workflowJob.scheduleBuild2(0) returned null").get();

            jenkinsRule.waitForCompletion(workflowRun);
            jenkinsRule.assertBuildStatus(Result.UNSTABLE, workflowRun);
            jenkinsRule.assertLogContains("summary: fail=1 skip=1 pass=1 total=3", workflowRun);
            jenkinsRule.assertLogContains("next summary: fail=1 skip=0 pass=0 total=1", workflowRun);
            assertFalse(new File(workflowRun.getRootDir(), "junitResult.xml").isFile());
            String buildXml = FileUtils.readFileToString(new File(workflowRun.getRootDir(), "build.xml"), StandardCharsets.UTF_8);
            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
                    .parse(new ByteArrayInputStream(buildXml.getBytes(StandardCharsets.UTF_8)));
            NodeList testResultActionList = doc.getElementsByTagName("hudson.tasks.junit.TestResultAction");
            assertEquals(1, testResultActionList.getLength(), buildXml);
            Element testResultActionElement = (Element) testResultActionList.item(0);
            NodeList childNodes = testResultActionElement.getChildNodes();
            Set<String> childNames = new TreeSet<>();
            for (int i = 0; i < childNodes.getLength(); i++) {
                Node item = childNodes.item(i);
                if (item instanceof Element) {
                    childNames.add(((Element) item).getTagName());
                }
            }

            printAndVerifyCaseResultsTable(true);

            assertEquals(ImmutableSet.of("healthScaleFactor", "testData", "descriptions"),
                    childNames,
                    buildXml);
            TestResultAction testResultAction = workflowRun.getAction(TestResultAction.class);
            assertNotNull(testResultAction);

            assertEquals(2, testResultAction.getFailCount());
            assertEquals(1, testResultAction.getSkipCount());
            assertEquals(4, testResultAction.getTotalCount());
            assertEquals(2, testResultAction.getResult().getFailCount());
            assertEquals(1, testResultAction.getResult().getSkipCount());
            assertEquals(4, testResultAction.getResult().getTotalCount());
            assertEquals(1, testResultAction.getResult().getPassCount());
            assertEquals(2, testResultAction.getResult().getSuites().size());
            List<CaseResult> failedTests = testResultAction.getFailedTests();
            assertEquals(2, failedTests.size());
            // CaseResult query results carry no guaranteed ordering (no ORDER BY is used, by design,
            // to avoid forcing an unnecessary sort on potentially huge result sets), so locate each
            // expected failure by its class name instead of assuming a fixed position.
            final CaseResult klazzTest1 = failedTests.stream()
                    .filter(caseResult -> caseResult.getClassName().equals("Klazz"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("Klazz", klazzTest1.getClassName());
            assertEquals("test1", klazzTest1.getName());
            assertEquals("failure", klazzTest1.getErrorDetails());
            assertThat(klazzTest1.getDuration(), is(198.0f));
            final CaseResult anotherKlazzTest1 = failedTests.stream()
                    .filter(caseResult -> caseResult.getClassName().equals("another.Klazz"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("test1", anotherKlazzTest1.getName());
            assertEquals("another failure", anotherKlazzTest1.getErrorDetails());

            List<CaseResult> skippedTests = testResultAction.getSkippedTests();
            assertEquals(1, skippedTests.size());
            assertEquals("other.Klazz", skippedTests.get(0).getClassName());
            assertEquals("test3", skippedTests.get(0).getName());
            assertEquals("Not actually run.", skippedTests.get(0).getSkippedMessage());

            List<CaseResult> passedTests = testResultAction.getPassedTests();
            assertEquals(1, passedTests.size());
            assertEquals("Klazz", passedTests.get(0).getClassName());
            assertEquals("test2", passedTests.get(0).getName());

            PackageResult another = testResultAction.getResult().byPackage("another");
            List<CaseResult> packageFailedTests = another.getFailedTests();
            assertEquals(1, packageFailedTests.size());
            assertEquals("another.Klazz", packageFailedTests.get(0).getClassName());

            PackageResult other = testResultAction.getResult().byPackage("other");
            List<CaseResult> packageSkippedTests = other.getSkippedTests();
            assertEquals(1, packageSkippedTests.size());
            assertEquals("other.Klazz", packageSkippedTests.get(0).getClassName());
            assertEquals("Not actually run.", packageSkippedTests.get(0).getSkippedMessage());

            PackageResult root = testResultAction.getResult().byPackage("(root)");
            List<CaseResult> rootPassedTests = root.getPassedTests();
            assertEquals(1, rootPassedTests.size());
            assertEquals("Klazz", rootPassedTests.get(0).getClassName());

            TestResultImpl pluggableStorage =
                    requireNonNull(testResultAction.getResult().getPluggableStorage());
            List<TrendTestResultSummary> trendTestResultSummary = pluggableStorage.getTrendTestResultSummary();
            assertThat(trendTestResultSummary, hasSize(1));
            TestResultSummary testResultSummary = trendTestResultSummary.get(0).getTestResultSummary();
            assertThat(testResultSummary.getFailCount(), equalTo(2));
            assertThat(testResultSummary.getPassCount(), equalTo(1));
            assertThat(testResultSummary.getSkipCount(), equalTo(1));
            assertThat(testResultSummary.getTotalCount(), equalTo(4));

            int countOfBuildsWithTestResults = pluggableStorage.getCountOfBuildsWithTestResults();
            assertThat(countOfBuildsWithTestResults, is(1));

            final List<TestDurationResultSummary> testDurationResultSummary =
                    pluggableStorage.getTestDurationResultSummary();
            assertThat(testDurationResultSummary.get(0).getDuration(), is(200));

            // Reads the same persisted per-build summary as the trend/duration/count calls above,
            // confirming the accumulated counts from the two separate junit steps in BUILD_PIPELINE
            // (fail=1 skip=1 pass=1, then fail=1 skip=0 pass=0) are reflected correctly as a single
            // merged history row for the build.
            List<HistoryTestResultSummary> historySummary = pluggableStorage.getHistorySummary(0);
            assertThat(historySummary, hasSize(1));
            HistoryTestResultSummary historyEntry = historySummary.get(0);
            assertThat(historyEntry.getFailCount(), is(2));
            assertThat(historyEntry.getSkipCount(), is(1));
            assertThat(historyEntry.getPassCount(), is(1));
            assertThat(historyEntry.getTotalCount(), is(4));
            assertThat(historyEntry.getDuration(), is(200.0f));

            //check storage getSuites method
            Collection<SuiteResult> suiteResults = pluggableStorage.getSuites();
            assertThat(suiteResults, hasSize(2));
            //check the two suites name
            assertThat(suiteResults, containsInAnyOrder(hasProperty("name", equalTo("supersweet")),
                    hasProperty("name", equalTo("sweet"))));

            //check one suite detail
            SuiteResult supersweetSuite = suiteResults.stream()
                    .filter(suite -> suite.getName().equals("supersweet"))
                    .findFirst()
                    .get();
            assertThat(supersweetSuite.getCases(), hasSize(1));
            assertThat(supersweetSuite.getCases().get(0).getName(), equalTo("test1"));
            assertThat(supersweetSuite.getCases().get(0).getClassName(), equalTo("another.Klazz"));
        }
    }

    @Test
    void testResultCleanup() throws Exception {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(TEST_IMAGE)) {
            setupPlugin(postgres);

            Thread.sleep(5000);

            jenkinsRule.createOnlineSlave(Label.get("remote"));
            var workflowJob = jenkinsRule.createProject(WorkflowJob.class, "p");
            workflowJob.setDefinition(new CpsFlowDefinition(BUILD_PIPELINE, true));

            var workflowRun = workflowJob.scheduleBuild2(0).get();
            jenkinsRule.assertBuildStatus(Result.UNSTABLE, workflowRun);

            workflowRun = workflowJob.scheduleBuild2(0).get();
            jenkinsRule.assertBuildStatus(Result.UNSTABLE, workflowRun);

            workflowRun = workflowJob.scheduleBuild2(0).get();
            jenkinsRule.assertBuildStatus(Result.UNSTABLE, workflowRun);

            printCaseResultsTable();

            // 3 sets of test results
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource()
                    .getConnection();
                    PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM caseResults");
                    ResultSet result = statement.executeQuery()) {
                result.next();
                int count = result.getInt(1);
                assertThat(count, is(12));
            }
            // one caseResultsSummary row per build, regardless of each build having published via
            // two separate junit steps (see BUILD_PIPELINE): confirms the summary upsert accumulates
            // into a single row per build rather than one row per publish() call.
            assertThat(countSummaryRows("p"), is(3));

            System.out.println("Deleting a workflowRun...");
            workflowRun.delete();
            Thread.sleep(5000);
            printCaseResultsTable();

            // 2 sets of test results
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource()
                    .getConnection();
                    PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM caseResults");
                    ResultSet result = statement.executeQuery()) {
                result.next();
                int anInt = result.getInt(1);
                assertThat(anInt, is(8));
            }
            assertThat(countSummaryRows("p"), is(2));

            System.out.println("Deleting the workflowJob ...");
            workflowJob.delete();
            printCaseResultsTable();

            // 0 test results
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource()
                    .getConnection();
                    PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM caseResults");
                    ResultSet result = statement.executeQuery()) {
                result.next();
                int anInt = result.getInt(1);
                assertThat(anInt, is(0));
            }
            assertThat(countSummaryRows("p"), is(0));
        }
    }

    private int countSummaryRows(String job) throws Exception {
        try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource()
                .getConnection();
                PreparedStatement statement =
                        connection.prepareStatement("SELECT count(*) FROM caseResultsSummary WHERE job = ?")) {
            statement.setString(1, job);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getInt(1);
            }
        }
    }

    @Test
    void testResultCleanup_skipped_if_disabled() throws Exception {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(TEST_IMAGE)) {
            setupPlugin(postgres);

            DatabaseTestResultStorage storage = new DatabaseTestResultStorage();
            storage.setSkipCleanupRunsOnDeletion(true);
            JunitTestResultStorageConfiguration.get().setStorage(storage);

            jenkinsRule.createOnlineSlave(Label.get("remote"));
            WorkflowJob workflowJob = jenkinsRule.createProject(WorkflowJob.class, "workflowJob");
            workflowJob.setDefinition(new CpsFlowDefinition(BUILD_PIPELINE, true));

            WorkflowRun workflowRun = workflowJob.scheduleBuild2(0).get();
            jenkinsRule.assertBuildStatus(Result.UNSTABLE, workflowRun);

            System.out.println("Current contents of the CaseResults table");
            printCaseResultsTable();
            // 1 sets of test results
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource()
                    .getConnection();
                    PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM caseResults");
                    ResultSet result = statement.executeQuery()) {
                result.next();
                int count = result.getInt(1);
                assertThat(count, is(4));
            }
            System.out.println("Deleting the workflowRun...");
            workflowRun.delete();
            printCaseResultsTable();

            // 1 set of test results
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource()
                    .getConnection();
                    PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM caseResults");
                    ResultSet result = statement.executeQuery()) {
                result.next();
                int count = result.getInt(1);
                assertThat(count, is(4));
            }

            System.out.println("Deleting the workflowJob...");
            workflowJob.delete();
            printCaseResultsTable();

            // 1 set of test results
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource()
                    .getConnection();
                    PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM caseResults");
                    ResultSet result = statement.executeQuery()) {
                result.next();
                int count = result.getInt(1);
                assertThat(count, is(4));
            }
        }
    }

    @Test
    void testResult_long_string() throws Exception {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(TEST_IMAGE)) {
            setupPlugin(postgres);

            DatabaseTestResultStorage storage = new DatabaseTestResultStorage();
            JunitTestResultStorageConfiguration.get().setStorage(storage);

            WorkflowJob p = jenkinsRule.createProject(WorkflowJob.class, "p");

            p.setDefinition(new CpsFlowDefinition(
                    """
                            node('remote') {
                                def s = junit 'x.xml'
                                echo(/summary: fail=$s.failCount skip=$s.skipCount pass=$s.passCount total=$s.totalCount/)
                                writeFile file: 'x.xml', text: '''<testsuite name='supersweet'>
                                    <testcase classname='another.Klazz' name='test1'><error message='another failure'/></testcase>
                                </testsuite>'''
                                s = junit 'x.xml'
                                echo(/next summary: fail=$s.failCount skip=$s.skipCount pass=$s.passCount total=$s.totalCount/)
                            }
                            """, true));

            //Because writeFile can't handle long string
            //We use file copy to prepare the test result
            DumbSlave agent = jenkinsRule.createOnlineSlave(Label.get("remote"));
            URI longStringFileUri = DatabaseTestResultStorageTest.class.getResource("long-string.xml").toURI();
            agent.getWorkspaceFor(p).child("x.xml").copyFrom(longStringFileUri.toURL());
            WorkflowRun b = p.scheduleBuild2(0).get();
            jenkinsRule.assertBuildStatus(Result.UNSTABLE, b);

            printCaseResultsTable();

            // 1 sets of test results
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource()
                    .getConnection();
                    PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM caseResults");
                    ResultSet result = statement.executeQuery()) {
                result.next();
                int anInt = result.getInt(1);
                assertThat(anInt, is(4));
            }

            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource()
                    .getConnection();
                    PreparedStatement statement = connection.prepareStatement(
                            "SELECT job, build, suite, package, className, testName, errorDetails, skipped, duration, stdout, stderr, stacktrace FROM caseResults where testName='test1'");
                    ResultSet result = statement.executeQuery()) {
                result.next();
                String suiteNameInDatabase = result.getString("suite");
                assertThat(suiteNameInDatabase.length(), is(MAX_SUITE_LENGTH));
                String errorDetailsInDatabase = result.getString("errorDetails");
                assertThat(errorDetailsInDatabase.length(), is(MAX_ERROR_DETAILS_LENGTH));
            }

        }
    }

    @Test
    void failedSince_longMultibyteTestIdentity_postgres() throws Exception {
        // Given: a job/classname/testname combination long enough and multibyte enough (CJK
        // characters are 3 bytes each in UTF-8) that a plain btree index over the full columns would
        // exceed PostgreSQL's per-entry index size limit -- this is exactly the scenario
        // V2026_10_07_0733__failed-since-index.sql's testidentityhash-based index (rather than
        // indexing job/classname/testname directly) exists to support. Regression test for
        // https://github.com/jenkinsci/junit-sql-storage-plugin/pull/539#pullrequestreview-5439494112.
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(TEST_IMAGE)) {
            setupPlugin(postgres);

            DatabaseTestResultStorage storage = new DatabaseTestResultStorage();
            JunitTestResultStorageConfiguration.get().setStorage(storage);

            WorkflowJob p = jenkinsRule.createProject(WorkflowJob.class, "longMultibyteIdentity");
            p.setDefinition(new CpsFlowDefinition("node { echo 'build' }", true));
            WorkflowRun build1 = jenkinsRule.buildAndAssertSuccess(p);
            WorkflowRun build2 = jenkinsRule.buildAndAssertSuccess(p);

            // No '.' in the class name, so CaseResult#getPackageName() resolves it to "(root)" --
            // matching how the plugin itself derives and stores the "package" column -- while
            // getClassName() (stored verbatim in the "classname" column) returns this full value.
            // High-entropy (non-repeating) CJK characters are used rather than a simple repeated
            // pattern: PostgreSQL's TOAST storage transparently PGLZ-compresses long column/index
            // values before storing them, and a short repeating pattern compresses so well that even
            // a "long" value stays well under the per-entry index size limit after compression --
            // which would make a test built from one make the plain-index assertion below pass for
            // the wrong reason (looking long on paper, but not actually triggering the limit).
            String longClassName = randomCjkText(MAX_CLASSNAME_LENGTH, 1); // 255 chars of CJK text
            String longTestName = randomCjkText(MAX_TEST_NAME_LENGTH, 2); // 500 chars of CJK text
            String suite = "suite1";
            String pkg = "(root)";

            // Insert directly rather than through a real junit XML publish, to precisely control the
            // exact identity values without needing to worry about XML-encoding such long/multibyte
            // content; this also exercises the testidentityhash generated column on insert itself,
            // which is where an oversized plain index would have failed.
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource()
                    .getConnection()) {
                try (PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO caseResults (job, build, suite, package, className, testName, errorDetails, duration) "
                                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
                    insert.setString(1, p.getFullName());
                    insert.setInt(2, build1.getNumber());
                    insert.setString(3, suite);
                    insert.setString(4, pkg);
                    insert.setString(5, longClassName);
                    insert.setString(6, longTestName);
                    insert.setNull(7, Types.VARCHAR);
                    insert.setFloat(8, 0.1f);
                    insert.executeUpdate();

                    insert.setString(1, p.getFullName());
                    insert.setInt(2, build2.getNumber());
                    insert.setString(3, suite);
                    insert.setString(4, pkg);
                    insert.setString(5, longClassName);
                    insert.setString(6, longTestName);
                    insert.setString(7, "it broke");
                    insert.setFloat(8, 0.1f);
                    insert.executeUpdate();
                }
            }

            var testResultStorage =
                    (DatabaseTestResultStorage.TestResultStorage) storage.load(p.getFullName(), build2.getNumber());
            SuiteResult suiteResult = new SuiteResult(suite, null, null, null);
            CaseResult caseResult = new CaseResult(suiteResult, longClassName, longTestName, "it broke",
                    null, 0.1f, null, null, null);

            // When
            var failedSinceRun = testResultStorage.getFailedSinceRun(caseResult);

            // Then: the lookup both succeeds (no index-row-size error) and resolves to the actual
            // first failing build, not some unrelated/collided identity.
            assertNotNull(failedSinceRun);
            assertEquals(build2.getNumber(), failedSinceRun.getNumber());

            // And: this fixture's (job, classname, testname) combination, if indexed directly rather
            // than via the bounded-size testidentityhash, would actually exceed PostgreSQL's btree
            // per-entry size limit -- proving this regression test's data would really have hit the
            // bug a plain "(job, classname, testname, build)" index has, not just resembling it.
            // longClassName/longTestName alone are already 765 + 1500 = 2265 UTF-8 bytes (and, being
            // high-entropy, do not compress away); a synthetic (filesystem-unconstrained, since this
            // probe never needs a real Jenkins job directory) job value of 150 CJK characters (450
            // bytes) pushes the combined row comfortably past the ~2704-byte limit.
            String oversizedJob = randomCjkText(150, 3); // 150 chars of CJK text, 450 bytes in UTF-8
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource()
                    .getConnection()) {
                try (PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO caseResults (job, build, suite, package, className, testName, duration) "
                                + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                    insert.setString(1, oversizedJob);
                    insert.setInt(2, build1.getNumber());
                    insert.setString(3, suite);
                    insert.setString(4, pkg);
                    insert.setString(5, longClassName);
                    insert.setString(6, longTestName);
                    insert.setFloat(7, 0.1f);
                    insert.executeUpdate();
                }
                try (var statement = connection.createStatement()) {
                    SQLException thrown = assertThrows(SQLException.class, () -> statement.execute(
                            "CREATE INDEX proof_plain_identity_index ON caseResults (job, className, testName, build)"));
                    assertThat(thrown.getMessage(), containsString("index row size"));
                }
            }
        }
    }

    @Test
    void publish_summaryFailureAfterBatchInsert_rollsBackChunkButKeepsEarlierChunk() throws Exception {
        // Given: a build with more than MAX_DB_BATCH_SIZE (2000) cases, published as two chunks --
        // the first chunk (2000 passing cases) flushed/committed successfully, the second chunk (500
        // failing cases) engineered to fail during its caseResultsSummary upsert (via a trigger that
        // raises an error whenever a row's failCount becomes positive) after its caseResults batch
        // insert has already run in the same open transaction. Regression test for
        // https://github.com/jenkinsci/junit-sql-storage-plugin/pull/539#pullrequestreview-5439872190
        // (finding #3): proves that (a) a summary-maintenance failure after the case-batch insert
        // rolls back both tables together for that chunk, and (b) an earlier chunk that already
        // committed is unaffected by a later chunk's failure.
        // Published directly via createRemotePublisher/publish (mirroring what JUnitParser does
        // internally -- see hudson.tasks.junit.JUnitParser.ParseResultCallable#invoke) rather than
        // through a real pipeline/junit step, since a 2500-testcase JUnit XML report embedded as a
        // single inline Groovy string literal in a CPS pipeline script would exceed the JVM class
        // file's 64KB string-constant limit.
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(TEST_IMAGE)) {
            setupPlugin(postgres);

            var workflowJob = jenkinsRule.createProject(WorkflowJob.class, "bulk-publish");
            workflowJob.setDefinition(new CpsFlowDefinition("echo 'noop'", true));
            WorkflowRun workflowRun = jenkinsRule.buildAndAssertSuccess(workflowJob);

            int passingCases = DatabaseTestResultStorage.MAX_DB_BATCH_SIZE;
            int failingCases = 500;
            StringBuilder xml = new StringBuilder("<testsuite name='bulk'>");
            for (int i = 0; i < passingCases; i++) {
                xml.append("<testcase classname='Bulk' name='pass").append(i).append("' time='0.01'/>");
            }
            for (int i = 0; i < failingCases; i++) {
                xml.append("<testcase classname='Bulk' name='fail").append(i)
                        .append("' time='0.01'><error message='boom'/></testcase>");
            }
            xml.append("</testsuite>");
            File reportsDir = Files.createTempDirectory("bulk-publish-reports").toFile();
            FileUtils.writeStringToFile(new File(reportsDir, "x.xml"), xml.toString(), StandardCharsets.UTF_8);
            DirectoryScanner directoryScanner = Util.createFileSet(reportsDir, "*.xml").getDirectoryScanner();
            TestResult testResult = new TestResult(System.currentTimeMillis(), directoryScanner, false, false, null, false);
            testResult.tally();

            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource()
                    .getConnection()) {
                // Installed before publishing so the first chunk's upsert (all passing, never
                // setting failCount above zero) succeeds normally, and only the second (failing)
                // chunk's upsert trips it.
                try (var statement = connection.createStatement()) {
                    statement.execute(
                            "CREATE OR REPLACE FUNCTION fail_on_positive_failcount() RETURNS trigger AS $$ "
                                    + "BEGIN IF NEW.failcount > 0 THEN "
                                    + "RAISE EXCEPTION 'injected failure for test'; END IF; RETURN NEW; END; $$ "
                                    + "LANGUAGE plpgsql");
                    statement.execute(
                            "CREATE TRIGGER fail_on_positive_failcount_trigger BEFORE INSERT OR UPDATE "
                                    + "ON caseresultssummary FOR EACH ROW "
                                    + "EXECUTE FUNCTION fail_on_positive_failcount()");
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
                        // cases) are present; the second chunk's 500 failing-case rows, inserted in
                        // the same transaction as the trigger-induced summary failure, were rolled
                        // back along with it rather than left as orphaned detail rows.
                        assertThat(result.getInt(1), is(passingCases));
                    }
                }
                try (PreparedStatement summary = connection.prepareStatement(
                        "SELECT passCount, failCount FROM caseResultsSummary WHERE job = ? AND build = ?")) {
                    summary.setString(1, workflowJob.getFullName());
                    summary.setInt(2, workflowRun.getNumber());
                    try (ResultSet result = summary.executeQuery()) {
                        assertTrue(result.next());
                        // And: the summary row reflects only the first chunk too -- not left
                        // half-updated with the second (rolled-back) chunk's counts.
                        assertThat(result.getInt("passCount"), is(passingCases));
                        assertThat(result.getInt("failCount"), is(0));
                    }
                }
            }
        }
    }

    @Test
    void getCaseResults_mockDatabase() throws SQLException {
        // Given
        var databaseTestResultStorage = new DatabaseTestResultStorage();
        databaseTestResultStorage.connectionSupplier = Mockito.mock(DatabaseTestResultStorage.ConnectionSupplier.class);
        var connection = Mockito.mock(Connection.class);
        Mockito.when(databaseTestResultStorage.connectionSupplier.connection()).thenReturn(connection);
        var preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.contains("SELECT suite, package"))).thenReturn(preparedStatement);
        List<CaseResult> expectedCaseResults = getCaseResults("package1", "class11", 1, 1, 1);
        expectedCaseResults.addAll(getCaseResults("package1", "class12", 2, 0, 0));
        expectedCaseResults.addAll(getCaseResults("package2", "class21", 3, 0, 2));

        var resultSet = mockResultSet(expectedCaseResults);
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        // The aggregate summary (used by getPassCount/getFailCount/getSkipCount/getTotalCount) is
        // computed via a dedicated SQL query, not by reloading the full case list.
        var summaryStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.contains("SELECT COUNT(*)"))).thenReturn(summaryStatement);
        var summaryResultSet = Mockito.mock(ResultSet.class);
        Mockito.when(summaryResultSet.next()).thenReturn(true);
        Mockito.when(summaryResultSet.getInt("total")).thenReturn(10);
        Mockito.when(summaryResultSet.getInt("passcount")).thenReturn(6);
        Mockito.when(summaryResultSet.getInt("failcount")).thenReturn(1);
        Mockito.when(summaryResultSet.getInt("skipcount")).thenReturn(3);
        Mockito.when(summaryResultSet.getFloat("totalduration")).thenReturn(1.0f);
        Mockito.when(summaryStatement.executeQuery()).thenReturn(summaryResultSet);

        String job = "jobName-mockDatabase";
        int build = 1;

        // When
        var testResultStorage = (DatabaseTestResultStorage.TestResultStorage) databaseTestResultStorage.load(job, build);

        // Then
        List<CaseResult> actualCaseResults = testResultStorage.getCaseResults();
        assertEquals(expectedCaseResults.size(), actualCaseResults.size());
        verifyCaseResultsMatch("", expectedCaseResults, actualCaseResults);

        assertEquals(6, testResultStorage.getPassCount(), "Unexpected pass count");
        assertEquals(1, testResultStorage.getFailCount(), "Unexpected fail count");
        assertEquals(3, testResultStorage.getSkipCount(), "Unexpected skip count");

        List<CaseResult> expectedFailedTests = expectedCaseResults.stream()
                .filter(CaseResult::isFailed)
                .toList();
        List<CaseResult> actualFailedTests = testResultStorage.getFailedTests();
        verifyCaseResultsMatch("failed tests", expectedFailedTests, actualFailedTests);

        List<CaseResult> expectedSkippedTests = expectedCaseResults.stream()
                .filter(CaseResult::isSkipped)
                .toList();
        List<CaseResult> actualSkippedTests = testResultStorage.getSkippedTests();
        verifyCaseResultsMatch("skipped tests", expectedSkippedTests, actualSkippedTests);

        List<CaseResult> expectedSkippedTestsByPackage = expectedCaseResults.stream()
                .filter(caseResult -> caseResult.getPackageName().equals("package1"))
                .filter(CaseResult::isSkipped)
                .toList();
        List<CaseResult> actualSkippedTestsByPackage = testResultStorage.getSkippedTestsByPackage("package1");
        verifyCaseResultsMatch("skipped tests by package1", expectedSkippedTestsByPackage, actualSkippedTestsByPackage);

        expectedSkippedTestsByPackage = expectedCaseResults.stream()
                .filter(caseResult -> caseResult.getPackageName().equals("package2"))
                .filter(CaseResult::isSkipped)
                .toList();
        actualSkippedTestsByPackage = testResultStorage.getSkippedTestsByPackage("package2");
        verifyCaseResultsMatch("skipped tests by package2", expectedSkippedTestsByPackage, actualSkippedTestsByPackage);

        List<CaseResult> expectedPassedTests = expectedCaseResults.stream()
                .filter(CaseResult::isPassed)
                .toList();
        List<CaseResult> actualPassedTests = testResultStorage.getPassedTests();
        verifyCaseResultsMatch("passed tests", expectedPassedTests, actualPassedTests);

        List<CaseResult> expectedPassedTestsByPackage = expectedCaseResults.stream()
                .filter(caseResult -> caseResult.getPackageName().equals("package1"))
                .filter(CaseResult::isPassed)
                .toList();
        List<CaseResult> actualPassedTestsByPackage = testResultStorage.getPassedTestsByPackage("package1");
        verifyCaseResultsMatch("passed tests by package1", expectedPassedTestsByPackage, actualPassedTestsByPackage);

        expectedPassedTestsByPackage = expectedCaseResults.stream()
                .filter(caseResult -> caseResult.getPackageName().equals("package2"))
                .filter(CaseResult::isPassed)
                .toList();
        actualPassedTestsByPackage = testResultStorage.getPassedTestsByPackage("package2");
        verifyCaseResultsMatch("passed tests by package2", expectedPassedTestsByPackage, actualPassedTestsByPackage);

        assertEquals(10, testResultStorage.getTotalCount(), "Unexpected total count");

        // Full case list and aggregate summary are each loaded from the database at most once per
        // build, no matter how many times the various accessors above are called.
        Mockito.verify(preparedStatement, Mockito.times(1)).executeQuery();
        Mockito.verify(summaryStatement, Mockito.times(1)).executeQuery();
    }

    @Test
    void getCaseResults_cacheInvalidation_mockDatabase() throws SQLException {
        // Given: a cache entry is populated for a build, as happens repeatedly while a build is
        // still running and its progressive test results page is polled.
        var databaseTestResultStorage = new DatabaseTestResultStorage();
        databaseTestResultStorage.connectionSupplier = Mockito.mock(DatabaseTestResultStorage.ConnectionSupplier.class);
        var connection = Mockito.mock(Connection.class);
        Mockito.when(databaseTestResultStorage.connectionSupplier.connection()).thenReturn(connection);
        var preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.contains("SELECT suite, package"))).thenReturn(preparedStatement);

        List<CaseResult> firstCaseResults = getCaseResults("package1", "class11", 1, 0, 0);
        var firstResultSet = mockResultSet(firstCaseResults);
        Mockito.when(preparedStatement.executeQuery()).thenReturn(firstResultSet);

        // Use a job name unique to this test: the results cache is a static, process-wide cache, so
        // reusing a (job, build) pair from another test could pick up its leftover cache entry.
        String job = "jobName-cacheInvalidation";
        int build = 1;
        var testResultStorage = (DatabaseTestResultStorage.TestResultStorage) databaseTestResultStorage.load(job, build);

        List<CaseResult> firstRead = testResultStorage.getCaseResults();
        assertEquals(1, firstRead.size());

        // Repeated reads of the same build while it's still running (e.g. polling the UI) must not
        // re-query the database, since nothing has published a new invalidation yet.
        List<CaseResult> secondRead = testResultStorage.getCaseResults();
        assertEquals(1, secondRead.size());
        Mockito.verify(preparedStatement, Mockito.times(1)).executeQuery();

        // When: new results are published for the same build (e.g. junit step runs again as the
        // build progresses), the cache must be invalidated so the next read picks up fresh data.
        List<CaseResult> secondCaseResults = getCaseResults("package1", "class11", 2, 0, 0);
        var secondResultSet = mockResultSet(secondCaseResults);
        Mockito.when(preparedStatement.executeQuery()).thenReturn(secondResultSet);
        DatabaseTestResultStorage.invalidate(job, build);

        // Then
        List<CaseResult> thirdRead = testResultStorage.getCaseResults();
        assertEquals(2, thirdRead.size(), "Cache was not refreshed after invalidation");
        Mockito.verify(preparedStatement, Mockito.times(2)).executeQuery();
    }

    @Test
    void getCaseResults_jobNameIsolation_mockDatabase() throws SQLException {
        // Given: two jobs whose names share a prefix, to guard against the cache key matching by
        // prefix rather than exact job name.
        var databaseTestResultStorage = new DatabaseTestResultStorage();
        databaseTestResultStorage.connectionSupplier = Mockito.mock(DatabaseTestResultStorage.ConnectionSupplier.class);
        var connection = Mockito.mock(Connection.class);
        Mockito.when(databaseTestResultStorage.connectionSupplier.connection()).thenReturn(connection);
        var preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.contains("SELECT suite, package"))).thenReturn(preparedStatement);

        List<CaseResult> fooCaseResults = getCaseResults("package1", "class11", 1, 0, 0);
        var fooResultSet = mockResultSet(fooCaseResults);
        Mockito.when(preparedStatement.executeQuery()).thenReturn(fooResultSet);

        var fooStorage = (DatabaseTestResultStorage.TestResultStorage) databaseTestResultStorage.load("foo", 1);
        assertEquals(1, fooStorage.getCaseResults().size());

        // When: the cache for the differently-named job "foobar" is invalidated, it must not affect
        // the already-cached entry for "foo".
        DatabaseTestResultStorage.invalidateJob("foobar");

        // Then
        fooStorage.getCaseResults();
        Mockito.verify(preparedStatement, Mockito.times(1)).executeQuery();
    }

    @Test
    void getSuite_doesNotHydrateFullBuild_mockDatabase() throws SQLException {
        // Given: an uncached build whose full case list has never been loaded. getSuite() is the path
        // used heavily by CaseResult#getPreviousResult() walking historical builds for age/"failed
        // since" computation, so it must not force a full-build hydration (all suites, all stdout/
        // stderr/stacktrace text) just to resolve one suite's cases.
        var databaseTestResultStorage = new DatabaseTestResultStorage();
        databaseTestResultStorage.connectionSupplier = Mockito.mock(DatabaseTestResultStorage.ConnectionSupplier.class);
        var connection = Mockito.mock(Connection.class);
        Mockito.when(databaseTestResultStorage.connectionSupplier.connection()).thenReturn(connection);

        var suiteStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(
                Mockito.argThat(sql -> sql != null && sql.contains("SELECT suite, package") && sql.contains("AND suite = ?"))))
                .thenReturn(suiteStatement);
        var fullStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(
                Mockito.argThat(sql -> sql != null && sql.contains("SELECT suite, package") && !sql.contains("AND suite = ?"))))
                .thenReturn(fullStatement);

        List<CaseResult> suite1Results = getCaseResults("package1", "class11", 1, 0, 0);
        var suite1ResultSet = mockResultSet(suite1Results);
        Mockito.when(suiteStatement.executeQuery()).thenReturn(suite1ResultSet);
        Mockito.when(fullStatement.executeQuery()).thenThrow(
                new AssertionError("getSuite() must not fall back to a full-build load"));

        String job = "jobName-getSuite";
        int build = 1;
        var testResultStorage = (DatabaseTestResultStorage.TestResultStorage) databaseTestResultStorage.load(job, build);

        // When
        SuiteResult suiteResult = testResultStorage.getSuite(suite1Results.get(0).getSuiteResult().getName());

        // Then: only the narrow, suite-scoped query ran.
        assertEquals(1, suiteResult.getCases().size());
        assertEquals(suite1Results.get(0).getName(), suiteResult.getCases().get(0).getName());
        Mockito.verify(suiteStatement, Mockito.times(1)).executeQuery();
        Mockito.verify(fullStatement, Mockito.never()).executeQuery();

        // And: looking up the same suite again reuses the already-loaded partial result rather than
        // issuing a second query.
        testResultStorage.getSuite(suite1Results.get(0).getSuiteResult().getName());
        Mockito.verify(suiteStatement, Mockito.times(1)).executeQuery();
    }

    private void printCaseResultsTable() throws Exception {
        printAndVerifyCaseResultsTable(false);
    }

    private void printAndVerifyCaseResultsTable(boolean verifyTable) throws Exception {
        try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT * FROM caseResults", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
             ResultSet result = statement.executeQuery()) {
            if (verifyTable) verifyTableStructure(connection);
            printResultSet(result);
        }
    }

    private void verifyTableStructure(Connection connection) throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet = metaData.getColumns(null, null, "caseresults", null);

        while (resultSet.next()) {
            String columnName = resultSet.getString("COLUMN_NAME");
            String columnType = resultSet.getString("TYPE_NAME");

            Map<String, String> mapOfColumnTypes = getCaseResultsColumnTypes();

            assertThat(mapOfColumnTypes, hasKey(columnName));
            assertThat("Unexpected columnType for column '" + columnName + "'",
                    columnType, equalToIgnoringCase(mapOfColumnTypes.get(columnName)));
        }
    }

    private static @NonNull Map<String, String> getCaseResultsColumnTypes() {
        Map<String, String> mapOfColumnTypes = new HashMap<>();
        mapOfColumnTypes.put("id", "bigserial");
        mapOfColumnTypes.put("job", "VARCHAR");
        mapOfColumnTypes.put("build", "INT4");
        mapOfColumnTypes.put("suite", "VARCHAR");
        mapOfColumnTypes.put("package", "VARCHAR");
        mapOfColumnTypes.put("classname", "VARCHAR");
        mapOfColumnTypes.put("testname", "VARCHAR");
        mapOfColumnTypes.put("errordetails", "VARCHAR");
        mapOfColumnTypes.put("skipped", "VARCHAR");
        mapOfColumnTypes.put("duration", "NUMERIC");
        mapOfColumnTypes.put("stdout", "VARCHAR");
        mapOfColumnTypes.put("stderr", "VARCHAR");
        mapOfColumnTypes.put("stacktrace", "VARCHAR");
        mapOfColumnTypes.put("timestamp", "TIMESTAMP");
        mapOfColumnTypes.put("testidentityhash", "VARCHAR");
        return mapOfColumnTypes;
    }

    private static void verifyCaseResultsMatch(String message, List<CaseResult> expectedCaseResults,
            List<CaseResult> actualCaseResults) {
        for (int i = 0; i < expectedCaseResults.size(); i++) {
            assertEquals(expectedCaseResults.get(i).getPackageName(),
                    actualCaseResults.get(i).getPackageName(),
                    "Unexpected packageName for " + message);
            assertEquals(expectedCaseResults.get(i).getClassName(),
                    actualCaseResults.get(i).getClassName(),
                    "Unexpected className for " + message);
            assertEquals(expectedCaseResults.get(i).getName(),
                    actualCaseResults.get(i).getName(),
                    "Unexpected name for " + message);
            assertEquals(expectedCaseResults.get(i).getErrorDetails(),
                    actualCaseResults.get(i).getErrorDetails(),
                    "Unexpected errorDetails for " + message);
            assertEquals(expectedCaseResults.get(i).getSkippedMessage(),
                    actualCaseResults.get(i).getSkippedMessage(),
                    "Unexpected skippedMessage for " + message);
            assertEquals(expectedCaseResults.get(i).getStdout(),
                    actualCaseResults.get(i).getStdout(),
                    "Unexpected stdout for " + message);
            assertEquals(expectedCaseResults.get(i).getStderr(),
                    actualCaseResults.get(i).getStderr(),
                    "Unexpected stderr for " + message);
            assertEquals(expectedCaseResults.get(i).getErrorStackTrace(),
                    actualCaseResults.get(i).getErrorStackTrace(),
                    "Unexpected errorStackTrace for " + message);
            assertEquals(expectedCaseResults.get(i).getDuration(),
                    actualCaseResults.get(i).getDuration(), 0.01, "Unexpected duration for " + message);
        }
    }

    private ResultSet mockResultSet(List<CaseResult> caseResults) throws SQLException {
        var resultSet = Mockito.mock(ResultSet.class);
        var hasNextCounter = new AtomicInteger(caseResults.size());
        Mockito.when(resultSet.next()).thenAnswer(invocation -> hasNextCounter.getAndDecrement() > 0);
        var packageCounter = new AtomicInteger(0);
        Mockito.when(resultSet.getString("package")).thenAnswer(invocation -> {
            int index = packageCounter.getAndIncrement();
            if (index < caseResults.size()) {
                return caseResults.get(index).getPackageName();
            } else {
                throw getMockException(index, "package");
            }
        });
        var classNameCounter = new AtomicInteger(0);
        Mockito.when(resultSet.getString("classname")).thenAnswer(invocation -> {
            int index = classNameCounter.getAndIncrement();
            if (index < caseResults.size()) {
                return caseResults.get(index).getClassName();
            } else {
                throw getMockException(index, "classname");
            }
        });
        var testNameCounter = new AtomicInteger(0);
        Mockito.when(resultSet.getString("testname")).thenAnswer(invocation -> {
            int index = testNameCounter.getAndIncrement();
            if (index < caseResults.size()) {
                return caseResults.get(index).getName();
            } else {
                throw getMockException(index, "testname");
            }
        });
        var errorDetailsCounter = new AtomicInteger(0);
        Mockito.when(resultSet.getString("errordetails")).thenAnswer(invocation -> {
            int index = errorDetailsCounter.getAndIncrement();
            if (index < caseResults.size()) {
                return caseResults.get(index).getErrorDetails();
            } else {
                throw getMockException(index, "errordetails");
            }
        });
        var skippedCounter = new AtomicInteger(0);
        Mockito.when(resultSet.getString("skipped")).thenAnswer(invocation -> {
            int index = skippedCounter.getAndIncrement();
            if (index < caseResults.size()) {
                return caseResults.get(index).getSkippedMessage();
            } else {
                throw getMockException(index, "skipped");
            }
        });
        var stdoutCounter = new AtomicInteger(0);
        Mockito.when(resultSet.getString("stdout")).thenAnswer(invocation -> {
            int index = stdoutCounter.getAndIncrement();
            if (index < caseResults.size()) {
                return caseResults.get(index).getStdout();
            } else {
                throw getMockException(index, "stdout");
            }
        });
        var stderrCounter = new AtomicInteger(0);
        Mockito.when(resultSet.getString("stderr")).thenAnswer(invocation -> {
            int index = stderrCounter.getAndIncrement();
            if (index < caseResults.size()) {
                return caseResults.get(index).getStderr();
            } else {
                throw getMockException(index, "stderr");
            }
        });
        var stacktraceCounter = new AtomicInteger(0);
        Mockito.when(resultSet.getString("stacktrace")).thenAnswer(invocation -> {
            int index = stacktraceCounter.getAndIncrement();
            if (index < caseResults.size()) {
                return caseResults.get(index).getErrorStackTrace();
            } else {
                throw getMockException(index, "stacktrace");
            }
        });
        var durationCounter = new AtomicInteger(0);
        Mockito.when(resultSet.getFloat("duration")).thenAnswer(invocation -> {
            int index = durationCounter.getAndIncrement();
            if (index < caseResults.size()) {
                return caseResults.get(index).getDuration();
            } else {
                throw getMockException(index, "duration");
            }
        });
        return resultSet;
    }

    private static @NonNull MockitoInitializationException getMockException(int index,
            String columnLabel) {
        return new MockitoInitializationException("Did not expect " + index + "'" + columnLabel + "' calls");
    }

    /**
     * Generates {@code length} pseudo-random (deterministically seeded, for reproducibility)
     * characters from the CJK Unified Ideographs block (3 bytes each in UTF-8). Deliberately
     * non-repeating, unlike a simple {@code "x".repeat(n)} pattern, so the result does not compress
     * away under PostgreSQL's transparent TOAST/PGLZ compression -- a short repeating pattern can
     * compress a "long" value down to a small fraction of its raw byte count, which would silently
     * undermine any test relying on the raw (uncompressed) byte count exceeding a storage limit.
     */
    private static String randomCjkText(int length, long seed) {
        var random = new java.util.Random(seed);
        var text = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            text.append((char) (0x4E00 + random.nextInt(0x9FFF - 0x4E00)));
        }
        return text.toString();
    }

    private List<CaseResult> getCaseResults(String packageName, String className, int numPass, int numFail,
            int numSkip) {
        List<CaseResult> caseResults = new ArrayList<>();
        String suite = packageName + "." + className;
        SuiteResult suiteResult = new SuiteResult(suite, null, null, null);
        if (numPass > 0) {
            for (int i = 0; i < numPass; i++) {
                CaseResult caseResult =
                        new CaseResult(suiteResult, suite, "testPass_" + i, null, null, 0.1f, "we did it!", null, null);
                caseResults.add(caseResult);
            }
        }
        if (numFail > 0) {
            for (int i = 0; i < numFail; i++) {
                CaseResult caseResult = new CaseResult(suiteResult, suite, "testFail_" + i, "error", null, 0.1f,
                        "Something went wrong!", "Failure!", "FailureStacktrace!");
                caseResults.add(caseResult);
            }
        }
        if (numSkip > 0) {
            for (int i = 0; i < numSkip; i++) {
                CaseResult caseResult =
                        new CaseResult(suiteResult, suite, "testSkip_" + i, null, "skipped", 0.1f, null, null, null);
                caseResults.add(caseResult);
            }
        }

        return caseResults;
    }

    private void setupPlugin(PostgreSQLContainer<?> postgres) {
        // comment this out if you hit the below test containers issue
        postgres.start();

        PostgreSQLDatabase database = new PostgreSQLDatabase(postgres.getHost() + ":" + postgres.getMappedPort(5432),
                postgres.getDatabaseName(), postgres.getUsername(), Secret.fromString(postgres.getPassword()), null);
        //        Use the below if test containers doesn't work for you, i.e. MacOS edge release of docker broken Sep 2020
        //        https://github.com/testcontainers/testcontainers-java/issues/3166
        //        PostgreSQLDatabase database = new PostgreSQLDatabase("localhost", "postgres", "postgres", Secret.fromString("postgres"), null);
        database.setValidationQuery("SELECT 1");
        GlobalDatabaseConfiguration.get().setDatabase(database);
        JunitTestResultStorageConfiguration.get().setStorage(new DatabaseTestResultStorage());
        DatabaseSchemaLoader.migrateSchema();
    }

    /**
     * @param resultSet the result set to print (note: that the resultSet should be scrollable and not forward only)
     * @throws Exception if an error occurs while accessing the ResultSet
     */
    private static void printResultSet(ResultSet resultSet) throws Exception {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnsNumber = resultSetMetaData.getColumnCount();

        // Step 2: Create an array to store the maximum width of each column
        int[] maxWidths = new int[columnsNumber];
        for (int i = 1; i <= columnsNumber; i++) {
            maxWidths[i - 1] = resultSetMetaData.getColumnName(i).length();
        }

        // Step 3: Iterate over the ResultSet to update the maximum width of each column
        int maxColumnValueLength = 15;
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                String columnValue = resultSet.getString(i);
                if (columnValue != null) {
                    if (columnValue.length() < maxColumnValueLength) {
                        maxWidths[i - 1] = Math.max(maxWidths[i - 1], columnValue.length());
                    } else {
                        maxWidths[i - 1] = Math.max(maxWidths[i - 1],
                                maxColumnValueLength + ("[" + (columnValue.length() - maxColumnValueLength)
                                        + "]").length());
                    }
                }
            }
        }
        resultSet.beforeFirst(); // Reset the cursor to the start of the ResultSet

        // Step 4: Create a format string based on the maximum widths of the columns
        StringBuilder formatBuilder = new StringBuilder();
        for (int i = 0; i < columnsNumber; i++) {
            if (i > 0) {
                formatBuilder.append(" | ");
            }
            formatBuilder.append("%-").append(maxWidths[i]).append("s");
        }
        String format = formatBuilder.toString();

        // Step 5: Print the column names using the format string
        String[] columnNames = new String[columnsNumber];
        for (int i = 1; i <= columnsNumber; i++) {
            columnNames[i - 1] = resultSetMetaData.getColumnName(i);
        }
        System.out.printf((format) + "%n", (Object[]) columnNames);

        // Step 6: Iterate over the ResultSet again to print the rows using the format string
        while (resultSet.next()) {
            String[] columnValues = new String[columnsNumber];
            for (int i = 1; i <= columnsNumber; i++) {
                String columnValue = resultSet.getString(i);
                if (columnValue != null && columnValue.length() > maxColumnValueLength) {
                    columnValue = columnValue.substring(0, maxColumnValueLength) + "[" + (columnValue.length()
                            - maxColumnValueLength) + "]";
                }
                columnValues[i - 1] = columnValue;
            }
            System.out.printf((format) + "%n", (Object[]) columnValues);
        }
    }
}
