package io.jenkins.plugins.junit.storage.database;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
import hudson.model.Label;
import hudson.model.Result;
import hudson.slaves.DumbSlave;
import hudson.tasks.junit.CaseResult;
import hudson.tasks.junit.PackageResult;
import hudson.tasks.junit.SuiteResult;
import hudson.tasks.junit.TestDurationResultSummary;
import hudson.tasks.junit.TestResultAction;
import hudson.tasks.junit.TestResultSummary;
import hudson.tasks.junit.TrendTestResultSummary;
import hudson.util.Secret;
import io.jenkins.plugins.junit.storage.JunitTestResultStorageConfiguration;
import io.jenkins.plugins.junit.storage.TestResultImpl;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.commons.io.FileUtils;
import org.jenkinsci.plugins.database.GlobalDatabaseConfiguration;
import org.jenkinsci.plugins.database.postgresql.PostgreSQLDatabase;
import org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition;
import org.jenkinsci.plugins.workflow.job.WorkflowJob;
import org.jenkinsci.plugins.workflow.job.WorkflowRun;
import org.jetbrains.annotations.NotNull;
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
import static io.jenkins.plugins.junit.storage.database.DatabaseTestResultStorage.MAX_SUITE_LENGTH;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
            final CaseResult klazzTest1 = failedTests.get(0);
            assertEquals("Klazz", klazzTest1.getClassName());
            assertEquals("test1", klazzTest1.getName());
            assertEquals("failure", klazzTest1.getErrorDetails());
            assertThat(klazzTest1.getDuration(), is(198.0f));
            assertEquals("another.Klazz", failedTests.get(1).getClassName());
            assertEquals("test1", failedTests.get(1).getName());
            assertEquals("another failure", failedTests.get(1).getErrorDetails());

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
    void getCaseResults_mockDatabase() throws SQLException {
        // Given
        var databaseTestResultStorage = new DatabaseTestResultStorage();
        databaseTestResultStorage.connectionSupplier = Mockito.mock(DatabaseTestResultStorage.ConnectionSupplier.class);
        var connection = Mockito.mock(Connection.class);
        Mockito.when(databaseTestResultStorage.connectionSupplier.connection()).thenReturn(connection);
        var preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(preparedStatement);
        List<CaseResult> expectedCaseResults = getCaseResults("package1", "class11", 1, 1, 1);
        expectedCaseResults.addAll(getCaseResults("package1", "class12", 2, 0, 0));
        expectedCaseResults.addAll(getCaseResults("package2", "class21", 3, 0, 2));

        var resultSet = mockResultSet(expectedCaseResults);
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        String job = "jobName";
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

        Mockito.verify(preparedStatement, Mockito.times(1)).executeQuery();
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

    private static @NotNull Map<String, String> getCaseResultsColumnTypes() {
        Map<String, String> mapOfColumnTypes = new HashMap<>();
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

    private static @NotNull MockitoInitializationException getMockException(int index,
            String columnLabel) {
        return new MockitoInitializationException("Did not expect " + index + "'" + columnLabel + "' calls");
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
