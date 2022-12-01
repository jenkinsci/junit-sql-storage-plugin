package io.jenkins.plugins.junit.storage.database;

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
import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.commons.io.FileUtils;
import org.jenkinsci.plugins.database.GlobalDatabaseConfiguration;
import org.jenkinsci.plugins.database.postgresql.PostgreSQLDatabase;
import org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition;
import org.jenkinsci.plugins.workflow.job.WorkflowJob;
import org.jenkinsci.plugins.workflow.job.WorkflowRun;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.jvnet.hudson.test.BuildWatcher;
import org.jvnet.hudson.test.JenkinsRule;
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
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class DatabaseTestResultStorageTest {

    public static final String TEST_IMAGE = "postgres:12-alpine";
    @ClassRule
    public static BuildWatcher buildWatcher = new BuildWatcher();

    @Rule
    public JenkinsRule r = new JenkinsRule();

    @Test
    public void smokes() throws Exception {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(TEST_IMAGE)) {
            setupPlugin(postgres);

            r.createOnlineSlave(Label.get("remote"));
            WorkflowJob p = r.createProject(WorkflowJob.class, "p");
            p.setDefinition(new CpsFlowDefinition(
                "node('remote') {\n" +
                    "  writeFile file: 'x.xml', text: '''<testsuite name='sweet' time='200.0'>" +
                    "<testcase classname='Klazz' name='test1' time='198.0'><error message='failure'/></testcase>" +
                    "<testcase classname='Klazz' name='test2' time='2.0'/>" +
                    "<testcase classname='other.Klazz' name='test3'><skipped message='Not actually run.'/></testcase>" +
                    "</testsuite>'''\n" +
                    "  def s = junit 'x.xml'\n" +
                    "  echo(/summary: fail=$s.failCount skip=$s.skipCount pass=$s.passCount total=$s.totalCount/)\n" +
                    "  writeFile file: 'x.xml', text: '''<testsuite name='supersweet'>" +
                    "<testcase classname='another.Klazz' name='test1'><error message='another failure'/></testcase>" +
                    "</testsuite>'''\n" +
                    "  s = junit 'x.xml'\n" +
                    "  echo(/next summary: fail=$s.failCount skip=$s.skipCount pass=$s.passCount total=$s.totalCount/)\n" +
                    "}", true));
            WorkflowRun b = p.scheduleBuild2(0).get();
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource().getConnection();
                 PreparedStatement statement = connection.prepareStatement("SELECT * FROM caseResults");
                 ResultSet result = statement.executeQuery()) {
                printResultSet(result);
            }
            // TODO verify table structure
            r.assertBuildStatus(Result.UNSTABLE, b);
            r.assertLogContains("summary: fail=1 skip=1 pass=1 total=3", b);
            r.assertLogContains("next summary: fail=1 skip=0 pass=0 total=1", b);
            assertFalse(new File(b.getRootDir(), "junitResult.xml").isFile());
            {
                String buildXml = FileUtils.readFileToString(new File(b.getRootDir(), "build.xml"));
                Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new ByteArrayInputStream(buildXml.getBytes(StandardCharsets.UTF_8)));
                NodeList testResultActionList = doc.getElementsByTagName("hudson.tasks.junit.TestResultAction");
                assertEquals(buildXml, 1, testResultActionList.getLength());
                Element testResultAction = (Element) testResultActionList.item(0);
                NodeList childNodes = testResultAction.getChildNodes();
                Set<String> childNames = new TreeSet<>();
                for (int i = 0; i < childNodes.getLength(); i++) {
                    Node item = childNodes.item(i);
                    if (item instanceof Element) {
                        childNames.add(((Element) item).getTagName());
                    }
                }
                assertEquals(buildXml, ImmutableSet.of("healthScaleFactor", "testData", "descriptions"), childNames);
            }
            {
                TestResultAction a = b.getAction(TestResultAction.class);
                assertNotNull(a);
                assertEquals(2, a.getFailCount());
                assertEquals(1, a.getSkipCount());
                assertEquals(4, a.getTotalCount());
                assertEquals(2, a.getResult().getFailCount());
                assertEquals(1, a.getResult().getSkipCount());
                assertEquals(4, a.getResult().getTotalCount());
                assertEquals(1, a.getResult().getPassCount());
                assertEquals(2, a.getResult().getSuites().size());
                List<CaseResult> failedTests = a.getFailedTests();
                assertEquals(2, failedTests.size());
                final CaseResult klazzTest1 = failedTests.get(0);
                assertEquals("Klazz", klazzTest1.getClassName());
                assertEquals("test1", klazzTest1.getName());
                assertEquals("failure", klazzTest1.getErrorDetails());
                assertThat(klazzTest1.getDuration(), is(198.0f));
                assertEquals("another.Klazz", failedTests.get(1).getClassName());
                assertEquals("test1", failedTests.get(1).getName());
                assertEquals("another failure", failedTests.get(1).getErrorDetails());

                List<CaseResult> skippedTests = a.getSkippedTests();
                assertEquals(1, skippedTests.size());
                assertEquals("other.Klazz", skippedTests.get(0).getClassName());
                assertEquals("test3", skippedTests.get(0).getName());
                assertEquals("Not actually run.", skippedTests.get(0).getSkippedMessage());

                List<CaseResult> passedTests = a.getPassedTests();
                assertEquals(1, passedTests.size());
                assertEquals("Klazz", passedTests.get(0).getClassName());
                assertEquals("test2", passedTests.get(0).getName());

                PackageResult another = a.getResult().byPackage("another");
                List<CaseResult> packageFailedTests = another.getFailedTests();
                assertEquals(1, packageFailedTests.size());
                assertEquals("another.Klazz", packageFailedTests.get(0).getClassName());

                PackageResult other = a.getResult().byPackage("other");
                List<CaseResult> packageSkippedTests = other.getSkippedTests();
                assertEquals(1, packageSkippedTests.size());
                assertEquals("other.Klazz", packageSkippedTests.get(0).getClassName());
                assertEquals("Not actually run.", packageSkippedTests.get(0).getSkippedMessage());

                PackageResult root = a.getResult().byPackage("(root)");
                List<CaseResult> rootPassedTests = root.getPassedTests();
                assertEquals(1, rootPassedTests.size());
                assertEquals("Klazz", rootPassedTests.get(0).getClassName());

                TestResultImpl pluggableStorage = requireNonNull(a.getResult().getPluggableStorage());
                List<TrendTestResultSummary> trendTestResultSummary = pluggableStorage.getTrendTestResultSummary();
                assertThat(trendTestResultSummary, hasSize(1));
                TestResultSummary testResultSummary = trendTestResultSummary.get(0).getTestResultSummary();
                assertThat(testResultSummary.getFailCount(), equalTo(2));
                assertThat(testResultSummary.getPassCount(), equalTo(1));
                assertThat(testResultSummary.getSkipCount(), equalTo(1));
                assertThat(testResultSummary.getTotalCount(), equalTo(4));

                int countOfBuildsWithTestResults = pluggableStorage.getCountOfBuildsWithTestResults();
                assertThat(countOfBuildsWithTestResults, is(1));

                final List<TestDurationResultSummary> testDurationResultSummary = pluggableStorage.getTestDurationResultSummary();
                assertThat(testDurationResultSummary.get(0).getDuration(), is(200));

                //check storage getSuites method
                Collection<SuiteResult> suiteResults = pluggableStorage.getSuites();
                assertThat(suiteResults, hasSize(2));
                //check the two suites name
                assertThat(suiteResults, containsInAnyOrder(hasProperty("name", equalTo("supersweet")), hasProperty("name", equalTo("sweet"))));

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
    }

    @Test
    public void testResultCleanup() throws Exception {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(TEST_IMAGE)) {
            setupPlugin(postgres);

            r.createOnlineSlave(Label.get("remote"));
            WorkflowJob p = r.createProject(WorkflowJob.class, "p");
            p.setDefinition(new CpsFlowDefinition(
                    "node('remote') {\n" +
                            "  writeFile file: 'x.xml', text: '''<testsuite name='sweet' time='200.0'>" +
                            "<testcase classname='Klazz' name='test1' time='198.0'><error message='failure'/></testcase>" +
                            "<testcase classname='Klazz' name='test2' time='2.0'/>" +
                            "<testcase classname='other.Klazz' name='test3'><skipped message='Not actually run.'/></testcase>" +
                            "</testsuite>'''\n" +
                            "  def s = junit 'x.xml'\n" +
                            "  echo(/summary: fail=$s.failCount skip=$s.skipCount pass=$s.passCount total=$s.totalCount/)\n" +
                            "  writeFile file: 'x.xml', text: '''<testsuite name='supersweet'>" +
                            "<testcase classname='another.Klazz' name='test1'><error message='another failure'/></testcase>" +
                            "</testsuite>'''\n" +
                            "  s = junit 'x.xml'\n" +
                            "  echo(/next summary: fail=$s.failCount skip=$s.skipCount pass=$s.passCount total=$s.totalCount/)\n" +
                            "}", true));

            WorkflowRun b = p.scheduleBuild2(0).get();
            r.assertBuildStatus(Result.UNSTABLE, b);

            b = p.scheduleBuild2(0).get();
            r.assertBuildStatus(Result.UNSTABLE, b);

            b = p.scheduleBuild2(0).get();
            r.assertBuildStatus(Result.UNSTABLE, b);

            // 3 sets of test results
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource().getConnection();
                 PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM caseResults");
                 ResultSet result = statement.executeQuery()) {
                result.next();
                int anInt = result.getInt(1);
                assertThat(anInt, is(12));
            }

            b.delete();

            // 2 sets of test results
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource().getConnection();
                 PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM caseResults");
                 ResultSet result = statement.executeQuery()) {
                result.next();
                int anInt = result.getInt(1);
                assertThat(anInt, is(8));
            }

            p.delete();

            // 0 test results
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource().getConnection();
                 PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM caseResults");
                 ResultSet result = statement.executeQuery()) {
                result.next();
                int anInt = result.getInt(1);
                assertThat(anInt, is(0));
            }
        }
    }

    @Test
    public void testResultCleanup_skipped_if_disabled() throws Exception {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(TEST_IMAGE)) {
            setupPlugin(postgres);

            DatabaseTestResultStorage storage = new DatabaseTestResultStorage();
            storage.setSkipCleanupRunsOnDeletion(true);
            JunitTestResultStorageConfiguration.get().setStorage(storage);


            r.createOnlineSlave(Label.get("remote"));
            WorkflowJob p = r.createProject(WorkflowJob.class, "p");
            p.setDefinition(new CpsFlowDefinition(
                    "node('remote') {\n" +
                            "  writeFile file: 'x.xml', text: '''<testsuite name='sweet' time='200.0'>" +
                            "<testcase classname='Klazz' name='test1' time='198.0'><error message='failure'/></testcase>" +
                            "<testcase classname='Klazz' name='test2' time='2.0'/>" +
                            "<testcase classname='other.Klazz' name='test3'><skipped message='Not actually run.'/></testcase>" +
                            "</testsuite>'''\n" +
                            "  def s = junit 'x.xml'\n" +
                            "  echo(/summary: fail=$s.failCount skip=$s.skipCount pass=$s.passCount total=$s.totalCount/)\n" +
                            "  writeFile file: 'x.xml', text: '''<testsuite name='supersweet'>" +
                            "<testcase classname='another.Klazz' name='test1'><error message='another failure'/></testcase>" +
                            "</testsuite>'''\n" +
                            "  s = junit 'x.xml'\n" +
                            "  echo(/next summary: fail=$s.failCount skip=$s.skipCount pass=$s.passCount total=$s.totalCount/)\n" +
                            "}", true));

            WorkflowRun b = p.scheduleBuild2(0).get();
            r.assertBuildStatus(Result.UNSTABLE, b);


            // 1 sets of test results
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource().getConnection();
                 PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM caseResults");
                 ResultSet result = statement.executeQuery()) {
                result.next();
                int anInt = result.getInt(1);
                assertThat(anInt, is(4));
            }

            b.delete();

            // 1 set of test results
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource().getConnection();
                 PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM caseResults");
                 ResultSet result = statement.executeQuery()) {
                result.next();
                int anInt = result.getInt(1);
                assertThat(anInt, is(4));
            }

            p.delete();

            // 1 set of test results
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource().getConnection();
                 PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM caseResults");
                 ResultSet result = statement.executeQuery()) {
                result.next();
                int anInt = result.getInt(1);
                assertThat(anInt, is(4));
            }
        }
    }


    @Test
    public void testResult_long_string() throws Exception {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(TEST_IMAGE)) {
            setupPlugin(postgres);

            DatabaseTestResultStorage storage = new DatabaseTestResultStorage();
            JunitTestResultStorageConfiguration.get().setStorage(storage);


            WorkflowJob p = r.createProject(WorkflowJob.class, "p");

            p.setDefinition(new CpsFlowDefinition(
                    "node('remote') {\n" +
                            "  def s = junit 'x.xml'\n" +
                            "  echo(/summary: fail=$s.failCount skip=$s.skipCount pass=$s.passCount total=$s.totalCount/)\n" +
                            "  writeFile file: 'x.xml', text: '''<testsuite name='supersweet'>" +
                            "<testcase classname='another.Klazz' name='test1'><error message='another failure'/></testcase>" +
                            "</testsuite>'''\n" +
                            "  s = junit 'x.xml'\n" +
                            "  echo(/next summary: fail=$s.failCount skip=$s.skipCount pass=$s.passCount total=$s.totalCount/)\n" +
                            "}", true));

            //Because writeFile can't handle long string
            //We use file copy to prepare the test result
            DumbSlave agent = r.createOnlineSlave(Label.get("remote"));
            URI longStringFileUri = DatabaseTestResultStorageTest.class.getResource("long-string.xml").toURI();
            agent.getWorkspaceFor(p).child("x.xml").copyFrom(longStringFileUri.toURL());
            WorkflowRun b = p.scheduleBuild2(0).get();
            r.assertBuildStatus(Result.UNSTABLE, b);


            // 1 sets of test results
            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource().getConnection();
                 PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM caseResults");
                 ResultSet result = statement.executeQuery()) {
                result.next();
                int anInt = result.getInt(1);
                assertThat(anInt, is(4));
            }

            try (Connection connection = requireNonNull(GlobalDatabaseConfiguration.get().getDatabase()).getDataSource().getConnection();
                 PreparedStatement statement = connection.prepareStatement("SELECT job, build, suite, package, className, testName, errorDetails, skipped, duration, stdout, stderr, stacktrace FROM caseResults where testName='test1'");
                 ResultSet result = statement.executeQuery()) {
                result.next();
                String suiteNameInDatabase = result.getString("suite");
                assertThat(suiteNameInDatabase.length(), is(MAX_SUITE_LENGTH));
                String errorDetailsInDatabase = result.getString("errorDetails");
                assertThat(errorDetailsInDatabase.length(), is(MAX_ERROR_DETAILS_LENGTH));
            }


        }
    }


    private void setupPlugin(PostgreSQLContainer<?> postgres) throws SQLException {
        // comment this out if you hit the below test containers issue
        postgres.start();

        PostgreSQLDatabase database = new PostgreSQLDatabase(postgres.getHost() + ":" + postgres.getMappedPort(5432), postgres.getDatabaseName(), postgres.getUsername(), Secret.fromString(postgres.getPassword()), null);
//        Use the below if test containers doesn't work for you, i.e. MacOS edge release of docker broken Sep 2020
//        https://github.com/testcontainers/testcontainers-java/issues/3166
//        PostgreSQLDatabase database = new PostgreSQLDatabase("localhost", "postgres", "postgres", Secret.fromString("postgres"), null);
        database.setValidationQuery("SELECT 1");
        GlobalDatabaseConfiguration.get().setDatabase(database);
        JunitTestResultStorageConfiguration.get().setStorage(new DatabaseTestResultStorage());
        DatabaseSchemaLoader.migrateSchema();
    }

    // https://gist.github.com/mikbuch/299568988fa7997cb28c7c84309232b1
    private static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) {
                System.out.print("\t|\t");
            }
            System.out.print(rsmd.getColumnName(i));
        }
        System.out.println();
        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) {
                    System.out.print("\t|\t");
                }
                System.out.print(rs.getString(i));
            }
            System.out.println();
        }
    }


}
