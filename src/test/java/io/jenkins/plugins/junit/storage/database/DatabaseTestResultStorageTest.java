package io.jenkins.plugins.junit.storage.database;

import com.google.common.collect.ImmutableSet;
import hudson.model.Label;
import hudson.model.Result;
import hudson.tasks.junit.CaseResult;
import hudson.tasks.junit.PackageResult;
import hudson.tasks.junit.TestDurationResultSummary;
import hudson.tasks.junit.TestResultAction;
import hudson.tasks.junit.TestResultSummary;
import hudson.tasks.junit.TrendTestResultSummary;
import hudson.util.Secret;
import io.jenkins.plugins.junit.storage.JunitTestResultStorageConfiguration;
import io.jenkins.plugins.junit.storage.TestResultImpl;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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

import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class DatabaseTestResultStorageTest {

    @ClassRule
    public static BuildWatcher buildWatcher = new BuildWatcher();

    @Rule
    public JenkinsRule r = new JenkinsRule();

    @Test
    public void smokes() throws Exception {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:12-alpine")) {
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
            }
        }
    }

    private void setupPlugin(PostgreSQLContainer<?> postgres) {
        // comment this out if you hit the below test containers issue
//        postgres.start();
            
//        PostgreSQLDatabase database = new PostgreSQLDatabase(postgres.getHost() + ":" + postgres.getMappedPort(5432), postgres.getDatabaseName(), postgres.getUsername(), Secret.fromString(postgres.getPassword()), null);
//        Use the below if test containers doesn't work for you, i.e. MacOS edge release of docker broken Sep 2020 
//        https://github.com/testcontainers/testcontainers-java/issues/3166
        PostgreSQLDatabase database = new PostgreSQLDatabase("localhost", "postgres", "postgres", Secret.fromString("postgres"), null);
        database.setValidationQuery("SELECT 1");
        GlobalDatabaseConfiguration.get().setDatabase(database);
        JunitTestResultStorageConfiguration.get().setStorage(new DatabaseTestResultStorage());
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
