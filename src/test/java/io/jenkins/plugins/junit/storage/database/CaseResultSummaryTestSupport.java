package io.jenkins.plugins.junit.storage.database;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import hudson.Util;
import hudson.tasks.junit.CaseResult;
import hudson.tasks.junit.SuiteResult;
import hudson.tasks.junit.TestResult;
import hudson.util.StreamTaskListener;
import io.jenkins.plugins.junit.storage.CaseResultSummary;
import io.jenkins.plugins.junit.storage.TestResultImpl;
import org.apache.commons.io.FileUtils;
import org.apache.tools.ant.DirectoryScanner;
import org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition;
import org.jenkinsci.plugins.workflow.job.WorkflowJob;
import org.jenkinsci.plugins.workflow.job.WorkflowRun;
import org.jvnet.hudson.test.JenkinsRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shared check for {@link TestResultImpl#forEachCaseResultSummary}, run against each supported database.
 */
final class CaseResultSummaryTestSupport {

    private CaseResultSummaryTestSupport() {}

    /**
     * Publishes builds 1 and 2 with a mix of passing, failing and skipped cases (build 3 publishes nothing)
     * and asserts that streaming summaries over the range agrees with fully loading each build.
     */
    static void assertSummariesMatchFullLoad(JenkinsRule jenkinsRule, DatabaseTestResultStorage storage)
            throws Exception {
        WorkflowJob job = jenkinsRule.createProject(WorkflowJob.class, "summaries");
        job.setDefinition(new CpsFlowDefinition("echo 'noop'", true));
        WorkflowRun build1 = jenkinsRule.buildAndAssertSuccess(job);
        WorkflowRun build2 = jenkinsRule.buildAndAssertSuccess(job);
        jenkinsRule.buildAndAssertSuccess(job);

        var listener = StreamTaskListener.fromStdout();
        storage.createRemotePublisher(build1).publish(testResult(
                "<testsuite name='s1'>"
                        + "<testcase classname='org.example.ATest' name='passes' time='0.5'/>"
                        + "<testcase classname='org.example.ATest' name='fails' time='0.25'><failure message='x'/></testcase>"
                        + "<testcase classname='org.example.ATest' name='skipped' time='0'><skipped/></testcase>"
                        + "<testcase classname='RootTest' name='errors' time='1'><error message='y'/></testcase>"
                        + "<testcase classname='RootTest' name='traceOnly' time='1'><failure>at Foo.bar</failure></testcase>"
                        + "</testsuite>"), listener);
        storage.createRemotePublisher(build2).publish(testResult(
                "<testsuite name='s1'>"
                        + "<testcase classname='org.example.ATest' name='passes' time='0.5'/>"
                        + "<testcase classname='org.example.ATest' name='fails' time='0.25'/>"
                        + "<testcase classname='RootTest' name='errors' time='1'/>"
                        + "</testsuite>"), listener);

        TestResultImpl impl = storage.load(job.getFullName(), 3);
        assertTrue(impl.supportsCaseResultSummaries());

        List<CaseResultSummary> summaries = new ArrayList<>();
        impl.forEachCaseResultSummary(1, 3, summaries::add);

        List<String> expected = new ArrayList<>();
        for (int build : new int[] {1, 2}) {
            for (SuiteResult suite : new TestResult(storage.load(job.getFullName(), build)).getSuites()) {
                for (CaseResult c : suite.getCases()) {
                    expected.add(describe(build, suite.getName(), c.getClassName(), c.getPackageName(),
                            c.getSimpleName(), c.getName(), c.isFailed(), c.isSkipped(), c.getDuration()));
                }
            }
        }
        List<String> actual = summaries.stream()
                .map(s -> describe(s.getBuild(), s.getSuiteName(), s.getClassName(), s.getPackageName(),
                        s.getSimpleName(), s.getName(), s.isFailed(), s.isSkipped(), s.getDuration()))
                .toList();
        assertEquals(8, actual.size(), actual::toString);
        assertThat(actual, containsInAnyOrder(expected.toArray(new String[0])));
        // a failure without a message only has a stack trace, but still counts as failed
        assertTrue(summaries.stream()
                .filter(s -> s.getName().equals("traceOnly"))
                .allMatch(CaseResultSummary::isFailed));
        // grouped by build, ascending
        assertThat(summaries.stream().map(CaseResultSummary::getBuild).distinct().toList(), contains(1, 2));
        assertTrue(summaries.subList(0, 5).stream().allMatch(s -> s.getBuild() == 1));

        List<String> onlyBuild2 = new ArrayList<>();
        impl.forEachCaseResultSummary(2, 2, s -> onlyBuild2.add(s.getBuild() + ":" + s.getName()));
        assertEquals(3, onlyBuild2.size());
        assertThat(onlyBuild2, everyItem(startsWith("2:")));
    }

    private static String describe(int build, String suite, String className, String packageName,
            String simpleName, String name, boolean failed, boolean skipped, float duration) {
        return build + "|" + suite + "|" + className + "|" + packageName + "|" + simpleName + "|" + name
                + "|failed=" + failed + "|skipped=" + skipped + "|" + duration;
    }

    private static TestResult testResult(String xml) throws IOException {
        File reportsDir = Files.createTempDirectory("case-result-summaries").toFile();
        FileUtils.writeStringToFile(new File(reportsDir, "x.xml"), xml, StandardCharsets.UTF_8);
        DirectoryScanner directoryScanner = Util.createFileSet(reportsDir, "*.xml").getDirectoryScanner();
        TestResult testResult = new TestResult(System.currentTimeMillis(), directoryScanner, false, false, null, false);
        testResult.tally();
        return testResult;
    }
}
