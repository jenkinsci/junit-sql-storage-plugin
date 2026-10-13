package io.jenkins.plugins.junit.storage.database;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import hudson.tasks.junit.CaseResult;
import hudson.tasks.junit.SuiteResult;
import hudson.tasks.junit.TestResult;
import hudson.util.StreamTaskListener;
import io.jenkins.plugins.junit.storage.TestResultImpl;
import org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition;
import org.jenkinsci.plugins.workflow.job.WorkflowJob;
import org.jvnet.hudson.test.JenkinsRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shared check for {@link TestResultImpl#getPreviousCaseResult}, run against each supported database.
 */
final class PreviousCaseResultTestSupport {

    private PreviousCaseResultTestSupport() {}

    private static final int COMMON_CASES = 1000;
    private static final int OLDER_CASES = DatabaseTestResultStorage.PREVIOUS_CASE_RESULT_FULL_LOAD_THRESHOLD + 100;

    /**
     * Publishes four builds, each case's duration being the build number it was published in, and checks
     * that every case of build 4 resolves to its nearest earlier occurrence:
     * <ul>
     * <li>{@code common*} (in every build) and {@code since3} resolve to build 3, the immediately previous build;
     * <li>{@code since2*} (absent from build 3) resolve to build 2, so build 2 has to be matched as well;
     * <li>{@code onlyIn1} (absent from builds 2 and 3) resolves to build 1 after loading whole builds has
     * stopped, through the batched identity-match lookup;
     * <li>{@code new} has no earlier occurrence.
     * </ul>
     */
    static void assertPreviousCaseResultsResolveToNearestBuild(JenkinsRule jenkinsRule,
            DatabaseTestResultStorage storage) throws Exception {
        WorkflowJob job = jenkinsRule.createProject(WorkflowJob.class, "previous-case-results");
        job.setDefinition(new CpsFlowDefinition("echo 'noop'", true));
        var listener = StreamTaskListener.fromStdout();
        for (int build = 1; build <= 4; build++) {
            var run = jenkinsRule.buildAndAssertSuccess(job);
            assertEquals(build, run.getNumber());
            StringBuilder xml = new StringBuilder("<testsuites>");
            xml.append("<testsuite name='common'>");
            for (int i = 0; i < COMMON_CASES; i++) {
                xml.append(testCase("org.example.Common" + (i % 10) + "Test", "common" + i, build, i % 3 == 0));
            }
            xml.append("</testsuite><testsuite name='other'>");
            if (build == 1 || build == 4) {
                xml.append(testCase("org.example.OtherTest", "onlyIn1", build, build == 1));
            }
            if (build == 2 || build == 4) {
                for (int i = 0; i < OLDER_CASES; i++) {
                    xml.append(testCase("org.example.Since2Test", "since2" + i, build, false));
                }
            }
            if (build >= 3) {
                xml.append(testCase("org.example.OtherTest", "since3", build, false));
            }
            if (build == 4) {
                xml.append(testCase("org.example.OtherTest", "new", build, false));
            }
            xml.append("</testsuite></testsuites>");
            storage.createRemotePublisher(run).publish(CaseResultSummaryTestSupport.testResult(xml.toString()),
                    listener);
        }

        TestResultImpl impl = storage.load(job.getFullName(), 4);
        assertTrue(impl.supportsPreviousCaseResultLookup());
        Map<Integer, Integer> resolvedToBuild = new HashMap<>();
        int checked = 0;
        for (SuiteResult suite : new TestResult(impl).getSuites()) {
            for (CaseResult current : suite.getCases()) {
                Optional<CaseResult> previous = impl.getPreviousCaseResult(current);
                String name = current.getName();
                int expectedBuild = name.startsWith("common") || name.equals("since3") ? 3
                        : name.startsWith("since2") ? 2
                        : name.equals("onlyIn1") ? 1
                        : 0;
                if (expectedBuild == 0) {
                    assertEquals(Optional.empty(), previous, name);
                } else {
                    assertTrue(previous.isPresent(), name);
                    assertEquals(expectedBuild, (int) previous.get().getDuration(), name);
                    assertEquals(current.getClassName(), previous.get().getClassName(), name);
                    assertEquals(name, previous.get().getName());
                    assertEquals(suite.getName(), previous.get().getSuiteResult().getName(), name);
                    assertEquals(expectedBuild, previous.get().getRun().getNumber(), name);
                }
                resolvedToBuild.merge(expectedBuild, 1, Integer::sum);
                checked++;
            }
        }
        assertEquals(COMMON_CASES + OLDER_CASES + 3, checked);
        assertEquals(Map.of(3, COMMON_CASES + 1, 2, OLDER_CASES, 1, 1, 0, 1), resolvedToBuild);
        // a failing case in build 1 is reported as such through the batched identity-match lookup
        CaseResult onlyIn1 = new TestResult(impl).getSuite("other").getCase("org.example.OtherTest.onlyIn1");
        assertTrue(onlyIn1.getPreviousResult().isFailed());
        assertEquals(CaseResult.Status.FIXED, onlyIn1.getCondition());
    }

    private static String testCase(String className, String name, int build, boolean failed) {
        return "<testcase classname='" + className + "' name='" + name + "' time='" + build + "'>"
                + (failed ? "<failure message='broken'/>" : "") + "</testcase>";
    }
}
