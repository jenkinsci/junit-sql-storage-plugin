package io.jenkins.plugins.junit.storage.database.benchmarks;

import edu.umd.cs.findbugs.annotations.NonNull;
import hudson.model.queue.QueueTaskFuture;
import hudson.tasks.test.TestResultProjectAction;
import io.jenkins.plugins.casc.misc.jmh.CascJmhBenchmarkState;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.ExecutionException;
import jenkins.benchmark.jmh.JmhBenchmark;
import jenkins.model.Jenkins;
import org.jenkinsci.plugins.database.Database;
import org.jenkinsci.plugins.database.GlobalDatabaseConfiguration;
import org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition;
import org.jenkinsci.plugins.workflow.job.WorkflowJob;
import org.jenkinsci.plugins.workflow.job.WorkflowRun;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import static java.util.Objects.requireNonNull;

@JmhBenchmark
public class TrendGraphBenchmark {

    @State(Scope.Benchmark)
    public static class CascState extends CascJmhBenchmarkState {

        WorkflowJob p;

        public static final String SIMPLE_TEST_RESULT = """
                node {
                  writeFile file: 'x.xml', text: '''<testsuite name='sweet' time='200.0'>\
                <testcase classname='Klazz' name='test1' time='198.0'><error message='failure'/></testcase>\
                <testcase classname='Klazz' name='test2' time='2.0'/>\
                <testcase classname='other.Klazz' name='test3'><skipped message='Not actually run.'/></testcase>\
                </testsuite>'''
                  def s = junit 'x.xml'
                  echo(/summary: fail=$s.failCount skip=$s.skipCount pass=$s.passCount total=$s.totalCount/)
                  writeFile file: 'x.xml', text: '''<testsuite name='supersweet'>\
                <testcase classname='another.Klazz' name='test1'><error message='another failure'/></testcase>\
                </testsuite>'''
                  s = junit 'x.xml'
                  echo(/next summary: fail=$s.failCount skip=$s.skipCount pass=$s.passCount total=$s.totalCount/)
                }""";

        @NonNull
        @Override
        protected String getResourcePath() {
            return "config.yml";
        }

        @NonNull
        @Override
        protected Class<?> getEnclosingClass() {
            return TrendGraphBenchmark.class;
        }

        @Override
        public void setup() throws Exception {
            super.setup();


            Database database = GlobalDatabaseConfiguration.get().getDatabase();
            try (
                Connection connection = requireNonNull(database).getDataSource().getConnection();
                PreparedStatement statement = connection.prepareStatement("TRUNCATE table caseResults")
            ) {
                statement.execute();
            }

            Jenkins jenkins = Jenkins.get();

            p = jenkins.createProject(WorkflowJob.class, "p");
            p.setDefinition(new CpsFlowDefinition(
                SIMPLE_TEST_RESULT, true));
            int buildsToRun = 1000;
            List<QueueTaskFuture<WorkflowRun>> queueTaskFutures = new java.util.ArrayList<>(buildsToRun);
            for (int i = 0; i < buildsToRun; i++) {
                QueueTaskFuture<WorkflowRun> e = p.scheduleBuild2(0);
                e.waitForStart();
                queueTaskFutures.add(e);
                if (i % 10 == 0) {
                    Thread.sleep(100);
                }
            }
            System.out.println("Count of futures is: " + queueTaskFutures.size());
            queueTaskFutures.forEach(future -> {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            });

            System.out.println("Next build number: " + p.getNextBuildNumber());


        }
    }

    @Benchmark
    public void benchmark(CascState cascState, Blackhole blackhole) {
        TestResultProjectAction action = cascState.p.getAction(TestResultProjectAction.class);
        blackhole.consume(action.getBuildTrendModel());
    }
}
