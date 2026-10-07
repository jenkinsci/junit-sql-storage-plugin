#!/usr/bin/env bash
# Creates a scripted-pipeline Jenkins job for concurrent-publish load testing against the JUnit SQL
# Storage plugin: generates synthetic JUnit XML (same approach as examples/issue-532/Jenkinsfile) and
# publishes it via the `junit` step, so it exercises the real RemotePublisher/DatabaseTestResultStorage
# write path rather than inserting rows directly.
#
# Safe to point at a remote Jenkins (see scripts/benchmark/lib.sh for connection env vars). Only ever
# run this against a disposable Jenkins instance/job -- it is designed to generate load, not to be a
# realistic production job.
#
# Usage:
#   ./scripts/benchmark/create-load-job.sh [job-name]
#
# Env vars (all optional, see lib.sh for defaults):
#   JENKINS_URL, JENKINS_USER, JENKINS_API_TOKEN, LOAD_JOB_NAME
#   AGENT_LABEL   - label of the agent(s) that should run builds of this job (default: "agent")
#   CASE_COUNT    - synthetic test case count per build (default: 1320, matches the "smoke" profile
#                   from examples/issue-532/Jenkinsfile)
#   PACKAGE_COUNT - number of distinct packages/suites to spread cases across (default: 38)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./lib.sh

JOB_NAME="${1:-$LOAD_JOB_NAME}"
AGENT_LABEL="${AGENT_LABEL:-agent}"
CASE_COUNT="${CASE_COUNT:-1320}"
PACKAGE_COUNT="${PACKAGE_COUNT:-38}"

bench_require curl

PIPELINE_SCRIPT=$(cat <<GROOVY
// RUN_ID is declared as a job property directly in the created job's config.xml (not via a
// properties() step here) so it is available starting from the very first build -- a properties()
// step only takes effect for builds *after* the one that executes it.
node('${AGENT_LABEL}') {
    dir("bench-\${env.BUILD_NUMBER}") {
        stage('Generate report') {
            // The generator deliberately avoids any backslash escape sequences (no \" and no \n) in
            // its source: this text passes through an unquoted Bash heredoc and then a Groovy
            // triple-single-quoted string before it ever reaches Python, and both of those layers
            // perform their own backslash-escape processing, so any \" or \n written here would be
            // silently consumed before Python sees it. Using single-quoted f-strings for attribute
            // values (so the literal double quotes they contain need no escaping) and print(...,
            // file=f) (which appends the newline itself) sidesteps that entirely.
            writeFile file: 'gen.py', text: '''
suite_count = ${PACKAGE_COUNT}
case_count = ${CASE_COUNT}
base_cases_per_suite = max(1, case_count // suite_count)
remainder = max(0, case_count - base_cases_per_suite * suite_count)
idx = 0
for s in range(suite_count):
    cases_this_suite = base_cases_per_suite + (1 if s < remainder else 0)
    with open(f"result-{s}.xml", "w") as f:
        print(f'<testsuite name="bench.suite{s}" tests="{cases_this_suite}">', file=f)
        for c in range(cases_this_suite):
            idx += 1
            name = f"test{c}"
            if idx % 37 == 0:
                print(f'<testcase classname="bench.suite{s}.Klazz" name="{name}" time="0.01"><failure message="synthetic failure {idx}"/></testcase>', file=f)
            elif idx % 53 == 0:
                print(f'<testcase classname="bench.suite{s}.Klazz" name="{name}" time="0.0"><skipped/></testcase>', file=f)
            else:
                print(f'<testcase classname="bench.suite{s}.Klazz" name="{name}" time="0.01"/>', file=f)
        print('</testsuite>', file=f)
'''
            sh 'python3 gen.py || python gen.py'
        }
        stage('Publish') {
            junit testResults: 'result-*.xml', allowEmptyResults: false
        }
    }
}
GROOVY
)

JOB_CONFIG=$(cat <<XML
<?xml version='1.1' encoding='UTF-8'?>
<flow-definition plugin="workflow-job">
  <description>Created by scripts/benchmark/create-load-job.sh for concurrent-publish load testing. Safe to delete.</description>
  <keepDependencies>false</keepDependencies>
  <properties>
    <hudson.model.ParametersDefinitionProperty>
      <parameterDefinitions>
        <hudson.model.StringParameterDefinition>
          <name>RUN_ID</name>
          <description>Unique value per trigger so Jenkins does not coalesce rapid-fire concurrent build requests into one queue item</description>
          <defaultValue>0</defaultValue>
          <trim>false</trim>
        </hudson.model.StringParameterDefinition>
      </parameterDefinitions>
    </hudson.model.ParametersDefinitionProperty>
  </properties>
  <definition class="org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition" plugin="workflow-cps">
    <script><![CDATA[${PIPELINE_SCRIPT}]]></script>
    <sandbox>true</sandbox>
  </definition>
  <triggers/>
  <disabled>false</disabled>
</flow-definition>
XML
)

crumb="$(bench_crumb_header)"
crumb_args=()
[[ -n "$crumb" ]] && crumb_args=(-H "$crumb")

bench_log "Creating job '${JOB_NAME}' at ${JENKINS_URL} (agent label: ${AGENT_LABEL}, ${CASE_COUNT} cases / ${PACKAGE_COUNT} suites per build)"
status=$(bench_curl -o /tmp/bench-create-job-response.html -w '%{http_code}' \
    "${crumb_args[@]}" \
    -H 'Content-Type: application/xml' \
    --data-binary "${JOB_CONFIG}" \
    "${JENKINS_URL}/createItem?name=${JOB_NAME}")

if [[ "$status" != "200" ]]; then
    bench_log "Failed to create job (HTTP ${status}); response saved to /tmp/bench-create-job-response.html"
    exit 1
fi

bench_log "Created '${JOB_NAME}'. Trigger load with: ./scripts/benchmark/trigger-concurrent-builds.sh ${JOB_NAME} <build-count>"
