# junit-sql-storage-plugin

Jenkins plugin that implements the pluggable storage API of the [JUnit plugin](https://plugins.jenkins.io/junit/),
writing test results to a SQL database (PostgreSQL or MySQL) instead of to `JENKINS_HOME`.

## Build, test, and lint

This is a standard Jenkins plugin, built with Maven (parent POM `org.jenkins-ci.plugins:plugin`).

```
mvn clean package -P quick-build   # build the .hpi, skipping tests/checks for speed
mvn clean test                     # run the full test suite
```

Run a single test class or method (Surefire):
```
mvn test -Dtest=DatabaseTestResultStorageTest
mvn test -Dtest=DatabaseTestResultStorageTest#smokes
```

Tests use `@WithJenkins` (JUnit 5 + `JenkinsRule`) and spin up a real `postgres:16-alpine` container via
Testcontainers (`PostgreSQLContainer`), so Docker must be available to run most tests in
`DatabaseTestResultStorageTest`. There is no dedicated lint step beyond what `mvn` runs as part of the
parent POM (checkstyle/spotbugs come from the Jenkins plugin parent); `ban-junit4-imports.skip` is
explicitly `false`, so don't introduce JUnit 4 imports (`org.junit.*`) — use JUnit 5 (`org.junit.jupiter.*`).

### Manually trying out the plugin

`./deploy.sh` builds the `.hpi` then runs `docker compose up` to start Jenkins + PostgreSQL + Jaeger
(`docker-compose.yaml`). Jenkins is on `:8080`, Jaeger UI on `:16686`, Postgres on `:5432` (use host `db`
when configuring the plugin from inside the Jenkins container). Query results directly with
`docker compose exec db psql -U postgres`.

## Architecture

- **`DatabaseTestResultStorage`** (`@Extension`) is the plugin's entry point: it implements
  `JunitTestResultStorage`/`JunitTestResultStorageDescriptor` from the junit-plugin's pluggable storage
  SPI and is selected via the JUnit plugin's global "SQL Database" storage option. It owns:
  - `RemotePublisher`, a `SlaveToMasterCallable` that batches `CaseResult`s (up to
    `MAX_DB_BATCH_SIZE` = 2000 rows) and JDBC-batches them into the `caseResults` table from the agent
    side, since test results are produced on build agents but the DB is only reachable from controller.
  - `TestResultStorage` (implements `TestResultImpl`), the controller-side read/query path used to
    render test result pages, trends, and history (`SuiteResult`, `PackageResult`, `CaseResult`,
    `TestDurationResultSummary`, `TrendTestResultSummary`, etc.) by issuing SQL against `caseResults`.
  - A static Caffeine `resultsCache` keyed by job+build, bounded by *weight* (total cached `CaseResult`
    count, not entry count) rather than entry count — see the Javadoc on `MAX_CACHED_CASE_RESULTS` for
    why. Entries must be explicitly invalidated via `invalidate(job, build)` after writes/deletes; don't
    rely solely on the time-based expiry.
  - OpenTelemetry spans (`GlobalOpenTelemetry`/`Tracer`) wrap the expensive DB calls for tracing in
    Jaeger (see docker-compose setup above).
- **`DatabaseSchemaLoader`** runs at Jenkins startup (`@Initializer(after = SYSTEM_CONFIG_ADAPTED)`) and
  applies Flyway migrations from `src/main/resources/db/migration/{postgres,mysql}` against whatever
  `Database` is configured via the `database` plugin's `GlobalDatabaseConfiguration`. It picks the
  migration directory by sniffing the configured JDBC driver class name for `"mysql"`. New schema
  changes go in a new timestamped `V<yyyy_MM_dd_HHmm>__description.sql` file in both migration dirs as
  needed — existing migration files must never be edited once released.
- **`TestResultCleanupListener`** hooks `RunListener`/`ItemListener` to delete rows from `caseResults`
  when a build or job is deleted (unless `skipCleanupRunsOnDeletion` is set), and a separate
  `RunListener.onFinalized` invalidates the results cache as a safety net in case the agent-side
  publisher's own invalidation was missed (e.g. an agent that crashed mid-publish).
- The `caseResults` table (see `V2020_09_21_2052__initial-schema.sql` and later migrations for the
  evolved schema/length limits) is the single denormalized table backing everything; row-length caps
  (`MAX_SUITE_LENGTH`, `MAX_TEST_NAME_LENGTH`, `MAX_STDOUT_LENGTH`, etc.) are enforced in
  `DatabaseTestResultStorage` before insert and must stay in sync with the column widths in the SQL
  migrations.

## Conventions

- New tests should use JUnit 5 (`org.junit.jupiter.api.*`) with `@WithJenkins`, not the legacy JUnit 4
  `@Rule public JenkinsRule`.
- When adding DB columns/behavior, update migrations for **both** `postgres` and `mysql` schema
  directories, and add/extend assertions in `DatabaseTestResultStorageTest`, which is the main
  integration test exercising a real Postgres container end-to-end (pipeline build → `junit` step →
  read back via `TestResultStorage`).
- Configuration-as-code support (`DatabaseJcascTestResultStorageTest`) is validated against
  `src/test/resources/.../configuration-as-code.yml` and `configuration-as-code-expected.yml` — keep
  these fixtures in sync with any new `@DataBoundSetter` fields on `DatabaseTestResultStorage`.
