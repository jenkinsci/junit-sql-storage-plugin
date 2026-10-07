# JUnit SQL Storage Plugin

[![Build Status](https://ci.jenkins.io/job/Plugins/job/junit-sql-storage-plugin/job/master/badge/icon)](https://ci.jenkins.io/job/Plugins/job/junit-sql-storage-plugin/job/master/)
[![Contributors](https://img.shields.io/github/contributors/jenkinsci/junit-sql-storage-plugin.svg)](https://github.com/jenkinsci/junit-sql-storage-plugin/graphs/contributors)
[![Jenkins Plugin](https://img.shields.io/jenkins/plugin/v/junit-sql-storage.svg)](https://plugins.jenkins.io/junit-sql-storage)
[![GitHub release](https://img.shields.io/github/release/jenkinsci/junit-sql-storage-plugin.svg?label=changelog)](https://github.com/jenkinsci/junit-sql-storage-plugin/releases/latest)
[![Jenkins Plugin Installs](https://img.shields.io/jenkins/plugin/i/junit-sql-storage.svg?color=blue)](https://plugins.jenkins.io/junit-sql-storage)

## Introduction

Implements the pluggable storage API for the [JUnit plugin](https://plugins.jenkins.io/junit/).

In common CI/CD use-cases a lot of the space is consumed by test reports. 
This data is stored within JENKINS_HOME, and the current storage format requires huge overheads when retrieving statistics and, especially trends. 
In order to display trends, each report has to be loaded and then processed in-memory.

The main purpose of externalising Test Results is to optimize Jenkins performance by querying the desired data from external storage.

This plugin adds a SQL extension, we currently support PostgreSQL and MySQL, others can be added, create an issue or send a pull request.

Tables will be automatically created.

## Getting started

To install the plugin login to Jenkins → Manage Jenkins → Manage Plugins → Available → Search for 'JUnit SQL Storage' → Install.

Use the following steps if you want to build and install the plugin from source.

### Building

To build the plugin use the `package` goal
```
$ mvn clean package -P quick-build
```
to run the tests use the `test` goal
```
$ mvn clean test
```

### Deploying Jenkins and the plugin

To try out your changes you can deploy Jenkins using the `deploy.sh` file provided in the repository.
```
$ ./deploy.sh
```
This will compile the junit-sql-storage plugin (.hpi file), build a docker image of Jenkins with the compiled 
junit-sql-storage plugin installed, then deploy the docker swarm of jaeger, postgresql, and Jenkins. 

### UI

You can also use the Jenkins UI to install the plugin and configure it.

### Installing Compiled Plugin

Once you've compiled the plugin (see above) you can install it from the Jenkins UI. Go to 'Manage Jenkins' → 'Plugins' 
→ 'Advanced' → 'Deploy' → 'Choose File' → 'Deploy'

<img alt="Install hpi plugin file" src="images/install-junit-sql-storage-plugin.png" width="800">

Next, install your database vendor specific plugin, you can use the Jenkins plugin site to search for it:

https://plugins.jenkins.io/ui/search/?labels=database

e.g. you could install the [PostgreSQL Database](https://plugins.jenkins.io/database-postgresql/) plugin or the 
[MySQL Database](https://plugins.jenkins.io/database-mysql/) plugin.

Manage Jenkins → Configure System → Junit

In the dropdown select 'SQL Database'

![JUnit SQL plugin configuration](images/junit-sql-config-screen.png)

Manage Jenkins → Configure System → Global Database

Select the database implementation you want to use and click 'Test Connection' to verify Jenkins can connect

![JUnit SQL plugin database configuration](images/junit-sql-database-config.png)

> **Note:** use `db` as the 'Host Name' if running Jenkins from inside a docker container as part of the 
> docker-compose.yaml deployment

Click 'Save'

### Configuration as code

You can also configure the plugin using the [Configuration as Code](https://plugins.jenkins.io/configuration-as-code/) plugin.

```yaml
unclassified:
  globalDatabaseConfiguration:
    database:
      postgreSQL:
        database: "jenkins"
        hostname: "${DB_HOST_NAME}"
        password: "${DB_PASSWORD}"
        username: "${DB_USERNAME}"
        validationQuery: "SELECT 1"
  junitTestResultStorage:
    storage: "database"
```

Here's an example of how to use it

```
java -jar java-cli.jar -s http://<host.domain.name>:8080 -auth admin:<api_token> apply-configuration < junit-sql-storage-plugin-config.yml
```

### Accessing Jaeger

You can access the Jaeger UI by going to `http://localhost:16686`, here you can view the performance of the Jenkins 
server and your plugin changes.

### Accessing the database

You can also query the postgres database by connecting to the `db` container.

```
$ docker compose exec db psql -U postgres
psql (16.3 (Debian 16.3-1.pgdg120+1))
Type "help" for help.

postgres=# SELECT * FROM caseResults LIMIT 2;
    job     | build |                           suite                            |                  package                  |                         classname                          |         testname         | errordetails | skipped | duration | stdout | stderr | st
acktrace |         timestamp
------------+-------+------------------------------------------------------------+-------------------------------------------+------------------------------------------------------------+--------------------------+--------------+---------+----------+--------+--------+---
---------+----------------------------
 xxxx-xxxxx |     1 | xxx.xxxx.xxx.xxxx.xxxxxx.xxxxxxxxxxxxxxxxxxxxxxxx          | xxx.xxxx.xxx.xxxx.xxxxxx                  | xxx.xxxx.xxx.xxxx.xxxxxx.xxxxxxxxxxxxxxxxxxxxxxxx          | xxxxxxxxxxxxxxxxxxxxxxxx |              |         |    0.331 |        |        |
         | 2024-06-13 18:18:26.897532
 xxxx-xxxxx |     1 | xxx.xxxx.xxx.xxxxxxxxx.xxxxxxx.xxxxxxxxxx.xxxxxxxxxxxxxxxx | xxx.xxxx.xxx.xxxxxxxxx.xxxxxxx.xxxxxxxxxx | xxx.xxxx.xxx.xxxxxxxxx.xxxxxxx.xxxxxxxxxx.xxxxxxxxxxxxxxxx | xxxxxxxxxxxxxxxxxx       |              |         |    0.292 |        |        |
         | 2024-06-13 18:18:26.897532
(2 rows)
```

## Reproducing issues

### Performance

[examples/issue-532/Jenkinsfile](examples/issue-532/Jenkinsfile) is a self-contained Pipeline that can create a large number of test results to help reproduce performance issues.

If working with a larger number of test results you will want to increase the memory from the default.
If you're using the dev server, e.g. `-Dmaven.hpi.run.jvmArgs=-Xms512M -Xmx3G -XX:+HeapDumpOnOutOfMemoryError`

### Scaling to large test result history

This plugin writes each build's test cases in batches (JDBC `addBatch`/`executeBatch`), but by default both
the MySQL and PostgreSQL JDBC drivers still send each statement in a batch as its own round trip to the
server unless batch rewriting is explicitly enabled on the connection. For installs with large numbers of
test cases per build, enabling the driver's batch-rewriting option turns each flushed batch into a single
multi-row `INSERT`, significantly reducing publish time and server-side overhead. Configure this as part of
the JDBC URL/connection properties in the `database`/`database-mysql`/`database-postgresql` plugin's
configuration (Manage Jenkins → Configure System → Global Database):

* MySQL: append `rewriteBatchedStatements=true` to the JDBC URL.
* PostgreSQL: append `reWriteBatchedInserts=true` to the JDBC URL (enabled by default since pgjdbc 42.x
  in some distributions, but safe to set explicitly).

If you have a job with a very large amount of test history (many thousands of builds, or very large
per-build test case counts), be aware that:

* Trend/history/duration pages read from a small persisted per-build summary table
  (`caseResultsSummary`), not by aggregating `caseResults` on every request, so these stay fast regardless
  of total history size.
* "Failed since" lookups (shown next to failing tests) are served by a dedicated index on
  `(job, classname, testname, build)`, so they stay fast even when a job's full history is tens of millions
  of rows.
* The `caseResults` table is clustered by `(job, build, id)` on MySQL (a primary key, so a single build's rows are stored contiguously on disk rather than scattered
  in insertion order — this keeps single-build reads (history pages, build summaries, suite lookups) fast
  even on large tables.
  PostgreSQL also gains the primary key (for row identity/future maintenance tooling), but remains a heap
  table — it does not get the same automatic physical-clustering read speedup MySQL does.

## Contributing

Refer to our [contribution guidelines](https://github.com/jenkinsci/.github/blob/master/CONTRIBUTING.md)

## LICENSE

Licensed under MIT, see [LICENSE](LICENSE.md)

