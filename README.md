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

Install your database vendor specific plugin, you can use the Jenkins plugin site to search for it:

https://plugins.jenkins.io/ui/search/?labels=database

e.g. you could install the [PostgreSQL Database](https://plugins.jenkins.io/database-postgresql/) plugin.

### UI

Manage Jenkins → Configure System → Junit

In the dropdown select 'SQL Database'

![JUnit SQL plugin configuration](images/junit-sql-config-screen.png)

Manage Jenkins → Configure System → Global Database

Select the database implementation you want to use and click 'Test Connection' to verify Jenkins can connect

![JUnit SQL plugin database configuration](images/junit-sql-database-config.png)

Click 'Save'

### Configuration as code

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

## Contributing

Refer to our [contribution guidelines](https://github.com/jenkinsci/.github/blob/master/CONTRIBUTING.md)

## LICENSE

Licensed under MIT, see [LICENSE](LICENSE.md)

