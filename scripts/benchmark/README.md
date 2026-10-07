# Benchmark scripts

Small, parameterized shell scripts for generating and observing concurrent test-result publish load
against a running Jenkins + JUnit SQL Storage plugin instance, so that connection pooling, locking,
and query performance can be exercised and watched live (e.g. alongside the Jaeger UI or
`docker compose exec db psql`). They complement [`examples/issue-532/Jenkinsfile`](../../examples/issue-532/Jenkinsfile)
(a single large build) by driving *many concurrent* builds.

None of these scripts hardcode a host/IP: every connection setting is an environment variable with a
`localhost` default, so the same scripts work against the local `docker compose`/`mvn hpi:run` dev
stack or a remote Jenkins/database by just exporting different values first.

Only ever point these at a disposable Jenkins instance/job -- they are designed to generate load, not
to be realistic jobs, and `watch-db-activity.sh` reports data from `pg_stat_activity`/
`performance_schema` that may include other sessions' queries on a shared database.

## Configuration

All scripts source [`lib.sh`](lib.sh), which reads these environment variables (all optional):

| Variable | Default | Purpose |
|---|---|---|
| `JENKINS_URL` | `http://localhost:8080` | Base URL of the Jenkins instance to drive |
| `JENKINS_USER` | *(none, anonymous)* | Username for Basic auth; set this together with `JENKINS_API_TOKEN` (an API token from the user's Jenkins account page) rather than relying on anonymous/session access. Recommended whenever the instance has authentication enabled, and required for a remote Jenkins with CSRF protection and security enabled. |
| `JENKINS_API_TOKEN` | *(none)* | API token paired with `JENKINS_USER`. When set, requests authenticate with Basic auth and skip fetching a CSRF crumb entirely -- Jenkins' crumb/CSRF check only applies to session (cookie) based requests, not to Basic-auth/API-token requests, so no crumb is needed or fetched in this mode. Without a token, scripts fall back to an anonymous session and fetch a crumb as needed. |
| `LOAD_JOB_NAME` | `bench-concurrent-publish` | Name of the job the scripts create/trigger/delete |
| `DB_EXEC_MODE` | `compose` | `compose` runs through the local `docker compose exec db ...` dev stack; `direct` connects straight to `DB_HOST`/`DB_PORT` (use this for a remote database) |
| `DB_ENGINE` | `postgres` | `postgres` or `mysql` |
| `DB_HOST` / `DB_PORT` | `localhost` / engine default | Only used when `DB_EXEC_MODE=direct` |
| `DB_USER` / `DB_NAME` | `postgres` / engine default | Database credentials/name |
| `DB_PASSWORD` | *(none)* | Passed through as `PGPASSWORD`/`MYSQL_PWD`; never printed or logged |

Example, pointing at a remote Jenkins and a remote standalone Postgres instance instead of the local
dev stack:

```bash
export JENKINS_URL=http://jenkins.example.internal:8080
export JENKINS_USER=admin
export JENKINS_API_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
export DB_EXEC_MODE=direct
export DB_HOST=db.example.internal
export DB_USER=jenkins
export DB_NAME=jenkins
export DB_PASSWORD=xxxxxxxx   # kept only in your shell's environment, never written to disk by these scripts
```

## Usage

```bash
# 1. Create a scripted-pipeline job that generates synthetic JUnit results and publishes them.
#    Requires at least one agent matching AGENT_LABEL (default: "agent") with Python available.
./scripts/benchmark/create-load-job.sh

# 2. In one terminal, watch database connection/lock activity while load runs.
./scripts/benchmark/watch-db-activity.sh 180 2   # watch for 180s, sampling every 2s

# 3. In another terminal, fire many concurrent builds and wait for them all to finish.
./scripts/benchmark/trigger-concurrent-builds.sh bench-concurrent-publish 100

# 4. Clean up the load-test job when done.
./scripts/benchmark/delete-load-job.sh
```

Tune `CASE_COUNT`/`PACKAGE_COUNT` (passed to `create-load-job.sh`) and `CONCURRENCY`/
`TIMEOUT_SECONDS` (passed to `trigger-concurrent-builds.sh`) to scale the load up or down; see the
comments at the top of each script for the full list of variables they accept.

## What to look for

- **`watch-db-activity.sh`** samples `pg_stat_activity` (or `performance_schema` on MySQL) for total
  vs. active connections and lock waits. A connection count that keeps climbing with build
  concurrency (rather than staying roughly flat) suggests a leaked/unbounded connection pool;
  sustained non-zero lock waits suggest write contention worth investigating with
  `EXPLAIN (ANALYZE, BUFFERS)` on the queries involved.
- **Jaeger** (`http://localhost:16686` on the local dev stack) shows per-request span timings for the
  publish/read paths exercised by the load, useful for spotting which specific query or code path
  dominates under concurrency.
- **`trigger-concurrent-builds.sh`**'s own timing output (total trigger time, queue-drain time,
  build-completion time) gives a coarse end-to-end throughput number to compare before/after a change.
