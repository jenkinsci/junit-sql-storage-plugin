FROM jenkins/jenkins:lts-jdk17

# install the plugin from update center to get all dependencies
RUN jenkins-plugin-cli --plugins junit-sql-storage configuration-as-code database-postgresql opentelemetry
# override with locally built version
COPY target/junit-sql-storage.hpi /usr/share/jenkins/ref/plugins/junit-sql-storage.jpi

# configuration as code config for configuring this plugin
COPY jenkins-config.yaml /usr/share/jenkins/ref/jenkins.yaml