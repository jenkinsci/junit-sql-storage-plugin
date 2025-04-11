package io.jenkins.plugins.junit.storage.database;

import io.jenkins.plugins.casc.ConfigurationContext;
import io.jenkins.plugins.casc.ConfiguratorRegistry;
import io.jenkins.plugins.casc.misc.ConfiguredWithCode;
import io.jenkins.plugins.casc.misc.JenkinsConfiguredWithCodeRule;
import io.jenkins.plugins.casc.misc.junit.jupiter.WithJenkinsConfiguredWithCode;
import io.jenkins.plugins.casc.model.CNode;
import io.jenkins.plugins.junit.storage.JunitTestResultStorageConfiguration;
import org.junit.jupiter.api.Test;

import static io.jenkins.plugins.casc.misc.Util.getUnclassifiedRoot;
import static io.jenkins.plugins.casc.misc.Util.toStringFromYamlFile;
import static io.jenkins.plugins.casc.misc.Util.toYamlString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;

@WithJenkinsConfiguredWithCode
class DatabaseJcascTestResultStorageTest {

    @Test
    @ConfiguredWithCode("configuration-as-code.yml")
    void should_support_configuration_as_code(JenkinsConfiguredWithCodeRule j) {
        assertThat(JunitTestResultStorageConfiguration.get().getStorage(), isA(DatabaseTestResultStorage.class));
    }

    @Test
    @ConfiguredWithCode("configuration-as-code.yml")
    void should_support_configuration_export(JenkinsConfiguredWithCodeRule j) throws Exception {
        ConfiguratorRegistry registry = ConfiguratorRegistry.get();
        ConfigurationContext context = new ConfigurationContext(registry);
        CNode yourAttribute = getUnclassifiedRoot(context).get("junitTestResultStorage");

        String exported = toYamlString(yourAttribute);

        String expected = toStringFromYamlFile(this, "configuration-as-code-expected.yml");

        assertThat(exported, is(expected));
    }
}
