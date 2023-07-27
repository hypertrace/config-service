package ai.traceable.splunk.integration.config.service.validation;

import static ai.traceable.splunk.integration.config.service.SplunkIntegrationConfigServiceImpl.SPLUNK_INTEGRATION_CONFIG_SERVICE_CONFIG;
import static ai.traceable.splunk.integration.config.service.validation.SplunkIntegrationConfigRequestValidator.SPLUNK_HTTP_SUPPORT_ENABLED;

import ai.traceable.splunk.integration.config.service.store.SplunkIntegrationConfigStore;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.File;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SplunkIntegrationConfigRequestValidatorTest {

  @Test
  public void testValidateWebhookHttpsSupport() {
    SplunkIntegrationConfigRequestValidator splunkIntegrationConfigRequestValidator =
        new SplunkIntegrationConfigRequestValidator(
            Mockito.mock(SplunkIntegrationConfigStore.class));
    File configFile = new File(ClassLoader.getSystemResource("application.conf").getPath());
    Config config = ConfigFactory.parseFile(configFile);
    Config splunkIntegrationChannelConfig =
        config.getConfig(SPLUNK_INTEGRATION_CONFIG_SERVICE_CONFIG);
    String splunkConfigUrlHttp = "http://localhost:9000/test";
    // As http support disabled RuntimeException should be thrown.
    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          splunkIntegrationConfigRequestValidator.validateEventCollectorURL(
              splunkConfigUrlHttp, splunkIntegrationChannelConfig);
        },
        "RuntimeException was expected");

    // In valid URl not accepted
    String invalidUrl = "localhost";
    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          splunkIntegrationConfigRequestValidator.validateEventCollectorURL(
              invalidUrl, splunkIntegrationChannelConfig);
        },
        "RuntimeException was expected");

    // Valid webhook config with https url.
    String validHttpsURL = "https://localhost:9000/test";
    splunkIntegrationConfigRequestValidator.validateEventCollectorURL(
        validHttpsURL, splunkIntegrationChannelConfig);

    // Update config with http support enabled and verify no exceptions for http url
    Config updatedSplunkIntegrationConfig =
        config.withValue(SPLUNK_HTTP_SUPPORT_ENABLED, ConfigValueFactory.fromAnyRef("true"));

    String httpUrl = "http://localhost:9000/test";
    splunkIntegrationConfigRequestValidator.validateEventCollectorURL(
        httpUrl, updatedSplunkIntegrationConfig);
  }
}
