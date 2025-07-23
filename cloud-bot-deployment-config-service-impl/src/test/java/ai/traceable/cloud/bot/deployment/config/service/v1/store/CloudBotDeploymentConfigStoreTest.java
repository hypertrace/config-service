package ai.traceable.cloud.bot.deployment.config.service.v1.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.SiteConfig;
import com.google.protobuf.Value;
import java.util.Optional;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class CloudBotDeploymentConfigStoreTest {
  @Mock private ConfigServiceBlockingStub configServiceBlockingStub;
  @Mock private ConfigChangeEventGenerator configChangeEventGenerator;

  private CloudBotDeploymentConfigStore store;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    store =
        new CloudBotDeploymentConfigStore(configServiceBlockingStub, configChangeEventGenerator);
  }

  @Test
  void testBuildDataFromValue() {
    // Create a simple config for testing
    CloudBotDeploymentConfig originalConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId("test-id")
            .setSiteConfig(
                SiteConfig.newBuilder()
                    .setSiteName("Test Site")
                    .setSiteKey("test-key")
                    .addDomains("example.com"))
            .build();

    // Convert to Value
    Value value = store.buildValueFromData(originalConfig);

    // Convert back to CloudBotDeploymentConfig
    Optional<CloudBotDeploymentConfig> result = store.buildDataFromValue(value);

    assertTrue(result.isPresent());
    assertEquals("test-id", result.get().getId());
    assertEquals("Test Site", result.get().getSiteConfig().getSiteName());
    assertEquals("test-key", result.get().getSiteConfig().getSiteKey());
    assertEquals("example.com", result.get().getSiteConfig().getDomains(0));
  }
}
