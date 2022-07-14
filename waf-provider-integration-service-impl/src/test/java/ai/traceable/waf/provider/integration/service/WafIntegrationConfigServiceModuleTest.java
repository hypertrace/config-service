package ai.traceable.waf.provider.integration.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class WafIntegrationConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    Config mockConfig = mock(Config.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new WafIntegrationConfigServiceModule(
                        mockConfig, mockChannel, configChangeEventGenerator))
                .getAllBindings());
  }
}
