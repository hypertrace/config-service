package ai.traceable.threatmanagement.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class ThreatManagementConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    Config config = mock(Config.class);
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new ThreatManagementConfigServiceModule(
                        mockChannel, config, configChangeEventGenerator))
                .getAllBindings());
  }
}
