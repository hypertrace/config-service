package ai.traceable.malicioussources.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import java.util.Map;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class MaliciousSourcesConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);

    ConfigChangeEventGenerator mockConfigChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new MaliciousSourcesConfigServiceModule(
                        mockChannel,
                        mockConfigChangeEventGenerator,
                        ConfigFactory.parseMap(
                            Map.of(
                                "malicious.sources.config.service.changeLog1.migrationDisabled",
                                false))))
                .getAllBindings());
  }
}
