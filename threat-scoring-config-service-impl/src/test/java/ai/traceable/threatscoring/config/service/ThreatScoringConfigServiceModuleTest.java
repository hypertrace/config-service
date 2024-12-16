package ai.traceable.threatscoring.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class ThreatScoringConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new ThreatScoringConfigServiceModule(mockChannel, configChangeEventGenerator))
                .getAllBindings());
  }
}
