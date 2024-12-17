package ai.traceable.edge.decision.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class EdgeDecisionConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Config config = mock(Config.class);
    Channel mockChannel = mock(Channel.class);
    ConfigChangeEventGenerator mockEventGenerator = mock(ConfigChangeEventGenerator.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    new EdgeDecisionConfigServiceModule(config, mockChannel, mockEventGenerator))
                .getAllBindings());
  }
}
