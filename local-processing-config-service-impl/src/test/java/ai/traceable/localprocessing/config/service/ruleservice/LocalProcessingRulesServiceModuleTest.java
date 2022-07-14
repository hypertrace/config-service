package ai.traceable.localprocessing.config.service.ruleservice;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class LocalProcessingRulesServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    Config mockConfig = mock(Config.class);
    ConfigChangeEventGenerator mockConfigChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new LocalProcessingRulesServiceModule(
                        mockChannel, mockConfig, mockConfigChangeEventGenerator))
                .getAllBindings());
  }
}
