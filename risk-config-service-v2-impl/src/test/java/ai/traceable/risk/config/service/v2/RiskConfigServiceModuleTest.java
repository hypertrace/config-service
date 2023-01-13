package ai.traceable.risk.config.service.v2;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

public class RiskConfigServiceModuleTest {
  @Test
  public void testResolveBindings() {
    Config mockConfig = mock(Config.class);
    Channel mockChannel = mock(Channel.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new RiskConfigServiceModule(
                        mockChannel, mockConfig, mock(ConfigChangeEventGenerator.class)))
                .getAllBindings());
  }
}
