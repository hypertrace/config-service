package ai.traceable.risk.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

class RiskConfigServiceModuleTest {
  @Test
  public void testResolveBindings() {
    Config mockConfig = mock(Config.class);
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(new RiskConfigServiceModule(mockChannel, mockConfig))
                .getAllBindings());
  }
}
