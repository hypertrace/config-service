package ai.traceable.threatmanagement.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

class ThreatManagementConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    Config config = mock(Config.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(new ThreatManagementConfigServiceModule(mockChannel, config))
                .getAllBindings());
  }
}
