package ai.traceable.external.userattribution.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

class ExternalUserAttributionConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    ManagedChannel mockChannel = mock(ManagedChannel.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(new ExternalUserAttributionConfigServiceModule(mockChannel))
                .getAllBindings());
  }
}
