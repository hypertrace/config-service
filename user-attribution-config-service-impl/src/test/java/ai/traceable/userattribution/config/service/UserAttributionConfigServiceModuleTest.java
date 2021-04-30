package ai.traceable.userattribution.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

class UserAttributionConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    ManagedChannel mockChannel = mock(ManagedChannel.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(new UserAttributionConfigServiceModule(mockChannel))
                .getAllBindings());
  }
}
