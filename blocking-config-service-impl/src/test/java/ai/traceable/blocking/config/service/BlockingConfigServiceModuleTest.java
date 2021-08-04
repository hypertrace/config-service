package ai.traceable.blocking.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

class BlockingConfigServiceModuleTest {

  @Test
  void testResolveBindings() {
    ManagedChannel mockChannel = mock(ManagedChannel.class);

    assertDoesNotThrow(
        () -> Guice.createInjector(new BlockingConfigServiceModule(mockChannel)).getAllBindings());
  }
}
