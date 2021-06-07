package ai.traceable.iprange.config.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

class IpRangeConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    ManagedChannel mockChannel = mock(ManagedChannel.class);

    assertDoesNotThrow(
        () -> Guice.createInjector(new IpRangeConfigServiceModule(mockChannel)).getAllBindings());
  }
}
