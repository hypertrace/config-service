package org.hypertrace.span.processing.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

class SpanProcessingConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(new SpanProcessingConfigServiceModule(mockChannel))
                .getAllBindings());
  }
}
