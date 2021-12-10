package ai.traceable.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class DataClassificationConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new DataClassificationConfigServiceModule(
                        mockChannel, configChangeEventGenerator))
                .getAllBindings());
  }
}
