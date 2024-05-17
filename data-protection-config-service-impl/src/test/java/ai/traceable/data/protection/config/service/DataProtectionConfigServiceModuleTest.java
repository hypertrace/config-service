package ai.traceable.data.protection.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class DataProtectionConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new DataProtectionConfigServiceModule(
                        mock(Channel.class), mock(ConfigChangeEventGenerator.class)))
                .getAllBindings());
  }
}
