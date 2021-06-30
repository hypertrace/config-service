package ai.traceable.localprocessing.config.service.ruleservice;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

class LocalProcessingRulesServiceModuleTest {
  @Test
  void testResolveBindings() {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    Config mockConfig = mock(Config.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(new LocalProcessingRulesServiceModule(mockChannel, mockConfig))
                .getAllBindings());
  }
}
