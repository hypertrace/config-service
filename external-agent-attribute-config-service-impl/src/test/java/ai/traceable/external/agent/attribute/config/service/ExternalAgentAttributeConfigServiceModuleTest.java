package ai.traceable.external.agent.attribute.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import io.grpc.Channel;
import org.junit.jupiter.api.Test;

class ExternalAgentAttributeConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(new ExternalAgentAttributeConfigServiceModule(mockChannel))
                .getAllBindings());
  }
}
