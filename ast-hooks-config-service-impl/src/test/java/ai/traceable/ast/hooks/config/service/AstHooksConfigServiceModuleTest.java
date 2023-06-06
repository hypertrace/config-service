package ai.traceable.ast.hooks.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.inject.Guice;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AstHooksConfigServiceModuleTest {
  @Mock ConfigChangeEventGenerator mockChangeEventGenerator;
  @Mock Channel mockChannel;

  @Test
  void testResolveBindings() {
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new AstHooksConfigServiceModule(mockChannel, mockChangeEventGenerator))
                .getAllBindings());
  }
}
