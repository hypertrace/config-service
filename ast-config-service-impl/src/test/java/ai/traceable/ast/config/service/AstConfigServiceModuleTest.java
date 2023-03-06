package ai.traceable.ast.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.junit.jupiter.api.Test;

class AstConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    Config mockConfig = mock(Config.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(new AstConfigServiceModule(mockChannel, mockConfig))
                .getAllBindings());
  }
}
