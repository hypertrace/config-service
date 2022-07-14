package ai.traceable.external.userattribution.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import org.junit.jupiter.api.Test;

class ExternalUserAttributionConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    Config mockConfig = ConfigFactory.parseResources("parsingRules.conf");
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new ExternalUserAttributionConfigServiceModule(mockChannel, mockConfig))
                .getAllBindings());
  }
}
