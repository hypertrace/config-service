package ai.traceable.waf.provider.integration.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

class WafIntegrationConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    final Channel mockChannel = mock(Channel.class);
    final ConfigChangeEventGenerator configChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);
    final Config mockConfig = mock(Config.class);
    final GrpcChannelRegistry mockChannelRegistry = mock(GrpcChannelRegistry.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new WafIntegrationConfigServiceModule(
                        mockConfig, mockChannel, configChangeEventGenerator, mockChannelRegistry))
                .getAllBindings());
  }
}
