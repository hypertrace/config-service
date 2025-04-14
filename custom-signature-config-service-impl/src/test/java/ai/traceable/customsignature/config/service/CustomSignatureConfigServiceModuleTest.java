package ai.traceable.customsignature.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

class CustomSignatureConfigServiceModuleTest {
  @Test
  public void testResolveBindings() {
    Config mockConfig = mock(Config.class);
    Channel mockChannel = mock(Channel.class);
    ConfigChangeEventGenerator mockConfigChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);
    FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);
    GrpcChannelRegistry mockGrpcChannelRegistry = mock(GrpcChannelRegistry.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new CustomSignatureConfigServiceModule(
                        mockChannel,
                        mockConfig,
                        mockConfigChangeEventGenerator,
                        featureCachingClient,
                        mockGrpcChannelRegistry))
                .getAllBindings());
  }
}
