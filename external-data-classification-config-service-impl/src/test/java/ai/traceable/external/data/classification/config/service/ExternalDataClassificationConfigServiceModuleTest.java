package ai.traceable.external.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

public class ExternalDataClassificationConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    Config mockConfig = mock(Config.class);
    GrpcChannelRegistry channelRegistry = mock(GrpcChannelRegistry.class);
    FeatureCachingClient mockFeatureClient = mock(FeatureCachingClient.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new ExternalDataClassificationConfigServiceModule(
                        mockChannel, mockConfig, channelRegistry, mockFeatureClient))
                .getAllBindings());
  }
}
