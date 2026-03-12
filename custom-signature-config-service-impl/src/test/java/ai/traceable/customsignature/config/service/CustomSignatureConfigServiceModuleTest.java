package ai.traceable.customsignature.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
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
    KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue>
        mockKafkaLiveEventListener =
            (KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue>)
                mock(KafkaLiveEventListener.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new CustomSignatureConfigServiceModule(
                        mockChannel,
                        mockConfig,
                        mockConfigChangeEventGenerator,
                        featureCachingClient,
                        mockGrpcChannelRegistry,
                        mockKafkaLiveEventListener))
                .getAllBindings());
  }
}
