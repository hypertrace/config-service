package ai.traceable.ratelimiting.config.service.v2;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceModule;
import com.google.inject.Guice;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

class RateLimitingConfigServiceFactoryTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    Config mockConfig = mock(Config.class);
    ActivityEventProducer mockActivityEventProducer = mock(ActivityEventProducer.class);
    GrpcChannelRegistry mockGrpcChannelRegistry = mock(GrpcChannelRegistry.class);
    FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);
    ConfigChangeEventGenerator mockConfigChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);

    when(mockConfig.getConfig("entity.service"))
        .thenReturn(
            ConfigFactory.parseString(
                "config {\n"
                    + "    host = localhost\n"
                    + "    port = 50061\n"
                    + "    timeout = 10s\n"
                    + "  }\n"
                    + "  attributeMap {\n"
                    + "    service.id = \"SERVICE.id\"\n"
                    + "    service.name = \"SERVICE.name\"\n"
                    + "    service.environment = \"SERVICE.environment\"\n"
                    + "  }"));
    when(mockConfig.getConfig("entity.fetcher.cache"))
        .thenReturn(
            ConfigFactory.parseString(
                "  service.mapping.cache = {\n"
                    + "    maxSize = 1000\n"
                    + "    refreshAfterWriteDuration = 10m\n"
                    + "    expireAfterWriteDuration = 1h\n"
                    + "  }\n"
                    + "  api.mapping.cache = {\n"
                    + "    maxSize = 1000\n"
                    + "    refreshAfterWriteDuration = 10m\n"
                    + "    expireAfterAccessDuration = 1h\n"
                    + "  }\n"));
    doReturn(mock(ManagedChannel.class))
        .when(mockGrpcChannelRegistry)
        .forPlaintextAddress("localhost", 50061);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    new RateLimitingConfigServiceModule(
                        mockChannel,
                        mockConfig,
                        mockActivityEventProducer,
                        mockConfigChangeEventGenerator,
                        mockGrpcChannelRegistry,
                        featureCachingClient))
                .getAllBindings());
  }
}
