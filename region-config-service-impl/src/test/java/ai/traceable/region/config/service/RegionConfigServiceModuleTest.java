package ai.traceable.region.config.service;

import static ai.traceable.region.config.service.RegionConfigServiceConfigTest.mockConfig;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class RegionConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Config mockConfig = mockConfig();
    Channel mockChannel = mock(Channel.class);
    ActivityEventProducer mockActivityEventProducer = mock(ActivityEventProducer.class);
    ConfigChangeEventGenerator mockConfigChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);
    FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new RegionConfigServiceModule(
                        mockChannel,
                        mockConfig,
                        mockActivityEventProducer,
                        mockConfigChangeEventGenerator,
                        featureCachingClient))
                .getAllBindings());
  }
}
