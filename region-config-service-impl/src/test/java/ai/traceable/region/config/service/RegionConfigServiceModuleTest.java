package ai.traceable.region.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import java.util.HashMap;
import java.util.Map;
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

  private Config mockConfig() {
    Config mockConfig = mock(Config.class);
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("neustar.countries.data.path", "/neustar/countries.csv");
    configMap.put("ipqs.countries.data.path", "/ipqs/countries.csv");
    when(mockConfig.getConfig("region.config.service"))
        .thenReturn(ConfigFactory.parseMap(configMap));
    return mockConfig;
  }
}
