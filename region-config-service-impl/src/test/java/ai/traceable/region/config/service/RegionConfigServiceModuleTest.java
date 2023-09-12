package ai.traceable.region.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class RegionConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Config mockConfig =
        ConfigFactory.parseString(
            "region.config.service {\n"
                + "neustar.countries.data {\n"
                + "    mode = RESOURCE_FILE\n"
                + "    resource.file = neustar/countries.csv\n"
                + "  }\n"
                + "ipqs.countries.data {\n"
                + "    mode = VERSIONS_DIR\n"
                + "    versions.dir = /var/ipqs/country_csv\n"
                + "    versions.file.name = countries.csv\n"
                + "    versions.refresh.duration = 24h\n"
                + "}\n"
                + "}");
    Channel mockChannel = mock(Channel.class);
    ActivityEventProducer mockActivityEventProducer = mock(ActivityEventProducer.class);
    ConfigChangeEventGenerator mockConfigChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);
    FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    new RegionConfigServiceModule(
                        mockChannel,
                        mockConfig,
                        mockActivityEventProducer,
                        mockConfigChangeEventGenerator,
                        featureCachingClient))
                .getAllBindings());
  }
}
