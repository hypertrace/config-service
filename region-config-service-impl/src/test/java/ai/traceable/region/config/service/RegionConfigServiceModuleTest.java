package ai.traceable.region.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class RegionConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Config mockConfig = mock(Config.class);
    Channel mockChannel = mock(Channel.class);
    ActivityEventProducer mockActivityEventProducer = mock(ActivityEventProducer.class);
    ConfigChangeEventGenerator mockConfigChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new RegionConfigServiceModule(
                        mockChannel,
                        mockConfig,
                        mockActivityEventProducer,
                        mockConfigChangeEventGenerator))
                .getAllBindings());
  }
}
