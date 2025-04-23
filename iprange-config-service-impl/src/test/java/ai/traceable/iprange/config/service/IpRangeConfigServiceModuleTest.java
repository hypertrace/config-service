package ai.traceable.iprange.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import com.google.inject.Guice;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import java.util.Map;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class IpRangeConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);

    ActivityEventProducer mockActivityEventProducer = mock(ActivityEventProducer.class);
    ConfigChangeEventGenerator mockConfigChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new IpRangeConfigServiceModule(
                        mockChannel,
                        ConfigFactory.parseMap(
                            Map.of("iprange.config.service.changeLog1.migrationDisabled", false)),
                        mockActivityEventProducer,
                        mockConfigChangeEventGenerator))
                .getAllBindings());
  }
}
