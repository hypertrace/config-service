package ai.traceable.ratelimiting.config.service.v2.rules;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceModule;
import com.google.inject.Guice;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class RateLimitingConfigServiceFactoryTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    Config mockConfig = mock(Config.class);
    ActivityEventProducer mockActivityEventProducer = mock(ActivityEventProducer.class);
    ConfigChangeEventGenerator mockConfigChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    new RateLimitingConfigServiceModule(
                        mockChannel,
                        mockConfig,
                        mockActivityEventProducer,
                        mockConfigChangeEventGenerator))
                .getAllBindings());
  }
}
