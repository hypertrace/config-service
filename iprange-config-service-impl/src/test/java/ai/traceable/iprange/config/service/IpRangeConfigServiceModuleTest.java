package ai.traceable.iprange.config.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

class IpRangeConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    ManagedChannel mockChannel = mock(ManagedChannel.class);

    Config mockConfig = mock(Config.class);
    ActivityEventProducer mockActivityEventProducer = mock(ActivityEventProducer.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new IpRangeConfigServiceModule(
                        mockChannel, mockConfig, mockActivityEventProducer))
                .getAllBindings());
  }
}
