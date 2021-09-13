package ai.traceable.threatmanagement.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

class ThreatManagementConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    Config config = mock(Config.class);
    ActivityEventProducer mockActivityEventProducer = mock(ActivityEventProducer.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new ThreatManagementConfigServiceModule(
                        mockChannel, config, mockActivityEventProducer))
                .getAllBindings());
  }
}
