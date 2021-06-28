package ai.traceable.customsignature.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

class CustomSignatureConfigServiceModuleTest {
  @Test
  public void testResolveBindings() {
    Config mockConfig = mock(Config.class);
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ActivityEventProducer mockActivityEventProducer = mock(ActivityEventProducer.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new CustomSignatureConfigServiceModule(
                        mockChannel, mockConfig, mockActivityEventProducer))
                .getAllBindings());
  }
}
