package ai.traceable.anomaly.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.ManagedChannel;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AnomalyConfigServiceModuleTest {
  @Test
  public void testResolveBindings() {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    Config config =
        ConfigFactory.parseMap(
            Map.of(
                "anomaly.config.service.global.disbled",
                true,
                "anomaly.config.service.internal",
                false));

    assertDoesNotThrow(
        () ->
            Guice.createInjector(new AnomalyConfigServiceModule(mockChannel, config))
                .getAllBindings());
  }
}
