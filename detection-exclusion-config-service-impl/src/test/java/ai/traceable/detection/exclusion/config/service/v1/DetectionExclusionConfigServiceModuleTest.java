package ai.traceable.detection.exclusion.config.service.v1;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class DetectionExclusionConfigServiceModuleTest {

  @Test
  void testResolveBindings() {
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new DetectionExclusionConfigServiceModule(
                        mock(Channel.class),
                        mock(ConfigChangeEventGenerator.class),
                        mock(FeatureCachingClient.class)))
                .getAllBindings());
  }
}
