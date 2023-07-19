package ai.traceable.sessionidentification.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class SessionIdentificationConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new SessionIdentificationConfigServiceModule(
                        mockChannel,
                        configChangeEventGenerator,
                        featureCachingClient,
                        mock(Config.class)))
                .getAllBindings());
  }
}
