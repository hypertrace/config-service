package ai.traceable.external.agent.attribute.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import io.grpc.Channel;
import org.junit.jupiter.api.Test;

class ExternalAgentAttributeConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    FeatureCachingClient mockFeatureClient = mock(FeatureCachingClient.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new ExternalAgentAttributeConfigServiceModule(mockChannel, mockFeatureClient))
                .getAllBindings());
  }
}
