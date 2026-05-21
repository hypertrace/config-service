package ai.traceable.genai.system.discovery.config.service.v1;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class GenAiSystemDiscoveryConfigServiceModuleTest {

  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    ConfigChangeEventGenerator mockConfigChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);
    FeatureCachingClient mockFeatureCachingClient = mock(FeatureCachingClient.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new GenAiSystemDiscoveryConfigServiceModule(
                        mockChannel, mockConfigChangeEventGenerator, mockFeatureCachingClient))
                .getAllBindings());
  }
}
