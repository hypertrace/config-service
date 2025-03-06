package ai.traceable.jwt.extraction.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JwtExtractionConfigServiceModuleTest {
  @Mock ConfigChangeEventGenerator mockChangeEventGenerator;
  @Mock Channel mockChannel;
  @Mock Config mockConfig;
  @Mock FeatureCachingClient mockFeatureCachingClient;

  @Test
  void testResolveBindings() {
    when(mockConfig.getConfig(anyString())).thenReturn(mock(Config.class));
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new JwtExtractionConfigServiceModule(
                        mockChannel,
                        mockChangeEventGenerator,
                        mockConfig,
                        mockFeatureCachingClient))
                .getAllBindings());
  }
}
