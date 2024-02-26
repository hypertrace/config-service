package ai.traceable.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import java.util.Collections;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataClassificationConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    Config mockConfig = mock(Config.class);
    when(mockConfig.getConfig(anyString())).thenReturn(mockConfig);
    when(mockConfig.getObjectList(anyString())).thenReturn(Collections.emptyList());
    FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new DataClassificationConfigServiceModule(
                        mockChannel, configChangeEventGenerator, mockConfig, featureCachingClient))
                .getAllBindings());
  }
}
