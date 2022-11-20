package ai.traceable.auth.detection.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AuthDetectionConfigServiceModuleTest {
  @Mock ConfigChangeEventGenerator mockChangeEventGenerator;
  @Mock Channel mockChannel;
  @Mock Config mockConfig;

  @Test
  void testResolveBindings() {

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new AuthDetectionConfigServiceModule(
                        mockChannel, mockChangeEventGenerator, mockConfig))
                .getAllBindings());
  }
}
