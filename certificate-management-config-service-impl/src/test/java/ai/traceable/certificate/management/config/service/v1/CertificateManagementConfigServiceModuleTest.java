package ai.traceable.certificate.management.config.service.v1;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.google.inject.Stage;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class CertificateManagementConfigServiceModuleTest {

  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    ConfigChangeEventGenerator mockConfigChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    new CertificateManagementConfigServiceModule(
                        mockChannel, mockConfigChangeEventGenerator))
                .getAllBindings());
  }
}
