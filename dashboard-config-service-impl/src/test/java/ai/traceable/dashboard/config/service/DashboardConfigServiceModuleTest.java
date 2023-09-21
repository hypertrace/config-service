package ai.traceable.dashboard.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.inject.Guice;
import com.google.inject.Stage;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DashboardConfigServiceModuleTest {
  @Mock ConfigChangeEventGenerator mockChangeEventGenerator;
  @Mock Channel mockChannel;

  @Test
  void testResolveBindings() {
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    new DashboardConfigServiceModule(mockChannel, mockChangeEventGenerator))
                .getAllBindings());
  }
}
