package ai.traceable.api.gateway.config.service;

import static com.google.inject.Stage.PRODUCTION;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.inject.Guice;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ApiGatewayConfigServiceBindingsTest {

  @Mock private Channel mockChannel;
  @Mock private ConfigChangeEventGenerator mockConfigChangeEventGenerator;

  @Test
  void testGetBindings() {
    final BindableService bindableService =
        Guice.createInjector(
                new ApiGatewayConfigServiceModule(mockChannel, mockConfigChangeEventGenerator))
            .getInstance(BindableService.class);
    assertTrue(bindableService instanceof ApiGatewayConfigServiceImpl);
  }

  @Test
  void testGetAllBindings() {
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    PRODUCTION,
                    new ApiGatewayConfigServiceModule(mockChannel, mockConfigChangeEventGenerator))
                .getAllBindings());
  }
}
