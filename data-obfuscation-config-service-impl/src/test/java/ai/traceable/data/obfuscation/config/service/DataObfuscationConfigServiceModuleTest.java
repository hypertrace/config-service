package ai.traceable.data.obfuscation.config.service;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DataObfuscationConfigServiceModuleTest {
  Channel mockChannel = Mockito.mock(Channel.class);
  ConfigChangeEventGenerator configChangeEventGenerator =
      Mockito.mock(ConfigChangeEventGenerator.class);

  @Test
  void configure() {
    Injector injector =
        Guice.createInjector(
            new DataObfuscationConfigServiceModule(mockChannel, configChangeEventGenerator));
    BindableService svc = injector.getInstance(BindableService.class);
    assertNotNull(svc);
  }
}
