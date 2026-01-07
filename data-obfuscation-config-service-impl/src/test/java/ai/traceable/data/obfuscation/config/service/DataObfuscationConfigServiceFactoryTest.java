package ai.traceable.data.obfuscation.config.service;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DataObfuscationConfigServiceFactoryTest {
  Channel mockChannel = Mockito.mock(Channel.class);
  ConfigChangeEventGenerator configChangeEventGenerator =
      Mockito.mock(ConfigChangeEventGenerator.class);

  @Test
  void build() {
    BindableService svc =
        DataObfuscationConfigServiceFactory.build(mockChannel, configChangeEventGenerator);
    assertNotNull(svc);
  }
}
