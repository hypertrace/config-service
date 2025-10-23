package ai.traceable.agent.action.config.service;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AgentActionConfigServiceModuleTest {

  Channel mockChannel = Mockito.mock(Channel.class);
  ConfigChangeEventGenerator configChangeEventGenerator =
      Mockito.mock(ConfigChangeEventGenerator.class);

  @Test
  void configure() {
    Injector injector =
        Guice.createInjector(
            new AgentActionConfigServiceModule(mockChannel, configChangeEventGenerator));
    BindableService svc = injector.getInstance(BindableService.class);
    assertNotNull(svc);
  }
}
