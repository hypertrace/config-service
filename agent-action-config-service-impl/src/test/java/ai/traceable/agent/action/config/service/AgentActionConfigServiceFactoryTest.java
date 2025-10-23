package ai.traceable.agent.action.config.service;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AgentActionConfigServiceFactoryTest {
  Channel mockChannel = Mockito.mock(Channel.class);
  ConfigChangeEventGenerator configChangeEventGenerator =
      Mockito.mock(ConfigChangeEventGenerator.class);

  @Test
  void build() {
    BindableService svc =
        AgentActionConfigServiceFactory.build(mockChannel, configChangeEventGenerator);
    assertNotNull(svc);
  }
}
