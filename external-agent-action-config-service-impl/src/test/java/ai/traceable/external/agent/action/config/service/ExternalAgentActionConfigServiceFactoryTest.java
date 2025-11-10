package ai.traceable.external.agent.action.config.service;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.grpc.BindableService;
import io.grpc.Channel;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ExternalAgentActionConfigServiceFactoryTest {
  Channel mockChannel = Mockito.mock(Channel.class);

  @Test
  void build() {
    BindableService svc = ExternalAgentActionConfigServiceFactory.build(mockChannel);
    assertNotNull(svc);
  }
}
