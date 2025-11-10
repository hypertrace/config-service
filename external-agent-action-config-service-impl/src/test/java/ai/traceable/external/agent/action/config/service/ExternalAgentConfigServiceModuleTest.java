package ai.traceable.external.agent.action.config.service;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ExternalAgentConfigServiceModuleTest {
  Channel mockChannel = Mockito.mock(Channel.class);

  @Test
  void configure() {
    Injector injector =
        Guice.createInjector(new ExternalAgentActionConfigServiceModule(mockChannel));
    BindableService svc = injector.getInstance(BindableService.class);
    assertNotNull(svc);
  }
}
