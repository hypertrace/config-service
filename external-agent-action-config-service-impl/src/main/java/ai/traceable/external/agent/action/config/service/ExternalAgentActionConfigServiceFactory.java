package ai.traceable.external.agent.action.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;

public class ExternalAgentActionConfigServiceFactory {
  private ExternalAgentActionConfigServiceFactory() {}

  public static BindableService build(Channel channel) {
    Injector injector = Guice.createInjector(new ExternalAgentActionConfigServiceModule(channel));
    return injector.getInstance(BindableService.class);
  }
}
