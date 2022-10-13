package ai.traceable.external.agent.attribute.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;

public class ExternalAgentAttributeConfigServiceFactory {
  private ExternalAgentAttributeConfigServiceFactory() {}

  public static BindableService build(Channel channel) {
    Injector injector =
        Guice.createInjector(new ExternalAgentAttributeConfigServiceModule(channel));
    return injector.getInstance(BindableService.class);
  }
}
