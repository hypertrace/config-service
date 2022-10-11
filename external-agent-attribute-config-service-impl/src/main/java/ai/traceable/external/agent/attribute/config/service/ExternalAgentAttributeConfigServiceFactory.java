package ai.traceable.external.agent.attribute.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;

public class ExternalAgentAttributeConfigServiceFactory {
  private ExternalAgentAttributeConfigServiceFactory() {}

  public static BindableService build(final Channel channel, final Config config) {
    Injector injector =
        Guice.createInjector(new ExternalAgentAttributeConfigServiceModule(channel, config));
    return injector.getInstance(BindableService.class);
  }
}
