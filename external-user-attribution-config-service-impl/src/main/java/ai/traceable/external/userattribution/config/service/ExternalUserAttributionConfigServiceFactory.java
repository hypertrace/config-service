package ai.traceable.external.userattribution.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;

public class ExternalUserAttributionConfigServiceFactory {
  private ExternalUserAttributionConfigServiceFactory() {}

  public static BindableService build(Channel channel) {
    Injector injector =
        Guice.createInjector(new ExternalUserAttributionConfigServiceModule(channel));
    return injector.getInstance(BindableService.class);
  }
}
