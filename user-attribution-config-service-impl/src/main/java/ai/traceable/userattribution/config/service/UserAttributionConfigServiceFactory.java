package ai.traceable.userattribution.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;

public class UserAttributionConfigServiceFactory {
  public static BindableService build(ManagedChannel channel) {
    Injector injector = Guice.createInjector(new UserAttributionConfigServiceModule(channel));
    return injector.getInstance(BindableService.class);
  }
}
