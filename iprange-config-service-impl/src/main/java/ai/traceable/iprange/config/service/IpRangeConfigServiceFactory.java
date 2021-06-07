package ai.traceable.iprange.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;

public class IpRangeConfigServiceFactory {
  private IpRangeConfigServiceFactory() {}

  public static BindableService build(ManagedChannel channel) {
    Injector injector = Guice.createInjector(new IpRangeConfigServiceModule(channel));
    return injector.getInstance(BindableService.class);
  }
}
