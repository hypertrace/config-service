package ai.traceable.region.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;

public class RegionConfigServiceFactory {
  public static BindableService build(Config config) {
    Injector injector = Guice.createInjector(new RegionConfigServiceModule(config));
    return injector.getInstance(BindableService.class);
  }
}
