package ai.traceable.threatmanagement.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;

public class ThreatManagementConfigServiceFactory {
  public static BindableService build(ManagedChannel channel, Config config) {
    Injector injector =
        Guice.createInjector(new ThreatManagementConfigServiceModule(channel, config));
    return injector.getInstance(BindableService.class);
  }
}
