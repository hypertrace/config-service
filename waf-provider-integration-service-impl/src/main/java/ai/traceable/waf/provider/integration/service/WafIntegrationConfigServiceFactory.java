package ai.traceable.waf.provider.integration.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class WafIntegrationConfigServiceFactory {
  private WafIntegrationConfigServiceFactory() {}

  public static BindableService build(
      ManagedChannel channel,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            new WafIntegrationConfigServiceModule(config, channel, configChangeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
