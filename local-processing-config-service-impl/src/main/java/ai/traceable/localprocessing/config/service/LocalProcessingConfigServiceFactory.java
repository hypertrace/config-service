package ai.traceable.localprocessing.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class LocalProcessingConfigServiceFactory {
  public static BindableService build(
      ManagedChannel channel,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            new LocalProcessingConfigServiceModule(channel, config, configChangeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
