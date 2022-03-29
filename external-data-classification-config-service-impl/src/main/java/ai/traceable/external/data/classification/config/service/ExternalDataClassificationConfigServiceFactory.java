package ai.traceable.external.data.classification.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class ExternalDataClassificationConfigServiceFactory {
  public static BindableService build(
      ManagedChannel channel,
      ConfigChangeEventGenerator configChangeEventGenerator,
      Config config) {
    Injector injector =
        Guice.createInjector(
            new ExternalDataClassificationConfigServiceModule(
                channel, configChangeEventGenerator, config));
    return injector.getInstance(BindableService.class);
  }
}
