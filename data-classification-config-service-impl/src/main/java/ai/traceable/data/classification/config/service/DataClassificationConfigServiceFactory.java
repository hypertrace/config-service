package ai.traceable.data.classification.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class DataClassificationConfigServiceFactory {
  public static BindableService build(
      ManagedChannel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            new DataClassificationConfigServiceModule(channel, configChangeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
