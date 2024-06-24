package ai.traceable.fraud.datamodel.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class FraudDataModelConfigServiceFactory {
  public static BindableService build(
      Config config, ConfigChangeEventGenerator changeEventGenerator) {
    Injector injector =
        Guice.createInjector(new FraudDataModelConfigServiceModule(config, changeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
