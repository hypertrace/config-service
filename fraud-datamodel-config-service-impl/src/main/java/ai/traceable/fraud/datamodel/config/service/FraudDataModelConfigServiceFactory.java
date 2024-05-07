package ai.traceable.fraud.datamodel.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;

public class FraudDataModelConfigServiceFactory {
  public static BindableService build(Config config) {
    Injector injector = Guice.createInjector(new FraudDataModelConfigServiceModule(config));
    return injector.getInstance(BindableService.class);
  }
}
