package ai.traceable.fraud.datamodel.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.documentstore.Datastore;

public class FraudDataModelConfigServiceFactory {
  public static BindableService build(
      ConfigChangeEventGenerator changeEventGenerator, Datastore datastore) {
    Injector injector =
        Guice.createInjector(
            new FraudDataModelConfigServiceModule(changeEventGenerator, datastore));
    return injector.getInstance(BindableService.class);
  }
}
