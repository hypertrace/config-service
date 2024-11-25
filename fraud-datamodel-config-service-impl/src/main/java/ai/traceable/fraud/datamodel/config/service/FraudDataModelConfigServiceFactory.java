package ai.traceable.fraud.datamodel.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class FraudDataModelConfigServiceFactory {
  public static BindableService build(
      ConfigChangeEventGenerator changeEventGenerator,
      Datastore datastore,
      Config config,
      GrpcChannelRegistry grpcChannelRegistry) {
    Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION,
            new FraudDataModelConfigServiceModule(
                changeEventGenerator, datastore, config, grpcChannelRegistry));
    return injector.getInstance(BindableService.class);
  }
}
