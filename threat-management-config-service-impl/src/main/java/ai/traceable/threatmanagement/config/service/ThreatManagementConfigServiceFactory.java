package ai.traceable.threatmanagement.config.service;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class ThreatManagementConfigServiceFactory {
  public static BindableService build(
      ManagedChannel channel,
      Config config,
      ActivityEventProducer activityEventProducer,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            new ThreatManagementConfigServiceModule(
                channel, config, activityEventProducer, configChangeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
