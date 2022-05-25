package ai.traceable.data.classification.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class DataClassificationConfigServiceFactory {
  public static BindableService build(
      ManagedChannel channel,
      ConfigChangeEventGenerator configChangeEventGenerator,
      Config config,
      FeatureCachingClient featureCachingClient) {
    Injector injector =
        Guice.createInjector(
            new DataClassificationConfigServiceModule(
                channel, configChangeEventGenerator, config, featureCachingClient));
    return injector.getInstance(BindableService.class);
  }
}
