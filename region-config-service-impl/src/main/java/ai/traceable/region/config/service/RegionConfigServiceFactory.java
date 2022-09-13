package ai.traceable.region.config.service;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class RegionConfigServiceFactory {
  public static BindableService build(
      Channel channel,
      Config config,
      ActivityEventProducer activityEventProducer,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient) {
    Injector injector =
        Guice.createInjector(
            new RegionConfigServiceModule(
                channel,
                config,
                activityEventProducer,
                configChangeEventGenerator,
                featureCachingClient));
    return injector.getInstance(BindableService.class);
  }
}
