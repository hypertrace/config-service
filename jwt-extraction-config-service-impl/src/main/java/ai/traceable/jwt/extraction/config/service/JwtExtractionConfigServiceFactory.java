package ai.traceable.jwt.extraction.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class JwtExtractionConfigServiceFactory {
  public static BindableService build(
      Channel channel,
      ConfigChangeEventGenerator configChangeEventGenerator,
      Config config,
      FeatureCachingClient featureCachingClient) {
    Injector injector =
        Guice.createInjector(
            new JwtExtractionConfigServiceModule(
                channel, configChangeEventGenerator, config, featureCachingClient));
    return injector.getInstance(BindableService.class);
  }
}
