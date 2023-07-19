package ai.traceable.sessionidentification.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class SessionIdentificationConfigServiceFactory {
  public static BindableService build(
      Channel channel,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient,
      Config config) {
    Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION,
            new SessionIdentificationConfigServiceModule(
                channel, configChangeEventGenerator, featureCachingClient, config));
    return injector.getInstance(BindableService.class);
  }
}
