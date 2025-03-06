package ai.traceable.customsignature.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class CustomSignatureConfigServiceFactory {
  public static BindableService build(
      Channel channel,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient) {
    Injector injector =
        Guice.createInjector(
            new CustomSignatureConfigServiceModule(
                channel, config, configChangeEventGenerator, featureCachingClient));
    return injector.getInstance(BindableService.class);
  }
}
