package ai.traceable.aiapp.protection.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import io.grpc.BindableService;
import io.grpc.Channel;

public class AiAppProtectionConfigServiceFactory {

  private AiAppProtectionConfigServiceFactory() {}

  public static BindableService build(Channel channel, FeatureCachingClient featureCachingClient) {
    Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION,
            new AiAppProtectionConfigServiceModule(channel, featureCachingClient));
    return injector.getInstance(BindableService.class);
  }
}
