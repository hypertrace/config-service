package ai.traceable.external.agent.attribute.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;

public class ExternalAgentAttributeConfigServiceFactory {
  private ExternalAgentAttributeConfigServiceFactory() {}

  public static BindableService build(Channel channel, FeatureCachingClient featureCachingClient) {
    Injector injector =
        Guice.createInjector(
            new ExternalAgentAttributeConfigServiceModule(channel, featureCachingClient));
    return injector.getInstance(BindableService.class);
  }
}
