package ai.traceable.external.data.classification.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class ExternalDataClassificationConfigServiceFactory {
  public static BindableService build(
      Channel channel,
      Config config,
      GrpcChannelRegistry channelRegistry,
      FeatureCachingClient featureCachingClient) {
    Injector injector =
        Guice.createInjector(
            new ExternalDataClassificationConfigServiceModule(
                channel, config, channelRegistry, featureCachingClient));
    return injector.getInstance(BindableService.class);
  }
}
