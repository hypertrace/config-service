package ai.traceable.blocking.config.service.v2;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class BlockingConfigServiceFactory {
  public static BindableService build(
      Channel channel,
      Config config,
      GrpcChannelRegistry grpcChannelRegistry,
      FeatureCachingClient featureCachingClient) {
    Injector injector =
        Guice.createInjector(
            new BlockingConfigServiceModule(
                channel, config, grpcChannelRegistry, featureCachingClient));
    return injector.getInstance(BindableService.class);
  }
}
