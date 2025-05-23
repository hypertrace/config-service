package ai.traceable.ratelimiting.service.v2;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class RateLimitingConfigServiceFactory {
  public static BindableService build(
      Channel channel,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator,
      GrpcChannelRegistry grpcChannelRegistry,
      FeatureCachingClient featureCachingClient) {
    Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION,
            new RateLimitingConfigServiceModule(
                channel,
                config,
                configChangeEventGenerator,
                grpcChannelRegistry,
                featureCachingClient));
    return injector.getInstance(BindableService.class);
  }
}
