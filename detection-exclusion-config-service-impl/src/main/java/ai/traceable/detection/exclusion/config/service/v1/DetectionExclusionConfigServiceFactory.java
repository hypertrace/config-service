package ai.traceable.detection.exclusion.config.service.v1;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class DetectionExclusionConfigServiceFactory {

  private DetectionExclusionConfigServiceFactory() {}

  public static BindableService build(
      Channel channel,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient,
      Config config,
      GrpcChannelRegistry grpcChannelRegistry) {
    Injector injector =
        Guice.createInjector(
            new DetectionExclusionConfigServiceModule(
                channel,
                configChangeEventGenerator,
                featureCachingClient,
                config,
                grpcChannelRegistry));
    return injector.getInstance(BindableService.class);
  }
}
