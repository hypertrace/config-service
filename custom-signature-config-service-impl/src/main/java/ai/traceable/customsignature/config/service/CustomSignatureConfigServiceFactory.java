package ai.traceable.customsignature.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class CustomSignatureConfigServiceFactory {
  public static BindableService build(
      Channel channel,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient,
      GrpcChannelRegistry grpcChannelRegistry) {
    Injector injector =
        Guice.createInjector(
            new CustomSignatureConfigServiceModule(
                channel,
                config,
                configChangeEventGenerator,
                featureCachingClient,
                grpcChannelRegistry));
    return injector.getInstance(BindableService.class);
  }
}
