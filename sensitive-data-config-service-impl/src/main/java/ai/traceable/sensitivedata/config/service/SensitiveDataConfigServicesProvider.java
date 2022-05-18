package ai.traceable.sensitivedata.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class SensitiveDataConfigServicesProvider {

  private final Injector injector;

  public SensitiveDataConfigServicesProvider(
      Channel channel,
      Config config,
      GrpcChannelRegistry channelRegistry,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient) {
    this.injector =
        Guice.createInjector(
            new SensitiveDataModule(
                channel,
                config,
                channelRegistry,
                configChangeEventGenerator,
                featureCachingClient));
  }

  public BindableService getSensitiveDataConfigService() {
    return this.injector.getInstance(SensitiveDataConfigServiceImpl.class);
  }

  public BindableService getPiiFilterConfigService() {
    return this.injector.getInstance(PiiFilterConfigServiceImpl.class);
  }
}
