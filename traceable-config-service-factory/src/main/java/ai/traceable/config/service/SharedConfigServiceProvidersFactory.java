package ai.traceable.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClientConfig;
import com.typesafe.config.Config;
import java.time.Clock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.hypertrace.config.service.change.event.impl.ConfigChangeEventGeneratorFactory;
import org.hypertrace.core.serviceframework.grpc.GrpcServiceContainerEnvironment;

public class SharedConfigServiceProvidersFactory {
  private final ConcurrentMap<GrpcServiceContainerEnvironment, SharedConfigServiceProviders>
      providersByEnvironment = new ConcurrentHashMap<>();
  private final Clock clock = Clock.systemUTC();
  static final String SERVICE_NAME = "traceable-config-service";

  SharedConfigServiceProviders getProvidersForEnvironment(
      GrpcServiceContainerEnvironment environment) {
    return this.providersByEnvironment.computeIfAbsent(environment, this::buildProviders);
  }

  private SharedConfigServiceProviders buildProviders(GrpcServiceContainerEnvironment environment) {
    Config config = environment.getConfig(SERVICE_NAME);
    return new SharedConfigServiceProviders(
        ConfigChangeEventGeneratorFactory.getInstance()
            .createConfigChangeEventGenerator(config, clock),
        new FeatureCachingClient(
            FeatureCachingClientConfig.fromConfig(config), environment.getChannelRegistry()),
        environment.getChannelRegistry(),
        config,
        environment.getChannelRegistry().forName(environment.getInProcessChannelName()),
        clock);
  }
}
