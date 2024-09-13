package ai.traceable.detection.exclusion.config.service.v1.rules.modsec;

import ai.traceable.anomaly.config.service.registry.AnomalyConfigRegistryModule;
import ai.traceable.customsignature.config.service.modsec.ModsecRulesManagerModule;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProviderModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class ExclusionModsecRulesModule extends AbstractModule {
  private static final String CACHED_SERVICE_MAPPING_NAME =
      "cachedServiceMapping-detectionExclusionConfig";

  private final Channel channel;
  private final Config config;
  private final GrpcChannelRegistry grpcChannelRegistry;

  public ExclusionModsecRulesModule(
      Channel channel, Config config, GrpcChannelRegistry grpcChannelRegistry) {
    this.channel = channel;
    this.config = config;
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  protected void configure() {
    bind(Channel.class).toInstance(channel);
    bind(ModsecClauseConverter.class).to(ModsecClauseConverterImpl.class);
    install(new AnomalyConfigRegistryModule());
    install(new ModsecRulesManagerModule());

    install(
        new CachedServiceMappingProviderModule(
            grpcChannelRegistry, config, CACHED_SERVICE_MAPPING_NAME));
  }

  @Provides
  ClientConfig provideClientConfig() {
    return ClientConfig.DEFAULT;
  }
}
