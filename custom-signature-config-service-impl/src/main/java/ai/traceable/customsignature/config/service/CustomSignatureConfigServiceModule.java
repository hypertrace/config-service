package ai.traceable.customsignature.config.service;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.customsignature.config.service.modsec.ModsecRulesManagerModule;
import ai.traceable.customsignature.config.service.rules.RulesManagerModule;
import ai.traceable.customsignature.config.service.rules.converter.evaluator.EvaluatorConverterModule;
import ai.traceable.customsignature.config.service.rules.converter.expression.ExpressionConverterModule;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProviderModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

class CustomSignatureConfigServiceModule extends AbstractModule {
  private static final String CACHED_SERVICE_MAPPING_NAME =
      "cachedServiceMapping-customSignatureConfig";
  private final Config config;
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private final FeatureCachingClient featureCachingClient;
  private final GrpcChannelRegistry grpcChannelRegistry;

  CustomSignatureConfigServiceModule(
      Channel channel,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient,
      GrpcChannelRegistry grpcChannelRegistry) {
    this.channel = channel;
    this.config = config;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.featureCachingClient = featureCachingClient;
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(CustomSignatureConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(CustomSignatureConfigServiceConfig.class)
        .toInstance(new CustomSignatureConfigServiceConfig(this.config));
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(FeatureCachingClient.class).toInstance(featureCachingClient);
    install(new RulesManagerModule());
    install(new ModsecRulesManagerModule());
    install(new ExpressionConverterModule());
    install(new EvaluatorConverterModule());
    install(
        new CachedServiceMappingProviderModule(
            grpcChannelRegistry, config, CACHED_SERVICE_MAPPING_NAME));
  }

  @Provides
  ModsecRuleVersion providesModsecRuleVersion(
      CustomSignatureConfigServiceConfig customSignatureConfigServiceConfig) {
    return customSignatureConfigServiceConfig.getModsecRuleVersion();
  }
}
