package ai.traceable.detection.exclusion.config.service.v1;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesManager;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesValidator;
import ai.traceable.detection.exclusion.config.service.v1.rules.RulesManager;
import ai.traceable.detection.exclusion.config.service.v1.rules.RulesValidator;
import ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition.DetectionExclusionRuleConditionModule;
import ai.traceable.detection.exclusion.config.service.v1.rules.migration.DetectionExclusionRulesMigrationModule;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ExclusionModsecRulesModule;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProviderModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class DetectionExclusionConfigServiceModule extends AbstractModule {
  private static final String CACHED_SERVICE_MAPPING_NAME =
      "cachedServiceMapping-detectionExclusionConfig";
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private final FeatureCachingClient featureCachingClient;
  private final Config config;
  private final GrpcChannelRegistry grpcChannelRegistry;

  DetectionExclusionConfigServiceModule(
      Channel channel,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient,
      Config config,
      GrpcChannelRegistry grpcChannelRegistry) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.featureCachingClient = featureCachingClient;
    this.config = config;
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  public void configure() {
    bind(Clock.class).toInstance(Clock.systemUTC());
    bind(BindableService.class).to(DetectionExclusionConfigServiceImpl.class);
    bind(RulesManager.class).to(DetectionExclusionRulesManager.class);
    bind(RulesValidator.class).to(DetectionExclusionRulesValidator.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(FeatureCachingClient.class).toInstance(featureCachingClient);

    install(new DetectionExclusionRulesMigrationModule(config, grpcChannelRegistry));
    install(new ExclusionModsecRulesModule(channel));
    install(new DetectionExclusionRuleConditionModule());
    install(
        new CachedServiceMappingProviderModule(
            grpcChannelRegistry, config, CACHED_SERVICE_MAPPING_NAME));
  }

  @Provides
  DetectionExclusionConfigServiceConfig providesConfig() {
    return new DetectionExclusionConfigServiceConfig(config);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub providesConfigService() {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
