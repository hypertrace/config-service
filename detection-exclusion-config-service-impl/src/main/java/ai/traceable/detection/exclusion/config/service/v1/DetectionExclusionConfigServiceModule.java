package ai.traceable.detection.exclusion.config.service.v1;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesManager;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesValidator;
import ai.traceable.detection.exclusion.config.service.v1.rules.RulesManager;
import ai.traceable.detection.exclusion.config.service.v1.rules.RulesValidator;
import ai.traceable.detection.exclusion.config.service.v1.rules.migration.DetectionExclusionRulesMigrationModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class DetectionExclusionConfigServiceModule extends AbstractModule {
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
    bind(BindableService.class).to(DetectionExclusionConfigServiceImpl.class);
    bind(RulesManager.class).to(DetectionExclusionRulesManager.class);
    bind(RulesValidator.class).to(DetectionExclusionRulesValidator.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);

    install(
        new DetectionExclusionRulesMigrationModule(
            featureCachingClient, config, grpcChannelRegistry));
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub providesConfigService() {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
