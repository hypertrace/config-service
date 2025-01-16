package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import ai.traceable.anomaly.config.service.exclusion.handlers.AnomalyExclusionRuleConfigStore;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.platform.config.provider.common.clients.ActorServiceClient;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class DetectionExclusionRulesMigrationModule extends AbstractModule {

  private final Config config;
  private final GrpcChannelRegistry grpcChannelRegistry;

  public DetectionExclusionRulesMigrationModule(
      Config config, GrpcChannelRegistry grpcChannelRegistry) {
    this.config = config;
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  public void configure() {
    requireBinding(AnomalyExclusionRuleConfigStore.class);
    requireBinding(FeatureCachingClient.class);
    bind(RulesMigrationManager.class).to(DetectionExclusionRulesMigrationManager.class);
  }

  @Provides
  @Singleton
  public ActorServiceClient provideActorServiceClient() {
    return new ActorServiceClient(config, grpcChannelRegistry);
  }
}
