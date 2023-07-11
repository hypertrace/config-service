package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import ai.traceable.anomaly.config.service.exclusion.handlers.AnomalyExclusionRuleConfigStore;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.platform.config.provider.common.clients.ActorServiceClient;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class DetectionExclusionRulesMigrationModule extends AbstractModule {

  private final FeatureCachingClient featureCachingClient;
  private final Config config;
  private final GrpcChannelRegistry grpcChannelRegistry;
  private final Channel channel;

  public DetectionExclusionRulesMigrationModule(
      FeatureCachingClient featureCachingClient,
      Config config,
      GrpcChannelRegistry grpcChannelRegistry,
      Channel channel) {
    this.featureCachingClient = featureCachingClient;
    this.config = config;
    this.grpcChannelRegistry = grpcChannelRegistry;
    this.channel = channel;
  }

  @Override
  public void configure() {
    requireBinding(AnomalyExclusionRuleConfigStore.class);
    bind(FeatureCachingClient.class).toInstance(this.featureCachingClient);
    bind(RulesMigrationManager.class).to(DetectionExclusionRulesMigrationManager.class);
  }

  @Provides
  @Singleton
  public ActorServiceClient provideActorServiceClient() {
    return new ActorServiceClient(config, grpcChannelRegistry);
  }
}
