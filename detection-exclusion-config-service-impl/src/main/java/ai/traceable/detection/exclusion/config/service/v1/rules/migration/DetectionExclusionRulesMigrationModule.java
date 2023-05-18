package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import ai.traceable.anomaly.config.service.exclusion.handlers.AnomalyExclusionRuleConfigStore;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.AbstractModule;

public class DetectionExclusionRulesMigrationModule extends AbstractModule {

  private final FeatureCachingClient featureCachingClient;

  public DetectionExclusionRulesMigrationModule(FeatureCachingClient featureCachingClient) {
    this.featureCachingClient = featureCachingClient;
  }

  @Override
  public void configure() {
    requireBinding(AnomalyExclusionRuleConfigStore.class);
    bind(FeatureCachingClient.class).toInstance(this.featureCachingClient);
    bind(RulesMigrationManager.class).to(DetectionExclusionRulesMigrationManager.class);
  }
}
