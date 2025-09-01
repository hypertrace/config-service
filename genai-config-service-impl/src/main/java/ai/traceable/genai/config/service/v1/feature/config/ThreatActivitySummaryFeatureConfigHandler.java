package ai.traceable.genai.config.service.v1.feature.config;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.ThreatActivitySummaryFeatureConfig;
import ai.traceable.genai.config.service.v1.UpdateGenAiConfigRequest;
import java.util.Optional;

public class ThreatActivitySummaryFeatureConfigHandler
    implements GenAiFeatureConfigHandler<ThreatActivitySummaryFeatureConfig> {

  private static final String FEATURE_NAME = "threat_activity_summary_feature_config";

  @Override
  public String getFeatureName() {
    return FEATURE_NAME;
  }

  @Override
  public Optional<ThreatActivitySummaryFeatureConfig> mergeConfigs(
      GenAiConfig highPriorityConfig, GenAiConfig lowPriorityConfig) {
    if (highPriorityConfig.hasThreatActivitySummaryFeatureConfig()) {
      return Optional.of(highPriorityConfig.getThreatActivitySummaryFeatureConfig());
    }
    if (highPriorityConfig.hasGlobalConfig()) {
      return Optional.of(
          ThreatActivitySummaryFeatureConfig.newBuilder()
              .setEnabled(highPriorityConfig.getGlobalConfig().getEnabled())
              .build());
    }
    if (lowPriorityConfig.hasThreatActivitySummaryFeatureConfig()) {
      return Optional.of(lowPriorityConfig.getThreatActivitySummaryFeatureConfig());
    }
    if (lowPriorityConfig.hasGlobalConfig()) {
      return Optional.of(
          ThreatActivitySummaryFeatureConfig.newBuilder()
              .setEnabled(lowPriorityConfig.getGlobalConfig().getEnabled())
              .build());
    }
    return Optional.empty();
  }

  @Override
  public Optional<ThreatActivitySummaryFeatureConfig> getFeatureConfigFromUpdate(
      GenAiFeatureConfigUpdate featureConfigUpdate) {
    if (featureConfigUpdate.hasThreatActivitySummaryFeatureConfig()) {
      return Optional.of(featureConfigUpdate.getThreatActivitySummaryFeatureConfig());
    }
    return Optional.empty();
  }

  @Override
  public Optional<ThreatActivitySummaryFeatureConfig> getFeatureLevelConfigFromUpdate(
      UpdateGenAiConfigRequest configUpdate) {
    if (configUpdate.hasGlobalConfigUpdate()) {
      return Optional.of(
          ThreatActivitySummaryFeatureConfig.newBuilder()
              .setEnabled(configUpdate.getGlobalConfigUpdate().getEnabled())
              .build());
    }
    if (configUpdate.hasFeatureLevelConfigUpdate()
        && configUpdate
            .getFeatureLevelConfigUpdate()
            .hasThreatActivitySummaryFeatureConfigUpdate()) {
      return Optional.of(
          configUpdate.getFeatureLevelConfigUpdate().getThreatActivitySummaryFeatureConfigUpdate());
    }
    return Optional.empty();
  }
}
