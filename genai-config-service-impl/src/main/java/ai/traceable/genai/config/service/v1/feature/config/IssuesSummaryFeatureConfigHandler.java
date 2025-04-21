package ai.traceable.genai.config.service.v1.feature.config;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.IssuesSummaryFeatureConfig;
import java.util.Optional;

public class IssuesSummaryFeatureConfigHandler
    implements GenAiFeatureConfigHandler<IssuesSummaryFeatureConfig> {

  private static final String FEATURE_NAME = "issues_summary_feature_config";

  @Override
  public String getFeatureName() {
    return FEATURE_NAME;
  }

  @Override
  public Optional<IssuesSummaryFeatureConfig> mergeConfigs(
      GenAiConfig highPriorityConfig, GenAiConfig lowPriorityConfig) {
    if (highPriorityConfig.hasIssuesSummaryFeatureConfig()) {
      return Optional.of(highPriorityConfig.getIssuesSummaryFeatureConfig());
    }
    if (lowPriorityConfig.hasIssuesSummaryFeatureConfig()) {
      return Optional.of(lowPriorityConfig.getIssuesSummaryFeatureConfig());
    }
    return Optional.empty();
  }

  @Override
  public Optional<IssuesSummaryFeatureConfig> getFeatureConfigFromUpdate(
      GenAiFeatureConfigUpdate featureConfigUpdate) {
    if (featureConfigUpdate.hasIssuesSummaryFeatureConfig()) {
      return Optional.of(featureConfigUpdate.getIssuesSummaryFeatureConfig());
    }
    return Optional.empty();
  }
}
