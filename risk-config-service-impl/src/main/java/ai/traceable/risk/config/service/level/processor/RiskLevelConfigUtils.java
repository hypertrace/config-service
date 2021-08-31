package ai.traceable.risk.config.service.level.processor;

import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskLevelConfigValues;
import io.grpc.Status;

public class RiskLevelConfigUtils extends RiskConfigUtils<RiskLevelConfigValues> {

  @Override
  public RiskLevelConfigValues.Builder getNewBuilder() {
    return RiskLevelConfigValues.newBuilder();
  }

  @Override
  public RiskLevelConfigValues mergeConfigs(
      RiskLevelConfigValues highPriorityConfig, RiskLevelConfigValues lowPriorityConfig) {
    if (highPriorityConfig.equals(RiskLevelConfigValues.getDefaultInstance())) {
      return lowPriorityConfig;
    }
    return highPriorityConfig;
  }

  @Override
  public boolean isConfigDefault(
      RiskLevelConfigValues specificConfig, RiskLevelConfigValues defaultConfig) {
    return specificConfig.equals(defaultConfig);
  }

  @Override
  public Status validateConfig(RiskLevelConfigValues config) {
    if (!isValidScore(config.getMediumLevelMinScore())
        || !isValidScore(config.getHighLevelMinScore())
        || !isValidScore(config.getCriticalLevelMinScore())) {
      return Status.OUT_OF_RANGE.withDescription("Score should be between 0 and 10");
    }
    if (!(config.getMediumLevelMinScore() <= config.getHighLevelMinScore()
        && config.getHighLevelMinScore() <= config.getCriticalLevelMinScore())) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Minimum Scores should satisfy the relation Medium <= High <= Critical");
    }
    return Status.OK;
  }
}
