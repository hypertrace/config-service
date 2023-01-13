package ai.traceable.risk.config.service.v2.level.builder;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskLevelConfigValues;

public class RiskLevelConfigBuilder extends RiskConfigBuilder<RiskLevelConfigValues> {

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
}
