package ai.traceable.risk.config.service.factors.processor.utils;

import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskElementConfig;
import io.grpc.Status;

public class RiskElementConfigUtils extends RiskConfigUtils<RiskElementConfig> {

  @Override
  public RiskElementConfig.Builder getNewBuilder() {
    return RiskElementConfig.newBuilder();
  }

  @Override
  public RiskElementConfig mergeConfigs(
      RiskElementConfig highPriorityConfig, RiskElementConfig lowPriorityConfig) {
    if (highPriorityConfig.equals(RiskElementConfig.getDefaultInstance())) {
      return lowPriorityConfig;
    }
    // element info cannot be modified - only scoring
    return lowPriorityConfig.toBuilder()
        .setRiskElementScoring(highPriorityConfig.getRiskElementScoring())
        .build();
  }

  @Override
  public boolean isConfigDefault(
      RiskElementConfig specificConfig, RiskElementConfig defaultConfig) {
    return specificConfig.equals(defaultConfig);
  }

  @Override
  public Status validateConfig(RiskElementConfig config) {
    if (config.getId().isBlank()) {
      return Status.INVALID_ARGUMENT.withDescription("Element should have a valid ID");
    }
    if (!isValidScore(config.getRiskElementScoring().getScore())) {
      return Status.OUT_OF_RANGE.withDescription("Score should be between 0 and 10");
    }
    return Status.OK;
  }
}
