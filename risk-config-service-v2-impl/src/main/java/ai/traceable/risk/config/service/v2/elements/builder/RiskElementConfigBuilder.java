package ai.traceable.risk.config.service.v2.elements.builder;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskElementConfig;

public class RiskElementConfigBuilder extends RiskConfigBuilder<RiskElementConfig> {

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

    return lowPriorityConfig.toBuilder()
        .setDisabled(highPriorityConfig.getDisabled())
        .setRiskElementScoring(highPriorityConfig.getRiskElementScoring())
        .build();
  }
}
