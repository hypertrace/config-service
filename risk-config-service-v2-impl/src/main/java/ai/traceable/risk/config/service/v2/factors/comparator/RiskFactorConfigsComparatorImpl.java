package ai.traceable.risk.config.service.v2.factors.comparator;

import ai.traceable.risk.config.service.v2.RiskElementConfig;
import ai.traceable.risk.config.service.v2.RiskElementScoring;
import ai.traceable.risk.config.service.v2.RiskFactorConfig;
import java.util.Map;
import java.util.stream.Collectors;

public class RiskFactorConfigsComparatorImpl implements RiskFactorConfigsComparator {

  @Override
  public boolean isFactorConfigEqual(
      RiskFactorConfig specificConfig, RiskFactorConfig defaultConfig) {
    if (specificConfig.getDisabled() != defaultConfig.getDisabled()) {
      return false;
    }

    if (specificConfig.getRiskElementConfigsCount() != defaultConfig.getRiskElementConfigsCount()) {
      return false;
    }

    if (!specificConfig.getRiskFactorCategory().equals(defaultConfig.getRiskFactorCategory())) {
      return false;
    }

    Map<String, RiskElementScoring> elementConfigMap =
        defaultConfig.getRiskElementConfigsList().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    RiskElementConfig::getId, RiskElementConfig::getRiskElementScoring));

    for (RiskElementConfig config : specificConfig.getRiskElementConfigsList()) {
      if (!elementConfigMap.containsKey(config.getId())
          || !config.getRiskElementScoring().equals(elementConfigMap.get(config.getId()))) {
        return false;
      }
    }
    return true;
  }
}
