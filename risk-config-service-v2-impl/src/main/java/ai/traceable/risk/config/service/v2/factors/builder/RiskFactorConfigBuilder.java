package ai.traceable.risk.config.service.v2.factors.builder;

import static ai.traceable.risk.config.service.v2.RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskElementConfig;
import ai.traceable.risk.config.service.v2.RiskFactorConfig;
import ai.traceable.risk.config.service.v2.elements.builder.LabelElementConfigsBuilder;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class RiskFactorConfigBuilder extends RiskConfigBuilder<RiskFactorConfig> {

  private final RiskConfigBuilder<RiskElementConfig> riskElementConfigBuilder;
  private final LabelElementConfigsBuilder labelElementConfigsBuilder;

  @Override
  public RiskFactorConfig.Builder getNewBuilder() {
    return RiskFactorConfig.newBuilder();
  }

  @Override
  public RiskFactorConfig mergeConfigs(
      RiskFactorConfig highPriorityConfig, RiskFactorConfig lowPriorityConfig) {
    Collection<RiskElementConfig> mergedRiskElementConfigs;
    if (RISK_FACTOR_CATEGORY_LABELS.equals(highPriorityConfig.getRiskFactorCategory())) {
      mergedRiskElementConfigs =
          labelElementConfigsBuilder.mergeRiskElementConfigs(
              highPriorityConfig.getRiskElementConfigsList(),
              lowPriorityConfig.getRiskElementConfigsList());
    } else {
      mergedRiskElementConfigs =
          mergeRiskElementConfigs(
              highPriorityConfig.getRiskElementConfigsList(),
              lowPriorityConfig.getRiskElementConfigsList());
    }
    RiskFactorConfig.Builder riskFactorConfigBuilder =
        RiskFactorConfig.newBuilder()
            .setRiskFactorCategory(highPriorityConfig.getRiskFactorCategory())
            .setDisabled(highPriorityConfig.getDisabled())
            .addAllRiskElementConfigs(mergedRiskElementConfigs);
    if (lowPriorityConfig.hasRiskConfigScope()) {
      riskFactorConfigBuilder.setRiskConfigScope(lowPriorityConfig.getRiskConfigScope());
    }
    return riskFactorConfigBuilder.build();
  }

  private Collection<RiskElementConfig> mergeRiskElementConfigs(
      Collection<RiskElementConfig> highPriorityRiskElementConfigsList,
      Collection<RiskElementConfig> lowPriorityRiskElementConfigsList) {

    Map<String, RiskElementConfig> lowPriorityElementConfigsMap =
        lowPriorityRiskElementConfigsList.stream()
            .collect(Collectors.toMap(RiskElementConfig::getId, Function.identity()));

    for (RiskElementConfig riskElementConfig : highPriorityRiskElementConfigsList) {
      String id = riskElementConfig.getId();
      RiskElementConfig mergedElementConfig = riskElementConfig;
      if (lowPriorityElementConfigsMap.containsKey(id)) {
        mergedElementConfig =
            riskElementConfigBuilder.mergeConfigs(
                riskElementConfig, lowPriorityElementConfigsMap.get(id));
      }
      lowPriorityElementConfigsMap.put(id, mergedElementConfig);
    }
    return lowPriorityElementConfigsMap.values();
  }
}
