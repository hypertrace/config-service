package ai.traceable.risk.config.service.v2.factors.builder;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskElementConfig;
import ai.traceable.risk.config.service.v2.RiskFactorConfig;
import io.grpc.Status;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RiskFactorConfigBuilder extends RiskConfigBuilder<RiskFactorConfig> {

  private final RiskConfigBuilder<RiskElementConfig> riskElementConfigBuilder;

  @Override
  public RiskFactorConfig.Builder getNewBuilder() {
    return RiskFactorConfig.newBuilder();
  }

  @Override
  public RiskFactorConfig mergeConfigs(
      RiskFactorConfig highPriorityConfig, RiskFactorConfig lowPriorityConfig) {
    Collection<RiskElementConfig> mergedRiskElementConfigs =
        mergeRiskElementConfigs(
            highPriorityConfig.getRiskElementConfigsList(),
            lowPriorityConfig.getRiskElementConfigsList());
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
      if (lowPriorityElementConfigsMap.containsKey(id)) {
        lowPriorityElementConfigsMap.put(
            id,
            riskElementConfigBuilder.mergeConfigs(
                riskElementConfig, lowPriorityElementConfigsMap.get(id)));
      } else {
        throw Status.NOT_FOUND
            .withDescription(String.format("Risk element id:%s NOT FOUND", id))
            .asRuntimeException();
      }
    }
    return lowPriorityElementConfigsMap.values();
  }
}
