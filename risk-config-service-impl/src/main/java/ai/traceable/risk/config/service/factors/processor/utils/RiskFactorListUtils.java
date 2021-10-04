package ai.traceable.risk.config.service.factors.processor.utils;

import ai.traceable.risk.config.service.v1.RiskElementConfig;
import ai.traceable.risk.config.service.v1.RiskFactor;
import ai.traceable.risk.config.service.v1.RiskFactorConfig;
import io.grpc.Status;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class RiskFactorListUtils {
  private final RiskFactorConfigUtils riskFactorConfigUtils;

  @Inject
  public RiskFactorListUtils(RiskFactorConfigUtils riskFactorConfigUtils) {
    this.riskFactorConfigUtils = riskFactorConfigUtils;
  }

  public Status validateFactorConfigs(List<RiskFactorConfig> configs) {
    for (RiskFactorConfig config : configs) {
      Status status = riskFactorConfigUtils.validateConfig(config);
      if (!status.isOk()) {
        return status;
      }
    }
    return Status.OK;
  }

  public Collection<RiskFactor> mergeFactorConfigs(
      List<RiskFactorConfig> factorConfigs,
      List<RiskElementConfig> elementConfigs,
      List<RiskFactor> defaultRiskFactors,
      boolean checkForConfigId) {

    Map<String, RiskFactor> factorMap =
        defaultRiskFactors.stream()
            .collect(
                Collectors.toMap(
                    config -> config.getRiskFactorConfig().getId(), Function.identity()));
    for (RiskFactorConfig config : factorConfigs) {
      String id = config.getId();
      if (factorMap.containsKey(id)) {
        factorMap.put(
            id, riskFactorConfigUtils.mergeConfigs(config, elementConfigs, factorMap.get(id)));
      } else if (checkForConfigId) {
        throw Status.NOT_FOUND
            .withDescription(String.format("Risk factor id:%s NOT FOUND", id))
            .asRuntimeException();
      }
    }
    return factorMap.values();
  }

  List<RiskFactor> mergeDefaultFactors(
      List<RiskFactor> factorList, List<RiskFactor> defaultRiskFactors) {
    Map<String, RiskFactor> factorMap =
        defaultRiskFactors.stream()
            .collect(
                Collectors.toMap(
                    config -> config.getRiskFactorConfig().getId(), Function.identity()));
    factorList.forEach(config -> factorMap.put(config.getRiskFactorConfig().getId(), config));
    return factorMap.values().stream()
        .map(riskFactor -> riskFactor.toBuilder().setIsDefault(true).build())
        .collect(Collectors.toList());
  }

  boolean areAllFactorConfigsDefault(
      List<RiskFactorConfig> factorList, List<RiskFactor> defaultRiskFactors) {
    Map<String, RiskFactor> factorMap =
        defaultRiskFactors.stream()
            .collect(
                Collectors.toMap(
                    config -> config.getRiskFactorConfig().getId(), Function.identity()));
    for (RiskFactorConfig config : factorList) {
      if (!factorMap.containsKey(config.getId())
          || !riskFactorConfigUtils.isConfigDefault(
              config, factorMap.get(config.getId()).getRiskFactorConfig())) {
        return false;
      }
    }
    return true;
  }
}
