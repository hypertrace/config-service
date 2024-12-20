package ai.traceable.risk.config.service.factors.processor.utils;

import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskContributorConfigs;
import ai.traceable.risk.config.service.v1.RiskFactor;
import ai.traceable.risk.config.service.v1.RiskFactorConfig;
import com.google.protobuf.Message;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

public class RiskContributorConfigUtils extends RiskConfigUtils<RiskContributorConfigs> {

  private final RiskFactorListUtils riskFactorListUtils;

  @Inject
  public RiskContributorConfigUtils(RiskFactorListUtils riskFactorListUtils) {
    this.riskFactorListUtils = riskFactorListUtils;
  }

  @Override
  public Message.Builder getNewBuilder() {
    return RiskContributorConfigs.newBuilder();
  }

  @Override
  public RiskContributorConfigs mergeConfigs(
      RiskContributorConfigs highPriorityConfig, RiskContributorConfigs lowPriorityConfig) {
    return RiskContributorConfigs.newBuilder()
        .addAllRiskFactors(
            riskFactorListUtils.mergeDefaultFactors(
                highPriorityConfig.getRiskFactorsList(), lowPriorityConfig.getRiskFactorsList()))
        .build();
  }

  @Override
  public boolean isConfigDefault(
      RiskContributorConfigs specificConfig, RiskContributorConfigs defaultConfig) {
    // not required at this level - is separately evaluated per factor..
    return riskFactorListUtils.areAllFactorConfigsDefault(
        getRiskFactorConfigs(specificConfig), defaultConfig.getRiskFactorsList());
  }

  @Override
  public Status validateConfig(RiskContributorConfigs config) {
    return riskFactorListUtils.validateFactorConfigs(getRiskFactorConfigs(config));
  }

  private List<RiskFactorConfig> getRiskFactorConfigs(RiskContributorConfigs configs) {
    return configs.getRiskFactorsList().stream()
        .map(RiskFactor::getRiskFactorConfig)
        .collect(Collectors.toList());
  }
}
