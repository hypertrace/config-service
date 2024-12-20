package ai.traceable.risk.config.service.v2.contributors.builder;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.factors.builder.RiskFactorListBuilder;
import com.google.protobuf.Message;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RiskContributorConfigBuilder extends RiskConfigBuilder<RiskContributorConfigs> {

  private final RiskFactorListBuilder riskFactorListBuilder;

  @Override
  public Message.Builder getNewBuilder() {
    return RiskContributorConfigs.newBuilder();
  }

  @Override
  public RiskContributorConfigs mergeConfigs(
      RiskContributorConfigs highPriorityConfig, RiskContributorConfigs lowPriorityConfig) {
    return RiskContributorConfigs.newBuilder()
        .addAllRiskFactors(
            riskFactorListBuilder.mergeDefaultFactors(
                highPriorityConfig.getRiskFactorsList(), lowPriorityConfig.getRiskFactorsList()))
        .build();
  }
}
