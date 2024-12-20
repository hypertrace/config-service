package ai.traceable.risk.config.service.v2.contributors;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskConfigServiceConfig;
import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.contributors.validator.RiskContributorConfigsValidator;
import com.google.inject.Provider;
import io.grpc.Status;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.AllArgsConstructor;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultRiskContributorConfigsProvider implements Provider<RiskContributorConfigs> {

  private static final String RISK_CONTRIBUTOR_CONFIGS_FILE_PATH = "risk-contributor-configs.conf";

  private final RiskConfigBuilder<RiskContributorConfigs> riskConfigBuilder;
  private final RiskContributorConfigsValidator contributorConfigsValidator;
  private final RiskConfigServiceConfig riskConfig;

  @Override
  public RiskContributorConfigs get() {
    RiskContributorConfigs riskContributorConfigs =
        riskConfigBuilder.mergeConfigs(
            riskConfig.getRiskContributorConfigs(), RISK_CONTRIBUTOR_CONFIGS_FILE_PATH);
    Status validationStatus =
        contributorConfigsValidator.validateRiskContributorConfigs(riskContributorConfigs);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }
    return riskContributorConfigs;
  }
}
