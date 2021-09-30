package ai.traceable.risk.config.service.factors;

import ai.traceable.risk.config.service.RiskConfigServiceConfig;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskContributorConfigs;
import io.grpc.Status;
import javax.inject.Inject;
import javax.inject.Provider;

public class DefaultRiskLikelihoodConfigsProvider implements Provider<RiskContributorConfigs> {

  private static final String RISK_LIKELIHOOD_CONFIGS_FILE_PATH = "risk-likelihood-configs.conf";

  private final RiskContributorConfigs riskLikelihoodConfigs;

  @Inject
  public DefaultRiskLikelihoodConfigsProvider(
      RiskConfigUtils<RiskContributorConfigs> riskLikelihoodConfigUtils,
      RiskConfigServiceConfig riskConfig) {

    riskLikelihoodConfigs =
        riskLikelihoodConfigUtils.mergeConfigs(
            riskConfig.getRiskLikelihoodConfigs(), RISK_LIKELIHOOD_CONFIGS_FILE_PATH);
    Status validationStatus = riskLikelihoodConfigUtils.validateConfig(riskLikelihoodConfigs);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }
  }

  @Override
  public RiskContributorConfigs get() {
    return riskLikelihoodConfigs;
  }
}
