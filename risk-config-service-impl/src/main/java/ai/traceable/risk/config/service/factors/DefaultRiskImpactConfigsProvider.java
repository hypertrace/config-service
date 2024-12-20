package ai.traceable.risk.config.service.factors;

import ai.traceable.risk.config.service.RiskConfigServiceConfig;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskContributorConfigs;
import com.google.inject.Provider;
import io.grpc.Status;
import jakarta.inject.Inject;

public class DefaultRiskImpactConfigsProvider implements Provider<RiskContributorConfigs> {

  private static final String RISK_IMPACT_CONFIGS_FILE_PATH = "risk-impact-configs.conf";

  private final RiskContributorConfigs riskImpactConfigs;

  @Inject
  public DefaultRiskImpactConfigsProvider(
      RiskConfigUtils<RiskContributorConfigs> riskImpactConfigUtils,
      RiskConfigServiceConfig riskConfig) {

    riskImpactConfigs =
        riskImpactConfigUtils.mergeConfigs(
            riskConfig.getRiskImpactConfigs(), RISK_IMPACT_CONFIGS_FILE_PATH);
    Status validationStatus = riskImpactConfigUtils.validateConfig(riskImpactConfigs);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }
  }

  @Override
  public RiskContributorConfigs get() {
    return riskImpactConfigs;
  }
}
