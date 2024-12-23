package ai.traceable.risk.config.service.level;

import ai.traceable.risk.config.service.RiskConfigServiceConfig;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskLevelConfigValues;
import io.grpc.Status;
import jakarta.inject.Inject;
import jakarta.inject.Provider;

public class DefaultRiskLevelConfigValuesProvider implements Provider<RiskLevelConfigValues> {

  private static final String RISK_LEVEL_DEFAULT_CONFIG_FILE_PATH = "risk-level-config-values.conf";

  private final RiskLevelConfigValues riskLevelConfigValues;

  @Inject
  public DefaultRiskLevelConfigValuesProvider(
      RiskConfigUtils<RiskLevelConfigValues> riskLevelConfigUtils,
      RiskConfigServiceConfig riskConfig) {
    riskLevelConfigValues =
        riskLevelConfigUtils.mergeConfigs(
            riskConfig.getRiskLevelConfigValues(), RISK_LEVEL_DEFAULT_CONFIG_FILE_PATH);
    Status validationStatus = riskLevelConfigUtils.validateConfig(riskLevelConfigValues);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }
  }

  @Override
  public RiskLevelConfigValues get() {
    return riskLevelConfigValues;
  }
}
