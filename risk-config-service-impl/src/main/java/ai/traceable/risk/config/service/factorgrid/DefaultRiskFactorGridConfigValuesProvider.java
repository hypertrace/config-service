package ai.traceable.risk.config.service.factorgrid;

import ai.traceable.risk.config.service.RiskConfigServiceConfig;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfigValues;
import com.google.inject.Provider;
import io.grpc.Status;
import jakarta.inject.Inject;

public class DefaultRiskFactorGridConfigValuesProvider
    implements Provider<RiskFactorGridConfigValues> {

  private static final String RISK_FACTOR_GRID_DEFAULT_CONFIG_FILE_PATH =
      "risk-factor-grid-config-values.conf";

  private final RiskFactorGridConfigValues riskFactorGridConfigValues;

  @Inject
  public DefaultRiskFactorGridConfigValuesProvider(
      RiskConfigUtils<RiskFactorGridConfigValues> riskFactorGridConfigUtils,
      RiskConfigServiceConfig riskConfig) {
    riskFactorGridConfigValues =
        riskFactorGridConfigUtils.mergeConfigs(
            riskConfig.getRiskFactorGridConfigValues(), RISK_FACTOR_GRID_DEFAULT_CONFIG_FILE_PATH);
    Status validationStatus = riskFactorGridConfigUtils.validateConfig(riskFactorGridConfigValues);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }
  }

  @Override
  public RiskFactorGridConfigValues get() {
    return riskFactorGridConfigValues;
  }
}
