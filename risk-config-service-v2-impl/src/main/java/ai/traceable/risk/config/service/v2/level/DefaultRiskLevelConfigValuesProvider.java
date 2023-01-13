package ai.traceable.risk.config.service.v2.level;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskConfigServiceConfig;
import ai.traceable.risk.config.service.v2.RiskLevelConfigValues;
import ai.traceable.risk.config.service.v2.level.validator.RiskLevelConfigValidator;
import io.grpc.Status;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import lombok.AllArgsConstructor;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultRiskLevelConfigValuesProvider implements Provider<RiskLevelConfigValues> {

  private static final String RISK_LEVEL_DEFAULT_CONFIG_FILE_PATH =
      "risk-scoring-level-config-values.conf";

  private final RiskConfigBuilder<RiskLevelConfigValues> configBuilder;
  private final RiskLevelConfigValidator validator;
  private final RiskConfigServiceConfig riskConfig;

  @Override
  public RiskLevelConfigValues get() {
    RiskLevelConfigValues riskLevelConfigValues =
        configBuilder.mergeConfigs(
            riskConfig.getRiskScoringLevelConfigValues(), RISK_LEVEL_DEFAULT_CONFIG_FILE_PATH);
    Status validationStatus = validator.validateRiskLevelConfigValues(riskLevelConfigValues);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }
    return riskLevelConfigValues;
  }
}
