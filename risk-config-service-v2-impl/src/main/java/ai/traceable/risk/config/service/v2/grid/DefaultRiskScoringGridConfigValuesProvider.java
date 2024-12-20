package ai.traceable.risk.config.service.v2.grid;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskConfigServiceConfig;
import ai.traceable.risk.config.service.v2.RiskScoringGridConfigValues;
import ai.traceable.risk.config.service.v2.grid.validator.RiskScoringGridConfigValidator;
import com.google.inject.Provider;
import io.grpc.Status;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.AllArgsConstructor;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultRiskScoringGridConfigValuesProvider
    implements Provider<RiskScoringGridConfigValues> {

  private static final String RISK_SCORING_GRID_DEFAULT_CONFIG_FILE_PATH =
      "risk-scoring-grid-config-values.conf";

  private final RiskConfigBuilder<RiskScoringGridConfigValues> riskConfigBuilder;
  private final RiskScoringGridConfigValidator validator;
  private final RiskConfigServiceConfig riskConfig;

  @Override
  public RiskScoringGridConfigValues get() {
    RiskScoringGridConfigValues riskScoringGridConfigValues =
        riskConfigBuilder.mergeConfigs(
            riskConfig.getRiskScoringGridConfigValues(),
            RISK_SCORING_GRID_DEFAULT_CONFIG_FILE_PATH);
    Status validationStatus = validator.validateRiskScoringGridValues(riskScoringGridConfigValues);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }
    return riskScoringGridConfigValues;
  }
}
