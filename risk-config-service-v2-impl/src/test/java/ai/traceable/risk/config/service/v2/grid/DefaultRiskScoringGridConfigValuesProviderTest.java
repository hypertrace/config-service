package ai.traceable.risk.config.service.v2.grid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.risk.config.service.v2.RiskConfigServiceConfig;
import ai.traceable.risk.config.service.v2.RiskConfigServiceRequestValidator;
import ai.traceable.risk.config.service.v2.RiskScoreCategory;
import ai.traceable.risk.config.service.v2.RiskScoringGridConfigValues;
import ai.traceable.risk.config.service.v2.grid.builder.RiskScoringGridConfigBuilder;
import ai.traceable.risk.config.service.v2.grid.validator.RiskScoringGridConfigValidator;
import ai.traceable.risk.config.service.v2.grid.validator.RiskScoringGridConfigValidatorImpl;
import com.typesafe.config.ConfigFactory;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

public class DefaultRiskScoringGridConfigValuesProviderTest {

  private final RiskScoringGridConfigBuilder configBuilder = new RiskScoringGridConfigBuilder();
  private final RiskScoringGridConfigValidator validator =
      new RiskScoringGridConfigValidatorImpl(new RiskConfigServiceRequestValidator());

  @Test
  public void testGetNoServiceConfig() {
    DefaultRiskScoringGridConfigValuesProvider provider =
        new DefaultRiskScoringGridConfigValuesProvider(
            configBuilder, validator, new RiskConfigServiceConfig(ConfigFactory.empty()));
    RiskScoringGridConfigValues configValues = provider.get();
    assertEquals(16, configValues.getRiskScoringGridCellsCount());
  }

  @Test
  public void testGetWithServiceConfig() {
    DefaultRiskScoringGridConfigValuesProvider provider =
        new DefaultRiskScoringGridConfigValuesProvider(
            configBuilder,
            validator,
            new RiskConfigServiceConfig(
                ConfigFactory.parseResources("valid-risk-scoring-grid-config-values.conf")));
    RiskScoringGridConfigValues configValues = provider.get();
    assertEquals(16, configValues.getRiskScoringGridCellsCount());
    configValues
        .getRiskScoringGridCellsList()
        .forEach(
            cell -> {
              if (cell.getLikelihoodScoreCategory()
                      .equals(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                  && cell.getImpactScoreCategory()
                      .equals(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)) {
                assertEquals(2, cell.getScore());
              } else if (cell.getLikelihoodScoreCategory()
                      .equals(RiskScoreCategory.RISK_SCORE_CATEGORY_MEDIUM)
                  && cell.getImpactScoreCategory()
                      .equals(RiskScoreCategory.RISK_SCORE_CATEGORY_MEDIUM)) {
                assertEquals(6, cell.getScore());
              }
            });
  }

  @Test
  public void testGetWithInvalidServiceConfig() {
    DefaultRiskScoringGridConfigValuesProvider provider =
        new DefaultRiskScoringGridConfigValuesProvider(
            configBuilder,
            validator,
            new RiskConfigServiceConfig(
                ConfigFactory.parseResources("invalid-risk-scoring-grid-config-values.conf")));
    assertThrows(StatusRuntimeException.class, provider::get);
  }
}
