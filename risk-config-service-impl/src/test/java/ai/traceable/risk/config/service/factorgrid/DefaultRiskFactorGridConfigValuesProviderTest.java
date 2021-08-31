package ai.traceable.risk.config.service.factorgrid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.risk.config.service.RiskConfigServiceConfig;
import ai.traceable.risk.config.service.factorgrid.processor.RiskFactorGridConfigUtils;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfigValues;
import ai.traceable.risk.config.service.v1.RiskScoreCategory;
import com.typesafe.config.ConfigFactory;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

public class DefaultRiskFactorGridConfigValuesProviderTest {

  private final RiskFactorGridConfigUtils configUtils = new RiskFactorGridConfigUtils();

  @Test
  public void testGetNoServiceConfig() {
    DefaultRiskFactorGridConfigValuesProvider provider =
        new DefaultRiskFactorGridConfigValuesProvider(
            configUtils, new RiskConfigServiceConfig(ConfigFactory.empty()));
    RiskFactorGridConfigValues configValues = provider.get();
    assertEquals(16, configValues.getRiskFactorGridCellsCount());
  }

  @Test
  public void testGetWithServiceConfig() {
    DefaultRiskFactorGridConfigValuesProvider provider =
        new DefaultRiskFactorGridConfigValuesProvider(
            configUtils,
            new RiskConfigServiceConfig(
                ConfigFactory.parseString(
                    "riskFactorGridConfigValues={riskFactorGridCells = [\n"
                        + "  {\n"
                        + "    likelihoodScoreCategory = RISK_SCORE_CATEGORY_LOW\n"
                        + "    impactScoreCategory = RISK_SCORE_CATEGORY_LOW\n"
                        + "    score = 2\n"
                        + "  },\n"
                        + "  {\n"
                        + "    likelihoodScoreCategory = RISK_SCORE_CATEGORY_MEDIUM\n"
                        + "    impactScoreCategory = RISK_SCORE_CATEGORY_LOW\n"
                        + "    score = 3\n"
                        + "  }\n"
                        + "]}")));
    RiskFactorGridConfigValues configValues = provider.get();
    assertEquals(16, configValues.getRiskFactorGridCellsCount());
    configValues
        .getRiskFactorGridCellsList()
        .forEach(
            cell -> {
              if (cell.getLikelihoodScoreCategory() == RiskScoreCategory.RISK_SCORE_CATEGORY_LOW
                  && cell.getImpactScoreCategory() == RiskScoreCategory.RISK_SCORE_CATEGORY_LOW) {
                assertEquals(2, cell.getScore());
              } else if (cell.getLikelihoodScoreCategory()
                      == RiskScoreCategory.RISK_SCORE_CATEGORY_MEDIUM
                  && cell.getImpactScoreCategory() == RiskScoreCategory.RISK_SCORE_CATEGORY_LOW) {
                assertEquals(3, cell.getScore());
              }
            });
  }

  @Test
  public void testGetWithInvalidServiceConfig() {
    assertThrows(
        StatusRuntimeException.class,
        () ->
            new DefaultRiskFactorGridConfigValuesProvider(
                configUtils,
                new RiskConfigServiceConfig(
                    ConfigFactory.parseString(
                        "riskFactorGridConfigValues={riskFactorGridCells = [\n"
                            + "  {\n"
                            + "    likelihoodScoreCategory = RISK_SCORE_CATEGORY_LOW\n"
                            + "    impactScoreCategory = RISK_SCORE_CATEGORY_LOW\n"
                            + "    score = 20\n"
                            + "  },\n"
                            + "  {\n"
                            + "    likelihoodScoreCategory = RISK_SCORE_CATEGORY_MEDIUM\n"
                            + "    impactScoreCategory = RISK_SCORE_CATEGORY_LOW\n"
                            + "    score = 3\n"
                            + "  }\n"
                            + "]}"))));
  }
}
