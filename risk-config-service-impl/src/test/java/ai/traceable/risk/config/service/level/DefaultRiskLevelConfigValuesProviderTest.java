package ai.traceable.risk.config.service.level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.risk.config.service.RiskConfigServiceConfig;
import ai.traceable.risk.config.service.level.processor.RiskLevelConfigUtils;
import ai.traceable.risk.config.service.v1.RiskLevelConfigValues;
import com.typesafe.config.ConfigFactory;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

public class DefaultRiskLevelConfigValuesProviderTest {

  private final RiskLevelConfigUtils configUtils = new RiskLevelConfigUtils();

  @Test
  public void testGetNoServiceConfig() {
    DefaultRiskLevelConfigValuesProvider provider =
        new DefaultRiskLevelConfigValuesProvider(
            configUtils, new RiskConfigServiceConfig(ConfigFactory.empty()));
    RiskLevelConfigValues configValues = provider.get();
    assertEquals(3, configValues.getMediumLevelMinScore());
    assertEquals(6, configValues.getHighLevelMinScore());
    assertEquals(9, configValues.getCriticalLevelMinScore());
  }

  @Test
  public void testGetWithServiceConfig() {
    DefaultRiskLevelConfigValuesProvider provider =
        new DefaultRiskLevelConfigValuesProvider(
            configUtils,
            new RiskConfigServiceConfig(
                ConfigFactory.parseString(
                    "riskLevelConfigValues={mediumLevelMinScore = 1\n"
                        + "highLevelMinScore = 5\n"
                        + "criticalLevelMinScore = 9}")));
    RiskLevelConfigValues configValues = provider.get();
    assertEquals(1, configValues.getMediumLevelMinScore());
    assertEquals(5, configValues.getHighLevelMinScore());
    assertEquals(9, configValues.getCriticalLevelMinScore());
  }

  @Test
  public void testGetWithInvalidServiceConfig() {
    assertThrows(
        StatusRuntimeException.class,
        () ->
            new DefaultRiskLevelConfigValuesProvider(
                configUtils,
                new RiskConfigServiceConfig(
                    ConfigFactory.parseString(
                        "riskLevelConfigValues={mediumLevelMinScore = 1\n"
                            + "highLevelMinScore = 8\n"
                            + "criticalLevelMinScore = 4}"))));
  }
}
