package ai.traceable.risk.config.service.v2.level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.risk.config.service.v2.RiskConfigServiceConfig;
import ai.traceable.risk.config.service.v2.RiskConfigServiceRequestValidator;
import ai.traceable.risk.config.service.v2.RiskLevelConfigValues;
import ai.traceable.risk.config.service.v2.level.builder.RiskLevelConfigBuilder;
import ai.traceable.risk.config.service.v2.level.validator.RiskLevelConfigValidator;
import ai.traceable.risk.config.service.v2.level.validator.RiskLevelConfigValidatorImpl;
import com.typesafe.config.ConfigFactory;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

public class DefaultRiskLevelConfigValuesProviderTest {

  private final RiskLevelConfigBuilder configBuilder = new RiskLevelConfigBuilder();
  private final RiskLevelConfigValidator validator =
      new RiskLevelConfigValidatorImpl(new RiskConfigServiceRequestValidator());

  @Test
  public void testGetNoServiceConfig() {
    DefaultRiskLevelConfigValuesProvider provider =
        new DefaultRiskLevelConfigValuesProvider(
            configBuilder, validator, new RiskConfigServiceConfig(ConfigFactory.empty()));
    RiskLevelConfigValues configValues = provider.get();
    assertEquals(3, configValues.getMediumLevelMinScore());
    assertEquals(6, configValues.getHighLevelMinScore());
    assertEquals(9, configValues.getCriticalLevelMinScore());
  }

  @Test
  public void testGetWithServiceConfig() {
    DefaultRiskLevelConfigValuesProvider provider =
        new DefaultRiskLevelConfigValuesProvider(
            configBuilder,
            validator,
            new RiskConfigServiceConfig(
                ConfigFactory.parseResources("valid-risk-scoring-level-config-values.conf")));
    RiskLevelConfigValues configValues = provider.get();
    assertEquals(1, configValues.getMediumLevelMinScore());
    assertEquals(5, configValues.getHighLevelMinScore());
    assertEquals(9, configValues.getCriticalLevelMinScore());
  }

  @Test
  public void testGetWithInvalidServiceConfig() {
    DefaultRiskLevelConfigValuesProvider provider =
        new DefaultRiskLevelConfigValuesProvider(
            configBuilder,
            validator,
            new RiskConfigServiceConfig(
                ConfigFactory.parseResources("invalid-risk-scoring-level-config-values.conf")));
    assertThrows(StatusRuntimeException.class, provider::get);
  }
}
