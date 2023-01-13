package ai.traceable.risk.config.service.v2.level;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.risk.config.service.v2.RiskConfigServiceRequestValidator;
import ai.traceable.risk.config.service.v2.RiskLevelConfigValues;
import ai.traceable.risk.config.service.v2.level.builder.RiskLevelConfigBuilder;
import ai.traceable.risk.config.service.v2.level.validator.RiskLevelConfigValidator;
import ai.traceable.risk.config.service.v2.level.validator.RiskLevelConfigValidatorImpl;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

public class RiskLevelConfigBuilderTest {

  private final RiskLevelConfigBuilder configBuilder = new RiskLevelConfigBuilder();
  private final RiskLevelConfigValidator validator =
      new RiskLevelConfigValidatorImpl(new RiskConfigServiceRequestValidator());

  @Test
  public void testMergeConfigs() {
    {
      RiskLevelConfigValues highPriorityConfig = RiskLevelConfigValues.getDefaultInstance();
      RiskLevelConfigValues lowPriorityConfig =
          RiskLevelConfigValues.newBuilder()
              .setMediumLevelMinScore(2)
              .setHighLevelMinScore(5)
              .setCriticalLevelMinScore(8)
              .build();
      assertEquals(
          lowPriorityConfig, configBuilder.mergeConfigs(highPriorityConfig, lowPriorityConfig));
    }
    {
      RiskLevelConfigValues highPriorityConfig =
          RiskLevelConfigValues.newBuilder()
              .setMediumLevelMinScore(1)
              .setHighLevelMinScore(4)
              .setCriticalLevelMinScore(7)
              .build();
      RiskLevelConfigValues lowPriorityConfig =
          RiskLevelConfigValues.newBuilder()
              .setMediumLevelMinScore(2)
              .setHighLevelMinScore(5)
              .setCriticalLevelMinScore(8)
              .build();
      assertEquals(
          highPriorityConfig, configBuilder.mergeConfigs(highPriorityConfig, lowPriorityConfig));
    }
  }

  @Test
  public void testValidateConfig() {
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        validator
            .validateRiskLevelConfigValues(
                RiskLevelConfigValues.newBuilder().setCriticalLevelMinScore(-1).build())
            .getCode());
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        validator
            .validateRiskLevelConfigValues(
                RiskLevelConfigValues.newBuilder().setCriticalLevelMinScore(11).build())
            .getCode());
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        validator
            .validateRiskLevelConfigValues(
                RiskLevelConfigValues.newBuilder().setHighLevelMinScore(-1).build())
            .getCode());
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        validator
            .validateRiskLevelConfigValues(
                RiskLevelConfigValues.newBuilder().setHighLevelMinScore(11).build())
            .getCode());
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        validator
            .validateRiskLevelConfigValues(
                RiskLevelConfigValues.newBuilder().setMediumLevelMinScore(-1).build())
            .getCode());
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        validator
            .validateRiskLevelConfigValues(
                RiskLevelConfigValues.newBuilder().setMediumLevelMinScore(11).build())
            .getCode());
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        validator
            .validateRiskLevelConfigValues(
                RiskLevelConfigValues.newBuilder()
                    .setMediumLevelMinScore(9)
                    .setHighLevelMinScore(7)
                    .build())
            .getCode());
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        validator
            .validateRiskLevelConfigValues(
                RiskLevelConfigValues.newBuilder()
                    .setHighLevelMinScore(9)
                    .setCriticalLevelMinScore(7)
                    .build())
            .getCode());
  }
}
