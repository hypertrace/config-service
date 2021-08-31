package ai.traceable.risk.config.service.level.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.risk.config.service.v1.RiskLevelConfigValues;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

public class RiskLevelConfigUtilsTest {

  private final RiskLevelConfigUtils configUtils = new RiskLevelConfigUtils();

  @Test
  public void testMergeConfigs() {
    {
      RiskLevelConfigValues highPriorityConfig = RiskLevelConfigValues.getDefaultInstance();
      RiskLevelConfigValues lowPriorityConfig =
          RiskLevelConfigValues.newBuilder()
              .setCriticalLevelMinScore(8)
              .setHighLevelMinScore(4)
              .build();
      assertEquals(
          lowPriorityConfig, configUtils.mergeConfigs(highPriorityConfig, lowPriorityConfig));
    }
    {
      RiskLevelConfigValues highPriorityConfig =
          RiskLevelConfigValues.newBuilder()
              .setCriticalLevelMinScore(6)
              .setHighLevelMinScore(4)
              .build();
      RiskLevelConfigValues lowPriorityConfig =
          RiskLevelConfigValues.newBuilder()
              .setCriticalLevelMinScore(8)
              .setHighLevelMinScore(4)
              .build();
      assertEquals(
          highPriorityConfig, configUtils.mergeConfigs(highPriorityConfig, lowPriorityConfig));
    }
  }

  @Test
  public void testIsConfigDefault() {
    {
      RiskLevelConfigValues specificConfig =
          RiskLevelConfigValues.newBuilder()
              .setCriticalLevelMinScore(6)
              .setHighLevelMinScore(4)
              .build();
      RiskLevelConfigValues defaultConfig =
          RiskLevelConfigValues.newBuilder()
              .setCriticalLevelMinScore(8)
              .setHighLevelMinScore(4)
              .build();
      assertFalse(configUtils.isConfigDefault(specificConfig, defaultConfig));
    }
    {
      RiskLevelConfigValues highPriorityConfig =
          RiskLevelConfigValues.newBuilder()
              .setCriticalLevelMinScore(6)
              .setHighLevelMinScore(4)
              .build();
      RiskLevelConfigValues lowPriorityConfig =
          RiskLevelConfigValues.newBuilder()
              .setCriticalLevelMinScore(6)
              .setHighLevelMinScore(4)
              .build();
      assertTrue(configUtils.isConfigDefault(highPriorityConfig, lowPriorityConfig));
    }
  }

  @Test
  public void testValidateConfig() {
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        configUtils
            .validateConfig(RiskLevelConfigValues.newBuilder().setCriticalLevelMinScore(-1).build())
            .getCode());
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        configUtils
            .validateConfig(RiskLevelConfigValues.newBuilder().setCriticalLevelMinScore(11).build())
            .getCode());
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        configUtils
            .validateConfig(RiskLevelConfigValues.newBuilder().setHighLevelMinScore(-1).build())
            .getCode());
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        configUtils
            .validateConfig(RiskLevelConfigValues.newBuilder().setHighLevelMinScore(11).build())
            .getCode());
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        configUtils
            .validateConfig(RiskLevelConfigValues.newBuilder().setMediumLevelMinScore(-1).build())
            .getCode());
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        configUtils
            .validateConfig(RiskLevelConfigValues.newBuilder().setMediumLevelMinScore(11).build())
            .getCode());
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        configUtils
            .validateConfig(
                RiskLevelConfigValues.newBuilder()
                    .setMediumLevelMinScore(9)
                    .setHighLevelMinScore(7)
                    .build())
            .getCode());
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        configUtils
            .validateConfig(
                RiskLevelConfigValues.newBuilder()
                    .setHighLevelMinScore(9)
                    .setCriticalLevelMinScore(7)
                    .build())
            .getCode());
  }
}
