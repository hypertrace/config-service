package ai.traceable.risk.config.service.factors.processor.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.risk.config.service.v1.RiskElementConfig;
import ai.traceable.risk.config.service.v1.RiskElementScoring;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

public class RiskElementConfigUtilsTest {

  private final RiskElementConfigUtils configUtils = new RiskElementConfigUtils();

  @Test
  public void testMergeConfigs() {
    {
      RiskElementConfig highPriorityConfig = RiskElementConfig.getDefaultInstance();
      RiskElementConfig lowPriorityConfig =
          RiskElementConfig.newBuilder()
              .setId("id")
              .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3))
              .build();
      assertEquals(
          lowPriorityConfig, configUtils.mergeConfigs(highPriorityConfig, lowPriorityConfig));
    }
    {
      RiskElementConfig highPriorityConfig =
          RiskElementConfig.newBuilder()
              .setId("id")
              .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3))
              .build();
      RiskElementConfig lowPriorityConfig =
          RiskElementConfig.newBuilder()
              .setId("id")
              .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(5))
              .build();
      assertEquals(
          highPriorityConfig, configUtils.mergeConfigs(highPriorityConfig, lowPriorityConfig));
    }
  }

  @Test
  public void testIsConfigDefault() {
    {
      RiskElementConfig specificConfig =
          RiskElementConfig.newBuilder()
              .setId("id")
              .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3))
              .build();
      RiskElementConfig defaultConfig =
          RiskElementConfig.newBuilder()
              .setId("id")
              .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(4))
              .build();
      assertFalse(configUtils.isConfigDefault(specificConfig, defaultConfig));
    }
    {
      RiskElementConfig specificConfig =
          RiskElementConfig.newBuilder()
              .setId("id")
              .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3))
              .build();
      RiskElementConfig defaultConfig =
          RiskElementConfig.newBuilder()
              .setId("id")
              .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3))
              .build();
      assertTrue(configUtils.isConfigDefault(specificConfig, defaultConfig));
    }
  }

  @Test
  public void testValidateConfig() {
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        configUtils.validateConfig(RiskElementConfig.getDefaultInstance()).getCode());
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        configUtils
            .validateConfig(
                RiskElementConfig.newBuilder()
                    .setId("id")
                    .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(13))
                    .build())
            .getCode());
  }
}
