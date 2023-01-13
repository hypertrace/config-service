package ai.traceable.risk.config.service.v2.elements.builder;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.risk.config.service.v2.EnumOperator;
import ai.traceable.risk.config.service.v2.ResponseSensitivityPredicate;
import ai.traceable.risk.config.service.v2.ResponseSensitivityValue;
import ai.traceable.risk.config.service.v2.RiskElementConfig;
import ai.traceable.risk.config.service.v2.RiskElementPredicate;
import ai.traceable.risk.config.service.v2.RiskElementScoring;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RiskElementConfigBuilderTest {

  private RiskElementConfigBuilder elementConfigBuilder;

  @BeforeEach
  void setUp() {
    elementConfigBuilder = new RiskElementConfigBuilder();
  }

  @Test
  void testGetNewBuilder() {
    assertEquals(
        elementConfigBuilder.getNewBuilder().build(), RiskElementConfig.getDefaultInstance());
  }

  @Test
  void testMergeConfigsWithDefaultHighPriorityConfig() {
    final RiskElementConfig highPriorityConfig = RiskElementConfig.getDefaultInstance();
    final RiskElementConfig lowPriorityConfig = getLowPriorityConfig();

    final RiskElementConfig result =
        elementConfigBuilder.mergeConfigs(highPriorityConfig, lowPriorityConfig);

    assertEquals(lowPriorityConfig, result);
  }

  @Test
  void testMergeConfigsWithNonDefaultHighPriorityConfig() {
    final RiskElementConfig highPriorityConfig = getHighPriorityConfig();
    final RiskElementConfig lowPriorityConfig = getLowPriorityConfig();

    final RiskElementConfig result =
        elementConfigBuilder.mergeConfigs(highPriorityConfig, lowPriorityConfig);

    assertEquals(highPriorityConfig, result);
  }

  private RiskElementConfig getHighPriorityConfig() {
    return RiskElementConfig.newBuilder()
        .setId("responseSensitivityCritical")
        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(9))
        .setRiskElementPredicate(
            RiskElementPredicate.newBuilder()
                .setResponseSensitivity(
                    ResponseSensitivityPredicate.newBuilder()
                        .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                        .setValue(ResponseSensitivityValue.RESPONSE_SENSITIVITY_VALUE_CRITICAL)))
        .build();
  }

  private RiskElementConfig getLowPriorityConfig() {
    return RiskElementConfig.newBuilder()
        .setId("responseSensitivityCritical")
        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(8))
        .setRiskElementPredicate(
            RiskElementPredicate.newBuilder()
                .setResponseSensitivity(
                    ResponseSensitivityPredicate.newBuilder()
                        .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                        .setValue(ResponseSensitivityValue.RESPONSE_SENSITIVITY_VALUE_CRITICAL)))
        .build();
  }
}
