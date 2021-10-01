package ai.traceable.risk.config.service.factors.processor.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.risk.config.service.v1.CustomizationOptions;
import ai.traceable.risk.config.service.v1.RiskElementConfig;
import ai.traceable.risk.config.service.v1.RiskElementInfo;
import ai.traceable.risk.config.service.v1.RiskElementScoring;
import ai.traceable.risk.config.service.v1.RiskFactor;
import ai.traceable.risk.config.service.v1.RiskFactorConfig;
import ai.traceable.risk.config.service.v1.RiskFactorScoreContribution;
import ai.traceable.risk.config.service.v1.RiskFactorScoring;
import ai.traceable.risk.config.service.v1.StringOperator;
import ai.traceable.risk.config.service.v1.StringPredicate;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class RiskFactorConfigUtilsTest {

  private final RiskFactorConfigUtils configUtils =
      new RiskFactorConfigUtils(new RiskElementConfigUtils());

  @Test
  public void testMergeConfigsScoring() {
    RiskFactorConfig specificConfig =
        RiskFactorConfig.newBuilder()
            .setId("id")
            .setRiskFactorScoring(
                RiskFactorScoring.newBuilder()
                    .setDisabled(true)
                    .setScoreContribution(
                        RiskFactorScoreContribution.RISK_FACTOR_SCORE_CONTRIBUTION_ABSOLUTE))
            .build();

    RiskFactor defaultFactor =
        RiskFactor.newBuilder()
            .setRiskFactorConfig(RiskFactorConfig.newBuilder().setId("id"))
            .build();
    assertEquals(
        defaultFactor.toBuilder()
            .setRiskFactorConfig(
                RiskFactorConfig.newBuilder()
                    .setId("id")
                    .setRiskFactorScoring(
                        RiskFactorScoring.newBuilder()
                            .setDisabled(true)
                            .setScoreContribution(
                                RiskFactorScoreContribution
                                    .RISK_FACTOR_SCORE_CONTRIBUTION_UNSPECIFIED)))
            .build(),
        configUtils.mergeConfigs(specificConfig, Collections.emptyList(), defaultFactor));

    defaultFactor =
        RiskFactor.newBuilder()
            .setRiskFactorConfig(RiskFactorConfig.newBuilder().setId("id"))
            .addCustomizationOptions(
                CustomizationOptions.CUSTOMIZATION_OPTIONS_FACTOR_SCORE_CONTRIBUTION)
            .build();
    assertEquals(
        defaultFactor.toBuilder().setRiskFactorConfig(specificConfig).build(),
        configUtils.mergeConfigs(specificConfig, Collections.emptyList(), defaultFactor));

    specificConfig = RiskFactorConfig.newBuilder().setId("id").build();
    defaultFactor =
        RiskFactor.newBuilder()
            .setRiskFactorConfig(
                RiskFactorConfig.newBuilder()
                    .setId("id")
                    .setRiskFactorScoring(RiskFactorScoring.newBuilder().setDisabled(true)))
            .setIsDefault(true)
            .build();
    assertEquals(
        defaultFactor,
        configUtils.mergeConfigs(specificConfig, Collections.emptyList(), defaultFactor));
  }

  @Test
  public void testMergeConfigsElements() {
    RiskFactorConfig specificConfig =
        RiskFactorConfig.newBuilder()
            .setId("id")
            .setRiskFactorScoring(RiskFactorScoring.getDefaultInstance())
            .addRiskElementConfigs(
                RiskElementConfig.newBuilder()
                    .setId("id1")
                    .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(4))
                    .build())
            .build();

    RiskElementConfig extraElementConfig1 =
        RiskElementConfig.newBuilder()
            .setId("id1")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(6))
            .build();
    RiskElementConfig extraElementConfig2 =
        RiskElementConfig.newBuilder()
            .setId("id2")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(6))
            .build();
    RiskElementConfig extraElementConfig3 =
        RiskElementConfig.newBuilder()
            .setId("id3")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(6))
            .build();
    List<RiskElementConfig> elementConfigs =
        List.of(extraElementConfig1, extraElementConfig2, extraElementConfig3);

    {
      assertThrows(
          StatusRuntimeException.class,
          () ->
              configUtils.mergeConfigs(
                  specificConfig,
                  elementConfigs,
                  RiskFactor.newBuilder()
                      .setRiskFactorConfig(
                          RiskFactorConfig.newBuilder()
                              .setId("random")
                              .setRiskFactorScoring(RiskFactorScoring.getDefaultInstance()))
                      .build()));
    }
    {
      RiskFactor defaultFactor1a =
          RiskFactor.newBuilder()
              .setRiskFactorConfig(
                  RiskFactorConfig.newBuilder()
                      .setId("id")
                      .setRiskFactorScoring(RiskFactorScoring.getDefaultInstance()))
              .build();
      assertThrows(
          StatusRuntimeException.class,
          () -> configUtils.mergeConfigs(specificConfig, elementConfigs, defaultFactor1a));
      RiskFactor defaultFactor1b =
          defaultFactor1a.toBuilder()
              .addCustomizationOptions(
                  CustomizationOptions.CUSTOMIZATION_OPTIONS_ELEMENT_ADD_DELETE)
              .build();
      assertEquals(
          defaultFactor1b.toBuilder().setRiskFactorConfig(specificConfig).build(),
          configUtils.mergeConfigs(specificConfig, elementConfigs, defaultFactor1b));
    }
    {
      RiskFactor defaultFactor2 =
          RiskFactor.newBuilder()
              .setRiskFactorConfig(
                  RiskFactorConfig.newBuilder()
                      .setId("id")
                      .setRiskFactorScoring(RiskFactorScoring.getDefaultInstance())
                      .addRiskElementConfigs(
                          RiskElementConfig.newBuilder()
                              .setId("id1")
                              .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3))
                              .build()))
              .build();
      assertEquals(
          defaultFactor2.toBuilder().setRiskFactorConfig(specificConfig).build(),
          configUtils.mergeConfigs(specificConfig, Collections.emptyList(), defaultFactor2));
      assertEquals(
          defaultFactor2.toBuilder()
              .setRiskFactorConfig(
                  specificConfig.toBuilder().setRiskElementConfigs(0, extraElementConfig1))
              .build(),
          configUtils.mergeConfigs(specificConfig, elementConfigs, defaultFactor2));
      defaultFactor2 =
          defaultFactor2.toBuilder()
              .addCustomizationOptions(
                  CustomizationOptions.CUSTOMIZATION_OPTIONS_ELEMENT_ADD_DELETE)
              .build();
      assertEquals(
          defaultFactor2.toBuilder().setRiskFactorConfig(specificConfig).build(),
          configUtils.mergeConfigs(specificConfig, elementConfigs, defaultFactor2));
    }
    {
      RiskElementConfig elementConfig =
          RiskElementConfig.newBuilder()
              .setId("id2")
              .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(5))
              .build();
      RiskFactor defaultFactor3 =
          RiskFactor.newBuilder()
              .setRiskFactorConfig(
                  RiskFactorConfig.newBuilder()
                      .setId("id")
                      .setRiskFactorScoring(RiskFactorScoring.getDefaultInstance())
                      .addRiskElementConfigs(elementConfig))
              .addCustomizationOptions(
                  CustomizationOptions.CUSTOMIZATION_OPTIONS_ELEMENT_ADD_DELETE)
              .build();
      assertEquals(
          defaultFactor3.toBuilder().setRiskFactorConfig(specificConfig).build(),
          configUtils.mergeConfigs(specificConfig, elementConfigs, defaultFactor3));
    }
  }

  @Test
  public void testIsConfigDefault() {
    {
      RiskFactorConfig specificConfig =
          RiskFactorConfig.newBuilder()
              .setId("id")
              .setRiskFactorScoring(
                  RiskFactorScoring.newBuilder()
                      .setScoreContribution(
                          RiskFactorScoreContribution.RISK_FACTOR_SCORE_CONTRIBUTION_UNSPECIFIED))
              .build();
      RiskFactorConfig defaultConfig = RiskFactorConfig.newBuilder().setId("id").build();
      assertTrue(configUtils.isConfigDefault(specificConfig, defaultConfig));
    }
    {
      RiskFactorConfig specificConfig =
          RiskFactorConfig.newBuilder()
              .setId("id")
              .setRiskFactorScoring(RiskFactorScoring.newBuilder().setDisabled(true))
              .build();
      RiskFactorConfig defaultConfig = RiskFactorConfig.newBuilder().setId("id").build();
      assertFalse(configUtils.isConfigDefault(specificConfig, defaultConfig));
    }
    {
      RiskFactorConfig specificConfig =
          RiskFactorConfig.newBuilder()
              .setId("id")
              .addRiskElementConfigs(
                  RiskElementConfig.newBuilder()
                      .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3)))
              .build();
      RiskFactorConfig defaultConfig = RiskFactorConfig.newBuilder().setId("id").build();
      assertFalse(configUtils.isConfigDefault(specificConfig, defaultConfig));
    }
    {
      RiskFactorConfig specificConfig =
          RiskFactorConfig.newBuilder()
              .setId("id")
              .addRiskElementConfigs(
                  RiskElementConfig.newBuilder()
                      .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3)))
              .build();
      RiskFactorConfig defaultConfig =
          RiskFactorConfig.newBuilder()
              .setId("id")
              .addRiskElementConfigs(
                  RiskElementConfig.newBuilder()
                      .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(4)))
              .build();
      assertFalse(configUtils.isConfigDefault(specificConfig, defaultConfig));
    }
    {
      RiskFactorConfig specificConfig =
          RiskFactorConfig.newBuilder()
              .setId("id")
              .addRiskElementConfigs(
                  RiskElementConfig.newBuilder()
                      .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3)))
              .build();
      RiskFactorConfig defaultConfig = RiskFactorConfig.newBuilder().setId("id2").build();
      assertFalse(configUtils.isConfigDefault(specificConfig, defaultConfig));
    }
  }

  @Test
  public void testValidateConfig() {
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        configUtils.validateConfig(RiskFactorConfig.getDefaultInstance()).getCode());
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        configUtils
            .validateConfig(
                RiskFactorConfig.newBuilder()
                    .setId("id")
                    .addRiskElementConfigs(
                        RiskElementConfig.newBuilder()
                            .setId("id")
                            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(13)))
                    .build())
            .getCode());
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        configUtils
            .validateConfig(
                RiskFactorConfig.newBuilder()
                    .setId("id")
                    .addRiskElementConfigs(
                        RiskElementConfig.newBuilder()
                            .setId("id")
                            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3)))
                    .build())
            .getCode());
    assertEquals(
        Status.OK.getCode(),
        configUtils
            .validateConfig(
                RiskFactorConfig.newBuilder()
                    .setId("id")
                    .addRiskElementConfigs(
                        RiskElementConfig.newBuilder()
                            .setId("id")
                            .setRiskElementInfo(
                                RiskElementInfo.newBuilder()
                                    .setLabelId(
                                        StringPredicate.newBuilder()
                                            .setOperator(StringOperator.STRING_OPERATOR_EQUALS)
                                            .setValue("random")))
                            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3)))
                    .build())
            .getCode());
  }
}
