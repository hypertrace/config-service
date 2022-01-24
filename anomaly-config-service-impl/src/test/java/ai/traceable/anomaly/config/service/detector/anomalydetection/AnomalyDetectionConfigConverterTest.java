package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyCategoryConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyEventCategory;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyEventScoreCategory;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiStateBasedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.EnumerationsAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.IntegerAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.LearntApiAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAllDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AnomalyDetectionConfigConverterTest {

  private final AnomalyDetectionConfigConverter detectionConfigConverter =
      new AnomalyDetectionConfigConverter();

  @Test
  void testModsecConfigConvert() throws InvalidProtocolBufferException {
    AnomalySubRuleConfig subRuleConfig1 =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("subRule1")
            .setCategoryConfig(
                AnomalyCategoryConfig.newBuilder()
                    .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_MALICIOUS))
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setInternal(false))
            .setBlockingEnabled(true)
            .build();

    AnomalySubRuleConfig subRuleConfig2 =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("subRule1")
            .setCategoryConfig(
                AnomalyCategoryConfig.newBuilder()
                    .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT)
                    .setEventScoreCategory(
                        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_MEDIUM)
                    .build())
            .setConfigStatus(
                AnomalyConfigStatusChange.newBuilder().setInternal(true).setDisabled(true))
            .build();

    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig1 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setApiScope(AnomalyApiScope.newBuilder().setId("api").build())
                    .build())
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT)
                            .build())
                    .setConfigStatus(
                        AnomalyConfigStatusChange.newBuilder().setInternal(true).build())
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setModsecAnomalyRule(
                                ModsecurityAnomalyRuleConfig.newBuilder()
                                    .setAnomalyRuleId("rule")
                                    .addSubRuleConfigs(subRuleConfig1)
                                    .build()))
                    .build())
            .build();

    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig2 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                    .build())
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventScoreCategory(
                                AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW)
                            .build())
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setModsecAnomalyRule(
                                ModsecurityAnomalyRuleConfig.newBuilder()
                                    .setAnomalyRuleId("rule")
                                    .addSubRuleConfigs(subRuleConfig2)
                                    .build()))
                    .build())
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setModsecAllDetection(
                                ModsecurityAllDetectionConfig.newBuilder()
                                    .setEnabledOnAllEntrySpans(true)
                                    .setExcludedParams(
                                        StringList.newBuilder()
                                            .addAllValues(List.of("a", "b", "c"))
                                            .build())
                                    .build()))
                    .build())
            .build();

    Value value = detectionConfigConverter.convert(scopedAnomalyDetectionConfig1);

    assertEquals(scopedAnomalyDetectionConfig1, detectionConfigConverter.convert(value));

    ScopedAnomalyDetectionConfig resultConfig =
        detectionConfigConverter.merge(
            scopedAnomalyDetectionConfig1, scopedAnomalyDetectionConfig2);
    assertEquals("api", resultConfig.getConfigScope().getApiScope().getId());
    assertTrue(
        resultConfig.getAnomalyDetectionConfigsList().get(0).getConfigStatus().getInternal());
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getCategoryConfig()
            .getEventScoreCategory());
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getCategoryConfig()
            .getEventCategory());
    assertEquals(
        "rule",
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getAnomalyRuleId());

    assertEquals(
        "subRule1",
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigsList()
            .get(0)
            .getSubRuleId());

    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_MALICIOUS,
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigsList()
            .get(0)
            .getCategoryConfig()
            .getEventCategory());

    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_MEDIUM,
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigsList()
            .get(0)
            .getCategoryConfig()
            .getEventScoreCategory());

    assertTrue(
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigsList()
            .get(0)
            .getBlockingEnabled());

    assertTrue(
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigsList()
            .get(0)
            .getConfigStatus()
            .getDisabled());

    assertFalse(
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigsList()
            .get(0)
            .getConfigStatus()
            .getInternal());

    assertTrue(
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(1)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAllDetection()
            .getEnabledOnAllEntrySpans());

    assertEquals(
        List.of("a", "b", "c"),
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(1)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAllDetection()
            .getExcludedParams()
            .getValuesList());
  }

  @Test
  void testStateBasedDetectionConfigsConvert() throws InvalidProtocolBufferException {
    ScopedAnomalyDetectionConfig config1 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setConfigStatus(
                        AnomalyConfigStatusChange.newBuilder()
                            .setDisabled(true)
                            .setInternal(true)
                            .build())
                    .setApiStateBasedAnomalyDetectionConfig(
                        ApiStateBasedAnomalyDetectionConfig.newBuilder()
                            .setLearntApi(
                                LearntApiAnomalyConfig.newBuilder()
                                    .setModsecurityEnabled(true)
                                    .setEvaluateAllModsecurityRules(true)
                                    .build())
                            .build())
                    .build())
            .build();

    ScopedAnomalyDetectionConfig config2 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT)
                            .setEventScoreCategory(
                                AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW))
                    .setApiStateBasedAnomalyDetectionConfig(
                        ApiStateBasedAnomalyDetectionConfig.newBuilder()
                            .setLearntApi(
                                LearntApiAnomalyConfig.newBuilder()
                                    .setEvaluateAllModsecurityRules(false)
                                    .build())
                            .build())
                    .build())
            .build();

    Value value = detectionConfigConverter.convert(config1);
    assertEquals(config1, detectionConfigConverter.convert(value));

    ScopedAnomalyDetectionConfig mergedConfig = detectionConfigConverter.merge(config2, config1);
    AnomalyDetectionConfig detectionConfig = mergedConfig.getAnomalyDetectionConfigsList().get(0);

    assertTrue(detectionConfig.getConfigStatus().getDisabled());
    assertTrue(detectionConfig.getConfigStatus().getInternal());
    assertTrue(
        detectionConfig
            .getApiStateBasedAnomalyDetectionConfig()
            .getLearntApi()
            .getModsecurityEnabled());
    // overridden values
    assertFalse(
        detectionConfig
            .getApiStateBasedAnomalyDetectionConfig()
            .getLearntApi()
            .getEvaluateAllModsecurityRules());
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        detectionConfig.getCategoryConfig().getEventCategory());
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        detectionConfig.getCategoryConfig().getEventScoreCategory());
  }

  @Test
  void testApiDefinitionDetectionConfigsConvert() throws InvalidProtocolBufferException {
    ScopedAnomalyDetectionConfig config1 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setConfigStatus(
                        AnomalyConfigStatusChange.newBuilder()
                            .setDisabled(true)
                            .setInternal(true)
                            .build())
                    .setApiDefinitionMetadataAnomalyDetectionConfig(
                        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("integer")
                            .setInteger(
                                IntegerAnomalyConfig.newBuilder()
                                    .setMaxLengthDifference(5)
                                    .setThresholdPercent(0.1))
                            .build())
                    .build())
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setApiDefinitionMetadataAnomalyDetectionConfig(
                        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("enum")
                            .setEnum(EnumerationsAnomalyConfig.getDefaultInstance())
                            .build())
                    .build())
            .build();

    ScopedAnomalyDetectionConfig config2 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT)
                            .setEventScoreCategory(
                                AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW))
                    .setApiDefinitionMetadataAnomalyDetectionConfig(
                        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                            .setInteger(
                                IntegerAnomalyConfig.newBuilder().setMaxLengthDifference(10))
                            .build())
                    .build())
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setConfigStatus(
                        AnomalyConfigStatusChange.newBuilder().setInternal(true).setDisabled(false))
                    .setApiDefinitionMetadataAnomalyDetectionConfig(
                        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("enum")
                            .build())
                    .build())
            .build();

    Value value = detectionConfigConverter.convert(config1);
    assertEquals(config1, detectionConfigConverter.convert(value));

    ScopedAnomalyDetectionConfig mergedConfig = detectionConfigConverter.merge(config2, config1);

    AnomalyDetectionConfig detectionConfig =
        getAnomalyDetectionConfig(
            mergedConfig.getAnomalyDetectionConfigsList(),
            ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.ENUM);

    assertFalse(detectionConfig.getConfigStatus().getDisabled());
    assertTrue(detectionConfig.getConfigStatus().getInternal());
    assertEquals(
        ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.ENUM,
        detectionConfig.getApiDefinitionMetadataAnomalyDetectionConfig().getConfigCase());

    detectionConfig =
        getAnomalyDetectionConfig(
            mergedConfig.getAnomalyDetectionConfigsList(),
            ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.INTEGER);

    assertTrue(detectionConfig.getConfigStatus().getDisabled());
    assertTrue(detectionConfig.getConfigStatus().getInternal());
    assertEquals(
        0.1,
        detectionConfig
            .getApiDefinitionMetadataAnomalyDetectionConfig()
            .getInteger()
            .getThresholdPercent());
    // overridden values
    assertEquals(
        10,
        detectionConfig
            .getApiDefinitionMetadataAnomalyDetectionConfig()
            .getInteger()
            .getMaxLengthDifference());
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        detectionConfig.getCategoryConfig().getEventCategory());
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        detectionConfig.getCategoryConfig().getEventScoreCategory());
  }

  private AnomalyDetectionConfig getAnomalyDetectionConfig(
      List<AnomalyDetectionConfig> detectionConfigs,
      ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase) {
    for (AnomalyDetectionConfig detectionConfig : detectionConfigs) {
      if (detectionConfig
          .getApiDefinitionMetadataAnomalyDetectionConfig()
          .getConfigCase()
          .equals(configCase)) return detectionConfig;
    }
    return null;
  }
}
