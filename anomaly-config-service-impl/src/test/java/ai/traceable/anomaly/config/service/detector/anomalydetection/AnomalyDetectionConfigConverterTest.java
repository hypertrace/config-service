package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyCategoryConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyEventCategory;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyEventScoreCategory;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
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
                    .setConfigStatus(AnomalyConfigStatus.newBuilder().setInternal(true).build())
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("rule")
                            .addSubRuleConfigs(subRuleConfig1)
                            .build())
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
                            .setAnomalyRuleId("rule")
                            .addSubRuleConfigs(subRuleConfig2)
                            .build())
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
            .getAnomalyRuleId());

    assertEquals(
        "subRule1",
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getSubRuleConfigsList()
            .get(0)
            .getSubRuleId());

    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_MALICIOUS,
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
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
            .getSubRuleConfigsList()
            .get(0)
            .getCategoryConfig()
            .getEventScoreCategory());

    assertTrue(
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getSubRuleConfigsList()
            .get(0)
            .getBlockingEnabled());
  }
}
