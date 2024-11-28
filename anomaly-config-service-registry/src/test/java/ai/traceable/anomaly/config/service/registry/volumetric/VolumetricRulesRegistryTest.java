package ai.traceable.anomaly.config.service.registry.volumetric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySeverityLevel;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.*;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class VolumetricRulesRegistryTest {

  @Test
  void testRules() throws Exception {
    VolumetricRulesRegistryImpl volumetricRulesRegistry =
        new VolumetricRulesRegistryImpl(new ConfigConverter());

    Map<String, AnomalyRuleInfo> anomalyRuleInfos =
        volumetricRulesRegistry.getVolumetricRuleInfos();
    assertEquals(2, anomalyRuleInfos.size());
    assertTrue(anomalyRuleInfos.containsKey("volumetricApiCallSpike"));
    assertEquals(
        "Unrestricted Resource Consumption",
        anomalyRuleInfos.get("volumetricApiCallSpike").getRuleName());
    assertTrue(anomalyRuleInfos.containsKey("volumetric"));
    assertEquals(
        "Unrestricted Resource Consumption", anomalyRuleInfos.get("volumetric").getRuleName());
    assertEquals(
        "volumetric_apiCallSpike",
        anomalyRuleInfos.get("volumetric").getSubRuleInfos(0).getRuleId());
    anomalyRuleInfos.entrySet().stream()
        .forEach(
            entry -> {
              for (AnomalySubRuleInfo subRuleInfo :
                  anomalyRuleInfos.get(entry.getKey()).getSubRuleInfosList()) {
                assertFalse(subRuleInfo.getEventLabelsMap().isEmpty());
                assertNotEquals(
                    AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_UNSPECIFIED,
                    subRuleInfo.getSeverityLevel());
              }
            });
  }

  @Test
  void testRuleIdToConfigMap() {
    VolumetricRulesRegistryImpl volumetricRulesRegistry =
        new VolumetricRulesRegistryImpl(new ConfigConverter());

    Map<String, VolumetricAnomalyDetectionConfig> ruleIdToConfigMap =
        volumetricRulesRegistry.getVolumetricRuleIdToConfigMap();

    Map<String, VolumetricAnomalyDetectionConfig> expectedMap =
        Map.of(
            "volumetricApiCallSpike",
            VolumetricAnomalyDetectionConfig.newBuilder()
                .setAnomalyRuleId("volumetricApiCallSpike")
                .setApiCallSpike(ApiCallSpikeAnomalyConfig.getDefaultInstance())
                .build(),
            "volumetric",
            VolumetricAnomalyDetectionConfig.newBuilder()
                .setAnomalyRuleId("volumetric")
                .setApiCallSpike(ApiCallSpikeAnomalyConfig.getDefaultInstance())
                .build());

    assertEquals(expectedMap, ruleIdToConfigMap);

    Set<String> configRuleIds = ruleIdToConfigMap.keySet();
    Set<String> ruleInfoIds = volumetricRulesRegistry.getVolumetricRuleInfos().keySet();

    assertEquals(ruleInfoIds, configRuleIds);
  }
}
