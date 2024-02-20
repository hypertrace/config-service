package ai.traceable.anomaly.config.service.registry.volumetric;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.*;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class VolumetricRulesRegistryTest {

  @Test
  void testRules() throws Exception {
    VolumetricRulesRegistryImpl volumetricRulesRegistry =
        new VolumetricRulesRegistryImpl(new ConfigConverter());

    Map<String, AnomalyRuleInfo> anomalyRuleInfos =
        volumetricRulesRegistry.getVolumetricRuleInfos();
    assertEquals(1, anomalyRuleInfos.size());
    assertEquals(
        "volumetricApiCallSpike :: Volumetric API Call Spike",
        anomalyRuleInfos.values().stream()
            .map(
                anomalyRuleInfo ->
                    anomalyRuleInfo.getRuleId() + " :: " + anomalyRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.joining("\n")));
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
                .build());

    assertEquals(expectedMap, ruleIdToConfigMap);

    Set<String> configRuleIds = ruleIdToConfigMap.keySet();
    Set<String> ruleInfoIds = volumetricRulesRegistry.getVolumetricRuleInfos().keySet();

    assertEquals(ruleInfoIds, configRuleIds);
  }
}
