package ai.traceable.anomaly.config.service.registry.credentialstuffing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.CredentialStuffingAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.CredentialStuffingAnomalyDetectionConfig;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class CredentialStuffingRulesRegistryTest {
  @Test
  void testRules() throws Exception {
    CredentialStuffingRulesRegistryImpl credentialStuffingRulesRegistry =
        new CredentialStuffingRulesRegistryImpl(new ConfigConverter());
    Map<String, AnomalyRuleInfo> anomalyRuleInfoMap =
        credentialStuffingRulesRegistry.getCredentialStuffingRuleInfos();
    assertEquals(1, anomalyRuleInfoMap.size());
    assertEquals(
        "credentialStuffing :: Credential Stuffing",
        anomalyRuleInfoMap.values().stream()
            .map(
                anomalyRuleInfo ->
                    anomalyRuleInfo.getRuleId() + " :: " + anomalyRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.joining("\n")));
    AnomalyRuleInfo bolaRuleInfo = anomalyRuleInfoMap.get("credentialStuffing");
    assertFalse(bolaRuleInfo.getEventDetails().getDescription().isBlank());
    assertFalse(bolaRuleInfo.getEventDetails().getMitigation().isBlank());
  }

  @Test
  void testRuleIdToConfigMap() {
    CredentialStuffingRulesRegistryImpl credentialStuffingRulesRegistry =
        new CredentialStuffingRulesRegistryImpl(new ConfigConverter());

    Map<String, CredentialStuffingAnomalyDetectionConfig> ruleIdToConfigMap =
        credentialStuffingRulesRegistry.getCredentialStuffingRuleIdToConfigMap();

    Map<String, CredentialStuffingAnomalyDetectionConfig> expectedMap =
        Map.of(
            "credentialStuffing",
            CredentialStuffingAnomalyDetectionConfig.newBuilder()
                .setAnomalyRuleId("credentialStuffing")
                .setCredentialStuffing(CredentialStuffingAnomalyConfig.getDefaultInstance())
                .build());

    assertEquals(expectedMap, ruleIdToConfigMap);
    Set<String> configRuleIds = ruleIdToConfigMap.keySet();
    Set<String> ruleInfoIds =
        credentialStuffingRulesRegistry.getCredentialStuffingRuleInfos().keySet();
    assertEquals(ruleInfoIds, configRuleIds);
  }
}
