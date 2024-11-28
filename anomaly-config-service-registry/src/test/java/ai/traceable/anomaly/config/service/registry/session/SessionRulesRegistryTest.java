package ai.traceable.anomaly.config.service.registry.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySeverityLevel;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.ObjectBolaAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionViolationConfig;
import ai.traceable.anomaly.config.service.v1.detector.UserIdBolaAnomalyConfig;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class SessionRulesRegistryTest {

  @Test
  public void testRules() throws Exception {
    SessionRulesRegistryImpl sessionRulesRegistry =
        new SessionRulesRegistryImpl(new ConfigConverter());

    Map<String, AnomalyRuleInfo> anomalyRuleInfos = sessionRulesRegistry.getSessionRuleInfos();
    assertEquals(3, anomalyRuleInfos.size());
    assertEquals(
        "bola :: Authorization Bypass - Object Level\nsessionv :: Session Violation\nuserIdBola :: Authorization Bypass - User Level",
        anomalyRuleInfos.values().stream()
            .map(
                anomalyRuleInfo ->
                    anomalyRuleInfo.getRuleId() + " :: " + anomalyRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.joining("\n")));

    AnomalyRuleInfo bolaRuleInfo = anomalyRuleInfos.get("bola");
    assertFalse(bolaRuleInfo.getEventDetails().getDescription().isBlank());
    assertFalse(bolaRuleInfo.getEventDetails().getMitigation().isBlank());
    assertFalse(bolaRuleInfo.getEventDetails().getImpact().isBlank());
    assertFalse(bolaRuleInfo.getEventDetails().getReferences().isBlank());
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
    SessionRulesRegistryImpl sessionDefinitionRegistry =
        new SessionRulesRegistryImpl(new ConfigConverter());

    Map<String, SessionDefinitionMetadataAnomalyDetectionConfig> ruleIdToConfigMap =
        sessionDefinitionRegistry.getSessionDefRuleIdToDetectionConfigMap();

    Map<String, SessionDefinitionMetadataAnomalyDetectionConfig> expectedMap =
        Map.of(
            "bola",
            SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                .setAnomalyRuleId("bola")
                .setObjectBola(ObjectBolaAnomalyConfig.getDefaultInstance())
                .build(),
            "userIdBola",
            SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                .setAnomalyRuleId("userIdBola")
                .setUserIdBola(UserIdBolaAnomalyConfig.getDefaultInstance())
                .build(),
            "sessionv",
            SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                .setAnomalyRuleId("sessionv")
                .setSessionViolation(SessionViolationConfig.getDefaultInstance())
                .build());

    assertEquals(expectedMap, ruleIdToConfigMap);

    Set<String> configRuleIds = ruleIdToConfigMap.keySet();
    Set<String> ruleInfoIds = sessionDefinitionRegistry.getSessionRuleInfos().keySet();

    assertEquals(ruleInfoIds, configRuleIds);
  }
}
